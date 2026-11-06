"""Schemas read from a database's catalog.

Binary COPY carries no types, so decoding needs a schema. These helpers build
one from the catalog with any DB-API cursor, so psycopg2 and psycopg 3 both
work. A domain decodes as the type it is based on.
"""

from __future__ import annotations

from typing import Any, Dict, List, Sequence, Tuple

from pgpq._pgpq import PostgresSchema

DEFAULT_TIMEZONE = "UTC"

# The type a column decodes as: a domain's base type, or the column's own type.
# Arrays of domains are not resolved, since their element is the domain.
DECODED_TYPE_JOIN = """
JOIN pg_type declared ON declared.oid = {type_oid}
JOIN pg_type t ON t.oid = CASE
    WHEN declared.typtype = 'd' THEN declared.typbasetype
    ELSE declared.oid
END
JOIN pg_namespace type_schema ON type_schema.oid = t.typnamespace
"""

# Every column of every requested table in COPY order, keyed by the name the
# caller passed. A name Postgres cannot resolve fails the cast to regclass.
TABLE_COLUMNS_QUERY = (
    """
SELECT requested.name, a.attname, type_schema.nspname, t.typname, NOT a.attnotnull
FROM unnest(%s::text[]) WITH ORDINALITY AS requested(name, position)
JOIN pg_attribute a ON a.attrelid = requested.name::regclass
"""
    + DECODED_TYPE_JOIN.format(type_oid="a.atttypid")
    + """
WHERE a.attnum > 0 AND NOT a.attisdropped
ORDER BY requested.position, a.attnum
"""
)

# The decoded type of each result column's type OID, in the order given.
RESULT_TYPES_QUERY = (
    """
SELECT type_schema.nspname, t.typname
FROM unnest(%s::oid[]) WITH ORDINALITY AS result(type_oid, position)
"""
    + DECODED_TYPE_JOIN.format(type_oid="result.type_oid")
    + """
ORDER BY result.position
"""
)


def schemas_for_tables(
    cursor: Any, tables: Sequence[str], timezone: str = DEFAULT_TIMEZONE
) -> Dict[str, PostgresSchema]:
    """The schema of each table's COPY output, keyed by the name passed in.

    Names resolve the way Postgres resolves them in a query, so "public.users"
    and "users" both work. Raises ValueError naming a column whose type has no
    decoder.
    """
    cursor.execute(TABLE_COLUMNS_QUERY, (list(tables),))
    rows: Dict[str, List[Tuple[str, str, str, bool]]] = {table: [] for table in tables}
    for table, column, type_schema, typname, nullable in cursor.fetchall():
        rows[table].append((column, type_schema, typname, nullable))
    return {
        table: PostgresSchema.from_catalog(columns, timezone)
        for table, columns in rows.items()
    }


def schema_for_query(
    cursor: Any, query: str, timezone: str = DEFAULT_TIMEZONE
) -> PostgresSchema:
    """The schema of a SELECT's COPY output.

    The query runs with LIMIT 0 to learn its result columns, so it reads no
    rows. A query's columns carry no NOT NULL constraints, so every column is
    nullable.
    """
    cursor.execute(f"SELECT * FROM ({query}) AS pgpq_query LIMIT 0")
    names = [column.name for column in cursor.description]
    type_oids = [column.type_code for column in cursor.description]
    cursor.execute(RESULT_TYPES_QUERY, (type_oids,))
    types = cursor.fetchall()
    columns = [
        (name, type_schema, typname, True)
        for name, (type_schema, typname) in zip(names, types)
    ]
    return PostgresSchema.from_catalog(columns, timezone)
