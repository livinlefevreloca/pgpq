"""Schemas built from the catalog, and ArrowCopyWriter, against a real
Postgres with both psycopg 3 and psycopg2."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterator, List

import pyarrow as pa
import pytest

import pgpq
from pgpq.schema import (
    Column,
    Int4,
    Int8,
    Jsonb,
    List as PgList,
    Numeric,
    PostgresSchema,
    Text,
    TimestampTz,
    Varchar,
)
from test_pgpq.conftest import Connection

TABLE_DDL = """
CREATE SCHEMA catalog_test;
CREATE DOMAIN catalog_test.positive AS int4 CHECK (VALUE > 0);
CREATE TABLE catalog_test.accounts (
    id int8 NOT NULL,
    dropped text,
    code varchar(10),
    balance numeric NOT NULL,
    score catalog_test.positive,
    doc jsonb,
    tags text[],
    opened timestamptz,
    doubled int8 GENERATED ALWAYS AS (id * 2) STORED
);
ALTER TABLE catalog_test.accounts DROP COLUMN dropped;
INSERT INTO catalog_test.accounts VALUES
    (1, 'a', 1.50, 3, '{"k": 1}', ARRAY['x', NULL], '2024-01-01 00:00+00'),
    (2, NULL, -2, NULL, NULL, NULL, NULL);
CREATE TABLE catalog_test.addresses (id int4 NOT NULL, ip inet);
"""

ACCOUNTS_SCHEMA = PostgresSchema(
    [
        ("id", Column(False, Int8())),
        ("code", Column(True, Varchar())),
        ("balance", Column(False, Numeric())),
        ("score", Column(True, Int4())),
        ("doc", Column(True, Jsonb())),
        ("tags", Column(True, PgList(Column(True, Text())))),
        ("opened", Column(True, TimestampTz("UTC"))),
    ]
)

ACCOUNTS_ROWS = [
    {
        "id": 1,
        "code": "a",
        "balance": "1.50",
        "score": 3,
        "doc": '{"k": 1}',
        "tags": ["x", None],
        "opened": datetime(2024, 1, 1, tzinfo=timezone.utc),
    },
    {
        "id": 2,
        "code": None,
        "balance": "-2",
        "score": None,
        "doc": None,
        "tags": None,
        "opened": None,
    },
]


@pytest.fixture(scope="module")
def tables(dbconn: Connection) -> Iterator[None]:
    with dbconn.cursor() as cursor:
        cursor.execute(TABLE_DDL)
    dbconn.commit()
    yield
    with dbconn.cursor() as cursor:
        cursor.execute("DROP SCHEMA catalog_test CASCADE")
    dbconn.commit()


@pytest.fixture(params=["psycopg", "psycopg2"])
def cursor(request: Any, dbconn: Connection, psycopg2_conn: Any, tables: None) -> Any:
    conn = dbconn if request.param == "psycopg" else psycopg2_conn
    with conn.cursor() as cursor:
        yield cursor
    conn.rollback()


def copy_out(dbconn: Connection, source: str) -> bytes:
    """Binary COPY output of source, a table name or a parenthesized query."""
    with dbconn.cursor() as cursor:
        with cursor.copy(f"COPY {source} TO STDOUT WITH (FORMAT binary)") as copy:
            return b"".join(bytes(chunk) for chunk in copy)


def test_schemas_for_tables(cursor: Any, dbconn: Connection) -> None:
    schemas = pgpq.schemas_for_tables(cursor, ["catalog_test.accounts"])

    assert list(schemas) == ["catalog_test.accounts"]
    assert schemas["catalog_test.accounts"] == ACCOUNTS_SCHEMA

    # A table's COPY output leaves out the generated column, and so does its schema.
    data = copy_out(dbconn, "catalog_test.accounts")
    batches = pgpq.decode_bytes(data, schemas["catalog_test.accounts"])
    rows = pa.Table.from_batches(batches).to_pylist()
    assert rows == ACCOUNTS_ROWS


def test_schemas_for_tables_uses_the_search_path(cursor: Any) -> None:
    cursor.execute("SET LOCAL search_path TO catalog_test")

    schemas = pgpq.schemas_for_tables(cursor, ["accounts"])

    assert schemas["accounts"] == ACCOUNTS_SCHEMA


def test_type_without_a_decoder_names_the_column(cursor: Any) -> None:
    with pytest.raises(ValueError, match="inet.*ip"):
        pgpq.schemas_for_tables(cursor, ["catalog_test.addresses"])


def test_unknown_table_raises(cursor: Any) -> None:
    with pytest.raises(Exception, match="no_such_table"):
        pgpq.schemas_for_tables(cursor, ["catalog_test.no_such_table"])


def test_schema_for_query(cursor: Any, dbconn: Connection) -> None:
    query = (
        "SELECT id, balance * 2 AS doubled, score, count(*) OVER () AS total, "
        "'100%' AS label FROM catalog_test.accounts ORDER BY id"
    )

    schema = pgpq.schema_for_query(cursor, query)

    assert schema == PostgresSchema(
        [
            ("id", Column(True, Int8())),
            ("doubled", Column(True, Numeric())),
            ("score", Column(True, Int4())),
            ("total", Column(True, Int8())),
            ("label", Column(True, Text())),
        ]
    )
    batches = pgpq.decode_bytes(copy_out(dbconn, f"({query})"), schema)
    assert pa.Table.from_batches(batches).to_pylist() == [
        {"id": 1, "doubled": "3.00", "score": 3, "total": 2, "label": "100%"},
        {"id": 2, "doubled": "-4", "score": None, "total": 2, "label": "100%"},
    ]


def test_schema_json_round_trip() -> None:
    json = ACCOUNTS_SCHEMA.to_json()

    assert PostgresSchema.from_json(json) == ACCOUNTS_SCHEMA
    assert '"type":"_text"' in json


def test_invalid_schema_json_raises() -> None:
    with pytest.raises(ValueError, match="nullable"):
        PostgresSchema.from_json('[{"name": "id", "type": "int4"}]')


def test_arrow_copy_writer_with_copy_expert(psycopg2_conn: Any, tables: None) -> None:
    batches: List[pa.RecordBatch] = []
    with psycopg2_conn.cursor() as cursor:
        schema = pgpq.schemas_for_tables(cursor, ["catalog_test.accounts"])
        with pgpq.ArrowCopyWriter(
            schema["catalog_test.accounts"], batches.append, batch_size=1
        ) as writer:
            cursor.copy_expert(
                "COPY catalog_test.accounts TO STDOUT WITH (FORMAT binary)",
                writer,
            )
    psycopg2_conn.rollback()

    assert len(batches) == 2
    assert all(batch.schema == writer.arrow_schema for batch in batches)
    rows = pa.Table.from_batches(batches).to_pylist()
    assert rows == ACCOUNTS_ROWS


def test_arrow_copy_writer_rejects_a_truncated_stream(
    dbconn: Connection, tables: None
) -> None:
    data = copy_out(dbconn, "catalog_test.accounts")
    writer = pgpq.ArrowCopyWriter(ACCOUNTS_SCHEMA, lambda batch: None)
    writer.write(data[:-2])

    with pytest.raises(pgpq.DecodeError):
        writer.close()
