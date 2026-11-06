"""Decodes real Postgres COPY output and compares it with the same rows read
through psycopg."""

from __future__ import annotations

import io
import json
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterator, List, Tuple

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import pgpq
from pgpq.schema import (
    Bool,
    Bytea,
    Char,
    Column,
    Date,
    Float4,
    Float8,
    Int2,
    Int4,
    Int8,
    Interval,
    Json,
    Jsonb,
    List as PgList,
    Numeric,
    PostgresSchema,
    Text,
    Time,
    Timestamp,
    TimestampTz,
)
from test_pgpq.conftest import Connection

ROW_COUNT = 300

# One row carries a jsonb value much larger than any single COPY chunk, so a row
# spans many writes.
LARGE_ROW_ID = 150

COLUMNS = [
    ("id", "int4 NOT NULL", Column(False, Int4())),
    ("small", "int2", Column(True, Int2())),
    ("big", "int8", Column(True, Int8())),
    ("flag", "bool", Column(True, Bool())),
    ("ratio", "float8", Column(True, Float8())),
    ("ratio4", "float4", Column(True, Float4())),
    ("name", "text", Column(True, Text())),
    ("code", "varchar(20)", Column(True, Char())),
    ("doc", "jsonb", Column(True, Jsonb())),
    ("raw_doc", "json", Column(True, Json())),
    ("amount", "numeric", Column(True, Numeric())),
    ("created", "timestamp", Column(True, Timestamp())),
    ("created_tz", "timestamptz", Column(True, TimestampTz("UTC"))),
    ("day", "date", Column(True, Date())),
    ("at", "time", Column(True, Time())),
    ("wait", "interval", Column(True, Interval())),
    ("blob", "bytea", Column(True, Bytea())),
    ("tags", "text[]", Column(True, PgList(Column(True, Text())))),
    ("scores", "int4[]", Column(True, PgList(Column(True, Int4())))),
]

SCHEMA = PostgresSchema([(name, column) for name, _, column in COLUMNS])

# Every seventh row is null in every nullable column.
INSERT_ROWS = f"""
INSERT INTO decode_source
SELECT
    i,
    CASE WHEN i % 7 = 0 THEN NULL ELSE (i % 30000)::int2 END,
    CASE WHEN i % 7 = 0 THEN NULL ELSE i::int8 * 10000000000 END,
    CASE WHEN i % 7 = 0 THEN NULL ELSE i % 2 = 0 END,
    CASE WHEN i % 7 = 0 THEN NULL ELSE i / 3.0 END,
    CASE WHEN i % 7 = 0 THEN NULL ELSE (i / 4.0)::float4 END,
    CASE WHEN i % 7 = 0 THEN NULL ELSE 'name ' || i || ' ✓' END,
    CASE WHEN i % 7 = 0 THEN NULL ELSE 'c' || i END,
    CASE
        WHEN i % 7 = 0 THEN NULL
        WHEN i = {LARGE_ROW_ID} THEN jsonb_build_object('big', repeat('x', 300000))
        ELSE jsonb_build_object('id', i, 'nested', jsonb_build_array(i, 'a', null))
    END,
    CASE WHEN i % 7 = 0 THEN NULL
        ELSE ('{{"id": ' || i || ',  "spaced": true}}')::json END,
    CASE WHEN i % 7 = 0 THEN NULL ELSE (i * 1.25 - 100)::numeric END,
    CASE WHEN i % 7 = 0 THEN NULL
        ELSE timestamp '2024-01-01 12:00' + i * interval '1 hour' END,
    CASE WHEN i % 7 = 0 THEN NULL
        ELSE timestamptz '2024-01-01 12:00+00' + i * interval '1 minute' END,
    CASE WHEN i % 7 = 0 THEN NULL ELSE date '1999-12-25' + i END,
    CASE WHEN i % 7 = 0 THEN NULL ELSE time '08:00' + i * interval '1 second' END,
    CASE WHEN i % 7 = 0 THEN NULL ELSE i * interval '1 day 3 hours' END,
    CASE WHEN i % 7 = 0 THEN NULL ELSE decode(lpad(to_hex(i), 8, '0'), 'hex') END,
    CASE WHEN i % 7 = 0 THEN NULL ELSE ARRAY['t' || i, NULL, 'u'] END,
    CASE WHEN i % 7 = 0 THEN NULL ELSE ARRAY[i, i + 1] END
FROM generate_series(1, {ROW_COUNT}) AS i
"""

SELECT_COLUMNS = ", ".join(name for name, _, _ in COLUMNS)
COPY_QUERY = (
    f"COPY (SELECT {SELECT_COLUMNS} FROM decode_source ORDER BY id) "
    "TO STDOUT WITH (FORMAT binary)"
)


@pytest.fixture(scope="module")
def source_table(dbconn: Connection) -> Iterator[None]:
    ddl = ", ".join(f"{name} {pg_type}" for name, pg_type, _ in COLUMNS)
    with dbconn.cursor() as cursor:
        cursor.execute(f"CREATE TABLE decode_source ({ddl})")
        cursor.execute(INSERT_ROWS)
    dbconn.commit()
    yield
    with dbconn.cursor() as cursor:
        cursor.execute("DROP TABLE decode_source")
    dbconn.commit()


@pytest.fixture(scope="module")
def expected_rows(dbconn: Connection, source_table: None) -> List[Tuple[Any, ...]]:
    with dbconn.cursor() as cursor:
        cursor.execute(f"SELECT {SELECT_COLUMNS} FROM decode_source ORDER BY id")
        return [normalize_db_row(row) for row in cursor.fetchall()]


@pytest.fixture(scope="module")
def copy_chunks(dbconn: Connection, source_table: None) -> List[memoryview]:
    """The COPY output exactly as psycopg hands it over."""
    with dbconn.cursor() as cursor:
        with cursor.copy(COPY_QUERY) as copy:
            return [chunk for chunk in copy]


@pytest.fixture(scope="module")
def copy_bytes(copy_chunks: List[memoryview]) -> bytes:
    return b"".join(bytes(chunk) for chunk in copy_chunks)


def normalize_db_row(row: Tuple[Any, ...]) -> Tuple[Any, ...]:
    """psycopg parses json, numeric and bytea into Python objects. Arrow keeps
    json and numeric as strings, so both sides are compared as parsed values."""
    values = list(row)
    names = [name for name, _, _ in COLUMNS]
    for column in ("doc", "raw_doc"):
        index = names.index(column)
        if values[index] is not None:
            values[index] = json.dumps(values[index], sort_keys=True)
    blob = names.index("blob")
    if values[blob] is not None:
        values[blob] = bytes(values[blob])
    return tuple(values)


def batch_rows(batches: List[pa.RecordBatch]) -> List[Tuple[Any, ...]]:
    names = [name for name, _, _ in COLUMNS]
    rows: List[Tuple[Any, ...]] = []
    for batch in batches:
        columns = [batch.column(name).to_pylist() for name in names]
        for values in zip(*columns):
            row = list(values)
            for column in ("doc", "raw_doc"):
                index = names.index(column)
                if row[index] is not None:
                    row[index] = json.dumps(json.loads(row[index]), sort_keys=True)
            amount = names.index("amount")
            if row[amount] is not None:
                row[amount] = Decimal(row[amount])
            rows.append(tuple(row))
    return rows


def chunked(data: bytes, size: int) -> Iterator[bytes]:
    for start in range(0, len(data), size):
        yield data[start : start + size]


@pytest.mark.parametrize("batch_size", [1, 7, 64, 100_000])
def test_decoder_matches_select(
    copy_chunks: List[memoryview],
    expected_rows: List[Tuple[Any, ...]],
    batch_size: int,
) -> None:
    decoder = pgpq.PostgresBinaryToArrowReader(SCHEMA, batch_size)

    batches = []
    for chunk in copy_chunks:
        batches.extend(decoder.write(chunk))
    decoder.finish()

    assert [batch.num_rows for batch in batches[:-1]] == [batch_size] * (
        len(batches) - 1
    )
    assert batch_rows(batches) == expected_rows


@pytest.mark.parametrize("piece_size", [1, 3, 4096, 1 << 20])
def test_decoder_is_independent_of_piece_size(
    copy_bytes: bytes, expected_rows: List[Tuple[Any, ...]], piece_size: int
) -> None:
    decoder = pgpq.PostgresBinaryToArrowReader(SCHEMA, 50)

    batches = []
    for piece in chunked(copy_bytes, piece_size):
        batches.extend(decoder.write(piece))
    decoder.finish()

    assert batch_rows(batches) == expected_rows


def test_decoder_accepts_bytearray(
    copy_bytes: bytes, expected_rows: List[Tuple[Any, ...]]
) -> None:
    decoder = pgpq.PostgresBinaryToArrowReader(SCHEMA)

    batches = decoder.write(bytearray(copy_bytes))
    decoder.finish()

    assert batch_rows(batches) == expected_rows


def test_decode_bytes(copy_bytes: bytes, expected_rows: List[Tuple[Any, ...]]) -> None:
    batches = pgpq.decode_bytes(copy_bytes, SCHEMA, batch_size=128)

    assert [batch.num_rows for batch in batches] == [128, 128, 44]
    assert batch_rows(batches) == expected_rows


def test_decode_file(
    tmp_path: Path, copy_bytes: bytes, expected_rows: List[Tuple[Any, ...]]
) -> None:
    path = tmp_path / "data.bin"
    path.write_bytes(copy_bytes)

    batches = list(pgpq.decode_file(path, SCHEMA, batch_size=128))

    assert [batch.num_rows for batch in batches] == [128, 128, 44]
    assert batch_rows(batches) == expected_rows


def test_decode_stream(copy_bytes: bytes, expected_rows: List[Tuple[Any, ...]]) -> None:
    stream = io.BytesIO(copy_bytes)

    batches = list(pgpq.decode_stream(stream, SCHEMA, batch_size=128, read_size=1000))

    assert batch_rows(batches) == expected_rows


def test_arrow_schema_matches_batches(copy_bytes: bytes) -> None:
    decoder = pgpq.PostgresBinaryToArrowReader(SCHEMA, batch_size=ROW_COUNT)

    batches = decoder.write(copy_bytes)

    assert len(batches) == 1
    assert batches[0].schema == decoder.arrow_schema
    assert decoder.arrow_schema.field("created_tz").type == pa.timestamp("us", tz="UTC")


def test_all_null_batches_write_to_one_parquet_file(
    tmp_path: Path, copy_bytes: bytes, expected_rows: List[Tuple[Any, ...]]
) -> None:
    # With one row per batch, every seventh batch is null in every nullable column.
    decoder = pgpq.PostgresBinaryToArrowReader(SCHEMA, batch_size=1)
    batches = decoder.write(copy_bytes)
    decoder.finish()

    path = tmp_path / "decoded.parquet"
    with pq.ParquetWriter(path, decoder.arrow_schema) as writer:
        for batch in batches:
            assert batch.schema == decoder.arrow_schema
            writer.write_batch(batch)

    assert batch_rows(pq.read_table(path).to_batches()) == expected_rows


def test_truncated_stream_raises(copy_bytes: bytes) -> None:
    decoder = pgpq.PostgresBinaryToArrowReader(SCHEMA)
    decoder.write(copy_bytes[:-100])

    with pytest.raises(pgpq.DecodeError, match="EOF in the middle of a tuple"):
        decoder.finish()


def test_truncated_file_raises(tmp_path: Path, copy_bytes: bytes) -> None:
    path = tmp_path / "truncated.bin"
    path.write_bytes(copy_bytes[:-100])

    with pytest.raises(pgpq.DecodeError):
        list(pgpq.decode_file(path, SCHEMA))


def test_schema_with_wrong_column_count_raises(copy_bytes: bytes) -> None:
    schema = PostgresSchema([("id", Column(False, Int4()))])
    decoder = pgpq.PostgresBinaryToArrowReader(schema)

    with pytest.raises(pgpq.DecodeError, match="fields but the schema has 1 columns"):
        decoder.write(copy_bytes)


def test_missing_file_raises_oserror(tmp_path: Path) -> None:
    with pytest.raises(OSError):
        pgpq.decode_file(tmp_path / "missing.bin", SCHEMA)


def test_zero_batch_size_raises() -> None:
    with pytest.raises(pgpq.DecodeError, match="at least one row"):
        pgpq.PostgresBinaryToArrowReader(SCHEMA, 0)
