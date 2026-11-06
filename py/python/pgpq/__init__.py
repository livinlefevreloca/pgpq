from __future__ import annotations

import os
from typing import Any, BinaryIO, Callable, Iterator, List, Optional, Union

import pyarrow

from pgpq._pgpq import (
    DEFAULT_BATCH_SIZE,
    ArrowToPostgresBinaryEncoder,
    DecodeError,
    EncodeError,
    PostgresBinaryFileDecoder,
    PostgresBinaryToArrowReader,
    PostgresSchema,
)
from pgpq.catalog import schema_for_query, schemas_for_tables

__all__ = [
    "ArrowCopyWriter",
    "ArrowToPostgresBinaryEncoder",
    "DEFAULT_BATCH_SIZE",
    "DecodeError",
    "EncodeError",
    "PostgresBinaryFileDecoder",
    "PostgresBinaryToArrowReader",
    "decode_bytes",
    "decode_file",
    "decode_stream",
    "schema_for_query",
    "schemas_for_tables",
]

# Bytes read from a stream per call to its read method.
STREAM_READ_SIZE = 1024 * 1024


def decode_bytes(
    data: Union[bytes, bytearray, memoryview],
    schema: PostgresSchema,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> List[pyarrow.RecordBatch]:
    """Decodes a complete binary COPY stream held in memory."""
    decoder = PostgresBinaryToArrowReader(schema, batch_size)
    batches = decoder.write(data)
    decoder.finish()
    return batches


def decode_file(
    path: Union[str, "os.PathLike[str]"],
    schema: PostgresSchema,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> PostgresBinaryFileDecoder:
    """Decodes a binary COPY file, yielding each batch as it fills.

    The file is opened and read in Rust, with the GIL released.
    """
    return PostgresBinaryFileDecoder(path, schema, batch_size)


def decode_stream(
    stream: BinaryIO,
    schema: PostgresSchema,
    batch_size: int = DEFAULT_BATCH_SIZE,
    read_size: int = STREAM_READ_SIZE,
) -> Iterator[pyarrow.RecordBatch]:
    """Decodes a binary COPY stream read from a file-like object, yielding each
    batch as it fills."""
    decoder = PostgresBinaryToArrowReader(schema, batch_size)
    while True:
        data = stream.read(read_size)
        if not data:
            break
        yield from decoder.write(data)
    decoder.finish()


class ArrowCopyWriter:
    """A writable file object that decodes binary COPY output as it arrives.

    Pass it as the file to psycopg2's copy_expert. Each batch goes to on_batch
    as soon as it fills, so memory holds about one batch. Closing the writer
    checks that the stream ended with its trailer and raises DecodeError if it
    did not, so a cut-off COPY is never taken for a complete one.

        with ArrowCopyWriter(schema, write_batch) as writer:
            cursor.copy_expert("COPY users TO STDOUT WITH (FORMAT binary)", writer)
    """

    def __init__(
        self,
        schema: PostgresSchema,
        on_batch: Callable[[pyarrow.RecordBatch], None],
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        self._decoder = PostgresBinaryToArrowReader(schema, batch_size)
        self._on_batch = on_batch
        self.closed = False

    @property
    def arrow_schema(self) -> pyarrow.Schema:
        return self._decoder.arrow_schema

    def write(self, data: Union[bytes, bytearray, memoryview]) -> int:
        for batch in self._decoder.write(data):
            self._on_batch(batch)
        return len(data)

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        self._decoder.finish()

    def __enter__(self) -> "ArrowCopyWriter":
        return self

    def __exit__(self, exc_type: Optional[type], exc: Any, traceback: Any) -> None:
        # An exception from the COPY already explains the failure, and a
        # missing trailer would only hide it.
        if exc_type is None:
            self.close()
