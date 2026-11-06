from __future__ import annotations

import os
from typing import BinaryIO, Iterator, List, Union

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

__all__ = [
    "ArrowToPostgresBinaryEncoder",
    "DEFAULT_BATCH_SIZE",
    "DecodeError",
    "EncodeError",
    "PostgresBinaryFileDecoder",
    "PostgresBinaryToArrowReader",
    "decode_bytes",
    "decode_file",
    "decode_stream",
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
