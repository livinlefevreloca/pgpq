from __future__ import annotations

from typing import Any, Iterator, Tuple

import psycopg
import pytest
from testing.postgresql import Postgresql

Connection = psycopg.Connection[Tuple[Any, ...]]


@pytest.fixture(scope="session")
def postgres():
    return Postgresql()


@pytest.fixture(scope="session")
def dbconn(postgres: Postgresql) -> Iterator[Connection]:
    print(postgres.url())
    with psycopg.connect(str(postgres.url())) as conn:
        yield conn


@pytest.fixture(scope="session")
def psycopg2_conn(postgres: Postgresql) -> Iterator[Any]:
    """A psycopg2 connection, the driver albert-main's dump uses."""
    import psycopg2

    conn = psycopg2.connect(**postgres.dsn())
    yield conn
    conn.close()
