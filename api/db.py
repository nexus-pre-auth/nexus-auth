"""
NexusAuth API — Database Connection Pool
=========================================
Provides a psycopg2 connection pool shared across all API requests.
Each request checks out a connection and returns it on completion.
"""

import os
import logging
from contextlib import contextmanager

import psycopg2
from psycopg2 import pool

logger = logging.getLogger(__name__)

DEFAULT_DATABASE_URL = "postgresql://nexusauth:nexusauth@localhost:5432/nexusauth"

_pool: pool.ThreadedConnectionPool | None = None


def init_pool(min_conn: int = 2, max_conn: int = 10) -> None:
    """Initialise the connection pool. Called once at app startup."""
    global _pool
    url = os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)
    _pool = pool.ThreadedConnectionPool(min_conn, max_conn, url)
    logger.info("DB pool initialised (min=%d, max=%d)", min_conn, max_conn)


def close_pool() -> None:
    """Close all connections. Called at app shutdown."""
    global _pool
    if _pool:
        _pool.closeall()
        _pool = None
        logger.info("DB pool closed")


@contextmanager
def get_conn():
    """
    Context manager that yields a psycopg2 connection from the pool.
    Commits on clean exit, rolls back on exception, always returns to pool.
    """
    if _pool is None:
        raise RuntimeError("DB pool not initialised — call init_pool() first")
    conn = _pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        _pool.putconn(conn)
