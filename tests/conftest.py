"""
CodeMed AI — Shared test fixtures and helpers
==============================================
This module is automatically loaded by pytest before any test file.

Shared helpers (available to all test modules):
  - make_cursor_conn()  — psycopg2 connection/cursor mock
  - make_openai_client() — OpenAI embeddings client mock
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest


# ---------------------------------------------------------------------------
# Sentinels
# ---------------------------------------------------------------------------

_UNSET = object()   # distinguishes "not provided" from "explicitly None"


# ---------------------------------------------------------------------------
# DB mock helpers (exposed as pytest fixtures AND plain functions)
# ---------------------------------------------------------------------------

def make_cursor_conn(fetchone=_UNSET, fetchall=None, description=None):
    """Return (mock_conn, mock_cursor) wired as a psycopg2 context manager.

    Args:
        fetchone:    Return value for cursor.fetchone(). Pass nothing to leave
                     unset (MagicMock default).
        fetchall:    Return value for cursor.fetchall(). Pass a list-of-lists
                     to simulate multiple sequential calls (side_effect).
        description: Simulated cursor.description (list of column descriptors).
    """
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    if fetchone is not _UNSET:
        mock_cursor.fetchone.return_value = fetchone
    if fetchall is not None:
        if isinstance(fetchall, list) and fetchall and isinstance(fetchall[0], list):
            mock_cursor.fetchall.side_effect = fetchall
        else:
            mock_cursor.fetchall.return_value = fetchall
    if description is not None:
        mock_cursor.description = description
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor


def make_openai_client(vectors=None, n=1):
    """Return a mock OpenAI client whose .embeddings.create() returns *n* vectors.

    Args:
        vectors: Explicit list of embedding vectors. Defaults to n × [0.1]*1536.
        n:       Number of default vectors to generate when *vectors* is None.
    """
    if vectors is None:
        vectors = [[0.1] * 1536] * n
    client = MagicMock()
    client.embeddings.create.return_value.data = [
        MagicMock(embedding=v) for v in vectors
    ]
    return client


# ---------------------------------------------------------------------------
# Pytest fixtures (thin wrappers so tests can request them via DI)
# ---------------------------------------------------------------------------

@pytest.fixture
def cursor_conn_factory():
    """Factory fixture — call with the same kwargs as make_cursor_conn()."""
    return make_cursor_conn


@pytest.fixture
def openai_client_factory():
    """Factory fixture — call with the same kwargs as make_openai_client()."""
    return make_openai_client
