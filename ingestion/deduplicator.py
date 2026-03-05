"""
NexusAuth Ingestion — Deduplicator & Raw Document Inserter
===========================================================
Handles SHA-256 content deduplication before inserting into `raw_documents`.

Deduplication strategy:
  1. Compute SHA-256 of the raw_content string.
  2. Check if that hash already exists in `raw_documents`.
  3. If duplicate: skip insertion, log, increment counter.
  4. If new: insert into `raw_documents` and return the new row ID.

This module is database-agnostic — it accepts a psycopg2 connection object
so it can be used with both local Docker Postgres and AWS RDS.
"""

import hashlib
import json
import logging
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Hash utility (re-exported for convenience)
# ---------------------------------------------------------------------------

def sha256_content(content: str) -> str:
    """Return SHA-256 hex digest of a UTF-8 string."""
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Deduplication check
# ---------------------------------------------------------------------------

def is_duplicate(conn, content_hash: str) -> bool:
    """
    Return True if a document with this content_hash already exists
    in `raw_documents`.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM raw_documents WHERE content_hash = %s LIMIT 1",
            (content_hash,),
        )
        return cur.fetchone() is not None


def get_existing_id(conn, content_hash: str) -> int | None:
    """
    Return the existing raw_document_id for a given hash, or None.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM raw_documents WHERE content_hash = %s LIMIT 1",
            (content_hash,),
        )
        row = cur.fetchone()
        return row[0] if row else None


# ---------------------------------------------------------------------------
# Raw document insertion
# ---------------------------------------------------------------------------

INSERT_RAW_SQL = """
INSERT INTO raw_documents (
    source_url,
    source_domain,
    document_type_hint,
    title,
    raw_content,
    content_hash,
    metadata,
    scraped_at,
    processing_status
) VALUES (
    %(source_url)s,
    %(source_domain)s,
    %(document_type_hint)s,
    %(title)s,
    %(raw_content)s,
    %(content_hash)s,
    %(metadata)s,
    %(scraped_at)s,
    'pending'
)
ON CONFLICT (content_hash) DO NOTHING
RETURNING id;
"""


def insert_raw_document(conn, doc: dict[str, Any]) -> int | None:
    """
    Insert a raw document into `raw_documents`.

    Returns the new row ID on success, or None if the document was a duplicate
    (ON CONFLICT DO NOTHING).

    Args:
        conn: psycopg2 connection object
        doc: Dict with keys: source_url, source_domain, document_type_hint,
             title, raw_content, content_hash, metadata, scraped_at
    """
    # Ensure content_hash is set
    if not doc.get("content_hash"):
        doc["content_hash"] = sha256_content(doc["raw_content"])

    # Serialize metadata to JSON string if it's a dict
    params = dict(doc)
    if isinstance(params.get("metadata"), dict):
        params["metadata"] = json.dumps(params["metadata"])

    # Ensure scraped_at is set
    if not params.get("scraped_at"):
        params["scraped_at"] = datetime.utcnow()

    with conn.cursor() as cur:
        cur.execute(INSERT_RAW_SQL, params)
        result = cur.fetchone()

    if result:
        new_id = result[0]
        logger.debug(
            "Inserted raw_document id=%d: %s (%s)",
            new_id,
            doc.get("title", "")[:60],
            doc.get("content_hash", "")[:12],
        )
        return new_id
    else:
        logger.debug(
            "Duplicate skipped: %s (%s)",
            doc.get("title", "")[:60],
            doc.get("content_hash", "")[:12],
        )
        return None


# ---------------------------------------------------------------------------
# Batch insertion with deduplication stats
# ---------------------------------------------------------------------------

class DeduplicationStats:
    """Track deduplication statistics across a batch run."""

    def __init__(self):
        self.total_seen = 0
        self.inserted = 0
        self.duplicates = 0
        self.errors = 0
        self.started_at = datetime.utcnow()

    @property
    def elapsed_seconds(self) -> float:
        return (datetime.utcnow() - self.started_at).total_seconds()

    def __repr__(self) -> str:
        return (
            f"DeduplicationStats("
            f"total={self.total_seen}, "
            f"inserted={self.inserted}, "
            f"duplicates={self.duplicates}, "
            f"errors={self.errors}, "
            f"elapsed={self.elapsed_seconds:.1f}s)"
        )


def process_batch(
    conn,
    documents: list[dict[str, Any]],
    batch_size: int = 100,
    commit_every: int = 100,
) -> DeduplicationStats:
    """
    Process a batch of raw documents with deduplication.

    Inserts new documents into `raw_documents`, skips duplicates.
    Commits in batches of `commit_every` to avoid large transactions.

    Args:
        conn: psycopg2 connection
        documents: Iterable of raw document dicts
        batch_size: Log progress every N documents
        commit_every: Commit transaction every N documents

    Returns:
        DeduplicationStats with counts of inserted/duplicate/error docs
    """
    stats = DeduplicationStats()

    for i, doc in enumerate(documents):
        stats.total_seen += 1

        try:
            new_id = insert_raw_document(conn, doc)
            if new_id is not None:
                stats.inserted += 1
            else:
                stats.duplicates += 1

        except Exception as exc:
            stats.errors += 1
            logger.error(
                "Error inserting document '%s': %s",
                doc.get("title", "")[:60],
                exc,
            )
            conn.rollback()
            continue

        # Commit in batches
        if stats.total_seen % commit_every == 0:
            conn.commit()
            logger.info(
                "Progress: %d processed, %d inserted, %d duplicates, %d errors",
                stats.total_seen,
                stats.inserted,
                stats.duplicates,
                stats.errors,
            )

    # Final commit
    conn.commit()
    logger.info("Batch complete: %s", stats)
    return stats


# ---------------------------------------------------------------------------
# Update processing status
# ---------------------------------------------------------------------------

def mark_processing_status(conn, raw_document_id: int, status: str, error_message: str = None) -> None:
    """
    Update the processing_status of a raw_document row.

    Status values: 'pending' | 'processing' | 'tagged' | 'embedded' | 'failed'
    """
    with conn.cursor() as cur:
        if error_message:
            cur.execute(
                """
                UPDATE raw_documents
                SET processing_status = %s,
                    processing_error = %s,
                    processed_at = NOW()
                WHERE id = %s
                """,
                (status, error_message, raw_document_id),
            )
        else:
            cur.execute(
                """
                UPDATE raw_documents
                SET processing_status = %s,
                    processed_at = NOW()
                WHERE id = %s
                """,
                (status, raw_document_id),
            )
    conn.commit()


def get_pending_documents(conn, limit: int = 100) -> list[dict[str, Any]]:
    """
    Fetch raw documents with processing_status = 'pending'.
    Returns list of dicts with all raw_document fields.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                id,
                source_url,
                source_domain,
                document_type_hint,
                title,
                raw_content,
                content_hash,
                metadata,
                scraped_at,
                processing_status
            FROM raw_documents
            WHERE processing_status = 'pending'
            ORDER BY scraped_at ASC
            LIMIT %s
            """,
            (limit,),
        )
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()

    return [dict(zip(columns, row)) for row in rows]
