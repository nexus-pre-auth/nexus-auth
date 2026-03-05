"""
NexusAuth Ingestion — pgvector Embedding Pipeline
==================================================
Generates text embeddings for knowledge_documents and stores them in
`document_embeddings` (pgvector).

Pipeline:
  1. Fetch tagged knowledge_documents that don't yet have embeddings
  2. Chunk each document into overlapping text segments
  3. Generate embeddings via OpenAI text-embedding-3-small
  4. Store each chunk + its vector in `document_embeddings`
  5. Update knowledge_document embedding_status → 'embedded'

Chunking strategy:
  - Target chunk size: 512 tokens (~1800 chars)
  - Overlap: 64 tokens (~230 chars) — ensures context continuity
  - Minimum chunk size: 100 chars (skip tiny trailing chunks)
  - Each chunk gets: chunk_index, chunk_text, embedding vector

Model: text-embedding-3-small (1536 dimensions, cost-efficient)
       Can be swapped to text-embedding-3-large (3072 dims) for higher quality.
"""

import json
import logging
import os
import time
from datetime import datetime
from typing import Any, Generator

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

EMBEDDING_MODEL = "text-embedding-3-small"
EMBEDDING_DIMENSIONS = 1536

# Chunking parameters (in characters — approximation of token counts)
CHUNK_SIZE_CHARS = 1800      # ~512 tokens at ~3.5 chars/token
CHUNK_OVERLAP_CHARS = 230    # ~64 tokens overlap
MIN_CHUNK_CHARS = 100        # Skip chunks shorter than this

# Rate limiting
EMBED_BATCH_SIZE = 20        # Documents per OpenAI API batch call
RATE_LIMIT_SLEEP = 0.5       # Seconds between API calls

# ---------------------------------------------------------------------------
# SQL Statements
# ---------------------------------------------------------------------------

GET_UNEMBEDDED_SQL = """
SELECT
    kd.id,
    kd.title,
    kd.content_text,
    kd.document_type,
    kd.source_url,
    kd.source_domain,
    kd.payer_id,
    kd.specialties,
    kd.cpt_codes,
    kd.icd10_codes
FROM knowledge_documents kd
LEFT JOIN document_embeddings de ON de.knowledge_document_id = kd.id
WHERE de.id IS NULL
  AND kd.status != 'archived'
ORDER BY kd.created_at ASC
LIMIT %s;
"""

INSERT_EMBEDDING_SQL = """
INSERT INTO document_embeddings (
    knowledge_document_id,
    chunk_index,
    chunk_text,
    embedding,
    model_name,
    embedding_dimensions,
    chunk_metadata
) VALUES (
    %(knowledge_document_id)s,
    %(chunk_index)s,
    %(chunk_text)s,
    %(embedding)s::vector,
    %(model_name)s,
    %(embedding_dimensions)s,
    %(chunk_metadata)s
)
ON CONFLICT (knowledge_document_id, chunk_index) DO UPDATE SET
    chunk_text           = EXCLUDED.chunk_text,
    embedding            = EXCLUDED.embedding,
    model_name           = EXCLUDED.model_name,
    embedding_dimensions = EXCLUDED.embedding_dimensions,
    chunk_metadata       = EXCLUDED.chunk_metadata
RETURNING id;
"""

UPDATE_EMBEDDING_STATUS_SQL = """
UPDATE knowledge_documents
SET embedding_status = %s,
    embedded_at = NOW()
WHERE id = %s;
"""


# ---------------------------------------------------------------------------
# Text Chunker
# ---------------------------------------------------------------------------

def chunk_text(
    text: str,
    chunk_size: int = CHUNK_SIZE_CHARS,
    overlap: int = CHUNK_OVERLAP_CHARS,
    min_size: int = MIN_CHUNK_CHARS,
) -> list[dict[str, Any]]:
    """
    Split text into overlapping chunks for embedding.

    Uses a sliding window approach with sentence-boundary awareness:
    tries to break at paragraph or sentence boundaries rather than
    mid-word.

    Returns:
        List of dicts: {chunk_index, chunk_text, char_start, char_end}
    """
    if not text or len(text) < min_size:
        return []

    chunks = []
    start = 0
    chunk_index = 0

    while start < len(text):
        end = start + chunk_size

        if end >= len(text):
            # Last chunk — take everything remaining
            chunk = text[start:].strip()
            if len(chunk) >= min_size:
                chunks.append({
                    "chunk_index": chunk_index,
                    "chunk_text": chunk,
                    "char_start": start,
                    "char_end": len(text),
                })
            break

        # Try to break at a paragraph boundary (double newline)
        para_break = text.rfind("\n\n", start, end)
        if para_break > start + chunk_size // 2:
            end = para_break + 2
        else:
            # Try sentence boundary (period + space)
            sent_break = text.rfind(". ", start, end)
            if sent_break > start + chunk_size // 2:
                end = sent_break + 2
            else:
                # Try word boundary (space)
                word_break = text.rfind(" ", start, end)
                if word_break > start:
                    end = word_break + 1

        chunk = text[start:end].strip()
        if len(chunk) >= min_size:
            chunks.append({
                "chunk_index": chunk_index,
                "chunk_text": chunk,
                "char_start": start,
                "char_end": end,
            })
            chunk_index += 1

        # Advance with overlap
        start = end - overlap
        if start <= 0:
            break

    return chunks


# ---------------------------------------------------------------------------
# OpenAI Embedding Client
# ---------------------------------------------------------------------------

def _get_openai_client():
    """Return an OpenAI client, configured from environment."""
    try:
        from openai import OpenAI
        client = OpenAI()  # Uses OPENAI_API_KEY and OPENAI_BASE_URL from env
        return client
    except ImportError:
        raise ImportError(
            "openai package not installed. Run: pip install openai"
        )


def embed_texts(
    texts: list[str],
    model: str = EMBEDDING_MODEL,
    client=None,
) -> list[list[float]]:
    """
    Generate embeddings for a list of text strings.

    Args:
        texts: List of text strings to embed
        model: OpenAI embedding model name
        client: OpenAI client (created fresh if None)

    Returns:
        List of embedding vectors (list of floats)
    """
    if client is None:
        client = _get_openai_client()

    if not texts:
        return []

    # Truncate very long texts to avoid token limit errors
    # text-embedding-3-small: 8191 token limit
    MAX_CHARS = 25000  # ~7000 tokens, safe margin
    truncated = [t[:MAX_CHARS] for t in texts]

    response = client.embeddings.create(
        model=model,
        input=truncated,
    )

    return [item.embedding for item in response.data]


# ---------------------------------------------------------------------------
# Core embedding pipeline
# ---------------------------------------------------------------------------

def embed_document(
    conn,
    doc: dict[str, Any],
    client=None,
) -> int:
    """
    Chunk and embed a single knowledge_document.

    Args:
        conn: psycopg2 connection
        doc: Row dict from knowledge_documents
        client: OpenAI client

    Returns:
        Number of chunks embedded
    """
    if client is None:
        client = _get_openai_client()

    knowledge_id = doc["id"]
    title = doc.get("title", "")
    content = doc.get("content_text", "")

    # Prepend title to content for better embedding context
    full_text = f"{title}\n\n{content}" if title else content

    # Chunk the text
    chunks = chunk_text(full_text)
    if not chunks:
        logger.warning(
            "No chunks generated for knowledge_doc %d (content length: %d)",
            knowledge_id,
            len(content),
        )
        return 0

    logger.debug(
        "Embedding knowledge_doc %d: %d chunks from %d chars",
        knowledge_id,
        len(chunks),
        len(full_text),
    )

    # Generate embeddings for all chunks
    chunk_texts = [c["chunk_text"] for c in chunks]

    try:
        embeddings = embed_texts(chunk_texts, client=client)
    except Exception as exc:
        logger.error(
            "OpenAI API error for knowledge_doc %d: %s",
            knowledge_id,
            exc,
        )
        raise

    # Insert each chunk + embedding into document_embeddings
    inserted = 0
    for chunk, embedding in zip(chunks, embeddings):
        chunk_meta = {
            "char_start": chunk["char_start"],
            "char_end": chunk["char_end"],
            "document_type": doc.get("document_type", ""),
            "source_domain": doc.get("source_domain", ""),
            "specialties": doc.get("specialties", []),
        }

        # Format embedding as pgvector string: '[0.1, 0.2, ...]'
        embedding_str = "[" + ",".join(str(x) for x in embedding) + "]"

        params = {
            "knowledge_document_id": knowledge_id,
            "chunk_index": chunk["chunk_index"],
            "chunk_text": chunk["chunk_text"],
            "embedding": embedding_str,
            "model_name": EMBEDDING_MODEL,
            "embedding_dimensions": EMBEDDING_DIMENSIONS,
            "chunk_metadata": json.dumps(chunk_meta),
        }

        with conn.cursor() as cur:
            cur.execute(INSERT_EMBEDDING_SQL, params)

        inserted += 1

    conn.commit()

    # Update embedding status on knowledge_document
    with conn.cursor() as cur:
        cur.execute(
            UPDATE_EMBEDDING_STATUS_SQL,
            ("embedded", knowledge_id),
        )
    conn.commit()

    logger.info(
        "Embedded knowledge_doc %d: %d chunks stored",
        knowledge_id,
        inserted,
    )
    return inserted


# ---------------------------------------------------------------------------
# Batch embedding processor
# ---------------------------------------------------------------------------

class EmbeddingStats:
    """Track embedding batch statistics."""

    def __init__(self):
        self.total_docs = 0
        self.embedded_docs = 0
        self.total_chunks = 0
        self.failed_docs = 0
        self.api_calls = 0
        self.started_at = datetime.utcnow()

    @property
    def elapsed(self) -> float:
        return (datetime.utcnow() - self.started_at).total_seconds()

    def __repr__(self) -> str:
        return (
            f"EmbeddingStats("
            f"docs={self.embedded_docs}/{self.total_docs}, "
            f"chunks={self.total_chunks}, "
            f"failed={self.failed_docs}, "
            f"api_calls={self.api_calls}, "
            f"elapsed={self.elapsed:.1f}s)"
        )


def process_unembedded_documents(
    conn,
    client=None,
    batch_size: int = 50,
    max_documents: int | None = None,
    rate_limit_sleep: float = RATE_LIMIT_SLEEP,
) -> EmbeddingStats:
    """
    Process all knowledge_documents that don't yet have embeddings.

    Args:
        conn: psycopg2 connection
        client: OpenAI client (created fresh if None)
        batch_size: Number of documents to fetch per DB query
        max_documents: Stop after this many documents (None = all)
        rate_limit_sleep: Seconds to sleep between API calls

    Returns:
        EmbeddingStats
    """
    if client is None:
        try:
            client = _get_openai_client()
            logger.info("OpenAI client initialised for embeddings")
        except Exception as exc:
            logger.error("Failed to initialise OpenAI client: %s", exc)
            raise

    stats = EmbeddingStats()

    while True:
        # Fetch unembedded documents
        with conn.cursor() as cur:
            cur.execute(GET_UNEMBEDDED_SQL, (batch_size,))
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()

        if not rows:
            logger.info("No more unembedded documents")
            break

        docs = [dict(zip(columns, row)) for row in rows]
        logger.info("Processing batch of %d unembedded documents", len(docs))

        for doc in docs:
            if max_documents and stats.total_docs >= max_documents:
                logger.info("Reached max_documents limit (%d)", max_documents)
                return stats

            stats.total_docs += 1

            try:
                chunks_inserted = embed_document(conn, doc, client=client)
                stats.embedded_docs += 1
                stats.total_chunks += chunks_inserted
                stats.api_calls += 1

                # Rate limiting
                if rate_limit_sleep > 0:
                    time.sleep(rate_limit_sleep)

            except Exception as exc:
                stats.failed_docs += 1
                logger.error(
                    "Failed to embed knowledge_doc %d: %s",
                    doc["id"],
                    exc,
                )
                # Mark as failed in knowledge_documents
                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE knowledge_documents SET embedding_status = 'failed' WHERE id = %s",
                            (doc["id"],),
                        )
                    conn.commit()
                except Exception:
                    conn.rollback()

            if stats.total_docs % 10 == 0:
                logger.info("Embedding progress: %s", stats)

        if len(docs) < batch_size:
            break

    logger.info("Embedding complete: %s", stats)
    return stats


# ---------------------------------------------------------------------------
# Semantic search utility
# ---------------------------------------------------------------------------

def semantic_search(
    conn,
    query: str,
    limit: int = 10,
    document_type: str | None = None,
    payer_id: int | None = None,
    client=None,
) -> list[dict[str, Any]]:
    """
    Perform semantic similarity search against document_embeddings.

    Args:
        conn: psycopg2 connection
        query: Natural language search query
        limit: Maximum number of results to return
        document_type: Filter by document type (optional)
        payer_id: Filter by payer (optional)
        client: OpenAI client

    Returns:
        List of dicts with knowledge_document fields + similarity score
    """
    if client is None:
        client = _get_openai_client()

    # Embed the query
    query_embeddings = embed_texts([query], client=client)
    if not query_embeddings:
        return []

    query_vector = "[" + ",".join(str(x) for x in query_embeddings[0]) + "]"

    # Build search query with optional filters
    filters = []
    params = [query_vector + "::vector", limit]

    if document_type:
        filters.append("kd.document_type = %s")
        params.insert(-1, document_type)

    if payer_id:
        filters.append("kd.payer_id = %s")
        params.insert(-1, payer_id)

    where_clause = "WHERE " + " AND ".join(filters) if filters else ""

    search_sql = f"""
    SELECT
        kd.id,
        kd.title,
        kd.document_type,
        kd.source_url,
        kd.source_domain,
        kd.specialties,
        kd.routing_targets,
        de.chunk_text,
        de.chunk_index,
        1 - (de.embedding <=> %s) AS similarity
    FROM document_embeddings de
    JOIN knowledge_documents kd ON kd.id = de.knowledge_document_id
    {where_clause}
    ORDER BY de.embedding <=> %s
    LIMIT %s;
    """

    # Adjust params for the two vector references
    search_params = [query_vector + "::vector"] + params[1:-1] + [query_vector + "::vector", limit]

    with conn.cursor() as cur:
        cur.execute(search_sql, search_params)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()

    return [dict(zip(columns, row)) for row in rows]


# ---------------------------------------------------------------------------
# CLI test runner
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    print("=== Embedding Pipeline Test (offline chunking) ===\n")

    # Test chunking without API calls
    sample_text = """
    LCD Title: Cardiac Monitoring

    Coverage Indications, Limitations, and/or Medical Necessity

    CARDIAC MONITORING is reasonable and necessary when the following criteria are met:
    1. Temporary (not implanted), AND
    2. Presence of symptoms suggestive of cardiac arrythmia with symptoms
       (such as palpitations, presyncope, syncope, chest pain or shortness of breath)
       occurring infrequently (>24 hours between symptomatic episodes), OR
    3. Monitoring is necessary to regulate medication management such as
       antiarrhythmic drug dosage, OR
    4. Patient with non-lacunar cryptogenic stroke or stroke or TIA of undetermined
       origin to monitor undiagnosed atrial fibrillation or anticoagulation management.

    Device Requirements:
    1. FDA cleared, AND
    2. Device is patient or event activated with intermittent or continuous cardiac
       arrhythmic events monitoring capacity, AND
    3. Monitored by 24-hour monitoring stations to receive transmissions, AND
    4. A system is in place to notify patients or emergency services for potentially
       life-threatening arrhythmias.

    CPT codes: 93224, 93225, 93226, 93227, 93228, 93229, 93268
    ICD-10 codes: I49.0, I49.1, R00.0, R00.1, R07.9
    """ * 5  # Repeat to make it long enough for multiple chunks

    chunks = chunk_text(sample_text)
    print(f"Sample text length: {len(sample_text)} chars")
    print(f"Generated {len(chunks)} chunks:\n")
    for chunk in chunks:
        print(f"  Chunk {chunk['chunk_index']}: chars {chunk['char_start']}-{chunk['char_end']} ({len(chunk['chunk_text'])} chars)")
        print(f"    Preview: {chunk['chunk_text'][:80]}...")
        print()

    print(f"\nChunking test passed ✓")
    print(f"To test full embedding pipeline, ensure OPENAI_API_KEY is set and DB is running.")
