"""
NexusAuth Ingestion — Tagger Integration Layer
===============================================
Bridges the raw_documents table and the knowledge_documents table.

Pipeline:
  1. Fetch pending raw_documents (processing_status = 'pending')
  2. Run DocumentTagger on each document
  3. Insert tagged result into knowledge_documents
  4. Update raw_document processing_status → 'tagged'
  5. On error → mark as 'failed' with error message

This module is designed to be idempotent — re-running it will skip
already-tagged documents and only process new ones.
"""

import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

# Add project root to path so tagging module is importable
sys.path.insert(0, str(Path(__file__).parent.parent))

from tagging.tagger import DocumentTagger, TaggingResult
from ingestion.deduplicator import (
    get_pending_documents,
    mark_processing_status,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SQL Statements
# ---------------------------------------------------------------------------

INSERT_KNOWLEDGE_SQL = """
INSERT INTO knowledge_documents (
    raw_document_id,
    payer_id,
    document_type,
    document_subtype,
    title,
    content_text,
    source_url,
    source_domain,
    effective_date,
    last_updated,
    specialties,
    cpt_codes,
    icd10_codes,
    hcpcs_codes,
    routing_targets,
    confidence_score,
    requires_review,
    tagger_metadata,
    status
) VALUES (
    %(raw_document_id)s,
    %(payer_id)s,
    %(document_type)s,
    %(document_subtype)s,
    %(title)s,
    %(content_text)s,
    %(source_url)s,
    %(source_domain)s,
    %(effective_date)s,
    %(last_updated)s,
    %(specialties)s,
    %(cpt_codes)s,
    %(icd10_codes)s,
    %(hcpcs_codes)s,
    %(routing_targets)s,
    %(confidence_score)s,
    %(requires_review)s,
    %(tagger_metadata)s,
    %(status)s
)
ON CONFLICT (raw_document_id) DO UPDATE SET
    document_type    = EXCLUDED.document_type,
    document_subtype = EXCLUDED.document_subtype,
    specialties      = EXCLUDED.specialties,
    cpt_codes        = EXCLUDED.cpt_codes,
    icd10_codes      = EXCLUDED.icd10_codes,
    hcpcs_codes      = EXCLUDED.hcpcs_codes,
    routing_targets  = EXCLUDED.routing_targets,
    confidence_score = EXCLUDED.confidence_score,
    requires_review  = EXCLUDED.requires_review,
    tagger_metadata  = EXCLUDED.tagger_metadata,
    last_updated     = NOW()
RETURNING id;
"""

LOOKUP_PAYER_SQL = """
SELECT id FROM payers WHERE payer_code = %s LIMIT 1;
"""


# ---------------------------------------------------------------------------
# Payer ID lookup
# ---------------------------------------------------------------------------

def _lookup_payer_id(conn, payer_code: str | None) -> int | None:
    """Return the payers.id for a given payer_code, or None."""
    if not payer_code:
        return None
    with conn.cursor() as cur:
        cur.execute(LOOKUP_PAYER_SQL, (payer_code,))
        row = cur.fetchone()
        return row[0] if row else None


# ---------------------------------------------------------------------------
# Date extraction from metadata
# ---------------------------------------------------------------------------

def _extract_effective_date(metadata: dict) -> datetime | None:
    """Try to extract an effective date from raw document metadata."""
    if not metadata:
        return None

    date_fields = [
        "rev_eff_date",
        "orig_det_eff_date",
        "ncd_eff_date",
        "NCD_efctv_dt",
        "mcd_publish_date",
    ]

    for field in date_fields:
        val = metadata.get(field, "")
        if val:
            for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                try:
                    return datetime.strptime(str(val).strip()[:26], fmt)
                except ValueError:
                    continue
    return None


def _extract_last_updated(metadata: dict) -> datetime | None:
    """Try to extract a last_updated date from raw document metadata."""
    if not metadata:
        return None

    date_fields = ["last_updated", "last_updt_tmstmp", "last_reviewed_on"]

    for field in date_fields:
        val = metadata.get(field, "")
        if val:
            for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                try:
                    return datetime.strptime(str(val).strip()[:26], fmt)
                except ValueError:
                    continue
    return None


# ---------------------------------------------------------------------------
# Core tagging + insertion function
# ---------------------------------------------------------------------------

def tag_and_insert(
    conn,
    raw_doc: dict[str, Any],
    tagger: DocumentTagger,
) -> int | None:
    """
    Tag a single raw document and insert into knowledge_documents.

    Args:
        conn: psycopg2 connection
        raw_doc: Row dict from raw_documents table
        tagger: Initialised DocumentTagger instance

    Returns:
        knowledge_document id on success, None on failure
    """
    raw_id = raw_doc["id"]
    title = raw_doc.get("title", "")
    content = raw_doc.get("raw_content", "")
    source_url = raw_doc.get("source_url", "")
    source_domain = raw_doc.get("source_domain", "")

    # Parse metadata (may be JSON string or dict)
    metadata = raw_doc.get("metadata", {})
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except (json.JSONDecodeError, TypeError):
            metadata = {}

    # Run tagger
    try:
        result: TaggingResult = tagger.tag(content, source_url=source_url)
    except Exception as exc:
        logger.error("Tagger error on raw_doc %d: %s", raw_id, exc)
        mark_processing_status(conn, raw_id, "failed", str(exc))
        return None

    # Lookup payer FK
    payer_id = _lookup_payer_id(conn, result.payer_code)

    # Extract dates from metadata
    effective_date = _extract_effective_date(metadata)
    last_updated = _extract_last_updated(metadata)

    # Determine status
    doc_status = "active"
    if result.requires_review:
        doc_status = "needs_review"

    # Build tagger metadata for audit trail
    tagger_meta = {
        "raw_scores": result.raw_scores,
        "document_type_hint": raw_doc.get("document_type_hint", ""),
        "payer_code": result.payer_code,
        "tagged_at": datetime.utcnow().isoformat(),
    }

    params = {
        "raw_document_id": raw_id,
        "payer_id": payer_id,
        "document_type": result.document_type,
        "document_subtype": result.document_subtype,
        "title": title,
        "content_text": content,
        "source_url": source_url,
        "source_domain": source_domain,
        "effective_date": effective_date,
        "last_updated": last_updated,
        "specialties": result.specialties,
        "cpt_codes": result.cpt_codes,
        "icd10_codes": result.icd10_codes,
        "hcpcs_codes": result.hcpcs_codes,
        "routing_targets": result.routing_targets,
        "confidence_score": result.confidence_score,
        "requires_review": result.requires_review,
        "tagger_metadata": json.dumps(tagger_meta),
        "status": doc_status,
    }

    try:
        with conn.cursor() as cur:
            cur.execute(INSERT_KNOWLEDGE_SQL, params)
            row = cur.fetchone()
            knowledge_id = row[0] if row else None

        conn.commit()

        # Update raw_document status
        mark_processing_status(conn, raw_id, "tagged")

        logger.info(
            "Tagged raw_doc %d → knowledge_doc %s: type=%s confidence=%.2f payer=%s",
            raw_id,
            knowledge_id,
            result.document_type,
            result.confidence_score,
            result.payer_code or "unknown",
        )
        return knowledge_id

    except Exception as exc:
        conn.rollback()
        logger.error("DB error inserting knowledge_doc for raw_doc %d: %s", raw_id, exc)
        mark_processing_status(conn, raw_id, "failed", str(exc))
        return None


# ---------------------------------------------------------------------------
# Batch tagging processor
# ---------------------------------------------------------------------------

class TaggingStats:
    """Track tagging batch statistics."""

    def __init__(self):
        self.total = 0
        self.tagged = 0
        self.failed = 0
        self.needs_review = 0
        self.started_at = datetime.utcnow()

    @property
    def elapsed(self) -> float:
        return (datetime.utcnow() - self.started_at).total_seconds()

    def __repr__(self) -> str:
        return (
            f"TaggingStats("
            f"total={self.total}, "
            f"tagged={self.tagged}, "
            f"failed={self.failed}, "
            f"needs_review={self.needs_review}, "
            f"elapsed={self.elapsed:.1f}s)"
        )


def process_pending_documents(
    conn,
    tagger: DocumentTagger | None = None,
    batch_size: int = 100,
    max_documents: int | None = None,
) -> TaggingStats:
    """
    Process all pending raw_documents through the tagger.

    Fetches documents in batches, tags each one, and inserts into
    knowledge_documents. Updates processing_status on completion.

    Args:
        conn: psycopg2 connection
        tagger: DocumentTagger instance (created fresh if None)
        batch_size: Number of documents to fetch per DB query
        max_documents: Stop after this many documents (None = process all)

    Returns:
        TaggingStats with counts
    """
    if tagger is None:
        tagger = DocumentTagger()
        logger.info("Initialised DocumentTagger")

    stats = TaggingStats()

    while True:
        # Fetch next batch of pending documents
        pending = get_pending_documents(conn, limit=batch_size)
        if not pending:
            logger.info("No more pending documents to process")
            break

        logger.info("Processing batch of %d pending documents", len(pending))

        for raw_doc in pending:
            if max_documents and stats.total >= max_documents:
                logger.info("Reached max_documents limit (%d)", max_documents)
                return stats

            stats.total += 1

            # Mark as processing to prevent concurrent re-processing
            mark_processing_status(conn, raw_doc["id"], "processing")

            knowledge_id = tag_and_insert(conn, raw_doc, tagger)

            if knowledge_id is not None:
                stats.tagged += 1
                # Check if it needs review
                # Re-run quick check on result
                try:
                    result = tagger.tag(
                        raw_doc.get("raw_content", ""),
                        source_url=raw_doc.get("source_url", ""),
                    )
                    if result.requires_review:
                        stats.needs_review += 1
                except Exception:
                    pass
            else:
                stats.failed += 1

            if stats.total % 50 == 0:
                logger.info("Tagging progress: %s", stats)

        # If we got fewer than batch_size, we've processed everything
        if len(pending) < batch_size:
            break

    logger.info("Tagging complete: %s", stats)
    return stats


# ---------------------------------------------------------------------------
# CLI test runner
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    print("=== Tagger Integration Test (offline mode) ===\n")

    # Test tagger directly without DB
    tagger = DocumentTagger()

    test_docs = [
        {
            "title": "Cardiac Monitoring LCD",
            "raw_content": """
            LCD Title: Temporary Nontherapeutic Ambulatory Cardiac Monitoring Devices
            LCD ID: L34721
            Contractor: CGS Administrators, LLC

            Coverage Indications, Limitations, and/or Medical Necessity

            CARDIAC MONITORING is reasonable and necessary when:
            1. Temporary (not implanted), AND
            2. Presence of symptoms suggestive of cardiac arrythmia with symptoms
               (such as palpitations, presyncope, syncope, chest pain or shortness of breath)

            CPT codes: 93224, 93225, 93226, 93227, 93228, 93229, 93268
            ICD-10 codes: I49.0, I49.1, R00.0, R00.1, R07.9
            """,
            "source_url": "https://www.cms.gov/medicare-coverage-database/view/lcd.aspx?lcdid=34721",
        },
        {
            "title": "Aetna Clinical Policy Bulletin: MRI",
            "raw_content": """
            Aetna considers MRI of the brain medically necessary for members with:
            - Suspected intracranial neoplasm
            - Evaluation of demyelinating disease
            - Unexplained seizures

            CPT codes: 70553, 70554, 70555
            Prior authorization required for outpatient MRI.
            """,
            "source_url": "https://www.aetna.com/cpb/medical/data/100_199/0143.html",
        },
    ]

    for doc in test_docs:
        result = tagger.tag(doc["raw_content"], source_url=doc["source_url"])
        print(f"Document: {doc['title']}")
        print(f"  Type: {result.document_type}")
        print(f"  Payer: {result.payer_code}")
        print(f"  Confidence: {result.confidence_score:.2f}")
        print(f"  Specialties: {result.specialties}")
        print(f"  CPT codes: {result.cpt_codes[:5]}")
        print(f"  ICD-10 codes: {result.icd10_codes[:5]}")
        print(f"  Routing: {result.routing_targets}")
        print(f"  Needs review: {result.requires_review}")
        print()
