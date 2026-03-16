"""
Knowledge Base Seeder
=====================
Loads structured regulatory mandate records into knowledge_documents.

This is a lightweight alternative to the full scrape→tag→embed pipeline for
data we already have in structured form (e.g. 2026_updates.py).  Records are
upserted on policy_number so re-running is always safe.

Usage:
    # Seed all mandate modules
    python -m knowledge-base.seeder

    # Seed a specific year
    python -m knowledge-base.seeder --year 2026

    # Dry run (print what would be inserted)
    python -m knowledge-base.seeder --dry-run

Environment variables:
    DATABASE_URL  — PostgreSQL connection string
"""

import argparse
import importlib.util
import json
import logging
import os
import sys
from datetime import date
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_DATABASE_URL = "postgresql://nexusauth:nexusauth@localhost:5432/nexusauth"


# ---------------------------------------------------------------------------
# Insert / upsert helpers
# ---------------------------------------------------------------------------

def _resolve_payer_ids(conn, payer_codes: list[str]) -> list[str]:
    """Return UUIDs for the given payer codes (skips unknowns silently)."""
    if not payer_codes:
        return []
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM payers WHERE payer_code = ANY(%s)",
            (payer_codes,),
        )
        return [str(row[0]) for row in cur.fetchall()]


def seed_mandate(conn, mandate: dict, dry_run: bool = False) -> str:
    """
    Upsert a single mandate dict into knowledge_documents.
    Returns 'inserted', 'updated', or 'dry_run'.
    """
    policy_number = mandate["policy_number"]
    payer_ids = _resolve_payer_ids(conn, mandate.get("payer_codes", []))

    # Use first payer_id as the FK (documents are payer-scoped to primary payer)
    payer_id = payer_ids[0] if payer_ids else None

    effective_date = mandate.get("effective_date")
    if isinstance(effective_date, str):
        effective_date = date.fromisoformat(effective_date)

    if dry_run:
        logger.info(
            "[DRY RUN] Would upsert: %s (%s)",
            mandate["title"][:80],
            mandate["document_type"],
        )
        return "dry_run"

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO knowledge_documents (
                payer_id,
                title,
                document_type,
                policy_number,
                effective_date,
                content_text,
                content_summary,
                cpt_codes,
                specialties,
                confidence_score,
                processing_status,
                is_active,
                metadata
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                'tagged', TRUE, %s
            )
            ON CONFLICT (policy_number)
            DO UPDATE SET
                title            = EXCLUDED.title,
                document_type    = EXCLUDED.document_type,
                effective_date   = EXCLUDED.effective_date,
                content_text     = EXCLUDED.content_text,
                content_summary  = EXCLUDED.content_summary,
                cpt_codes        = EXCLUDED.cpt_codes,
                specialties      = EXCLUDED.specialties,
                confidence_score = EXCLUDED.confidence_score,
                is_active        = TRUE,
                metadata         = EXCLUDED.metadata,
                updated_at       = NOW()
            RETURNING (xmax = 0) AS inserted
            """,
            (
                payer_id,
                mandate["title"],
                mandate["document_type"],
                policy_number,
                effective_date,
                mandate.get("content_text", ""),
                mandate.get("content_summary", ""),
                mandate.get("cpt_codes") or [],
                mandate.get("specialties") or [],
                mandate.get("confidence_score", 0.90),
                json.dumps(mandate.get("metadata", {})),
            ),
        )
        row = cur.fetchone()

    conn.commit()
    return "inserted" if row and row[0] else "updated"


def seed_all(conn, mandates: list[dict], dry_run: bool = False) -> dict:
    """Upsert all mandate records. Returns summary counts."""
    counts = {"inserted": 0, "updated": 0, "dry_run": 0, "errors": 0}

    for mandate in mandates:
        try:
            outcome = seed_mandate(conn, mandate, dry_run=dry_run)
            counts[outcome] = counts.get(outcome, 0) + 1
            logger.info(
                "%s: %s — %s",
                outcome.upper(),
                mandate["policy_number"],
                mandate["title"][:60],
            )
        except Exception as exc:
            counts["errors"] += 1
            logger.error("Failed to seed %s: %s", mandate.get("policy_number"), exc)
            conn.rollback()

    return counts


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="knowledge-base-seeder",
        description="Seed regulatory mandate data into knowledge_documents",
    )
    parser.add_argument(
        "--year", type=int, default=None,
        help="Seed a specific year's mandates (default: all available years)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print what would be inserted without writing to the database",
    )
    parser.add_argument(
        "--database-url", type=str, default=None,
        help="PostgreSQL connection string (overrides DATABASE_URL env var)",
    )
    parser.add_argument(
        "--log-level", type=str, default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Collect mandate records
    all_mandates: list[dict] = []

    _kb_dir = Path(__file__).parent

    if args.year is None or args.year == 2026:
        _spec = importlib.util.spec_from_file_location(
            "_mandates_2026", _kb_dir / "2026_updates.py"
        )
        _mod = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_mod)
        all_mandates.extend(_mod.MANDATES_2026)
        logger.info("Loaded %d mandate(s) from 2026_updates.py", len(_mod.MANDATES_2026))

    if not all_mandates:
        logger.warning("No mandate records found for the specified year(s).")
        return 0

    if args.dry_run:
        logger.info("DRY RUN mode — no database writes will occur")

    # Connect
    try:
        import psycopg2
    except ImportError:
        logger.error("psycopg2 not installed. Run: pip install psycopg2-binary")
        return 1

    url = args.database_url or os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)

    try:
        conn = psycopg2.connect(url)
    except Exception as exc:
        if args.dry_run:
            logger.warning("DB connection failed (dry-run, continuing): %s", exc)
            conn = None
        else:
            logger.error("Database connection failed: %s", exc)
            return 1

    try:
        counts = seed_all(conn, all_mandates, dry_run=args.dry_run)
    finally:
        if conn:
            conn.close()

    logger.info(
        "Seeder complete — inserted=%d updated=%d dry_run=%d errors=%d",
        counts.get("inserted", 0),
        counts.get("updated", 0),
        counts.get("dry_run", 0),
        counts.get("errors", 0),
    )
    return 1 if counts.get("errors") else 0


if __name__ == "__main__":
    sys.exit(main())
