"""
NexusAuth Ingestion Pipeline — Orchestrator
============================================
Main entry point for the Session 2 ingestion pipeline.

Runs the full pipeline in sequence:
  Stage 1: Scrape  — Download CMS LCD/NCD bulk data
  Stage 2: Ingest  — Deduplicate + insert into raw_documents
  Stage 3: Tag     — Run DocumentTagger → insert into knowledge_documents
  Stage 4: Embed   — Generate pgvector embeddings → document_embeddings

Usage:
  # Full pipeline (all stages)
  python -m ingestion.pipeline --all

  # Individual stages
  python -m ingestion.pipeline --scrape
  python -m ingestion.pipeline --tag
  python -m ingestion.pipeline --embed

  # Dry run (scrape only, no DB writes)
  python -m ingestion.pipeline --scrape --dry-run

  # Limit documents for testing
  python -m ingestion.pipeline --all --max-docs 50

  # Skip embeddings (useful when OpenAI key not set)
  python -m ingestion.pipeline --scrape --tag

Environment variables:
  DATABASE_URL    — PostgreSQL connection string
                    (default: postgresql://nexusauth:nexusauth@localhost:5432/nexusauth)
  OPENAI_API_KEY  — Required for --embed stage
  LOG_LEVEL       — Logging verbosity (default: INFO)
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Logging setup (must happen before any module imports that use logging)
# ---------------------------------------------------------------------------

def setup_logging(level: str = "INFO", log_file: str | None = None) -> None:
    """Configure structured logging with optional file output."""
    log_level = getattr(logging, level.upper(), logging.INFO)

    handlers = [logging.StreamHandler(sys.stdout)]
    if log_file:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=handlers,
        force=True,
    )

    # Suppress noisy third-party loggers
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)


logger = logging.getLogger("nexusauth.pipeline")


# ---------------------------------------------------------------------------
# Database connection
# ---------------------------------------------------------------------------

DEFAULT_DATABASE_URL = "postgresql://nexusauth:nexusauth@localhost:5432/nexusauth"


def get_db_connection(database_url: str | None = None):
    """
    Return a psycopg2 connection.
    Raises ImportError if psycopg2 is not installed.
    """
    try:
        import psycopg2
    except ImportError:
        raise ImportError(
            "psycopg2 not installed. Run: pip install psycopg2-binary"
        )

    url = database_url or os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)
    logger.info("Connecting to database: %s", url.split("@")[-1])  # Hide credentials

    conn = psycopg2.connect(url)
    conn.autocommit = False
    return conn


# ---------------------------------------------------------------------------
# Pipeline stages
# ---------------------------------------------------------------------------

def stage_scrape_and_ingest(
    conn,
    include_lcds: bool = True,
    include_ncds: bool = True,
    dry_run: bool = False,
    max_docs: int | None = None,
) -> dict:
    """
    Stage 1 + 2: Scrape CMS data and insert into raw_documents.

    Returns stats dict.
    """
    from ingestion.scrapers.cms_scraper import scrape_all_cms
    from ingestion.deduplicator import process_batch

    logger.info("=" * 60)
    logger.info("STAGE 1+2: Scrape + Ingest")
    logger.info("=" * 60)

    start = time.time()

    # Collect documents from scraper
    logger.info("Starting CMS scrape (LCDs=%s, NCDs=%s)...", include_lcds, include_ncds)
    documents = list(scrape_all_cms(
        include_lcds=include_lcds,
        include_ncds=include_ncds,
    ))
    logger.info("Scraped %d documents from CMS", len(documents))

    if max_docs:
        documents = documents[:max_docs]
        logger.info("Limited to %d documents (--max-docs)", max_docs)

    if dry_run:
        logger.info("DRY RUN: Skipping database insertion")
        # Show sample
        for doc in documents[:3]:
            logger.info(
                "  [DRY RUN] Would insert: %s (%s) hash=%s...",
                doc["title"][:60],
                doc["document_type_hint"],
                doc["content_hash"][:12],
            )
        return {
            "stage": "scrape+ingest",
            "dry_run": True,
            "scraped": len(documents),
            "inserted": 0,
            "duplicates": 0,
            "elapsed": time.time() - start,
        }

    # Insert into raw_documents with deduplication
    stats = process_batch(conn, documents)

    result = {
        "stage": "scrape+ingest",
        "dry_run": False,
        "scraped": stats.total_seen,
        "inserted": stats.inserted,
        "duplicates": stats.duplicates,
        "errors": stats.errors,
        "elapsed": time.time() - start,
    }
    logger.info("Stage 1+2 complete: %s", result)
    return result


def stage_tag(
    conn,
    batch_size: int = 100,
    max_docs: int | None = None,
) -> dict:
    """
    Stage 3: Tag pending raw_documents → knowledge_documents.

    Returns stats dict.
    """
    from ingestion.tagger_integration import process_pending_documents

    logger.info("=" * 60)
    logger.info("STAGE 3: Tag")
    logger.info("=" * 60)

    start = time.time()
    stats = process_pending_documents(
        conn,
        batch_size=batch_size,
        max_documents=max_docs,
    )

    result = {
        "stage": "tag",
        "total": stats.total,
        "tagged": stats.tagged,
        "failed": stats.failed,
        "needs_review": stats.needs_review,
        "elapsed": time.time() - start,
    }
    logger.info("Stage 3 complete: %s", result)
    return result


def stage_embed(
    conn,
    batch_size: int = 50,
    max_docs: int | None = None,
) -> dict:
    """
    Stage 4: Generate pgvector embeddings for knowledge_documents.

    Returns stats dict.
    """
    from ingestion.embedder import process_unembedded_documents

    logger.info("=" * 60)
    logger.info("STAGE 4: Embed")
    logger.info("=" * 60)

    if not os.environ.get("OPENAI_API_KEY"):
        logger.warning("OPENAI_API_KEY not set — skipping embedding stage")
        return {"stage": "embed", "skipped": True, "reason": "OPENAI_API_KEY not set"}

    start = time.time()
    stats = process_unembedded_documents(
        conn,
        batch_size=batch_size,
        max_documents=max_docs,
    )

    result = {
        "stage": "embed",
        "total_docs": stats.total_docs,
        "embedded_docs": stats.embedded_docs,
        "total_chunks": stats.total_chunks,
        "failed_docs": stats.failed_docs,
        "api_calls": stats.api_calls,
        "elapsed": time.time() - start,
    }
    logger.info("Stage 4 complete: %s", result)
    return result


# ---------------------------------------------------------------------------
# Pipeline summary report
# ---------------------------------------------------------------------------

def print_pipeline_summary(results: list[dict], total_elapsed: float) -> None:
    """Print a formatted pipeline run summary."""
    logger.info("")
    logger.info("=" * 60)
    logger.info("PIPELINE RUN SUMMARY")
    logger.info("=" * 60)
    logger.info("Total elapsed: %.1fs", total_elapsed)
    logger.info("")

    for r in results:
        stage = r.get("stage", "unknown")
        elapsed = r.get("elapsed", 0)

        if stage == "scrape+ingest":
            logger.info(
                "  Scrape+Ingest: scraped=%d, inserted=%d, duplicates=%d, errors=%d (%.1fs)",
                r.get("scraped", 0),
                r.get("inserted", 0),
                r.get("duplicates", 0),
                r.get("errors", 0),
                elapsed,
            )
        elif stage == "tag":
            logger.info(
                "  Tag:           total=%d, tagged=%d, failed=%d, needs_review=%d (%.1fs)",
                r.get("total", 0),
                r.get("tagged", 0),
                r.get("failed", 0),
                r.get("needs_review", 0),
                elapsed,
            )
        elif stage == "embed":
            if r.get("skipped"):
                logger.info("  Embed:         SKIPPED (%s)", r.get("reason", ""))
            else:
                logger.info(
                    "  Embed:         docs=%d, chunks=%d, failed=%d (%.1fs)",
                    r.get("embedded_docs", 0),
                    r.get("total_chunks", 0),
                    r.get("failed_docs", 0),
                    elapsed,
                )

    logger.info("=" * 60)


# ---------------------------------------------------------------------------
# CLI argument parser
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="nexusauth-pipeline",
        description="NexusAuth Knowledge Layer Ingestion Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full pipeline
  python -m ingestion.pipeline --all

  # Scrape + tag only (no embeddings)
  python -m ingestion.pipeline --scrape --tag

  # Test with 20 documents, dry run
  python -m ingestion.pipeline --scrape --dry-run --max-docs 20

  # Embed only (after scrape+tag already ran)
  python -m ingestion.pipeline --embed

  # Full pipeline with custom DB
  DATABASE_URL=postgresql://user:pass@host/db python -m ingestion.pipeline --all
        """,
    )

    # Stage selection
    stage_group = parser.add_argument_group("Pipeline stages")
    stage_group.add_argument(
        "--all", action="store_true",
        help="Run all stages: scrape → tag → embed"
    )
    stage_group.add_argument(
        "--scrape", action="store_true",
        help="Stage 1+2: Scrape CMS data and insert into raw_documents"
    )
    stage_group.add_argument(
        "--tag", action="store_true",
        help="Stage 3: Tag pending raw_documents → knowledge_documents"
    )
    stage_group.add_argument(
        "--embed", action="store_true",
        help="Stage 4: Generate pgvector embeddings for knowledge_documents"
    )

    # Scraper options
    scraper_group = parser.add_argument_group("Scraper options")
    scraper_group.add_argument(
        "--no-lcds", action="store_true",
        help="Skip LCD scraping"
    )
    scraper_group.add_argument(
        "--no-ncds", action="store_true",
        help="Skip NCD scraping"
    )

    # General options
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Scrape but do not write to database"
    )
    parser.add_argument(
        "--max-docs", type=int, default=None, metavar="N",
        help="Process at most N documents (for testing)"
    )
    parser.add_argument(
        "--batch-size", type=int, default=100, metavar="N",
        help="DB batch size (default: 100)"
    )
    parser.add_argument(
        "--database-url", type=str, default=None,
        help="PostgreSQL connection string (overrides DATABASE_URL env var)"
    )
    parser.add_argument(
        "--log-level", type=str, default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity (default: INFO)"
    )
    parser.add_argument(
        "--log-file", type=str, default=None, metavar="PATH",
        help="Write logs to file in addition to stdout"
    )
    parser.add_argument(
        "--output-json", type=str, default=None, metavar="PATH",
        help="Write pipeline run summary as JSON to this file"
    )

    return parser


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    """
    Main CLI entry point.
    Returns exit code (0 = success, 1 = error).
    """
    parser = build_parser()
    args = parser.parse_args(argv)

    # Setup logging
    setup_logging(args.log_level, args.log_file)

    # Validate: at least one stage must be selected
    if not (args.all or args.scrape or args.tag or args.embed):
        parser.print_help()
        logger.error("\nError: specify at least one stage (--all, --scrape, --tag, --embed)")
        return 1

    # Determine which stages to run
    run_scrape = args.all or args.scrape
    run_tag = args.all or args.tag
    run_embed = args.all or args.embed

    logger.info("NexusAuth Ingestion Pipeline starting")
    logger.info("Stages: scrape=%s, tag=%s, embed=%s", run_scrape, run_tag, run_embed)
    logger.info("Options: dry_run=%s, max_docs=%s, batch_size=%s",
                args.dry_run, args.max_docs, args.batch_size)

    pipeline_start = time.time()
    results = []

    # Connect to database (skip if dry-run scrape only)
    conn = None
    if not (args.dry_run and run_scrape and not run_tag and not run_embed):
        try:
            conn = get_db_connection(args.database_url)
            logger.info("Database connection established")
        except Exception as exc:
            if args.dry_run:
                logger.warning("DB connection failed (dry-run mode, continuing): %s", exc)
            else:
                logger.error("Database connection failed: %s", exc)
                return 1

    try:
        # Stage 1+2: Scrape + Ingest
        if run_scrape:
            result = stage_scrape_and_ingest(
                conn,
                include_lcds=not args.no_lcds,
                include_ncds=not args.no_ncds,
                dry_run=args.dry_run,
                max_docs=args.max_docs,
            )
            results.append(result)

        # Stage 3: Tag
        if run_tag and conn:
            result = stage_tag(
                conn,
                batch_size=args.batch_size,
                max_docs=args.max_docs,
            )
            results.append(result)

        # Stage 4: Embed
        if run_embed and conn:
            result = stage_embed(
                conn,
                batch_size=min(args.batch_size, 50),
                max_docs=args.max_docs,
            )
            results.append(result)

    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        if conn:
            conn.rollback()
        return 130

    except Exception as exc:
        logger.exception("Pipeline failed with unhandled exception: %s", exc)
        if conn:
            conn.rollback()
        return 1

    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed")

    total_elapsed = time.time() - pipeline_start
    print_pipeline_summary(results, total_elapsed)

    # Write JSON output if requested
    if args.output_json:
        output = {
            "run_at": datetime.utcnow().isoformat(),
            "total_elapsed": total_elapsed,
            "stages": results,
        }
        Path(args.output_json).parent.mkdir(parents=True, exist_ok=True)
        with open(args.output_json, "w") as f:
            json.dump(output, f, indent=2)
        logger.info("Pipeline summary written to %s", args.output_json)

    logger.info("Pipeline complete in %.1fs", total_elapsed)
    return 0


if __name__ == "__main__":
    sys.exit(main())
