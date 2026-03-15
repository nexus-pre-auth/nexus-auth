"""
CMS Medicare Physician Fee Schedule (MPFS) Scraper
===================================================
Downloads the annual MPFS National Payment Amount file from CMS and converts
each HCPCS/CPT code row into a fee_schedule document for CODEMED.

Data source:
  CMS Physician Fee Schedule Downloads:
  https://www.cms.gov/Medicare/Medicare-Fee-for-Service-Payment/PhysicianFeeSched/Downloads

The MPFS ZIP contains Addendum B (national payment amounts per HCPCS code).
Each row: HCPCS code, modifier, description, work/PE/MP RVUs, facility and
non-facility payment rates, status codes, and global period.

Documents are batched by CPT range (0000–0999, 1000–1999, … 9000–9999, A–Z)
to keep document sizes manageable for chunked embedding.

Env var: CMS_MPFS_URL  (override for local test fixtures)
"""

import csv
import hashlib
import io
import logging
import os
import re
import zipfile
from datetime import datetime
from typing import Any, Generator

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# CMS updates this URL each January for the new calendar year.
# Set CMS_MPFS_URL in your .env to point to the current year's file.
CMS_MPFS_URL_DEFAULT = (
    "https://www.cms.gov/medicare/medicare-fee-for-service-payment"
    "/physiciansfeeschedule/downloads/rvu26a.zip"
)

CMS_MPFS_BASE_URL = "https://www.cms.gov/Medicare/Medicare-Fee-for-Service-Payment/PhysicianFeeSched"

# Batch HCPCS codes into documents of this size to balance doc count vs. size
CODES_PER_DOCUMENT = 200


# ---------------------------------------------------------------------------
# Shared download helper (avoids importing from cms_scraper to keep modules
# independent and testable in isolation)
# ---------------------------------------------------------------------------

def _download_zip(url: str) -> zipfile.ZipFile:
    import urllib.request
    logger.info("Downloading MPFS file from %s ...", url)
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": (
                "NexusAuth-Ingestion/2.0 (healthcare knowledge pipeline; "
                "contact: admin@nexusauth.io)"
            )
        },
    )
    with urllib.request.urlopen(req, timeout=180) as resp:
        data = resp.read()
    logger.info("Downloaded %d bytes", len(data))
    return zipfile.ZipFile(io.BytesIO(data))


def _sha256(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# CSV parsing helpers
# ---------------------------------------------------------------------------

# CMS uses different column name conventions across years; map them
_COLUMN_ALIASES = {
    # HCPCS code
    "hcpcs_code": ["hcpcs", "hcpc", "code", "hcpcs_cd"],
    # Modifier
    "modifier": ["mod", "modifier", "mod_cd"],
    # Description
    "description": [
        "description", "short_description", "descriptor",
        "hcpcs_description", "procedure_description",
    ],
    # Work RVU
    "work_rvu": [
        "work_rvu", "rvu_work", "wrvu", "work rvu",
        "non-fac_rvu_work", "physician_work_rvu",
    ],
    # Non-facility total RVU
    "non_fac_total_rvu": [
        "non-fac_total_rvu", "nonfac_total_rvu", "total_rvu_nonfac",
        "non_fac_total", "nonfacility_total_rvu",
    ],
    # Facility total RVU
    "fac_total_rvu": [
        "fac_total_rvu", "facility_total_rvu", "total_rvu_fac",
        "fac_total",
    ],
    # Non-facility payment amount
    "non_fac_payment": [
        "non-fac_na_ind_payment", "nonfac_payment", "non_fac_pay",
        "non-fac_payment", "non_facility_payment",
    ],
    # Facility payment amount
    "fac_payment": [
        "fac_na_ind_payment", "fac_payment", "facility_payment",
        "fac_pay",
    ],
    # Global period
    "global_period": ["glob", "global", "global_period", "global_surg"],
    # Status code
    "status_code": ["status_code", "status", "pc_tc_indicator", "pctc"],
}


def _normalise_headers(headers: list[str]) -> dict[str, str]:
    """
    Build a mapping from canonical field name → actual CSV column name.
    Returns only fields for which a match was found.
    """
    lower_headers = {h.lower().strip().replace(" ", "_"): h for h in headers}
    mapping: dict[str, str] = {}
    for canonical, aliases in _COLUMN_ALIASES.items():
        for alias in aliases:
            if alias in lower_headers:
                mapping[canonical] = lower_headers[alias]
                break
    return mapping


def _get(row: dict, mapping: dict[str, str], key: str, default: str = "") -> str:
    col = mapping.get(key)
    if col is None:
        return default
    return (row.get(col) or "").strip()


def _to_float(value: str) -> float | None:
    try:
        return float(value.replace(",", "")) if value else None
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Main scraper
# ---------------------------------------------------------------------------

def _build_batch_text(batch: list[dict], payment_year: int) -> str:
    """Convert a batch of parsed HCPCS rows into a single document text."""
    lines = [
        f"Medicare Physician Fee Schedule — Payment Year {payment_year}",
        f"HCPCS codes: {batch[0]['hcpcs_code']} through {batch[-1]['hcpcs_code']}",
        "",
        "This document contains Medicare physician fee schedule payment rates "
        "for HCPCS/CPT procedure codes. Each entry lists the relative value units "
        "(RVUs) and national unadjusted payment rates for facility and non-facility "
        "settings before geographic adjustment.",
        "",
        "=" * 72,
    ]
    for row in batch:
        lines.append(
            f"HCPCS: {row['hcpcs_code']}"
            + (f" Modifier: {row['modifier']}" if row.get("modifier") else "")
        )
        if row.get("description"):
            lines.append(f"  Description: {row['description']}")
        if row.get("work_rvu") is not None:
            lines.append(f"  Work RVU: {row['work_rvu']}")
        if row.get("non_fac_total_rvu") is not None:
            lines.append(f"  Non-Facility Total RVU: {row['non_fac_total_rvu']}")
        if row.get("fac_total_rvu") is not None:
            lines.append(f"  Facility Total RVU: {row['fac_total_rvu']}")
        if row.get("non_fac_payment") is not None:
            lines.append(f"  Non-Facility Payment: ${row['non_fac_payment']:.2f}")
        if row.get("fac_payment") is not None:
            lines.append(f"  Facility Payment: ${row['fac_payment']:.2f}")
        if row.get("global_period"):
            lines.append(f"  Global Period: {row['global_period']}")
        lines.append("")
    return "\n".join(lines)


def scrape_cms_mpfs(
    url: str | None = None,
    payment_year: int | None = None,
) -> Generator[dict[str, Any], None, None]:
    """
    Download and parse the CMS MPFS national payment amount file.

    Yields batched fee_schedule raw document dicts.
    Each document covers CODES_PER_DOCUMENT HCPCS codes.
    """
    url = url or os.environ.get("CMS_MPFS_URL", CMS_MPFS_URL_DEFAULT)
    payment_year = payment_year or datetime.utcnow().year
    scraped_at = datetime.utcnow()

    try:
        outer_zip = _download_zip(url)
    except Exception as exc:
        logger.error("Failed to download MPFS file: %s", exc)
        return

    # Find the CSV file inside the ZIP (may be directly in ZIP or in nested ZIP)
    csv_file: str | None = None
    for name in outer_zip.namelist():
        if name.lower().endswith(".csv") and "addendum" in name.lower():
            csv_file = name
            break
    if csv_file is None:
        # Fall back to any CSV
        for name in outer_zip.namelist():
            if name.lower().endswith(".csv"):
                csv_file = name
                break

    if csv_file is None:
        logger.error("No CSV file found in MPFS ZIP. Contents: %s", outer_zip.namelist())
        return

    logger.info("Parsing MPFS CSV: %s", csv_file)
    content = outer_zip.read(csv_file).decode("utf-8", errors="replace")
    # Skip BOM if present
    content = content.lstrip("\ufeff")
    reader = csv.DictReader(content.splitlines())

    all_rows: list[dict] = []
    col_map = _normalise_headers(reader.fieldnames or [])
    logger.info("MPFS column mapping: %s", col_map)

    for raw_row in reader:
        hcpcs = _get(raw_row, col_map, "hcpcs_code")
        if not hcpcs or not re.match(r"^[0-9A-Z]{4,5}", hcpcs):
            continue

        all_rows.append({
            "hcpcs_code": hcpcs,
            "modifier": _get(raw_row, col_map, "modifier"),
            "description": _get(raw_row, col_map, "description"),
            "work_rvu": _to_float(_get(raw_row, col_map, "work_rvu")),
            "non_fac_total_rvu": _to_float(_get(raw_row, col_map, "non_fac_total_rvu")),
            "fac_total_rvu": _to_float(_get(raw_row, col_map, "fac_total_rvu")),
            "non_fac_payment": _to_float(_get(raw_row, col_map, "non_fac_payment")),
            "fac_payment": _to_float(_get(raw_row, col_map, "fac_payment")),
            "global_period": _get(raw_row, col_map, "global_period"),
            "status_code": _get(raw_row, col_map, "status_code"),
        })

    logger.info("Parsed %d MPFS rows, batching into documents of %d", len(all_rows), CODES_PER_DOCUMENT)

    batch_num = 0
    for i in range(0, len(all_rows), CODES_PER_DOCUMENT):
        batch = all_rows[i : i + CODES_PER_DOCUMENT]
        batch_num += 1

        raw_content = _build_batch_text(batch, payment_year)
        content_hash = _sha256(raw_content)

        first_code = batch[0]["hcpcs_code"]
        last_code = batch[-1]["hcpcs_code"]
        title = f"MPFS {payment_year}: HCPCS {first_code}–{last_code} Payment Rates"

        metadata = {
            "payment_year": payment_year,
            "batch_num": batch_num,
            "first_hcpcs": first_code,
            "last_hcpcs": last_code,
            "code_count": len(batch),
            "source_file": csv_file,
        }

        yield {
            "source_url": f"{CMS_MPFS_BASE_URL}?year={payment_year}&batch={batch_num}",
            "source_domain": "cms.gov",
            "document_type_hint": "fee_schedule",
            "title": title,
            "raw_content": raw_content,
            "content_hash": content_hash,
            "metadata": metadata,
            "scraped_at": scraped_at,
        }

    logger.info("MPFS scrape complete: %d batches yielded (%d codes)", batch_num, len(all_rows))
