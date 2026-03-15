"""
CMS NCCI (National Correct Coding Initiative) Scraper
=====================================================
Downloads CMS NCCI Procedure-to-Procedure (PTP) edits and Medically Unlikely
Edits (MUE) and converts them into ncci_edit documents for CODEMED.

Data sources (quarterly updates):
  PTP: https://www.cms.gov/files/zip/ncci-practitioner-py{yy}.zip
  MUE: https://www.cms.gov/files/zip/ncci-mue-practitioner.zip

PTP edits define pairs of CPT/HCPCS codes that cannot be billed together on
the same date without a valid modifier. Each pair has a modifier indicator:
  0 = Modifier not allowed (always bundled)
  1 = Modifier may allow separate payment
  9 = Not applicable (mutually exclusive)

MUE edits define the maximum units per claim/date of service for a given code.

Documents are batched by CODES_PER_DOCUMENT pairs to keep sizes manageable.

Env vars:
  CMS_NCCI_PTP_URL  — override PTP download URL
  CMS_NCCI_MUE_URL  — override MUE download URL
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

_YEAR_SHORT = str(datetime.utcnow().year)[2:]  # e.g. "26" for 2026

CMS_NCCI_PTP_URL_DEFAULT = (
    f"https://www.cms.gov/files/zip/ncci-practitioner-py{_YEAR_SHORT}.zip"
)
CMS_NCCI_MUE_URL_DEFAULT = (
    "https://www.cms.gov/files/zip/ncci-mue-practitioner.zip"
)

CMS_NCCI_BASE_URL = "https://www.cms.gov/medicare/coding-billing/national-correct-coding-initiative-ncci-edits"

PAIRS_PER_DOCUMENT = 500     # PTP pairs per document
CODES_PER_DOCUMENT = 300     # MUE entries per document


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _download_zip(url: str) -> zipfile.ZipFile:
    import urllib.request
    logger.info("Downloading NCCI file from %s ...", url)
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


def _parse_date(value: str) -> str:
    """Normalise CMS date strings to YYYY-MM-DD or return empty."""
    if not value or value.strip() in ("", "00/00/0000", "N/A"):
        return ""
    value = value.strip()
    for fmt in ("%m/%d/%Y", "%Y%m%d", "%Y-%m-%d"):
        try:
            from datetime import datetime as dt
            return dt.strptime(value, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return value


def _find_csv_in_zip(zf: zipfile.ZipFile, keyword: str = "") -> str | None:
    """Find the first CSV file in a ZipFile matching an optional keyword."""
    names = zf.namelist()
    # Prefer files with keyword in name
    if keyword:
        for name in names:
            if keyword.lower() in name.lower() and name.lower().endswith(".csv"):
                return name
    # Fall back to any CSV
    for name in names:
        if name.lower().endswith(".csv"):
            return name
    # Maybe there is a nested zip
    for name in names:
        if name.lower().endswith(".zip"):
            inner_data = zf.read(name)
            inner_zip = zipfile.ZipFile(io.BytesIO(inner_data))
            result = _find_csv_in_zip(inner_zip, keyword)
            if result:
                return f"{name}::{result}"
    return None


def _read_csv_from_zip(zf: zipfile.ZipFile, path: str) -> list[dict]:
    """Read a CSV from a zip, handling nested zip paths (inner::file.csv)."""
    if "::" in path:
        outer_name, inner_path = path.split("::", 1)
        inner_zip = zipfile.ZipFile(io.BytesIO(zf.read(outer_name)))
        content = inner_zip.read(inner_path).decode("utf-8", errors="replace")
    else:
        content = zf.read(path).decode("utf-8", errors="replace")
    content = content.lstrip("\ufeff")
    reader = csv.DictReader(content.splitlines())
    return list(reader)


def _normalise_ptp_row(row: dict) -> dict | None:
    """Extract PTP fields from a raw CSV row regardless of column naming."""
    # Try common column name variants
    def get(*keys: str) -> str:
        for k in keys:
            for actual_key in row:
                if actual_key.lower().strip().replace(" ", "_") == k.lower().replace(" ", "_"):
                    val = (row[actual_key] or "").strip()
                    if val:
                        return val
        return ""

    col1 = get("col_1_hcpcs_cd", "column_1", "col_1", "hcpcs_code_1", "code_1", "column1")
    col2 = get("col_2_hcpcs_cd", "column_2", "col_2", "hcpcs_code_2", "code_2", "column2")

    if not col1 or not col2:
        return None
    if not re.match(r"^[0-9A-Z]{4,5}", col1):
        return None

    return {
        "col1": col1,
        "col2": col2,
        "modifier_indicator": get(
            "modifier_indicator", "mod_indicator", "modifier", "mod"
        ),
        "effective_date": _parse_date(get("effective_date", "eff_date", "effdt")),
        "deletion_date": _parse_date(get("deletion_date", "del_date", "deldt")),
    }


def _normalise_mue_row(row: dict) -> dict | None:
    """Extract MUE fields from a raw CSV row."""
    def get(*keys: str) -> str:
        for k in keys:
            for actual_key in row:
                if actual_key.lower().strip().replace(" ", "_") == k.lower().replace(" ", "_"):
                    val = (row[actual_key] or "").strip()
                    if val:
                        return val
        return ""

    hcpcs = get("hcpcs_code", "hcpcs", "code", "procedure_code")
    if not hcpcs or not re.match(r"^[0-9A-Z]{4,5}", hcpcs):
        return None

    return {
        "hcpcs": hcpcs,
        "mue_value": get("mue_value", "mue", "units", "max_units"),
        "mai": get(
            "mue_adjudication_indicator", "mai", "adjudication_indicator"
        ),
        "rationale": get("rationale", "mue_rationale"),
    }


# ---------------------------------------------------------------------------
# Document builders
# ---------------------------------------------------------------------------

def _modifier_indicator_label(mi: str) -> str:
    return {
        "0": "Modifier not allowed (always bundled)",
        "1": "Modifier may allow separate payment",
        "9": "Not applicable / mutually exclusive",
    }.get(mi, mi or "Unknown")


def _build_ptp_document(
    batch: list[dict],
    batch_num: int,
    data_year: int,
    data_quarter: int,
) -> str:
    lines = [
        "NCCI Procedure-to-Procedure (PTP) Edits",
        f"Data Year: {data_year}  Quarter: Q{data_quarter}",
        f"Edit pairs: {batch[0]['col1']} (Col1) … batch {batch_num}",
        "",
        "These NCCI edits define CPT/HCPCS code pairs that cannot be billed "
        "together without a valid modifier. Column 1 codes take precedence over "
        "Column 2 codes. When a modifier indicator of 1 is present, an appropriate "
        "modifier appended to the Column 2 code may allow separate reimbursement.",
        "",
        "=" * 72,
    ]
    for pair in batch:
        mi_label = _modifier_indicator_label(pair.get("modifier_indicator", ""))
        lines.append(f"Column 1: {pair['col1']}  Column 2: {pair['col2']}")
        lines.append(f"  Modifier Indicator: {pair.get('modifier_indicator', '')} — {mi_label}")
        if pair.get("effective_date"):
            lines.append(f"  Effective: {pair['effective_date']}")
        if pair.get("deletion_date"):
            lines.append(f"  Deletion: {pair['deletion_date']}")
        lines.append("")
    return "\n".join(lines)


def _build_mue_document(
    batch: list[dict],
    batch_num: int,
    data_year: int,
    data_quarter: int,
) -> str:
    _MAI_LABELS = {
        "1": "Claim level (units apply across entire claim)",
        "2": "Date of Service level",
        "3": "Remittance Advice (RA) level",
    }
    lines = [
        "NCCI Medically Unlikely Edits (MUE) — Practitioner",
        f"Data Year: {data_year}  Quarter: Q{data_quarter}",
        f"HCPCS codes batch {batch_num}: {batch[0]['hcpcs']}–{batch[-1]['hcpcs']}",
        "",
        "Medically Unlikely Edits (MUEs) define the maximum units of service "
        "a provider would report for a HCPCS/CPT code on a single claim or date "
        "of service under most circumstances. Exceeding the MUE value typically "
        "results in a claim denial. The MUE Adjudication Indicator (MAI) "
        "determines how the edit is applied.",
        "",
        "=" * 72,
    ]
    for entry in batch:
        mai_label = _MAI_LABELS.get(entry.get("mai", ""), entry.get("mai", "Unknown"))
        lines.append(f"HCPCS: {entry['hcpcs']}")
        lines.append(f"  MUE Value: {entry.get('mue_value', 'N/A')} units")
        lines.append(f"  MAI: {entry.get('mai', '')} — {mai_label}")
        if entry.get("rationale"):
            lines.append(f"  Rationale: {entry['rationale']}")
        lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main scrapers
# ---------------------------------------------------------------------------

def scrape_cms_ncci_ptp(
    url: str | None = None,
    data_year: int | None = None,
    data_quarter: int | None = None,
) -> Generator[dict[str, Any], None, None]:
    """Scrape NCCI PTP edits and yield batched ncci_edit documents."""
    url = url or os.environ.get("CMS_NCCI_PTP_URL", CMS_NCCI_PTP_URL_DEFAULT)
    now = datetime.utcnow()
    data_year = data_year or now.year
    data_quarter = data_quarter or ((now.month - 1) // 3 + 1)
    scraped_at = now

    try:
        zf = _download_zip(url)
    except Exception as exc:
        logger.error("Failed to download NCCI PTP file: %s", exc)
        return

    csv_path = _find_csv_in_zip(zf, "practitioner")
    if csv_path is None:
        csv_path = _find_csv_in_zip(zf)
    if csv_path is None:
        logger.error("No CSV found in NCCI PTP ZIP. Contents: %s", zf.namelist())
        return

    logger.info("Parsing NCCI PTP CSV: %s", csv_path)
    rows_raw = _read_csv_from_zip(zf, csv_path)
    rows = [r for raw in rows_raw if (r := _normalise_ptp_row(raw)) is not None]
    logger.info("Parsed %d PTP edit pairs", len(rows))

    batch_num = 0
    for i in range(0, len(rows), PAIRS_PER_DOCUMENT):
        batch = rows[i : i + PAIRS_PER_DOCUMENT]
        batch_num += 1
        raw_content = _build_ptp_document(batch, batch_num, data_year, data_quarter)
        content_hash = _sha256(raw_content)
        title = (
            f"NCCI PTP Edits {data_year} Q{data_quarter} — "
            f"Batch {batch_num} ({batch[0]['col1']}–{batch[-1]['col1']})"
        )
        yield {
            "source_url": f"{CMS_NCCI_BASE_URL}?type=ptp&year={data_year}&q={data_quarter}&batch={batch_num}",
            "source_domain": "cms.gov",
            "document_type_hint": "ncci_edit",
            "title": title,
            "raw_content": raw_content,
            "content_hash": content_hash,
            "metadata": {
                "ncci_type": "PTP",
                "data_year": data_year,
                "data_quarter": data_quarter,
                "batch_num": batch_num,
                "pair_count": len(batch),
                "first_col1": batch[0]["col1"],
                "last_col1": batch[-1]["col1"],
            },
            "scraped_at": scraped_at,
        }

    logger.info("NCCI PTP scrape complete: %d batch documents", batch_num)


def scrape_cms_ncci_mue(
    url: str | None = None,
    data_year: int | None = None,
    data_quarter: int | None = None,
) -> Generator[dict[str, Any], None, None]:
    """Scrape NCCI MUE edits and yield batched ncci_edit documents."""
    url = url or os.environ.get("CMS_NCCI_MUE_URL", CMS_NCCI_MUE_URL_DEFAULT)
    now = datetime.utcnow()
    data_year = data_year or now.year
    data_quarter = data_quarter or ((now.month - 1) // 3 + 1)
    scraped_at = now

    try:
        zf = _download_zip(url)
    except Exception as exc:
        logger.error("Failed to download NCCI MUE file: %s", exc)
        return

    csv_path = _find_csv_in_zip(zf, "practitioner")
    if csv_path is None:
        csv_path = _find_csv_in_zip(zf)
    if csv_path is None:
        logger.error("No CSV found in NCCI MUE ZIP. Contents: %s", zf.namelist())
        return

    logger.info("Parsing NCCI MUE CSV: %s", csv_path)
    rows_raw = _read_csv_from_zip(zf, csv_path)
    rows = [r for raw in rows_raw if (r := _normalise_mue_row(raw)) is not None]
    logger.info("Parsed %d MUE entries", len(rows))

    batch_num = 0
    for i in range(0, len(rows), CODES_PER_DOCUMENT):
        batch = rows[i : i + CODES_PER_DOCUMENT]
        batch_num += 1
        raw_content = _build_mue_document(batch, batch_num, data_year, data_quarter)
        content_hash = _sha256(raw_content)
        title = (
            f"NCCI MUE {data_year} Q{data_quarter} — "
            f"Batch {batch_num} ({batch[0]['hcpcs']}–{batch[-1]['hcpcs']})"
        )
        yield {
            "source_url": f"{CMS_NCCI_BASE_URL}?type=mue&year={data_year}&q={data_quarter}&batch={batch_num}",
            "source_domain": "cms.gov",
            "document_type_hint": "ncci_edit",
            "title": title,
            "raw_content": raw_content,
            "content_hash": content_hash,
            "metadata": {
                "ncci_type": "MUE",
                "data_year": data_year,
                "data_quarter": data_quarter,
                "batch_num": batch_num,
                "code_count": len(batch),
                "first_hcpcs": batch[0]["hcpcs"],
                "last_hcpcs": batch[-1]["hcpcs"],
            },
            "scraped_at": scraped_at,
        }

    logger.info("NCCI MUE scrape complete: %d batch documents", batch_num)


def scrape_all_ncci(
    ptp_url: str | None = None,
    mue_url: str | None = None,
    include_ptp: bool = True,
    include_mue: bool = True,
) -> Generator[dict[str, Any], None, None]:
    """Unified entry point for all NCCI edit types."""
    if include_ptp:
        logger.info("Starting NCCI PTP scrape...")
        yield from scrape_cms_ncci_ptp(url=ptp_url)

    if include_mue:
        logger.info("Starting NCCI MUE scrape...")
        yield from scrape_cms_ncci_mue(url=mue_url)
