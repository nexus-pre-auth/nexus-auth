"""
CMS Average Sales Price (ASP) Drug Pricing Scraper
===================================================
Downloads CMS quarterly ASP drug pricing files and generates asp_drug_pricing
documents for CODEMED.

CMS publishes ASP pricing quarterly for Medicare Part B drugs. The payment
limit for most drugs is ASP + 6%. These prices are used for "buy and bill"
injectable/infusible drugs administered in a physician's office or outpatient
hospital setting.

Data source:
  https://www.cms.gov/medicare/medicare-fee-for-service-payment/mcrpartbdrugavgsalesprice/aspfiles

Env var: CMS_ASP_URL  (override for testing with local fixture file)
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

_NOW = datetime.utcnow()
_QUARTER = (_NOW.month - 1) // 3 + 1
_YEAR_SHORT = str(_NOW.year)[2:]

CMS_ASP_URL_DEFAULT = (
    f"https://www.cms.gov/medicare/medicare-fee-for-service-payment"
    f"/mcrpartbdrugavgsalesprice/downloads/asp{_YEAR_SHORT}q{_QUARTER}-noc.zip"
)
CMS_ASP_BASE_URL = (
    "https://www.cms.gov/medicare/medicare-fee-for-service-payment"
    "/mcrpartbdrugavgsalesprice/aspfiles"
)

CODES_PER_DOCUMENT = 150


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _download_zip(url: str) -> zipfile.ZipFile:
    import urllib.request
    logger.info("Downloading ASP file from %s ...", url)
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "NexusAuth-Ingestion/2.0 (healthcare knowledge pipeline)"},
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        data = resp.read()
    logger.info("Downloaded %d bytes", len(data))
    return zipfile.ZipFile(io.BytesIO(data))


def _sha256(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _find_csv(zf: zipfile.ZipFile) -> str | None:
    for name in zf.namelist():
        if name.lower().endswith(".csv"):
            return name
    # Check for nested zip
    for name in zf.namelist():
        if name.lower().endswith(".zip"):
            inner = zipfile.ZipFile(io.BytesIO(zf.read(name)))
            for inner_name in inner.namelist():
                if inner_name.lower().endswith(".csv"):
                    return f"{name}::{inner_name}"
    return None


def _read_csv(zf: zipfile.ZipFile, path: str) -> list[dict]:
    if "::" in path:
        outer, inner_path = path.split("::", 1)
        inner_zip = zipfile.ZipFile(io.BytesIO(zf.read(outer)))
        content = inner_zip.read(inner_path).decode("utf-8", errors="replace")
    else:
        content = zf.read(path).decode("utf-8", errors="replace")
    return list(csv.DictReader(content.lstrip("\ufeff").splitlines()))


def _get_col(row: dict, *candidates: str) -> str:
    """Get first matching value from a row dict by trying multiple key names."""
    lrow = {k.lower().strip().replace(" ", "_"): v for k, v in row.items()}
    for c in candidates:
        val = lrow.get(c.lower().replace(" ", "_"), "")
        if val:
            return val.strip()
    return ""


def _to_float(val: str) -> float | None:
    if not val:
        return None
    val = val.replace("$", "").replace(",", "").strip()
    try:
        return float(val)
    except ValueError:
        return None


def _parse_quarter_from_filename(filename: str) -> tuple[int, int]:
    """Extract year and quarter from CMS ASP filename conventions."""
    # Patterns: asp26q1, asp2026q2, 2026q3, etc.
    m = re.search(r"(?:asp)?(\d{2,4})q(\d)", filename.lower())
    if m:
        yr_str, q_str = m.group(1), m.group(2)
        yr = int(yr_str)
        if yr < 100:
            yr += 2000
        return yr, int(q_str)
    return datetime.utcnow().year, (_NOW.month - 1) // 3 + 1


# ---------------------------------------------------------------------------
# Document builder
# ---------------------------------------------------------------------------

def _build_asp_document(
    batch: list[dict],
    batch_num: int,
    effective_year: int,
    effective_quarter: int,
) -> str:
    lines = [
        f"CMS Medicare Part B Drug Average Sales Price (ASP) Pricing",
        f"Effective: {effective_year} Q{effective_quarter}",
        f"HCPCS codes batch {batch_num}: {batch[0]['hcpcs']}–{batch[-1]['hcpcs']}",
        "",
        "CMS publishes ASP pricing quarterly for Medicare Part B drugs and "
        "biologicals. The Medicare payment limit for most drugs is ASP + 6% "
        "of the average sales price. These rates apply to drugs administered "
        "in physician offices and outpatient hospital settings under the "
        "'buy and bill' model. J-codes and other HCPCS Level II codes "
        "identify the drug products covered.",
        "",
        "=" * 72,
    ]
    for entry in batch:
        lines.append(f"HCPCS: {entry['hcpcs']}")
        if entry.get("short_description"):
            lines.append(f"  Drug: {entry['short_description']}")
        if entry.get("dosage_form"):
            lines.append(f"  Dosage Form: {entry['dosage_form']}")
        if entry.get("asp_price") is not None:
            lines.append(f"  ASP Price: ${entry['asp_price']:.4f}")
        if entry.get("asp_plus_6") is not None:
            lines.append(f"  Payment Limit (ASP+6%): ${entry['asp_plus_6']:.4f}")
        lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main scraper
# ---------------------------------------------------------------------------

def scrape_cms_asp(
    url: str | None = None,
    effective_year: int | None = None,
    effective_quarter: int | None = None,
) -> Generator[dict[str, Any], None, None]:
    """
    Download and parse CMS ASP quarterly drug pricing file.
    Yields batched asp_drug_pricing documents.
    """
    url = url or os.environ.get("CMS_ASP_URL", CMS_ASP_URL_DEFAULT)
    scraped_at = datetime.utcnow()

    try:
        zf = _download_zip(url)
    except Exception as exc:
        logger.error("Failed to download ASP file: %s", exc)
        return

    csv_path = _find_csv(zf)
    if csv_path is None:
        logger.error("No CSV found in ASP ZIP. Contents: %s", zf.namelist())
        return

    # Derive year/quarter from filename if not provided
    if effective_year is None or effective_quarter is None:
        src_name = csv_path.split("::")[-1]
        ey, eq = _parse_quarter_from_filename(src_name)
        effective_year = effective_year or ey
        effective_quarter = effective_quarter or eq

    logger.info(
        "Parsing ASP CSV: %s (Year=%d Q%d)", csv_path, effective_year, effective_quarter
    )
    rows = _read_csv(zf, csv_path)

    parsed = []
    for row in rows:
        hcpcs = _get_col(
            row, "hcpcs_code", "hcpcs", "code", "billing_code", "j_code"
        )
        if not hcpcs or not re.match(r"^[A-Z0-9]{4,5}", hcpcs):
            continue
        asp_raw = _get_col(row, "asp_unit_cost", "asp_price", "asp", "price_asp")
        plus6_raw = _get_col(
            row,
            "payment_limit", "asp_plus_6", "plus_6_payment",
            "medicare_payment", "payment_rate",
        )
        parsed.append({
            "hcpcs": hcpcs,
            "short_description": _get_col(
                row, "short_description", "description", "drug_name", "drug"
            ),
            "dosage_form": _get_col(row, "dosage_form", "dosage", "form"),
            "asp_price": _to_float(asp_raw),
            "asp_plus_6": _to_float(plus6_raw),
        })

    logger.info("Parsed %d ASP entries", len(parsed))

    batch_num = 0
    for i in range(0, len(parsed), CODES_PER_DOCUMENT):
        batch = parsed[i : i + CODES_PER_DOCUMENT]
        batch_num += 1
        raw_content = _build_asp_document(batch, batch_num, effective_year, effective_quarter)
        content_hash = _sha256(raw_content)
        title = (
            f"ASP Drug Pricing {effective_year} Q{effective_quarter} — "
            f"Batch {batch_num} ({batch[0]['hcpcs']}–{batch[-1]['hcpcs']})"
        )
        yield {
            "source_url": f"{CMS_ASP_BASE_URL}?year={effective_year}&q={effective_quarter}&batch={batch_num}",
            "source_domain": "cms.gov",
            "document_type_hint": "asp_drug_pricing",
            "title": title,
            "raw_content": raw_content,
            "content_hash": content_hash,
            "metadata": {
                "effective_year": effective_year,
                "effective_quarter": effective_quarter,
                "batch_num": batch_num,
                "code_count": len(batch),
                "first_hcpcs": batch[0]["hcpcs"],
                "last_hcpcs": batch[-1]["hcpcs"],
            },
            "scraped_at": scraped_at,
        }

    logger.info("ASP scrape complete: %d documents (%d drug entries)", batch_num, len(parsed))
