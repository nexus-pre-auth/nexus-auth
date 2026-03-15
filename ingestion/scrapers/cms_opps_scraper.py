"""
CMS Outpatient Prospective Payment System (OPPS) / APC Scraper
==============================================================
Downloads the OPPS Addendum A and Addendum B files from CMS and generates
opps_apc documents for CODEMED.

Addendum A: Lists all Ambulatory Payment Classification (APC) groups with
  their payment rates and status indicators.
Addendum B: Maps each HCPCS/CPT code to its APC, status indicator, and
  payment rate for outpatient hospital services.

Data source:
  https://www.cms.gov/medicare/medicare-fee-for-service-payment/hospitaloutpatientpps/addenda-and-updates

Env var: CMS_OPPS_URL  (point to the current year's OPPS addenda ZIP)
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

CMS_OPPS_URL_DEFAULT = (
    "https://www.cms.gov/medicare/medicare-fee-for-service-payment"
    "/hospitaloutpatientpps/downloads/opps-addendum-a-and-b.zip"
)
CMS_OPPS_BASE_URL = (
    "https://www.cms.gov/medicare/medicare-fee-for-service-payment/hospitaloutpatientpps"
)

CODES_PER_DOCUMENT = 300

# APC Status Indicator descriptions
_STATUS_INDICATORS = {
    "A": "Non-covered item/service (not paid under OPPS)",
    "B": "Codes not recognized by OPPS when submitted on outpatient claim",
    "C": "Inpatient procedures (not paid under OPPS)",
    "D": "Discontinued codes",
    "E": "Items/services for which pricing information is not available",
    "F": "Corneal tissue acquisition",
    "G": "Pass-through drugs and biologicals",
    "H": "Pass-through device categories",
    "J1": "Hospital Part B services paid under a comprehensive APC",
    "J2": "Hospital Part B services that may be paid under a comprehensive APC",
    "K": "Non-pass-through drugs, biologicals, and therapeutic radiopharmaceuticals",
    "L": "Influenza vaccine, pneumococcal pneumonia vaccine",
    "M": "Items and services not billable to Medicare fiscal intermediary",
    "N": "Items and services packaged into APC rates",
    "P": "Partial hospitalization",
    "Q1": "STV-packaged codes",
    "Q2": "T-packaged codes",
    "Q3": "Codes that may be paid through a composite APC",
    "Q4": "Conditionally packaged laboratory tests",
    "R": "Blood and blood products",
    "S": "Procedure or service, not discounted when multiple",
    "T": "Procedure or service, multiple procedure discounting applies",
    "U": "Brachytherapy sources",
    "V": "Clinic or emergency department visit",
    "Y": "Non-implantable DME",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _download_zip(url: str) -> zipfile.ZipFile:
    import urllib.request
    logger.info("Downloading OPPS file from %s ...", url)
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "NexusAuth-Ingestion/2.0 (healthcare knowledge pipeline)"},
    )
    with urllib.request.urlopen(req, timeout=180) as resp:
        data = resp.read()
    logger.info("Downloaded %d bytes", len(data))
    return zipfile.ZipFile(io.BytesIO(data))


def _sha256(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _find_csv(zf: zipfile.ZipFile, keyword: str) -> str | None:
    for name in zf.namelist():
        if keyword.lower() in name.lower() and name.lower().endswith(".csv"):
            return name
    for name in zf.namelist():
        if name.lower().endswith(".csv"):
            return name
    return None


def _read_csv(zf: zipfile.ZipFile, path: str) -> list[dict]:
    content = zf.read(path).decode("utf-8", errors="replace").lstrip("\ufeff")
    return list(csv.DictReader(content.splitlines()))


def _norm_headers(headers: list[str]) -> dict[str, str]:
    """Return canonical→actual column mapping."""
    lh = {h.lower().strip().replace(" ", "_"): h for h in (headers or [])}
    mapping = {}
    searches = {
        "apc": ["apc", "apc_number", "apc_code"],
        "description": ["apc_description", "description", "apc_title", "descriptor"],
        "payment_rate": ["payment_rate", "apc_payment", "payment", "si_payment"],
        "status_indicator": ["status_indicator", "status", "si"],
        "hcpcs": ["hcpcs_code", "hcpcs", "code", "procedure_code"],
        "min_unadjusted": ["minimum_unadjusted_copayment", "min_copay", "copayment"],
    }
    for canonical, aliases in searches.items():
        for alias in aliases:
            if alias in lh:
                mapping[canonical] = lh[alias]
                break
    return mapping


def _get(row: dict, mapping: dict, key: str) -> str:
    col = mapping.get(key)
    return ((row.get(col) or "") if col else "").strip()


# ---------------------------------------------------------------------------
# Document builders
# ---------------------------------------------------------------------------

def _build_addendum_a_document(
    batch: list[dict],
    batch_num: int,
    payment_year: int,
) -> str:
    lines = [
        f"OPPS Addendum A — APC Payment Rates  (Payment Year {payment_year})",
        f"Batch {batch_num}: APC groups included",
        "",
        "Addendum A lists each Ambulatory Payment Classification (APC) group "
        "with its payment rate, status indicator, and minimum unadjusted "
        "copayment. APCs are the payment units under the Outpatient Prospective "
        "Payment System (OPPS) for hospital outpatient services.",
        "",
        "=" * 72,
    ]
    for entry in batch:
        si = entry.get("status_indicator", "")
        si_desc = _STATUS_INDICATORS.get(si, si)
        lines.append(f"APC: {entry.get('apc', 'N/A')}")
        if entry.get("description"):
            lines.append(f"  Description: {entry['description']}")
        lines.append(f"  Status Indicator: {si} — {si_desc}")
        if entry.get("payment_rate"):
            lines.append(f"  Payment Rate: ${entry['payment_rate']}")
        if entry.get("min_unadjusted"):
            lines.append(f"  Min Unadjusted Copayment: ${entry['min_unadjusted']}")
        lines.append("")
    return "\n".join(lines)


def _build_addendum_b_document(
    batch: list[dict],
    batch_num: int,
    payment_year: int,
) -> str:
    lines = [
        f"OPPS Addendum B — HCPCS Code APC Assignments  (Payment Year {payment_year})",
        f"Batch {batch_num}: HCPCS {batch[0].get('hcpcs', '')}–{batch[-1].get('hcpcs', '')}",
        "",
        "Addendum B maps each HCPCS/CPT procedure code to its APC group, "
        "status indicator, and payment rate under the Outpatient PPS. "
        "Status indicators determine whether a code is paid separately, "
        "packaged into another APC, or not covered under OPPS.",
        "",
        "=" * 72,
    ]
    for entry in batch:
        si = entry.get("status_indicator", "")
        si_desc = _STATUS_INDICATORS.get(si, si)
        lines.append(f"HCPCS: {entry.get('hcpcs', 'N/A')}")
        if entry.get("description"):
            lines.append(f"  Description: {entry['description']}")
        if entry.get("apc"):
            lines.append(f"  APC: {entry['apc']}")
        lines.append(f"  Status Indicator: {si} — {si_desc}")
        if entry.get("payment_rate"):
            lines.append(f"  Payment Rate: ${entry['payment_rate']}")
        lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main scrapers
# ---------------------------------------------------------------------------

def scrape_cms_opps(
    url: str | None = None,
    payment_year: int | None = None,
) -> Generator[dict[str, Any], None, None]:
    """
    Download and parse OPPS Addendum A and B files.
    Yields batched opps_apc documents.
    """
    url = url or os.environ.get("CMS_OPPS_URL", CMS_OPPS_URL_DEFAULT)
    payment_year = payment_year or datetime.utcnow().year
    scraped_at = datetime.utcnow()

    try:
        zf = _download_zip(url)
    except Exception as exc:
        logger.error("Failed to download OPPS file: %s", exc)
        return

    logger.info("OPPS ZIP contents: %s", zf.namelist())

    # --- Addendum A ---
    csv_a = _find_csv(zf, "addendum_a") or _find_csv(zf, "adda") or _find_csv(zf, "apc")
    if csv_a:
        logger.info("Parsing OPPS Addendum A: %s", csv_a)
        rows = _read_csv(zf, csv_a)
        col_map = _norm_headers(rows[0].keys() if rows else [])
        parsed = []
        for row in rows:
            apc = _get(row, col_map, "apc")
            if not apc:
                continue
            parsed.append({
                "apc": apc,
                "description": _get(row, col_map, "description"),
                "status_indicator": _get(row, col_map, "status_indicator"),
                "payment_rate": _get(row, col_map, "payment_rate"),
                "min_unadjusted": _get(row, col_map, "min_unadjusted"),
            })

        logger.info("Parsed %d Addendum A APC entries", len(parsed))
        batch_num = 0
        for i in range(0, len(parsed), CODES_PER_DOCUMENT):
            batch = parsed[i : i + CODES_PER_DOCUMENT]
            batch_num += 1
            raw_content = _build_addendum_a_document(batch, batch_num, payment_year)
            title = f"OPPS Addendum A {payment_year}: APC Rates Batch {batch_num}"
            yield {
                "source_url": f"{CMS_OPPS_BASE_URL}?addendum=A&year={payment_year}&batch={batch_num}",
                "source_domain": "cms.gov",
                "document_type_hint": "opps_apc",
                "title": title,
                "raw_content": raw_content,
                "content_hash": _sha256(raw_content),
                "metadata": {
                    "addendum": "A",
                    "payment_year": payment_year,
                    "batch_num": batch_num,
                    "apc_count": len(batch),
                },
                "scraped_at": scraped_at,
            }
        logger.info("OPPS Addendum A: %d documents", batch_num)

    # --- Addendum B ---
    csv_b = _find_csv(zf, "addendum_b") or _find_csv(zf, "addb")
    if csv_b and csv_b != csv_a:
        logger.info("Parsing OPPS Addendum B: %s", csv_b)
        rows = _read_csv(zf, csv_b)
        col_map = _norm_headers(rows[0].keys() if rows else [])
        parsed = []
        for row in rows:
            hcpcs = _get(row, col_map, "hcpcs")
            if not hcpcs or not re.match(r"^[0-9A-Z]{4,5}", hcpcs):
                continue
            parsed.append({
                "hcpcs": hcpcs,
                "description": _get(row, col_map, "description"),
                "apc": _get(row, col_map, "apc"),
                "status_indicator": _get(row, col_map, "status_indicator"),
                "payment_rate": _get(row, col_map, "payment_rate"),
            })

        logger.info("Parsed %d Addendum B HCPCS entries", len(parsed))
        batch_num = 0
        for i in range(0, len(parsed), CODES_PER_DOCUMENT):
            batch = parsed[i : i + CODES_PER_DOCUMENT]
            batch_num += 1
            raw_content = _build_addendum_b_document(batch, batch_num, payment_year)
            title = (
                f"OPPS Addendum B {payment_year}: HCPCS-APC Mapping "
                f"{batch[0]['hcpcs']}–{batch[-1]['hcpcs']}"
            )
            yield {
                "source_url": f"{CMS_OPPS_BASE_URL}?addendum=B&year={payment_year}&batch={batch_num}",
                "source_domain": "cms.gov",
                "document_type_hint": "opps_apc",
                "title": title,
                "raw_content": raw_content,
                "content_hash": _sha256(raw_content),
                "metadata": {
                    "addendum": "B",
                    "payment_year": payment_year,
                    "batch_num": batch_num,
                    "code_count": len(batch),
                    "first_hcpcs": batch[0]["hcpcs"],
                    "last_hcpcs": batch[-1]["hcpcs"],
                },
                "scraped_at": scraped_at,
            }
        logger.info("OPPS Addendum B: %d documents", batch_num)

    if not csv_a and not csv_b:
        logger.warning(
            "No Addendum A or B CSV found in OPPS ZIP. Contents: %s", zf.namelist()
        )
