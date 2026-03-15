"""
CMS MS-DRG (Medicare Severity Diagnosis Related Group) Scraper
==============================================================
Downloads the CMS MS-DRG Definitions Manual data and generates drg_policy
documents for CODEMED.

MS-DRGs are the payment units for Medicare inpatient hospital stays under
the Inpatient Prospective Payment System (IPPS). Each DRG has a relative
weight — a higher weight means a higher payment. DRGs are updated annually
each October with the new federal fiscal year.

Data source:
  https://www.cms.gov/medicare/medicare-fee-for-service-payment/acuteinpatientpps/ms-drg-classifications-and-software

The DRG data file is typically a text or CSV file with columns:
  MDC, Type, DRG, Title, Weights, GMLOS, AMLOS, Special Pay DRGs

Env var: CMS_DRG_URL  (override for testing with local fixture file)
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

_FY = datetime.utcnow().year  # CMS fiscal year starts Oct 1

CMS_DRG_URL_DEFAULT = (
    "https://www.cms.gov/medicare/medicare-fee-for-service-payment"
    f"/acuteinpatientpps/downloads/fy{_FY}-final-rule-ms-drg-grouper.zip"
)
CMS_DRG_BASE_URL = (
    "https://www.cms.gov/medicare/medicare-fee-for-service-payment"
    "/acuteinpatientpps/ms-drg-classifications-and-software"
)

DRGS_PER_DOCUMENT = 100

# Major Diagnostic Category descriptions
_MDC_LABELS = {
    "01": "Diseases and Disorders of the Nervous System",
    "02": "Diseases and Disorders of the Eye",
    "03": "Diseases and Disorders of the Ear, Nose, Mouth and Throat",
    "04": "Diseases and Disorders of the Respiratory System",
    "05": "Diseases and Disorders of the Circulatory System",
    "06": "Diseases and Disorders of the Digestive System",
    "07": "Diseases and Disorders of the Hepatobiliary System and Pancreas",
    "08": "Diseases and Disorders of the Musculoskeletal System and Connective Tissue",
    "09": "Diseases and Disorders of the Skin, Subcutaneous Tissue and Breast",
    "10": "Endocrine, Nutritional and Metabolic Diseases and Disorders",
    "11": "Diseases and Disorders of the Kidney and Urinary Tract",
    "12": "Diseases and Disorders of the Male Reproductive System",
    "13": "Diseases and Disorders of the Female Reproductive System",
    "14": "Pregnancy, Childbirth and the Puerperium",
    "15": "Newborns and Other Neonates with Conditions Originating in the Perinatal Period",
    "16": "Diseases and Disorders of the Blood and Blood Forming Organs and Immunological Disorders",
    "17": "Myeloproliferative DDs and Poorly Differentiated Neoplasms",
    "18": "Infectious and Parasitic Diseases (Systemic or Unspecified Sites)",
    "19": "Mental Diseases and Disorders",
    "20": "Alcohol/Drug Use and Alcohol/Drug Induced Organic Mental Disorders",
    "21": "Injuries, Poisonings and Toxic Effects of Drugs",
    "22": "Burns",
    "23": "Factors Influencing Health Status and Other Contacts with Health Services",
    "24": "Multiple Significant Trauma",
    "25": "Human Immunodeficiency Virus Infections",
    "PRE": "Pre-MDC",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _download_zip(url: str) -> zipfile.ZipFile:
    import urllib.request
    logger.info("Downloading DRG file from %s ...", url)
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


def _find_data_file(zf: zipfile.ZipFile) -> tuple[str | None, str]:
    """Find DRG data file in ZIP; return (path, format) where format is 'csv' or 'txt'."""
    names = zf.namelist()
    for name in names:
        lower = name.lower()
        if ("drg" in lower or "weight" in lower) and lower.endswith(".csv"):
            return name, "csv"
    for name in names:
        lower = name.lower()
        if ("drg" in lower or "weight" in lower) and lower.endswith(".txt"):
            return name, "txt"
    for name in names:
        if name.lower().endswith(".csv"):
            return name, "csv"
    for name in names:
        if name.lower().endswith(".txt"):
            return name, "txt"
    return None, ""


def _parse_drg_csv(content: str) -> list[dict]:
    """Parse DRG data from CSV format."""
    reader = csv.DictReader(content.lstrip("\ufeff").splitlines())
    rows = []
    for row in reader:
        lrow = {k.lower().strip().replace(" ", "_"): v for k, v in row.items()}

        def get(*keys):
            for k in keys:
                v = lrow.get(k, "").strip()
                if v:
                    return v
            return ""

        drg = get("ms-drg", "drg", "ms_drg", "drg_number", "drg_cd")
        if not drg or not re.match(r"^\d{1,3}$", drg):
            continue
        rows.append({
            "drg": drg.zfill(3),
            "mdc": get("mdc", "major_diagnostic_category", "mdc_code"),
            "drg_type": get("type", "drg_type", "med_surg"),
            "title": get("ms-drg_title", "drg_title", "title", "description"),
            "relative_weight": get("relative_weight", "weight", "rel_weight"),
            "gmlos": get("geometric_mean_length_of_stay", "gmlos", "geo_mean_los", "gm_los"),
            "amlos": get("arithmetic_mean_length_of_stay", "amlos", "arith_mean_los", "am_los"),
        })
    return rows


def _parse_drg_txt(content: str) -> list[dict]:
    """Parse DRG data from fixed-width or pipe-delimited text format."""
    rows = []
    for line in content.splitlines():
        line = line.strip()
        if not line:
            continue
        # Try pipe-delimited
        if "|" in line:
            parts = [p.strip() for p in line.split("|")]
            if len(parts) >= 4 and re.match(r"^\d{1,3}$", parts[0].lstrip("0") or "0"):
                rows.append({
                    "drg": parts[0].zfill(3),
                    "mdc": parts[1] if len(parts) > 1 else "",
                    "drg_type": parts[2] if len(parts) > 2 else "",
                    "title": parts[3] if len(parts) > 3 else "",
                    "relative_weight": parts[4] if len(parts) > 4 else "",
                    "gmlos": parts[5] if len(parts) > 5 else "",
                    "amlos": parts[6] if len(parts) > 6 else "",
                })
        # Try tab-delimited
        elif "\t" in line:
            parts = [p.strip() for p in line.split("\t")]
            if len(parts) >= 4 and re.match(r"^\d{1,3}$", (parts[0] or "").lstrip("0") or "0"):
                rows.append({
                    "drg": parts[0].zfill(3),
                    "mdc": parts[1] if len(parts) > 1 else "",
                    "drg_type": parts[2] if len(parts) > 2 else "",
                    "title": parts[3] if len(parts) > 3 else "",
                    "relative_weight": parts[4] if len(parts) > 4 else "",
                    "gmlos": parts[5] if len(parts) > 5 else "",
                    "amlos": parts[6] if len(parts) > 6 else "",
                })
    return rows


# ---------------------------------------------------------------------------
# Document builder
# ---------------------------------------------------------------------------

def _build_drg_document(
    batch: list[dict],
    batch_num: int,
    fiscal_year: int,
    mdc_group: str | None = None,
) -> str:
    mdc_desc = _MDC_LABELS.get(mdc_group or "", mdc_group or "Multiple MDCs")
    lines = [
        f"CMS MS-DRG Relative Weights — FY {fiscal_year}",
        f"MDC: {mdc_group or 'Mixed'} — {mdc_desc}",
        f"Batch {batch_num}: DRGs {batch[0]['drg']}–{batch[-1]['drg']}",
        "",
        "MS-DRG relative weights are used to calculate Medicare inpatient "
        "hospital payments under IPPS. A higher relative weight indicates "
        "greater resource use and results in higher payment. The base rate "
        "is multiplied by the DRG's relative weight and adjusted for wage "
        "index, teaching status, and disproportionate share. "
        "CC = complication or comorbidity; MCC = major CC.",
        "",
        "=" * 72,
    ]
    for drg in batch:
        mdc_label = _MDC_LABELS.get(drg.get("mdc", ""), drg.get("mdc", ""))
        lines.append(f"MS-DRG: {drg['drg']}")
        if drg.get("title"):
            lines.append(f"  Title: {drg['title']}")
        if mdc_label:
            lines.append(f"  MDC: {drg.get('mdc', '')} — {mdc_label}")
        if drg.get("drg_type"):
            lines.append(f"  Type: {drg['drg_type']}")
        if drg.get("relative_weight"):
            lines.append(f"  Relative Weight: {drg['relative_weight']}")
        if drg.get("gmlos"):
            lines.append(f"  Geometric Mean LOS: {drg['gmlos']} days")
        if drg.get("amlos"):
            lines.append(f"  Arithmetic Mean LOS: {drg['amlos']} days")
        lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main scraper
# ---------------------------------------------------------------------------

def scrape_cms_drg(
    url: str | None = None,
    fiscal_year: int | None = None,
) -> Generator[dict[str, Any], None, None]:
    """
    Download and parse CMS MS-DRG weight data.
    Yields batched drg_policy documents grouped by MDC where possible.
    """
    url = url or os.environ.get("CMS_DRG_URL", CMS_DRG_URL_DEFAULT)
    fiscal_year = fiscal_year or _FY
    scraped_at = datetime.utcnow()

    try:
        zf = _download_zip(url)
    except Exception as exc:
        logger.error("Failed to download DRG file: %s", exc)
        return

    data_file, fmt = _find_data_file(zf)
    if data_file is None:
        logger.error("No DRG data file found in ZIP. Contents: %s", zf.namelist())
        return

    logger.info("Parsing DRG file: %s (format=%s)", data_file, fmt)
    content = zf.read(data_file).decode("utf-8", errors="replace")

    if fmt == "csv":
        drgs = _parse_drg_csv(content)
    else:
        drgs = _parse_drg_txt(content)

    logger.info("Parsed %d DRG entries", len(drgs))

    if not drgs:
        logger.warning("No DRG entries parsed — check file format")
        return

    # Group by MDC for better document coherence
    mdc_groups: dict[str, list[dict]] = {}
    for drg in drgs:
        mdc = drg.get("mdc") or "00"
        mdc_groups.setdefault(mdc, []).append(drg)

    batch_num = 0
    for mdc, mdc_drgs in sorted(mdc_groups.items()):
        for i in range(0, len(mdc_drgs), DRGS_PER_DOCUMENT):
            batch = mdc_drgs[i : i + DRGS_PER_DOCUMENT]
            batch_num += 1
            raw_content = _build_drg_document(batch, batch_num, fiscal_year, mdc)
            content_hash = _sha256(raw_content)
            mdc_label = _MDC_LABELS.get(mdc, mdc)
            title = f"MS-DRG FY{fiscal_year}: MDC {mdc} — {mdc_label} (Batch {batch_num})"
            yield {
                "source_url": f"{CMS_DRG_BASE_URL}?fy={fiscal_year}&mdc={mdc}&batch={batch_num}",
                "source_domain": "cms.gov",
                "document_type_hint": "drg_policy",
                "title": title,
                "raw_content": raw_content,
                "content_hash": content_hash,
                "metadata": {
                    "fiscal_year": fiscal_year,
                    "mdc": mdc,
                    "mdc_label": mdc_label,
                    "batch_num": batch_num,
                    "drg_count": len(batch),
                    "first_drg": batch[0]["drg"],
                    "last_drg": batch[-1]["drg"],
                },
                "scraped_at": scraped_at,
            }

    logger.info("DRG scrape complete: %d documents (%d DRG entries)", batch_num, len(drgs))
