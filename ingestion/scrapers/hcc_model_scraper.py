"""
CMS-HCC Model V28 — Official Data Scraper
==========================================
Downloads and parses CMS's official V28 ICD-10-to-HCC mapping files and
model coefficient tables so the HCCEngine can use the authoritative full
crosswalk (~7,770 ICD-10 codes, 115 payment HCCs) rather than the built-in
starter set.

Data sources (2026 model year):
  Mappings: https://www.cms.gov/files/zip/2026-initial-icd-10-cm-mappings.zip
            https://www.cms.gov/files/zip/2026-midyear-final-icd-10-mappings.zip
  Software: https://www.cms.gov/files/zip/2026-initial-model-software.zip
            https://www.cms.gov/files/zip/2026-midyear-final-model-software.zip

CMS ZIP internal structure (approximate — filenames vary by year):
  ICD-10 mappings ZIP:
    ├── *ICD10*.txt          — tab-delimited: ICD_CD, HCC_NBR, HIER, HCC_LABEL
    └── *ICD10*.csv          — alternate format (same fields, CSV-delimited)
  Model software ZIP:
    ├── *coefficients*.txt   — segment coefficients: HCC_NBR, SEGMENT, COEFF
    ├── *hierarchies*.txt    — HCC hierarchy rules
    └── *.sas / *.r          — SAS/R model code (skipped)

Usage:
    from ingestion.scrapers.hcc_model_scraper import build_crosswalk_from_cms_data

    crosswalk = build_crosswalk_from_cms_data(
        mappings_url="https://www.cms.gov/files/zip/2026-initial-icd-10-cm-mappings.zip"
    )
    # crosswalk: dict[str, tuple[int, str, Optional[str], float]]
    # Keys are ICD-10 codes; values are (hcc_number, description, group, raf_weight)

    from codemed.hcc_engine import HCCEngine
    engine = HCCEngine.load_from_cms_zip(
        mappings_url="https://www.cms.gov/files/zip/2026-initial-icd-10-cm-mappings.zip"
    )
"""
from __future__ import annotations

import csv
import io
import logging
import re
import urllib.request
import zipfile
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default URLs — 2026 model year (override as new model years are published)
# ---------------------------------------------------------------------------
DEFAULT_MAPPINGS_URL = (
    "https://www.cms.gov/files/zip/2026-initial-icd-10-cm-mappings.zip"
)
DEFAULT_SOFTWARE_URL = (
    "https://www.cms.gov/files/zip/2026-initial-model-software.zip"
)

# Community non-dual aged segment label as it appears in CMS coefficient files.
# Override via parse_coefficient_zip(segment=...) if you need a different segment.
DEFAULT_SEGMENT = "CNA"  # Community Non-dual Aged

_USER_AGENT = (
    "CodeMed-Ingestion/2.0 "
    "(healthcare knowledge pipeline; contact: admin@codemedgroup.com)"
)

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _download_zip(url: str) -> zipfile.ZipFile:
    """Fetch URL and return as ZipFile. Raises urllib.error.URLError on failure."""
    logger.info("Downloading CMS HCC model file: %s", url)
    req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    with urllib.request.urlopen(req, timeout=120) as resp:
        data = resp.read()
    logger.info("Downloaded %d bytes", len(data))
    return zipfile.ZipFile(io.BytesIO(data))


def _open_local_zip(path: str) -> zipfile.ZipFile:
    """Open a locally cached CMS ZIP file."""
    return zipfile.ZipFile(Path(path).expanduser())


def _find_file(zf: zipfile.ZipFile, patterns: list[str]) -> Optional[str]:
    """
    Return the first filename inside zf whose name (lowercased) matches any
    of the given glob-style substrings. Returns None if nothing matches.
    """
    names = zf.namelist()
    for pattern in patterns:
        for name in names:
            if pattern.lower() in name.lower():
                return name
    return None


def _read_text(zf: zipfile.ZipFile, filename: str) -> str:
    return zf.read(filename).decode("utf-8", errors="replace")


def _sniff_delimiter(sample: str) -> str:
    """Detect whether a text file is tab- or comma-delimited."""
    tab_count = sample[:2000].count("\t")
    comma_count = sample[:2000].count(",")
    return "\t" if tab_count >= comma_count else ","


# ---------------------------------------------------------------------------
# Public: parse ICD-10 → HCC mapping file
# ---------------------------------------------------------------------------

def parse_icd10_mapping_zip(
    url: str | None = None,
    local_path: str | None = None,
) -> list[dict[str, Any]]:
    """
    Download (or open) the CMS ICD-10-to-HCC mapping ZIP and parse all rows.

    Returns a list of dicts:
        {
            "icd10_code": "E11.9",
            "hcc_number": 38,
            "hcc_label": "Diabetes with Glycemic, Unspecified, or No Complications",
            "add_hcc_flag": True,    # False = this code is a hierarchy bypass
        }

    Raises:
        FileNotFoundError: if no mapping file is found inside the ZIP.
        urllib.error.URLError: if the download fails.
    """
    zf = _open_local_zip(local_path) if local_path else _download_zip(
        url or DEFAULT_MAPPINGS_URL
    )

    # CMS mapping file name patterns (vary slightly by year)
    mapping_filename = _find_file(zf, [
        "icd10",           # covers "ICD10_to_HCC_Mappings_2026.txt" etc.
        "icd_to_hcc",
        "icd-to-hcc",
        "mapping",
    ])
    if not mapping_filename:
        raise FileNotFoundError(
            f"No ICD-10 mapping file found in ZIP. Contents: {zf.namelist()}"
        )

    logger.info("Parsing mapping file: %s", mapping_filename)
    text = _read_text(zf, mapping_filename)
    delimiter = _sniff_delimiter(text)

    rows = []
    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)

    # CMS uses various column-name conventions across years — normalise them.
    field_aliases = {
        "icd10_code": ["ICD_CD", "ICD10_CD", "ICD10", "ICD_CODE", "DIAG_CODE"],
        "hcc_number": ["HCC_NBR", "HCC_NUM", "HCC", "HCC_NUMBER"],
        "hcc_label":  ["HCC_LABEL", "HCC_DESC", "LABEL", "DESCRIPTION"],
        "add_hcc":    ["ADD_HCC_FLAG", "ADD_HCC", "HIER", "HIERARCHY_FLAG"],
    }

    def _get(row: dict, aliases: list[str], default: str = "") -> str:
        for alias in aliases:
            val = row.get(alias, row.get(alias.lower(), ""))
            if val:
                return val.strip()
        return default

    for row in reader:
        icd = _get(row, field_aliases["icd10_code"])
        hcc_raw = _get(row, field_aliases["hcc_number"])
        if not icd or not hcc_raw:
            continue
        try:
            hcc_num = int(hcc_raw)
        except ValueError:
            continue

        rows.append({
            "icd10_code": icd.upper().replace(" ", ""),
            "hcc_number": hcc_num,
            "hcc_label": _get(row, field_aliases["hcc_label"]),
            "add_hcc_flag": _get(row, field_aliases["add_hcc"], "1") not in ("0", "N", ""),
        })

    logger.info("Parsed %d ICD-10 → HCC mapping rows", len(rows))
    return rows


# ---------------------------------------------------------------------------
# Public: parse coefficient file from model software ZIP
# ---------------------------------------------------------------------------

def parse_coefficient_zip(
    url: str | None = None,
    local_path: str | None = None,
    segment: str = DEFAULT_SEGMENT,
) -> dict[int, float]:
    """
    Download (or open) the CMS model software ZIP and extract RAF coefficients
    for the specified payment segment.

    Args:
        url: URL of the CMS model software ZIP.
        local_path: Path to a locally downloaded software ZIP.
        segment: Payment segment code. Common values:
            "CNA"  — Community Non-dual Aged (most common MA population)
            "CND"  — Community Non-dual Disabled
            "CFA"  — Community Full Benefit Dual Aged
            "CFD"  — Community Full Benefit Dual Disabled
            "INS"  — Institutional
            "NE"   — New Enrollee

    Returns:
        dict mapping hcc_number → raf_coefficient for the given segment.
        Returns an empty dict if the coefficient file cannot be parsed.
    """
    try:
        zf = _open_local_zip(local_path) if local_path else _download_zip(
            url or DEFAULT_SOFTWARE_URL
        )
    except Exception as exc:
        logger.warning("Could not load coefficient ZIP: %s", exc)
        return {}

    coef_filename = _find_file(zf, ["coefficient", "coeff", "factor", "weights"])
    if not coef_filename:
        logger.warning(
            "No coefficient file found in software ZIP. Contents: %s", zf.namelist()
        )
        return {}

    logger.info("Parsing coefficient file: %s (segment=%s)", coef_filename, segment)
    text = _read_text(zf, coef_filename)
    delimiter = _sniff_delimiter(text)

    coefficients: dict[int, float] = {}
    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)

    seg_aliases  = ["SEGMENT", "SEG", "MODEL_SEGMENT", "PAYMENT_SEGMENT"]
    hcc_aliases  = ["HCC_NBR", "HCC_NUM", "HCC", "HCC_NUMBER", "VARIABLE"]
    coef_aliases = ["COEFF", "COEFFICIENT", "ESTIMATE", "PARAMETER_ESTIMATE", "RAF_WEIGHT"]

    def _get(row: dict, aliases: list[str], default: str = "") -> str:
        for alias in aliases:
            val = row.get(alias, row.get(alias.lower(), ""))
            if val:
                return val.strip()
        return default

    for row in reader:
        seg = _get(row, seg_aliases)
        if seg and seg.upper() != segment.upper():
            continue  # filter to requested segment

        hcc_raw  = _get(row, hcc_aliases)
        coef_raw = _get(row, coef_aliases)

        if not hcc_raw or not coef_raw:
            continue

        # HCC variables in CMS files are often prefixed: "HCC001", "HCC038"
        hcc_clean = re.sub(r"[^0-9]", "", hcc_raw)
        if not hcc_clean:
            continue

        try:
            hcc_num = int(hcc_clean)
            coeff   = float(coef_raw)
        except ValueError:
            continue

        coefficients[hcc_num] = coeff

    logger.info(
        "Parsed %d coefficient entries for segment '%s'", len(coefficients), segment
    )
    return coefficients


# ---------------------------------------------------------------------------
# Public: build_crosswalk_from_cms_data (main entry point)
# ---------------------------------------------------------------------------

def build_crosswalk_from_cms_data(
    mappings_url: str | None = None,
    software_url: str | None = None,
    local_path: str | None = None,
    segment: str = DEFAULT_SEGMENT,
    fallback_crosswalk: dict | None = None,
) -> dict[str, tuple[int, str, Optional[str], float]]:
    """
    Build a complete HCCEngine crosswalk dict from official CMS data.

    Combines the ICD-10→HCC mapping file (for code↔HCC links and descriptions)
    with the model software coefficient file (for RAF weights by segment).

    Falls back to `fallback_crosswalk` if the download or parse fails.

    Returns:
        dict[icd10_code → (hcc_number, description, hierarchy_group, raf_weight)]
        where hierarchy_group is populated from known V28 groups.
    """
    # ── Load ICD-10 → HCC rows ─────────────────────────────────────────────
    try:
        mapping_rows = parse_icd10_mapping_zip(
            url=mappings_url, local_path=local_path
        )
    except Exception as exc:
        logger.error(
            "Failed to parse CMS ICD-10 mapping file: %s — using fallback crosswalk",
            exc,
        )
        return fallback_crosswalk or {}

    # ── Load coefficients (optional) ──────────────────────────────────────
    coefficients: dict[int, float] = {}
    if software_url:
        coefficients = parse_coefficient_zip(url=software_url, segment=segment)
    if not coefficients:
        # Fall back to built-in coefficients for the HCCs we know
        logger.info(
            "No CMS coefficient data loaded — using built-in V28 community coefficients"
        )
        if fallback_crosswalk:
            coefficients = {
                hcc_num: raf
                for (hcc_num, _, _, raf) in fallback_crosswalk.values()
            }

    # ── Build crosswalk ────────────────────────────────────────────────────
    # Known V28 hierarchy groups keyed by HCC number
    _HCC_TO_GROUP: dict[int, str] = {
        **{hcc: "CANCER"       for hcc in [17, 18, 19, 20, 21, 22, 23]},
        **{hcc: "DIABETES"     for hcc in [36, 37, 38]},
        **{hcc: "LIVER"        for hcc in [63, 64, 65]},
        **{hcc: "DEMENTIA"     for hcc in [125, 126, 127]},
        **{hcc: "DEPRESSION"   for hcc in [155, 156, 157]},
        **{hcc: "HEART_FAILURE"for hcc in [222, 224, 226]},
        **{hcc: "CKD"          for hcc in [326, 327, 328, 329]},
    }

    crosswalk: dict[str, tuple[int, str, Optional[str], float]] = {}
    skipped = 0
    for row in mapping_rows:
        if not row.get("add_hcc_flag", True):
            # CMS marks some codes as hierarchy-bypass (ADD_HCC_FLAG=0)
            skipped += 1
            continue

        icd   = row["icd10_code"]
        hcc   = row["hcc_number"]
        label = row.get("hcc_label") or f"HCC {hcc}"
        raf   = coefficients.get(hcc, 0.0)
        group = _HCC_TO_GROUP.get(hcc)

        crosswalk[icd] = (hcc, label, group, raf)

    logger.info(
        "Built crosswalk: %d ICD-10 codes (%d skipped as hierarchy-bypass)",
        len(crosswalk), skipped,
    )
    return crosswalk


# ---------------------------------------------------------------------------
# CLI: download and summarise the 2026 CMS mapping file
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import json
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    parser = argparse.ArgumentParser(
        description="Download and summarise CMS V28 HCC model data"
    )
    parser.add_argument(
        "--mappings-url", default=DEFAULT_MAPPINGS_URL,
        help="URL of CMS ICD-10 mappings ZIP"
    )
    parser.add_argument(
        "--software-url", default=None,
        help="URL of CMS model software ZIP (for coefficients)"
    )
    parser.add_argument(
        "--local", default=None,
        help="Path to locally downloaded mappings ZIP"
    )
    parser.add_argument(
        "--segment", default=DEFAULT_SEGMENT,
        help="Payment segment (default: CNA = community non-dual aged)"
    )
    args = parser.parse_args()

    crosswalk = build_crosswalk_from_cms_data(
        mappings_url=args.mappings_url,
        software_url=args.software_url,
        local_path=args.local,
        segment=args.segment,
    )

    hcc_counts: dict[int, int] = {}
    for hcc_num, _, _, _ in crosswalk.values():
        hcc_counts[hcc_num] = hcc_counts.get(hcc_num, 0) + 1

    print(f"\n{'=' * 60}")
    print(f"Total ICD-10 codes: {len(crosswalk)}")
    print(f"Unique HCCs:        {len(hcc_counts)}")
    print(f"\nTop 20 HCCs by code count:")
    for hcc, count in sorted(hcc_counts.items(), key=lambda x: -x[1])[:20]:
        print(f"  HCC {hcc:4d}: {count:5d} codes")
