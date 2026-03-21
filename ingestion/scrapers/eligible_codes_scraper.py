"""
CodeMed AI — CMS MA Risk Adjustment Eligible CPT/HCPCS Codes Scraper
======================================================================
Downloads and parses the annual CMS Medicare Advantage risk-adjustment
eligible CPT/HCPCS code list.

CMS publishes this as a ZIP containing a tab-delimited or CSV file listing
every CPT and HCPCS Level II code that qualifies as a face-to-face encounter
for risk-adjustment data validation (RADV) and HCC capture purposes.

Official source (update URL each model year):
  https://www.cms.gov/files/zip/2026-medicare-advantage-risk-adjustment-eligible-cpt-hcpcs-codes.zip

Usage:
    from ingestion.scrapers.eligible_codes_scraper import (
        fetch_eligible_codes,
        load_eligible_codes_from_file,
        EligibleCode,
    )

    codes = fetch_eligible_codes()          # downloads from CMS
    codes = load_eligible_codes_from_file("/path/to/local.zip")  # offline

    # Lookup
    scraper = EligibleCodesScraper()
    scraper.load()
    print(scraper.is_eligible("99213"))     # True
    print(scraper.get("99213"))             # EligibleCode(...)
"""
from __future__ import annotations

import csv
import io
import logging
import os
import zipfile
from dataclasses import dataclass, field
from typing import Optional
from urllib.request import Request as UrlRequest, urlopen

logger = logging.getLogger(__name__)

# Default CMS download URL — override via CMS_ELIGIBLE_CODES_URL env var
DEFAULT_URL = (
    "https://www.cms.gov/files/zip/2026-medicare-advantage-risk-adjustment"
    "-eligible-cpt-hcpcs-codes.zip"
)

# Column name aliases across CMS model years
_CODE_ALIASES = ("CPT_HCPCS_CD", "CPT_CD", "HCPCS_CD", "CODE", "CPT/HCPCS")
_DESC_ALIASES = ("LONG_DESCRIPTION", "SHORT_DESCRIPTION", "DESCRIPTION", "DESC")
_TYPE_ALIASES = ("CODE_TYPE", "TYPE", "CPT_HCPCS_TYPE")
_CATEGORY_ALIASES = ("CATEGORY", "SERVICE_CATEGORY", "PLACE_OF_SERVICE", "POS_TYPE")


@dataclass
class EligibleCode:
    """A single CMS MA risk-adjustment eligible procedure code."""

    code: str
    description: str
    code_type: str          # "CPT" or "HCPCS"
    category: str = ""      # e.g. "Office Visit", "Telehealth", "Preventive"
    extra: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        d = {
            "code": self.code,
            "description": self.description,
            "code_type": self.code_type,
            "category": self.category,
        }
        if self.extra:
            d.update(self.extra)
        return d


class EligibleCodesScraper:
    """
    Downloads and indexes CMS MA risk-adjustment eligible CPT/HCPCS codes.

    Attributes:
        url:        CMS ZIP download URL.
        _codes:     Mapping of code → EligibleCode after load().
    """

    def __init__(self, url: Optional[str] = None) -> None:
        self.url: str = url or os.environ.get("CMS_ELIGIBLE_CODES_URL", DEFAULT_URL)
        self._codes: dict[str, EligibleCode] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load(self, local_path: Optional[str] = None) -> int:
        """
        Download (or read from *local_path*) and parse the eligible codes ZIP.

        Returns the number of codes loaded.
        Raises RuntimeError if the ZIP cannot be parsed.
        """
        if local_path:
            self._codes = load_eligible_codes_from_file(local_path)
        else:
            self._codes = fetch_eligible_codes(self.url)
        logger.info("Loaded %d eligible CPT/HCPCS codes", len(self._codes))
        return len(self._codes)

    def is_eligible(self, code: str) -> bool:
        """Return True if *code* appears in the CMS eligible code list."""
        return code.strip().upper() in self._codes

    def get(self, code: str) -> Optional[EligibleCode]:
        """Return the EligibleCode for *code*, or None if not found."""
        return self._codes.get(code.strip().upper())

    def all_codes(self) -> list[EligibleCode]:
        """Return all eligible codes as a list."""
        return list(self._codes.values())

    def filter_by_type(self, code_type: str) -> list[EligibleCode]:
        """Filter by code_type ('CPT' or 'HCPCS')."""
        t = code_type.upper()
        return [c for c in self._codes.values() if c.code_type.upper() == t]

    def filter_by_category(self, category: str) -> list[EligibleCode]:
        """Filter by service category (case-insensitive substring match)."""
        cat = category.lower()
        return [c for c in self._codes.values() if cat in c.category.lower()]

    def __len__(self) -> int:
        return len(self._codes)


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

def fetch_eligible_codes(url: Optional[str] = None) -> dict[str, EligibleCode]:
    """
    Download the CMS eligible codes ZIP from *url* and return a code → EligibleCode dict.

    Args:
        url: Override the default CMS download URL.

    Returns:
        dict mapping uppercase code → EligibleCode.

    Raises:
        RuntimeError: if download or parse fails.
    """
    target_url = url or os.environ.get("CMS_ELIGIBLE_CODES_URL", DEFAULT_URL)
    logger.info("Downloading eligible codes from %s", target_url)
    try:
        req = UrlRequest(
            target_url,
            headers={"User-Agent": "CodeMed-Ingestion/2.0 (admin@codemedgroup.com)"},
        )
        with urlopen(req, timeout=120) as resp:
            raw = resp.read()
    except Exception as exc:
        raise RuntimeError(f"Failed to download eligible codes ZIP: {exc}") from exc

    return _parse_zip_bytes(raw)


def load_eligible_codes_from_file(path: str) -> dict[str, EligibleCode]:
    """
    Parse a locally saved eligible codes ZIP file.

    Args:
        path: Filesystem path to the CMS ZIP file.

    Returns:
        dict mapping uppercase code → EligibleCode.
    """
    with open(path, "rb") as fh:
        raw = fh.read()
    return _parse_zip_bytes(raw)


# ---------------------------------------------------------------------------
# Internal parsing
# ---------------------------------------------------------------------------

def _parse_zip_bytes(raw: bytes) -> dict[str, EligibleCode]:
    """Extract and parse the eligible codes CSV/TSV from raw ZIP bytes."""
    try:
        outer = zipfile.ZipFile(io.BytesIO(raw))
    except zipfile.BadZipFile as exc:
        raise RuntimeError(f"Downloaded file is not a valid ZIP: {exc}") from exc

    # CMS sometimes wraps the CSV in a nested ZIP — find the data file
    data_file = _find_data_file(outer)
    if data_file is None:
        raise RuntimeError(
            f"Could not find CPT/HCPCS data file in ZIP. Contents: {outer.namelist()}"
        )

    raw_content = outer.read(data_file)

    # Handle nested ZIPs
    if data_file.lower().endswith(".zip"):
        inner = zipfile.ZipFile(io.BytesIO(raw_content))
        inner_csv = _find_data_file(inner)
        if inner_csv is None:
            raise RuntimeError(
                f"Could not find data file inside nested ZIP. Contents: {inner.namelist()}"
            )
        raw_content = inner.read(inner_csv)

    return _parse_csv_bytes(raw_content)


def _find_data_file(zf: zipfile.ZipFile) -> Optional[str]:
    """Return the name of the first CSV/TSV/XLSX/ZIP data file in *zf*."""
    preferred_extensions = (".csv", ".tsv", ".txt", ".zip")
    names = zf.namelist()
    # Prefer files with "cpt" or "hcpcs" in name
    for name in names:
        lower = name.lower()
        if any(lower.endswith(ext) for ext in preferred_extensions):
            if "cpt" in lower or "hcpcs" in lower or "eligible" in lower or "code" in lower:
                return name
    # Fall back to first file with a data extension
    for name in names:
        lower = name.lower()
        if any(lower.endswith(ext) for ext in preferred_extensions):
            return name
    return None


def _parse_csv_bytes(raw: bytes) -> dict[str, EligibleCode]:
    """Parse tab- or comma-delimited bytes into EligibleCode records."""
    # Decode — CMS files are typically latin-1 or utf-8-sig
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            text = raw.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    else:
        text = raw.decode("latin-1", errors="replace")

    # Sniff delimiter
    sniffer = csv.Sniffer()
    sample = text[:4096]
    try:
        dialect = sniffer.sniff(sample, delimiters="\t,|")
    except csv.Error:
        dialect = csv.excel  # type: ignore[assignment]

    reader = csv.DictReader(io.StringIO(text), dialect=dialect)
    if reader.fieldnames is None:
        raise RuntimeError("CSV has no header row")

    headers = [h.strip().upper() for h in reader.fieldnames]
    code_col = _resolve_alias(headers, _CODE_ALIASES)
    desc_col = _resolve_alias(headers, _DESC_ALIASES)
    type_col = _resolve_alias(headers, _TYPE_ALIASES)
    cat_col = _resolve_alias(headers, _CATEGORY_ALIASES)

    if code_col is None:
        raise RuntimeError(
            f"Cannot find code column. Available columns: {headers}. "
            f"Expected one of: {_CODE_ALIASES}"
        )

    codes: dict[str, EligibleCode] = {}
    known_cols = {code_col, desc_col, type_col, cat_col} - {None}

    for row in reader:
        # Normalise keys
        norm = {k.strip().upper(): (v or "").strip() for k, v in row.items() if k}
        raw_code = norm.get(code_col, "").upper()
        if not raw_code:
            continue

        description = norm.get(desc_col, "") if desc_col else ""
        raw_type = norm.get(type_col, "") if type_col else ""
        category = norm.get(cat_col, "") if cat_col else ""

        # Infer code type from the code itself if column missing
        if not raw_type:
            raw_type = _infer_code_type(raw_code)

        extra = {
            k: v for k, v in norm.items()
            if k not in known_cols and v
        }

        codes[raw_code] = EligibleCode(
            code=raw_code,
            description=description,
            code_type=raw_type.upper(),
            category=category,
            extra=extra,
        )

    logger.debug("Parsed %d eligible codes from CSV", len(codes))
    return codes


def _resolve_alias(headers: list[str], aliases: tuple[str, ...]) -> Optional[str]:
    """Return the first alias that appears in *headers*, else None."""
    for alias in aliases:
        if alias in headers:
            return alias
    return None


def _infer_code_type(code: str) -> str:
    """
    Heuristically classify a code as CPT or HCPCS.

    CPT codes are 5-digit numeric (e.g. 99213).
    HCPCS Level II codes start with a letter (e.g. G0438, A0428).
    """
    if code and code[0].isalpha():
        return "HCPCS"
    return "CPT"
