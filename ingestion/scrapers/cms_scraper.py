"""
CMS Medicare Coverage Database (MCD) Scraper
=============================================
Downloads bulk LCD and NCD data from CMS MCD Downloads page.
Uses the official bulk export ZIP files — no JavaScript rendering required.

Data sources:
  - Current LCDs: https://downloads.cms.gov/medicare-coverage-database/downloads/exports/current_lcd.zip
  - Current NCDs: https://downloads.cms.gov/medicare-coverage-database/downloads/exports/ncd.zip
  - All LCDs (incl. retired): https://downloads.cms.gov/medicare-coverage-database/downloads/exports/all_lcd.zip

Output: List of RawDocument dicts ready for deduplication + DB insertion.
"""

import csv
import hashlib
import io
import logging
import re
import sys
import urllib.request
import zipfile
from datetime import datetime
from typing import Any, Generator

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CMS_LCD_URL = "https://downloads.cms.gov/medicare-coverage-database/downloads/exports/current_lcd.zip"
CMS_NCD_URL = "https://downloads.cms.gov/medicare-coverage-database/downloads/exports/ncd.zip"
CMS_LCD_VIEW_BASE = "https://www.cms.gov/medicare-coverage-database/view/lcd.aspx?lcdid={lcd_id}"
CMS_NCD_VIEW_BASE = "https://www.cms.gov/medicare-coverage-database/view/ncd.aspx?ncdid={ncd_id}"

# Increase CSV field size limit for large HTML content fields
csv.field_size_limit(sys.maxsize)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _html_to_text(html: str) -> str:
    """Strip HTML tags and normalise whitespace."""
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator=" ", strip=True)
    # Collapse multiple spaces / newlines
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _sha256(content: str) -> str:
    """Return SHA-256 hex digest of a string."""
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _parse_date(value: str) -> datetime | None:
    """Parse CMS date strings like '2020-07-01 00:00:00'."""
    if not value:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(value.strip(), fmt)
        except ValueError:
            continue
    return None


def _download_zip(url: str) -> zipfile.ZipFile:
    """Download a URL and return as a ZipFile object."""
    logger.info("Downloading %s ...", url)
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "NexusAuth-Ingestion/2.0 (healthcare knowledge pipeline; contact: admin@nexusauth.io)"
        },
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        data = resp.read()
    logger.info("Downloaded %d bytes from %s", len(data), url)
    return zipfile.ZipFile(io.BytesIO(data))


def _open_inner_csv_zip(outer: zipfile.ZipFile, inner_name: str) -> zipfile.ZipFile:
    """Extract and open the inner CSV zip from the outer container zip."""
    inner_data = outer.read(inner_name)
    return zipfile.ZipFile(io.BytesIO(inner_data))


def _read_csv(zf: zipfile.ZipFile, filename: str) -> list[dict]:
    """Read a CSV file from a ZipFile and return list of dicts."""
    content = zf.read(filename).decode("utf-8", errors="replace")
    reader = csv.DictReader(content.splitlines())
    return list(reader)


# ---------------------------------------------------------------------------
# LCD Scraper
# ---------------------------------------------------------------------------

def _build_lcd_text(row: dict, contractor_map: dict[str, str]) -> str:
    """Combine all LCD text fields into a single searchable document."""
    parts = []

    title = row.get("title", "")
    if title:
        parts.append(f"LCD Title: {title}")

    display_id = row.get("display_id", "")
    if display_id:
        parts.append(f"LCD ID: {display_id}")

    contractor_name = contractor_map.get(row.get("lcd_id", ""), "")
    if contractor_name:
        parts.append(f"Contractor: {contractor_name}")

    for field in [
        "indication",
        "cms_cov_policy",
        "diagnoses_support",
        "diagnoses_dont_support",
        "coding_guidelines",
        "doc_reqs",
        "associated_info",
        "summary_of_evidence",
        "analysis_of_evidence",
        "source_info",
        "bibliography",
        "keywords",
        "issue",
    ]:
        raw = row.get(field, "")
        if raw:
            text = _html_to_text(raw)
            if text:
                parts.append(text)

    return "\n\n".join(parts)


def scrape_cms_lcds(url: str = CMS_LCD_URL) -> Generator[dict[str, Any], None, None]:
    """
    Download and parse CMS LCD bulk data.

    Yields raw document dicts with keys matching the `raw_documents` schema:
      source_url, source_domain, document_type_hint, title, raw_content,
      content_hash, metadata, scraped_at
    """
    outer = _download_zip(url)

    # Find the inner CSV zip
    csv_zip_name = next((n for n in outer.namelist() if n.endswith("_csv.zip")), None)
    if not csv_zip_name:
        raise ValueError(f"No inner CSV zip found in {url}")

    inner = _open_inner_csv_zip(outer, csv_zip_name)

    # Load LCD rows
    lcd_rows = _read_csv(inner, "lcd.csv")
    logger.info("Loaded %d LCD rows", len(lcd_rows))

    # Build contractor lookup: lcd_id -> contractor_bus_name
    contractor_map: dict[str, str] = {}
    if "lcd_x_contractor.csv" in inner.namelist() and "contractor.csv" in inner.namelist():
        contractors = {
            r["contractor_id"] + "_" + r["contractor_type_id"] + "_" + r["contractor_version"]: r.get("contractor_bus_name", "")
            for r in _read_csv(inner, "contractor.csv")
        }
        for xref in _read_csv(inner, "lcd_x_contractor.csv"):
            key = xref.get("contractor_id", "") + "_" + xref.get("contractor_type_id", "") + "_" + xref.get("contractor_version", "")
            lcd_id = xref.get("lcd_id", "")
            if lcd_id and key in contractors:
                contractor_map[lcd_id] = contractors[key]

    scraped_at = datetime.utcnow()

    for row in lcd_rows:
        lcd_id = row.get("lcd_id", "")
        display_id = row.get("display_id", "")
        title = row.get("title", "").strip()
        status = row.get("status", "A")

        # Skip retired/inactive unless explicitly requested
        if status not in ("A", "F"):  # A=Active, F=Future effective
            continue

        # Build combined text content
        raw_content = _build_lcd_text(row, contractor_map)
        if not raw_content.strip():
            logger.warning("LCD %s (%s) has no content, skipping", lcd_id, display_id)
            continue

        content_hash = _sha256(raw_content)
        source_url = CMS_LCD_VIEW_BASE.format(lcd_id=lcd_id)

        # Build metadata dict
        metadata = {
            "lcd_id": lcd_id,
            "lcd_version": row.get("lcd_version", ""),
            "display_id": display_id,
            "status": status,
            "orig_det_eff_date": row.get("orig_det_eff_date", ""),
            "rev_eff_date": row.get("rev_eff_date", ""),
            "last_updated": row.get("last_updated", ""),
            "last_reviewed_on": row.get("last_reviewed_on", ""),
            "contractor": contractor_map.get(lcd_id, ""),
            "keywords": row.get("keywords", ""),
            "icd10_doc": row.get("icd10_doc", ""),
            "source_lcd_id": row.get("source_lcd_id", ""),
            "mcd_publish_date": row.get("mcd_publish_date", ""),
        }

        yield {
            "source_url": source_url,
            "source_domain": "cms.gov",
            "document_type_hint": "lcd",
            "title": title,
            "raw_content": raw_content,
            "content_hash": content_hash,
            "metadata": metadata,
            "scraped_at": scraped_at,
        }


# ---------------------------------------------------------------------------
# NCD Scraper
# ---------------------------------------------------------------------------

def _build_ncd_text(row: dict) -> str:
    """Combine all NCD text fields into a single searchable document."""
    parts = []

    title = row.get("NCD_mnl_sect_title", "") or row.get("itm_srvc_desc", "")
    if title:
        parts.append(f"NCD Title: {title}")

    section = row.get("NCD_mnl_sect", "")
    if section:
        parts.append(f"NCD Manual Section: {section}")

    for field in ["indctn_lmtn", "xref_txt", "othr_txt", "ncd_keyword"]:
        raw = row.get(field, "")
        if raw:
            text = _html_to_text(raw)
            if text:
                parts.append(text)

    return "\n\n".join(parts)


def scrape_cms_ncds(url: str = CMS_NCD_URL) -> Generator[dict[str, Any], None, None]:
    """
    Download and parse CMS NCD bulk data.

    Yields raw document dicts with keys matching the `raw_documents` schema.
    """
    outer = _download_zip(url)

    csv_zip_name = next((n for n in outer.namelist() if n.endswith("_csv.zip")), None)
    if not csv_zip_name:
        raise ValueError(f"No inner CSV zip found in {url}")

    inner = _open_inner_csv_zip(outer, csv_zip_name)
    ncd_rows = _read_csv(inner, "ncd_trkg.csv")
    logger.info("Loaded %d NCD rows", len(ncd_rows))

    scraped_at = datetime.utcnow()

    for row in ncd_rows:
        ncd_id = row.get("NCD_id", "")
        title = (row.get("NCD_mnl_sect_title") or row.get("itm_srvc_desc") or "").strip()
        section = row.get("NCD_mnl_sect", "")

        raw_content = _build_ncd_text(row)
        if not raw_content.strip():
            logger.warning("NCD %s has no content, skipping", ncd_id)
            continue

        content_hash = _sha256(raw_content)
        source_url = CMS_NCD_VIEW_BASE.format(ncd_id=ncd_id)

        metadata = {
            "ncd_id": ncd_id,
            "ncd_version": row.get("NCD_vrsn_num", ""),
            "ncd_manual_section": section,
            "natl_cvrg_type": row.get("natl_cvrg_type", ""),
            "cvrg_lvl_cd": row.get("cvrg_lvl_cd", ""),
            "ncd_eff_date": row.get("NCD_efctv_dt", ""),
            "ncd_impltn_dt": row.get("NCD_impltn_dt", ""),
            "ncd_trmntn_dt": row.get("NCD_trmntn_dt", ""),
            "transmittal_num": row.get("trnsmtl_num", ""),
            "transmittal_url": row.get("trnsmtl_url", ""),
            "chg_rqst_num": row.get("chg_rqst_num", ""),
            "under_review": row.get("under_rvw", ""),
            "last_updated": row.get("last_updt_tmstmp", ""),
            "ncd_lab": row.get("NCD_lab", ""),
            "keywords": row.get("ncd_keyword", ""),
        }

        yield {
            "source_url": source_url,
            "source_domain": "cms.gov",
            "document_type_hint": "ncd",
            "title": title,
            "raw_content": raw_content,
            "content_hash": content_hash,
            "metadata": metadata,
            "scraped_at": scraped_at,
        }


# ---------------------------------------------------------------------------
# Unified entry point
# ---------------------------------------------------------------------------

def scrape_all_cms(
    include_lcds: bool = True,
    include_ncds: bool = True,
    lcd_url: str = CMS_LCD_URL,
    ncd_url: str = CMS_NCD_URL,
) -> Generator[dict[str, Any], None, None]:
    """
    Scrape all CMS LCD and NCD documents.

    Yields raw document dicts ready for deduplication and DB insertion.
    """
    if include_lcds:
        logger.info("Starting CMS LCD scrape...")
        count = 0
        for doc in scrape_cms_lcds(lcd_url):
            count += 1
            yield doc
        logger.info("CMS LCD scrape complete: %d documents", count)

    if include_ncds:
        logger.info("Starting CMS NCD scrape...")
        count = 0
        for doc in scrape_cms_ncds(ncd_url):
            count += 1
            yield doc
        logger.info("CMS NCD scrape complete: %d documents", count)


# ---------------------------------------------------------------------------
# CLI test runner
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    print("=== CMS MCD Scraper Test ===\n")

    # Test LCD scrape (first 5)
    print("Testing LCD scrape (first 5 documents):")
    for i, doc in enumerate(scrape_cms_lcds()):
        if i >= 5:
            break
        print(f"\n  [{i+1}] {doc['title']}")
        print(f"       URL: {doc['source_url']}")
        print(f"       Hash: {doc['content_hash'][:16]}...")
        print(f"       Content length: {len(doc['raw_content'])} chars")
        print(f"       Metadata keys: {list(doc['metadata'].keys())}")

    print("\n\nTesting NCD scrape (first 5 documents):")
    for i, doc in enumerate(scrape_cms_ncds()):
        if i >= 5:
            break
        print(f"\n  [{i+1}] {doc['title']}")
        print(f"       URL: {doc['source_url']}")
        print(f"       Hash: {doc['content_hash'][:16]}...")
        print(f"       Section: {doc['metadata'].get('ncd_manual_section', 'N/A')}")
