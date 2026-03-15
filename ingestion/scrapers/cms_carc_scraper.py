"""
CMS CARC / RARC Remittance Code Scraper
========================================
Scrapes Claim Adjustment Reason Codes (CARC) and Remittance Advice Remark
Codes (RARC) and generates remittance_policy documents for CODEMED.

These codes appear on Medicare and commercial payer Electronic Remittance
Advice (ERA / 835 transaction) to explain why a claim was adjusted or denied.
Understanding these codes is essential for revenue cycle management.

Data sources:
  RARC (CMS HTML page):
    https://www.cms.gov/medicare/claim-based-resources/remittance-advice-remark-codes
  CARC (X12 HTML page — Washington Publishing Company maintains):
    https://x12.org/codes/claim-adjustment-reason-codes

Env vars:
  CMS_RARC_URL   — override RARC source URL
  CMS_CARC_URL   — override CARC source URL
"""

import hashlib
import logging
import os
import re
import time
import urllib.request
from datetime import datetime
from typing import Any, Generator

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CMS_RARC_URL_DEFAULT = (
    "https://www.cms.gov/medicare/claim-based-resources/remittance-advice-remark-codes"
)
CMS_CARC_URL_DEFAULT = (
    "https://x12.org/codes/claim-adjustment-reason-codes"
)

CODES_PER_DOCUMENT = 50
REQUEST_DELAY = 1.5  # seconds between requests


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sha256(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _fetch_html(url: str) -> str:
    logger.info("Fetching %s ...", url)
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "NexusAuth-Ingestion/2.0 (healthcare knowledge pipeline)"},
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        data = resp.read()
    encoding = resp.headers.get_content_charset() or "utf-8"
    return data.decode(encoding, errors="replace")


def _parse_code_table(html: str) -> list[dict]:
    """
    Parse an HTML page containing a table of codes, descriptions, and notes.
    Returns list of {code, description, notes, group} dicts.
    """
    soup = BeautifulSoup(html, "html.parser")
    results = []

    # Try to find a table
    for table in soup.find_all("table"):
        headers = [
            th.get_text(strip=True).lower()
            for th in table.find_all("th")
        ]
        if not headers:
            # Try first row as header
            first_row = table.find("tr")
            if first_row:
                headers = [
                    td.get_text(strip=True).lower()
                    for td in first_row.find_all(["td", "th"])
                ]

        # Detect column positions
        def col_idx(*names):
            for name in names:
                for i, h in enumerate(headers):
                    if name in h:
                        return i
            return -1

        code_col = col_idx("code", "carc", "rarc", "number")
        desc_col = col_idx("description", "short description", "desc", "narrative")
        notes_col = col_idx("notes", "note", "usage", "remarks")
        group_col = col_idx("group", "category", "type")

        if code_col == -1 or desc_col == -1:
            continue  # Not the right table

        for row in table.find_all("tr")[1:]:  # Skip header row
            cells = row.find_all(["td", "th"])
            if len(cells) <= max(code_col, desc_col):
                continue

            code = cells[code_col].get_text(strip=True)
            if not code or not re.match(r"^[A-Z0-9][A-Z0-9\-]{0,5}$", code):
                continue

            desc = cells[desc_col].get_text(strip=True) if desc_col < len(cells) else ""
            notes = cells[notes_col].get_text(strip=True) if notes_col >= 0 and notes_col < len(cells) else ""
            group = cells[group_col].get_text(strip=True) if group_col >= 0 and group_col < len(cells) else ""

            results.append({
                "code": code,
                "description": desc,
                "notes": notes,
                "group": group,
            })

    # If table parsing failed, try definition lists or structured paragraphs
    if not results:
        results = _parse_dl_codes(soup)

    return results


def _parse_dl_codes(soup: BeautifulSoup) -> list[dict]:
    """Fallback: parse code definitions from <dl> or structured paragraph tags."""
    results = []
    for dl in soup.find_all("dl"):
        dt_list = dl.find_all("dt")
        dd_list = dl.find_all("dd")
        for dt, dd in zip(dt_list, dd_list):
            code = dt.get_text(strip=True)
            if not code or len(code) > 10:
                continue
            results.append({
                "code": code,
                "description": dd.get_text(strip=True),
                "notes": "",
                "group": "",
            })
    return results


# ---------------------------------------------------------------------------
# Document builders
# ---------------------------------------------------------------------------

def _build_remittance_document(
    batch: list[dict],
    code_type: str,
    batch_num: int,
    group_label: str,
) -> str:
    type_descriptions = {
        "CARC": (
            "Claim Adjustment Reason Codes (CARCs) are used in electronic "
            "remittance advices (ERA/835) to identify the reason a claim or "
            "service line was paid differently than billed. Group codes precede "
            "CARCs: CO=Contractual Obligation, PR=Patient Responsibility, "
            "OA=Other Adjustment, PI=Payer Initiated Reduction."
        ),
        "RARC": (
            "Remittance Advice Remark Codes (RARCs) provide additional "
            "information to supplement Claim Adjustment Reason Codes. They "
            "appear in the 835 ERA transaction to clarify payment decisions. "
            "RARC codes starting with 'N' are 'Supplemental' codes."
        ),
    }
    lines = [
        f"{code_type} Codes — {group_label}",
        f"Batch {batch_num}: Codes {batch[0]['code']}–{batch[-1]['code']}",
        "",
        type_descriptions.get(code_type, f"{code_type} denial and adjustment codes."),
        "",
        "=" * 72,
    ]
    for entry in batch:
        lines.append(f"Code: {entry['code']}")
        if entry.get("group"):
            lines.append(f"  Group: {entry['group']}")
        if entry.get("description"):
            lines.append(f"  Description: {entry['description']}")
        if entry.get("notes"):
            lines.append(f"  Notes: {entry['notes']}")
        lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main scrapers
# ---------------------------------------------------------------------------

def scrape_cms_rarc(
    url: str | None = None,
) -> Generator[dict[str, Any], None, None]:
    """Scrape CMS RARC codes page and yield remittance_policy documents."""
    url = url or os.environ.get("CMS_RARC_URL", CMS_RARC_URL_DEFAULT)
    scraped_at = datetime.utcnow()

    try:
        html = _fetch_html(url)
    except Exception as exc:
        logger.error("Failed to fetch RARC page: %s", exc)
        return

    codes = _parse_code_table(html)
    logger.info("Parsed %d RARC codes from %s", len(codes), url)

    if not codes:
        # Yield a single document with the raw text content
        soup = BeautifulSoup(html, "html.parser")
        raw_text = soup.get_text(separator="\n", strip=True)
        if raw_text:
            yield {
                "source_url": url,
                "source_domain": "cms.gov",
                "document_type_hint": "remittance_policy",
                "title": "CMS Remittance Advice Remark Codes (RARC) — Full Reference",
                "raw_content": f"CMS Remittance Advice Remark Codes\n\n{raw_text}",
                "content_hash": _sha256(raw_text),
                "metadata": {"code_type": "RARC", "source": "cms.gov"},
                "scraped_at": scraped_at,
            }
        return

    batch_num = 0
    for i in range(0, len(codes), CODES_PER_DOCUMENT):
        batch = codes[i : i + CODES_PER_DOCUMENT]
        batch_num += 1
        raw_content = _build_remittance_document(batch, "RARC", batch_num, "CMS RARC")
        title = f"RARC Codes Batch {batch_num}: {batch[0]['code']}–{batch[-1]['code']}"
        yield {
            "source_url": f"{url}#batch-{batch_num}",
            "source_domain": "cms.gov",
            "document_type_hint": "remittance_policy",
            "title": title,
            "raw_content": raw_content,
            "content_hash": _sha256(raw_content),
            "metadata": {
                "code_type": "RARC",
                "batch_num": batch_num,
                "code_count": len(batch),
                "first_code": batch[0]["code"],
                "last_code": batch[-1]["code"],
            },
            "scraped_at": scraped_at,
        }

    logger.info("RARC scrape complete: %d documents", batch_num)


def scrape_carc(
    url: str | None = None,
) -> Generator[dict[str, Any], None, None]:
    """Scrape CARC codes page and yield remittance_policy documents."""
    url = url or os.environ.get("CMS_CARC_URL", CMS_CARC_URL_DEFAULT)
    scraped_at = datetime.utcnow()

    try:
        time.sleep(REQUEST_DELAY)
        html = _fetch_html(url)
    except Exception as exc:
        logger.error("Failed to fetch CARC page: %s", exc)
        return

    codes = _parse_code_table(html)
    logger.info("Parsed %d CARC codes from %s", len(codes), url)

    if not codes:
        soup = BeautifulSoup(html, "html.parser")
        raw_text = soup.get_text(separator="\n", strip=True)
        if raw_text:
            yield {
                "source_url": url,
                "source_domain": "x12.org",
                "document_type_hint": "remittance_policy",
                "title": "Claim Adjustment Reason Codes (CARC) — Full Reference",
                "raw_content": f"Claim Adjustment Reason Codes (CARC)\n\n{raw_text}",
                "content_hash": _sha256(raw_text),
                "metadata": {"code_type": "CARC", "source": "x12.org"},
                "scraped_at": scraped_at,
            }
        return

    batch_num = 0
    for i in range(0, len(codes), CODES_PER_DOCUMENT):
        batch = codes[i : i + CODES_PER_DOCUMENT]
        batch_num += 1
        raw_content = _build_remittance_document(batch, "CARC", batch_num, "X12 CARC")
        title = f"CARC Codes Batch {batch_num}: {batch[0]['code']}–{batch[-1]['code']}"
        yield {
            "source_url": f"{url}#batch-{batch_num}",
            "source_domain": "x12.org",
            "document_type_hint": "remittance_policy",
            "title": title,
            "raw_content": raw_content,
            "content_hash": _sha256(raw_content),
            "metadata": {
                "code_type": "CARC",
                "batch_num": batch_num,
                "code_count": len(batch),
                "first_code": batch[0]["code"],
                "last_code": batch[-1]["code"],
            },
            "scraped_at": scraped_at,
        }

    logger.info("CARC scrape complete: %d documents", batch_num)


def scrape_all_remittance_codes(
    rarc_url: str | None = None,
    carc_url: str | None = None,
    include_rarc: bool = True,
    include_carc: bool = True,
) -> Generator[dict[str, Any], None, None]:
    """Unified entry point for all remittance code types."""
    if include_rarc:
        logger.info("Starting RARC scrape...")
        yield from scrape_cms_rarc(url=rarc_url)

    if include_carc:
        logger.info("Starting CARC scrape...")
        yield from scrape_carc(url=carc_url)
