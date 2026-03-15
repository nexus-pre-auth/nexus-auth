"""
Commercial Payer Policy Scraper
================================
Scrapes publicly available clinical policy and medical necessity policy
documents from major commercial payers.

Covered payers:
  - Aetna Clinical Policy Bulletins (CPBs)
  - UnitedHealthcare Medical Policies
  - Cigna Coverage Policies
  - Humana Coverage Determinations

These documents define payer-specific medical necessity criteria, coverage
policies, and clinical guidelines — critical NexusAuth training data for
prior-authorization automation.

Documents are yielded as clinical_policy or medical_necessity_policy types
depending on payer-specific terminology.

Env vars (override index page URLs):
  AETNA_CPB_URL     — Aetna CPB index page
  UHC_POLICY_URL    — UHC Medical Policy index page
  CIGNA_POLICY_URL  — Cigna coverage policy index
  HUMANA_POLICY_URL — Humana coverage determination index

IMPORTANT: This scraper respects a 2-second delay between requests
and only accesses publicly available index and policy pages.
"""

import hashlib
import logging
import os
import re
import time
import urllib.request
import urllib.parse
from datetime import datetime
from typing import Any, Generator

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

AETNA_CPB_URL_DEFAULT = (
    "https://www.aetna.com/health-care-professionals/clinical-policy-bulletins"
    "/medical-clinical-policy-bulletins.html"
)
UHC_POLICY_URL_DEFAULT = (
    "https://www.uhcprovider.com/en/policies-protocols/coverage-policies.html"
)
CIGNA_POLICY_URL_DEFAULT = (
    "https://www.cigna.com/healthcare-professionals/coverage-and-reimbursement"
    "/clinical-coverage-policy"
)
HUMANA_POLICY_URL_DEFAULT = (
    "https://www.humana.com/provider/medical-resources/medical-clinical-coverage-guidelines"
)

REQUEST_DELAY = 2.0   # Polite delay between requests
MAX_POLICIES_PER_PAYER = 200  # Cap to avoid overwhelming scrape runs


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sha256(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _fetch_html(url: str, timeout: int = 30) -> str | None:
    """Fetch HTML; return None on error (non-fatal — payer sites can block)."""
    try:
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": (
                    "NexusAuth-Ingestion/2.0 (healthcare knowledge pipeline; "
                    "contact: admin@nexusauth.io)"
                ),
                "Accept": "text/html,application/xhtml+xml",
            },
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = resp.read()
        encoding = resp.headers.get_content_charset() or "utf-8"
        return data.decode(encoding, errors="replace")
    except Exception as exc:
        logger.warning("Could not fetch %s: %s", url, exc)
        return None


def _html_to_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    # Remove nav, footer, and script elements
    for tag in soup(["script", "style", "nav", "footer", "header", "aside"]):
        tag.decompose()
    return re.sub(r"\s+", " ", soup.get_text(separator=" ", strip=True)).strip()


def _extract_policy_links(
    html: str, base_url: str, domain: str
) -> list[dict]:
    """
    Extract policy document links and titles from a payer index page.
    Returns list of {title, url} dicts.
    """
    soup = BeautifulSoup(html, "html.parser")
    policies = []
    seen_urls: set[str] = set()

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        title = a.get_text(strip=True)

        if not title or len(title) < 10:
            continue

        # Skip navigation links
        if any(
            skip in href.lower()
            for skip in [
                "#", "javascript:", "mailto:", "login", "sign-in",
                "contact", "about", "careers", "privacy",
            ]
        ):
            continue

        # Build absolute URL
        if href.startswith("http"):
            abs_url = href
        elif href.startswith("/"):
            parsed = urllib.parse.urlparse(base_url)
            abs_url = f"{parsed.scheme}://{parsed.netloc}{href}"
        else:
            abs_url = urllib.parse.urljoin(base_url, href)

        # Keep only same-domain links
        if domain not in abs_url:
            continue

        if abs_url in seen_urls:
            continue

        # Filter to likely policy document links
        lower_href = abs_url.lower()
        lower_title = title.lower()
        if any(
            kw in lower_href or kw in lower_title
            for kw in [
                "policy", "cpb", "clinical", "coverage", "criteria",
                "guideline", "bulletin", "determination",
            ]
        ):
            seen_urls.add(abs_url)
            policies.append({"title": title, "url": abs_url})

    return policies


def _scrape_policy_page(url: str, payer_code: str, doc_type: str) -> dict | None:
    """Fetch a single policy page and return a raw document dict."""
    time.sleep(REQUEST_DELAY)
    html = _fetch_html(url)
    if not html:
        return None

    text = _html_to_text(html)
    if len(text) < 200:
        return None  # Too short to be a real policy

    # Extract title from HTML
    soup = BeautifulSoup(html, "html.parser")
    title_tag = soup.find("h1") or soup.find("h2") or soup.find("title")
    title = title_tag.get_text(strip=True) if title_tag else url.split("/")[-1]

    # Prepend title for better embedding context
    full_text = f"{title}\n\n{text}"

    return {
        "source_url": url,
        "source_domain": urllib.parse.urlparse(url).netloc.replace("www.", ""),
        "document_type_hint": doc_type,
        "title": title[:400],
        "raw_content": full_text,
        "content_hash": _sha256(full_text),
        "metadata": {
            "payer_code": payer_code,
            "scraped_from": url,
        },
        "scraped_at": datetime.utcnow(),
    }


# ---------------------------------------------------------------------------
# Payer-specific index scrapers
# ---------------------------------------------------------------------------

def _scrape_payer_policies(
    index_url: str,
    domain: str,
    payer_code: str,
    doc_type: str,
    max_policies: int = MAX_POLICIES_PER_PAYER,
) -> Generator[dict[str, Any], None, None]:
    """Generic payer policy scraper: fetch index, follow links, yield docs."""
    logger.info("Fetching %s policy index: %s", payer_code, index_url)
    index_html = _fetch_html(index_url)
    if not index_html:
        logger.warning("Could not access %s policy index — skipping", payer_code)
        return

    links = _extract_policy_links(index_html, index_url, domain)
    logger.info("Found %d policy links for %s", len(links), payer_code)

    count = 0
    for link in links[:max_policies]:
        if count >= max_policies:
            break
        doc = _scrape_policy_page(link["url"], payer_code, doc_type)
        if doc:
            # Use the link's title if the scraped title is too generic
            if len(link["title"]) > len(doc["title"]):
                doc["title"] = link["title"][:400]
            count += 1
            yield doc

    logger.info("%s policy scrape complete: %d documents", payer_code, count)


def scrape_aetna_cpbs(
    url: str | None = None,
) -> Generator[dict[str, Any], None, None]:
    """Scrape Aetna Clinical Policy Bulletins."""
    url = url or os.environ.get("AETNA_CPB_URL", AETNA_CPB_URL_DEFAULT)
    yield from _scrape_payer_policies(
        index_url=url,
        domain="aetna.com",
        payer_code="AETNA",
        doc_type="clinical_policy",
    )


def scrape_uhc_policies(
    url: str | None = None,
) -> Generator[dict[str, Any], None, None]:
    """Scrape UnitedHealthcare Medical Policies."""
    url = url or os.environ.get("UHC_POLICY_URL", UHC_POLICY_URL_DEFAULT)
    yield from _scrape_payer_policies(
        index_url=url,
        domain="uhcprovider.com",
        payer_code="UHC",
        doc_type="medical_necessity_policy",
    )


def scrape_cigna_policies(
    url: str | None = None,
) -> Generator[dict[str, Any], None, None]:
    """Scrape Cigna coverage policies."""
    url = url or os.environ.get("CIGNA_POLICY_URL", CIGNA_POLICY_URL_DEFAULT)
    yield from _scrape_payer_policies(
        index_url=url,
        domain="cigna.com",
        payer_code="CIGNA",
        doc_type="medical_necessity_policy",
    )


def scrape_humana_policies(
    url: str | None = None,
) -> Generator[dict[str, Any], None, None]:
    """Scrape Humana coverage determination guidelines."""
    url = url or os.environ.get("HUMANA_POLICY_URL", HUMANA_POLICY_URL_DEFAULT)
    yield from _scrape_payer_policies(
        index_url=url,
        domain="humana.com",
        payer_code="HUMANA",
        doc_type="medical_necessity_policy",
    )


def scrape_all_payer_policies(
    include_aetna: bool = True,
    include_uhc: bool = True,
    include_cigna: bool = True,
    include_humana: bool = True,
) -> Generator[dict[str, Any], None, None]:
    """
    Unified entry point for all commercial payer policy scrapers.
    Yields clinical_policy and medical_necessity_policy documents.
    """
    if include_aetna:
        logger.info("Starting Aetna CPB scrape...")
        yield from scrape_aetna_cpbs()

    if include_uhc:
        logger.info("Starting UHC policy scrape...")
        yield from scrape_uhc_policies()

    if include_cigna:
        logger.info("Starting Cigna policy scrape...")
        yield from scrape_cigna_policies()

    if include_humana:
        logger.info("Starting Humana policy scrape...")
        yield from scrape_humana_policies()
