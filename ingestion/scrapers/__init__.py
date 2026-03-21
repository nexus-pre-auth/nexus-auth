"""
CodeMed AI — Ingestion Scrapers
================================
Scrapers for CMS data sources:
  - cms_scraper:            CMS MCD bulk LCD/NCD policy downloads
  - hcc_model_scraper:      CMS-HCC Model V28 ICD-10 mapping + coefficient ZIPs
  - eligible_codes_scraper: CMS MA risk-adjustment eligible CPT/HCPCS codes
"""

from .cms_scraper import scrape_all_cms, scrape_cms_lcds, scrape_cms_ncds
from .hcc_model_scraper import (
    build_crosswalk_from_cms_data,
    parse_icd10_mapping_zip,
    parse_coefficient_zip,
)
from .eligible_codes_scraper import (
    EligibleCode,
    EligibleCodesScraper,
    fetch_eligible_codes,
    load_eligible_codes_from_file,
)

__all__ = [
    "scrape_all_cms",
    "scrape_cms_lcds",
    "scrape_cms_ncds",
    "build_crosswalk_from_cms_data",
    "parse_icd10_mapping_zip",
    "parse_coefficient_zip",
    "EligibleCode",
    "EligibleCodesScraper",
    "fetch_eligible_codes",
    "load_eligible_codes_from_file",
]
