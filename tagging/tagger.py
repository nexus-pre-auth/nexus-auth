"""
NexusAuth Document Tagger
=========================
Classifies raw documents using the taxonomy.yaml keyword scoring system.
Detects payer, document type, medical specialties, and billing codes.
Routes documents to the correct downstream tool (NexusAuth / CODEMED).

Session 1 artifact — used by the Session 2 ingestion pipeline.
"""

from __future__ import annotations

import re
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import yaml

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# Data Structures
# ─────────────────────────────────────────────────────────────

@dataclass
class TaggingResult:
    """Output of the tagger for a single document."""
    payer_code: Optional[str] = None
    document_type: str = "unknown"
    document_subtype: Optional[str] = None
    specialties: list[str] = field(default_factory=list)
    cpt_codes: list[str] = field(default_factory=list)
    icd10_codes: list[str] = field(default_factory=list)
    hcpcs_codes: list[str] = field(default_factory=list)
    routing_targets: list[str] = field(default_factory=list)
    confidence_score: float = 0.0
    requires_review: bool = False
    raw_scores: dict[str, float] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "payer_code": self.payer_code,
            "document_type": self.document_type,
            "document_subtype": self.document_subtype,
            "specialties": self.specialties,
            "cpt_codes": self.cpt_codes,
            "icd10_codes": self.icd10_codes,
            "hcpcs_codes": self.hcpcs_codes,
            "routing_targets": self.routing_targets,
            "confidence_score": self.confidence_score,
            "requires_review": self.requires_review,
        }


# ─────────────────────────────────────────────────────────────
# Tagger
# ─────────────────────────────────────────────────────────────

class DocumentTagger:
    """
    Keyword-scoring document classifier.

    Loads taxonomy.yaml on init and exposes a single `tag()` method
    that returns a TaggingResult for any (url, text) pair.
    """

    # Regex patterns for billing code extraction
    CPT_PATTERN   = re.compile(r'\b(\d{5}[A-Z]?)\b')
    ICD10_PATTERN = re.compile(r'\b([A-Z]\d{2}(?:\.\d{1,4})?)\b')
    HCPCS_PATTERN = re.compile(r'\b([A-Z]\d{4})\b')

    def __init__(self, taxonomy_path: Optional[str] = None):
        if taxonomy_path is None:
            taxonomy_path = Path(__file__).parent / "taxonomy.yaml"
        with open(taxonomy_path, "r") as f:
            self.taxonomy = yaml.safe_load(f)

        self.doc_types    = self.taxonomy["document_types"]
        self.payer_domains = self.taxonomy["payer_domains"]
        self.specialties  = self.taxonomy["specialties"]
        self.routing      = self.taxonomy["routing_matrix"]
        self.thresholds   = self.taxonomy["confidence_thresholds"]

        logger.info("DocumentTagger loaded taxonomy with %d document types", len(self.doc_types))

    # ── Public API ────────────────────────────────────────────

    def tag(self, text: str, source_url: str = "") -> TaggingResult:
        """
        Tag a document given its text content and source URL.

        Args:
            text:       Full text content of the document.
            source_url: Original URL (used for payer detection).

        Returns:
            TaggingResult with all classification outputs.
        """
        result = TaggingResult()
        text_lower = text.lower()

        result.payer_code        = self._detect_payer(source_url, text_lower)
        doc_type, confidence, scores = self._classify_document_type(text_lower)
        result.document_type     = doc_type
        result.confidence_score  = confidence
        result.raw_scores        = scores
        result.specialties       = self._detect_specialties(text_lower)
        result.cpt_codes         = self._extract_cpt(text)
        result.icd10_codes       = self._extract_icd10(text)
        result.hcpcs_codes       = self._extract_hcpcs(text)
        result.routing_targets   = self._determine_routing(doc_type)
        result.requires_review   = confidence < self.thresholds["review_required"]

        logger.debug(
            "Tagged document: type=%s confidence=%.2f payer=%s review=%s",
            result.document_type, result.confidence_score,
            result.payer_code, result.requires_review
        )
        return result

    # ── Private Methods ───────────────────────────────────────

    def _detect_payer(self, source_url: str, text_lower: str) -> Optional[str]:
        """Detect payer from source URL domain, then fall back to text keywords."""
        if source_url:
            try:
                domain = urlparse(source_url).netloc.lower()
                for payer_code, domains in self.payer_domains.items():
                    if any(d in domain for d in domains):
                        return payer_code
            except Exception:
                pass

        # Text-based fallback
        payer_keywords = {
            "CMS":   ["cms.gov", "medicare", "medicaid", "cms coverage"],
            "AETNA": ["aetna", "aetna clinical policy"],
            "UHC":   ["unitedhealthcare", "uhc", "optum"],
            "BCBS":  ["blue cross", "blue shield", "bcbs", "anthem"],
        }
        for payer_code, keywords in payer_keywords.items():
            if any(kw in text_lower for kw in keywords):
                return payer_code

        return None

    def _classify_document_type(
        self, text_lower: str
    ) -> tuple[str, float, dict[str, float]]:
        """
        Score each document type by keyword hits, return best match.

        Returns:
            (document_type, confidence_score, raw_scores_dict)
        """
        scores: dict[str, float] = {}

        for doc_type, config in self.doc_types.items():
            if doc_type == "unknown":
                continue
            keywords = config.get("keywords", [])
            weight   = config.get("weight", 1.0)
            hits     = sum(1 for kw in keywords if kw.lower() in text_lower)
            if hits > 0:
                # Normalise: hits / total_keywords, then apply weight
                raw_score = hits / max(len(keywords), 1)
                scores[doc_type] = raw_score * weight

        if not scores:
            return "unknown", 0.0, {}

        best_type  = max(scores, key=scores.__getitem__)
        best_raw   = scores[best_type]

        # Normalise confidence to [0, 1] using sigmoid-like scaling
        # A score of 0.5 after weighting → ~0.75 confidence
        confidence = min(best_raw / (best_raw + 0.3), 1.0)

        # If confidence is below reject threshold, classify as unknown
        if confidence < self.thresholds["reject"]:
            return "unknown", confidence, scores

        return best_type, confidence, scores

    def _detect_specialties(self, text_lower: str) -> list[str]:
        """Return list of detected medical specialties."""
        detected = []
        for specialty, keywords in self.specialties.items():
            if any(kw.lower() in text_lower for kw in keywords):
                detected.append(specialty)
        return detected

    def _extract_cpt(self, text: str) -> list[str]:
        """Extract unique CPT codes (5-digit numeric, optional alpha suffix)."""
        matches = self.CPT_PATTERN.findall(text)
        # Filter out obvious non-CPT 5-digit numbers (e.g. zip codes)
        return sorted(set(m for m in matches if not m.isdigit() or 10000 <= int(m) <= 99999))

    def _extract_icd10(self, text: str) -> list[str]:
        """Extract unique ICD-10 codes (letter + 2 digits + optional decimal)."""
        matches = self.ICD10_PATTERN.findall(text)
        # Basic ICD-10 format validation: starts with letter, followed by digits
        valid = [m for m in matches if re.match(r'^[A-Z]\d{2}', m)]
        return sorted(set(valid))

    def _extract_hcpcs(self, text: str) -> list[str]:
        """Extract unique HCPCS Level II codes (letter + 4 digits)."""
        matches = self.HCPCS_PATTERN.findall(text)
        return sorted(set(matches))

    def _determine_routing(self, document_type: str) -> list[str]:
        """Look up routing targets from the routing matrix."""
        routing_config = self.routing.get(document_type, self.routing.get("unknown", {}))
        return routing_config.get("targets", ["REVIEW"])


# ─────────────────────────────────────────────────────────────
# Quick test harness
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    tagger = DocumentTagger()

    test_cases = [
        {
            "name": "CMS LCD — Cardiac Monitoring",
            "url": "https://www.cms.gov/medicare-coverage-database/view/lcd.aspx?lcdid=L33822",
            "text": (
                "Local Coverage Determination (LCD): Cardiac Event Monitors (L33822). "
                "This LCD provides coverage criteria for cardiac event monitoring services. "
                "Prior authorization is required for CPT codes 93268, 93270, 93271, 93272. "
                "ICD-10 codes I48.0, I48.1, I49.9 support medical necessity. "
                "MAC Jurisdiction: Noridian Healthcare Solutions."
            ),
        },
        {
            "name": "Aetna CPB — Spinal Cord Stimulation",
            "url": "https://www.aetna.com/cpb/medical/data/700_799/0757.html",
            "text": (
                "Aetna Clinical Policy Bulletin: Spinal Cord Stimulation. "
                "Prior authorization is required for spinal cord stimulation procedures. "
                "Coverage criteria include failed back surgery syndrome and complex regional pain syndrome. "
                "CPT codes: 63650, 63655, 63685. HCPCS: E0756. "
                "ICD-10: G89.29, M54.5."
            ),
        },
        {
            "name": "CMS Fee Schedule — Physician",
            "url": "https://www.cms.gov/medicare/physician-fee-schedule",
            "text": (
                "Medicare Physician Fee Schedule 2024. "
                "The Medicare Physician Fee Schedule (MPFS) sets payment rates for physician services. "
                "Conversion factor: $32.74. Relative Value Units (RVUs) determine allowable amounts. "
                "Facility rate and non-facility rate apply based on place of service."
            ),
        },
    ]

    for tc in test_cases:
        result = tagger.tag(tc["text"], tc["url"])
        print(f"\n{'='*60}")
        print(f"Test: {tc['name']}")
        print(f"  Payer:          {result.payer_code}")
        print(f"  Document Type:  {result.document_type}")
        print(f"  Confidence:     {result.confidence_score:.2f}")
        print(f"  Specialties:    {result.specialties}")
        print(f"  CPT Codes:      {result.cpt_codes}")
        print(f"  ICD-10 Codes:   {result.icd10_codes}")
        print(f"  HCPCS Codes:    {result.hcpcs_codes}")
        print(f"  Routing:        {result.routing_targets}")
        print(f"  Needs Review:   {result.requires_review}")
