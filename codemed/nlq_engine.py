"""
CodeMed AI — Natural Language Query (NLQ) Engine
=================================================
Provides plain-English search across ICD-10, CPT, and HCPCS codes.

Users can type queries like:
  "cardiac monitoring for atrial fibrillation"
  "knee replacement surgery"
  "diabetes with kidney complications"

The engine returns ranked code results with:
  - Matching codes and descriptions
  - Code type (ICD-10 / CPT / HCPCS)
  - Relevance score
  - Relevant CMS LCD/NCD policy references (if knowledge layer is connected)

Two search modes:
  1. Keyword search  — fast regex/substring matching (no DB required)
  2. Semantic search — pgvector cosine similarity (requires DB + embeddings)
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Built-in code reference tables (representative subset)
# For production, load from the full CMS code files.
# ---------------------------------------------------------------------------

# ICD-10 code samples: (code, description, category)
_ICD10_CODES = [
    # Diabetes
    ("E10.9",   "Type 1 diabetes mellitus without complications",                "Endocrinology"),
    ("E11.9",   "Type 2 diabetes mellitus without complications",                "Endocrinology"),
    ("E11.40",  "Type 2 diabetes mellitus with diabetic neuropathy, unspecified","Endocrinology"),
    ("E11.65",  "Type 2 diabetes mellitus with hyperglycemia",                   "Endocrinology"),
    ("E11.51",  "Type 2 diabetes mellitus with diabetic peripheral angiopathy",  "Endocrinology"),
    # Cardiac
    ("I48.0",   "Paroxysmal atrial fibrillation",                                "Cardiology"),
    ("I48.11",  "Longstanding persistent atrial fibrillation",                   "Cardiology"),
    ("I50.22",  "Chronic systolic (congestive) heart failure",                   "Cardiology"),
    ("I50.9",   "Heart failure, unspecified",                                    "Cardiology"),
    ("I25.10",  "Atherosclerotic heart disease of native coronary artery",       "Cardiology"),
    ("I21.9",   "Acute myocardial infarction, unspecified",                      "Cardiology"),
    ("I10",     "Essential (primary) hypertension",                              "Cardiology"),
    # Orthopedics
    ("M17.11",  "Primary osteoarthritis, right knee",                            "Orthopedics"),
    ("M17.12",  "Primary osteoarthritis, left knee",                             "Orthopedics"),
    ("M54.5",   "Low back pain",                                                 "Orthopedics"),
    ("M16.11",  "Primary osteoarthritis, right hip",                             "Orthopedics"),
    # Kidney
    ("N18.4",   "Chronic kidney disease, stage 4 (severe)",                      "Nephrology"),
    ("N18.5",   "Chronic kidney disease, stage 5",                               "Nephrology"),
    ("N18.6",   "End-stage renal disease",                                       "Nephrology"),
    # Pulmonology
    ("J44.1",   "Chronic obstructive pulmonary disease with acute exacerbation", "Pulmonology"),
    ("J44.9",   "Chronic obstructive pulmonary disease, unspecified",            "Pulmonology"),
    ("J18.9",   "Pneumonia, unspecified organism",                               "Pulmonology"),
    # Neurology
    ("G30.9",   "Alzheimer's disease, unspecified",                              "Neurology"),
    ("G20",     "Parkinson's disease",                                           "Neurology"),
    ("I63.9",   "Cerebral infarction, unspecified",                              "Neurology"),
    # Mental Health
    ("F32.9",   "Major depressive disorder, single episode, unspecified",        "Psychiatry"),
    ("F41.1",   "Generalized anxiety disorder",                                  "Psychiatry"),
    ("F10.20",  "Alcohol dependence, uncomplicated",                             "Psychiatry"),
    # Oncology
    ("C34.10",  "Malignant neoplasm of upper lobe, bronchus or lung, unspec.",   "Oncology"),
    ("C61",     "Malignant neoplasm of prostate",                                "Oncology"),
    ("C50.911", "Malignant neoplasm of unspecified site of right female breast", "Oncology"),
]

# CPT code samples: (code, description, category)
_CPT_CODES = [
    # E&M
    ("99213",  "Office or other outpatient visit, established patient, 20-29 min", "E&M"),
    ("99214",  "Office or other outpatient visit, established patient, 30-39 min", "E&M"),
    ("99215",  "Office or other outpatient visit, established patient, 40-54 min", "E&M"),
    ("99232",  "Subsequent hospital care, per day, 25-34 min",                     "E&M"),
    # Cardiac
    ("93224",  "External electrocardiographic recording 48-hour",                  "Cardiology"),
    ("93268",  "Patient-activated electrocardiographic rhythm derived event",      "Cardiology"),
    ("93306",  "Echocardiography, transthoracic",                                  "Cardiology"),
    ("93458",  "Catheter placement in coronary artery for coronary angiography",   "Cardiology"),
    ("33361",  "Transcatheter aortic valve replacement (TAVR)",                    "Cardiology"),
    # Orthopedics
    ("27447",  "Total knee arthroplasty",                                          "Orthopedics"),
    ("27130",  "Total hip arthroplasty",                                           "Orthopedics"),
    ("22612",  "Posterior lumbar interbody arthrodesis (spinal fusion)",           "Orthopedics"),
    # Gastroenterology
    ("45378",  "Colonoscopy, flexible; diagnostic",                                "Gastroenterology"),
    ("43239",  "Esophagogastroduodenoscopy with biopsy",                           "Gastroenterology"),
    # Dialysis
    ("90935",  "Hemodialysis procedure with single evaluation",                    "Nephrology"),
    ("90937",  "Hemodialysis procedure requiring repeated evaluation",             "Nephrology"),
    # Radiology
    ("70553",  "MRI brain with and without contrast",                              "Radiology"),
    ("71250",  "CT thorax without contrast",                                       "Radiology"),
    ("72148",  "MRI spinal canal and contents, lumbar; without contrast",          "Radiology"),
    # Psychiatry
    ("90837",  "Psychotherapy, 60 minutes with patient",                           "Psychiatry"),
    ("90791",  "Psychiatric diagnostic evaluation",                                "Psychiatry"),
    ("90792",  "Psychiatric diagnostic evaluation with medical services",          "Psychiatry"),
]

# HCPCS Level II code samples: (code, description, category)
_HCPCS_CODES = [
    ("A4253",  "Blood glucose test or reagent strips for home blood glucose monitor", "Diabetic"),
    ("A4258",  "Spring-powered device for lancet",                                    "Diabetic"),
    ("E0855",  "Orthotic device, ankle-foot, prefabricated",                          "DME"),
    ("E0856",  "Dynamic adjustable ankle extension/flexion device",                   "DME"),
    ("E0601",  "Continuous positive airway pressure (CPAP) device",                   "Respiratory"),
    ("E0470",  "Respiratory assist device, bi-level pressure capability",             "Respiratory"),
    ("J0171",  "Injection, adrenalin, epinephrine",                                   "Drug"),
    ("J0131",  "Injection, acetaminophen",                                            "Drug"),
    ("J1745",  "Injection, infliximab, 10 mg",                                        "Drug"),
    ("J9035",  "Injection, bevacizumab, 10 mg",                                       "Oncology Drug"),
    ("G0444",  "Annual depression screening, 15 minutes",                             "Preventive"),
    ("G0439",  "Annual wellness visit; subsequent",                                   "Preventive"),
    ("L0641",  "Lumbar orthosis, sagittal-coronal control",                           "Orthotics"),
    ("Q0092",  "Set-up portable x-ray equipment",                                     "Radiology"),
    ("V2020",  "Frames, purchases",                                                   "Vision"),
]


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class CodeSearchResult:
    """A single code search result."""
    code: str
    description: str
    code_type: str          # "ICD-10" | "CPT" | "HCPCS"
    category: str
    relevance_score: float  # 0.0–1.0
    matched_terms: list[str] = field(default_factory=list)
    policy_references: list[dict] = field(default_factory=list)  # LCD/NCD refs

    def to_dict(self) -> dict:
        return {
            "code": self.code,
            "description": self.description,
            "code_type": self.code_type,
            "category": self.category,
            "relevance_score": round(self.relevance_score, 3),
            "matched_terms": self.matched_terms,
            "policy_references": self.policy_references,
        }


# ---------------------------------------------------------------------------
# NLQ Engine
# ---------------------------------------------------------------------------

class NLQEngine:
    """
    Natural Language Query engine for medical coding.

    Provides instant guidance for ICD-10, CPT, and HCPCS codes via
    plain-English search queries.

    Usage (offline keyword mode):
        engine = NLQEngine()
        results = engine.search("cardiac monitoring atrial fibrillation")
        for r in results:
            print(r.code, r.description, r.relevance_score)

    Usage (semantic mode with DB):
        engine = NLQEngine(db_conn=conn, openai_client=client)
        results = engine.search("knee replacement therapy", mode="semantic")
    """

    def __init__(
        self,
        db_conn=None,
        openai_client=None,
        icd10_codes: list | None = None,
        cpt_codes: list | None = None,
        hcpcs_codes: list | None = None,
    ):
        self._db_conn = db_conn
        self._openai_client = openai_client

        # Load code reference tables
        self._icd10 = icd10_codes or _ICD10_CODES
        self._cpt = cpt_codes or _CPT_CODES
        self._hcpcs = hcpcs_codes or _HCPCS_CODES

        logger.info(
            "NLQEngine initialised: %d ICD-10, %d CPT, %d HCPCS codes",
            len(self._icd10), len(self._cpt), len(self._hcpcs),
        )

    # ── Public API ────────────────────────────────────────────────────────

    def search(
        self,
        query: str,
        code_types: list[str] | None = None,
        max_results: int = 10,
        mode: str = "keyword",
    ) -> list[CodeSearchResult]:
        """
        Search for medical codes matching a plain-English query.

        Args:
            query:       Natural language query string
            code_types:  Filter by code type: ["ICD-10", "CPT", "HCPCS"]
                         (None = search all types)
            max_results: Maximum results to return
            mode:        "keyword" (fast, offline) or "semantic" (requires DB+OpenAI)

        Returns:
            List of CodeSearchResult sorted by relevance_score descending
        """
        query = query.strip()
        if not query:
            return []

        code_types = code_types or ["ICD-10", "CPT", "HCPCS"]

        if mode == "semantic" and self._db_conn and self._openai_client:
            return self._semantic_search(query, code_types, max_results)
        else:
            return self._keyword_search(query, code_types, max_results)

    def suggest_codes(
        self,
        diagnosis_text: str,
        procedure_text: str | None = None,
        max_per_type: int = 5,
    ) -> dict[str, list[CodeSearchResult]]:
        """
        Suggest relevant ICD-10, CPT, and HCPCS codes for a clinical scenario.

        Args:
            diagnosis_text:  Description of the diagnosis/condition
            procedure_text:  Description of the procedure/service (optional)
            max_per_type:    Maximum codes to return per type

        Returns:
            Dict of {"ICD-10": [...], "CPT": [...], "HCPCS": [...]}
        """
        results: dict[str, list[CodeSearchResult]] = {}

        # ICD-10: search using diagnosis text
        icd10_results = self.search(
            diagnosis_text,
            code_types=["ICD-10"],
            max_results=max_per_type,
        )
        results["ICD-10"] = icd10_results

        # CPT: search using procedure text if given, else diagnosis text
        cpt_query = procedure_text or diagnosis_text
        cpt_results = self.search(
            cpt_query,
            code_types=["CPT"],
            max_results=max_per_type,
        )
        results["CPT"] = cpt_results

        # HCPCS: search using combined text
        hcpcs_query = f"{diagnosis_text} {procedure_text or ''}".strip()
        hcpcs_results = self.search(
            hcpcs_query,
            code_types=["HCPCS"],
            max_results=max_per_type,
        )
        results["HCPCS"] = hcpcs_results

        return results

    def lookup_code(self, code: str) -> Optional[CodeSearchResult]:
        """
        Exact lookup of a specific code (ICD-10, CPT, or HCPCS).

        Returns CodeSearchResult or None if not found.
        """
        code = code.strip().upper()

        for row in self._icd10:
            if row[0].upper() == code:
                return CodeSearchResult(
                    code=row[0], description=row[1], code_type="ICD-10",
                    category=row[2], relevance_score=1.0,
                )
        for row in self._cpt:
            if row[0] == code:
                return CodeSearchResult(
                    code=row[0], description=row[1], code_type="CPT",
                    category=row[2], relevance_score=1.0,
                )
        for row in self._hcpcs:
            if row[0].upper() == code:
                return CodeSearchResult(
                    code=row[0], description=row[1], code_type="HCPCS",
                    category=row[2], relevance_score=1.0,
                )
        return None

    # ── Private: keyword search ───────────────────────────────────────────

    def _keyword_search(
        self,
        query: str,
        code_types: list[str],
        max_results: int,
    ) -> list[CodeSearchResult]:
        """
        Score all codes by keyword overlap with the query.
        """
        query_terms = self._tokenize_query(query)
        candidates: list[CodeSearchResult] = []

        all_codes: list[tuple] = []
        type_map: dict[tuple, str] = {}

        if "ICD-10" in code_types:
            for row in self._icd10:
                all_codes.append(row)
                type_map[row] = "ICD-10"
        if "CPT" in code_types:
            for row in self._cpt:
                all_codes.append(row)
                type_map[row] = "CPT"
        if "HCPCS" in code_types:
            for row in self._hcpcs:
                all_codes.append(row)
                type_map[row] = "HCPCS"

        for row in all_codes:
            code, description, category = row[0], row[1], row[2]
            score, matched = self._score_code(query_terms, code, description, category)
            if score > 0:
                candidates.append(CodeSearchResult(
                    code=code,
                    description=description,
                    code_type=type_map[row],
                    category=category,
                    relevance_score=score,
                    matched_terms=matched,
                ))

        # Sort by relevance descending
        candidates.sort(key=lambda r: r.relevance_score, reverse=True)
        return candidates[:max_results]

    def _score_code(
        self,
        query_terms: list[str],
        code: str,
        description: str,
        category: str,
    ) -> tuple[float, list[str]]:
        """
        Score a code row against query terms.
        Returns (score, matched_terms).
        """
        if not query_terms:
            return 0.0, []

        desc_lower = description.lower()
        cat_lower = category.lower()
        code_lower = code.lower()
        matched: list[str] = []

        total_score = 0.0

        for term in query_terms:
            term_lower = term.lower()

            # Exact code match = maximum score
            if term_lower == code_lower:
                total_score += 2.0
                matched.append(term)
                continue

            # Exact word match in description
            if re.search(r'\b' + re.escape(term_lower) + r'\b', desc_lower):
                total_score += 1.0
                matched.append(term)
            # Partial match in description
            elif term_lower in desc_lower:
                total_score += 0.5
                matched.append(term)
            # Category match
            elif term_lower in cat_lower:
                total_score += 0.3

        if not matched:
            return 0.0, []

        # Normalise: max possible score = 2.0 * len(terms)
        max_score = len(query_terms) * 1.0
        normalised = min(total_score / max_score, 1.0)
        return normalised, matched

    def _tokenize_query(self, query: str) -> list[str]:
        """
        Tokenize query into meaningful search terms.
        Removes short stopwords and returns unique terms.
        """
        stopwords = {
            "the", "a", "an", "is", "are", "was", "were", "for", "of",
            "in", "on", "at", "to", "with", "and", "or", "not", "this",
            "that", "it", "its", "by", "be", "been", "has", "have",
        }
        tokens = re.findall(r'\b[a-zA-Z0-9\-]+\b', query)
        terms = [t for t in tokens if len(t) > 2 and t.lower() not in stopwords]
        # Deduplicate while preserving order
        seen: set[str] = set()
        unique = []
        for t in terms:
            if t.lower() not in seen:
                seen.add(t.lower())
                unique.append(t)
        return unique

    # ── Private: semantic search ──────────────────────────────────────────

    def _semantic_search(
        self,
        query: str,
        code_types: list[str],
        max_results: int,
    ) -> list[CodeSearchResult]:
        """
        Perform semantic similarity search against the knowledge layer.
        Requires DB connection + OpenAI client.
        Falls back to keyword search on any error.
        """
        try:
            from ingestion.embedder import semantic_search as _semantic_search

            doc_type_filter = None
            if len(code_types) == 1:
                type_to_doc = {
                    "ICD-10": "coverage_determination",
                    "CPT": "billing_guidelines",
                    "HCPCS": "billing_guidelines",
                }
                doc_type_filter = type_to_doc.get(code_types[0])

            raw_results = _semantic_search(
                conn=self._db_conn,
                query=query,
                limit=max_results,
                document_type=doc_type_filter,
                client=self._openai_client,
            )

            results = []
            for row in raw_results:
                results.append(CodeSearchResult(
                    code=row.get("lcd_id") or row.get("ncd_id") or "N/A",
                    description=row.get("title", ""),
                    code_type="LCD/NCD",
                    category=row.get("document_type", ""),
                    relevance_score=row.get("similarity", 0.0),
                    policy_references=[{
                        "source_url": row.get("source_url"),
                        "chunk_text": row.get("chunk_text", "")[:300],
                    }],
                ))
            return results

        except Exception as exc:
            logger.warning(
                "Semantic search failed (%s) — falling back to keyword search",
                exc,
            )
            return self._keyword_search(query, code_types, max_results)


# ---------------------------------------------------------------------------
# CLI test harness
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import json
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    engine = NLQEngine()

    test_queries = [
        "cardiac monitoring atrial fibrillation",
        "knee replacement surgery",
        "diabetes insulin pump management",
        "COPD exacerbation",
        "colonoscopy screening",
        "depression anxiety mental health",
    ]

    for query in test_queries:
        print(f"\n{'=' * 60}")
        print(f"Query: '{query}'")
        results = engine.search(query, max_results=5)
        for r in results:
            print(f"  [{r.code_type}] {r.code}: {r.description[:70]} (score={r.relevance_score:.2f})")
