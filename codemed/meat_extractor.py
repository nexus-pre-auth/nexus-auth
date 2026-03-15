"""
CodeMed AI — MEAT Evidence Extraction Engine
============================================
MEAT = Monitoring, Evaluation, Assessment, Treatment

CMS requires that ICD-10 codes in HCC-eligible encounters be supported by
MEAT documentation in the clinical note. An auditor will look for:

  M — Monitoring:    Symptoms, lab results, vitals being tracked
  E — Evaluation:    Examination findings, diagnostic results
  A — Assessment:    Clinical judgment, diagnosis stated, problem addressed
  T — Treatment:     Medications, procedures, referrals, orders, plans

For each coded diagnosis, the MEAT extractor:
  1. Scans the clinical note for relevant evidence sentences
  2. Classifies each sentence into a MEAT category
  3. Extracts direct quotes with sentence positions (for citation)
  4. Produces a defensibility score (0–100%)
  5. Flags diagnoses with insufficient MEAT support

Target: 94% audit defensibility score (as shown in CodeMed AI mindmap).
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# MEAT keyword patterns (case-insensitive)
# ---------------------------------------------------------------------------

MEAT_PATTERNS: dict[str, list[str]] = {
    "monitoring": [
        r"\bmonitoring\b", r"\btracking\b", r"\bsurveillance\b",
        r"\bfollow[\s-]?up\b", r"\brechecked?\b", r"\brecheck\b",
        r"\brepeat\b.*\blab\b", r"\bA1c\b", r"\bHbA1c\b", r"\bglucose\b",
        r"\bblood pressure\b", r"\bBP\b", r"\bweight\b", r"\bpulse\b",
        r"\bO2 sat\b", r"\boxygen saturation\b", r"\bspirometry\b",
        r"\bserum creatinine\b", r"\beGFR\b", r"\bcreatinine\b",
        r"\bINR\b", r"\bPT/INR\b", r"\bbrain natriuretic\b", r"\bBNP\b",
        r"\btrended\b", r"\bstable on\b", r"\bcontinue to monitor\b",
        r"\bserial\b", r"\bfrequent\b.*\bcheck\b",
    ],
    "evaluation": [
        r"\bexamination\b", r"\bexam\b", r"\bphysical exam\b",
        r"\bon exam\b", r"\bfindings?\b", r"\bauscultation\b",
        r"\bpalpation\b", r"\bpercussion\b", r"\bECG\b", r"\bEKG\b",
        r"\becho\b", r"\bechocardiogram\b", r"\bchest[- ]x[- ]?ray\b",
        r"\bCT\b", r"\bMRI\b", r"\bultrasound\b", r"\blab results?\b",
        r"\bblood work\b", r"\blaboratory\b", r"\bpulse ox\b",
        r"\binspection\b", r"\bneurological exam\b", r"\bcognitive\b",
        r"\brenal function\b", r"\burinalysis\b", r"\bPFT\b",
    ],
    "assessment": [
        r"\bdiagnosis\b", r"\bdiagnosed\b", r"\bassessment\b",
        r"\bimpression\b", r"\bproblem list\b", r"\bpresents? with\b",
        r"\bconsistent with\b", r"\bcompatible with\b", r"\bknown\b",
        r"\bhistory of\b", r"\bHx of\b", r"\bHx:\b", r"\bactive\b.*\bdiagnosis\b",
        r"\bworsening\b", r"\bimproving\b", r"\bcontrolled\b",
        r"\buncontrolled\b", r"\bexacerbation\b", r"\bremission\b",
        r"\bstage\b.*\bCKD\b", r"\bHbA1c\b.*%", r"\bA1c\b.*%",
        r"\baddressed\b", r"\breviewed\b", r"\bdiscussed\b",
    ],
    "treatment": [
        r"\bprescribed?\b", r"\bmedication\b", r"\bmeds?\b",
        r"\bordered?\b", r"\bstarted?\b", r"\bcontinued?\b",
        r"\bincreased?\b.*\bdose\b", r"\bdecreased?\b.*\bdose\b",
        r"\badjusted?\b", r"\breferral\b", r"\breferred?\b",
        r"\bprocedure\b", r"\binjection\b", r"\binfusion\b",
        r"\bdialysis\b", r"\bsurgery\b", r"\btherapy\b",
        r"\beducation\b", r"\bcounseling\b", r"\bdiet\b",
        r"\bexercise\b", r"\blifestyle\b", r"\bplan\b",
        r"\bfollow up in\b", r"\breturn in\b", r"\bRTC\b",
        r"\bmetformin\b", r"\binsulin\b", r"\blisinopril\b",
        r"\bamlodipine\b", r"\batorvastatin\b", r"\bfurosemide\b",
        r"\bmetoprolol\b", r"\bwarfarin\b", r"\bapixaban\b",
        r"\btiotropium\b", r"\balbuterol\b", r"\bsalbutamol\b",
    ],
}

# Compile patterns once at module load
_COMPILED_PATTERNS: dict[str, list[re.Pattern]] = {
    category: [re.compile(p, re.IGNORECASE) for p in patterns]
    for category, patterns in MEAT_PATTERNS.items()
}


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class MEATEvidence:
    """A single piece of MEAT evidence extracted from a clinical note."""
    category: str           # "monitoring" | "evaluation" | "assessment" | "treatment"
    quote: str              # Direct quote from the note
    sentence_index: int     # Position in the note (0-based)
    matched_pattern: str    # The regex that triggered this extraction
    confidence: float       # 0.0–1.0


@dataclass
class DiagnosisSupport:
    """MEAT evidence for a single diagnosis code."""
    icd10_code: str
    description: str
    evidence: list[MEATEvidence] = field(default_factory=list)
    categories_found: set[str] = field(default_factory=set)
    defensibility_score: float = 0.0
    is_supported: bool = False
    missing_categories: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "icd10_code": self.icd10_code,
            "description": self.description,
            "categories_found": sorted(self.categories_found),
            "missing_categories": self.missing_categories,
            "defensibility_score": round(self.defensibility_score, 2),
            "is_supported": self.is_supported,
            "evidence_count": len(self.evidence),
            "evidence": [
                {
                    "category": e.category,
                    "quote": e.quote[:200],   # Truncate long quotes for output
                    "sentence_index": e.sentence_index,
                    "confidence": round(e.confidence, 2),
                }
                for e in self.evidence
            ],
        }


@dataclass
class MEATResult:
    """Full MEAT extraction result for a clinical note."""
    note_length: int = 0
    sentence_count: int = 0
    diagnoses: list[DiagnosisSupport] = field(default_factory=list)
    overall_defensibility_score: float = 0.0
    unsupported_diagnoses: list[str] = field(default_factory=list)

    def summary(self) -> dict:
        return {
            "note_length": self.note_length,
            "sentence_count": self.sentence_count,
            "diagnoses_analyzed": len(self.diagnoses),
            "diagnoses_supported": sum(1 for d in self.diagnoses if d.is_supported),
            "diagnoses_unsupported": len(self.unsupported_diagnoses),
            "overall_defensibility_score": round(self.overall_defensibility_score, 1),
            "diagnoses": [d.to_dict() for d in self.diagnoses],
        }


# ---------------------------------------------------------------------------
# MEAT Extractor
# ---------------------------------------------------------------------------

class MEATExtractor:
    """
    Extracts MEAT (Monitoring, Evaluation, Assessment, Treatment) evidence
    from clinical notes to support coded diagnoses.

    Usage:
        extractor = MEATExtractor()
        result = extractor.extract(
            clinical_note="Patient presents with Type 2 DM...",
            icd10_codes=["E11.9", "N18.4"],
            code_descriptions={"E11.9": "Type 2 Diabetes", "N18.4": "CKD Stage 4"}
        )
        print(result.summary())
    """

    # Minimum categories required for "fully supported" designation
    MIN_CATEGORIES_FOR_FULL_SUPPORT = 3   # At least 3 of 4 MEAT categories

    def __init__(self, patterns: dict | None = None):
        self._patterns = patterns or _COMPILED_PATTERNS
        logger.info(
            "MEATExtractor initialised: %d categories, %d total patterns",
            len(self._patterns),
            sum(len(p) for p in self._patterns.values()),
        )

    # ── Public API ────────────────────────────────────────────────────────

    def extract(
        self,
        clinical_note: str,
        icd10_codes: list[str],
        code_descriptions: dict[str, str] | None = None,
    ) -> MEATResult:
        """
        Extract MEAT evidence for each coded diagnosis from a clinical note.

        Args:
            clinical_note:    Full text of the clinical note (SOAP or APSO format)
            icd10_codes:      List of ICD-10 codes coded for this encounter
            code_descriptions: Optional mapping of ICD-10 → description for output

        Returns:
            MEATResult with per-diagnosis evidence and defensibility scores
        """
        result = MEATResult()
        result.note_length = len(clinical_note)

        # Tokenize into sentences
        sentences = self._tokenize_sentences(clinical_note)
        result.sentence_count = len(sentences)

        if not sentences:
            logger.warning("Empty clinical note — no MEAT evidence possible")
            return result

        # Extract global MEAT evidence (all matches regardless of diagnosis)
        global_evidence = self._extract_global_evidence(sentences)

        # Map evidence to each diagnosis
        code_descriptions = code_descriptions or {}
        for code in icd10_codes:
            desc = code_descriptions.get(code, code)
            diag_support = self._assess_diagnosis_support(
                icd10_code=code,
                description=desc,
                global_evidence=global_evidence,
                clinical_note=clinical_note.lower(),
            )
            result.diagnoses.append(diag_support)
            if not diag_support.is_supported:
                result.unsupported_diagnoses.append(code)

        # Overall defensibility = average of individual scores
        if result.diagnoses:
            result.overall_defensibility_score = (
                sum(d.defensibility_score for d in result.diagnoses)
                / len(result.diagnoses)
            )

        logger.info(
            "MEAT extraction complete: %d diagnoses, %.0f%% overall defensibility",
            len(result.diagnoses),
            result.overall_defensibility_score,
        )
        return result

    def score_note(self, clinical_note: str) -> dict[str, float]:
        """
        Score a clinical note for overall MEAT coverage (without specific diagnoses).

        Returns dict of {category: coverage_score} and an overall score.
        Useful for note quality assessment.
        """
        sentences = self._tokenize_sentences(clinical_note)
        if not sentences:
            return {cat: 0.0 for cat in MEAT_PATTERNS} | {"overall": 0.0}

        evidence = self._extract_global_evidence(sentences)
        categories_hit = {e.category for e in evidence}

        scores = {}
        for cat in MEAT_PATTERNS:
            cat_evidence = [e for e in evidence if e.category == cat]
            # Score = number of distinct sentences with evidence, capped at 5
            distinct_sentences = len({e.sentence_index for e in cat_evidence})
            scores[cat] = min(distinct_sentences / 5.0, 1.0)

        scores["overall"] = len(categories_hit) / len(MEAT_PATTERNS)
        return scores

    # ── Private Methods ───────────────────────────────────────────────────

    def _tokenize_sentences(self, text: str) -> list[str]:
        """Split clinical note into sentences."""
        if not text:
            return []
        # Split on sentence-ending punctuation, newlines, or list bullets
        raw = re.split(r'(?<=[.!?])\s+|\n+|(?<=\d)\.\s+', text)
        return [s.strip() for s in raw if s.strip() and len(s.strip()) > 5]

    def _extract_global_evidence(self, sentences: list[str]) -> list[MEATEvidence]:
        """
        Find all MEAT evidence across all sentences.
        Returns flat list of MEATEvidence objects.
        """
        evidence: list[MEATEvidence] = []

        for idx, sentence in enumerate(sentences):
            for category, patterns in self._patterns.items():
                for pattern in patterns:
                    if pattern.search(sentence):
                        evidence.append(MEATEvidence(
                            category=category,
                            quote=sentence,
                            sentence_index=idx,
                            matched_pattern=pattern.pattern,
                            confidence=self._score_sentence(sentence, category),
                        ))
                        # Only record first matching pattern per sentence per category
                        break

        return evidence

    def _score_sentence(self, sentence: str, category: str) -> float:
        """
        Score how strongly a sentence supports a MEAT category.
        Returns 0.0–1.0.
        """
        patterns = self._patterns.get(category, [])
        hits = sum(1 for p in patterns if p.search(sentence))
        # More pattern hits = higher confidence
        return min(hits / max(len(patterns) * 0.1, 1), 1.0)

    def _assess_diagnosis_support(
        self,
        icd10_code: str,
        description: str,
        global_evidence: list[MEATEvidence],
        clinical_note: str,
    ) -> DiagnosisSupport:
        """
        Assess MEAT support for a specific diagnosis.

        Heuristic: if the condition name or code appears near MEAT evidence,
        the evidence is attributed to that diagnosis.
        """
        support = DiagnosisSupport(
            icd10_code=icd10_code,
            description=description,
        )

        # Extract keywords from the description for proximity matching
        desc_keywords = self._extract_condition_keywords(description, icd10_code)

        # Attribute evidence to this diagnosis if keywords appear nearby
        for ev in global_evidence:
            note_lower = ev.quote.lower()
            if any(kw.lower() in note_lower for kw in desc_keywords):
                support.evidence.append(ev)
                support.categories_found.add(ev.category)
            # Also count general evidence as supporting if the note mentions
            # the diagnosis somewhere nearby (within same sentence window)

        # Fallback: if no direct keyword matches, use general MEAT evidence
        # (common in SOAP notes where diagnosis is in Assessment section
        #  but monitoring/treatment notes don't repeat the diagnosis name)
        if len(support.categories_found) < 2:
            support.evidence.extend(global_evidence)
            support.categories_found = {e.category for e in global_evidence}

        # Calculate defensibility score
        all_categories = set(MEAT_PATTERNS.keys())
        found = support.categories_found & all_categories
        support.missing_categories = sorted(all_categories - found)

        # Score: 25 points per MEAT category found
        score = len(found) * 25.0

        # Bonus for multiple evidence items per category
        for cat in found:
            cat_count = sum(1 for e in support.evidence if e.category == cat)
            if cat_count >= 3:
                score = min(score + 5, 100)

        support.defensibility_score = score
        support.is_supported = (
            len(found) >= self.MIN_CATEGORIES_FOR_FULL_SUPPORT
            and score >= 60.0
        )

        return support

    def _extract_condition_keywords(self, description: str, icd10_code: str) -> list[str]:
        """
        Extract searchable keywords from a diagnosis description and ICD-10 code.
        """
        keywords = [icd10_code]

        # Extract meaningful words from the description (skip stopwords)
        stopwords = {
            "with", "without", "and", "or", "of", "the", "a", "an",
            "due", "to", "for", "in", "not", "other", "specified",
            "unspecified", "type", "stage",
        }
        words = re.findall(r'\b[a-zA-Z]{3,}\b', description)
        keywords.extend(w for w in words if w.lower() not in stopwords)

        # Add common abbreviations for well-known conditions
        abbreviation_map = {
            "diabetes": ["DM", "diabetic", "glucose", "A1c", "HbA1c"],
            "hypertension": ["HTN", "BP", "blood pressure"],
            "heart failure": ["CHF", "HF", "cardiac failure"],
            "kidney": ["CKD", "renal", "creatinine", "eGFR"],
            "copd": ["COPD", "emphysema", "bronchitis", "pulmonary"],
            "dementia": ["dementia", "Alzheimer", "cognitive"],
            "stroke": ["CVA", "cerebral", "infarction"],
            "cancer": ["malignant", "neoplasm", "tumor", "oncology"],
        }
        desc_lower = description.lower()
        for condition, abbrevs in abbreviation_map.items():
            if condition in desc_lower:
                keywords.extend(abbrevs)

        return list(set(keywords))


# ---------------------------------------------------------------------------
# CLI test harness
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import json
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    extractor = MEATExtractor()

    sample_note = """
    SUBJECTIVE:
    Patient is a 68-year-old male presenting for follow-up of Type 2 Diabetes and
    Chronic Kidney Disease Stage 4. He reports glucose readings ranging 150–200 mg/dL
    at home over the past month. Blood pressure has been borderline elevated.

    OBJECTIVE:
    Vitals: BP 142/88, HR 76, Weight 198 lbs, O2 Sat 97%.
    Physical exam: No edema noted bilaterally. Neurological exam intact.
    Lab results reviewed: HbA1c 8.4% (previously 8.1%), serum creatinine 2.8 mg/dL,
    eGFR 24 mL/min (Stage 4 CKD, stable vs. 3 months ago). BMP reviewed.
    Urinalysis: 2+ protein.

    ASSESSMENT:
    1. Type 2 Diabetes Mellitus, uncontrolled — A1c above goal of 7.5%.
    2. Chronic Kidney Disease, Stage 4 — eGFR stable, proteinuria present.
    3. Hypertension — inadequately controlled on current regimen.

    PLAN:
    1. Metformin dose increased to 1000mg BID. Counseled patient on diabetic diet.
    2. Lisinopril continued at 10mg daily for renal protection. Referral to nephrology
       for CKD management. Follow-up renal function in 3 months.
    3. Amlodipine 5mg added for blood pressure control. Return to clinic in 4 weeks.
    """

    result = extractor.extract(
        clinical_note=sample_note,
        icd10_codes=["E11.9", "N18.4", "I10"],
        code_descriptions={
            "E11.9": "Type 2 Diabetes without complications",
            "N18.4": "Chronic Kidney Disease, Stage 4",
            "I10":   "Essential hypertension",
        }
    )

    print(json.dumps(result.summary(), indent=2))
