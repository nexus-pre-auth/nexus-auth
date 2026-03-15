"""
CodeMed AI — Test Suite
========================
Tests for all CodeMed AI components:
  - HCC V28 hierarchy enforcement engine
  - MEAT evidence extraction
  - Natural language coding query engine
  - Prior authorization appeal generator
  - FastAPI endpoint validation

Run with: pytest tests/test_codemed.py -v
"""

import sys
from pathlib import Path

import pytest

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))


# =============================================================================
# Fixtures
# =============================================================================

SAMPLE_CLINICAL_NOTE = """
SUBJECTIVE:
Patient is a 72-year-old female presenting for follow-up of Type 2 Diabetes
and Chronic Kidney Disease Stage 4. She reports checking blood glucose at home
twice daily with readings between 140-190 mg/dL. Blood pressure has been
stable. No edema noted.

OBJECTIVE:
Vitals: BP 138/84, HR 72, Weight 165 lbs, O2 Sat 98%.
Physical exam: No peripheral edema. Neurological exam intact.
Lab results reviewed: HbA1c 8.1% (target <7.5%), serum creatinine 2.6 mg/dL,
eGFR 26 mL/min. BMP reviewed with electrolytes within normal limits.
Urinalysis: trace protein.

ASSESSMENT:
1. Type 2 Diabetes Mellitus, uncontrolled — A1c above goal.
2. Chronic Kidney Disease, Stage 4 — eGFR 26, mildly worse vs. 3 months ago.

PLAN:
1. Metformin dose adjusted, held due to CKD. Started empagliflozin 10mg daily.
   Counseled patient on low-carb diabetic diet.
2. Lisinopril continued at 20mg daily for renal protection.
   Referral to nephrology for CKD Stage 4 management.
   Repeat renal function in 3 months.
"""

SAMPLE_DENIAL = {
    "patient_name": "Mary Smith",
    "patient_dob": "1952-06-14",
    "patient_id": "PT-2024-11234",
    "insurance_member_id": "MBR-998877665",
    "provider_name": "Springfield Internal Medicine",
    "provider_npi": "9876543210",
    "service_date": "2024-12-01",
    "claim_number": "CLM-2024-889922",
    "denied_cpt_codes": ["93224"],
    "diagnosis_codes": ["I48.0", "R00.1"],
    "denial_reason": "Medical necessity not demonstrated for extended ECG monitoring",
    "denial_date": "2024-12-15",
    "payer_name": "Aetna",
    "clinical_summary": "Patient with paroxysmal atrial fibrillation, symptomatic palpitations.",
    "meat_evidence": [
        "MONITORING: Holter monitor non-diagnostic; palpitations persist.",
        "ASSESSMENT: Paroxysmal AFib (I48.0) — extended monitoring indicated.",
    ],
    "policy_ids": ["L33822"],
}


# =============================================================================
# HCC Engine Tests
# =============================================================================

class TestHCCEngine:
    """Tests for V28 HCC hierarchy enforcement."""

    @pytest.fixture(autouse=True)
    def setup_engine(self):
        from codemed.hcc_engine import HCCEngine
        self.engine = HCCEngine()

    def test_engine_initialises(self):
        """Engine loads V28 crosswalk on init."""
        from codemed.hcc_engine import HCCEngine
        engine = HCCEngine()
        assert len(engine.get_supported_icd10_codes()) > 0

    def test_map_known_icd10_returns_hcc(self):
        """Known ICD-10 code maps to an HCC."""
        hcc = self.engine.map_icd10_to_hcc("E11.9")
        assert hcc is not None
        assert hcc.hcc_number == 19
        assert hcc.source_icd10 == "E11.9"
        assert hcc.raf_weight > 0

    def test_map_unknown_icd10_returns_none(self):
        """Unknown ICD-10 code returns None."""
        result = self.engine.map_icd10_to_hcc("Z99.ZZZ")
        assert result is None

    def test_icd10_code_normalised_to_uppercase(self):
        """Lowercase ICD-10 code is normalised."""
        hcc = self.engine.map_icd10_to_hcc("e11.9")
        assert hcc is not None
        assert hcc.source_icd10 == "E11.9"

    def test_no_conflict_single_code(self):
        """Single code — no hierarchy conflict."""
        result = self.engine.enforce_hierarchy(["E11.9"])
        assert len(result.active_hccs) == 1
        assert len(result.suppressed_hccs) == 0
        assert len(result.hierarchy_conflicts) == 0

    def test_diabetes_hierarchy_enforcement(self):
        """HCC 18 (DM with complications) trumps HCC 19 (DM without complications)."""
        result = self.engine.enforce_hierarchy(["E11.40", "E11.9"])
        assert len(result.hierarchy_conflicts) == 1
        active_hccs = [h.hcc_number for h in result.active_hccs]
        suppressed_hccs = [h.hcc_number for h in result.suppressed_hccs]
        assert 18 in active_hccs   # E11.40 = HCC 18 (with complications)
        assert 19 in suppressed_hccs  # E11.9 = HCC 19 (without complications)

    def test_ckd_hierarchy_enforcement(self):
        """Stage 5 CKD trumps Stage 4 and Stage 3b."""
        result = self.engine.enforce_hierarchy(["N18.5", "N18.4", "N18.32"])
        active_hccs = [h.hcc_number for h in result.active_hccs]
        assert 330 in active_hccs  # N18.5 = HCC 330 (Stage 5)
        assert 331 not in active_hccs
        assert 332 not in active_hccs

    def test_no_conflict_different_groups(self):
        """Codes in different hierarchy groups — no conflicts."""
        result = self.engine.enforce_hierarchy(["E11.9", "I50.22", "G30.9"])
        assert len(result.hierarchy_conflicts) == 0
        assert len(result.active_hccs) == 3

    def test_raf_before_gte_raf_after(self):
        """RAF before enforcement >= RAF after (hierarchy can only reduce)."""
        result = self.engine.enforce_hierarchy(["E11.40", "E11.9", "N18.4"])
        assert result.raf_before_enforcement >= result.raf_after_enforcement

    def test_raf_delta_negative_on_conflict(self):
        """RAF delta is negative when codes are suppressed."""
        result = self.engine.enforce_hierarchy(["E11.40", "E11.9"])
        assert result.raf_delta <= 0

    def test_unmapped_codes_tracked(self):
        """Unmapped codes are returned in unmapped_codes list."""
        result = self.engine.enforce_hierarchy(["E11.9", "Z99.999"])
        assert "Z99.999" in result.unmapped_codes

    def test_empty_input_returns_empty_result(self):
        """Empty code list returns empty result."""
        result = self.engine.enforce_hierarchy([])
        assert len(result.all_hccs) == 0
        assert result.raf_before_enforcement == 0.0
        assert result.raf_after_enforcement == 0.0

    def test_suppressed_hcc_has_trumped_by(self):
        """Suppressed HCC records which HCC trumped it."""
        result = self.engine.enforce_hierarchy(["E11.40", "E11.9"])
        suppressed = result.suppressed_hccs
        assert len(suppressed) == 1
        assert suppressed[0].trumped_by_hcc is not None

    def test_get_hcc_info(self):
        """get_hcc_info returns codes for a given HCC number."""
        info = self.engine.get_hcc_info(19)
        assert len(info) > 0
        assert all(item["hcc_number"] == 19 for item in info)

    def test_summary_has_required_keys(self):
        """result.summary() contains all expected keys."""
        result = self.engine.enforce_hierarchy(["E11.9"])
        summary = result.summary()
        for key in ["total_icd10_codes", "hccs_found", "hccs_active",
                    "hccs_suppressed", "raf_before", "raf_after",
                    "raf_delta", "hierarchy_conflicts"]:
            assert key in summary, f"Missing key: {key}"


# =============================================================================
# MEAT Extractor Tests
# =============================================================================

class TestMEATExtractor:
    """Tests for MEAT evidence extraction engine."""

    @pytest.fixture(autouse=True)
    def setup_extractor(self):
        from codemed.meat_extractor import MEATExtractor
        self.extractor = MEATExtractor()

    def test_extractor_initialises(self):
        """Extractor loads patterns on init."""
        from codemed.meat_extractor import MEATExtractor
        ext = MEATExtractor()
        assert ext is not None

    def test_extract_returns_meat_result(self):
        """extract() returns a MEATResult object."""
        from codemed.meat_extractor import MEATResult
        result = self.extractor.extract(
            clinical_note=SAMPLE_CLINICAL_NOTE,
            icd10_codes=["E11.9", "N18.4"],
        )
        assert isinstance(result, MEATResult)

    def test_note_metadata_captured(self):
        """Note length and sentence count are captured."""
        result = self.extractor.extract(
            clinical_note=SAMPLE_CLINICAL_NOTE,
            icd10_codes=["E11.9"],
        )
        assert result.note_length == len(SAMPLE_CLINICAL_NOTE)
        assert result.sentence_count > 0

    def test_diagnoses_created_for_each_code(self):
        """One DiagnosisSupport is created per ICD-10 code."""
        result = self.extractor.extract(
            clinical_note=SAMPLE_CLINICAL_NOTE,
            icd10_codes=["E11.9", "N18.4"],
        )
        assert len(result.diagnoses) == 2

    def test_defensibility_score_in_range(self):
        """Defensibility score is between 0 and 100."""
        result = self.extractor.extract(
            clinical_note=SAMPLE_CLINICAL_NOTE,
            icd10_codes=["E11.9"],
        )
        for diag in result.diagnoses:
            assert 0.0 <= diag.defensibility_score <= 100.0

    def test_overall_defensibility_score_in_range(self):
        """Overall defensibility score is between 0 and 100."""
        result = self.extractor.extract(
            clinical_note=SAMPLE_CLINICAL_NOTE,
            icd10_codes=["E11.9", "N18.4"],
        )
        assert 0.0 <= result.overall_defensibility_score <= 100.0

    def test_rich_note_produces_high_score(self):
        """A note with all MEAT categories produces a high defensibility score."""
        result = self.extractor.extract(
            clinical_note=SAMPLE_CLINICAL_NOTE,
            icd10_codes=["E11.9"],
        )
        # Sample note has M, E, A, T — should score well
        assert result.overall_defensibility_score >= 50.0

    def test_empty_note_returns_zero_score(self):
        """Empty note returns no evidence and zero defensibility."""
        result = self.extractor.extract(
            clinical_note="",
            icd10_codes=["E11.9"],
        )
        assert result.overall_defensibility_score == 0.0

    def test_summary_has_required_keys(self):
        """result.summary() contains all expected keys."""
        result = self.extractor.extract(
            clinical_note=SAMPLE_CLINICAL_NOTE,
            icd10_codes=["E11.9"],
        )
        summary = result.summary()
        for key in ["note_length", "sentence_count", "diagnoses_analyzed",
                    "diagnoses_supported", "overall_defensibility_score"]:
            assert key in summary, f"Missing key: {key}"

    def test_diagnosis_to_dict_has_required_keys(self):
        """DiagnosisSupport.to_dict() returns all expected keys."""
        result = self.extractor.extract(
            clinical_note=SAMPLE_CLINICAL_NOTE,
            icd10_codes=["E11.9"],
            code_descriptions={"E11.9": "Type 2 Diabetes without complications"},
        )
        d = result.diagnoses[0].to_dict()
        for key in ["icd10_code", "description", "categories_found",
                    "missing_categories", "defensibility_score",
                    "is_supported", "evidence_count", "evidence"]:
            assert key in d, f"Missing key: {key}"

    def test_score_note_returns_all_categories(self):
        """score_note() returns scores for all MEAT categories + overall."""
        scores = self.extractor.score_note(SAMPLE_CLINICAL_NOTE)
        assert "monitoring" in scores
        assert "evaluation" in scores
        assert "assessment" in scores
        assert "treatment" in scores
        assert "overall" in scores
        assert 0.0 <= scores["overall"] <= 1.0

    def test_code_descriptions_used_in_output(self):
        """Custom code descriptions appear in the output."""
        result = self.extractor.extract(
            clinical_note=SAMPLE_CLINICAL_NOTE,
            icd10_codes=["E11.9"],
            code_descriptions={"E11.9": "Custom description for diabetes"},
        )
        assert result.diagnoses[0].description == "Custom description for diabetes"


# =============================================================================
# NLQ Engine Tests
# =============================================================================

class TestNLQEngine:
    """Tests for natural language coding query engine."""

    @pytest.fixture(autouse=True)
    def setup_engine(self):
        from codemed.nlq_engine import NLQEngine
        self.engine = NLQEngine()

    def test_engine_initialises(self):
        """NLQ engine initialises with default code tables."""
        from codemed.nlq_engine import NLQEngine
        engine = NLQEngine()
        assert engine is not None

    def test_search_returns_results(self):
        """search() returns non-empty results for common queries."""
        results = self.engine.search("cardiac monitoring atrial fibrillation")
        assert len(results) > 0

    def test_search_results_are_sorted_by_relevance(self):
        """Results are sorted by relevance_score descending."""
        results = self.engine.search("diabetes", max_results=10)
        scores = [r.relevance_score for r in results]
        assert scores == sorted(scores, reverse=True)

    def test_search_relevance_score_in_range(self):
        """Relevance score is between 0.0 and 1.0."""
        results = self.engine.search("knee replacement surgery")
        for r in results:
            assert 0.0 <= r.relevance_score <= 1.0

    def test_search_code_type_filter_icd10(self):
        """Filtering by ICD-10 returns only ICD-10 codes."""
        results = self.engine.search("diabetes", code_types=["ICD-10"])
        for r in results:
            assert r.code_type == "ICD-10"

    def test_search_code_type_filter_cpt(self):
        """Filtering by CPT returns only CPT codes."""
        results = self.engine.search("monitoring cardiac", code_types=["CPT"])
        for r in results:
            assert r.code_type == "CPT"

    def test_search_max_results_respected(self):
        """max_results parameter is respected."""
        results = self.engine.search("diabetes", max_results=3)
        assert len(results) <= 3

    def test_search_empty_query_returns_empty(self):
        """Empty query returns empty list."""
        results = self.engine.search("")
        assert results == []

    def test_search_result_has_required_fields(self):
        """Each result has the required fields."""
        results = self.engine.search("heart failure")
        if results:
            r = results[0]
            assert hasattr(r, "code")
            assert hasattr(r, "description")
            assert hasattr(r, "code_type")
            assert hasattr(r, "category")
            assert hasattr(r, "relevance_score")

    def test_lookup_known_icd10_code(self):
        """lookup_code() finds a known ICD-10 code."""
        result = self.engine.lookup_code("E11.9")
        assert result is not None
        assert result.code == "E11.9"
        assert result.code_type == "ICD-10"
        assert result.relevance_score == 1.0

    def test_lookup_known_cpt_code(self):
        """lookup_code() finds a known CPT code."""
        result = self.engine.lookup_code("99214")
        assert result is not None
        assert result.code == "99214"
        assert result.code_type == "CPT"

    def test_lookup_unknown_code_returns_none(self):
        """lookup_code() returns None for unknown code."""
        result = self.engine.lookup_code("ZZZZ9")
        assert result is None

    def test_lookup_case_insensitive(self):
        """lookup_code() handles lowercase input."""
        result = self.engine.lookup_code("e11.9")
        assert result is not None

    def test_suggest_codes_returns_three_types(self):
        """suggest_codes() returns results for all three code types."""
        suggestions = self.engine.suggest_codes(
            diagnosis_text="atrial fibrillation",
            procedure_text="cardiac monitoring",
        )
        assert "ICD-10" in suggestions
        assert "CPT" in suggestions
        assert "HCPCS" in suggestions

    def test_suggest_codes_max_per_type(self):
        """suggest_codes() respects max_per_type."""
        suggestions = self.engine.suggest_codes(
            diagnosis_text="diabetes",
            max_per_type=2,
        )
        for code_type, results in suggestions.items():
            assert len(results) <= 2

    def test_to_dict_returns_expected_keys(self):
        """CodeSearchResult.to_dict() has all required keys."""
        results = self.engine.search("diabetes", code_types=["ICD-10"])
        if results:
            d = results[0].to_dict()
            for key in ["code", "description", "code_type", "category",
                        "relevance_score", "matched_terms"]:
                assert key in d, f"Missing key: {key}"


# =============================================================================
# Appeals Generator Tests
# =============================================================================

class TestAppealsGenerator:
    """Tests for prior auth appeal letter generator."""

    @pytest.fixture(autouse=True)
    def setup_generator(self):
        from codemed.appeals_generator import AppealsGenerator, DenialScenario
        self.generator = AppealsGenerator()
        self.DenialScenario = DenialScenario

    def _make_scenario(self, **overrides):
        """Create a DenialScenario with sample data."""
        data = dict(SAMPLE_DENIAL)
        data.update(overrides)
        return self.DenialScenario(**data)

    def test_generator_initialises(self):
        """Generator loads policy index on init."""
        from codemed.appeals_generator import AppealsGenerator
        gen = AppealsGenerator()
        assert gen is not None

    def test_generate_returns_appeal_letter(self):
        """generate() returns an AppealLetter object."""
        from codemed.appeals_generator import AppealLetter
        scenario = self._make_scenario()
        letter = self.generator.generate(scenario)
        assert isinstance(letter, AppealLetter)

    def test_letter_text_not_empty(self):
        """Generated letter text is non-empty."""
        scenario = self._make_scenario()
        letter = self.generator.generate(scenario)
        assert len(letter.letter_text) > 100

    def test_letter_contains_patient_name(self):
        """Letter contains the patient name."""
        scenario = self._make_scenario()
        letter = self.generator.generate(scenario)
        assert "Mary Smith" in letter.letter_text

    def test_letter_contains_denied_cpt_code(self):
        """Letter references the denied CPT code."""
        scenario = self._make_scenario()
        letter = self.generator.generate(scenario)
        assert "93224" in letter.letter_text

    def test_letter_contains_policy_citation(self):
        """Letter cites the specified LCD policy."""
        scenario = self._make_scenario(policy_ids=["L33822"])
        letter = self.generator.generate(scenario)
        assert "L33822" in letter.letter_text

    def test_letter_contains_regulatory_citations(self):
        """Letter includes regulatory citations."""
        scenario = self._make_scenario()
        letter = self.generator.generate(scenario)
        assert "Social Security Act" in letter.letter_text or "1862" in letter.letter_text

    def test_letter_contains_meat_evidence(self):
        """MEAT evidence quotes appear in the letter."""
        scenario = self._make_scenario()
        letter = self.generator.generate(scenario)
        assert "MONITORING" in letter.letter_text or "ASSESSMENT" in letter.letter_text

    def test_letter_without_meat_has_no_meat_section(self):
        """When include_meat=False, MEAT section is omitted."""
        scenario = self._make_scenario()
        letter = self.generator.generate(scenario, include_meat=False)
        assert "MEAT EVIDENCE" not in letter.letter_text

    def test_word_count_is_positive(self):
        """Word count is positive."""
        scenario = self._make_scenario()
        letter = self.generator.generate(scenario)
        assert letter.word_count > 0
        assert letter.word_count == len(letter.letter_text.split())

    def test_policy_citations_list_returned(self):
        """Policy citations are returned in the letter."""
        scenario = self._make_scenario(policy_ids=["L33822"])
        letter = self.generator.generate(scenario)
        assert len(letter.policy_citations) > 0

    def test_to_dict_has_required_keys(self):
        """AppealLetter.to_dict() has all required keys."""
        scenario = self._make_scenario()
        letter = self.generator.generate(scenario)
        d = letter.to_dict()
        for key in ["letter_text", "word_count", "policy_citations",
                    "regulatory_citations", "generated_at"]:
            assert key in d, f"Missing key: {key}"

    def test_find_applicable_policies_by_cpt(self):
        """find_applicable_policies returns policies matching CPT codes."""
        policies = self.generator.find_applicable_policies(
            cpt_codes=["93224"],
            icd10_codes=[],
        )
        assert "L33822" in policies

    def test_find_applicable_policies_by_icd10(self):
        """find_applicable_policies returns policies matching ICD-10 codes."""
        policies = self.generator.find_applicable_policies(
            cpt_codes=[],
            icd10_codes=["I48.0"],
        )
        assert len(policies) > 0

    def test_no_matching_policies_still_generates_letter(self):
        """Letter is generated even when no specific policies are found."""
        scenario = self._make_scenario(
            denied_cpt_codes=["99999"],
            diagnosis_codes=["Z00.00"],
            policy_ids=[],
        )
        letter = self.generator.generate(scenario)
        assert len(letter.letter_text) > 100


# =============================================================================
# API Validation Tests (no running server required)
# =============================================================================

class TestAPIModels:
    """Tests for FastAPI request/response model validation."""

    def test_hcc_request_normalises_codes(self):
        """HCCEnforceRequest normalises ICD-10 codes to uppercase."""
        from codemed.api import HCCEnforceRequest
        req = HCCEnforceRequest(icd10_codes=["e11.9", "n18.4"])
        assert req.icd10_codes == ["E11.9", "N18.4"]

    def test_hcc_request_requires_at_least_one_code(self):
        """HCCEnforceRequest rejects empty code list."""
        from codemed.api import HCCEnforceRequest
        with pytest.raises(Exception):
            HCCEnforceRequest(icd10_codes=[])

    def test_code_search_request_validates_code_type(self):
        """CodeSearchRequest rejects invalid code_type."""
        from codemed.api import CodeSearchRequest
        with pytest.raises(Exception):
            CodeSearchRequest(
                query="test",
                code_types=["INVALID_TYPE"],
            )

    def test_code_search_request_validates_mode(self):
        """CodeSearchRequest rejects invalid mode."""
        from codemed.api import CodeSearchRequest
        with pytest.raises(Exception):
            CodeSearchRequest(query="test", mode="invalid_mode")

    def test_code_search_request_valid(self):
        """Valid CodeSearchRequest is accepted."""
        from codemed.api import CodeSearchRequest
        req = CodeSearchRequest(
            query="cardiac monitoring",
            code_types=["ICD-10", "CPT"],
            max_results=5,
            mode="keyword",
        )
        assert req.query == "cardiac monitoring"
        assert req.max_results == 5

    def test_meat_request_normalises_codes(self):
        """MEATExtractRequest normalises ICD-10 codes."""
        from codemed.api import MEATExtractRequest
        req = MEATExtractRequest(
            clinical_note="Patient presents with diabetes and CKD.",
            icd10_codes=["e11.9", "n18.4"],
        )
        assert req.icd10_codes == ["E11.9", "N18.4"]


# =============================================================================
# Integration: HCC + MEAT pipeline
# =============================================================================

class TestIntegrationPipeline:
    """Integration tests combining multiple CodeMed AI components."""

    def test_hcc_then_meat_pipeline(self):
        """Enforce HCC hierarchy, then extract MEAT evidence for active HCCs."""
        from codemed.hcc_engine import HCCEngine
        from codemed.meat_extractor import MEATExtractor

        engine = HCCEngine()
        extractor = MEATExtractor()

        # Step 1: Enforce hierarchy
        input_codes = ["E11.40", "E11.9", "N18.4"]
        hcc_result = engine.enforce_hierarchy(input_codes)

        # Only active HCC source codes should be documented
        active_icd10s = [h.source_icd10 for h in hcc_result.active_hccs]
        assert "E11.40" in active_icd10s  # HCC 18 wins over HCC 19

        # Step 2: Extract MEAT for active codes
        meat_result = extractor.extract(
            clinical_note=SAMPLE_CLINICAL_NOTE,
            icd10_codes=active_icd10s,
        )
        assert len(meat_result.diagnoses) == len(active_icd10s)
        assert meat_result.overall_defensibility_score >= 0.0

    def test_nlq_then_hcc_pipeline(self):
        """Search for diagnosis codes, then enforce HCC hierarchy."""
        from codemed.nlq_engine import NLQEngine
        from codemed.hcc_engine import HCCEngine

        nlq = NLQEngine()
        hcc = HCCEngine()

        # Step 1: Find diabetes ICD-10 codes via NLQ
        results = nlq.search("diabetes complications kidney", code_types=["ICD-10"])
        icd10_codes = [r.code for r in results[:3]]
        assert len(icd10_codes) > 0

        # Step 2: Enforce hierarchy
        hcc_result = hcc.enforce_hierarchy(icd10_codes)
        # Should return a valid result without errors
        summary = hcc_result.summary()
        assert "hccs_found" in summary

    def test_full_codemed_pipeline(self):
        """Full pipeline: HCC → MEAT → Appeals."""
        from codemed.hcc_engine import HCCEngine
        from codemed.meat_extractor import MEATExtractor
        from codemed.appeals_generator import AppealsGenerator, DenialScenario

        engine = HCCEngine()
        extractor = MEATExtractor()
        generator = AppealsGenerator()

        # 1. Enforce HCC hierarchy
        codes = ["E11.9", "I48.0"]
        hcc_result = engine.enforce_hierarchy(codes)
        active_codes = [h.source_icd10 for h in hcc_result.active_hccs]

        # 2. Extract MEAT
        meat_result = extractor.extract(
            clinical_note=SAMPLE_CLINICAL_NOTE,
            icd10_codes=active_codes,
        )
        meat_quotes = [
            e.quote for d in meat_result.diagnoses for e in d.evidence[:1]
        ]

        # 3. Generate appeal
        scenario = DenialScenario(
            patient_name="Test Patient",
            patient_dob="1960-01-01",
            patient_id="PT-001",
            insurance_member_id="MBR-001",
            provider_name="Test Provider",
            provider_npi="1111111111",
            service_date="2024-12-01",
            claim_number="CLM-001",
            denied_cpt_codes=["93224"],
            diagnosis_codes=active_codes,
            denial_reason="Not medically necessary",
            denial_date="2024-12-15",
            payer_name="TestPayer",
            clinical_summary="Patient with diabetes and AFib.",
            meat_evidence=meat_quotes[:2],
            policy_ids=["L33822"],
        )
        letter = generator.generate(scenario)
        assert len(letter.letter_text) > 0
        assert letter.word_count > 50
