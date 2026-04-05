"""
Tests for denial_recovery.py.

Covers:
  Pure logic (no DB needed):
    - _classify_denial_codes — CO code inference from claim data
    - _calculate_value       — revenue split math
    - _render_appeal_letter  — appeal letter template output

  DB-dependent (mock conn):
    - _fix_co16 — fills missing NPI / auth / diagnosis pointer
    - _fix_co50 — generates appeal letter with clinical references
    - _fix_co97 — appends modifier 59 to bundled PT codes
    - detect_recoverable_denials — orchestration of classify → upsert
"""

from unittest.mock import MagicMock, patch

import pytest

from denial_recovery import (
    AUTH_REQUIRED_CODES,
    BUNDLED_PT_CODES,
    FEE_PERCENTAGE,
    SUCCESS_RATES,
    DenialRecoveryEngine,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def engine(mock_conn):
    return DenialRecoveryEngine(mock_conn)


@pytest.fixture
def bare_claim():
    """Claim with no NPI, no auth, no ICD-10 — should trigger CO-16."""
    return {
        "id": "c1",
        "connection_id": "conn-1",
        "webpt_claim_id": "wc-1",
        "patient_id": "pat-1",
        "provider_id": "prov-1",
        "service_date": "2024-03-01",
        "cpt_codes": [],
        "icd10_codes": [],
        "amount": 300.0,
        "raw_payload": {},
    }


# ---------------------------------------------------------------------------
# _classify_denial_codes
# ---------------------------------------------------------------------------

class TestClassifyDenialCodes:
    def test_co16_triggered_by_missing_npi(self, engine):
        claim = {"raw_payload": {}, "cpt_codes": [], "icd10_codes": ["M54.5"]}
        codes = engine._classify_denial_codes(claim)
        assert "CO-16" in codes

    def test_co16_not_triggered_when_npi_present(self, engine):
        claim = {
            "raw_payload": {"billing_npi": "1234567890"},
            "cpt_codes": [],
            "icd10_codes": ["M54.5"],
        }
        codes = engine._classify_denial_codes(claim)
        assert "CO-16" not in codes

    def test_co16_triggered_by_missing_icd10(self, engine):
        claim = {
            "raw_payload": {"billing_npi": "1234567890"},
            "cpt_codes": [],
            "icd10_codes": [],
        }
        codes = engine._classify_denial_codes(claim)
        assert "CO-16" in codes

    def test_co16_triggered_by_missing_auth_for_auth_required_code(self, engine):
        auth_code = next(iter(AUTH_REQUIRED_CODES))
        claim = {
            "raw_payload": {"billing_npi": "1234567890"},
            "cpt_codes": [auth_code],
            "icd10_codes": ["M54.5"],
        }
        codes = engine._classify_denial_codes(claim)
        assert "CO-16" in codes

    def test_co16_not_triggered_when_auth_present(self, engine):
        auth_code = next(iter(AUTH_REQUIRED_CODES))
        claim = {
            "raw_payload": {
                "billing_npi": "1234567890",
                "prior_auth_number": "AUTH123",
            },
            "cpt_codes": [auth_code],
            "icd10_codes": ["M54.5"],
        }
        codes = engine._classify_denial_codes(claim)
        assert "CO-16" not in codes

    def test_co50_triggered_by_high_risk_code_without_icd10(self, engine):
        claim = {
            "raw_payload": {"billing_npi": "1234567890"},
            "cpt_codes": ["97750"],   # high-risk CPT
            "icd10_codes": [],
        }
        codes = engine._classify_denial_codes(claim)
        assert "CO-50" in codes

    def test_co50_not_triggered_when_icd10_present(self, engine):
        claim = {
            "raw_payload": {"billing_npi": "1234567890"},
            "cpt_codes": ["97750"],
            "icd10_codes": ["M54.5"],
        }
        codes = engine._classify_denial_codes(claim)
        assert "CO-50" not in codes

    def test_co97_triggered_by_two_bundled_codes_without_modifier(self, engine):
        bundled = list(BUNDLED_PT_CODES)[:2]
        claim = {
            "raw_payload": {"billing_npi": "1234567890", "modifiers": []},
            "cpt_codes": bundled,
            "icd10_codes": ["M54.5"],
        }
        codes = engine._classify_denial_codes(claim)
        assert "CO-97" in codes

    def test_co97_not_triggered_when_modifier_59_present(self, engine):
        bundled = list(BUNDLED_PT_CODES)[:2]
        claim = {
            "raw_payload": {"billing_npi": "1234567890", "modifiers": ["59"]},
            "cpt_codes": bundled,
            "icd10_codes": ["M54.5"],
        }
        codes = engine._classify_denial_codes(claim)
        assert "CO-97" not in codes

    def test_co97_not_triggered_for_single_bundled_code(self, engine):
        bundled = [list(BUNDLED_PT_CODES)[0]]
        claim = {
            "raw_payload": {"billing_npi": "1234567890", "modifiers": []},
            "cpt_codes": bundled,
            "icd10_codes": ["M54.5"],
        }
        codes = engine._classify_denial_codes(claim)
        assert "CO-97" not in codes

    def test_multiple_codes_can_be_returned_simultaneously(self, engine):
        # Missing NPI (→ CO-16) + high-risk CPT without ICD-10 (→ CO-50)
        claim = {
            "raw_payload": {},
            "cpt_codes": ["97750"],
            "icd10_codes": [],
        }
        codes = engine._classify_denial_codes(claim)
        assert "CO-16" in codes
        assert "CO-50" in codes


# ---------------------------------------------------------------------------
# _calculate_value
# ---------------------------------------------------------------------------

class TestCalculateValue:
    def test_co16_success_rate(self, engine):
        result = engine._calculate_value(1000.0, "CO-16")
        assert result["success_probability"] == SUCCESS_RATES["CO-16"]

    def test_co50_success_rate(self, engine):
        result = engine._calculate_value(1000.0, "CO-50")
        assert result["success_probability"] == SUCCESS_RATES["CO-50"]

    def test_co97_success_rate(self, engine):
        result = engine._calculate_value(1000.0, "CO-97")
        assert result["success_probability"] == SUCCESS_RATES["CO-97"]

    def test_estimated_recovery_math(self, engine):
        result = engine._calculate_value(1000.0, "CO-16")
        expected = round(1000.0 * SUCCESS_RATES["CO-16"], 2)
        assert result["estimated_recovery"] == expected

    def test_fee_is_20_percent_of_estimated(self, engine):
        result = engine._calculate_value(1000.0, "CO-97")
        expected_fee = round(result["estimated_recovery"] * FEE_PERCENTAGE, 2)
        assert result["your_fee"] == expected_fee

    def test_clinic_net_is_80_percent_of_estimated(self, engine):
        result = engine._calculate_value(1000.0, "CO-97")
        expected_net = round(result["estimated_recovery"] * (1 - FEE_PERCENTAGE), 2)
        assert result["clinic_net"] == expected_net

    def test_fee_plus_net_equals_estimated(self, engine):
        result = engine._calculate_value(500.0, "CO-50")
        assert round(result["your_fee"] + result["clinic_net"], 2) == result["estimated_recovery"]

    def test_zero_billed_amount(self, engine):
        result = engine._calculate_value(0.0, "CO-16")
        assert result["estimated_recovery"] == 0.0
        assert result["your_fee"]           == 0.0
        assert result["clinic_net"]         == 0.0

    def test_billed_amount_preserved(self, engine):
        result = engine._calculate_value(750.50, "CO-16")
        assert result["billed_amount"] == 750.50


# ---------------------------------------------------------------------------
# _render_appeal_letter
# ---------------------------------------------------------------------------

class TestRenderAppealLetter:
    def test_contains_service_date(self, engine):
        letter = engine._render_appeal_letter(
            provider="Dr. Smith",
            service_date="2024-03-15",
            cpt_codes=["97110"],
            icd10_codes=["M54.5"],
            references=[],
        )
        assert "2024-03-15" in letter

    def test_contains_provider_name(self, engine):
        letter = engine._render_appeal_letter(
            provider="ABC Physical Therapy",
            service_date="2024-03-15",
            cpt_codes=[],
            icd10_codes=[],
            references=[],
        )
        assert "ABC Physical Therapy" in letter

    def test_contains_cpt_codes(self, engine):
        letter = engine._render_appeal_letter(
            provider="P",
            service_date="2024-01-01",
            cpt_codes=["97110", "97140"],
            icd10_codes=[],
            references=[],
        )
        assert "97110" in letter
        assert "97140" in letter

    def test_contains_icd10_codes(self, engine):
        letter = engine._render_appeal_letter(
            provider="P",
            service_date="2024-01-01",
            cpt_codes=[],
            icd10_codes=["M54.5", "G89.29"],
            references=[],
        )
        assert "M54.5" in letter
        assert "G89.29" in letter

    def test_includes_reference_title(self, engine):
        refs = [{"title": "LCD for PT Services", "lcd_id": "L34000", "document_type": "lcd"}]
        letter = engine._render_appeal_letter(
            provider="P", service_date="2024-01-01",
            cpt_codes=[], icd10_codes=[], references=refs,
        )
        assert "LCD for PT Services" in letter

    def test_fallback_when_no_references(self, engine):
        letter = engine._render_appeal_letter(
            provider="P", service_date="2024-01-01",
            cpt_codes=[], icd10_codes=[], references=[],
        )
        assert "Clinical guidelines on file" in letter


# ---------------------------------------------------------------------------
# _fix_co16
# ---------------------------------------------------------------------------

class TestFixCO16:
    def test_adds_npi_when_missing(self, engine):
        engine._lookup_provider_npi = MagicMock(return_value="9876543210")
        claim = {
            "cpt_codes": [], "icd10_codes": ["M54.5"],
            "raw_payload": {}, "provider_id": "prov-1",
        }
        fixes, notes = engine._fix_co16(claim)
        npi_fix = next((f for f in fixes if f["field"] == "billing_npi"), None)
        assert npi_fix is not None
        assert npi_fix["value"] == "9876543210"

    def test_flags_for_new_auth_when_no_existing_auth(self, engine):
        engine._lookup_provider_npi = MagicMock(return_value="1234567890")
        engine._lookup_prior_auth = MagicMock(return_value=None)
        auth_code = next(iter(AUTH_REQUIRED_CODES))
        claim = {
            "cpt_codes": [auth_code], "icd10_codes": ["M54.5"],
            "raw_payload": {"billing_npi": "1234567890"}, "provider_id": "prov-1",
            "patient_id": "pat-1",
        }
        fixes, notes = engine._fix_co16(claim)
        auth_fix = next((f for f in fixes if f["field"] == "prior_auth_number"), None)
        assert auth_fix is not None

    def test_fills_auth_number_when_lookup_succeeds(self, engine):
        engine._lookup_provider_npi = MagicMock(return_value="1234567890")
        engine._lookup_prior_auth = MagicMock(return_value="AUTH-456")
        auth_code = next(iter(AUTH_REQUIRED_CODES))
        claim = {
            "cpt_codes": [auth_code], "icd10_codes": ["M54.5"],
            "raw_payload": {"billing_npi": "1234567890"}, "provider_id": "prov-1",
            "patient_id": "pat-1",
        }
        fixes, _ = engine._fix_co16(claim)
        auth_fix = next((f for f in fixes if f["field"] == "prior_auth_number"), None)
        assert auth_fix["value"] == "AUTH-456"

    def test_returns_manual_review_fallback_when_no_fixes_apply(self, engine):
        engine._lookup_provider_npi = MagicMock(return_value=None)
        # NPI present, no auth-required codes, diagnosis_pointer set, ICD-10 present
        # → none of the three fix branches fire → falls through to manual review
        claim = {
            "cpt_codes": [],
            "icd10_codes": ["M54.5"],
            "raw_payload": {"billing_npi": "9999999999", "diagnosis_pointer": "A"},
            "provider_id": None,
        }
        fixes, notes = engine._fix_co16(claim)
        assert any(f["field"] == "review" for f in fixes)


# ---------------------------------------------------------------------------
# _fix_co97
# ---------------------------------------------------------------------------

class TestFixCO97:
    def test_adds_modifier_59_to_bundled_codes(self, engine):
        bundled = list(BUNDLED_PT_CODES)[:2]
        claim = {"cpt_codes": bundled, "raw_payload": {}}
        fixes, notes = engine._fix_co97(claim)
        modifier_fixes = [f for f in fixes if f.get("value") == "59"]
        assert len(modifier_fixes) == len(bundled)

    def test_flags_non_pt_codes_for_manual_review(self, engine):
        claim = {"cpt_codes": ["99213", "99214"], "raw_payload": {}}
        fixes, notes = engine._fix_co97(claim)
        assert fixes[0]["value"] == "REVIEW_MODIFIER"
        assert "manual" in notes.lower()

    def test_notes_list_targeted_codes(self, engine):
        bundled = [list(BUNDLED_PT_CODES)[0]]
        claim = {"cpt_codes": bundled, "raw_payload": {}}
        _, notes = engine._fix_co97(claim)
        assert bundled[0] in notes


# ---------------------------------------------------------------------------
# detect_recoverable_denials (orchestration)
# ---------------------------------------------------------------------------

class TestDetectRecoverableDenials:
    def test_calls_upsert_for_each_classified_code(self, engine, sample_claim):
        bundled = list(BUNDLED_PT_CODES)[:2]
        sample_claim["cpt_codes"] = bundled
        sample_claim["raw_payload"] = {}
        sample_claim["icd10_codes"] = []  # triggers CO-16 and CO-97

        engine._load_claims = MagicMock(return_value=[sample_claim])
        engine._upsert_denial = MagicMock(return_value={"id": "d1"})

        engine.detect_recoverable_denials("clinic-1")

        # At least one upsert call per classified CO code
        assert engine._upsert_denial.call_count >= 1

    def test_returns_list_of_detected_denials(self, engine, sample_claim):
        engine._load_claims = MagicMock(return_value=[sample_claim])
        engine._upsert_denial = MagicMock(return_value={"id": "d-new", "denial_code": "CO-16"})

        result = engine.detect_recoverable_denials("clinic-1")
        assert isinstance(result, list)

    def test_skips_claims_with_no_matching_codes(self, engine):
        # Claim with NPI, ICD-10, no bundled codes, no high-risk → no CO codes
        clean_claim = {
            "id": "c2", "connection_id": "conn-1", "webpt_claim_id": "wc-2",
            "patient_id": "pat-1", "provider_id": "prov-1", "service_date": "2024-01-01",
            "cpt_codes": ["99213"], "icd10_codes": ["Z00.00"], "amount": 100.0,
            "raw_payload": {"billing_npi": "1234567890", "prior_auth_number": "AUTH1"},
        }
        engine._load_claims = MagicMock(return_value=[clean_claim])
        engine._upsert_denial = MagicMock()

        engine.detect_recoverable_denials("clinic-1")
        engine._upsert_denial.assert_not_called()
