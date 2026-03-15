"""
NexusAuth — Moat Expansion Tests
==================================
Tests for the 9 new document types, 7 new scrapers, new specialties,
and new payer domains added in migration 003 / taxonomy v2.0.

All tests are unit tests — no Docker or network required.
Scraper network calls are replaced with CSV/HTML fixture data.

Run with:
  pytest tests/test_moat_expansion.py -v
  pytest tests/test_moat_expansion.py -k "TestNewDocumentTypes" -v
"""

import csv
import hashlib
import io
import sys
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))


# =============================================================================
# CSV / HTML fixture builders
# =============================================================================

def _make_zip_with_csv(filename: str, headers: list[str], rows: list[list]) -> bytes:
    """Create an in-memory ZIP file containing one CSV."""
    csv_buf = io.StringIO()
    writer = csv.writer(csv_buf)
    writer.writerow(headers)
    writer.writerows(rows)
    csv_bytes = csv_buf.getvalue().encode("utf-8")

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(filename, csv_bytes)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# MPFS fixture: 5 CPT codes
# ---------------------------------------------------------------------------
MPFS_HEADERS = [
    "HCPCS", "Modifier", "Description",
    "Work RVU", "Non-Fac PE RVU", "Fac PE RVU", "MP RVU",
    "Non-Fac Total RVU", "Fac Total RVU",
    "Non-Fac NA Ind Payment", "Fac NA Ind Payment",
    "Glob",
]
MPFS_ROWS = [
    ["99213", "", "Office/outpatient visit, est", "0.97", "1.47", "0.80", "0.07", "2.51", "1.84", "92.24", "67.61", "010"],
    ["99214", "", "Office/outpatient visit, est", "1.50", "2.07", "1.12", "0.11", "3.68", "2.73", "135.21", "100.38", "010"],
    ["99215", "", "Office/outpatient visit, est", "2.11", "2.88", "1.52", "0.15", "5.14", "3.78", "188.94", "138.95", "010"],
    ["43239", "", "EGD biopsy single/multiple", "5.22", "8.14", "4.65", "0.38", "13.74", "10.25", "504.98", "376.69", "090"],
    ["27447", "", "Total knee arthroplasty",    "22.39", "25.22", "14.28", "1.64", "49.25", "38.31", "1810.41", "1408.26", "090"],
]
MPFS_ZIP = _make_zip_with_csv("addendum_b_rvu26a.csv", MPFS_HEADERS, MPFS_ROWS)

# ---------------------------------------------------------------------------
# NCCI PTP fixture: 6 edit pairs
# ---------------------------------------------------------------------------
NCCI_PTP_HEADERS = [
    "Col_1_HCPCS_Cd", "Col_2_HCPCS_Cd",
    "Effective_Date", "Deletion_Date", "Modifier_Indicator",
]
NCCI_PTP_ROWS = [
    ["27447", "27370", "01/01/2026", "", "0"],
    ["27447", "27372", "01/01/2026", "", "0"],
    ["99213", "99212", "04/01/2025", "", "1"],
    ["43239", "43235", "01/01/2024", "", "0"],
    ["43239", "43236", "01/01/2024", "", "0"],
    ["99215", "99214", "01/01/2023", "", "1"],
]
NCCI_PTP_ZIP = _make_zip_with_csv("ncci_practitioner_py26.csv", NCCI_PTP_HEADERS, NCCI_PTP_ROWS)

# ---------------------------------------------------------------------------
# NCCI MUE fixture: 4 entries
# ---------------------------------------------------------------------------
NCCI_MUE_HEADERS = [
    "HCPCS_Code", "MUE_Value", "MUE_Adjudication_Indicator", "Rationale",
]
NCCI_MUE_ROWS = [
    ["99213", "1", "3", "Clinical"],
    ["99214", "1", "3", "Clinical"],
    ["27447", "1", "2", "Anatomical"],
    ["J0135", "10", "2", "Clinical"],
]
NCCI_MUE_ZIP = _make_zip_with_csv("ncci_mue_practitioner.csv", NCCI_MUE_HEADERS, NCCI_MUE_ROWS)

# ---------------------------------------------------------------------------
# ASP fixture: 3 J-codes
# ---------------------------------------------------------------------------
ASP_HEADERS = [
    "HCPCS_Code", "Short_Description", "Dosage_Form",
    "ASP_Unit_Cost", "Payment_Limit",
]
ASP_ROWS = [
    ["J0135", "Adalimumab injection",    "40 mg/0.4 mL", "1258.00", "1333.48"],
    ["J0178", "Aflibercept injection",   "1 mg",         "882.30",  "935.24"],
    ["J9999", "Not otherwise classified","vial",         "0.00",    "0.00"],
]
ASP_FILENAME = "asp26q1-noc.csv"
ASP_ZIP = _make_zip_with_csv(ASP_FILENAME, ASP_HEADERS, ASP_ROWS)

# ---------------------------------------------------------------------------
# DRG fixture: 4 DRGs
# ---------------------------------------------------------------------------
DRG_HEADERS = [
    "MS-DRG", "MDC", "Type", "MS-DRG Title",
    "Relative Weight", "Geometric Mean LOS", "Arithmetic Mean LOS",
]
DRG_ROWS = [
    ["470", "08", "SURG",    "Major hip and knee joint replacement without MCC", "2.1291", "2.2", "2.5"],
    ["293", "05", "MED",     "Heart failure and shock with MCC",                 "1.6021", "4.9", "5.9"],
    ["291", "05", "MED",     "Heart failure and shock without CC/MCC",           "0.7118", "2.8", "3.1"],
    ["871", "18", "MED",     "Septicemia without MV >96 hrs with MCC",           "1.8143", "5.3", "6.4"],
]
DRG_ZIP = _make_zip_with_csv("fy2026_ms_drg_weights.csv", DRG_HEADERS, DRG_ROWS)

# ---------------------------------------------------------------------------
# CARC/RARC HTML fixture
# ---------------------------------------------------------------------------
CARC_HTML = """
<html><body>
<h1>Claim Adjustment Reason Codes</h1>
<table>
  <tr><th>Code</th><th>Description</th><th>Notes</th></tr>
  <tr><td>1</td><td>Deductible amount</td><td>Group: PR</td></tr>
  <tr><td>2</td><td>Coinsurance amount</td><td>Group: PR</td></tr>
  <tr><td>4</td><td>The service/equipment/drug is not covered by the payer.</td><td>Group: CO</td></tr>
  <tr><td>16</td><td>Claim/service lacks information which is needed for adjudication.</td><td>Group: CO</td></tr>
  <tr><td>97</td><td>The benefit for this service is included in the payment for another service.</td><td>Group: CO</td></tr>
</table>
</body></html>
"""

RARC_HTML = """
<html><body>
<h1>Remittance Advice Remark Codes</h1>
<table>
  <tr><th>Code</th><th>Description</th></tr>
  <tr><td>N1</td><td>Alert: You may appeal this decision in writing within the required time limits.</td></tr>
  <tr><td>N2</td><td>This allowance has been made in accordance with the most appropriate level of care.</td></tr>
  <tr><td>MA01</td><td>Alert: If you do not agree with this payment, you have the right to appeal.</td></tr>
</table>
</body></html>
"""


# =============================================================================
# Test: New document types classified by tagger
# =============================================================================

class TestNewDocumentTypes:
    """Verify the tagger correctly classifies the 9 new document types."""

    def _tag(self, text: str):
        from tagging.tagger import DocumentTagger
        tagger = DocumentTagger()
        return tagger.tag(text)

    def test_ncci_edit_classified(self):
        text = (
            "NCCI procedure-to-procedure edit. Column 1 code 27447 vs Column 2 code 27370. "
            "Modifier indicator 0 — modifier not allowed. These codes cannot be billed together "
            "on the same date of service. Correct Coding Initiative."
        )
        result = self._tag(text)
        assert result.document_type == "ncci_edit", (
            f"Expected ncci_edit, got {result.document_type} "
            f"(scores: {result.raw_scores})"
        )
        assert "CODEMED" in result.routing_targets

    def test_remittance_policy_classified(self):
        text = (
            "Claim Adjustment Reason Code CARC 97: The benefit for this service is included "
            "in the payment for another service. Remittance Advice Remark Code RARC N1. "
            "Group code CO — Contractual Obligation. ERA 835 transaction denial code."
        )
        result = self._tag(text)
        assert result.document_type == "remittance_policy", (
            f"Expected remittance_policy, got {result.document_type}"
        )
        assert "CODEMED" in result.routing_targets

    def test_modifier_policy_classified(self):
        text = (
            "Modifier -59 Distinct Procedural Service: Use this modifier when a procedure "
            "or service is distinct or independent from other non-E/M services performed on "
            "the same day. Modifier -25 Significant, separately identifiable E/M service. "
            "Global period restrictions apply."
        )
        result = self._tag(text)
        assert result.document_type == "modifier_policy", (
            f"Expected modifier_policy, got {result.document_type}"
        )
        assert "CODEMED" in result.routing_targets

    def test_drg_policy_classified(self):
        text = (
            "MS-DRG 470: Major Hip and Knee Joint Replacement without MCC. "
            "Relative weight: 2.1291. Geometric mean LOS: 2.2 days. "
            "Inpatient Prospective Payment System IPPS fiscal year update. "
            "CC/MCC complication comorbidity DRG grouper."
        )
        result = self._tag(text)
        assert result.document_type == "drg_policy", (
            f"Expected drg_policy, got {result.document_type}"
        )
        assert "CODEMED" in result.routing_targets

    def test_opps_apc_classified(self):
        text = (
            "Ambulatory Payment Classification APC 5114: Level 4 Endoscopy Upper. "
            "OPPS Outpatient Prospective Payment System status indicator T. "
            "Addendum B HCPCS code mapping. Hospital outpatient payment rate. "
            "Comprehensive APC packaging."
        )
        result = self._tag(text)
        assert result.document_type == "opps_apc", (
            f"Expected opps_apc, got {result.document_type}"
        )
        assert "CODEMED" in result.routing_targets

    def test_asp_drug_pricing_classified(self):
        text = (
            "Average Sales Price ASP for J0135 Adalimumab injection: $1,258.00 per unit. "
            "Medicare Part B drug payment limit ASP+6% = $1,333.48. "
            "Quarterly update buy and bill infusion drug reimbursement rate."
        )
        result = self._tag(text)
        assert result.document_type == "asp_drug_pricing", (
            f"Expected asp_drug_pricing, got {result.document_type}"
        )
        assert "CODEMED" in result.routing_targets

    def test_icd10_guidelines_classified(self):
        text = (
            "ICD-10-CM Official Coding Guidelines FY 2026. Section I: Conventions. "
            "Principal diagnosis selection for inpatient coding. Code first the "
            "etiology/manifestation convention. Present on admission (POA) indicator. "
            "Combination code sequela."
        )
        result = self._tag(text)
        assert result.document_type == "icd10_guidelines", (
            f"Expected icd10_guidelines, got {result.document_type}"
        )
        assert "NexusAuth" in result.routing_targets

    def test_medical_necessity_policy_classified(self):
        text = (
            "Medical Necessity Policy: InterQual Level of Care Criteria for acute inpatient "
            "admission. Clinical criteria for utilization management. Milliman Care Guidelines "
            "admission criteria continued stay criteria. Evidence-based appropriateness criteria "
            "for clinical decision support."
        )
        result = self._tag(text)
        assert result.document_type == "medical_necessity_policy", (
            f"Expected medical_necessity_policy, got {result.document_type}"
        )
        assert "NexusAuth" in result.routing_targets

    def test_transmittal_classified(self):
        text = (
            "CMS Transmittal 12590. Change Request CR 13597. Medicare Claims Processing Manual "
            "Chapter 12 update. Internet Only Manual IOM update. Program memorandum effective "
            "date of implementation. One-time notification."
        )
        result = self._tag(text)
        assert result.document_type == "transmittal", (
            f"Expected transmittal, got {result.document_type}"
        )
        assert "NexusAuth" in result.routing_targets


# =============================================================================
# Test: New medical specialties
# =============================================================================

class TestNewSpecialties:
    """Verify the 7 new specialties are detected by the tagger."""

    def _specialties(self, text: str) -> list[str]:
        from tagging.tagger import DocumentTagger
        return DocumentTagger().tag(text).specialties

    def test_nephrology_detected(self):
        text = "Patient has CKD stage 4 requiring hemodialysis. eGFR 18. ESRD management."
        assert "nephrology" in self._specialties(text)

    def test_dermatology_detected(self):
        text = "Psoriasis treatment with biologics. Mohs surgery for melanoma. Atopic dermatitis."
        assert "dermatology" in self._specialties(text)

    def test_ophthalmology_detected(self):
        text = "AMD treatment with anti-VEGF intravitreal injection. Macular degeneration cataract."
        assert "ophthalmology" in self._specialties(text)

    def test_physical_therapy_detected(self):
        text = "Physical therapy for post-surgical rehabilitation. Gait training therapeutic exercise."
        assert "physical_therapy" in self._specialties(text)

    def test_infectious_disease_detected(self):
        text = "HIV treatment antiretroviral therapy. Sepsis management antibiotic therapy. MRSA."
        assert "infectious_disease" in self._specialties(text)

    def test_hematology_detected(self):
        text = "Leukemia chemotherapy bone marrow biopsy. Coagulation disorder anticoagulation warfarin."
        assert "hematology" in self._specialties(text)

    def test_allergy_immunology_detected(self):
        text = "Allergen immunotherapy desensitization. IgE-mediated anaphylaxis allergic rhinitis."
        assert "allergy_immunology" in self._specialties(text)


# =============================================================================
# Test: New payer domain detection
# =============================================================================

class TestNewPayers:
    """Verify CIGNA, HUMANA, CENTENE, MAGELLAN payer detection."""

    def _payer(self, source_url: str, text: str = "medical policy") -> str | None:
        from tagging.tagger import DocumentTagger
        return DocumentTagger().tag(text, source_url=source_url).payer_code

    def test_cigna_domain_detected(self):
        assert self._payer("https://www.cigna.com/coverage/policy/123") == "CIGNA"

    def test_evernorth_domain_detected(self):
        assert self._payer("https://www.evernorth.com/policy/abc") == "CIGNA"

    def test_humana_domain_detected(self):
        assert self._payer("https://www.humana.com/provider/coverage/xyz") == "HUMANA"

    def test_centene_domain_detected(self):
        assert self._payer("https://www.centene.com/policy/123") == "CENTENE"

    def test_wellcare_domain_detected(self):
        assert self._payer("https://www.wellcare.com/policy") == "CENTENE"

    def test_magellan_domain_detected(self):
        assert self._payer("https://www.magellanhealth.com/provider/guidelines") == "MAGELLAN"


# =============================================================================
# Test: CMS MPFS Scraper
# =============================================================================

class TestMPFSScraper:
    """Unit tests for the MPFS scraper using fixture ZIP data."""

    def _run(self, zip_bytes: bytes) -> list[dict]:
        """Run MPFS scraper with in-memory fixture ZIP."""
        import urllib.request
        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_resp = MagicMock()
            mock_resp.read.return_value = zip_bytes
            mock_resp.__enter__ = lambda s: mock_resp
            mock_resp.__exit__ = MagicMock(return_value=False)
            mock_urlopen.return_value = mock_resp
            from ingestion.scrapers.cms_mpfs_scraper import scrape_cms_mpfs
            return list(scrape_cms_mpfs(url="https://fake/rvu.zip", payment_year=2026))

    def test_yields_documents(self):
        docs = self._run(MPFS_ZIP)
        assert len(docs) >= 1

    def test_document_type_hint(self):
        docs = self._run(MPFS_ZIP)
        for doc in docs:
            assert doc["document_type_hint"] == "fee_schedule"

    def test_title_contains_hcpcs_range(self):
        docs = self._run(MPFS_ZIP)
        assert any("HCPCS" in doc["title"] for doc in docs)

    def test_content_contains_rvu_data(self):
        docs = self._run(MPFS_ZIP)
        combined = " ".join(d["raw_content"] for d in docs)
        assert "99213" in combined
        assert "RVU" in combined

    def test_metadata_has_payment_year(self):
        docs = self._run(MPFS_ZIP)
        for doc in docs:
            assert doc["metadata"]["payment_year"] == 2026

    def test_content_hash_is_sha256(self):
        docs = self._run(MPFS_ZIP)
        for doc in docs:
            h = doc["content_hash"]
            assert len(h) == 64
            assert all(c in "0123456789abcdef" for c in h)

    def test_source_domain_is_cms(self):
        docs = self._run(MPFS_ZIP)
        for doc in docs:
            assert doc["source_domain"] == "cms.gov"


# =============================================================================
# Test: CMS NCCI Scraper
# =============================================================================

class TestNCCIScraper:
    """Unit tests for the NCCI PTP and MUE scrapers."""

    def _run_ptp(self, zip_bytes: bytes) -> list[dict]:
        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_resp = MagicMock()
            mock_resp.read.return_value = zip_bytes
            mock_resp.__enter__ = lambda s: mock_resp
            mock_resp.__exit__ = MagicMock(return_value=False)
            mock_urlopen.return_value = mock_resp
            from ingestion.scrapers.cms_ncci_scraper import scrape_cms_ncci_ptp
            return list(scrape_cms_ncci_ptp(
                url="https://fake/ncci.zip",
                data_year=2026,
                data_quarter=1,
            ))

    def _run_mue(self, zip_bytes: bytes) -> list[dict]:
        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_resp = MagicMock()
            mock_resp.read.return_value = zip_bytes
            mock_resp.__enter__ = lambda s: mock_resp
            mock_resp.__exit__ = MagicMock(return_value=False)
            mock_urlopen.return_value = mock_resp
            from ingestion.scrapers.cms_ncci_scraper import scrape_cms_ncci_mue
            return list(scrape_cms_ncci_mue(
                url="https://fake/mue.zip",
                data_year=2026,
                data_quarter=1,
            ))

    def test_ptp_yields_documents(self):
        docs = self._run_ptp(NCCI_PTP_ZIP)
        assert len(docs) >= 1

    def test_ptp_document_type_hint(self):
        docs = self._run_ptp(NCCI_PTP_ZIP)
        for doc in docs:
            assert doc["document_type_hint"] == "ncci_edit"

    def test_ptp_content_contains_edit_pairs(self):
        docs = self._run_ptp(NCCI_PTP_ZIP)
        combined = " ".join(d["raw_content"] for d in docs)
        assert "27447" in combined
        assert "27370" in combined

    def test_ptp_metadata_ncci_type(self):
        docs = self._run_ptp(NCCI_PTP_ZIP)
        for doc in docs:
            assert doc["metadata"]["ncci_type"] == "PTP"

    def test_mue_yields_documents(self):
        docs = self._run_mue(NCCI_MUE_ZIP)
        assert len(docs) >= 1

    def test_mue_document_type_hint(self):
        docs = self._run_mue(NCCI_MUE_ZIP)
        for doc in docs:
            assert doc["document_type_hint"] == "ncci_edit"

    def test_mue_content_contains_max_units(self):
        docs = self._run_mue(NCCI_MUE_ZIP)
        combined = " ".join(d["raw_content"] for d in docs)
        assert "MUE" in combined or "Medically Unlikely" in combined

    def test_mue_metadata_ncci_type(self):
        docs = self._run_mue(NCCI_MUE_ZIP)
        for doc in docs:
            assert doc["metadata"]["ncci_type"] == "MUE"


# =============================================================================
# Test: CMS ASP Scraper
# =============================================================================

class TestASPScraper:
    """Unit tests for the ASP drug pricing scraper."""

    def _run(self, zip_bytes: bytes) -> list[dict]:
        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_resp = MagicMock()
            mock_resp.read.return_value = zip_bytes
            mock_resp.__enter__ = lambda s: mock_resp
            mock_resp.__exit__ = MagicMock(return_value=False)
            mock_urlopen.return_value = mock_resp
            from ingestion.scrapers.cms_asp_scraper import scrape_cms_asp
            return list(scrape_cms_asp(
                url="https://fake/asp.zip",
                effective_year=2026,
                effective_quarter=1,
            ))

    def test_yields_documents(self):
        docs = self._run(ASP_ZIP)
        assert len(docs) >= 1

    def test_document_type_hint(self):
        docs = self._run(ASP_ZIP)
        for doc in docs:
            assert doc["document_type_hint"] == "asp_drug_pricing"

    def test_content_contains_j_codes(self):
        docs = self._run(ASP_ZIP)
        combined = " ".join(d["raw_content"] for d in docs)
        assert "J0135" in combined

    def test_metadata_has_quarter(self):
        docs = self._run(ASP_ZIP)
        for doc in docs:
            assert doc["metadata"]["effective_year"] == 2026
            assert doc["metadata"]["effective_quarter"] == 1

    def test_content_contains_asp_plus_6(self):
        docs = self._run(ASP_ZIP)
        combined = " ".join(d["raw_content"] for d in docs)
        assert "ASP" in combined


# =============================================================================
# Test: CMS DRG Scraper
# =============================================================================

class TestDRGScraper:
    """Unit tests for the MS-DRG scraper."""

    def _run(self, zip_bytes: bytes) -> list[dict]:
        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_resp = MagicMock()
            mock_resp.read.return_value = zip_bytes
            mock_resp.__enter__ = lambda s: mock_resp
            mock_resp.__exit__ = MagicMock(return_value=False)
            mock_urlopen.return_value = mock_resp
            from ingestion.scrapers.cms_drg_scraper import scrape_cms_drg
            return list(scrape_cms_drg(url="https://fake/drg.zip", fiscal_year=2026))

    def test_yields_documents(self):
        docs = self._run(DRG_ZIP)
        assert len(docs) >= 1

    def test_document_type_hint(self):
        docs = self._run(DRG_ZIP)
        for doc in docs:
            assert doc["document_type_hint"] == "drg_policy"

    def test_content_contains_drg_numbers(self):
        docs = self._run(DRG_ZIP)
        combined = " ".join(d["raw_content"] for d in docs)
        assert "470" in combined
        assert "relative weight" in combined.lower() or "Relative Weight" in combined

    def test_metadata_has_fiscal_year(self):
        docs = self._run(DRG_ZIP)
        for doc in docs:
            assert doc["metadata"]["fiscal_year"] == 2026

    def test_drg_grouped_by_mdc(self):
        docs = self._run(DRG_ZIP)
        # Each document should be for a specific MDC
        for doc in docs:
            assert "mdc" in doc["metadata"]


# =============================================================================
# Test: CMS CARC/RARC Scraper
# =============================================================================

class TestCARCScraper:
    """Unit tests for the CARC/RARC HTML scraper."""

    def _run_rarc(self, html: str) -> list[dict]:
        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_resp = MagicMock()
            mock_resp.read.return_value = html.encode("utf-8")
            mock_resp.headers.get_content_charset.return_value = "utf-8"
            mock_resp.__enter__ = lambda s: mock_resp
            mock_resp.__exit__ = MagicMock(return_value=False)
            mock_urlopen.return_value = mock_resp
            from ingestion.scrapers.cms_carc_scraper import scrape_cms_rarc
            return list(scrape_cms_rarc(url="https://fake/rarc"))

    def _run_carc(self, html: str) -> list[dict]:
        with patch("urllib.request.urlopen") as mock_urlopen, \
             patch("time.sleep"):  # Skip delay in tests
            mock_resp = MagicMock()
            mock_resp.read.return_value = html.encode("utf-8")
            mock_resp.headers.get_content_charset.return_value = "utf-8"
            mock_resp.__enter__ = lambda s: mock_resp
            mock_resp.__exit__ = MagicMock(return_value=False)
            mock_urlopen.return_value = mock_resp
            from ingestion.scrapers.cms_carc_scraper import scrape_carc
            return list(scrape_carc(url="https://fake/carc"))

    def test_rarc_yields_documents(self):
        docs = self._run_rarc(RARC_HTML)
        assert len(docs) >= 1

    def test_rarc_document_type_hint(self):
        docs = self._run_rarc(RARC_HTML)
        for doc in docs:
            assert doc["document_type_hint"] == "remittance_policy"

    def test_rarc_content_contains_codes(self):
        docs = self._run_rarc(RARC_HTML)
        combined = " ".join(d["raw_content"] for d in docs)
        assert "N1" in combined or "MA01" in combined

    def test_carc_yields_documents(self):
        docs = self._run_carc(CARC_HTML)
        assert len(docs) >= 1

    def test_carc_document_type_hint(self):
        docs = self._run_carc(CARC_HTML)
        for doc in docs:
            assert doc["document_type_hint"] == "remittance_policy"

    def test_carc_content_contains_codes(self):
        docs = self._run_carc(CARC_HTML)
        combined = " ".join(d["raw_content"] for d in docs)
        # Should contain at least one of the test CARC codes
        assert any(code in combined for code in ["1", "2", "4", "16", "97"])

    def test_carc_metadata_code_type(self):
        docs = self._run_carc(CARC_HTML)
        for doc in docs:
            assert doc["metadata"].get("code_type") == "CARC"


# =============================================================================
# Test: Taxonomy YAML structure
# =============================================================================

class TestTaxonomyStructure:
    """Verify taxonomy.yaml v2.0 structure is valid and complete."""

    @pytest.fixture(scope="class")
    def taxonomy(self):
        import yaml
        path = Path(__file__).parent.parent / "tagging" / "taxonomy.yaml"
        with open(path) as f:
            return yaml.safe_load(f)

    def test_all_new_document_types_present(self, taxonomy):
        new_types = [
            "ncci_edit", "remittance_policy", "modifier_policy",
            "drg_policy", "opps_apc", "asp_drug_pricing",
            "icd10_guidelines", "medical_necessity_policy", "transmittal",
        ]
        for t in new_types:
            assert t in taxonomy["document_types"], f"Missing type: {t}"

    def test_all_new_types_have_routing(self, taxonomy):
        new_types = [
            "ncci_edit", "remittance_policy", "modifier_policy",
            "drg_policy", "opps_apc", "asp_drug_pricing",
            "icd10_guidelines", "medical_necessity_policy", "transmittal",
        ]
        for t in new_types:
            assert t in taxonomy["routing_matrix"], f"No routing for: {t}"
            assert "targets" in taxonomy["routing_matrix"][t]

    def test_codemed_routes_correct(self, taxonomy):
        codemed_types = [
            "ncci_edit", "remittance_policy", "modifier_policy",
            "drg_policy", "opps_apc", "asp_drug_pricing",
        ]
        for t in codemed_types:
            targets = taxonomy["routing_matrix"][t]["targets"]
            assert "CODEMED" in targets, f"{t} should route to CODEMED"

    def test_nexusauth_routes_correct(self, taxonomy):
        nexusauth_types = ["icd10_guidelines", "medical_necessity_policy", "transmittal"]
        for t in nexusauth_types:
            targets = taxonomy["routing_matrix"][t]["targets"]
            assert "NexusAuth" in targets, f"{t} should route to NexusAuth"

    def test_new_payers_present(self, taxonomy):
        new_payers = ["CIGNA", "HUMANA", "CENTENE", "MAGELLAN"]
        for payer in new_payers:
            assert payer in taxonomy["payer_domains"], f"Missing payer: {payer}"

    def test_new_specialties_present(self, taxonomy):
        new_specialties = [
            "nephrology", "dermatology", "ophthalmology",
            "physical_therapy", "infectious_disease", "hematology",
            "allergy_immunology",
        ]
        for spec in new_specialties:
            assert spec in taxonomy["specialties"], f"Missing specialty: {spec}"

    def test_total_document_types_is_18(self, taxonomy):
        assert len(taxonomy["document_types"]) == 18, (
            f"Expected 18 document types, got {len(taxonomy['document_types'])}"
        )

    def test_total_specialties_is_17(self, taxonomy):
        assert len(taxonomy["specialties"]) == 17, (
            f"Expected 17 specialties, got {len(taxonomy['specialties'])}"
        )


# =============================================================================
# Test: Pipeline CLI — new flags
# =============================================================================

class TestPipelineCLIExtended:
    """Verify new CLI flags parse correctly."""

    def _parse(self, args: list[str]):
        from ingestion.pipeline import build_parser
        return build_parser().parse_args(args)

    def test_extended_flag_parses(self):
        args = self._parse(["--extended"])
        assert args.extended is True

    def test_no_mpfs_flag(self):
        args = self._parse(["--extended", "--no-mpfs"])
        assert args.no_mpfs is True

    def test_no_ncci_flag(self):
        args = self._parse(["--extended", "--no-ncci"])
        assert args.no_ncci is True

    def test_no_opps_flag(self):
        args = self._parse(["--extended", "--no-opps"])
        assert args.no_opps is True

    def test_no_asp_flag(self):
        args = self._parse(["--extended", "--no-asp"])
        assert args.no_asp is True

    def test_no_drg_flag(self):
        args = self._parse(["--extended", "--no-drg"])
        assert args.no_drg is True

    def test_no_carc_flag(self):
        args = self._parse(["--extended", "--no-carc"])
        assert args.no_carc is True

    def test_no_payer_policies_flag(self):
        args = self._parse(["--extended", "--no-payer-policies"])
        assert args.no_payer_policies is True

    def test_all_includes_extended(self):
        """--all implies extended scrape."""
        from ingestion.pipeline import build_parser
        parser = build_parser()
        args = parser.parse_args(["--all"])
        assert args.all is True
        # The main() function uses args.all to set run_extended
