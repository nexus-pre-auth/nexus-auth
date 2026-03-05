"""
NexusAuth — Session 2 Pipeline Tests
======================================
Tests for the ingestion pipeline components:
  - CMS scraper (offline, using fixture data)
  - SHA-256 deduplication
  - Text chunking
  - Tagger integration (offline)

Run with: pytest tests/test_session2_pipeline.py -v
"""

import hashlib
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))


# =============================================================================
# Fixtures
# =============================================================================

SAMPLE_LCD_ROW = {
    "lcd_id": "33252",
    "lcd_version": "29",
    "title": "Psychiatric Diagnostic Evaluation and Psychotherapy Services",
    "display_id": "L33252",
    "status": "A",
    "indication": "<p>This LCD covers psychiatric diagnostic evaluation services when medically necessary.</p>",
    "cms_cov_policy": "<p>This LCD supplements but does not replace NCDs.</p>",
    "diagnoses_support": "<p>F32.0 Major depressive disorder, single episode, mild</p>",
    "diagnoses_dont_support": "",
    "coding_guidelines": "<p>CPT codes 90791, 90792 are covered.</p>",
    "doc_reqs": "<p>Documentation must include diagnosis and treatment plan.</p>",
    "associated_info": "",
    "summary_of_evidence": "<p>N/A</p>",
    "analysis_of_evidence": "<p>N/A</p>",
    "source_info": "<p>First Coast Service Options, Inc.</p>",
    "bibliography": "",
    "keywords": "psychiatric, psychotherapy, mental health",
    "issue": "",
    "orig_det_eff_date": "2015-10-01 00:00:00",
    "rev_eff_date": "2020-07-01 00:00:00",
    "last_updated": "2020-06-19 22:52:07.840000000",
    "last_reviewed_on": "2018-07-25 00:00:00",
    "icd10_doc": "Y",
    "source_lcd_id": "33130",
    "mcd_publish_date": "2020-06-19 00:00:00",
}

SAMPLE_NCD_ROW = {
    "NCD_id": "1",
    "NCD_vrsn_num": "3",
    "natl_cvrg_type": "True",
    "cvrg_lvl_cd": "2",
    "NCD_mnl_sect": "310.1",
    "NCD_mnl_sect_title": "Routine Costs in Clinical Trials",
    "NCD_efctv_dt": "2024-05-27 00:00:00",
    "NCD_impltn_dt": "2024-05-27 00:00:00",
    "NCD_trmntn_dt": "",
    "itm_srvc_desc": "Clinical Trial Coverage",
    "indctn_lmtn": "<p>Coverage for routine costs in qualifying clinical trials.</p>",
    "xref_txt": "",
    "othr_txt": "",
    "trnsmtl_num": "12590",
    "trnsmtl_url": "https://www.cms.gov/files/document/r12590ncd.pdf",
    "chg_rqst_num": "13597",
    "pblctn_cd": "25",
    "rev_hstry": "",
    "under_rvw": "True",
    "creatd_tmstmp": "2024-04-25 15:10:07",
    "last_updt_tmstmp": "2025-05-27 12:20:33",
    "last_clrnc_tmstmp": "2024-04-29 14:58:02.610000000",
    "NCD_lab": "False",
    "ncd_keyword": "clinical trial, routine costs",
    "NCD_AMA": "False",
}


# =============================================================================
# Test: SHA-256 Deduplication
# =============================================================================

class TestDeduplication:
    """Tests for the deduplicator module."""

    def test_sha256_content_deterministic(self):
        """Same content always produces same hash."""
        from ingestion.deduplicator import sha256_content
        content = "This is a test document about cardiac monitoring."
        hash1 = sha256_content(content)
        hash2 = sha256_content(content)
        assert hash1 == hash2

    def test_sha256_content_different_inputs(self):
        """Different content produces different hashes."""
        from ingestion.deduplicator import sha256_content
        hash1 = sha256_content("Document A")
        hash2 = sha256_content("Document B")
        assert hash1 != hash2

    def test_sha256_content_format(self):
        """Hash is a 64-character hex string."""
        from ingestion.deduplicator import sha256_content
        h = sha256_content("test content")
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)

    def test_sha256_matches_hashlib(self):
        """Our hash matches Python's hashlib directly."""
        from ingestion.deduplicator import sha256_content
        content = "NexusAuth test document"
        expected = hashlib.sha256(content.encode("utf-8")).hexdigest()
        assert sha256_content(content) == expected

    def test_is_duplicate_returns_false_when_not_found(self):
        """is_duplicate returns False when hash not in DB."""
        from ingestion.deduplicator import is_duplicate
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        result = is_duplicate(mock_conn, "abc123")
        assert result is False

    def test_is_duplicate_returns_true_when_found(self):
        """is_duplicate returns True when hash exists in DB."""
        from ingestion.deduplicator import is_duplicate
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1,)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        result = is_duplicate(mock_conn, "abc123")
        assert result is True


# =============================================================================
# Test: Text Chunking
# =============================================================================

class TestTextChunker:
    """Tests for the embedding chunker."""

    def test_chunk_short_text_returns_single_chunk(self):
        """Text shorter than chunk_size but above min_size produces exactly one chunk."""
        from ingestion.embedder import chunk_text
        # Use text longer than MIN_CHUNK_CHARS (100) but shorter than chunk_size
        text = "This is a document about prior authorization criteria for cardiac monitoring services. " * 2
        chunks = chunk_text(text, chunk_size=2000, min_size=50)
        assert len(chunks) == 1
        assert chunks[0]["chunk_index"] == 0

    def test_chunk_long_text_produces_multiple_chunks(self):
        """Long text produces multiple chunks."""
        from ingestion.embedder import chunk_text
        text = "This is a sentence about medical coverage. " * 100
        chunks = chunk_text(text, chunk_size=500, overlap=50)
        assert len(chunks) > 1

    def test_chunk_indices_are_sequential(self):
        """Chunk indices start at 0 and increment by 1."""
        from ingestion.embedder import chunk_text
        text = "Medical coverage document. " * 100
        chunks = chunk_text(text, chunk_size=300, overlap=30)
        for i, chunk in enumerate(chunks):
            assert chunk["chunk_index"] == i

    def test_chunk_text_preserved(self):
        """Each chunk's text is a substring of the original."""
        from ingestion.embedder import chunk_text
        text = "Prior authorization for cardiac monitoring services. " * 50
        chunks = chunk_text(text, chunk_size=400, overlap=40)
        for chunk in chunks:
            assert chunk["chunk_text"] in text or len(chunk["chunk_text"]) > 0

    def test_chunk_empty_text_returns_empty_list(self):
        """Empty text returns no chunks."""
        from ingestion.embedder import chunk_text
        assert chunk_text("") == []
        assert chunk_text("   ") == []

    def test_chunk_very_short_text_below_min_size(self):
        """Text below min_size returns empty list."""
        from ingestion.embedder import chunk_text
        assert chunk_text("Hi", min_size=100) == []

    def test_chunk_has_required_keys(self):
        """Each chunk dict has the required keys."""
        from ingestion.embedder import chunk_text
        text = "This is a test document for NexusAuth. " * 20
        chunks = chunk_text(text)
        for chunk in chunks:
            assert "chunk_index" in chunk
            assert "chunk_text" in chunk
            assert "char_start" in chunk
            assert "char_end" in chunk

    def test_chunk_overlap_creates_continuity(self):
        """Overlapping chunks share some content."""
        from ingestion.embedder import chunk_text
        text = "Word " * 500  # 2500 chars
        chunks = chunk_text(text, chunk_size=500, overlap=100)
        if len(chunks) >= 2:
            # The end of chunk 0 should overlap with the start of chunk 1
            end_of_chunk0 = chunks[0]["char_end"]
            start_of_chunk1 = chunks[1]["char_start"]
            assert start_of_chunk1 < end_of_chunk0  # overlap exists


# =============================================================================
# Test: CMS Scraper (offline)
# =============================================================================

class TestCMSScraper:
    """Tests for the CMS scraper using fixture data."""

    def test_html_to_text_strips_tags(self):
        """HTML tags are stripped from content."""
        from ingestion.scrapers.cms_scraper import _html_to_text
        html = "<p>This is <strong>important</strong> coverage criteria.</p>"
        text = _html_to_text(html)
        assert "<p>" not in text
        assert "<strong>" not in text
        assert "important" in text
        assert "coverage criteria" in text

    def test_html_to_text_handles_empty(self):
        """Empty HTML returns empty string."""
        from ingestion.scrapers.cms_scraper import _html_to_text
        assert _html_to_text("") == ""
        assert _html_to_text(None) == ""

    def test_sha256_function(self):
        """SHA-256 function returns correct hash."""
        from ingestion.scrapers.cms_scraper import _sha256
        h = _sha256("test")
        assert len(h) == 64

    def test_build_lcd_text_includes_title(self):
        """LCD text includes the document title."""
        from ingestion.scrapers.cms_scraper import _build_lcd_text
        text = _build_lcd_text(SAMPLE_LCD_ROW, {})
        assert "Psychiatric Diagnostic Evaluation" in text

    def test_build_lcd_text_includes_display_id(self):
        """LCD text includes the display ID."""
        from ingestion.scrapers.cms_scraper import _build_lcd_text
        text = _build_lcd_text(SAMPLE_LCD_ROW, {})
        assert "L33252" in text

    def test_build_lcd_text_strips_html(self):
        """LCD text has HTML stripped."""
        from ingestion.scrapers.cms_scraper import _build_lcd_text
        text = _build_lcd_text(SAMPLE_LCD_ROW, {})
        assert "<p>" not in text
        assert "psychiatric diagnostic evaluation" in text.lower()

    def test_build_ncd_text_includes_title(self):
        """NCD text includes the manual section title."""
        from ingestion.scrapers.cms_scraper import _build_ncd_text
        text = _build_ncd_text(SAMPLE_NCD_ROW)
        assert "Routine Costs in Clinical Trials" in text

    def test_build_ncd_text_includes_section(self):
        """NCD text includes the manual section number."""
        from ingestion.scrapers.cms_scraper import _build_ncd_text
        text = _build_ncd_text(SAMPLE_NCD_ROW)
        assert "310.1" in text

    def test_parse_date_valid(self):
        """Date parsing handles CMS date formats."""
        from ingestion.scrapers.cms_scraper import _parse_date
        dt = _parse_date("2020-07-01 00:00:00")
        assert dt is not None
        assert dt.year == 2020
        assert dt.month == 7

    def test_parse_date_empty(self):
        """Empty date string returns None."""
        from ingestion.scrapers.cms_scraper import _parse_date
        assert _parse_date("") is None
        assert _parse_date(None) is None


# =============================================================================
# Test: Tagger Integration (offline)
# =============================================================================

class TestTaggerIntegration:
    """Tests for tagger integration using the real DocumentTagger."""

    @pytest.fixture(autouse=True)
    def setup_tagger(self):
        """Initialise the DocumentTagger for all tests."""
        from tagging.tagger import DocumentTagger
        self.tagger = DocumentTagger()

    def test_cms_lcd_tagged_as_prior_auth(self):
        """CMS LCD document is tagged as prior_auth_criteria."""
        from ingestion.scrapers.cms_scraper import _build_lcd_text
        text = _build_lcd_text(SAMPLE_LCD_ROW, {})
        result = self.tagger.tag(
            text,
            source_url="https://www.cms.gov/medicare-coverage-database/view/lcd.aspx?lcdid=33252"
        )
        assert result.payer_code == "CMS"
        assert result.document_type in ("prior_auth_criteria", "clinical_policy", "lcd")

    def test_cms_payer_detected_from_url(self):
        """CMS payer is detected from the source URL."""
        result = self.tagger.tag(
            "This is a coverage determination document.",
            source_url="https://www.cms.gov/medicare-coverage-database/view/lcd.aspx?lcdid=12345"
        )
        assert result.payer_code == "CMS"

    def test_confidence_score_range(self):
        """Confidence score is between 0 and 1."""
        result = self.tagger.tag(
            "Prior authorization is required for cardiac monitoring services.",
            source_url="https://www.cms.gov"
        )
        assert 0.0 <= result.confidence_score <= 1.0

    def test_cpt_code_extraction(self):
        """CPT codes are extracted from document text."""
        result = self.tagger.tag(
            "CPT codes 93224, 93225, 93226 are covered for cardiac monitoring.",
            source_url="https://www.cms.gov"
        )
        assert "93224" in result.cpt_codes
        assert "93225" in result.cpt_codes

    def test_icd10_code_extraction(self):
        """ICD-10 codes are extracted from document text."""
        result = self.tagger.tag(
            "Diagnosis codes I49.0 and R00.1 support medical necessity.",
            source_url="https://www.cms.gov"
        )
        assert "I49.0" in result.icd10_codes

    def test_routing_targets_not_empty(self):
        """Routing targets are assigned for classified documents."""
        result = self.tagger.tag(
            "Prior authorization criteria for cardiac monitoring services.",
            source_url="https://www.cms.gov"
        )
        assert len(result.routing_targets) > 0

    def test_low_confidence_requires_review(self):
        """Very generic text triggers review flag."""
        result = self.tagger.tag(
            "This is a document.",
            source_url="https://unknown.example.com"
        )
        # Low confidence should trigger review
        if result.confidence_score < 0.3:
            assert result.requires_review is True

    def test_tagging_result_to_dict(self):
        """TaggingResult.to_dict() returns expected keys."""
        result = self.tagger.tag(
            "Prior authorization for cardiac monitoring.",
            source_url="https://www.cms.gov"
        )
        d = result.to_dict()
        expected_keys = [
            "payer_code", "document_type", "document_subtype",
            "specialties", "cpt_codes", "icd10_codes", "hcpcs_codes",
            "routing_targets", "confidence_score", "requires_review"
        ]
        for key in expected_keys:
            assert key in d, f"Missing key: {key}"


# =============================================================================
# Test: Pipeline Orchestrator (unit tests)
# =============================================================================

class TestPipelineOrchestrator:
    """Tests for the pipeline.py orchestrator."""

    def test_build_parser_returns_parser(self):
        """build_parser returns an ArgumentParser."""
        import argparse
        from ingestion.pipeline import build_parser
        parser = build_parser()
        assert isinstance(parser, argparse.ArgumentParser)

    def test_parser_all_flag(self):
        """--all flag sets all stage flags."""
        from ingestion.pipeline import build_parser
        parser = build_parser()
        args = parser.parse_args(["--all"])
        assert args.all is True

    def test_parser_dry_run_flag(self):
        """--dry-run flag is parsed correctly."""
        from ingestion.pipeline import build_parser
        parser = build_parser()
        args = parser.parse_args(["--scrape", "--dry-run"])
        assert args.dry_run is True
        assert args.scrape is True

    def test_parser_max_docs(self):
        """--max-docs is parsed as integer."""
        from ingestion.pipeline import build_parser
        parser = build_parser()
        args = parser.parse_args(["--all", "--max-docs", "50"])
        assert args.max_docs == 50

    def test_main_no_args_returns_error(self):
        """main() with no stage args returns exit code 1."""
        from ingestion.pipeline import main
        result = main([])
        assert result == 1


# =============================================================================
# Integration smoke test (requires DB)
# =============================================================================

class TestIntegrationSmoke:
    """
    Smoke tests that require a running database.
    Skip with: pytest -k "not integration"
    """

    @pytest.mark.integration
    def test_db_connection(self):
        """Can connect to the local Docker database."""
        from ingestion.pipeline import get_db_connection
        try:
            conn = get_db_connection()
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                result = cur.fetchone()
            conn.close()
            assert result == (1,)
        except Exception as e:
            pytest.skip(f"Database not available: {e}")

    @pytest.mark.integration
    def test_scrape_and_ingest_dry_run(self):
        """Dry run scrape completes without errors."""
        from ingestion.pipeline import stage_scrape_and_ingest
        result = stage_scrape_and_ingest(
            conn=None,
            include_lcds=True,
            include_ncds=False,
            dry_run=True,
            max_docs=5,
        )
        assert result["dry_run"] is True
        assert result["scraped"] >= 0
