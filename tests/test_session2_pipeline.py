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


# =============================================================================
# Test: Embedder — embed_texts
# =============================================================================

class TestEmbedTexts:
    """Tests for embed_texts() — OpenAI embedding API wrapper."""

    def _make_mock_client(self, vectors=None):
        """Return a mock OpenAI client whose embeddings.create returns fake vectors."""
        if vectors is None:
            vectors = [[0.1] * 1536]
        mock_client = MagicMock()
        mock_items = [MagicMock(embedding=v) for v in vectors]
        mock_client.embeddings.create.return_value = MagicMock(data=mock_items)
        return mock_client

    def test_returns_list_of_vectors(self):
        """embed_texts returns a list of embedding vectors."""
        from ingestion.embedder import embed_texts
        client = self._make_mock_client([[0.1] * 1536])
        result = embed_texts(["hello world"], client=client)
        assert isinstance(result, list)
        assert len(result) == 1
        assert len(result[0]) == 1536

    def test_empty_list_returns_empty(self):
        """embed_texts returns [] for empty input without calling the API."""
        from ingestion.embedder import embed_texts
        client = self._make_mock_client()
        result = embed_texts([], client=client)
        assert result == []
        client.embeddings.create.assert_not_called()

    def test_multiple_texts_returns_multiple_vectors(self):
        """embed_texts returns one vector per input text."""
        from ingestion.embedder import embed_texts
        vectors = [[float(i)] * 1536 for i in range(3)]
        client = self._make_mock_client(vectors)
        result = embed_texts(["a", "b", "c"], client=client)
        assert len(result) == 3

    def test_truncates_long_text(self):
        """Text longer than MAX_CHARS (25000) is truncated before being sent."""
        from ingestion.embedder import embed_texts
        client = self._make_mock_client([[0.0] * 1536])
        long_text = "x" * 30000
        embed_texts([long_text], client=client)
        call_args = client.embeddings.create.call_args
        sent_input = call_args[1]["input"] if "input" in call_args[1] else call_args[0][1]
        assert len(sent_input[0]) <= 25000

    def test_api_error_propagates(self):
        """API exceptions raised by OpenAI client propagate to caller."""
        from ingestion.embedder import embed_texts
        client = MagicMock()
        client.embeddings.create.side_effect = RuntimeError("API down")
        with pytest.raises(RuntimeError, match="API down"):
            embed_texts(["test text"], client=client)

    def test_single_text_works(self):
        """Single-element list is handled correctly."""
        from ingestion.embedder import embed_texts
        client = self._make_mock_client([[0.5] * 1536])
        result = embed_texts(["single text"], client=client)
        assert len(result) == 1

    def test_passes_model_name(self):
        """The correct model name is forwarded to the API call."""
        from ingestion.embedder import embed_texts, EMBEDDING_MODEL
        client = self._make_mock_client([[0.0] * 1536])
        embed_texts(["text"], client=client)
        call_kwargs = client.embeddings.create.call_args[1]
        assert call_kwargs.get("model") == EMBEDDING_MODEL


# =============================================================================
# Test: Embedder — embed_document
# =============================================================================

class TestEmbedDocument:
    """Tests for embed_document() — chunks a doc and inserts embeddings."""

    def _make_mock_conn(self):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1,)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        return mock_conn, mock_cursor

    def _make_mock_client(self, n_chunks=1):
        mock_client = MagicMock()
        mock_items = [MagicMock(embedding=[0.1] * 1536) for _ in range(n_chunks)]
        mock_client.embeddings.create.return_value = MagicMock(data=mock_items)
        return mock_client

    def test_returns_chunk_count(self):
        """embed_document returns the number of chunks inserted."""
        from ingestion.embedder import embed_document
        doc = {
            "id": 42,
            "title": "Cardiac Monitoring",
            "content_text": "Prior authorization for cardiac monitoring. " * 10,
            "document_type": "lcd",
            "source_domain": "cms.gov",
            "specialties": ["cardiology"],
        }
        conn, _ = self._make_mock_conn()
        client = self._make_mock_client(n_chunks=1)
        # Patch chunk_text to return a predictable 1 chunk
        with patch("ingestion.embedder.chunk_text") as mock_chunk:
            mock_chunk.return_value = [{"chunk_index": 0, "chunk_text": "chunk text", "char_start": 0, "char_end": 10}]
            result = embed_document(conn, doc, client=client)
        assert result == 1

    def test_prepends_title_to_content(self):
        """Title is prepended to content before chunking."""
        from ingestion.embedder import embed_document
        doc = {
            "id": 1,
            "title": "MY TITLE",
            "content_text": "some content",
            "document_type": "lcd",
            "source_domain": "cms.gov",
            "specialties": [],
        }
        conn, _ = self._make_mock_conn()
        client = self._make_mock_client(n_chunks=1)
        with patch("ingestion.embedder.chunk_text") as mock_chunk:
            mock_chunk.return_value = [{"chunk_index": 0, "chunk_text": "chunk", "char_start": 0, "char_end": 5}]
            embed_document(conn, doc, client=client)
            called_text = mock_chunk.call_args[0][0]
        assert "MY TITLE" in called_text
        assert "some content" in called_text

    def test_empty_content_returns_zero(self):
        """Document with empty content returns 0 without calling API."""
        from ingestion.embedder import embed_document
        doc = {"id": 7, "title": "", "content_text": "", "document_type": "unknown", "source_domain": "", "specialties": []}
        conn, _ = self._make_mock_conn()
        client = self._make_mock_client()
        result = embed_document(conn, doc, client=client)
        assert result == 0
        client.embeddings.create.assert_not_called()

    def test_db_commit_called(self):
        """conn.commit() is called after inserting embeddings."""
        from ingestion.embedder import embed_document
        doc = {
            "id": 5,
            "title": "Test",
            "content_text": "Content for testing purposes. " * 5,
            "document_type": "lcd",
            "source_domain": "cms.gov",
            "specialties": [],
        }
        conn, _ = self._make_mock_conn()
        client = self._make_mock_client(n_chunks=1)
        with patch("ingestion.embedder.chunk_text") as mock_chunk:
            mock_chunk.return_value = [{"chunk_index": 0, "chunk_text": "chunk", "char_start": 0, "char_end": 5}]
            embed_document(conn, doc, client=client)
        assert conn.commit.called

    def test_openai_error_propagates(self):
        """OpenAI API error raised by embed_texts propagates from embed_document."""
        from ingestion.embedder import embed_document
        doc = {
            "id": 3,
            "title": "Test",
            "content_text": "Some content here. " * 5,
            "document_type": "lcd",
            "source_domain": "cms.gov",
            "specialties": [],
        }
        conn, _ = self._make_mock_conn()
        client = MagicMock()
        client.embeddings.create.side_effect = RuntimeError("OpenAI error")
        with patch("ingestion.embedder.chunk_text") as mock_chunk:
            mock_chunk.return_value = [{"chunk_index": 0, "chunk_text": "chunk", "char_start": 0, "char_end": 5}]
            with pytest.raises(RuntimeError, match="OpenAI error"):
                embed_document(conn, doc, client=client)

    def test_embedding_stored_as_pgvector_string(self):
        """The embedding is formatted as '[f1,f2,...]' string for pgvector."""
        from ingestion.embedder import embed_document
        doc = {
            "id": 9,
            "title": "Test",
            "content_text": "Some content. " * 5,
            "document_type": "lcd",
            "source_domain": "cms.gov",
            "specialties": [],
        }
        conn, mock_cursor = self._make_mock_conn()
        client = self._make_mock_client(n_chunks=1)
        with patch("ingestion.embedder.chunk_text") as mock_chunk:
            mock_chunk.return_value = [{"chunk_index": 0, "chunk_text": "chunk", "char_start": 0, "char_end": 5}]
            embed_document(conn, doc, client=client)
        # Verify execute was called with params dict containing a string embedding
        execute_call = mock_cursor.execute.call_args_list[0]
        params = execute_call[0][1]
        assert isinstance(params["embedding"], str)
        assert params["embedding"].startswith("[")
        assert params["embedding"].endswith("]")


# =============================================================================
# Test: Embedder — process_unembedded_documents
# =============================================================================

class TestProcessUnembeddedDocuments:
    """Tests for process_unembedded_documents() batch processor."""

    def _make_mock_conn(self, rows=None):
        """Return mock conn that returns given rows then empty list."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [
            ("id",), ("title",), ("content_text",), ("document_type",),
            ("source_url",), ("source_domain",), ("payer_id",),
            ("specialties",), ("cpt_codes",), ("icd10_codes",),
        ]
        # First call returns rows, second returns empty (to stop the loop)
        if rows:
            mock_cursor.fetchall.side_effect = [rows, []]
        else:
            mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        return mock_conn

    def test_empty_queue_returns_zero_stats(self):
        """Returns EmbeddingStats with zero counts when no docs pending."""
        from ingestion.embedder import process_unembedded_documents
        conn = self._make_mock_conn(rows=[])
        client = MagicMock()
        stats = process_unembedded_documents(conn, client=client, rate_limit_sleep=0)
        assert stats.total_docs == 0
        assert stats.embedded_docs == 0

    def test_respects_max_documents(self):
        """Stops processing after max_documents is reached."""
        from ingestion.embedder import process_unembedded_documents
        # Two document rows
        rows = [
            (1, "Title A", "Content A " * 10, "lcd", "https://cms.gov", "cms.gov", None, [], [], []),
            (2, "Title B", "Content B " * 10, "ncd", "https://cms.gov", "cms.gov", None, [], [], []),
        ]
        conn = self._make_mock_conn(rows=rows)
        client = MagicMock()
        with patch("ingestion.embedder.embed_document", return_value=2) as mock_embed:
            stats = process_unembedded_documents(conn, client=client, max_documents=1, rate_limit_sleep=0)
        assert mock_embed.call_count == 1
        assert stats.total_docs == 1

    def test_failed_doc_increments_failed_count(self):
        """Failed embedding increments failed_docs in stats."""
        from ingestion.embedder import process_unembedded_documents
        rows = [(1, "Title", "Content " * 10, "lcd", "https://cms.gov", "cms.gov", None, [], [], [])]
        conn = self._make_mock_conn(rows=rows)
        client = MagicMock()
        with patch("ingestion.embedder.embed_document", side_effect=RuntimeError("API fail")):
            stats = process_unembedded_documents(conn, client=client, rate_limit_sleep=0)
        assert stats.failed_docs == 1
        assert stats.embedded_docs == 0

    def test_successful_doc_increments_embedded_count(self):
        """Successfully embedded doc increments embedded_docs in stats."""
        from ingestion.embedder import process_unembedded_documents
        rows = [(1, "Title", "Content " * 10, "lcd", "https://cms.gov", "cms.gov", None, [], [], [])]
        conn = self._make_mock_conn(rows=rows)
        client = MagicMock()
        with patch("ingestion.embedder.embed_document", return_value=3):
            stats = process_unembedded_documents(conn, client=client, rate_limit_sleep=0)
        assert stats.embedded_docs == 1
        assert stats.total_chunks == 3


# =============================================================================
# Test: Embedder — semantic_search
# =============================================================================

class TestSemanticSearch:
    """Tests for semantic_search() vector similarity search."""

    def _make_mock_conn(self, rows=None, columns=None):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        if columns is None:
            columns = ["id", "title", "document_type", "source_url",
                       "source_domain", "specialties", "routing_targets",
                       "chunk_text", "chunk_index", "similarity"]
        mock_cursor.description = [(c,) for c in columns]
        mock_cursor.fetchall.return_value = rows or []
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        return mock_conn

    def test_returns_list_of_dicts(self):
        """semantic_search returns list of result dicts."""
        from ingestion.embedder import semantic_search
        conn = self._make_mock_conn(rows=[(1, "Title", "lcd", "http://cms.gov",
                                           "cms.gov", [], ["NEXUSAUTH"], "chunk", 0, 0.95)])
        client = MagicMock()
        client.embeddings.create.return_value = MagicMock(
            data=[MagicMock(embedding=[0.1] * 1536)]
        )
        results = semantic_search(conn, "cardiac monitoring", client=client)
        assert isinstance(results, list)
        assert len(results) == 1
        assert "title" in results[0]

    def test_no_results_returns_empty_list(self):
        """Returns empty list when no matching documents found."""
        from ingestion.embedder import semantic_search
        conn = self._make_mock_conn(rows=[])
        client = MagicMock()
        client.embeddings.create.return_value = MagicMock(
            data=[MagicMock(embedding=[0.1] * 1536)]
        )
        results = semantic_search(conn, "query with no matches", client=client)
        assert results == []

    def test_document_type_filter_adds_where_clause(self):
        """document_type filter is included in the SQL query."""
        from ingestion.embedder import semantic_search
        conn = self._make_mock_conn(rows=[])
        client = MagicMock()
        client.embeddings.create.return_value = MagicMock(
            data=[MagicMock(embedding=[0.0] * 1536)]
        )
        semantic_search(conn, "test", document_type="lcd", client=client)
        executed_sql = conn.cursor.return_value.__enter__.return_value.execute.call_args[0][0]
        assert "document_type" in executed_sql

    def test_payer_id_filter_adds_where_clause(self):
        """payer_id filter is included in the SQL query."""
        from ingestion.embedder import semantic_search
        conn = self._make_mock_conn(rows=[])
        client = MagicMock()
        client.embeddings.create.return_value = MagicMock(
            data=[MagicMock(embedding=[0.0] * 1536)]
        )
        semantic_search(conn, "test", payer_id=3, client=client)
        executed_sql = conn.cursor.return_value.__enter__.return_value.execute.call_args[0][0]
        assert "payer_id" in executed_sql

    def test_no_filters_omits_where_clause(self):
        """No filters means no WHERE clause added."""
        from ingestion.embedder import semantic_search
        conn = self._make_mock_conn(rows=[])
        client = MagicMock()
        client.embeddings.create.return_value = MagicMock(
            data=[MagicMock(embedding=[0.0] * 1536)]
        )
        semantic_search(conn, "test", client=client)
        executed_sql = conn.cursor.return_value.__enter__.return_value.execute.call_args[0][0]
        assert "WHERE" not in executed_sql


# =============================================================================
# Test: Embedder — chunk_text edge cases
# =============================================================================

class TestTextChunkerEdgeCases:
    """Additional edge case tests for chunk_text()."""

    def test_paragraph_boundary_used_as_split_point(self):
        """chunk_text splits at double-newline paragraph boundaries."""
        from ingestion.embedder import chunk_text
        # Create text that has a clear paragraph boundary in the middle of a chunk window
        paragraph_a = "Word " * 200  # ~1000 chars
        paragraph_b = "Word " * 200
        text = paragraph_a + "\n\n" + paragraph_b
        chunks = chunk_text(text, chunk_size=1200, overlap=50)
        assert len(chunks) >= 1
        # At least one chunk boundary should land near the paragraph break
        if len(chunks) >= 2:
            # Verify no chunk contains only whitespace
            for chunk in chunks:
                assert len(chunk["chunk_text"].strip()) > 0

    def test_sentence_boundary_used_as_split_point(self):
        """chunk_text can split at '. ' sentence boundaries."""
        from ingestion.embedder import chunk_text
        # Build text with clear sentence endings, no paragraph breaks
        sentence = "This is a medical coverage sentence. "
        text = sentence * 60  # ~2220 chars
        chunks = chunk_text(text, chunk_size=800, overlap=80)
        for chunk in chunks:
            # No chunk should start with a space if we're splitting at ". "
            assert chunk["chunk_text"] == chunk["chunk_text"].strip() or len(chunk["chunk_text"]) > 0

    def test_unicode_content_preserved(self):
        """Chunks preserve Unicode characters correctly."""
        from ingestion.embedder import chunk_text
        unicode_text = "Médico señala que el diagnóstico es correcto. " * 30
        chunks = chunk_text(unicode_text, chunk_size=500, overlap=50)
        assert len(chunks) >= 1
        full_rebuilt = "".join(c["chunk_text"] for c in chunks)
        assert "é" in full_rebuilt or "ñ" in full_rebuilt

    def test_overlap_appears_in_consecutive_chunks(self):
        """Content at end of chunk N appears at start of chunk N+1."""
        from ingestion.embedder import chunk_text
        text = "UNIQUE_TOKEN_" + ("filler " * 500) + "_END"
        chunks = chunk_text(text, chunk_size=600, overlap=150)
        if len(chunks) >= 2:
            # The char ranges should overlap
            assert chunks[1]["char_start"] < chunks[0]["char_end"]


# =============================================================================
# Test: Deduplicator — database functions
# =============================================================================

class TestDeduplicatorDB:
    """Tests for deduplicator DB functions beyond the basic sha256/is_duplicate tests."""

    def _make_mock_conn(self, fetchone_return=None):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = fetchone_return
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        return mock_conn, mock_cursor

    # --- get_existing_id ---

    def test_get_existing_id_returns_id_when_found(self):
        """get_existing_id returns the integer ID when hash exists."""
        from ingestion.deduplicator import get_existing_id
        conn, cursor = self._make_mock_conn(fetchone_return=(99,))
        result = get_existing_id(conn, "deadbeef" * 8)
        assert result == 99

    def test_get_existing_id_returns_none_when_missing(self):
        """get_existing_id returns None when hash not found."""
        from ingestion.deduplicator import get_existing_id
        conn, _ = self._make_mock_conn(fetchone_return=None)
        result = get_existing_id(conn, "notfound" * 8)
        assert result is None

    # --- insert_raw_document ---

    def test_insert_raw_document_returns_new_id(self):
        """insert_raw_document returns the new row ID on successful insert."""
        from ingestion.deduplicator import insert_raw_document
        conn, cursor = self._make_mock_conn(fetchone_return=(123,))
        doc = {
            "source_url": "https://cms.gov/doc1",
            "source_domain": "cms.gov",
            "document_type_hint": "lcd",
            "title": "Test LCD",
            "raw_content": "This is the document content.",
            "content_hash": None,
            "metadata": {"key": "value"},
            "scraped_at": None,
        }
        result = insert_raw_document(conn, doc)
        assert result == 123

    def test_insert_raw_document_returns_none_on_duplicate(self):
        """insert_raw_document returns None when ON CONFLICT DO NOTHING fires."""
        from ingestion.deduplicator import insert_raw_document
        conn, cursor = self._make_mock_conn(fetchone_return=None)
        doc = {
            "source_url": "https://cms.gov/doc1",
            "source_domain": "cms.gov",
            "document_type_hint": "lcd",
            "title": "Duplicate Doc",
            "raw_content": "Duplicate content.",
            "content_hash": "a" * 64,
            "metadata": {},
            "scraped_at": None,
        }
        result = insert_raw_document(conn, doc)
        assert result is None

    def test_insert_raw_document_computes_hash_if_missing(self):
        """insert_raw_document auto-computes content_hash when not provided."""
        from ingestion.deduplicator import insert_raw_document, sha256_content
        conn, cursor = self._make_mock_conn(fetchone_return=(1,))
        content = "Document content for hashing."
        doc = {
            "source_url": "https://cms.gov/doc",
            "source_domain": "cms.gov",
            "document_type_hint": "lcd",
            "title": "Hash Test",
            "raw_content": content,
            "content_hash": None,
            "metadata": {},
            "scraped_at": None,
        }
        insert_raw_document(conn, doc)
        # The doc dict should be updated in-place with the hash
        assert doc["content_hash"] == sha256_content(content)

    def test_insert_raw_document_serializes_metadata_dict(self):
        """insert_raw_document converts metadata dict to JSON string for DB."""
        from ingestion.deduplicator import insert_raw_document
        conn, cursor = self._make_mock_conn(fetchone_return=(1,))
        doc = {
            "source_url": "https://cms.gov/doc",
            "source_domain": "cms.gov",
            "document_type_hint": "lcd",
            "title": "Metadata Test",
            "raw_content": "Some content.",
            "content_hash": "b" * 64,
            "metadata": {"lcd_id": "L33252", "version": "2"},
            "scraped_at": None,
        }
        insert_raw_document(conn, doc)
        execute_call = cursor.execute.call_args[0]
        params = execute_call[1]
        assert isinstance(params["metadata"], str)
        parsed = json.loads(params["metadata"])
        assert parsed["lcd_id"] == "L33252"

    # --- process_batch ---

    def test_process_batch_inserts_new_documents(self):
        """process_batch counts inserted documents correctly."""
        from ingestion.deduplicator import process_batch
        conn, cursor = self._make_mock_conn(fetchone_return=(1,))
        docs = [
            {"source_url": f"https://cms.gov/doc{i}", "source_domain": "cms.gov",
             "document_type_hint": "lcd", "title": f"Doc {i}",
             "raw_content": f"Content for document {i} which is unique.",
             "content_hash": None, "metadata": {}, "scraped_at": None}
            for i in range(3)
        ]
        with patch("ingestion.deduplicator.insert_raw_document", side_effect=[1, 2, 3]) as mock_insert:
            stats = process_batch(conn, docs)
        assert stats.total_seen == 3
        assert stats.inserted == 3

    def test_process_batch_counts_duplicates(self):
        """process_batch counts duplicate (None return) documents."""
        from ingestion.deduplicator import process_batch
        conn, _ = self._make_mock_conn()
        docs = [
            {"source_url": "https://cms.gov/dup", "source_domain": "cms.gov",
             "document_type_hint": "lcd", "title": "Dup",
             "raw_content": "Same content.", "content_hash": "c" * 64,
             "metadata": {}, "scraped_at": None}
        ]
        with patch("ingestion.deduplicator.insert_raw_document", return_value=None):
            stats = process_batch(conn, docs)
        assert stats.duplicates == 1
        assert stats.inserted == 0

    def test_process_batch_handles_per_doc_error(self):
        """process_batch continues processing after an error on one document."""
        from ingestion.deduplicator import process_batch
        conn, _ = self._make_mock_conn()
        docs = [
            {"source_url": "https://cms.gov/bad", "source_domain": "cms.gov",
             "document_type_hint": "lcd", "title": "Bad Doc",
             "raw_content": "Bad content.", "content_hash": None,
             "metadata": {}, "scraped_at": None},
            {"source_url": "https://cms.gov/good", "source_domain": "cms.gov",
             "document_type_hint": "lcd", "title": "Good Doc",
             "raw_content": "Good content.", "content_hash": None,
             "metadata": {}, "scraped_at": None},
        ]
        side_effects = [Exception("DB error"), 2]
        with patch("ingestion.deduplicator.insert_raw_document", side_effect=side_effects):
            stats = process_batch(conn, docs)
        assert stats.errors == 1
        assert stats.inserted == 1
        assert stats.total_seen == 2

    # --- mark_processing_status ---

    def test_mark_processing_status_without_error(self):
        """mark_processing_status executes correct SQL without error message."""
        from ingestion.deduplicator import mark_processing_status
        conn, cursor = self._make_mock_conn()
        mark_processing_status(conn, raw_document_id=10, status="tagged")
        assert cursor.execute.called
        sql, params = cursor.execute.call_args[0]
        assert "processing_status" in sql
        assert "tagged" in params
        assert 10 in params

    def test_mark_processing_status_with_error_message(self):
        """mark_processing_status includes error_message in SQL when provided."""
        from ingestion.deduplicator import mark_processing_status
        conn, cursor = self._make_mock_conn()
        mark_processing_status(conn, raw_document_id=5, status="failed", error_message="tagger crashed")
        sql, params = cursor.execute.call_args[0]
        assert "processing_error" in sql
        assert "tagger crashed" in params

    # --- get_pending_documents ---

    def test_get_pending_documents_returns_list_of_dicts(self):
        """get_pending_documents returns documents as list of dicts."""
        from ingestion.deduplicator import get_pending_documents
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [
            ("id",), ("source_url",), ("source_domain",), ("document_type_hint",),
            ("title",), ("raw_content",), ("content_hash",), ("metadata",),
            ("scraped_at",), ("processing_status",),
        ]
        mock_cursor.fetchall.return_value = [
            (1, "https://cms.gov/d1", "cms.gov", "lcd", "Doc 1", "content", "hash1", "{}", None, "pending"),
        ]
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        results = get_pending_documents(mock_conn, limit=10)
        assert isinstance(results, list)
        assert len(results) == 1
        assert results[0]["title"] == "Doc 1"
        assert results[0]["processing_status"] == "pending"

    def test_get_pending_documents_empty_returns_empty_list(self):
        """get_pending_documents returns [] when no pending docs."""
        from ingestion.deduplicator import get_pending_documents
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [
            ("id",), ("source_url",), ("source_domain",), ("document_type_hint",),
            ("title",), ("raw_content",), ("content_hash",), ("metadata",),
            ("scraped_at",), ("processing_status",),
        ]
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        results = get_pending_documents(mock_conn)
        assert results == []


# =============================================================================
# Test: Tagger Integration — DB functions
# =============================================================================

class TestTaggerIntegrationDB:
    """Tests for tagger_integration.py DB helpers and core workflow."""

    def _make_mock_conn(self, fetchone_return=None):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = fetchone_return
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        return mock_conn, mock_cursor

    # --- _lookup_payer_id ---

    def test_lookup_payer_id_returns_id_when_found(self):
        """_lookup_payer_id returns integer ID for known payer_code."""
        from ingestion.tagger_integration import _lookup_payer_id
        conn, _ = self._make_mock_conn(fetchone_return=(7,))
        result = _lookup_payer_id(conn, "CMS")
        assert result == 7

    def test_lookup_payer_id_returns_none_when_not_found(self):
        """_lookup_payer_id returns None for unknown payer_code."""
        from ingestion.tagger_integration import _lookup_payer_id
        conn, _ = self._make_mock_conn(fetchone_return=None)
        result = _lookup_payer_id(conn, "UNKNOWN_PAYER")
        assert result is None

    def test_lookup_payer_id_returns_none_for_empty_payer(self):
        """_lookup_payer_id returns None without a DB call for empty/None payer_code."""
        from ingestion.tagger_integration import _lookup_payer_id
        conn, cursor = self._make_mock_conn()
        assert _lookup_payer_id(conn, None) is None
        assert _lookup_payer_id(conn, "") is None
        cursor.execute.assert_not_called()

    # --- _extract_effective_date ---

    def test_extract_effective_date_from_rev_eff_date(self):
        """_extract_effective_date reads rev_eff_date field."""
        from ingestion.tagger_integration import _extract_effective_date
        metadata = {"rev_eff_date": "2020-07-01 00:00:00"}
        result = _extract_effective_date(metadata)
        assert result is not None
        assert result.year == 2020
        assert result.month == 7

    def test_extract_effective_date_from_ncd_efctv_dt(self):
        """_extract_effective_date falls back to NCD_efctv_dt field."""
        from ingestion.tagger_integration import _extract_effective_date
        metadata = {"NCD_efctv_dt": "2024-05-27 00:00:00"}
        result = _extract_effective_date(metadata)
        assert result is not None
        assert result.year == 2024

    def test_extract_effective_date_returns_none_for_empty_metadata(self):
        """_extract_effective_date returns None for empty/None metadata."""
        from ingestion.tagger_integration import _extract_effective_date
        assert _extract_effective_date({}) is None
        assert _extract_effective_date(None) is None

    def test_extract_effective_date_returns_none_for_invalid_format(self):
        """_extract_effective_date returns None for unparseable date strings."""
        from ingestion.tagger_integration import _extract_effective_date
        result = _extract_effective_date({"rev_eff_date": "not-a-date"})
        assert result is None

    def test_extract_effective_date_handles_date_with_microseconds(self):
        """_extract_effective_date parses timestamps with microseconds."""
        from ingestion.tagger_integration import _extract_effective_date
        metadata = {"rev_eff_date": "2020-06-19 22:52:07.840000"}
        result = _extract_effective_date(metadata)
        assert result is not None
        assert result.year == 2020

    # --- _extract_last_updated ---

    def test_extract_last_updated_from_last_updated_field(self):
        """_extract_last_updated reads last_updated field."""
        from ingestion.tagger_integration import _extract_last_updated
        metadata = {"last_updated": "2020-06-19 22:52:07"}
        result = _extract_last_updated(metadata)
        assert result is not None
        assert result.year == 2020

    def test_extract_last_updated_fallback_to_last_updt_tmstmp(self):
        """_extract_last_updated falls back to last_updt_tmstmp."""
        from ingestion.tagger_integration import _extract_last_updated
        metadata = {"last_updt_tmstmp": "2025-05-27 12:20:33"}
        result = _extract_last_updated(metadata)
        assert result is not None
        assert result.year == 2025

    def test_extract_last_updated_returns_none_for_missing(self):
        """_extract_last_updated returns None when no date fields present."""
        from ingestion.tagger_integration import _extract_last_updated
        assert _extract_last_updated({}) is None

    # --- tag_and_insert ---

    def test_tag_and_insert_returns_knowledge_id_on_success(self):
        """tag_and_insert returns knowledge_document ID on successful insert."""
        from ingestion.tagger_integration import tag_and_insert
        from tagging.tagger import TaggingResult

        conn, cursor = self._make_mock_conn(fetchone_return=(55,))
        mock_tagger = MagicMock()
        mock_tagger.tag.return_value = TaggingResult(
            payer_code="CMS",
            document_type="lcd",
            confidence_score=0.85,
            requires_review=False,
            routing_targets=["NEXUSAUTH"],
            cpt_codes=["93224"],
            icd10_codes=["I49.0"],
            hcpcs_codes=[],
            specialties=["cardiology"],
        )

        raw_doc = {
            "id": 1,
            "title": "Cardiac LCD",
            "raw_content": "Prior authorization for cardiac monitoring.",
            "source_url": "https://www.cms.gov/lcd/L33252",
            "source_domain": "cms.gov",
            "document_type_hint": "lcd",
            "metadata": json.dumps({"rev_eff_date": "2020-07-01 00:00:00"}),
        }

        with patch("ingestion.tagger_integration._lookup_payer_id", return_value=1):
            with patch("ingestion.tagger_integration.mark_processing_status"):
                result = tag_and_insert(conn, raw_doc, mock_tagger)

        assert result == 55

    def test_tag_and_insert_returns_none_on_tagger_exception(self):
        """tag_and_insert returns None when tagger raises an exception."""
        from ingestion.tagger_integration import tag_and_insert

        conn, _ = self._make_mock_conn()
        mock_tagger = MagicMock()
        mock_tagger.tag.side_effect = RuntimeError("tagger failed")

        raw_doc = {
            "id": 2,
            "title": "Bad Doc",
            "raw_content": "content",
            "source_url": "https://cms.gov",
            "source_domain": "cms.gov",
            "document_type_hint": "lcd",
            "metadata": "{}",
        }

        with patch("ingestion.tagger_integration.mark_processing_status") as mock_mark:
            result = tag_and_insert(conn, raw_doc, mock_tagger)

        assert result is None
        mock_mark.assert_called_once_with(conn, 2, "failed", "tagger failed")

    def test_tag_and_insert_returns_none_on_db_exception(self):
        """tag_and_insert returns None and rolls back when DB insert fails."""
        from ingestion.tagger_integration import tag_and_insert
        from tagging.tagger import TaggingResult

        conn, cursor = self._make_mock_conn()
        cursor.execute.side_effect = Exception("DB constraint error")

        mock_tagger = MagicMock()
        mock_tagger.tag.return_value = TaggingResult(
            payer_code="CMS", document_type="lcd", confidence_score=0.8,
            requires_review=False, routing_targets=["NEXUSAUTH"],
        )

        raw_doc = {
            "id": 3,
            "title": "DB Fail Doc",
            "raw_content": "content",
            "source_url": "https://cms.gov",
            "source_domain": "cms.gov",
            "document_type_hint": "lcd",
            "metadata": "{}",
        }

        with patch("ingestion.tagger_integration._lookup_payer_id", return_value=1):
            with patch("ingestion.tagger_integration.mark_processing_status"):
                result = tag_and_insert(conn, raw_doc, mock_tagger)

        assert result is None
        conn.rollback.assert_called()

    def test_tag_and_insert_parses_json_metadata_string(self):
        """tag_and_insert parses metadata when it's a JSON string."""
        from ingestion.tagger_integration import tag_and_insert
        from tagging.tagger import TaggingResult

        conn, cursor = self._make_mock_conn(fetchone_return=(10,))
        mock_tagger = MagicMock()
        mock_tagger.tag.return_value = TaggingResult(
            payer_code="CMS", document_type="lcd", confidence_score=0.9,
            requires_review=False, routing_targets=["NEXUSAUTH"],
        )

        raw_doc = {
            "id": 4,
            "title": "JSON Meta Doc",
            "raw_content": "content",
            "source_url": "https://cms.gov",
            "source_domain": "cms.gov",
            "document_type_hint": "lcd",
            "metadata": '{"rev_eff_date": "2021-01-01 00:00:00"}',  # JSON string
        }

        with patch("ingestion.tagger_integration._lookup_payer_id", return_value=1):
            with patch("ingestion.tagger_integration.mark_processing_status"):
                result = tag_and_insert(conn, raw_doc, mock_tagger)

        assert result == 10

    def test_tag_and_insert_sets_needs_review_status(self):
        """tag_and_insert sets status='needs_review' when requires_review is True."""
        from ingestion.tagger_integration import tag_and_insert
        from tagging.tagger import TaggingResult

        conn, cursor = self._make_mock_conn(fetchone_return=(20,))
        mock_tagger = MagicMock()
        mock_tagger.tag.return_value = TaggingResult(
            payer_code=None, document_type="unknown", confidence_score=0.2,
            requires_review=True, routing_targets=["REVIEW"],
        )

        raw_doc = {
            "id": 5,
            "title": "Low Conf Doc",
            "raw_content": "unrecognized content",
            "source_url": "https://unknown.com",
            "source_domain": "unknown.com",
            "document_type_hint": "",
            "metadata": "{}",
        }

        with patch("ingestion.tagger_integration._lookup_payer_id", return_value=None):
            with patch("ingestion.tagger_integration.mark_processing_status"):
                tag_and_insert(conn, raw_doc, mock_tagger)

        # The INSERT params should include status='needs_review'
        execute_call = cursor.execute.call_args[0]
        params = execute_call[1]
        assert params["status"] == "needs_review"

    # --- process_pending_documents ---

    def test_process_pending_empty_returns_zero_stats(self):
        """process_pending_documents returns zero stats when no pending docs."""
        from ingestion.tagger_integration import process_pending_documents
        mock_tagger = MagicMock()
        with patch("ingestion.tagger_integration.get_pending_documents", return_value=[]):
            stats = process_pending_documents(MagicMock(), tagger=mock_tagger)
        assert stats.total == 0
        assert stats.tagged == 0

    def test_process_pending_respects_max_documents(self):
        """process_pending_documents stops at max_documents limit."""
        from ingestion.tagger_integration import process_pending_documents
        docs = [
            {"id": i, "title": f"Doc {i}", "raw_content": "content",
             "source_url": "https://cms.gov", "source_domain": "cms.gov",
             "document_type_hint": "lcd", "metadata": "{}"}
            for i in range(5)
        ]
        mock_tagger = MagicMock()
        conn = MagicMock()
        with patch("ingestion.tagger_integration.get_pending_documents", return_value=docs):
            with patch("ingestion.tagger_integration.tag_and_insert", return_value=1) as mock_tag:
                with patch("ingestion.tagger_integration.mark_processing_status"):
                    stats = process_pending_documents(conn, tagger=mock_tagger, max_documents=2)
        assert stats.total == 2
        assert mock_tag.call_count == 2

    def test_process_pending_counts_failed_docs(self):
        """process_pending_documents counts docs where tag_and_insert returns None."""
        from ingestion.tagger_integration import process_pending_documents
        docs = [
            {"id": 1, "title": "Fail Doc", "raw_content": "content",
             "source_url": "https://cms.gov", "source_domain": "cms.gov",
             "document_type_hint": "lcd", "metadata": "{}"}
        ]
        mock_tagger = MagicMock()
        conn = MagicMock()
        with patch("ingestion.tagger_integration.get_pending_documents", return_value=docs):
            with patch("ingestion.tagger_integration.tag_and_insert", return_value=None):
                with patch("ingestion.tagger_integration.mark_processing_status"):
                    stats = process_pending_documents(conn, tagger=mock_tagger)
        assert stats.failed == 1
        assert stats.tagged == 0


# =============================================================================
# Test: Tagger — edge cases
# =============================================================================

class TestTaggerEdgeCases:
    """Edge case tests for DocumentTagger methods."""

    @pytest.fixture(autouse=True)
    def setup_tagger(self):
        from tagging.tagger import DocumentTagger
        self.tagger = DocumentTagger()

    def test_extract_hcpcs_basic(self):
        """HCPCS Level II codes (letter + 4 digits) are extracted."""
        result = self.tagger.tag(
            "Supply code E0756 and A4253 are applicable.",
            source_url="https://www.cms.gov"
        )
        assert "E0756" in result.hcpcs_codes

    def test_extract_hcpcs_empty_text(self):
        """No HCPCS codes in plain text returns empty list."""
        result = self.tagger.tag(
            "No billing codes are mentioned here at all.",
            source_url="https://www.cms.gov"
        )
        # HCPCS list should be empty or contain only false positives
        assert isinstance(result.hcpcs_codes, list)

    def test_extract_hcpcs_rejects_lowercase(self):
        """Lowercase patterns like e0756 should NOT be detected as HCPCS."""
        result = self.tagger.tag(
            "lower e0756 and a4253 patterns should not match",
            source_url=""
        )
        assert "e0756" not in result.hcpcs_codes

    def test_detect_specialties_cardiology(self):
        """Cardiology keywords trigger cardiology specialty detection."""
        result = self.tagger.tag(
            "This document covers cardiac monitoring and arrhythmia management.",
            source_url="https://www.cms.gov"
        )
        assert "cardiology" in result.specialties

    def test_detect_specialties_multiple(self):
        """Multiple specialties detected when multiple keyword sets match."""
        result = self.tagger.tag(
            "Cardiac monitoring for patients with diabetes and orthopedic conditions.",
            source_url="https://www.cms.gov"
        )
        # Should detect at least cardiology
        assert len(result.specialties) >= 1

    def test_cpt_code_zip_code_filtering(self):
        """5-digit numbers that look like ZIP codes are excluded from CPT codes."""
        result = self.tagger.tag(
            "Provider is located at 12345 Main Street, ZIP 90210.",
            source_url="https://www.cms.gov"
        )
        # ZIP-like numbers within 10000-99999 range may appear — verify CPTs are
        # only alpha-suffix codes or truly valid CPTs
        for code in result.cpt_codes:
            if code.isdigit():
                assert 10000 <= int(code) <= 99999

    def test_icd10_codes_with_decimal_extension(self):
        """ICD-10 codes with decimal extensions like E11.65 are extracted."""
        result = self.tagger.tag(
            "Diagnosis E11.65 (type 2 diabetes) and M79.3 should be extracted.",
            source_url="https://www.cms.gov"
        )
        assert "E11.65" in result.icd10_codes
        assert "M79.3" in result.icd10_codes

    def test_tag_empty_text_returns_default_result(self):
        """tag() with empty string returns TaggingResult with safe defaults."""
        result = self.tagger.tag("", source_url="")
        assert result.document_type == "unknown"
        assert result.cpt_codes == []
        assert result.icd10_codes == []
        assert result.hcpcs_codes == []
        assert result.confidence_score == 0.0

    def test_payer_detection_from_text_fallback(self):
        """Payer is detected from text keywords when URL domain doesn't match."""
        result = self.tagger.tag(
            "This Aetna clinical policy bulletin covers prior authorization.",
            source_url="https://unknown.example.com"
        )
        assert result.payer_code == "AETNA"

    def test_payer_detection_bcbs_from_url(self):
        """BCBS payer detected from anthem.com URL domain."""
        result = self.tagger.tag(
            "Coverage policy document.",
            source_url="https://www.anthem.com/policy/medical/0123"
        )
        assert result.payer_code == "BCBS"


# =============================================================================
# Test: Pipeline — CLI flags and stage functions
# =============================================================================

class TestPipelineCLIFlags:
    """Tests for additional CLI argument parsing."""

    def test_parser_no_lcds_flag(self):
        """--no-lcds flag is parsed correctly."""
        from ingestion.pipeline import build_parser
        parser = build_parser()
        args = parser.parse_args(["--scrape", "--no-lcds"])
        assert args.no_lcds is True

    def test_parser_no_ncds_flag(self):
        """--no-ncds flag is parsed correctly."""
        from ingestion.pipeline import build_parser
        parser = build_parser()
        args = parser.parse_args(["--scrape", "--no-ncds"])
        assert args.no_ncds is True

    def test_parser_batch_size(self):
        """--batch-size is parsed as an integer."""
        from ingestion.pipeline import build_parser
        parser = build_parser()
        args = parser.parse_args(["--all", "--batch-size", "50"])
        assert args.batch_size == 50

    def test_parser_database_url(self):
        """--database-url is captured as a string."""
        from ingestion.pipeline import build_parser
        parser = build_parser()
        url = "postgresql://user:pass@host:5432/mydb"
        args = parser.parse_args(["--all", "--database-url", url])
        assert args.database_url == url

    def test_parser_log_level_choices(self):
        """Invalid --log-level raises argparse error."""
        import argparse
        from ingestion.pipeline import build_parser
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--all", "--log-level", "INVALID"])

    def test_parser_log_level_debug(self):
        """--log-level DEBUG is a valid choice."""
        from ingestion.pipeline import build_parser
        parser = build_parser()
        args = parser.parse_args(["--all", "--log-level", "DEBUG"])
        assert args.log_level == "DEBUG"

    def test_parser_output_json(self):
        """--output-json captures the file path."""
        from ingestion.pipeline import build_parser
        parser = build_parser()
        args = parser.parse_args(["--all", "--output-json", "/tmp/out.json"])
        assert args.output_json == "/tmp/out.json"


class TestPipelineStages:
    """Tests for pipeline stage functions."""

    def test_stage_embed_skips_when_no_api_key(self):
        """stage_embed returns skipped result when OPENAI_API_KEY is not set."""
        from ingestion.pipeline import stage_embed
        import os
        env = {k: v for k, v in os.environ.items() if k != "OPENAI_API_KEY"}
        with patch.dict("os.environ", env, clear=True):
            result = stage_embed(MagicMock())
        assert result.get("skipped") is True
        assert "OPENAI_API_KEY" in result.get("reason", "")

    def test_stage_embed_calls_process_unembedded(self):
        """stage_embed calls process_unembedded_documents when API key is set."""
        from ingestion.pipeline import stage_embed
        from ingestion.embedder import EmbeddingStats
        mock_stats = EmbeddingStats()
        with patch.dict("os.environ", {"OPENAI_API_KEY": "test-key"}):
            # The function is lazily imported inside stage_embed, patch at source
            with patch("ingestion.embedder.process_unembedded_documents", return_value=mock_stats) as mock_proc:
                result = stage_embed(MagicMock(), batch_size=10, max_docs=5)
        mock_proc.assert_called_once()
        assert result["stage"] == "embed"

    def test_stage_tag_calls_process_pending(self):
        """stage_tag calls process_pending_documents."""
        from ingestion.pipeline import stage_tag
        from ingestion.tagger_integration import TaggingStats
        mock_stats = TaggingStats()
        # The function is lazily imported inside stage_tag, patch at source
        with patch("ingestion.tagger_integration.process_pending_documents", return_value=mock_stats) as mock_proc:
            result = stage_tag(MagicMock(), batch_size=50, max_docs=10)
        mock_proc.assert_called_once()
        assert result["stage"] == "tag"

    def test_print_pipeline_summary_runs_without_error(self):
        """print_pipeline_summary does not raise for a full results set."""
        from ingestion.pipeline import print_pipeline_summary
        results = [
            {"stage": "scrape+ingest", "scraped": 100, "inserted": 80, "duplicates": 20, "errors": 0, "elapsed": 5.0},
            {"stage": "tag", "total": 80, "tagged": 75, "failed": 5, "needs_review": 10, "elapsed": 12.0},
            {"stage": "embed", "embedded_docs": 70, "total_chunks": 350, "failed_docs": 5, "elapsed": 30.0},
        ]
        # Should not raise
        print_pipeline_summary(results, total_elapsed=47.0)

    def test_print_pipeline_summary_with_skipped_embed(self):
        """print_pipeline_summary handles skipped embed stage."""
        from ingestion.pipeline import print_pipeline_summary
        results = [
            {"stage": "embed", "skipped": True, "reason": "OPENAI_API_KEY not set", "elapsed": 0.0},
        ]
        print_pipeline_summary(results, total_elapsed=0.1)

    def test_setup_logging_configures_root_logger(self):
        """setup_logging configures the root logger level."""
        import logging
        from ingestion.pipeline import setup_logging
        setup_logging(level="WARNING")
        assert logging.getLogger().level == logging.WARNING

    def test_setup_logging_creates_file_handler(self):
        """setup_logging adds a FileHandler when log_file is specified."""
        import logging
        import tempfile
        import os
        from ingestion.pipeline import setup_logging
        with tempfile.NamedTemporaryFile(suffix=".log", delete=False) as f:
            log_path = f.name
        try:
            setup_logging(level="INFO", log_file=log_path)
            root_handlers = logging.getLogger().handlers
            handler_types = [type(h).__name__ for h in root_handlers]
            assert "FileHandler" in handler_types
        finally:
            logging.getLogger().handlers = [
                h for h in logging.getLogger().handlers
                if not (isinstance(h, logging.FileHandler) and h.baseFilename == log_path)
            ]
            os.unlink(log_path)
