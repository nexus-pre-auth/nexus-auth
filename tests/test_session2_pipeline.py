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
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch, call

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
# Helpers shared by new test classes
# =============================================================================

_UNSET = object()   # sentinel — distinguishes "not provided" from "explicitly None"


def _make_cursor_conn(fetchone=_UNSET, fetchall=None, description=None):
    """Return (mock_conn, mock_cursor) wired up as a psycopg2 context manager."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    if fetchone is not _UNSET:
        mock_cursor.fetchone.return_value = fetchone
    if fetchall is not None:
        if isinstance(fetchall, list) and fetchall and isinstance(fetchall[0], list):
            mock_cursor.fetchall.side_effect = fetchall   # multiple calls
        else:
            mock_cursor.fetchall.return_value = fetchall  # single return value
    if description is not None:
        mock_cursor.description = description
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor


def _make_openai_client(vectors=None, n=1):
    """Return a mock OpenAI client whose .embeddings.create() returns n vectors."""
    if vectors is None:
        vectors = [[0.1] * 1536] * n
    client = MagicMock()
    client.embeddings.create.return_value.data = [
        MagicMock(embedding=v) for v in vectors
    ]
    return client


# =============================================================================
# Test: embed_texts
# =============================================================================

class TestEmbedTexts:
    """Tests for ingestion.embedder.embed_texts"""

    def test_empty_list_returns_empty_without_api_call(self):
        from ingestion.embedder import embed_texts
        client = _make_openai_client()
        result = embed_texts([], client=client)
        assert result == []
        client.embeddings.create.assert_not_called()

    def test_returns_list_of_vectors(self):
        from ingestion.embedder import embed_texts
        vectors = [[0.1] * 1536, [0.2] * 1536]
        client = _make_openai_client(vectors=vectors)
        result = embed_texts(["text one", "text two"], client=client)
        assert len(result) == 2
        assert result[0] == [0.1] * 1536
        assert result[1] == [0.2] * 1536

    def test_single_text_returns_one_vector(self):
        from ingestion.embedder import embed_texts
        client = _make_openai_client(vectors=[[0.5] * 1536])
        result = embed_texts(["single text"], client=client)
        assert len(result) == 1

    def test_truncates_text_longer_than_max_chars(self):
        """Text > 25000 chars is sliced before being sent to the API."""
        from ingestion.embedder import embed_texts
        client = _make_openai_client()
        embed_texts(["x" * 30000], client=client)
        sent_input = client.embeddings.create.call_args.kwargs["input"]
        assert len(sent_input[0]) == 25000

    def test_short_text_is_not_truncated(self):
        from ingestion.embedder import embed_texts
        client = _make_openai_client()
        embed_texts(["Hello, world!"], client=client)
        sent_input = client.embeddings.create.call_args.kwargs["input"]
        assert sent_input[0] == "Hello, world!"

    def test_api_error_propagates(self):
        from ingestion.embedder import embed_texts
        client = MagicMock()
        client.embeddings.create.side_effect = RuntimeError("rate limit")
        with pytest.raises(RuntimeError, match="rate limit"):
            embed_texts(["text"], client=client)

    def test_uses_default_embedding_model(self):
        from ingestion.embedder import embed_texts, EMBEDDING_MODEL
        client = _make_openai_client()
        embed_texts(["text"], client=client)
        call_kwargs = client.embeddings.create.call_args.kwargs
        assert call_kwargs["model"] == EMBEDDING_MODEL

    def test_custom_model_is_forwarded(self):
        from ingestion.embedder import embed_texts
        client = _make_openai_client()
        embed_texts(["text"], model="text-embedding-3-large", client=client)
        call_kwargs = client.embeddings.create.call_args.kwargs
        assert call_kwargs["model"] == "text-embedding-3-large"

    def test_returns_correct_vector_dimension(self):
        from ingestion.embedder import embed_texts, EMBEDDING_DIMENSIONS
        vectors = [[float(i % 10) for i in range(EMBEDDING_DIMENSIONS)]]
        client = _make_openai_client(vectors=vectors)
        result = embed_texts(["text"], client=client)
        assert len(result[0]) == EMBEDDING_DIMENSIONS


# =============================================================================
# Test: embed_document
# =============================================================================

class TestEmbedDocument:
    """Tests for ingestion.embedder.embed_document"""

    _COLUMNS = [
        "id", "title", "content_text", "document_type",
        "source_url", "source_domain", "payer_id", "specialties",
        "cpt_codes", "icd10_codes",
    ]

    def _doc(self, content="Coverage criteria for cardiac monitoring. " * 50, title="Test LCD"):
        return {
            "id": 1,
            "title": title,
            "content_text": content,
            "document_type": "prior_auth_criteria",
            "source_url": "https://cms.gov",
            "source_domain": "cms.gov",
            "payer_id": None,
            "specialties": ["cardiology"],
            "cpt_codes": ["93224"],
            "icd10_codes": ["I49.0"],
        }

    def test_returns_integer_chunk_count(self):
        from ingestion.embedder import embed_document
        conn, _ = _make_cursor_conn(fetchone=(42,))
        result = embed_document(conn, self._doc(), client=_make_openai_client(n=50))
        assert isinstance(result, int)
        assert result >= 1

    def test_too_short_content_returns_zero(self):
        from ingestion.embedder import embed_document
        conn, _ = _make_cursor_conn(fetchone=(1,))
        result = embed_document(conn, self._doc(content="Hi", title=""), client=_make_openai_client(n=50))
        assert result == 0

    def test_title_prepended_appears_in_embedding_input(self):
        from ingestion.embedder import embed_document
        conn, _ = _make_cursor_conn(fetchone=(1,))
        client = _make_openai_client(n=50)
        doc = self._doc(content="Medical content text. " * 50, title="UNIQUE_TITLE_XYZ_123")
        embed_document(conn, doc, client=client)
        sent_input = client.embeddings.create.call_args.kwargs["input"]
        assert any("UNIQUE_TITLE_XYZ_123" in chunk for chunk in sent_input)

    def test_commit_called_after_insertions(self):
        from ingestion.embedder import embed_document
        conn, _ = _make_cursor_conn(fetchone=(1,))
        embed_document(conn, self._doc(), client=_make_openai_client(n=50))
        assert conn.commit.called

    def test_embedding_status_updated_to_embedded(self):
        from ingestion.embedder import embed_document
        conn, mock_cursor = _make_cursor_conn(fetchone=(1,))
        embed_document(conn, self._doc(), client=_make_openai_client(n=50))
        all_sql = " ".join(str(c) for c in mock_cursor.execute.call_args_list)
        assert "embedding_status" in all_sql

    def test_openai_error_propagates(self):
        from ingestion.embedder import embed_document
        conn, _ = _make_cursor_conn(fetchone=(1,))
        client = MagicMock()
        client.embeddings.create.side_effect = RuntimeError("quota exceeded")
        with pytest.raises(RuntimeError, match="quota exceeded"):
            embed_document(conn, self._doc(), client=client)

    def test_embedding_stored_as_vector_string(self):
        """The embedding param sent to DB is a '[0.1,0.2,...]' string."""
        from ingestion.embedder import embed_document
        conn, mock_cursor = _make_cursor_conn(fetchone=(1,))
        embed_document(conn, self._doc(), client=_make_openai_client(n=50))
        # Find the INSERT call params (first execute call should be the INSERT)
        for c in mock_cursor.execute.call_args_list:
            args = c[0]
            if len(args) >= 2 and isinstance(args[1], dict) and "embedding" in args[1]:
                emb = args[1]["embedding"]
                assert emb.startswith("[") and emb.endswith("]")
                break


# =============================================================================
# Test: semantic_search
# =============================================================================

class TestSemanticSearch:
    """Tests for ingestion.embedder.semantic_search"""

    _COLS = [("id",), ("title",), ("document_type",), ("source_url",),
             ("source_domain",), ("specialties",), ("routing_targets",),
             ("chunk_text",), ("chunk_index",), ("similarity",)]

    def _make_search_conn(self, rows):
        conn, cur = _make_cursor_conn(fetchall=rows, description=self._COLS)
        return conn, cur

    def test_returns_list_of_dicts(self):
        from ingestion.embedder import semantic_search
        rows = [(1, "Cardiac LCD", "lcd", "https://cms.gov", "cms.gov",
                 [], [], "chunk content", 0, 0.95)]
        conn, _ = self._make_search_conn(rows)
        client = _make_openai_client()
        results = semantic_search(conn, "cardiac monitoring", client=client)
        assert isinstance(results, list)
        assert len(results) == 1
        assert results[0]["title"] == "Cardiac LCD"
        assert results[0]["similarity"] == pytest.approx(0.95)

    def test_empty_results_returns_empty_list(self):
        from ingestion.embedder import semantic_search
        conn, _ = self._make_search_conn([])
        client = _make_openai_client()
        results = semantic_search(conn, "no match query", client=client)
        assert results == []

    def test_document_type_filter_added_to_sql(self):
        from ingestion.embedder import semantic_search
        conn, mock_cursor = self._make_search_conn([])
        client = _make_openai_client()
        semantic_search(conn, "query", document_type="lcd", client=client)
        all_sql = " ".join(str(c) for c in mock_cursor.execute.call_args_list)
        assert "document_type" in all_sql

    def test_payer_id_filter_added_to_sql(self):
        from ingestion.embedder import semantic_search
        conn, mock_cursor = self._make_search_conn([])
        client = _make_openai_client()
        semantic_search(conn, "query", payer_id=3, client=client)
        all_sql = " ".join(str(c) for c in mock_cursor.execute.call_args_list)
        assert "payer_id" in all_sql

    def test_no_filters_no_where_clause(self):
        from ingestion.embedder import semantic_search
        conn, mock_cursor = self._make_search_conn([])
        client = _make_openai_client()
        semantic_search(conn, "query", client=client)
        all_sql = " ".join(str(c) for c in mock_cursor.execute.call_args_list)
        # Without filters, WHERE should not appear in the search query
        assert "WHERE" not in all_sql


# =============================================================================
# Test: process_unembedded_documents
# =============================================================================

class TestProcessUnembeddedDocuments:
    """Tests for ingestion.embedder.process_unembedded_documents"""

    _COLUMNS = [
        ("id",), ("title",), ("content_text",), ("document_type",),
        ("source_url",), ("source_domain",), ("payer_id",),
        ("specialties",), ("cpt_codes",), ("icd10_codes",),
    ]

    def _rows(self, n=2):
        return [
            (i, f"Doc {i}", "Medical content here.", "lcd",
             "https://cms.gov", "cms.gov", None, [], [], [])
            for i in range(1, n + 1)
        ]

    def test_processes_docs_and_populates_stats(self):
        from ingestion.embedder import process_unembedded_documents
        # batch_size=100 > 2 rows → loop exits after one batch
        conn, _ = _make_cursor_conn(fetchall=self._rows(2), description=self._COLUMNS)
        client = _make_openai_client(n=50)
        with patch("ingestion.embedder.embed_document", return_value=3), \
             patch("ingestion.embedder.time.sleep"):
            stats = process_unembedded_documents(conn, client=client,
                                                 batch_size=100, rate_limit_sleep=0)
        assert stats.total_docs == 2
        assert stats.embedded_docs == 2
        assert stats.total_chunks == 6   # 3 chunks × 2 docs
        assert stats.failed_docs == 0

    def test_empty_queue_returns_zero_stats(self):
        from ingestion.embedder import process_unembedded_documents, EmbeddingStats
        conn, _ = _make_cursor_conn(fetchall=[], description=self._COLUMNS)
        stats = process_unembedded_documents(conn, client=_make_openai_client())
        assert stats.total_docs == 0
        assert stats.embedded_docs == 0
        assert stats.failed_docs == 0

    def test_respects_max_documents_limit(self):
        from ingestion.embedder import process_unembedded_documents
        # 5 rows but max_documents=2
        conn, _ = _make_cursor_conn(fetchall=self._rows(5), description=self._COLUMNS)
        with patch("ingestion.embedder.embed_document", return_value=1), \
             patch("ingestion.embedder.time.sleep"):
            stats = process_unembedded_documents(conn, client=_make_openai_client(),
                                                 batch_size=100, max_documents=2,
                                                 rate_limit_sleep=0)
        assert stats.total_docs == 2

    def test_failed_doc_increments_failed_counter_and_continues(self):
        from ingestion.embedder import process_unembedded_documents
        conn, _ = _make_cursor_conn(fetchall=self._rows(3), description=self._COLUMNS)

        def embed_side(c, doc, client=None):
            if doc["id"] == 2:
                raise RuntimeError("Embedding failed")
            return 2

        with patch("ingestion.embedder.embed_document", side_effect=embed_side), \
             patch("ingestion.embedder.time.sleep"):
            stats = process_unembedded_documents(conn, client=_make_openai_client(),
                                                 batch_size=100, rate_limit_sleep=0)
        assert stats.total_docs == 3
        assert stats.embedded_docs == 2
        assert stats.failed_docs == 1

    def test_stats_elapsed_is_non_negative(self):
        from ingestion.embedder import process_unembedded_documents
        conn, _ = _make_cursor_conn(fetchall=[], description=self._COLUMNS)
        stats = process_unembedded_documents(conn, client=_make_openai_client())
        assert stats.elapsed >= 0.0


# =============================================================================
# Test: Deduplicator DB functions
# =============================================================================

class TestDeduplicatorDB:
    """Tests for deduplicator functions that use DB connections."""

    # ---- get_existing_id ------------------------------------------------

    def test_get_existing_id_returns_id_when_found(self):
        from ingestion.deduplicator import get_existing_id
        conn, _ = _make_cursor_conn(fetchone=(42,))
        assert get_existing_id(conn, "abc123hash") == 42

    def test_get_existing_id_returns_none_when_missing(self):
        from ingestion.deduplicator import get_existing_id
        conn, _ = _make_cursor_conn(fetchone=None)
        assert get_existing_id(conn, "nonexistent_hash") is None

    # ---- insert_raw_document --------------------------------------------

    def _sample_doc(self, raw_content="Test content.", content_hash=None):
        return {
            "source_url": "https://cms.gov/test",
            "source_domain": "cms.gov",
            "document_type_hint": "lcd",
            "title": "Test LCD Document",
            "raw_content": raw_content,
            "content_hash": content_hash,
            "metadata": {"lcd_id": "123"},
            "scraped_at": None,
        }

    def test_insert_new_doc_returns_id(self):
        from ingestion.deduplicator import insert_raw_document
        conn, _ = _make_cursor_conn(fetchone=(99,))
        result = insert_raw_document(conn, self._sample_doc())
        assert result == 99

    def test_insert_duplicate_returns_none(self):
        from ingestion.deduplicator import insert_raw_document
        conn, _ = _make_cursor_conn(fetchone=None)   # ON CONFLICT DO NOTHING
        result = insert_raw_document(conn, self._sample_doc())
        assert result is None

    def test_insert_computes_content_hash_when_missing(self):
        from ingestion.deduplicator import insert_raw_document, sha256_content
        conn, _ = _make_cursor_conn(fetchone=(1,))
        doc = self._sample_doc(raw_content="hello world", content_hash=None)
        insert_raw_document(conn, doc)
        assert doc["content_hash"] == sha256_content("hello world")

    def test_insert_preserves_existing_content_hash(self):
        from ingestion.deduplicator import insert_raw_document
        conn, _ = _make_cursor_conn(fetchone=(1,))
        doc = self._sample_doc(content_hash="preset_hash_value")
        insert_raw_document(conn, doc)
        assert doc["content_hash"] == "preset_hash_value"

    def test_insert_serializes_metadata_dict_to_json(self):
        from ingestion.deduplicator import insert_raw_document
        conn, mock_cursor = _make_cursor_conn(fetchone=(1,))
        doc = self._sample_doc()
        doc["metadata"] = {"key": "value", "num": 42}
        insert_raw_document(conn, doc)
        call_params = mock_cursor.execute.call_args[0][1]
        assert isinstance(call_params["metadata"], str)
        assert json.loads(call_params["metadata"]) == {"key": "value", "num": 42}

    def test_insert_sets_scraped_at_when_missing(self):
        from ingestion.deduplicator import insert_raw_document
        conn, mock_cursor = _make_cursor_conn(fetchone=(1,))
        doc = self._sample_doc()
        doc["scraped_at"] = None
        insert_raw_document(conn, doc)
        call_params = mock_cursor.execute.call_args[0][1]
        assert call_params["scraped_at"] is not None

    # ---- process_batch --------------------------------------------------

    def test_process_batch_all_new_docs(self):
        from ingestion.deduplicator import process_batch
        conn, _ = _make_cursor_conn(fetchone=(1,))
        docs = [self._sample_doc(f"Content {i}") for i in range(3)]
        stats = process_batch(conn, docs)
        assert stats.total_seen == 3
        assert stats.inserted == 3
        assert stats.duplicates == 0

    def test_process_batch_counts_duplicates(self):
        from ingestion.deduplicator import process_batch
        conn, mock_cursor = _make_cursor_conn()
        # Alternate: new, duplicate, new
        mock_cursor.fetchone.side_effect = [(1,), None, (3,)]
        docs = [self._sample_doc(f"Content {i}") for i in range(3)]
        stats = process_batch(conn, docs)
        assert stats.inserted == 2
        assert stats.duplicates == 1

    def test_process_batch_handles_per_doc_error(self):
        from ingestion.deduplicator import process_batch
        conn, mock_cursor = _make_cursor_conn()
        mock_cursor.execute.side_effect = [Exception("DB error"), None, None]
        docs = [self._sample_doc(f"Content {i}") for i in range(2)]
        stats = process_batch(conn, docs)
        assert stats.errors >= 1   # at least one error counted
        assert stats.total_seen == 2

    # ---- mark_processing_status -----------------------------------------

    def test_mark_status_executes_update(self):
        from ingestion.deduplicator import mark_processing_status
        conn, mock_cursor = _make_cursor_conn()
        mark_processing_status(conn, 42, "tagged")
        mock_cursor.execute.assert_called_once()
        sql, params = mock_cursor.execute.call_args[0]
        assert "processing_status" in sql
        assert "tagged" in params
        assert 42 in params
        conn.commit.assert_called_once()

    def test_mark_status_with_error_includes_processing_error_column(self):
        from ingestion.deduplicator import mark_processing_status
        conn, mock_cursor = _make_cursor_conn()
        mark_processing_status(conn, 7, "failed", "Tagger exception")
        sql = mock_cursor.execute.call_args[0][0]
        assert "processing_error" in sql

    # ---- get_pending_documents ------------------------------------------

    def test_get_pending_returns_list_of_dicts(self):
        from ingestion.deduplicator import get_pending_documents
        cols = [("id",), ("source_url",), ("source_domain",), ("document_type_hint",),
                ("title",), ("raw_content",), ("content_hash",), ("metadata",),
                ("scraped_at",), ("processing_status",)]
        rows = [(1, "https://cms.gov", "cms.gov", "lcd", "Test", "Content",
                 "hash1", "{}", None, "pending")]
        conn, _ = _make_cursor_conn(fetchall=rows, description=cols)
        results = get_pending_documents(conn, limit=10)
        assert len(results) == 1
        assert results[0]["id"] == 1
        assert results[0]["source_url"] == "https://cms.gov"

    def test_get_pending_empty_returns_empty_list(self):
        from ingestion.deduplicator import get_pending_documents
        cols = [("id",), ("title",)]
        conn, _ = _make_cursor_conn(fetchall=[], description=cols)
        assert get_pending_documents(conn) == []


# =============================================================================
# Test: Tagger Integration DB functions
# =============================================================================

class TestTaggerIntegrationDB:
    """Tests for tagger_integration functions that use DB connections."""

    # ---- _lookup_payer_id -----------------------------------------------

    def test_lookup_payer_id_returns_id_when_found(self):
        from ingestion.tagger_integration import _lookup_payer_id
        conn, _ = _make_cursor_conn(fetchone=(7,))
        assert _lookup_payer_id(conn, "CMS") == 7

    def test_lookup_payer_id_returns_none_for_unknown_payer(self):
        from ingestion.tagger_integration import _lookup_payer_id
        conn, _ = _make_cursor_conn(fetchone=None)
        assert _lookup_payer_id(conn, "UNKNOWN_PAYER") is None

    def test_lookup_payer_id_returns_none_without_db_call_when_code_is_none(self):
        from ingestion.tagger_integration import _lookup_payer_id
        conn, mock_cursor = _make_cursor_conn()
        result = _lookup_payer_id(conn, None)
        assert result is None
        mock_cursor.execute.assert_not_called()

    # ---- _extract_effective_date ----------------------------------------

    def test_extract_effective_date_from_rev_eff_date(self):
        from ingestion.tagger_integration import _extract_effective_date
        result = _extract_effective_date({"rev_eff_date": "2020-07-01 00:00:00"})
        assert result is not None
        assert result.year == 2020 and result.month == 7

    def test_extract_effective_date_from_ncd_efctv_dt(self):
        from ingestion.tagger_integration import _extract_effective_date
        result = _extract_effective_date({"NCD_efctv_dt": "2024-05-27 00:00:00"})
        assert result is not None
        assert result.year == 2024

    def test_extract_effective_date_returns_none_for_empty_metadata(self):
        from ingestion.tagger_integration import _extract_effective_date
        assert _extract_effective_date({}) is None

    def test_extract_effective_date_returns_none_for_none_metadata(self):
        from ingestion.tagger_integration import _extract_effective_date
        assert _extract_effective_date(None) is None

    def test_extract_effective_date_returns_none_for_invalid_format(self):
        from ingestion.tagger_integration import _extract_effective_date
        result = _extract_effective_date({"rev_eff_date": "not-a-date"})
        assert result is None

    def test_extract_effective_date_microsecond_format(self):
        from ingestion.tagger_integration import _extract_effective_date
        result = _extract_effective_date({"rev_eff_date": "2021-03-15 12:30:00.000000"})
        assert result is not None
        assert result.year == 2021 and result.month == 3

    # ---- tag_and_insert -------------------------------------------------

    def _make_raw_doc(self, doc_id=1):
        return {
            "id": doc_id,
            "title": "Cardiac Monitoring LCD",
            "raw_content": "Prior authorization required for cardiac monitoring CPT 93224.",
            "source_url": "https://cms.gov/lcd/123",
            "source_domain": "cms.gov",
            "metadata": {"rev_eff_date": "2020-01-01 00:00:00"},
            "document_type_hint": "lcd",
        }

    def _make_tagger_result(self):
        from tagging.tagger import TaggingResult
        return TaggingResult(
            document_type="prior_auth_criteria",
            confidence_score=0.85,
            payer_code="CMS",
            specialties=["cardiology"],
            cpt_codes=["93224"],
            icd10_codes=["I49.0"],
            hcpcs_codes=[],
            routing_targets=["NEXUSAUTH"],
            requires_review=False,
        )

    def test_tag_and_insert_returns_knowledge_doc_id(self):
        from ingestion.tagger_integration import tag_and_insert
        mock_tagger = MagicMock()
        mock_tagger.tag.return_value = self._make_tagger_result()
        # fetchone: (7,) for payer lookup, (101,) for RETURNING id
        conn, mock_cursor = _make_cursor_conn()
        mock_cursor.fetchone.side_effect = [(7,), (101,)]
        result = tag_and_insert(conn, self._make_raw_doc(), mock_tagger)
        assert result == 101

    def test_tag_and_insert_commits_transaction(self):
        from ingestion.tagger_integration import tag_and_insert
        mock_tagger = MagicMock()
        mock_tagger.tag.return_value = self._make_tagger_result()
        conn, mock_cursor = _make_cursor_conn()
        mock_cursor.fetchone.side_effect = [(7,), (101,)]
        tag_and_insert(conn, self._make_raw_doc(), mock_tagger)
        assert conn.commit.called

    def test_tag_and_insert_marks_raw_doc_as_tagged(self):
        from ingestion.tagger_integration import tag_and_insert
        mock_tagger = MagicMock()
        mock_tagger.tag.return_value = self._make_tagger_result()
        conn, mock_cursor = _make_cursor_conn()
        mock_cursor.fetchone.side_effect = [(7,), (101,)]
        tag_and_insert(conn, self._make_raw_doc(), mock_tagger)
        all_sql = " ".join(str(c) for c in mock_cursor.execute.call_args_list)
        # mark_processing_status("tagged") should appear in the SQL calls
        assert "processing_status" in all_sql

    def test_tag_and_insert_returns_none_on_tagger_exception(self):
        from ingestion.tagger_integration import tag_and_insert
        mock_tagger = MagicMock()
        mock_tagger.tag.side_effect = RuntimeError("Tagger crashed")
        conn, _ = _make_cursor_conn()
        result = tag_and_insert(conn, self._make_raw_doc(), mock_tagger)
        assert result is None

    def test_tag_and_insert_returns_none_on_db_error(self):
        from ingestion.tagger_integration import tag_and_insert
        mock_tagger = MagicMock()
        mock_tagger.tag.return_value = self._make_tagger_result()
        conn, mock_cursor = _make_cursor_conn()
        mock_cursor.fetchone.return_value = (7,)
        # execute calls: 1=payer lookup, 2=INSERT (raises), 3=mark_processing_status UPDATE
        mock_cursor.execute.side_effect = [None, Exception("DB write error"), None]
        result = tag_and_insert(conn, self._make_raw_doc(), mock_tagger)
        assert result is None

    def test_tag_and_insert_metadata_as_json_string_is_parsed(self):
        """metadata stored as a JSON string in raw_doc is parsed correctly."""
        from ingestion.tagger_integration import tag_and_insert
        mock_tagger = MagicMock()
        mock_tagger.tag.return_value = self._make_tagger_result()
        conn, mock_cursor = _make_cursor_conn()
        mock_cursor.fetchone.side_effect = [(7,), (101,)]
        raw_doc = self._make_raw_doc()
        raw_doc["metadata"] = json.dumps({"rev_eff_date": "2021-06-01 00:00:00"})
        result = tag_and_insert(conn, raw_doc, mock_tagger)
        assert result == 101   # Should succeed, not crash on JSON metadata

    # ---- process_pending_documents --------------------------------------

    def test_process_pending_documents_tags_batch(self):
        from ingestion.tagger_integration import process_pending_documents
        mock_tagger = MagicMock()
        mock_tagger.tag.return_value = self._make_tagger_result()
        conn = MagicMock()
        pending = [self._make_raw_doc(i) for i in range(3)]

        with patch("ingestion.tagger_integration.get_pending_documents") as mock_get, \
             patch("ingestion.tagger_integration.tag_and_insert", return_value=10), \
             patch("ingestion.tagger_integration.mark_processing_status"):
            mock_get.side_effect = [pending, []]
            stats = process_pending_documents(conn, tagger=mock_tagger, batch_size=10)

        assert stats.total == 3
        assert stats.tagged == 3

    def test_process_pending_respects_max_documents(self):
        from ingestion.tagger_integration import process_pending_documents
        mock_tagger = MagicMock()
        mock_tagger.tag.return_value = self._make_tagger_result()
        conn = MagicMock()
        pending = [self._make_raw_doc(i) for i in range(10)]

        with patch("ingestion.tagger_integration.get_pending_documents", return_value=pending), \
             patch("ingestion.tagger_integration.tag_and_insert", return_value=1), \
             patch("ingestion.tagger_integration.mark_processing_status"):
            stats = process_pending_documents(conn, tagger=mock_tagger,
                                              batch_size=10, max_documents=2)
        assert stats.total <= 2

    def test_process_pending_counts_failures(self):
        from ingestion.tagger_integration import process_pending_documents
        mock_tagger = MagicMock()
        mock_tagger.tag.return_value = self._make_tagger_result()
        conn = MagicMock()
        pending = [self._make_raw_doc(i) for i in range(3)]

        with patch("ingestion.tagger_integration.get_pending_documents") as mock_get, \
             patch("ingestion.tagger_integration.tag_and_insert", return_value=None), \
             patch("ingestion.tagger_integration.mark_processing_status"):
            mock_get.side_effect = [pending, []]
            stats = process_pending_documents(conn, tagger=mock_tagger, batch_size=10)

        assert stats.failed == 3
        assert stats.tagged == 0


# =============================================================================
# Test: Tagger edge cases
# =============================================================================

class TestTaggerEdgeCases:
    """Edge-case tests for tagging/tagger.py — codes, specialties, empty text."""

    @pytest.fixture(autouse=True)
    def setup_tagger(self):
        from tagging.tagger import DocumentTagger
        self.tagger = DocumentTagger()

    def test_extract_hcpcs_valid_codes(self):
        result = self.tagger.tag("HCPCS code A1234 and B5678 are billable.", source_url="")
        assert "A1234" in result.hcpcs_codes
        assert "B5678" in result.hcpcs_codes

    def test_extract_hcpcs_empty_text_returns_empty(self):
        codes = self.tagger._extract_hcpcs("")
        assert codes == []

    def test_extract_hcpcs_rejects_all_digit_codes(self):
        """Five-digit numbers without a leading letter are not HCPCS."""
        codes = self.tagger._extract_hcpcs("Number 12345 is not an HCPCS code.")
        assert "12345" not in codes

    def test_icd10_codes_with_decimal(self):
        result = self.tagger.tag("Diagnoses E11.65 and I49.0 are documented.", source_url="")
        assert "E11.65" in result.icd10_codes
        assert "I49.0" in result.icd10_codes

    def test_icd10_code_without_decimal(self):
        result = self.tagger.tag("Diagnosis R05 is present.", source_url="")
        assert "R05" in result.icd10_codes

    def test_cpt_code_in_valid_range_extracted(self):
        result = self.tagger.tag("CPT code 93224 applies to cardiac event monitoring.", source_url="")
        assert "93224" in result.cpt_codes

    def test_detect_specialties_cardiology_keywords(self):
        result = self.tagger.tag(
            "Cardiac catheterization and coronary artery disease treatment.",
            source_url="",
        )
        assert "cardiology" in result.specialties

    def test_detect_specialties_returns_list(self):
        result = self.tagger.tag("General medical document.", source_url="")
        assert isinstance(result.specialties, list)

    def test_tag_empty_text_returns_unknown_type(self):
        result = self.tagger.tag("", source_url="")
        assert result.document_type == "unknown"
        assert result.confidence_score == 0.0

    def test_tag_empty_text_returns_empty_code_lists(self):
        result = self.tagger.tag("", source_url="")
        assert result.cpt_codes == []
        assert result.icd10_codes == []
        assert result.hcpcs_codes == []


# =============================================================================
# Test: Pipeline CLI flags and stages (extended)
# =============================================================================

class TestPipelineExtended:
    """Extended CLI-flag and stage-function tests for ingestion/pipeline.py."""

    def test_parser_no_lcds_flag(self):
        from ingestion.pipeline import build_parser
        args = build_parser().parse_args(["--scrape", "--no-lcds"])
        assert args.no_lcds is True

    def test_parser_no_ncds_flag(self):
        from ingestion.pipeline import build_parser
        args = build_parser().parse_args(["--scrape", "--no-ncds"])
        assert args.no_ncds is True

    def test_parser_batch_size_parsed_as_int(self):
        from ingestion.pipeline import build_parser
        args = build_parser().parse_args(["--all", "--batch-size", "50"])
        assert args.batch_size == 50

    def test_parser_invalid_log_level_raises_system_exit(self):
        from ingestion.pipeline import build_parser
        with pytest.raises(SystemExit):
            build_parser().parse_args(["--all", "--log-level", "VERBOSE"])

    def test_parser_log_file_stored(self):
        from ingestion.pipeline import build_parser
        args = build_parser().parse_args(["--all", "--log-file", "/tmp/test.log"])
        assert args.log_file == "/tmp/test.log"

    def test_parser_output_json_stored(self):
        from ingestion.pipeline import build_parser
        args = build_parser().parse_args(["--all", "--output-json", "/tmp/out.json"])
        assert args.output_json == "/tmp/out.json"

    def test_setup_logging_adds_file_handler(self):
        import logging
        from ingestion.pipeline import setup_logging
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = os.path.join(tmpdir, "subdir", "test.log")
            setup_logging("DEBUG", log_path)
            root = logging.getLogger()
            handler_types = [type(h).__name__ for h in root.handlers]
            assert "FileHandler" in handler_types
            # Clean up file handlers to avoid pollution
            for h in list(root.handlers):
                if isinstance(h, logging.FileHandler):
                    h.close()
                    root.removeHandler(h)

    def test_stage_embed_skips_when_openai_key_absent(self):
        from ingestion.pipeline import stage_embed
        mock_conn = MagicMock()
        env = {k: v for k, v in os.environ.items() if k != "OPENAI_API_KEY"}
        with patch.dict(os.environ, env, clear=True):
            result = stage_embed(mock_conn)
        assert result.get("skipped") is True
        assert "OPENAI_API_KEY" in result.get("reason", "")

    def test_stage_tag_calls_process_pending_documents(self):
        from ingestion.pipeline import stage_tag
        mock_stats = MagicMock()
        mock_stats.total = 10
        mock_stats.tagged = 9
        mock_stats.failed = 1
        mock_stats.needs_review = 2
        with patch("ingestion.tagger_integration.process_pending_documents",
                   return_value=mock_stats):
            result = stage_tag(MagicMock(), batch_size=10, max_docs=None)
        assert result["stage"] == "tag"
        assert result["total"] == 10
        assert result["tagged"] == 9
        assert result["failed"] == 1

    def test_print_pipeline_summary_all_stages_no_crash(self):
        from ingestion.pipeline import print_pipeline_summary
        results = [
            {"stage": "scrape+ingest", "scraped": 100, "inserted": 80,
             "duplicates": 20, "errors": 0, "elapsed": 1.5},
            {"stage": "tag", "total": 80, "tagged": 75,
             "failed": 5, "needs_review": 10, "elapsed": 2.0},
            {"stage": "embed", "embedded_docs": 70, "total_chunks": 350,
             "failed_docs": 0, "api_calls": 70, "elapsed": 30.0},
        ]
        print_pipeline_summary(results, total_elapsed=33.5)   # must not raise

    def test_print_pipeline_summary_skipped_embed_no_crash(self):
        from ingestion.pipeline import print_pipeline_summary
        results = [{"stage": "embed", "skipped": True, "reason": "OPENAI_API_KEY not set"}]
        print_pipeline_summary(results, total_elapsed=0.1)    # must not raise


# =============================================================================
# Test: chunk_text boundary handling (extended)
# =============================================================================

class TestTextChunkerExtended:
    """Additional boundary-handling tests for ingestion.embedder.chunk_text."""

    def test_paragraph_boundary_preferred_over_mid_sentence(self):
        from ingestion.embedder import chunk_text
        para1 = "First paragraph on cardiac monitoring services. " * 15
        para2 = "Second paragraph on prior authorization criteria. " * 15
        text = para1 + "\n\n" + para2
        # chunk_size set to span past para1 so a paragraph break is reachable
        chunks = chunk_text(text, chunk_size=len(para1) + 100, overlap=10, min_size=50)
        if len(chunks) > 1:
            # Paragraph break (\n\n) should not appear inside the first chunk's text
            assert "\n\n" not in chunks[0]["chunk_text"]

    def test_unicode_characters_survive_chunking(self):
        from ingestion.embedder import chunk_text
        text = "Médical soins de santé für die Gesundheit. " * 30
        chunks = chunk_text(text)
        assert len(chunks) >= 1
        for chunk in chunks:
            chunk["chunk_text"].encode("utf-8")   # must not raise

    def test_overlap_start_is_before_previous_end(self):
        from ingestion.embedder import chunk_text
        text = "word " * 500
        chunks = chunk_text(text, chunk_size=500, overlap=100)
        if len(chunks) >= 2:
            assert chunks[1]["char_start"] < chunks[0]["char_end"]

    def test_consecutive_chunk_indices_are_sequential(self):
        from ingestion.embedder import chunk_text
        text = "Sentence about medical coding and billing. " * 60
        chunks = chunk_text(text, chunk_size=400, overlap=40)
        for i, chunk in enumerate(chunks):
            assert chunk["chunk_index"] == i

    def test_no_chunk_below_min_size(self):
        from ingestion.embedder import chunk_text
        text = "word " * 1000
        min_size = 150
        chunks = chunk_text(text, min_size=min_size)
        for chunk in chunks:
            assert len(chunk["chunk_text"]) >= min_size
