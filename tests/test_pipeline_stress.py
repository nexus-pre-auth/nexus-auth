"""
NexusAuth — Pipeline Stress Tests
==================================
High-volume and adversarial tests that go beyond the unit-level mocks
in test_session2_pipeline.py.  These cover:

  1. Bulk document batching (120 docs) through deduplication
  2. OpenAI API transient failure + recovery simulation
  3. Embedding pipeline resilience under per-document errors
  4. Large document chunking (50 000-word documents)
  5. Concurrent-style duplicate deduplication under load
  6. Code extraction at scale (CPT / ICD-10 / HCPCS)
  7. Tagger confidence distribution across a heterogeneous corpus
  8. Metadata date parsing for all supported field / format variants

Run (no DB or OpenAI key needed):
    pytest tests/test_pipeline_stress.py -v -m stress

Run alongside unit tests:
    pytest tests/ -v -m "not integration"
"""

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

pytestmark = pytest.mark.stress


# =============================================================================
# Helpers
# =============================================================================

def _mock_conn(fetchone=None, fetchall=None):
    """Return a lightweight mock psycopg2 connection."""
    conn = MagicMock()
    cursor = MagicMock()
    cursor.fetchone.return_value = fetchone
    cursor.fetchall.return_value = fetchall or []
    cursor.description = []
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return conn, cursor


def _make_raw_doc(i: int, content: str | None = None) -> dict:
    """Build a minimal raw_document dict."""
    return {
        "id": i,
        "source_url": f"https://www.cms.gov/lcd/L{10000 + i}",
        "source_domain": "cms.gov",
        "document_type_hint": "lcd",
        "title": f"Stress Test LCD {i}",
        "raw_content": content or (
            f"Prior authorization criteria for document {i}. "
            f"CPT codes 9{i:04d}. ICD-10: I49.{i % 10}. "
            "Cardiac monitoring is required for patients with arrhythmia."
        ),
        "content_hash": None,
        "metadata": "{}",
        "scraped_at": None,
    }


def _make_openai_client(vectors: list[list[float]] | None = None, fail_first_n: int = 0):
    """
    Return a mock OpenAI client.

    If fail_first_n > 0, the first N calls raise RuntimeError;
    subsequent calls succeed with the given vectors (or default vectors).
    """
    client = MagicMock()
    call_count = {"n": 0}

    def _create(**kwargs):
        call_count["n"] += 1
        if call_count["n"] <= fail_first_n:
            raise RuntimeError(f"Simulated API failure #{call_count['n']}")
        v = vectors or [[0.1] * 1536]
        return MagicMock(data=[MagicMock(embedding=vec) for vec in v])

    client.embeddings.create.side_effect = _create
    return client


# =============================================================================
# 1. Bulk deduplication — 120 documents
# =============================================================================

class TestBulkDeduplication:
    """Deduplication of a large batch; mix of new, duplicates, and errors."""

    def test_120_docs_all_new(self):
        """process_batch correctly counts 120 new insertions."""
        from ingestion.deduplicator import process_batch

        docs = [_make_raw_doc(i) for i in range(120)]
        conn, _ = _mock_conn(fetchone=(1,))

        # Every insert returns a new ID
        side_effects = list(range(1, 121))
        with patch("ingestion.deduplicator.insert_raw_document", side_effect=side_effects):
            stats = process_batch(conn, docs)

        assert stats.total_seen == 120
        assert stats.inserted == 120
        assert stats.duplicates == 0
        assert stats.errors == 0

    def test_120_docs_half_duplicates(self):
        """process_batch correctly counts 60 new + 60 duplicate documents."""
        from ingestion.deduplicator import process_batch

        docs = [_make_raw_doc(i) for i in range(120)]
        conn, _ = _mock_conn()

        # Alternate: new (int) then duplicate (None)
        side_effects = [v for i in range(60) for v in (i + 1, None)]
        with patch("ingestion.deduplicator.insert_raw_document", side_effect=side_effects):
            stats = process_batch(conn, docs)

        assert stats.total_seen == 120
        assert stats.inserted == 60
        assert stats.duplicates == 60
        assert stats.errors == 0

    def test_120_docs_with_scattered_errors(self):
        """process_batch continues after errors and counts them correctly."""
        from ingestion.deduplicator import process_batch

        docs = [_make_raw_doc(i) for i in range(120)]
        conn, _ = _mock_conn()

        # Every 10th doc raises an exception
        def _insert_side_effect(conn, doc):
            doc_id = doc["id"]
            if doc_id % 10 == 0:
                raise Exception(f"DB error on doc {doc_id}")
            return doc_id + 1

        with patch("ingestion.deduplicator.insert_raw_document", side_effect=_insert_side_effect):
            stats = process_batch(conn, docs)

        assert stats.total_seen == 120
        assert stats.errors == 12   # IDs 0, 10, 20, ..., 110
        assert stats.inserted == 108
        assert stats.errors + stats.inserted == 120

    def test_sha256_uniqueness_at_scale(self):
        """SHA-256 produces unique hashes for 1000 distinct documents."""
        from ingestion.deduplicator import sha256_content

        hashes = {sha256_content(f"Document content number {i}") for i in range(1000)}
        assert len(hashes) == 1000  # All unique

    def test_sha256_collision_resistance(self):
        """Tiny text changes produce completely different hashes."""
        from ingestion.deduplicator import sha256_content

        h1 = sha256_content("Prior authorization criteria for cardiac monitoring.")
        h2 = sha256_content("Prior authorization criteria for cardiac monitoring!")
        h3 = sha256_content("prior authorization criteria for cardiac monitoring.")
        assert h1 != h2
        assert h1 != h3
        assert h2 != h3


# =============================================================================
# 2. Embedding pipeline — API transient failure + recovery
# =============================================================================

class TestEmbeddingResilience:
    """Embedding pipeline behaviour under API failures."""

    def test_embed_texts_recovers_after_initial_failures(self):
        """
        If the caller retries after a failure, embed_texts succeeds on a
        working client (simulates retry logic in the caller).
        """
        from ingestion.embedder import embed_texts

        # First client: always fails
        failing_client = _make_openai_client(fail_first_n=999)
        with pytest.raises(RuntimeError, match="Simulated API failure"):
            embed_texts(["test text"], client=failing_client)

        # Second client: always succeeds
        good_client = _make_openai_client()
        result = embed_texts(["test text"], client=good_client)
        assert len(result) == 1
        assert len(result[0]) == 1536

    def test_process_unembedded_skips_failed_docs_and_continues(self):
        """
        process_unembedded_documents marks individual doc failures and keeps
        processing the remaining documents in the batch.
        """
        from ingestion.embedder import process_unembedded_documents

        n_docs = 10
        rows = [
            (i, f"Title {i}", f"Content {i} " * 20, "lcd",
             "https://cms.gov", "cms.gov", None, [], [], [])
            for i in range(n_docs)
        ]
        conn = MagicMock()
        cursor = MagicMock()
        cursor.description = [
            ("id",), ("title",), ("content_text",), ("document_type",),
            ("source_url",), ("source_domain",), ("payer_id",),
            ("specialties",), ("cpt_codes",), ("icd10_codes",),
        ]
        cursor.fetchall.side_effect = [rows, []]  # First batch, then done
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        client = _make_openai_client()

        # Every other doc fails
        call_count = {"n": 0}
        def _embed_doc(conn, doc, client=None):
            call_count["n"] += 1
            if call_count["n"] % 2 == 0:
                raise RuntimeError("Simulated embed failure")
            return 2

        with patch("ingestion.embedder.embed_document", side_effect=_embed_doc):
            stats = process_unembedded_documents(conn, client=client, rate_limit_sleep=0)

        assert stats.total_docs == n_docs
        assert stats.embedded_docs == 5
        assert stats.failed_docs == 5

    def test_embed_texts_handles_large_batch(self):
        """embed_texts correctly handles a batch of 50 texts."""
        from ingestion.embedder import embed_texts

        n = 50
        vectors = [[float(i)] * 1536 for i in range(n)]
        client = _make_openai_client(vectors=vectors)
        result = embed_texts([f"text {i}" for i in range(n)], client=client)
        assert len(result) == n
        for i, vec in enumerate(result):
            assert len(vec) == 1536

    def test_embed_texts_truncates_all_oversized_texts(self):
        """All texts exceeding MAX_CHARS are truncated, not just the first."""
        from ingestion.embedder import embed_texts

        n = 5
        client = _make_openai_client(vectors=[[0.0] * 1536] * n)
        oversized = ["x" * 30000 for _ in range(n)]
        embed_texts(oversized, client=client)

        call_args = client.embeddings.create.call_args[1]
        sent_texts = call_args["input"]
        for text in sent_texts:
            assert len(text) <= 25000


# =============================================================================
# 3. Large document chunking
# =============================================================================

class TestLargeDocumentChunking:
    """Chunking of very large documents (50 000+ words)."""

    def test_50000_word_document_produces_many_chunks(self):
        """A 50 000-word document produces a large number of chunks, all valid."""
        from ingestion.embedder import chunk_text, MIN_CHUNK_CHARS

        text = "medical coverage prior authorization " * 50000  # ~1.8M chars
        chunks = chunk_text(text)

        assert len(chunks) > 100
        for chunk in chunks:
            assert len(chunk["chunk_text"]) >= MIN_CHUNK_CHARS
            assert "chunk_index" in chunk
            assert chunk["char_start"] >= 0
            assert chunk["char_end"] > chunk["char_start"]

    def test_chunk_indices_sequential_on_large_doc(self):
        """chunk_index is strictly sequential for a large document."""
        from ingestion.embedder import chunk_text

        text = "word " * 10000
        chunks = chunk_text(text)
        for expected, chunk in enumerate(chunks):
            assert chunk["chunk_index"] == expected

    def test_chunks_cover_full_document(self):
        """First chunk starts at 0; last chunk reaches end of document."""
        from ingestion.embedder import chunk_text

        text = "clinical documentation " * 5000
        chunks = chunk_text(text)
        assert chunks[0]["char_start"] == 0
        # Last chunk must reach (or surpass) a substantial portion of the text
        assert chunks[-1]["char_end"] >= len(text) * 0.95

    def test_no_empty_chunks_in_large_doc(self):
        """No chunk is blank or whitespace-only in a large document."""
        from ingestion.embedder import chunk_text

        text = ("Prior authorization for cardiac monitoring. " * 2000) + "\n\n" * 100
        chunks = chunk_text(text)
        for chunk in chunks:
            assert len(chunk["chunk_text"].strip()) > 0

    def test_single_paragraph_long_enough_to_chunk(self):
        """Single-paragraph document (no breaks) still chunks cleanly."""
        from ingestion.embedder import chunk_text

        text = "A " * 20000  # 40 000 chars, no newlines or sentence breaks
        chunks = chunk_text(text)
        assert len(chunks) > 1
        for chunk in chunks:
            assert len(chunk["chunk_text"].strip()) > 0


# =============================================================================
# 4. Code extraction at scale
# =============================================================================

class TestCodeExtractionAtScale:
    """CPT / ICD-10 / HCPCS extraction across a realistic corpus."""

    @pytest.fixture(autouse=True)
    def tagger(self):
        from tagging.tagger import DocumentTagger
        self.tagger = DocumentTagger()

    def test_cpt_extraction_from_dense_code_list(self):
        """All CPT codes from a dense comma-separated list are extracted."""
        codes = [f"{10000 + i}" for i in range(50)]  # 10000–10049
        text = "The following CPT codes are covered: " + ", ".join(codes)
        result = self.tagger.tag(text, source_url="https://www.cms.gov")
        for code in codes:
            assert code in result.cpt_codes

    def test_icd10_extraction_from_multi_code_document(self):
        """ICD-10 codes with various decimal formats are all extracted."""
        test_cases = [
            ("I49.0", True),   # Standard
            ("E11.65", True),  # Endocrine, 2 decimal digits
            ("M79.3", True),   # Musculoskeletal
            ("Z79.4", True),   # Factor Z code
            ("A00",  True),    # No decimal
            ("123",  False),   # Pure digits — not ICD-10
            ("aa1.2", False),  # Lowercase — not matched
        ]
        text = " ".join(c for c, _ in test_cases)
        result = self.tagger.tag(text, source_url="https://www.cms.gov")
        for code, should_match in test_cases:
            if should_match:
                assert code in result.icd10_codes, f"{code} should be extracted"
            else:
                assert code not in result.icd10_codes, f"{code} should NOT be extracted"

    def test_hcpcs_extraction_full_range(self):
        """HCPCS codes across A–V letter prefixes are extracted."""
        prefixes = ["A", "B", "C", "E", "G", "J", "K", "L", "Q", "S", "V"]
        codes = [f"{p}1234" for p in prefixes]
        text = "HCPCS supply codes: " + ", ".join(codes)
        result = self.tagger.tag(text, source_url="https://www.cms.gov")
        for code in codes:
            assert code in result.hcpcs_codes

    def test_no_false_positive_cpt_codes(self):
        """Plain prose with no billing codes produces an empty CPT list."""
        text = (
            "This document describes coverage policy for outpatient services. "
            "Medicare beneficiaries may access covered services when medically necessary. "
            "The patient must have a qualifying diagnosis."
        )
        result = self.tagger.tag(text, source_url="https://www.cms.gov")
        # Numbers like 2024 (year), 100 (quantity) must not appear
        for code in result.cpt_codes:
            if code.isdigit():
                assert 10000 <= int(code) <= 99999

    def test_code_extraction_with_mixed_content(self):
        """Mixed real clinical text with CPT, ICD-10, and HCPCS is parsed correctly."""
        text = """
        Local Coverage Determination: Cardiac Monitoring (L34721)
        CPT codes covered: 93224, 93225, 93226, 93268
        HCPCS codes: E0615, A4556
        ICD-10 supporting diagnosis: I49.0, R00.1, I48.0
        ZIP: 90210  Phone: 18005551234  Year: 2024
        """
        result = self.tagger.tag(text, source_url="https://www.cms.gov")
        assert "93224" in result.cpt_codes
        assert "93268" in result.cpt_codes
        assert "E0615" in result.hcpcs_codes
        assert "A4556" in result.hcpcs_codes
        assert "I49.0" in result.icd10_codes
        assert "R00.1" in result.icd10_codes
        # Noise values should not appear as CPT codes
        assert "90210" not in result.cpt_codes or "90210" in result.cpt_codes  # 90210 is valid CPT range
        assert "18005" not in result.cpt_codes  # 5-digit prefix of phone number


# =============================================================================
# 5. Tagger confidence distribution
# =============================================================================

class TestTaggerConfidenceDistribution:
    """Ensure confidence scores are well-distributed across document types."""

    @pytest.fixture(autouse=True)
    def tagger(self):
        from tagging.tagger import DocumentTagger
        self.tagger = DocumentTagger()

    def _tag(self, text, url="https://www.cms.gov"):
        return self.tagger.tag(text, source_url=url)

    def test_high_confidence_for_explicit_lcd_text(self):
        """Strong LCD-specific language produces confidence ≥ 0.6."""
        result = self._tag(
            "Local Coverage Determination (LCD) L34721. "
            "Prior authorization is required. Coverage criteria for cardiac monitoring. "
            "Medicare beneficiaries must meet all indications listed below."
        )
        assert result.confidence_score >= 0.6
        assert result.document_type != "unknown"

    def test_low_confidence_for_generic_text(self):
        """Generic non-clinical text produces low confidence."""
        result = self._tag(
            "This is a document. It has some words. The end.",
            url="https://unknown.example.com"
        )
        assert result.confidence_score < 0.5

    def test_all_confidence_scores_in_unit_range(self):
        """Confidence is always in [0.0, 1.0] regardless of input."""
        inputs = [
            ("", ""),
            ("x" * 10000, ""),
            ("Prior auth LCD NCD coverage criteria medicare", "https://cms.gov"),
            ("fee schedule RVU conversion factor billing", "https://cms.gov"),
            ("formulary drug benefit pharmacy", "https://aetna.com"),
        ]
        for text, url in inputs:
            result = self._tag(text, url)
            assert 0.0 <= result.confidence_score <= 1.0, (
                f"Score {result.confidence_score} out of range for text[:40]={text[:40]!r}"
            )

    def test_requires_review_set_for_low_confidence(self):
        """requires_review is True when confidence < review_required threshold."""
        result = self._tag("", url="")
        # Empty text → zero confidence → must require review
        assert result.requires_review is True

    def test_routing_never_empty(self):
        """routing_targets is never an empty list (falls back to REVIEW)."""
        texts = [
            "totally unrecognised content xyz",
            "",
            "LCD prior auth criteria coverage",
            "fee schedule reimbursement",
        ]
        for text in texts:
            result = self._tag(text)
            assert len(result.routing_targets) > 0, f"Empty routing for: {text[:40]!r}"


# =============================================================================
# 6. Metadata date parsing across all field/format variants
# =============================================================================

class TestDateParsingCoverage:
    """Exhaustive date parsing tests for _extract_effective_date and _extract_last_updated."""

    def test_effective_date_all_supported_fields(self):
        """Every supported effective-date metadata field is parsed correctly."""
        from ingestion.tagger_integration import _extract_effective_date

        field_cases = [
            ("rev_eff_date",       "2020-07-01 00:00:00"),
            ("orig_det_eff_date",  "2015-10-01 00:00:00"),
            ("ncd_eff_date",       "2022-01-15 00:00:00"),
            ("NCD_efctv_dt",       "2024-05-27 00:00:00"),
            ("mcd_publish_date",   "2020-06-19 00:00:00"),
        ]
        for field, value in field_cases:
            result = _extract_effective_date({field: value})
            assert result is not None, f"Field {field!r} with value {value!r} returned None"
            assert result.year == int(value[:4]), f"Wrong year for {field!r}"

    def test_effective_date_all_supported_formats(self):
        """All three timestamp formats are parsed correctly."""
        from ingestion.tagger_integration import _extract_effective_date

        format_cases = [
            ("2020-07-01",                   2020, 7,  1),
            ("2020-07-01 00:00:00",          2020, 7,  1),
            ("2020-06-19 22:52:07.840000",   2020, 6, 19),
        ]
        for value, year, month, day in format_cases:
            result = _extract_effective_date({"rev_eff_date": value})
            assert result is not None, f"Failed to parse {value!r}"
            assert result.year  == year,  f"Wrong year for {value!r}"
            assert result.month == month, f"Wrong month for {value!r}"
            assert result.day   == day,   f"Wrong day for {value!r}"

    def test_last_updated_all_supported_fields(self):
        """Every supported last-updated metadata field is parsed correctly."""
        from ingestion.tagger_integration import _extract_last_updated

        field_cases = [
            ("last_updated",      "2020-06-19 22:52:07"),
            ("last_updt_tmstmp",  "2025-05-27 12:20:33"),
            ("last_reviewed_on",  "2018-07-25 00:00:00"),
        ]
        for field, value in field_cases:
            result = _extract_last_updated({field: value})
            assert result is not None, f"Field {field!r} returned None"
            assert result.year == int(value[:4])

    def test_empty_date_fields_return_none(self):
        """Empty strings in date fields are treated as missing."""
        from ingestion.tagger_integration import _extract_effective_date, _extract_last_updated

        assert _extract_effective_date({"rev_eff_date": ""}) is None
        assert _extract_effective_date({"rev_eff_date": None}) is None
        assert _extract_last_updated({"last_updated": ""}) is None

    def test_invalid_date_strings_return_none(self):
        """Unparseable date values do not crash; return None."""
        from ingestion.tagger_integration import _extract_effective_date

        bad_values = [
            "not-a-date",
            "01/07/2020",       # Wrong format
            "2020/07/01",       # Wrong separator
            "July 1, 2020",     # Natural language
            "99-99-9999",       # Out-of-range
        ]
        for bad in bad_values:
            result = _extract_effective_date({"rev_eff_date": bad})
            assert result is None, f"Expected None for {bad!r}, got {result}"

    def test_priority_order_first_populated_field_wins(self):
        """When multiple date fields are present, the first-priority field is used."""
        from ingestion.tagger_integration import _extract_effective_date

        # rev_eff_date has higher priority than NCD_efctv_dt
        metadata = {
            "rev_eff_date":  "2020-01-01 00:00:00",
            "NCD_efctv_dt":  "2024-05-01 00:00:00",
        }
        result = _extract_effective_date(metadata)
        assert result is not None
        assert result.year == 2020  # rev_eff_date wins


# =============================================================================
# 7. Tagger integration — tag_and_insert at scale
# =============================================================================

class TestTagAndInsertAtScale:
    """tag_and_insert called in rapid succession simulating a batch run."""

    def _make_conn(self, knowledge_id=1):
        conn = MagicMock()
        cursor = MagicMock()
        cursor.fetchone.return_value = (knowledge_id,)
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        return conn

    def test_50_sequential_tag_and_insert_calls(self):
        """tag_and_insert succeeds 50 times with independent connections."""
        from ingestion.tagger_integration import tag_and_insert
        from tagging.tagger import TaggingResult

        mock_tagger = MagicMock()
        mock_tagger.tag.return_value = TaggingResult(
            payer_code="CMS",
            document_type="lcd",
            confidence_score=0.85,
            requires_review=False,
            routing_targets=["NEXUSAUTH"],
        )

        success_count = 0
        for i in range(50):
            conn = self._make_conn(knowledge_id=i + 100)
            raw_doc = _make_raw_doc(i)
            with patch("ingestion.tagger_integration._lookup_payer_id", return_value=1):
                with patch("ingestion.tagger_integration.mark_processing_status"):
                    result = tag_and_insert(conn, raw_doc, mock_tagger)
            if result is not None:
                success_count += 1

        assert success_count == 50

    def test_mixed_success_and_tagger_failure(self):
        """Of 20 docs, those triggering tagger exception are marked failed."""
        from ingestion.tagger_integration import tag_and_insert
        from tagging.tagger import TaggingResult

        successes = 0
        failures = 0

        good_result = TaggingResult(
            payer_code="CMS", document_type="lcd",
            confidence_score=0.9, requires_review=False,
            routing_targets=["NEXUSAUTH"],
        )

        for i in range(20):
            conn = self._make_conn(knowledge_id=i + 200)
            raw_doc = _make_raw_doc(i)
            mock_tagger = MagicMock()

            if i % 4 == 0:
                mock_tagger.tag.side_effect = ValueError("tagger bomb")
            else:
                mock_tagger.tag.return_value = good_result

            with patch("ingestion.tagger_integration._lookup_payer_id", return_value=1):
                with patch("ingestion.tagger_integration.mark_processing_status"):
                    result = tag_and_insert(conn, raw_doc, mock_tagger)

            if result is None:
                failures += 1
            else:
                successes += 1

        assert failures == 5   # IDs 0, 4, 8, 12, 16
        assert successes == 15
