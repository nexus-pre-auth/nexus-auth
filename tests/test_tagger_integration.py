"""
Tests for ingestion/tagger_integration.py.

Covers:
  Pure functions (no DB/tagger mock needed):
    - _extract_effective_date — date parsing from metadata dict
    - _extract_last_updated   — date parsing from metadata dict
    - TaggingStats            — counters, elapsed property, __repr__

  DB/tagger-dependent:
    - _lookup_payer_id        — queries payers table by payer_code
    - tag_and_insert          — orchestrates tagger + DB insert
    - process_pending_documents — batch loop with stats
"""

import json
import time
from datetime import datetime
from unittest.mock import MagicMock, call, patch

import pytest

from ingestion.tagger_integration import (
    TaggingStats,
    _extract_effective_date,
    _extract_last_updated,
    _lookup_payer_id,
    tag_and_insert,
    process_pending_documents,
)


# ---------------------------------------------------------------------------
# _extract_effective_date
# ---------------------------------------------------------------------------

class TestExtractEffectiveDate:
    def test_parses_rev_eff_date(self):
        meta = {"rev_eff_date": "2020-07-01 00:00:00"}
        dt = _extract_effective_date(meta)
        assert dt is not None
        assert dt.year == 2020
        assert dt.month == 7

    def test_parses_ncd_efctv_dt(self):
        meta = {"NCD_efctv_dt": "2024-05-27 00:00:00"}
        dt = _extract_effective_date(meta)
        assert dt is not None
        assert dt.year == 2024

    def test_parses_orig_det_eff_date(self):
        meta = {"orig_det_eff_date": "2015-10-01 00:00:00"}
        dt = _extract_effective_date(meta)
        assert dt is not None
        assert dt.year == 2015

    def test_returns_none_for_empty_metadata(self):
        assert _extract_effective_date({}) is None
        assert _extract_effective_date(None) is None

    def test_returns_none_when_no_matching_field(self):
        meta = {"other_field": "2020-01-01 00:00:00"}
        assert _extract_effective_date(meta) is None

    def test_returns_none_for_empty_date_string(self):
        meta = {"rev_eff_date": ""}
        assert _extract_effective_date(meta) is None

    def test_parses_microsecond_format(self):
        meta = {"rev_eff_date": "2020-06-19 22:52:07.840000000"}
        dt = _extract_effective_date(meta)
        assert dt is not None
        assert dt.year == 2020

    def test_parses_date_only_format(self):
        meta = {"mcd_publish_date": "2020-06-19 00:00:00"}
        dt = _extract_effective_date(meta)
        assert dt is not None


# ---------------------------------------------------------------------------
# _extract_last_updated
# ---------------------------------------------------------------------------

class TestExtractLastUpdated:
    def test_parses_last_updated(self):
        meta = {"last_updated": "2020-06-19 22:52:07.840000000"}
        dt = _extract_last_updated(meta)
        assert dt is not None
        assert dt.year == 2020

    def test_parses_last_updt_tmstmp(self):
        meta = {"last_updt_tmstmp": "2025-05-27 12:20:33"}
        dt = _extract_last_updated(meta)
        assert dt is not None
        assert dt.year == 2025

    def test_parses_last_reviewed_on(self):
        meta = {"last_reviewed_on": "2018-07-25 00:00:00"}
        dt = _extract_last_updated(meta)
        assert dt is not None
        assert dt.month == 7

    def test_returns_none_for_empty_metadata(self):
        assert _extract_last_updated({}) is None
        assert _extract_last_updated(None) is None


# ---------------------------------------------------------------------------
# TaggingStats
# ---------------------------------------------------------------------------

class TestTaggingStats:
    def test_initial_counters_are_zero(self):
        stats = TaggingStats()
        assert stats.total == 0
        assert stats.tagged == 0
        assert stats.failed == 0
        assert stats.needs_review == 0

    def test_elapsed_is_non_negative(self):
        stats = TaggingStats()
        assert stats.elapsed >= 0.0

    def test_elapsed_increases_over_time(self):
        stats = TaggingStats()
        t1 = stats.elapsed
        time.sleep(0.05)
        t2 = stats.elapsed
        assert t2 > t1

    def test_repr_contains_counts(self):
        stats = TaggingStats()
        stats.total = 10
        stats.tagged = 8
        stats.failed = 2
        r = repr(stats)
        assert "total=10" in r
        assert "tagged=8" in r
        assert "failed=2" in r

    def test_repr_contains_elapsed(self):
        stats = TaggingStats()
        r = repr(stats)
        assert "elapsed=" in r

    def test_counters_can_be_incremented(self):
        stats = TaggingStats()
        stats.total += 5
        stats.tagged += 4
        stats.failed += 1
        assert stats.total == 5
        assert stats.tagged == 4
        assert stats.failed == 1


# ---------------------------------------------------------------------------
# _lookup_payer_id
# ---------------------------------------------------------------------------

class TestLookupPayerId:
    def test_returns_none_for_none_payer_code(self, mock_conn):
        result = _lookup_payer_id(mock_conn, None)
        assert result is None

    def test_returns_none_for_empty_payer_code(self, mock_conn):
        result = _lookup_payer_id(mock_conn, "")
        assert result is None

    def test_returns_id_when_found(self, mock_conn, mock_cursor):
        mock_cursor.fetchone.return_value = (42,)
        result = _lookup_payer_id(mock_conn, "CMS")
        assert result == 42

    def test_returns_none_when_not_found(self, mock_conn, mock_cursor):
        mock_cursor.fetchone.return_value = None
        result = _lookup_payer_id(mock_conn, "UNKNOWN")
        assert result is None

    def test_queries_by_payer_code(self, mock_conn, mock_cursor):
        mock_cursor.fetchone.return_value = None
        _lookup_payer_id(mock_conn, "AETNA")
        sql, params = mock_cursor.execute.call_args[0]
        assert "payer_code" in sql
        assert params == ("AETNA",)


# ---------------------------------------------------------------------------
# tag_and_insert
# ---------------------------------------------------------------------------

class TestTagAndInsert:
    def _make_raw_doc(self, **overrides):
        doc = {
            "id": 1,
            "title": "Test LCD",
            "raw_content": "Prior auth required for CPT 97110.",
            "source_url": "https://www.cms.gov/lcd/test",
            "source_domain": "cms.gov",
            "metadata": {},
        }
        doc.update(overrides)
        return doc

    def _make_tagging_result(self, **overrides):
        result = MagicMock()
        result.payer_code = "CMS"
        result.document_type = "lcd"
        result.document_subtype = "coverage"
        result.specialties = ["physical_therapy"]
        result.cpt_codes = ["97110"]
        result.icd10_codes = ["M54.5"]
        result.hcpcs_codes = []
        result.routing_targets = ["prior_auth"]
        result.confidence_score = 0.85
        result.requires_review = False
        result.raw_scores = {}
        for k, v in overrides.items():
            setattr(result, k, v)
        return result

    def test_returns_knowledge_doc_id_on_success(self, mock_conn, mock_cursor):
        mock_cursor.fetchone.side_effect = [
            None,    # _lookup_payer_id → not found
            (99,),   # INSERT RETURNING id
        ]

        tagger = MagicMock()
        tagger.tag.return_value = self._make_tagging_result()

        with patch("ingestion.tagger_integration.mark_processing_status"):
            result = tag_and_insert(mock_conn, self._make_raw_doc(), tagger)

        assert result == 99

    def test_calls_tagger_with_content_and_url(self, mock_conn, mock_cursor):
        mock_cursor.fetchone.side_effect = [None, (1,)]
        tagger = MagicMock()
        tagger.tag.return_value = self._make_tagging_result()

        with patch("ingestion.tagger_integration.mark_processing_status"):
            tag_and_insert(
                mock_conn,
                self._make_raw_doc(
                    raw_content="CPT 97110 prior auth",
                    source_url="https://cms.gov/lcd/1",
                ),
                tagger,
            )

        tagger.tag.assert_called_once_with(
            "CPT 97110 prior auth", source_url="https://cms.gov/lcd/1"
        )

    def test_marks_status_tagged_on_success(self, mock_conn, mock_cursor):
        mock_cursor.fetchone.side_effect = [None, (1,)]
        tagger = MagicMock()
        tagger.tag.return_value = self._make_tagging_result()

        with patch("ingestion.tagger_integration.mark_processing_status") as mock_mark:
            tag_and_insert(mock_conn, self._make_raw_doc(), tagger)

        # Should be called with "tagged" at some point
        statuses = [c[0][2] for c in mock_mark.call_args_list]
        assert "tagged" in statuses

    def test_returns_none_on_tagger_error(self, mock_conn):
        tagger = MagicMock()
        tagger.tag.side_effect = RuntimeError("tagger crashed")

        with patch("ingestion.tagger_integration.mark_processing_status") as mock_mark:
            result = tag_and_insert(mock_conn, self._make_raw_doc(), tagger)

        assert result is None
        statuses = [c[0][2] for c in mock_mark.call_args_list]
        assert "failed" in statuses

    def test_parses_metadata_json_string(self, mock_conn, mock_cursor):
        mock_cursor.fetchone.side_effect = [None, (1,)]
        tagger = MagicMock()
        tagger.tag.return_value = self._make_tagging_result()

        metadata_str = json.dumps({"rev_eff_date": "2020-07-01 00:00:00"})
        with patch("ingestion.tagger_integration.mark_processing_status"):
            result = tag_and_insert(
                mock_conn, self._make_raw_doc(metadata=metadata_str), tagger
            )
        # Should succeed (metadata parsed without error)
        assert result is not None

    def test_returns_none_on_db_error(self, mock_conn, mock_cursor):
        tagger = MagicMock()
        tagger.tag.return_value = self._make_tagging_result()
        mock_cursor.fetchone.side_effect = [None, Exception("DB error")]

        with patch("ingestion.tagger_integration.mark_processing_status"):
            result = tag_and_insert(mock_conn, self._make_raw_doc(), tagger)

        assert result is None
        mock_conn.rollback.assert_called()


# ---------------------------------------------------------------------------
# process_pending_documents
# ---------------------------------------------------------------------------

class TestProcessPendingDocuments:
    def _make_raw_doc(self, doc_id: int):
        return {
            "id": doc_id,
            "title": f"Doc {doc_id}",
            "raw_content": "Prior auth for CPT 97110.",
            "source_url": "https://cms.gov",
            "source_domain": "cms.gov",
            "metadata": {},
        }

    def test_returns_tagging_stats(self, mock_conn):
        tagger = MagicMock()
        with patch("ingestion.tagger_integration.get_pending_documents", return_value=[]), \
             patch("ingestion.tagger_integration.mark_processing_status"):
            result = process_pending_documents(mock_conn, tagger=tagger)
        assert isinstance(result, TaggingStats)

    def test_increments_total_for_each_doc(self, mock_conn):
        docs = [self._make_raw_doc(i) for i in range(3)]
        tagger = MagicMock()

        with patch("ingestion.tagger_integration.get_pending_documents",
                   side_effect=[docs, []]), \
             patch("ingestion.tagger_integration.tag_and_insert", return_value=1), \
             patch("ingestion.tagger_integration.mark_processing_status"):
            stats = process_pending_documents(mock_conn, tagger=tagger)

        assert stats.total == 3

    def test_increments_tagged_on_success(self, mock_conn):
        docs = [self._make_raw_doc(1)]
        tagger = MagicMock()

        with patch("ingestion.tagger_integration.get_pending_documents",
                   side_effect=[docs, []]), \
             patch("ingestion.tagger_integration.tag_and_insert", return_value=42), \
             patch("ingestion.tagger_integration.mark_processing_status"):
            stats = process_pending_documents(mock_conn, tagger=tagger)

        assert stats.tagged == 1

    def test_increments_failed_when_tag_and_insert_returns_none(self, mock_conn):
        docs = [self._make_raw_doc(1)]
        tagger = MagicMock()

        with patch("ingestion.tagger_integration.get_pending_documents",
                   side_effect=[docs, []]), \
             patch("ingestion.tagger_integration.tag_and_insert", return_value=None), \
             patch("ingestion.tagger_integration.mark_processing_status"):
            stats = process_pending_documents(mock_conn, tagger=tagger)

        assert stats.failed == 1

    def test_stops_at_max_documents(self, mock_conn):
        docs = [self._make_raw_doc(i) for i in range(10)]
        tagger = MagicMock()

        with patch("ingestion.tagger_integration.get_pending_documents",
                   return_value=docs), \
             patch("ingestion.tagger_integration.tag_and_insert", return_value=1), \
             patch("ingestion.tagger_integration.mark_processing_status"):
            stats = process_pending_documents(mock_conn, tagger=tagger, max_documents=3)

        assert stats.total == 3

    def test_creates_default_tagger_when_none_passed(self, mock_conn):
        with patch("ingestion.tagger_integration.get_pending_documents", return_value=[]), \
             patch("ingestion.tagger_integration.DocumentTagger") as MockTagger:
            process_pending_documents(mock_conn, tagger=None)
        MockTagger.assert_called_once()
