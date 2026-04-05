"""
Tests for revenue_tracking.py.

Covers:
  - record_payment  — 80/20 split math; DB UPDATE called correctly
  - rollup_monthly  — aggregate query → upsert into revenue_shares
  - get_clinic_summary — unpacks the big SELECT into the expected dict shape
  - get_monthly_report — returns summary + line_items
"""

from datetime import date, datetime, timezone
from unittest.mock import MagicMock, call, patch

import pytest

from revenue_tracking import FEE_PERCENTAGE, RevenueTracker


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def tracker(mock_conn):
    return RevenueTracker(mock_conn)


# ---------------------------------------------------------------------------
# record_payment
# ---------------------------------------------------------------------------

class TestRecordPayment:
    def test_calculates_20_percent_fee(self, tracker, mock_cursor):
        mock_cursor.fetchone.return_value = ("clinic-1",)
        with patch.object(tracker, "rollup_monthly"):
            result = tracker.record_payment("d-1", 200.0)
        assert result["your_fee"] == 40.0

    def test_calculates_80_percent_clinic_net(self, tracker, mock_cursor):
        mock_cursor.fetchone.return_value = ("clinic-1",)
        with patch.object(tracker, "rollup_monthly"):
            result = tracker.record_payment("d-1", 200.0)
        assert result["clinic_net"] == 160.0

    def test_fee_plus_net_equals_paid_amount(self, tracker, mock_cursor):
        mock_cursor.fetchone.return_value = ("clinic-1",)
        with patch.object(tracker, "rollup_monthly"):
            result = tracker.record_payment("d-1", 333.33)
        assert round(result["your_fee"] + result["clinic_net"], 2) == result["paid_amount"]

    def test_returns_denial_id_in_result(self, tracker, mock_cursor):
        mock_cursor.fetchone.return_value = ("clinic-1",)
        with patch.object(tracker, "rollup_monthly"):
            result = tracker.record_payment("d-abc", 100.0)
        assert result["denial_id"] == "d-abc"

    def test_commits_after_update(self, tracker, mock_conn, mock_cursor):
        mock_cursor.fetchone.return_value = ("clinic-1",)
        with patch.object(tracker, "rollup_monthly"):
            tracker.record_payment("d-1", 100.0)
        mock_conn.commit.assert_called()

    def test_raises_when_denial_not_found(self, tracker, mock_cursor):
        mock_cursor.fetchone.return_value = None
        with pytest.raises(ValueError, match="not found"):
            tracker.record_payment("d-missing", 100.0)

    def test_triggers_monthly_rollup(self, tracker, mock_cursor):
        mock_cursor.fetchone.return_value = ("clinic-1",)
        with patch.object(tracker, "rollup_monthly") as mock_rollup:
            tracker.record_payment("d-1", 100.0)
        mock_rollup.assert_called_once()
        # First arg should be clinic_id
        assert mock_rollup.call_args[0][0] == "clinic-1"

    def test_update_sql_contains_status_paid(self, tracker, mock_cursor):
        mock_cursor.fetchone.return_value = ("clinic-1",)
        with patch.object(tracker, "rollup_monthly"):
            tracker.record_payment("d-1", 100.0)
        sql, params = mock_cursor.execute.call_args[0]
        assert "status" in sql.lower()
        assert "paid" in sql.lower()


# ---------------------------------------------------------------------------
# rollup_monthly
# ---------------------------------------------------------------------------

class TestRollupMonthly:
    def test_returns_correct_shape(self, tracker, mock_cursor):
        mock_cursor.fetchone.return_value = (500.0, 100.0, 400.0, 3)
        result = tracker.rollup_monthly("clinic-1", date(2024, 3, 1))

        assert result["clinic_id"] == "clinic-1"
        assert result["period_month"] == "2024-03-01"
        assert result["total_recovered"] == 500.0
        assert result["your_fee"] == 100.0
        assert result["clinic_payout"] == 400.0
        assert result["denial_count"] == 3

    def test_commits_after_upsert(self, tracker, mock_conn, mock_cursor):
        mock_cursor.fetchone.return_value = (0.0, 0.0, 0.0, 0)
        tracker.rollup_monthly("clinic-1", date(2024, 3, 1))
        mock_conn.commit.assert_called()

    def test_upsert_sql_called_with_clinic_id(self, tracker, mock_cursor):
        mock_cursor.fetchone.return_value = (0.0, 0.0, 0.0, 0)
        tracker.rollup_monthly("clinic-99", date(2024, 6, 1))
        # The second execute call is the upsert (first is the aggregate SELECT)
        all_calls = mock_cursor.execute.call_args_list
        assert len(all_calls) == 2
        upsert_sql, upsert_params = all_calls[1][0]
        assert "INSERT INTO revenue_shares" in upsert_sql
        assert upsert_params[0] == "clinic-99"


# ---------------------------------------------------------------------------
# get_clinic_summary
# ---------------------------------------------------------------------------

class TestGetClinicSummary:
    def _make_row(self):
        # Matches the column order in the SELECT
        return (
            10,         # total_denials
            5000.0,     # total_billed
            3000.0,     # total_potential
            5, 2000.0,  # co16_count, co16_value
            3, 500.0,   # co50_count, co50_value
            2, 500.0,   # co97_count, co97_value
            4,          # detected
            3,          # fixed
            2,          # submitted
            1,          # paid
            800.0,      # total_recovered
            160.0,      # fees_earned
            640.0,      # clinic_net
        )

    def test_returns_clinic_id(self, tracker, mock_cursor):
        mock_cursor.fetchone.return_value = self._make_row()
        result = tracker.get_clinic_summary("clinic-1")
        assert result["clinic_id"] == "clinic-1"

    def test_summary_has_expected_keys(self, tracker, mock_cursor):
        mock_cursor.fetchone.return_value = self._make_row()
        result = tracker.get_clinic_summary("clinic-1")
        expected_keys = {
            "total_denials", "total_billed", "total_potential",
            "total_recovered", "fees_earned", "clinic_net",
        }
        assert expected_keys.issubset(result["summary"].keys())

    def test_by_code_has_co16_co50_co97(self, tracker, mock_cursor):
        mock_cursor.fetchone.return_value = self._make_row()
        result = tracker.get_clinic_summary("clinic-1")
        assert "CO-16" in result["by_code"]
        assert "CO-50" in result["by_code"]
        assert "CO-97" in result["by_code"]

    def test_pipeline_has_status_counts(self, tracker, mock_cursor):
        mock_cursor.fetchone.return_value = self._make_row()
        result = tracker.get_clinic_summary("clinic-1")
        pipeline = result["pipeline"]
        assert pipeline["detected"]  == 4
        assert pipeline["fixed"]     == 3
        assert pipeline["submitted"] == 2
        assert pipeline["paid"]      == 1

    def test_numeric_fields_are_floats(self, tracker, mock_cursor):
        mock_cursor.fetchone.return_value = self._make_row()
        result = tracker.get_clinic_summary("clinic-1")
        assert isinstance(result["summary"]["total_recovered"], float)
        assert isinstance(result["summary"]["fees_earned"], float)


# ---------------------------------------------------------------------------
# get_monthly_report
# ---------------------------------------------------------------------------

class TestGetMonthlyReport:
    def test_returns_correct_period(self, tracker, mock_cursor):
        mock_cursor.fetchone.return_value = None  # no revenue_shares row
        mock_cursor.fetchall.return_value = []
        result = tracker.get_monthly_report("clinic-1", 2024, 3)
        assert result["period"] == "2024-03-01"

    def test_returns_empty_summary_when_no_share_row(self, tracker, mock_cursor):
        mock_cursor.fetchone.return_value = None
        mock_cursor.fetchall.return_value = []
        result = tracker.get_monthly_report("clinic-1", 2024, 3)
        assert result["summary"]["total_recovered"] == 0

    def test_returns_line_items(self, tracker, mock_cursor):
        line = {
            "webpt_claim_id": "wc-1", "denial_code": "CO-16",
            "billed_amount": 300.0, "paid_amount": 200.0,
            "your_fee": 40.0, "clinic_net": 160.0, "paid_at": None,
        }
        mock_cursor.fetchone.return_value = None
        mock_cursor.fetchall.return_value = [line]
        result = tracker.get_monthly_report("clinic-1", 2024, 3)
        assert len(result["line_items"]) == 1
        assert result["line_items"][0]["webpt_claim_id"] == "wc-1"

    def test_returns_clinic_id(self, tracker, mock_cursor):
        mock_cursor.fetchone.return_value = None
        mock_cursor.fetchall.return_value = []
        result = tracker.get_monthly_report("clinic-99", 2024, 5)
        assert result["clinic_id"] == "clinic-99"
