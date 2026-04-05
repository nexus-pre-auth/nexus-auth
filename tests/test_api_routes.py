"""
Tests for api_routes.py (recovery_bp Flask blueprint).

Uses a minimal Flask test app with only recovery_bp registered — no WebPTConnector
involved.  All DB access and business logic classes are mocked.

Covers:
  - _serialize_denial()   — pure JSON-serialization helper
  - detect_denials        POST /api/recovery/detect/<clinic_id>
  - process_denial        POST /api/recovery/process/<denial_id>
  - batch_process         POST /api/recovery/batch/<clinic_id>
  - recovery_stats        GET  /api/recovery/stats/<clinic_id>
  - monthly_report        GET  /api/recovery/report/<clinic_id>
  - record_payment        POST /api/recovery/payment/<denial_id>
  - list_denials          GET  /api/recovery/denials/<clinic_id>
"""

import json
from datetime import date, datetime
from unittest.mock import MagicMock, patch
import uuid

import pytest

from api_routes import _serialize_denial


# ---------------------------------------------------------------------------
# _serialize_denial (pure function — no Flask client needed)
# ---------------------------------------------------------------------------

class TestSerializeDenial:
    def test_passes_through_primitive_values(self):
        d = {"id": "d1", "status": "detected", "billed_amount": 500.0}
        result = _serialize_denial(d)
        assert result == d

    def test_converts_date_to_isoformat(self):
        d = {"detected_at": date(2024, 3, 15)}
        result = _serialize_denial(d)
        assert result["detected_at"] == "2024-03-15"

    def test_converts_datetime_to_isoformat(self):
        d = {"fixed_at": datetime(2024, 3, 15, 10, 30, 0)}
        result = _serialize_denial(d)
        assert result["fixed_at"] == "2024-03-15T10:30:00"

    def test_converts_uuid_to_string(self):
        uid = uuid.UUID("12345678-1234-5678-1234-567812345678")
        d = {"id": uid}
        result = _serialize_denial(d)
        assert result["id"] == "12345678-1234-5678-1234-567812345678"

    def test_preserves_none_values(self):
        d = {"fix_notes": None, "paid_amount": None}
        result = _serialize_denial(d)
        assert result["fix_notes"] is None

    def test_preserves_list_values(self):
        d = {"fixes_applied": [{"field": "billing_npi", "value": "123"}]}
        result = _serialize_denial(d)
        assert result["fixes_applied"] == d["fixes_applied"]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_engine():
    return MagicMock()


@pytest.fixture
def mock_tracker():
    return MagicMock()


# ---------------------------------------------------------------------------
# POST /api/recovery/detect/<clinic_id>
# ---------------------------------------------------------------------------

class TestDetectDenials:
    def test_returns_200_with_detected_count(self, recovery_client, mock_engine):
        mock_engine.detect_recoverable_denials.return_value = [
            {"id": "d1", "denial_code": "CO-16", "billed_amount": 500.0},
        ]
        with patch("api_routes._db", return_value=MagicMock()), \
             patch("api_routes.DenialRecoveryEngine", return_value=mock_engine):
            resp = recovery_client.post("/api/recovery/detect/clinic-1")

        assert resp.status_code == 200
        data = resp.get_json()
        assert data["clinic_id"] == "clinic-1"
        assert data["detected"] == 1

    def test_returns_denials_list(self, recovery_client, mock_engine):
        mock_engine.detect_recoverable_denials.return_value = [
            {"id": "d1", "denial_code": "CO-97", "billed_amount": 300.0},
            {"id": "d2", "denial_code": "CO-16", "billed_amount": 100.0},
        ]
        with patch("api_routes._db", return_value=MagicMock()), \
             patch("api_routes.DenialRecoveryEngine", return_value=mock_engine):
            resp = recovery_client.post("/api/recovery/detect/clinic-1")

        data = resp.get_json()
        assert len(data["denials"]) == 2


# ---------------------------------------------------------------------------
# POST /api/recovery/process/<denial_id>
# ---------------------------------------------------------------------------

class TestProcessDenial:
    def test_returns_200_with_fix_result(self, recovery_client, mock_engine):
        mock_engine.process_denial.return_value = {
            "denial_id": "d-1",
            "denial_code": "CO-16",
            "status": "fixed",
        }
        with patch("api_routes._db", return_value=MagicMock()), \
             patch("api_routes.DenialRecoveryEngine", return_value=mock_engine):
            resp = recovery_client.post("/api/recovery/process/d-1")

        assert resp.status_code == 200
        data = resp.get_json()
        assert data["status"] == "fixed"

    def test_calls_engine_with_denial_id(self, recovery_client, mock_engine):
        mock_engine.process_denial.return_value = {"status": "fixed", "denial_id": "d-99"}
        with patch("api_routes._db", return_value=MagicMock()), \
             patch("api_routes.DenialRecoveryEngine", return_value=mock_engine):
            recovery_client.post("/api/recovery/process/d-99")

        mock_engine.process_denial.assert_called_once_with("d-99")


# ---------------------------------------------------------------------------
# POST /api/recovery/batch/<clinic_id>
# ---------------------------------------------------------------------------

class TestBatchProcess:
    def test_returns_batch_summary(self, recovery_client, mock_engine):
        mock_engine.batch_process.return_value = {
            "clinic_id": "clinic-1",
            "total_processed": 3,
            "fixed": 2,
            "estimated_recovery": 850.0,
            "results": [],
        }
        with patch("api_routes._db", return_value=MagicMock()), \
             patch("api_routes.DenialRecoveryEngine", return_value=mock_engine):
            resp = recovery_client.post("/api/recovery/batch/clinic-1")

        assert resp.status_code == 200
        data = resp.get_json()
        assert data["fixed"] == 2


# ---------------------------------------------------------------------------
# GET /api/recovery/stats/<clinic_id>
# ---------------------------------------------------------------------------

class TestRecoveryStats:
    def test_returns_200_with_summary(self, recovery_client, mock_tracker):
        mock_tracker.get_clinic_summary.return_value = {
            "clinic_id": "clinic-1",
            "summary": {"total_denials": 5, "total_recovered": 1200.0},
            "by_code": {},
            "pipeline": {},
        }
        with patch("api_routes._db", return_value=MagicMock()), \
             patch("api_routes.RevenueTracker", return_value=mock_tracker):
            resp = recovery_client.get("/api/recovery/stats/clinic-1")

        assert resp.status_code == 200
        data = resp.get_json()
        assert data["summary"]["total_denials"] == 5


# ---------------------------------------------------------------------------
# GET /api/recovery/report/<clinic_id>
# ---------------------------------------------------------------------------

class TestMonthlyReport:
    def test_returns_200_with_report(self, recovery_client, mock_tracker):
        mock_tracker.get_monthly_report.return_value = {
            "clinic_id": "clinic-1",
            "period": "2024-03-01",
            "summary": {"total_recovered": 400.0},
            "line_items": [],
        }
        with patch("api_routes._db", return_value=MagicMock()), \
             patch("api_routes.RevenueTracker", return_value=mock_tracker):
            resp = recovery_client.get("/api/recovery/report/clinic-1?year=2024&month=3")

        assert resp.status_code == 200
        data = resp.get_json()
        assert data["period"] == "2024-03-01"

    def test_passes_year_and_month_to_tracker(self, recovery_client, mock_tracker):
        mock_tracker.get_monthly_report.return_value = {
            "clinic_id": "clinic-1", "period": "2024-06-01",
            "summary": {}, "line_items": [],
        }
        with patch("api_routes._db", return_value=MagicMock()), \
             patch("api_routes.RevenueTracker", return_value=mock_tracker):
            recovery_client.get("/api/recovery/report/clinic-1?year=2024&month=6")

        mock_tracker.get_monthly_report.assert_called_once_with("clinic-1", 2024, 6)

    def test_returns_400_for_non_integer_year(self, recovery_client):
        with patch("api_routes._db", return_value=MagicMock()):
            resp = recovery_client.get("/api/recovery/report/clinic-1?year=abc")
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# POST /api/recovery/payment/<denial_id>
# ---------------------------------------------------------------------------

class TestRecordPayment:
    def test_returns_200_with_split(self, recovery_client, mock_tracker):
        mock_tracker.record_payment.return_value = {
            "denial_id": "d-1",
            "paid_amount": 200.0,
            "your_fee": 40.0,
            "clinic_net": 160.0,
        }
        with patch("api_routes._db", return_value=MagicMock()), \
             patch("api_routes.RevenueTracker", return_value=mock_tracker):
            resp = recovery_client.post(
                "/api/recovery/payment/d-1",
                json={"paid_amount": 200.0},
            )

        assert resp.status_code == 200
        data = resp.get_json()
        assert data["your_fee"] == 40.0

    def test_returns_400_when_paid_amount_missing(self, recovery_client):
        with patch("api_routes._db", return_value=MagicMock()):
            resp = recovery_client.post("/api/recovery/payment/d-1", json={})
        assert resp.status_code == 400

    def test_returns_400_when_paid_amount_not_numeric(self, recovery_client):
        with patch("api_routes._db", return_value=MagicMock()):
            resp = recovery_client.post(
                "/api/recovery/payment/d-1",
                json={"paid_amount": "not-a-number"},
            )
        assert resp.status_code == 400

    def test_calls_tracker_with_float_amount(self, recovery_client, mock_tracker):
        mock_tracker.record_payment.return_value = {
            "denial_id": "d-1", "paid_amount": 150.0, "your_fee": 30.0, "clinic_net": 120.0,
        }
        with patch("api_routes._db", return_value=MagicMock()), \
             patch("api_routes.RevenueTracker", return_value=mock_tracker):
            recovery_client.post("/api/recovery/payment/d-1", json={"paid_amount": "150"})

        mock_tracker.record_payment.assert_called_once_with("d-1", 150.0)


# ---------------------------------------------------------------------------
# GET /api/recovery/denials/<clinic_id>
# ---------------------------------------------------------------------------

class TestListDenials:
    def _make_denial_row(self, status="detected"):
        return {
            "id": "d-1", "webpt_claim_id": "wc-1", "denial_code": "CO-16",
            "billed_amount": 500.0, "estimated_recovery": 425.0,
            "success_probability": 0.85, "status": status,
            "fixes_applied": None, "fix_notes": None,
            "detected_at": None, "fixed_at": None,
            "paid_amount": None, "your_fee": None,
            "clinic_net": None, "paid_at": None,
        }

    def test_returns_200_with_denials_list(self, recovery_client, mock_conn, mock_cursor):
        mock_cursor.fetchall.return_value = [self._make_denial_row()]
        mock_cursor.fetchone.return_value = {"count": 1}

        with patch("api_routes._db", return_value=mock_conn):
            resp = recovery_client.get("/api/recovery/denials/clinic-1")

        assert resp.status_code == 200
        data = resp.get_json()
        assert data["clinic_id"] == "clinic-1"
        assert isinstance(data["denials"], list)

    def test_returns_400_for_non_integer_limit(self, recovery_client):
        with patch("api_routes._db", return_value=MagicMock()):
            resp = recovery_client.get("/api/recovery/denials/clinic-1?limit=abc")
        assert resp.status_code == 400

    def test_default_limit_is_100(self, recovery_client, mock_conn, mock_cursor):
        mock_cursor.fetchall.return_value = []
        mock_cursor.fetchone.return_value = {"count": 0}

        with patch("api_routes._db", return_value=mock_conn):
            resp = recovery_client.get("/api/recovery/denials/clinic-1")

        data = resp.get_json()
        assert data["limit"] == 100

    def test_caps_limit_at_500(self, recovery_client, mock_conn, mock_cursor):
        mock_cursor.fetchall.return_value = []
        mock_cursor.fetchone.return_value = {"count": 0}

        with patch("api_routes._db", return_value=mock_conn):
            resp = recovery_client.get("/api/recovery/denials/clinic-1?limit=9999")

        data = resp.get_json()
        assert data["limit"] == 500
