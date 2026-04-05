"""
Tests for app.py (main Flask application).

The module-level `connector = WebPTConnector()` is initialised when `app`
is imported; since conftest.py sets DATABASE_URL before import, this is safe.
Individual tests patch `app.connector` methods to avoid real DB/network calls.

Covers:
  - _verify_webhook_signature — HMAC-SHA256 correctness
  - GET  /auth/webpt          — missing clinic_id → 400
  - GET  /auth/webpt/callback — CSRF mismatch → 403; success → 200
  - POST /webhooks/webpt      — bad signature → 401; good event → 200
  - GET  /api/webpt/status    — connector forwarded; 404 on missing connection
  - POST /api/webpt/disconnect — always 200
  - POST /internal/webpt/sync-complete — auth check; missing connection_id → 400
  - GET  /health              — always 200
"""

import hashlib
import hmac
import json
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Import the Flask app (DATABASE_URL etc. set by conftest.py)
# ---------------------------------------------------------------------------

import app as flask_app

CLIENT = flask_app.app.test_client()
flask_app.app.config["TESTING"] = True
# Use a fixed secret key for session stability in tests
flask_app.app.secret_key = "test-secret-key-for-testing-only"


# ---------------------------------------------------------------------------
# Helper: compute the HMAC-SHA256 signature WebPT would send
# ---------------------------------------------------------------------------

WEBHOOK_SECRET = "test-webhook-secret"   # matches conftest.py env var


def _make_signature(payload: bytes) -> str:
    expected = hmac.new(
        WEBHOOK_SECRET.encode(), payload, hashlib.sha256
    ).hexdigest()
    return f"sha256={expected}"


# ---------------------------------------------------------------------------
# _verify_webhook_signature (pure function)
# ---------------------------------------------------------------------------

class TestVerifyWebhookSignature:
    def test_valid_signature_returns_true(self):
        payload = b'{"event": "claim.created"}'
        sig = _make_signature(payload)
        assert flask_app._verify_webhook_signature(payload, sig) is True

    def test_invalid_signature_returns_false(self):
        payload = b'{"event": "claim.created"}'
        assert flask_app._verify_webhook_signature(payload, "sha256=badhash") is False

    def test_tampered_payload_returns_false(self):
        payload = b'{"event": "claim.created"}'
        sig = _make_signature(payload)
        tampered = b'{"event": "claim.deleted"}'
        assert flask_app._verify_webhook_signature(tampered, sig) is False

    def test_empty_secret_skips_verification(self):
        # When secret is empty, verification is skipped (returns True for dev)
        with patch.object(flask_app, "WEBPT_WEBHOOK_SECRET", ""):
            result = flask_app._verify_webhook_signature(b"payload", "sha256=anything")
        assert result is True


# ---------------------------------------------------------------------------
# GET /auth/webpt
# ---------------------------------------------------------------------------

class TestInitiateWebptOAuth:
    def test_missing_clinic_id_returns_400(self):
        resp = CLIENT.get("/auth/webpt")
        assert resp.status_code == 400

    def test_redirects_when_clinic_id_provided(self):
        mock_connector = MagicMock()
        mock_connector.build_oauth_redirect.return_value = (
            "https://auth.webpt.test/oauth/authorize?code=x", "state-abc"
        )
        with patch.object(flask_app, "connector", mock_connector):
            resp = CLIENT.get("/auth/webpt?clinic_id=clinic-1")
        # Flask redirect returns 302
        assert resp.status_code == 302


# ---------------------------------------------------------------------------
# GET /auth/webpt/callback
# ---------------------------------------------------------------------------

class TestWebptOAuthCallback:
    def test_missing_code_returns_400(self):
        resp = CLIENT.get("/auth/webpt/callback?state=abc")
        assert resp.status_code == 400

    def test_missing_state_returns_400(self):
        resp = CLIENT.get("/auth/webpt/callback?code=abc")
        assert resp.status_code == 400

    def test_csrf_state_mismatch_returns_403(self):
        with CLIENT.session_transaction() as sess:
            sess["webpt_oauth_state"] = "correct-state"
        resp = CLIENT.get("/auth/webpt/callback?code=abc&state=wrong-state")
        assert resp.status_code == 403

    def test_oauth_error_param_returns_400(self):
        resp = CLIENT.get("/auth/webpt/callback?error=access_denied&error_description=User+denied")
        assert resp.status_code == 400

    def test_valid_callback_calls_handle_oauth_callback(self):
        mock_connector = MagicMock()
        mock_connector.handle_oauth_callback.return_value = {
            "status": "connected", "clinic_id": "clinic-1"
        }
        with CLIENT.session_transaction() as sess:
            sess["webpt_oauth_state"] = "valid-state"

        with patch.object(flask_app, "connector", mock_connector):
            resp = CLIENT.get("/auth/webpt/callback?code=auth-code&state=valid-state")

        assert resp.status_code == 200
        mock_connector.handle_oauth_callback.assert_called_once()


# ---------------------------------------------------------------------------
# POST /webhooks/webpt
# ---------------------------------------------------------------------------

class TestWebptWebhook:
    def test_invalid_signature_returns_401(self):
        payload = json.dumps({"event": "claim.created", "data": {}}).encode()
        resp = CLIENT.post(
            "/webhooks/webpt",
            data=payload,
            content_type="application/json",
            headers={"X-WebPT-Signature": "sha256=badsig"},
        )
        assert resp.status_code == 401

    def test_valid_signature_and_claim_event_returns_200(self):
        payload = json.dumps({
            "event": "claim.created",
            "data": {"connection_id": "conn-1", "id": "claim-1"},
        }).encode()
        sig = _make_signature(payload)

        with patch.object(flask_app, "_handle_claim_event") as mock_handler:
            resp = CLIENT.post(
                "/webhooks/webpt",
                data=payload,
                content_type="application/json",
                headers={"X-WebPT-Signature": sig},
            )

        assert resp.status_code == 200
        mock_handler.assert_called_once()

    def test_unknown_event_type_still_returns_200(self):
        payload = json.dumps({"event": "unknown.type", "data": {}}).encode()
        sig = _make_signature(payload)
        resp = CLIENT.post(
            "/webhooks/webpt",
            data=payload,
            content_type="application/json",
            headers={"X-WebPT-Signature": sig},
        )
        assert resp.status_code == 200

    def test_invalid_json_returns_400(self):
        payload = b"not json at all"
        sig = _make_signature(payload)
        resp = CLIENT.post(
            "/webhooks/webpt",
            data=payload,
            content_type="application/octet-stream",
            headers={"X-WebPT-Signature": sig},
        )
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# GET /api/webpt/status/<clinic_id>
# ---------------------------------------------------------------------------

class TestConnectionStatus:
    def test_returns_status_from_connector(self):
        mock_connector = MagicMock()
        mock_connector.get_connection_status.return_value = {
            "status": "ready", "clinic_id": "clinic-1"
        }
        with patch.object(flask_app, "connector", mock_connector):
            resp = CLIENT.get("/api/webpt/status/clinic-1")
        assert resp.status_code == 200
        assert resp.get_json()["status"] == "ready"

    def test_returns_404_when_no_connection(self):
        mock_connector = MagicMock()
        mock_connector.get_connection_status.return_value = None
        with patch.object(flask_app, "connector", mock_connector):
            resp = CLIENT.get("/api/webpt/status/unknown-clinic")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# POST /api/webpt/disconnect/<clinic_id>
# ---------------------------------------------------------------------------

class TestDisconnect:
    def test_returns_200_disconnected(self):
        mock_connector = MagicMock()
        with patch.object(flask_app, "connector", mock_connector):
            resp = CLIENT.post("/api/webpt/disconnect/clinic-1")
        assert resp.status_code == 200
        assert resp.get_json()["status"] == "disconnected"

    def test_calls_connector_disconnect(self):
        mock_connector = MagicMock()
        with patch.object(flask_app, "connector", mock_connector):
            CLIENT.post("/api/webpt/disconnect/clinic-1")
        mock_connector.disconnect.assert_called_once_with("clinic-1")


# ---------------------------------------------------------------------------
# POST /internal/webpt/sync-complete
# ---------------------------------------------------------------------------

class TestSyncComplete:
    INTERNAL_SECRET = "test-internal-secret"   # matches conftest.py env var

    def test_missing_connection_id_returns_400(self):
        resp = CLIENT.post(
            "/internal/webpt/sync-complete",
            json={},
            headers={"X-Internal-Secret": self.INTERNAL_SECRET},
        )
        assert resp.status_code == 400

    def test_wrong_internal_secret_returns_401(self):
        resp = CLIENT.post(
            "/internal/webpt/sync-complete",
            json={"connection_id": "conn-1"},
            headers={"X-Internal-Secret": "wrong-secret"},
        )
        assert resp.status_code == 401

    def test_valid_request_returns_200(self):
        mock_connector = MagicMock()
        mock_connector.finalize_after_sync.return_value = {"status": "ready"}
        with patch.object(flask_app, "connector", mock_connector):
            resp = CLIENT.post(
                "/internal/webpt/sync-complete",
                json={"connection_id": "conn-1"},
                headers={"X-Internal-Secret": self.INTERNAL_SECRET},
            )
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# GET /health
# ---------------------------------------------------------------------------

class TestHealth:
    def test_returns_200_ok(self):
        resp = CLIENT.get("/health")
        assert resp.status_code == 200
        assert resp.get_json()["status"] == "ok"
