"""
Shared pytest fixtures and environment setup for nexus-auth tests.

All unit tests in this suite are designed to run offline (no DB, no network).
Environment variables are set here before any project module is imported so
that module-level constants (e.g. WEBPT_WEBHOOK_SECRET, DATABASE_URL) are
populated correctly when the test modules load their imports.
"""

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock

# ---------------------------------------------------------------------------
# Environment variables — must be set before project imports
# ---------------------------------------------------------------------------
os.environ.setdefault("DATABASE_URL",           "postgresql://fake/testdb")
os.environ.setdefault("FLASK_SECRET_KEY",       "test-secret-key-for-testing-only")
os.environ.setdefault("WEBPT_CLIENT_ID",        "test-client-id")
os.environ.setdefault("WEBPT_CLIENT_SECRET",    "test-client-secret")
os.environ.setdefault("WEBPT_REDIRECT_URI",     "http://localhost/auth/webpt/callback")
os.environ.setdefault("WEBPT_AUTH_URL",         "https://auth.webpt.test/oauth/authorize")
os.environ.setdefault("WEBPT_TOKEN_URL",        "https://auth.webpt.test/oauth/token")
os.environ.setdefault("WEBPT_WEBHOOK_SECRET",   "test-webhook-secret")
os.environ.setdefault("INTERNAL_API_SECRET",    "test-internal-secret")

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest


# ---------------------------------------------------------------------------
# DB connection mocks
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_conn():
    """
    Mock psycopg2 connection with a cursor that supports the context manager
    protocol used throughout the codebase:  `with conn.cursor() as cur:`.
    """
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return conn


@pytest.fixture
def mock_cursor(mock_conn):
    """The cursor extracted from mock_conn (same object returned by __enter__)."""
    return mock_conn.cursor.return_value.__enter__.return_value


# ---------------------------------------------------------------------------
# Sample domain objects
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_claim():
    """Minimal webpt_claims-style dict used by denial recovery tests."""
    return {
        "id": "claim-1",
        "connection_id": "conn-1",
        "webpt_claim_id": "wc-100",
        "patient_id": "pat-1",
        "provider_id": "prov-1",
        "service_date": "2024-01-15",
        "cpt_codes": ["97110", "97140"],
        "icd10_codes": ["M54.5"],
        "claim_status": "denied",
        "amount": 500.0,
        "raw_payload": {"billing_npi": "1234567890"},
    }


@pytest.fixture
def sample_denial():
    """Minimal recoverable_denials-style dict."""
    return {
        "id": "denial-1",
        "clinic_id": "clinic-1",
        "connection_id": "conn-1",
        "webpt_claim_id": "wc-100",
        "denial_code": "CO-16",
        "billed_amount": 500.0,
        "estimated_recovery": 425.0,
        "success_probability": 0.85,
        "status": "detected",
        "fixes_applied": None,
        "fix_notes": None,
        "detected_at": None,
        "fixed_at": None,
        "paid_amount": None,
        "your_fee": None,
        "clinic_net": None,
        "paid_at": None,
    }


# ---------------------------------------------------------------------------
# Flask test client (api_routes blueprint only — no WebPTConnector needed)
# ---------------------------------------------------------------------------

@pytest.fixture
def recovery_client():
    """
    Minimal Flask app with only the recovery blueprint registered.
    Does NOT import app.py so WebPTConnector is never instantiated.
    """
    from flask import Flask
    from api_routes import recovery_bp

    flask_app = Flask(__name__)
    flask_app.register_blueprint(recovery_bp)
    flask_app.config["TESTING"] = True
    with flask_app.test_client() as client:
        yield client
