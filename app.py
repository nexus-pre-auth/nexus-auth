"""
NexusAuth / CodeMed — WebPT OAuth Integration Server

Endpoints
---------
GET  /auth/webpt                       Initiate OAuth flow for a clinic
GET  /auth/webpt/callback              OAuth callback (code exchange)
GET  /api/webpt/status/<clinic_id>     Poll connection + sync status
POST /api/webpt/disconnect/<clinic_id> Revoke connection
POST /webhooks/webpt                   Receive real-time claim events from WebPT
POST /internal/webpt/sync-complete     Called by RQ worker when sync finishes
"""

import hashlib
import hmac
import logging
import os

from flask import Flask, jsonify, redirect, request, session

from webpt.connect import WebPTConnector

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", os.urandom(32))

connector = WebPTConnector()

# ---------------------------------------------------------------------------
# OAuth flow
# ---------------------------------------------------------------------------

@app.get("/auth/webpt")
def initiate_webpt_oauth():
    """
    Step 1: Clinic owner clicks "Connect WebPT".
    Redirects to WebPT OAuth authorization page.

    Query params:
      clinic_id (required) — your internal clinic identifier
    """
    clinic_id = request.args.get("clinic_id")
    if not clinic_id:
        return jsonify({"error": "clinic_id is required"}), 400

    authorization_url, state = connector.build_oauth_redirect(clinic_id)

    # Store state in session for CSRF validation in the callback
    session["webpt_oauth_state"] = state
    session["webpt_clinic_id"] = clinic_id

    logger.info("Redirecting clinic %s to WebPT OAuth", clinic_id)
    return redirect(authorization_url)


@app.get("/auth/webpt/callback")
def webpt_oauth_callback():
    """
    Step 2: WebPT redirects back after the clinic owner approves access.

    WebPT provides:
      code  — authorization code
      state — CSRF nonce (must match session)
    """
    code  = request.args.get("code")
    state = request.args.get("state")
    error = request.args.get("error")

    if error:
        logger.warning("WebPT OAuth error: %s", error)
        return jsonify({"error": error, "description": request.args.get("error_description")}), 400

    if not code or not state:
        return jsonify({"error": "Missing code or state parameter"}), 400

    # CSRF check
    session_state = session.get("webpt_oauth_state")
    if not session_state or not hmac.compare_digest(state, session_state):
        return jsonify({"error": "State mismatch — possible CSRF attack"}), 403

    practice_id = request.args.get("practice_id")  # WebPT may include this

    result = connector.handle_oauth_callback(code, state, practice_id=practice_id)

    # Clear OAuth session keys
    session.pop("webpt_oauth_state", None)
    session.pop("webpt_clinic_id", None)

    logger.info("OAuth callback complete: %s", result)
    return jsonify(result), 200


# ---------------------------------------------------------------------------
# Status polling
# ---------------------------------------------------------------------------

@app.get("/api/webpt/status/<clinic_id>")
def connection_status(clinic_id: str):
    """
    Step 3: Frontend polls this to track sync progress.

    Possible status values:
      pending    → OAuth not yet completed
      connected  → OAuth done, sync queued ("Connected! Loading your data...")
      syncing    → Historical sync in progress
      ready      → Sync + pattern detection done ("Your dashboard is ready!")
      error      → Something went wrong
    """
    status = connector.get_connection_status(clinic_id)
    if status is None:
        return jsonify({"error": "No connection found for this clinic"}), 404
    return jsonify(status), 200


# ---------------------------------------------------------------------------
# Disconnect
# ---------------------------------------------------------------------------

@app.post("/api/webpt/disconnect/<clinic_id>")
def disconnect(clinic_id: str):
    """Revoke WebPT access and clean up the connection."""
    connector.disconnect(clinic_id)
    return jsonify({"status": "disconnected"}), 200


# ---------------------------------------------------------------------------
# WebPT → CodeMed webhook (real-time claim events)
# ---------------------------------------------------------------------------

WEBPT_WEBHOOK_SECRET = os.getenv("WEBPT_WEBHOOK_SECRET", "")


def _verify_webhook_signature(payload: bytes, signature: str) -> bool:
    """Verify HMAC-SHA256 signature sent by WebPT."""
    if not WEBPT_WEBHOOK_SECRET:
        return True  # Skip verification in development if secret not configured
    expected = hmac.new(
        WEBPT_WEBHOOK_SECRET.encode(),
        payload,
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(f"sha256={expected}", signature)


@app.post("/webhooks/webpt")
def webpt_webhook():
    """
    Receive real-time claim events from WebPT after successful connection.
    Stores new/updated claims and re-runs pattern detection incrementally.
    """
    signature = request.headers.get("X-WebPT-Signature", "")
    payload   = request.get_data()

    if not _verify_webhook_signature(payload, signature):
        logger.warning("Webhook signature verification failed")
        return jsonify({"error": "Invalid signature"}), 401

    event = request.get_json(force=True, silent=True)
    if not event:
        return jsonify({"error": "Invalid JSON payload"}), 400

    event_type = event.get("event")
    claim_data = event.get("data") or {}

    logger.info("Received WebPT webhook event: %s", event_type)

    if event_type in ("claim.created", "claim.updated", "claim.submitted"):
        _handle_claim_event(claim_data)

    return jsonify({"received": True}), 200


def _handle_claim_event(claim_data: dict) -> None:
    """Persist a single claim event received via webhook."""
    import psycopg2
    import psycopg2.extras

    connection_id = claim_data.get("connection_id")
    if not connection_id:
        logger.warning("Claim event missing connection_id — skipping")
        return

    database_url = os.environ.get("DATABASE_URL", "")
    conn = psycopg2.connect(database_url)
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO webpt_claims
                    (connection_id, webpt_claim_id, patient_id, provider_id,
                     service_date, cpt_codes, icd10_codes, claim_status, amount, raw_payload)
                VALUES %s
                ON CONFLICT (connection_id, webpt_claim_id) DO UPDATE SET
                    claim_status = EXCLUDED.claim_status,
                    amount       = EXCLUDED.amount,
                    raw_payload  = EXCLUDED.raw_payload,
                    synced_at    = NOW()
                """,
                [(
                    connection_id,
                    str(claim_data.get("id") or claim_data.get("claim_id", "")),
                    claim_data.get("patient_id"),
                    claim_data.get("provider_id"),
                    claim_data.get("service_date") or claim_data.get("date_of_service"),
                    claim_data.get("cpt_codes") or [],
                    claim_data.get("icd10_codes") or [],
                    claim_data.get("status") or claim_data.get("claim_status"),
                    claim_data.get("amount"),
                    psycopg2.extras.Json(claim_data),
                )],
            )
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Internal: sync-complete notification from RQ worker
# ---------------------------------------------------------------------------

INTERNAL_SECRET = os.getenv("INTERNAL_API_SECRET", "")


@app.post("/internal/webpt/sync-complete")
def sync_complete():
    """
    Called by the RQ worker (or a post-sync hook) once the historical sync
    job finishes.  Triggers pattern detection on the Intelligence Graph
    and transitions the connection to 'ready'.
    """
    # Simple shared-secret auth for internal calls
    if INTERNAL_SECRET:
        auth = request.headers.get("X-Internal-Secret", "")
        if not hmac.compare_digest(auth, INTERNAL_SECRET):
            return jsonify({"error": "Unauthorized"}), 401

    body = request.get_json(force=True, silent=True) or {}
    connection_id = body.get("connection_id")
    if not connection_id:
        return jsonify({"error": "connection_id is required"}), 400

    result = connector.finalize_after_sync(connection_id)
    return jsonify(result), 200


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    return jsonify({"status": "ok"}), 200


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    debug = os.getenv("FLASK_DEBUG", "false").lower() == "true"
    app.run(host="0.0.0.0", port=port, debug=debug)
