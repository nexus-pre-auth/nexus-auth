"""
Denial Recovery & Revenue API Routes.

Mount this blueprint in app.py:

    from api_routes import recovery_bp
    app.register_blueprint(recovery_bp)

Endpoints
---------
POST /api/recovery/detect/<clinic_id>          Scan claims, insert recoverable_denials
POST /api/recovery/process/<denial_id>         Fix a single denial
POST /api/recovery/batch/<clinic_id>           Detect + fix all pending denials

GET  /api/recovery/stats/<clinic_id>           Full pipeline summary
GET  /api/recovery/report/<clinic_id>          Monthly invoice report (?year=&month=)

POST /api/recovery/payment/<denial_id>         Record payer payment (body: {paid_amount})
GET  /api/recovery/denials/<clinic_id>         List denials with optional ?status= filter
"""

import os
from datetime import date

import psycopg2
import psycopg2.extras
from flask import Blueprint, jsonify, request

from denial_recovery import DenialRecoveryEngine
from revenue_tracking import RevenueTracker

recovery_bp = Blueprint("recovery", __name__)


def _db() -> psycopg2.extensions.connection:
    return psycopg2.connect(os.environ["DATABASE_URL"])


# ---------------------------------------------------------------------------
# Detection & processing
# ---------------------------------------------------------------------------

@recovery_bp.post("/api/recovery/detect/<clinic_id>")
def detect_denials(clinic_id: str):
    """Scan the clinic's synced claims and surface CO-16/50/97 opportunities."""
    conn = _db()
    try:
        engine = DenialRecoveryEngine(conn)
        detected = engine.detect_recoverable_denials(clinic_id)
    finally:
        conn.close()

    return jsonify({
        "clinic_id": clinic_id,
        "detected": len(detected),
        "denials": [_serialize_denial(d) for d in detected],
    }), 200


@recovery_bp.post("/api/recovery/process/<denial_id>")
def process_denial(denial_id: str):
    """Apply the appropriate fix for a single denial."""
    conn = _db()
    try:
        engine = DenialRecoveryEngine(conn)
        result = engine.process_denial(denial_id)
    finally:
        conn.close()

    return jsonify(result), 200


@recovery_bp.post("/api/recovery/batch/<clinic_id>")
def batch_process(clinic_id: str):
    """Detect then fix all pending CO-16/50/97 denials for a clinic."""
    conn = _db()
    try:
        engine = DenialRecoveryEngine(conn)
        result = engine.batch_process(clinic_id)
    finally:
        conn.close()

    return jsonify(result), 200


# ---------------------------------------------------------------------------
# Stats & reporting
# ---------------------------------------------------------------------------

@recovery_bp.get("/api/recovery/stats/<clinic_id>")
def recovery_stats(clinic_id: str):
    """
    Full pipeline summary — counts by code, counts by status, and realised revenue.
    This is the primary data feed for the clinic dashboard.
    """
    conn = _db()
    try:
        tracker = RevenueTracker(conn)
        summary = tracker.get_clinic_summary(clinic_id)
    finally:
        conn.close()

    return jsonify(summary), 200


@recovery_bp.get("/api/recovery/report/<clinic_id>")
def monthly_report(clinic_id: str):
    """
    Invoice-ready monthly breakdown.

    Query params:
      year  (int, default: current year)
      month (int, default: current month)
    """
    today = date.today()
    try:
        year  = int(request.args.get("year",  today.year))
        month = int(request.args.get("month", today.month))
    except ValueError:
        return jsonify({"error": "year and month must be integers"}), 400

    conn = _db()
    try:
        tracker = RevenueTracker(conn)
        report = tracker.get_monthly_report(clinic_id, year, month)
    finally:
        conn.close()

    return jsonify(report), 200


# ---------------------------------------------------------------------------
# Payments
# ---------------------------------------------------------------------------

@recovery_bp.post("/api/recovery/payment/<denial_id>")
def record_payment(denial_id: str):
    """
    Record that a payer paid a recovered claim.

    Body (JSON): { "paid_amount": 123.45 }
    """
    body = request.get_json(silent=True) or {}
    paid_amount = body.get("paid_amount")
    if paid_amount is None:
        return jsonify({"error": "paid_amount is required"}), 400
    try:
        paid_amount = float(paid_amount)
    except (TypeError, ValueError):
        return jsonify({"error": "paid_amount must be a number"}), 400

    conn = _db()
    try:
        tracker = RevenueTracker(conn)
        result = tracker.record_payment(denial_id, paid_amount)
    finally:
        conn.close()

    return jsonify(result), 200


# ---------------------------------------------------------------------------
# Denial list
# ---------------------------------------------------------------------------

@recovery_bp.get("/api/recovery/denials/<clinic_id>")
def list_denials(clinic_id: str):
    """
    List denials for a clinic.

    Query params:
      status  — filter by status (detected | fixed | submitted | paid | rejected)
      code    — filter by denial code (CO-16 | CO-50 | CO-97)
      limit   — max rows (default 100)
      offset  — pagination offset (default 0)
    """
    status = request.args.get("status")
    code   = request.args.get("code")
    try:
        limit  = min(int(request.args.get("limit",  100)), 500)
        offset = int(request.args.get("offset", 0))
    except ValueError:
        return jsonify({"error": "limit and offset must be integers"}), 400

    filters = ["clinic_id = %s"]
    params: list = [clinic_id]

    if status:
        filters.append("status = %s")
        params.append(status)
    if code:
        filters.append("denial_code = %s")
        params.append(code)

    where = " AND ".join(filters)
    params += [limit, offset]

    conn = _db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT id, webpt_claim_id, denial_code, billed_amount,
                       estimated_recovery, success_probability, status,
                       fixes_applied, fix_notes, detected_at, fixed_at,
                       paid_amount, your_fee, clinic_net, paid_at
                FROM recoverable_denials
                WHERE {where}
                ORDER BY detected_at DESC
                LIMIT %s OFFSET %s
                """,
                params,
            )
            rows = [dict(r) for r in cur.fetchall()]

            cur.execute(
                f"SELECT COUNT(*) FROM recoverable_denials WHERE {where[:-len(' LIMIT %s OFFSET %s') if False else '']}",
                params[:-2],
            )
            total = cur.fetchone()["count"]
    finally:
        conn.close()

    return jsonify({
        "clinic_id": clinic_id,
        "total": total,
        "limit": limit,
        "offset": offset,
        "denials": [_serialize_denial(r) for r in rows],
    }), 200


# ---------------------------------------------------------------------------
# Serializer
# ---------------------------------------------------------------------------

def _serialize_denial(d: dict) -> dict:
    """Convert a denial row to a JSON-safe dict."""
    result = {}
    for k, v in d.items():
        if hasattr(v, "isoformat"):       # date / datetime
            result[k] = v.isoformat()
        elif hasattr(v, "__str__") and not isinstance(v, (str, int, float, bool, list, dict, type(None))):
            result[k] = str(v)            # UUID etc.
        else:
            result[k] = v
    return result
