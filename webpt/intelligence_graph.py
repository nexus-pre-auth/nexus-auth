"""
Intelligence Graph — claims storage and pattern detection.

After the historical sync loads raw claims into webpt_claims, this module:
  1. Stores enriched claim data into the intelligence layer
  2. Runs pattern detection across the claim set to surface anomalies
     and actionable insights for the clinic owner

Detected pattern types (mirrors pattern_type_enum in migration 003):
  - high_denial_rate     : codes with denial rate > threshold
  - missing_auth         : claims lacking required prior auth
  - code_mismatch        : ICD-10 / CPT pairs that don't align
  - duplicate_billing    : same service billed multiple times
  - frequency_anomaly    : visit frequency outside normal range
  - documentation_gap    : claims with incomplete documentation signals
"""

import logging
from collections import Counter, defaultdict
from typing import Optional

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

# Thresholds
DENIAL_RATE_THRESHOLD   = 0.20   # 20 % denial rate triggers a pattern
DUPLICATE_WINDOW_DAYS   = 7      # same patient + code within N days = duplicate
MIN_OCCURRENCES         = 3      # minimum hits before a pattern is flagged


# ---------------------------------------------------------------------------
# Public entrypoint
# ---------------------------------------------------------------------------

def run_pattern_detection(conn: psycopg2.extensions.connection, connection_id: str) -> int:
    """
    Analyse claims for connection_id and insert detected patterns.
    Returns the number of patterns found.
    """
    logger.info("Running pattern detection for connection %s", connection_id)

    claims = _load_claims(conn, connection_id)
    if not claims:
        logger.info("No claims found — skipping pattern detection")
        return 0

    patterns = []
    patterns.extend(_detect_high_denial_rate(claims))
    patterns.extend(_detect_duplicate_billing(claims))
    patterns.extend(_detect_frequency_anomaly(claims))
    patterns.extend(_detect_missing_auth(claims))

    _insert_patterns(conn, connection_id, patterns)
    logger.info("Detected %d patterns for connection %s", len(patterns), connection_id)
    return len(patterns)


# ---------------------------------------------------------------------------
# Claim loader
# ---------------------------------------------------------------------------

def _load_claims(conn, connection_id: str) -> list:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT webpt_claim_id, patient_id, provider_id, service_date,
                   cpt_codes, icd10_codes, claim_status, amount, raw_payload
            FROM webpt_claims
            WHERE connection_id = %s
            ORDER BY service_date DESC
            """,
            (connection_id,),
        )
        return cur.fetchall()


# ---------------------------------------------------------------------------
# Pattern detectors
# ---------------------------------------------------------------------------

def _detect_high_denial_rate(claims: list) -> list:
    """Flag CPT codes with denial rate above threshold."""
    totals: Counter = Counter()
    denials: Counter = Counter()

    for c in claims:
        status = (c.get("claim_status") or "").lower()
        for code in (c.get("cpt_codes") or []):
            totals[code] += 1
            if "denied" in status or "reject" in status:
                denials[code] += 1

    patterns = []
    for code, total in totals.items():
        if total < MIN_OCCURRENCES:
            continue
        rate = denials[code] / total
        if rate >= DENIAL_RATE_THRESHOLD:
            patterns.append({
                "pattern_type": "high_denial_rate",
                "description": (
                    f"CPT {code} has a {rate:.0%} denial rate "
                    f"({denials[code]}/{total} claims denied)"
                ),
                "affected_codes": [code],
                "occurrence_count": denials[code],
                "confidence": min(0.5 + rate, 0.99),
                "severity": "high" if rate >= 0.4 else "medium",
            })
    return patterns


def _detect_duplicate_billing(claims: list) -> list:
    """Detect same patient + CPT code billed within DUPLICATE_WINDOW_DAYS."""
    from datetime import timedelta

    # Group by (patient_id, cpt_code) → list of service dates
    groups: dict = defaultdict(list)
    for c in claims:
        if not c.get("service_date") or not c.get("patient_id"):
            continue
        for code in (c.get("cpt_codes") or []):
            groups[(c["patient_id"], code)].append(c["service_date"])

    patterns = []
    for (patient_id, code), dates in groups.items():
        dates_sorted = sorted(dates)
        duplicates = 0
        for i in range(1, len(dates_sorted)):
            delta = dates_sorted[i] - dates_sorted[i - 1]
            if hasattr(delta, "days") and delta.days <= DUPLICATE_WINDOW_DAYS:
                duplicates += 1

        if duplicates >= MIN_OCCURRENCES:
            patterns.append({
                "pattern_type": "duplicate_billing",
                "description": (
                    f"CPT {code} billed {duplicates} times within "
                    f"{DUPLICATE_WINDOW_DAYS} days for same patient"
                ),
                "affected_codes": [code],
                "occurrence_count": duplicates,
                "confidence": 0.80,
                "severity": "high",
            })
    return patterns


def _detect_frequency_anomaly(claims: list) -> list:
    """Flag providers with abnormally high claim volume per day."""
    provider_dates: dict = defaultdict(set)
    for c in claims:
        if c.get("provider_id") and c.get("service_date"):
            provider_dates[c["provider_id"]].add(c["service_date"])

    # Rough heuristic: > 20 distinct service days in the dataset is normal;
    # a provider averaging > 15 claims/day is an anomaly worth reviewing.
    provider_claims: Counter = Counter()
    for c in claims:
        if c.get("provider_id"):
            provider_claims[c["provider_id"]] += 1

    patterns = []
    for provider_id, days in provider_dates.items():
        if not days:
            continue
        avg_per_day = provider_claims[provider_id] / len(days)
        if avg_per_day > 15 and provider_claims[provider_id] >= MIN_OCCURRENCES:
            patterns.append({
                "pattern_type": "frequency_anomaly",
                "description": (
                    f"Provider {provider_id} averages {avg_per_day:.1f} claims/day "
                    f"({provider_claims[provider_id]} total across {len(days)} days)"
                ),
                "affected_codes": [],
                "occurrence_count": provider_claims[provider_id],
                "confidence": 0.70,
                "severity": "medium",
            })
    return patterns


def _detect_missing_auth(claims: list) -> list:
    """
    Flag CPT codes that typically require prior auth but are missing
    an auth reference in the raw payload.
    """
    # Codes commonly requiring prior auth in PT/rehab settings
    AUTH_REQUIRED_CODES = {
        "97110", "97112", "97116", "97530", "97535", "97542",
        "97750", "97755", "97760", "97761", "97763",
    }

    code_no_auth: Counter = Counter()
    for c in claims:
        payload = c.get("raw_payload") or {}
        has_auth = bool(
            payload.get("prior_auth_number")
            or payload.get("auth_number")
            or payload.get("authorization")
        )
        if has_auth:
            continue
        for code in (c.get("cpt_codes") or []):
            if code in AUTH_REQUIRED_CODES:
                code_no_auth[code] += 1

    patterns = []
    for code, count in code_no_auth.items():
        if count >= MIN_OCCURRENCES:
            patterns.append({
                "pattern_type": "missing_auth",
                "description": (
                    f"CPT {code} appears {count} times without a prior authorization reference"
                ),
                "affected_codes": [code],
                "occurrence_count": count,
                "confidence": 0.85,
                "severity": "high",
            })
    return patterns


# ---------------------------------------------------------------------------
# Pattern persistence
# ---------------------------------------------------------------------------

def _insert_patterns(
    conn: psycopg2.extensions.connection,
    connection_id: str,
    patterns: list,
) -> None:
    if not patterns:
        return

    rows = [
        (
            connection_id,
            p["pattern_type"],
            p["description"],
            p.get("affected_codes") or [],
            p.get("occurrence_count", 0),
            p.get("confidence"),
            p.get("severity", "medium"),
        )
        for p in patterns
    ]

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO intelligence_patterns
                (connection_id, pattern_type, description, affected_codes,
                 occurrence_count, confidence, severity)
            VALUES %s
            """,
            rows,
        )
    conn.commit()
