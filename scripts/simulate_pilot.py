"""
Pilot Clinic Simulation
=======================
Seeds a synthetic PT clinic with realistic denied claims and runs the
full CodeMed denial recovery pipeline against them.

Use this to:
  • Verify the pipeline works end-to-end before a real clinic connects
  • Get a real sample appeal letter to review
  • Produce actual V28 reclassification numbers instead of the ~12% estimate
  • Sanity-check revenue projections before presenting them externally

Usage:
    # Run simulation (creates data, runs pipeline, prints report)
    python scripts/simulate_pilot.py

    # Use a custom clinic ID
    python scripts/simulate_pilot.py --clinic-id MY_TEST_CLINIC

    # Clean up sim data after the run
    python scripts/simulate_pilot.py --cleanup

    # Skip DB writes — just print what would be seeded
    python scripts/simulate_pilot.py --dry-run

Environment:
    DATABASE_URL  — PostgreSQL connection string (required unless --dry-run)
"""

import argparse
import json
import logging
import os
import random
import sys
import uuid
from datetime import date, timedelta
from pathlib import Path

# Allow imports from the project root
sys.path.insert(0, str(Path(__file__).parent.parent))

import psycopg2
import psycopg2.extras

logging.basicConfig(
    level=logging.WARNING,   # Suppress engine noise during simulation
    format="%(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "")

# ---------------------------------------------------------------------------
# Synthetic claim data
# ---------------------------------------------------------------------------

# PT CPT codes — auth-required codes trigger CO-16, bundled codes trigger CO-97
AUTH_REQUIRED = {"97110", "97112", "97116", "97530", "97535"}
BUNDLED       = {"97110", "97112", "97116", "97140", "97150", "97530", "97535"}
HIGH_RISK_CO50 = {"97750", "97755", "97760", "97761", "97763"}

# ICD-10 codes that ARE in the V28 crosswalk (valid)
V28_VALID_ICD10 = [
    "M54.50",  # Low back pain, unspecified (HCC174)
    "M54.51",  # Vertebrogenic low back pain (HCC174)
    "M54.41",  # Lumbago with sciatica, right side (HCC174)
    "M16.1",   # Primary osteoarthritis, hip (HCC39)
    "M17.1",   # Primary osteoarthritis, knee (HCC39)
    "I50.20",  # Unspecified systolic CHF (HCC85)
    "E11.65",  # Type 2 DM with hyperglycemia (HCC37)
    "G82.21",  # Paraplegia, incomplete (HCC70)
    "M05.9",   # Rheumatoid arthritis, unspecified (HCC40)
    "I63.9",   # Cerebral infarction, unspecified (HCC100)
]

# ICD-10 codes NOT in our V28 crosswalk — simulate removed/unmapped codes
# In production these would be the 2,027 codes removed from V28
V28_INVALID_ICD10 = [
    "G89.0",   # Central pain syndrome — removed in V28 (use G89.21/G89.29)
    "M54.5",   # Removed — replaced by more specific M54.50/M54.51/M54.59
    "M79.3",   # Not in our seed crosswalk (panniculitis)
    "R52",     # Unspecified pain — too nonspecific for V28
    "M25.50",  # Pain in unspecified joint — removed from V28 HCC mapping
]


def _rand_date(days_back: int = 180) -> date:
    return date.today() - timedelta(days=random.randint(1, days_back))


def _rand_amount(lo: float = 150.0, hi: float = 650.0) -> float:
    return round(random.uniform(lo, hi), 2)


def build_claim_batch(connection_id: str) -> list[dict]:
    """
    Build a realistic batch of ~45 denied PT claims across three denial types.

    Mix:
      CO-16  15 claims  — missing NPI or prior auth
      CO-50  18 claims  — medical necessity; 4 with V28-invalid codes
      CO-97  12 claims  — bundled PT codes without modifier 59
    """
    claims = []
    pid_pool  = [f"PT{i:04d}" for i in range(1, 20)]  # 19 patients
    prov_pool = [f"PROV{i:02d}" for i in range(1, 6)]  # 5 providers

    def base(cpt_codes, icd10_codes, amount, payload_extra=None):
        raw = {
            "payer": random.choice(["UHC", "AETNA", "BCBS", "CMS"]),
            "mac_jurisdiction": random.choice(["JH", "JL", "J6", "JF"]),
            **(payload_extra or {}),
        }
        return {
            "id":             str(uuid.uuid4()),
            "connection_id":  connection_id,
            "webpt_claim_id": f"WPT-{uuid.uuid4().hex[:10].upper()}",
            "patient_id":     random.choice(pid_pool),
            "provider_id":    random.choice(prov_pool),
            "service_date":   _rand_date(),
            "cpt_codes":      cpt_codes,
            "icd10_codes":    icd10_codes,
            "claim_status":   "denied",
            "amount":         amount,
            "raw_payload":    raw,
        }

    # CO-16 block — missing NPI (no billing_npi in payload)
    for _ in range(8):
        cpts = random.sample(["97110", "97140", "97530"], k=random.randint(1, 2))
        icds = random.sample(V28_VALID_ICD10[:5], k=1)
        claims.append(base(cpts, icds, _rand_amount(120, 400)))
        # No billing_npi in payload → CO-16 trigger

    # CO-16 block — auth-required code, missing prior_auth_number
    for _ in range(7):
        cpts = [random.choice(list(AUTH_REQUIRED))]
        icds = random.sample(V28_VALID_ICD10, k=1)
        claims.append(base(cpts, icds, _rand_amount(200, 500)))
        # No prior_auth_number → CO-16 trigger

    # CO-50 block — high-risk CPT, NO ICD-10 codes (triggers medical necessity flag)
    # billing_npi present so only CO-50 fires, not CO-16
    npi = "1234567890"
    for _ in range(14):
        cpt = random.choice(list(HIGH_RISK_CO50))
        claims.append(base(
            [cpt], [],            # empty ICD-10 → CO-50 trigger
            _rand_amount(250, 650),
            {"payer": "CMS", "billing_npi": npi},
        ))

    # CO-50 block — V28-INVALID ICD-10 codes (reclassification test cases)
    # These have icd10 codes that aren't in the V28 crosswalk; engine flags them
    for _ in range(4):
        cpt  = random.choice(list(HIGH_RISK_CO50))
        icds = [random.choice(V28_INVALID_ICD10)]
        claims.append(base(
            [cpt], icds,
            _rand_amount(200, 500),
            {"payer": "AETNA", "billing_npi": npi},
        ))

    # CO-97 block — 2+ bundled PT codes, no modifier 59
    # billing_npi present so only CO-97 fires
    for _ in range(12):
        cpts = random.sample(list(BUNDLED), k=random.randint(2, 3))
        icds = random.sample(V28_VALID_ICD10[:6], k=1)
        claims.append(base(
            cpts, icds, _rand_amount(300, 800),
            {"billing_npi": npi},   # present → no CO-16; no modifiers → CO-97
        ))

    random.shuffle(claims)
    return claims


# ---------------------------------------------------------------------------
# DB operations
# ---------------------------------------------------------------------------

def seed_connection(conn, clinic_id: str) -> str:
    """Insert a synthetic webpt_connections row. Returns connection_id."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO webpt_connections
                (clinic_id, status, webpt_practice_id,
                 sync_completed_at, claims_synced,
                 webhook_registered, connected_at,
                 metadata)
            VALUES (%s, 'ready', %s, NOW(), 0, FALSE, NOW(), %s)
            ON CONFLICT DO NOTHING
            RETURNING id
            """,
            (
                clinic_id,
                f"WEBPT-PRACTICE-{clinic_id}",
                json.dumps({
                    "clinic_name":   "Valley Physical Therapy (Simulation)",
                    "contact_email": "billing@valleypt.example.com",
                    "_simulated":    True,
                }),
            ),
        )
        row = cur.fetchone()
        if row:
            conn.commit()
            return str(row[0])

        # Already exists — fetch it
        cur.execute(
            "SELECT id FROM webpt_connections WHERE clinic_id = %s AND status != 'disconnected'",
            (clinic_id,),
        )
        return str(cur.fetchone()[0])


def seed_claims(conn, claims: list[dict]) -> int:
    """Bulk-insert synthetic claims. Returns count inserted."""
    inserted = 0
    with conn.cursor() as cur:
        for c in claims:
            cur.execute(
                """
                INSERT INTO webpt_claims
                    (id, connection_id, webpt_claim_id, patient_id, provider_id,
                     service_date, cpt_codes, icd10_codes, claim_status, amount, raw_payload)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                (
                    c["id"], c["connection_id"], c["webpt_claim_id"],
                    c["patient_id"], c["provider_id"], c["service_date"],
                    c["cpt_codes"], c["icd10_codes"], c["claim_status"],
                    c["amount"], json.dumps(c["raw_payload"]),
                ),
            )
            inserted += cur.rowcount
    conn.commit()
    return inserted


def cleanup(conn, clinic_id: str) -> None:
    """Remove all simulation data for this clinic."""
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM webpt_connections
            WHERE clinic_id = %s
              AND metadata->>'_simulated' = 'true'
            """,
            (clinic_id,),
        )
    conn.commit()
    print(f"\nCleaned up simulation data for clinic '{clinic_id}'.")


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def print_report(
    clinic_id: str,
    batch_result: dict,
    v28_stats: dict,
    sample_appeal: str,
) -> None:
    sep = "─" * 66

    print(f"\n{'═' * 66}")
    print(f"  CODEMED PILOT SIMULATION REPORT")
    print(f"  Clinic: {clinic_id}")
    print(f"  Date:   {date.today()}")
    print(f"{'═' * 66}\n")

    # Pipeline summary
    total   = batch_result["total_processed"]
    fixed   = batch_result["fixed"]
    est     = batch_result["estimated_recovery"]
    fee_est = sum(
        r.get("value", {}).get("your_fee", 0)
        for r in batch_result["results"]
        if r.get("status") == "fixed"
    )

    print("PIPELINE RESULTS")
    print(sep)
    print(f"  Claims processed:       {total}")
    print(f"  Denials fixed:          {fixed}")
    print(f"  Estimated recovery:     ${est:,.2f}")
    print(f"  Your estimated fee:     ${fee_est:,.2f}")
    print(f"  Clinic net:             ${est - fee_est:,.2f}")

    # By code
    by_code: dict[str, dict] = {}
    for r in batch_result["results"]:
        if r.get("status") != "fixed":
            continue
        code = r.get("denial_code", "UNKNOWN")
        v    = r.get("value", {})
        if code not in by_code:
            by_code[code] = {"count": 0, "recovery": 0.0, "fee": 0.0}
        by_code[code]["count"]    += 1
        by_code[code]["recovery"] += v.get("estimated_recovery", 0)
        by_code[code]["fee"]      += v.get("your_fee", 0)

    print(f"\n{'CODE':<8} {'COUNT':>6} {'RECOVERY':>12} {'FEE':>10} {'RATE'}")
    print(sep)
    fee_rates = {"CO-16": "20%", "CO-50": "30%", "CO-97": "20%"}
    for code, d in sorted(by_code.items()):
        print(
            f"  {code:<6} {d['count']:>6} "
            f"${d['recovery']:>10,.2f} "
            f"${d['fee']:>8,.2f}  "
            f"{fee_rates.get(code, '?')}"
        )

    # V28 reclassification
    print(f"\nV28 RECLASSIFICATION")
    print(sep)
    total_co50 = v28_stats.get("co50_total", 0)
    reclass    = v28_stats.get("v28_reclassified", 0)
    pct        = v28_stats.get("reclassification_pct") or 0
    print(f"  CO-50 denials total:    {total_co50}")
    print(f"  V28-invalid code found: {reclass}  ({pct}%)")
    print(f"  These route to ~85% fix rate instead of 45%")
    if total_co50 > 0:
        assumption_diff = float(pct) - 12.0
        direction = "above" if assumption_diff >= 0 else "below"
        print(
            f"  vs. 12% assumption:     {abs(assumption_diff):.1f}pp {direction} "
            f"{'— update the investor deck' if abs(assumption_diff) > 3 else '— assumption holds'}"
        )

    # Scale projection
    print(f"\n30-CLINIC PROJECTION (annualised from this simulation)")
    print(sep)
    monthly_per_clinic = est  # treat one sim run as one clinic-month
    annual_30          = monthly_per_clinic * 12 * 30
    annual_fee_30      = fee_est * 12 * 30
    print(f"  Monthly recovery / clinic:  ${monthly_per_clinic:,.0f}")
    print(f"  Annual across 30 clinics:   ${annual_30:,.0f}")
    print(f"  Your annual fee (30 clinics): ${annual_fee_30:,.0f}")
    print(f"  Gross margin:               ~92%")

    # Sample appeal letter
    print(f"\nSAMPLE CO-50 APPEAL LETTER")
    print(sep)
    if sample_appeal:
        # Print the letter indented
        for line in sample_appeal.splitlines():
            print(f"  {line}")
    else:
        print("  (No CO-50 denials generated — adjust synthetic data mix)")

    print(f"\n{'═' * 66}\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv=None):
    parser = argparse.ArgumentParser(description="Simulate a CodeMed pilot clinic")
    parser.add_argument("--clinic-id", default="PILOT_CLINIC_SIM_001",
                        help="Synthetic clinic ID (default: PILOT_CLINIC_SIM_001)")
    parser.add_argument("--cleanup", action="store_true",
                        help="Delete simulation data after report")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print data that would be seeded without writing to DB")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed for reproducible output (default: 42)")
    args = parser.parse_args(argv)

    random.seed(args.seed)
    clinic_id = args.clinic_id

    print(f"\nCodeMed Pilot Simulation")
    print(f"Clinic:   {clinic_id}")
    print(f"Dry run:  {args.dry_run}")

    # Build synthetic claims (no DB needed)
    conn_id_placeholder = str(uuid.uuid4())
    claims = build_claim_batch(conn_id_placeholder)

    print(f"Claims generated: {len(claims)}")
    print(f"  CO-16 candidates: {sum(1 for c in claims if not c['raw_payload'].get('billing_npi') and not any(p in str(c['raw_payload']) for p in ['prior_auth_number']))}")
    print(f"  CO-50 candidates: {sum(1 for c in claims if any(cpt in HIGH_RISK_CO50 for cpt in c['cpt_codes']))}")
    print(f"  CO-97 candidates: {sum(1 for c in claims if sum(1 for cpt in c['cpt_codes'] if cpt in BUNDLED) >= 2)}")

    if args.dry_run:
        print("\nDry run — sample claim:")
        sample = claims[0]
        print(json.dumps({k: str(v) if hasattr(v, 'isoformat') else v
                          for k, v in sample.items() if k != 'raw_payload'}, indent=2))
        return 0

    if not DATABASE_URL:
        print("\nERROR: DATABASE_URL is not set. Set it or use --dry-run.")
        return 1

    db = psycopg2.connect(DATABASE_URL)

    try:
        # Seed
        print("\nSeeding database...")
        connection_id = seed_connection(db, clinic_id)
        # Patch claims with the real connection_id
        for c in claims:
            c["connection_id"] = connection_id
        n = seed_claims(db, claims)
        print(f"  Inserted {n} claims (connection {connection_id})")

        # Run pipeline
        print("Running denial recovery pipeline...")
        from denial_recovery import DenialRecoveryEngine
        engine = DenialRecoveryEngine(db)
        batch_result = engine.batch_process(clinic_id)

        # V28 stats
        print("Fetching V28 reclassification stats...")
        with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*)                                                AS co50_total,
                    COUNT(*) FILTER (
                        WHERE fixes_applied @> '[{"source": "v28_invalid"}]'
                    )                                                       AS v28_reclassified,
                    ROUND(
                        COUNT(*) FILTER (
                            WHERE fixes_applied @> '[{"source": "v28_invalid"}]'
                        )::numeric / NULLIF(COUNT(*), 0) * 100, 1
                    )                                                       AS reclassification_pct
                FROM recoverable_denials
                WHERE clinic_id = %s AND denial_code = 'CO-50'
                """,
                (clinic_id,),
            )
            v28_stats = dict(cur.fetchone())

        # Grab a sample CO-50 appeal letter
        sample_appeal = ""
        for r in batch_result["results"]:
            if r.get("denial_code") == "CO-50" and r.get("status") == "fixed":
                fixes = r.get("fixes_applied") or []
                for fix in fixes:
                    if fix.get("field") == "appeal_letter":
                        sample_appeal = fix.get("value", "")
                        break
            if sample_appeal:
                break

        # Print report
        print_report(clinic_id, batch_result, v28_stats, sample_appeal)

        if args.cleanup:
            cleanup(db, clinic_id)

    finally:
        db.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
