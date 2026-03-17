"""
CO-16 / CO-50 / CO-97 Denial Recovery Engine.

Each denial code has a distinct root cause and a distinct fix strategy:

  CO-16  Missing or invalid information (NPI, auth number, diagnosis pointer)
           → auto-fill from existing data in the claim / connection record
           → success rate: ~85 %

  CO-50  Non-covered service / medical necessity not established
           → generate a structured appeal letter citing clinical evidence
           → success rate: ~45 %

  CO-97  Procedure/service bundled into another service
           → append modifier 59 (distinct procedural service) to CPT code
           → success rate: ~75 %

Revenue model (tiered by complexity):
  CO-16  20 % — commodity fix, quick turnaround
  CO-50  30 % — premium LCD/NCD appeals, specialist work
  CO-97  20 % — commodity fix, quick turnaround
Zero upfront cost to the clinic; fee charged only on recovered amounts.
"""

import logging
import secrets
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import psycopg2
import psycopg2.extras

from agents.risk_adjustment.v28_engine import V28RiskAdjustmentEngine
from agents.nexus_auth.policies_2026 import PolicyEngine2026

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Per-code fee rates — CO-50 commands a premium because it requires
# payer-specific LCD/NCD policy research; CO-16 and CO-97 are commodity fixes.
FEE_RATES = {
    "CO-16": 0.20,
    "CO-50": 0.30,
    "CO-97": 0.20,
}

SUCCESS_RATES = {
    "CO-16": 0.85,
    "CO-50": 0.45,
    "CO-97": 0.75,
}

# PT/rehab codes that payers routinely bundle — modifier 59 unlocks separate payment
BUNDLED_PT_CODES = {
    "97110",  # Therapeutic exercises
    "97112",  # Neuromuscular reeducation
    "97116",  # Gait training
    "97140",  # Manual therapy
    "97150",  # Therapeutic procedure, group
    "97530",  # Therapeutic activities
    "97535",  # Self-care / home management
}

# Codes that almost always require prior auth from commercial payers
AUTH_REQUIRED_CODES = {
    "97110", "97112", "97116", "97530", "97535",
    "97542", "97750", "97755", "97760", "97761", "97763",
}


# ---------------------------------------------------------------------------
# Main engine
# ---------------------------------------------------------------------------

class DenialRecoveryEngine:
    """Detects, fixes, and tracks CO-16/50/97 denials for a clinic."""

    def __init__(self, db_conn: psycopg2.extensions.connection):
        self._conn = db_conn

    # ------------------------------------------------------------------
    # Detection
    # ------------------------------------------------------------------

    def detect_recoverable_denials(self, clinic_id: str) -> list[dict]:
        """
        Scan webpt_claims for patterns that map to CO-16, CO-50, or CO-97
        and insert rows into recoverable_denials (idempotent — skips dupes).
        Returns the newly detected rows.
        """
        claims = self._load_claims(clinic_id)
        detected = []

        for claim in claims:
            codes = self._classify_denial_codes(claim)
            for code in codes:
                row = self._upsert_denial(clinic_id, claim, code)
                if row:
                    detected.append(row)

        logger.info(
            "Detected %d recoverable denial(s) for clinic %s", len(detected), clinic_id
        )
        return detected

    # ------------------------------------------------------------------
    # Processing
    # ------------------------------------------------------------------

    def process_denial(self, denial_id: str) -> dict:
        """
        Apply the appropriate fix for a single recoverable denial.
        Returns a result dict with fixes_applied and estimated recovery value.
        """
        denial = self._get_denial(denial_id)
        if denial is None:
            raise ValueError(f"Denial {denial_id} not found")

        code = denial["denial_code"]
        claim = self._get_claim_by_webpt_id(
            denial["connection_id"], denial["webpt_claim_id"]
        )

        # Route to the right fixer
        if code == "CO-16":
            fixes, notes = self._fix_co16(claim)
        elif code == "CO-50":
            fixes, notes = self._fix_co50(claim)
        elif code == "CO-97":
            fixes, notes = self._fix_co97(claim)
        else:
            return {"status": "unsupported", "denial_id": denial_id, "code": code}

        value = self._calculate_value(denial["billed_amount"], code)

        with self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE recoverable_denials
                SET status = 'pending_approval',
                    fixes_applied = %s,
                    fix_notes = %s,
                    fixed_at = NOW(),
                    estimated_recovery = %s,
                    success_probability = %s,
                    your_fee = %s,
                    clinic_net = %s
                WHERE id = %s
                """,
                (
                    psycopg2.extras.Json(fixes),
                    notes,
                    value["estimated_recovery"],
                    value["success_probability"],
                    value["your_fee"],
                    value["clinic_net"],
                    denial_id,
                ),
            )
        self._conn.commit()

        logger.info(
            "Fixed denial %s (%s) — estimated recovery $%.2f — awaiting approval",
            denial_id, code, value["estimated_recovery"],
        )
        return {
            "denial_id": denial_id,
            "denial_code": code,
            "fixes_applied": fixes,
            "fix_notes": notes,
            "value": value,
            "status": "pending_approval",
        }

    def batch_process(self, clinic_id: str) -> dict:
        """Detect then fix all pending denials for a clinic."""
        # Detect new ones first
        self.detect_recoverable_denials(clinic_id)

        # Load everything still in 'detected' state
        with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id FROM recoverable_denials
                WHERE clinic_id = %s AND status = 'detected'
                ORDER BY billed_amount DESC
                """,
                (clinic_id,),
            )
            rows = cur.fetchall()

        results = []
        for row in rows:
            try:
                result = self.process_denial(str(row["id"]))
                results.append(result)
            except Exception as exc:
                logger.warning("Could not process denial %s: %s", row["id"], exc)
                results.append({"denial_id": str(row["id"]), "status": "error", "error": str(exc)})

        fixed = sum(1 for r in results if r.get("status") == "pending_approval")
        total_value = sum(
            r.get("value", {}).get("estimated_recovery", 0)
            for r in results
            if r.get("status") == "pending_approval"
        )

        logger.info(
            "Batch processed %d denials for clinic %s — %d fixed, $%.2f estimated",
            len(results), clinic_id, fixed, total_value,
        )
        return {
            "clinic_id": clinic_id,
            "total_processed": len(results),
            "fixed": fixed,
            "estimated_recovery": round(total_value, 2),
            "results": results,
        }

    # ------------------------------------------------------------------
    # CO-16: Missing / invalid information
    # ------------------------------------------------------------------

    def _fix_co16(self, claim: dict) -> tuple[list, str]:
        """
        CO-16: identify which required fields are absent and populate them
        from data already in the claim record or the connection's practice data.
        """
        fixes = []
        reasons = []
        payload = claim.get("raw_payload") or {}

        # Check NPI
        if not payload.get("billing_npi") and not payload.get("rendering_npi"):
            npi = self._lookup_provider_npi(claim.get("provider_id"))
            if npi:
                fixes.append({"field": "billing_npi", "value": npi, "source": "provider_lookup"})
                reasons.append("added missing billing NPI")

        # Check prior auth for codes that require it
        cpt_codes = claim.get("cpt_codes") or []
        needs_auth = any(c in AUTH_REQUIRED_CODES for c in cpt_codes)
        if needs_auth and not payload.get("prior_auth_number") and not payload.get("auth_number"):
            auth = self._lookup_prior_auth(claim)
            if auth:
                fixes.append({"field": "prior_auth_number", "value": auth, "source": "auth_lookup"})
                reasons.append("added prior auth number")
            else:
                fixes.append({"field": "prior_auth_number", "value": "REQUEST_NEW_AUTH", "source": "flag"})
                reasons.append("flagged for new auth request")

        # Check diagnosis pointer (ICD-10 → CPT linkage)
        if not payload.get("diagnosis_pointer") and claim.get("icd10_codes"):
            fixes.append({
                "field": "diagnosis_pointer",
                "value": "A",   # Point all lines to primary diagnosis
                "source": "auto",
            })
            reasons.append("set diagnosis pointer to primary ICD-10")

        if not fixes:
            fixes.append({"field": "review", "value": "manual_review_required", "source": "fallback"})
            reasons.append("no auto-fix available — queued for manual review")

        return fixes, "; ".join(reasons)

    # ------------------------------------------------------------------
    # CO-50: Medical necessity / non-covered
    # ------------------------------------------------------------------

    def _fix_co50(self, claim: dict) -> tuple[list, str]:
        """
        CO-50: generate a structured appeal letter asserting medical necessity.

        Pipeline:
          1. V28 engine — validate ICD-10 codes; compute RAF score for context
          2. PolicyEngine2026 — determine NCD/LCD coverage and appeal path
          3. _lookup_clinical_policies — pull supporting policy documents
          4. Render appeal letter with all gathered context
        """
        cpt_codes  = claim.get("cpt_codes") or []
        icd_codes  = claim.get("icd10_codes") or []
        provider   = claim.get("provider_id", "Provider")
        svc_date   = claim.get("service_date") or date.today().isoformat()
        jurisdiction = (claim.get("raw_payload") or {}).get("mac_jurisdiction", "unknown")

        # Step 1: V28 ICD-10 validation + RAF score
        v28 = V28RiskAdjustmentEngine()
        v28_result = v28.validate_codes(icd_codes)
        raf_impact = v28_result.get("raf_impact", {})
        invalid_codes = v28_result.get("invalid_codes", [])

        if invalid_codes:
            logger.info(
                "CO-50 fix: %d ICD-10 code(s) not in V28 mapping for claim %s — "
                "tagging as v28_invalid for reclassification tracking",
                len(invalid_codes),
                claim.get("webpt_claim_id"),
            )

        # Step 2: Policy coverage check (NCD → LCD → SAD)
        primary_cpt = cpt_codes[0] if cpt_codes else ""
        primary_icd = v28_result["valid_codes"][0] if v28_result["valid_codes"] else (icd_codes[0] if icd_codes else "")
        policy_engine = PolicyEngine2026(self._conn)
        coverage = policy_engine.check_coverage(primary_cpt, primary_icd, jurisdiction)

        # Step 3: Knowledge graph policy references
        references = self._lookup_clinical_policies(cpt_codes, icd_codes)

        # Step 4: Render the letter
        letter = self._render_appeal_letter(
            provider=provider,
            service_date=str(svc_date),
            cpt_codes=cpt_codes,
            icd10_codes=icd_codes,
            references=references,
            coverage=coverage,
            raf_impact=raf_impact,
            v28_violations=v28_result.get("hierarchy_violations", []),
        )

        notes_parts = [
            f"Generated medical necessity appeal for CPT {', '.join(cpt_codes)}",
            f"with {len(references)} supporting policy reference(s)",
        ]
        if raf_impact.get("total_raf"):
            notes_parts.append(f"RAF score {raf_impact['total_raf']:.3f} included")
        if coverage.get("appeal_path"):
            notes_parts.append(f"appeal path: {coverage['appeal_path']}")

        fixes = [{
            "field":      "appeal_letter",
            "value":      letter,
            "source":     "generated",
            "references": [r["title"] for r in references],
            "coverage":   coverage,
            "v28_valid_codes":   v28_result["valid_codes"],
            "v28_invalid_codes": [c["code"] for c in invalid_codes],
            "raf_total":         raf_impact.get("total_raf"),
        }]

        # Tag each V28-invalid code as a separate fix entry so we can
        # measure the reclassification rate with:
        #   SELECT COUNT(*) FILTER (
        #       WHERE fixes_applied @> '[{"source":"v28_invalid"}]'
        #   ) FROM recoverable_denials WHERE denial_code = 'CO-50'
        for bad in invalid_codes:
            fixes.append({
                "field":  "icd10_code",
                "code":   bad["code"],
                "source": "v28_invalid",
                "reason": bad.get("reason", "Not mapped in V28"),
                "action": bad.get("action", "Verify documentation supports more specific code"),
            })

        return fixes, "; ".join(notes_parts)

    # ------------------------------------------------------------------
    # CO-97: Bundled services — modifier 59
    # ------------------------------------------------------------------

    def _fix_co97(self, claim: dict) -> tuple[list, str]:
        """
        CO-97: append modifier 59 to each CPT code that falls in the
        bundled PT set, signalling to the payer that each service was
        a distinct and independent procedure.
        """
        cpt_codes = claim.get("cpt_codes") or []
        targeted  = [c for c in cpt_codes if c in BUNDLED_PT_CODES]

        if not targeted:
            # Non-PT bundling — flag for manual modifier review
            fixes = [{"field": "modifier", "value": "REVIEW_MODIFIER", "codes": cpt_codes}]
            return fixes, "Non-PT bundling — queued for manual modifier selection"

        fixes = [
            {"field": "modifier", "value": "59", "code": code, "source": "auto"}
            for code in targeted
        ]
        notes = f"Appended modifier 59 to {', '.join(targeted)} (distinct procedural services)"
        return fixes, notes

    # ------------------------------------------------------------------
    # Approval gate
    # ------------------------------------------------------------------

    def generate_approval_token(self, denial_id: str, ttl_days: int = 7) -> str:
        """
        Create a one-time approval token for a pending_approval denial.
        Returns the raw token string (embed in the email link).
        Idempotent: invalidates any previous unused token for this denial.
        """
        token = secrets.token_urlsafe(32)
        expires_at = datetime.now(timezone.utc) + timedelta(days=ttl_days)

        with self._conn.cursor() as cur:
            # Expire any previous unused tokens for this denial
            cur.execute(
                "UPDATE approval_tokens SET used_at = NOW(), used_action = 'superseded' "
                "WHERE denial_id = %s AND used_at IS NULL",
                (denial_id,),
            )
            cur.execute(
                """
                INSERT INTO approval_tokens (denial_id, token, expires_at)
                VALUES (%s, %s, %s)
                """,
                (denial_id, token, expires_at),
            )
        self._conn.commit()
        return token

    def approve_denial(self, token: str) -> dict:
        """
        Validate token and transition denial to 'approved'.
        Returns the updated denial row or raises ValueError on bad/expired token.
        """
        token_row = self._consume_token(token, "approve")
        denial_id = str(token_row["denial_id"])

        with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                UPDATE recoverable_denials
                SET status = 'approved', approved_at = NOW()
                WHERE id = %s AND status = 'pending_approval'
                RETURNING *
                """,
                (denial_id,),
            )
            row = cur.fetchone()
        self._conn.commit()

        if not row:
            raise ValueError(f"Denial {denial_id} is not in pending_approval state")

        logger.info("Denial %s approved via email link", denial_id)
        return dict(row)

    def reject_denial(self, token: str, reason: str = "") -> dict:
        """
        Validate token and transition denial back to 'rejected'.
        Stores the rejection reason so the fix can be revised.
        """
        token_row = self._consume_token(token, "reject")
        denial_id = str(token_row["denial_id"])

        if reason:
            with self._conn.cursor() as cur:
                cur.execute(
                    "UPDATE approval_tokens SET rejection_reason = %s WHERE denial_id = %s",
                    (reason, denial_id),
                )

        with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                UPDATE recoverable_denials
                SET status = 'rejected', rejection_reason = %s
                WHERE id = %s AND status = 'pending_approval'
                RETURNING *
                """,
                (reason or None, denial_id),
            )
            row = cur.fetchone()
        self._conn.commit()

        if not row:
            raise ValueError(f"Denial {denial_id} is not in pending_approval state")

        logger.info("Denial %s rejected via email link — reason: %s", denial_id, reason or "none")
        return dict(row)

    def submit_denial(self, denial_id: str) -> dict:
        """
        Transition an approved denial to 'submitted' and record the attempt.
        This is where clearinghouse/EDI integration will plug in.
        Returns the resubmission record.
        """
        denial = self._get_denial(denial_id)
        if denial is None:
            raise ValueError(f"Denial {denial_id} not found")
        if denial["status"] != "approved":
            raise ValueError(
                f"Denial {denial_id} is '{denial['status']}' — must be 'approved' to submit"
            )

        # Count prior attempts so we can number this one
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT COALESCE(MAX(attempt_number), 0) FROM denial_resubmissions WHERE denial_id = %s",
                (denial_id,),
            )
            attempt = cur.fetchone()[0] + 1

        resubmission_id = str(uuid.uuid4())
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO denial_resubmissions
                    (id, denial_id, attempt_number, fixes_snapshot, status)
                VALUES (%s, %s, %s, %s, 'pending')
                """,
                (
                    resubmission_id,
                    denial_id,
                    attempt,
                    psycopg2.extras.Json(denial.get("fixes_applied") or []),
                ),
            )
            cur.execute(
                "UPDATE recoverable_denials SET status = 'submitted', submitted_at = NOW() WHERE id = %s",
                (denial_id,),
            )
        self._conn.commit()

        logger.info(
            "Denial %s submitted (attempt #%d) — resubmission_id=%s",
            denial_id, attempt, resubmission_id,
        )
        return {
            "denial_id": denial_id,
            "resubmission_id": resubmission_id,
            "attempt_number": attempt,
            "status": "submitted",
        }

    def _consume_token(self, token: str, action: str) -> dict:
        """
        Look up and consume a token. Raises ValueError if not found, expired, or already used.
        """
        with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM approval_tokens WHERE token = %s",
                (token,),
            )
            row = cur.fetchone()

        if not row:
            raise ValueError("Invalid approval token")
        if row["used_at"] is not None:
            raise ValueError("This approval link has already been used")
        if row["expires_at"] < datetime.now(timezone.utc):
            raise ValueError("This approval link has expired")

        with self._conn.cursor() as cur:
            cur.execute(
                "UPDATE approval_tokens SET used_at = NOW(), used_action = %s WHERE id = %s",
                (action, str(row["id"])),
            )
        self._conn.commit()
        return dict(row)

    # ------------------------------------------------------------------
    # Approval queue helpers
    # ------------------------------------------------------------------

    def get_pending_approval_denials(self, clinic_id: str) -> list[dict]:
        """Return all denials awaiting clinic approval that have no active token."""
        with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT rd.*
                FROM recoverable_denials rd
                WHERE rd.clinic_id = %s
                  AND rd.status = 'pending_approval'
                  AND NOT EXISTS (
                      SELECT 1 FROM approval_tokens at
                      WHERE at.denial_id = rd.id
                        AND at.used_at IS NULL
                        AND at.expires_at > NOW()
                  )
                ORDER BY rd.estimated_recovery DESC
                """,
                (clinic_id,),
            )
            return [dict(r) for r in cur.fetchall()]

    def get_approved_denials(self, clinic_id: str) -> list[dict]:
        """Return all denials approved by the clinic but not yet submitted."""
        with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT * FROM recoverable_denials
                WHERE clinic_id = %s AND status = 'approved'
                ORDER BY approved_at ASC
                """,
                (clinic_id,),
            )
            return [dict(r) for r in cur.fetchall()]

    # ------------------------------------------------------------------
    # Value calculation
    # ------------------------------------------------------------------

    def _calculate_value(self, billed_amount: float, denial_code: str) -> dict:
        rate = SUCCESS_RATES.get(denial_code, 0.5)
        fee_rate = FEE_RATES.get(denial_code, 0.20)
        estimated = float(billed_amount or 0) * rate
        your_fee  = estimated * fee_rate
        clinic_net = estimated - your_fee
        return {
            "billed_amount":       round(float(billed_amount or 0), 2),
            "success_probability": rate,
            "fee_rate":            fee_rate,
            "estimated_recovery":  round(estimated, 2),
            "your_fee":            round(your_fee, 2),
            "clinic_net":          round(clinic_net, 2),
        }

    # ------------------------------------------------------------------
    # Denial code classifier
    # ------------------------------------------------------------------

    def _classify_denial_codes(self, claim: dict) -> list[str]:
        """
        Infer which CO codes apply to a claim based on its data.
        A single claim can trigger multiple codes.
        """
        codes = []
        payload    = claim.get("raw_payload") or {}
        cpt_codes  = claim.get("cpt_codes") or []
        icd_codes  = claim.get("icd10_codes") or []

        # CO-16: missing required info
        missing_npi  = not payload.get("billing_npi") and not payload.get("rendering_npi")
        needs_auth   = any(c in AUTH_REQUIRED_CODES for c in cpt_codes)
        missing_auth = needs_auth and not payload.get("prior_auth_number") and not payload.get("auth_number")
        missing_diag = not icd_codes
        if missing_npi or missing_auth or missing_diag:
            codes.append("CO-16")

        # CO-50: high-risk CPT with missing or V28-invalid ICD-10 codes.
        # Real CO-50 denials usually DO have diagnosis codes — the payer rejects
        # the medical necessity argument.  We trigger on two scenarios:
        #   a) No ICD-10 at all — obvious documentation gap
        #   b) ICD-10 codes present but none map in V28 — coding error masquerading
        #      as a medical necessity denial (the V28 reclassification case)
        HIGH_RISK_CPTS = {"97750", "97755", "97760", "97761", "97763"}
        if any(c in HIGH_RISK_CPTS for c in cpt_codes):
            v28_check = V28RiskAdjustmentEngine()
            v28_mapped = v28_check.load_v28_mapping()
            all_icd_invalid = bool(icd_codes) and all(c not in v28_mapped for c in icd_codes)
            if not icd_codes or all_icd_invalid:
                codes.append("CO-50")

        # CO-97: multiple bundled PT codes billed on the same date without modifier
        bundled = [c for c in cpt_codes if c in BUNDLED_PT_CODES]
        modifiers = payload.get("modifiers") or []
        if len(bundled) >= 2 and "59" not in modifiers and "XS" not in modifiers:
            codes.append("CO-97")

        return codes

    # ------------------------------------------------------------------
    # DB helpers
    # ------------------------------------------------------------------

    def _load_claims(self, clinic_id: str) -> list:
        with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT wc.id, wc.connection_id, wc.webpt_claim_id, wc.patient_id,
                       wc.provider_id, wc.service_date, wc.cpt_codes, wc.icd10_codes,
                       wc.claim_status, wc.amount, wc.raw_payload
                FROM webpt_claims wc
                JOIN webpt_connections conn ON wc.connection_id = conn.id
                WHERE conn.clinic_id = %s
                  AND wc.amount > 0
                ORDER BY wc.service_date DESC
                """,
                (clinic_id,),
            )
            return cur.fetchall()

    def _get_denial(self, denial_id: str) -> Optional[dict]:
        with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM recoverable_denials WHERE id = %s", (denial_id,)
            )
            row = cur.fetchone()
        return dict(row) if row else None

    def _get_claim_by_webpt_id(self, connection_id: str, webpt_claim_id: str) -> dict:
        with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT * FROM webpt_claims
                WHERE connection_id = %s AND webpt_claim_id = %s
                """,
                (connection_id, webpt_claim_id),
            )
            row = cur.fetchone()
        return dict(row) if row else {}

    def _upsert_denial(self, clinic_id: str, claim: dict, denial_code: str) -> Optional[dict]:
        connection_id  = str(claim["connection_id"])
        webpt_claim_id = claim["webpt_claim_id"]
        billed         = float(claim.get("amount") or 0)
        value          = self._calculate_value(billed, denial_code)

        with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO recoverable_denials
                    (connection_id, claim_id, webpt_claim_id, clinic_id,
                     denial_code, billed_amount, estimated_recovery, success_probability)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (connection_id, webpt_claim_id, denial_code) DO NOTHING
                RETURNING *
                """,
                (
                    connection_id,
                    str(claim["id"]) if claim.get("id") else None,
                    webpt_claim_id,
                    clinic_id,
                    denial_code,
                    billed,
                    value["estimated_recovery"],
                    value["success_probability"],
                ),
            )
            row = cur.fetchone()
        self._conn.commit()
        return dict(row) if row else None

    def _lookup_provider_npi(self, provider_id: Optional[str]) -> Optional[str]:
        """Look up NPI from the raw_payload of previous claims for this provider."""
        if not provider_id:
            return None
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT raw_payload->>'billing_npi'
                FROM webpt_claims
                WHERE provider_id = %s
                  AND raw_payload->>'billing_npi' IS NOT NULL
                LIMIT 1
                """,
                (provider_id,),
            )
            row = cur.fetchone()
        return row[0] if row else None

    def _lookup_prior_auth(self, claim: dict) -> Optional[str]:
        """Look for a prior auth number in previous claims for the same patient + codes."""
        patient_id = claim.get("patient_id")
        cpt_codes  = claim.get("cpt_codes") or []
        if not patient_id or not cpt_codes:
            return None
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT raw_payload->>'prior_auth_number'
                FROM webpt_claims
                WHERE patient_id = %s
                  AND cpt_codes && %s
                  AND raw_payload->>'prior_auth_number' IS NOT NULL
                ORDER BY service_date DESC
                LIMIT 1
                """,
                (patient_id, cpt_codes),
            )
            row = cur.fetchone()
        return row[0] if row else None

    def _lookup_clinical_policies(self, cpt_codes: list, icd10_codes: list) -> list[dict]:
        """
        Pull the most relevant LCD / NCD / clinical_policy documents from the
        knowledge graph to support a CO-50 medical necessity appeal.
        """
        if not cpt_codes and not icd10_codes:
            return []
        with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT title, document_type, lcd_id, ncd_id, source_url
                FROM knowledge_documents
                WHERE (cpt_codes && %s OR icd10_codes && %s)
                  AND document_type IN (
                      'lcd', 'ncd', 'clinical_policy',
                      'prior_auth_criteria', 'coverage_determination',
                      'billing_guidelines'
                  )
                  AND is_active = TRUE
                ORDER BY confidence_score DESC
                LIMIT 5
                """,
                (cpt_codes or [], icd10_codes or []),
            )
            return [dict(r) for r in cur.fetchall()]

    def _render_appeal_letter(
        self,
        provider: str,
        service_date: str,
        cpt_codes: list,
        icd10_codes: list,
        references: list,
        coverage: Optional[dict] = None,
        raf_impact: Optional[dict] = None,
        v28_violations: Optional[list] = None,
    ) -> str:
        ref_lines = "\n".join(
            f"  • {r['title']} ({r.get('lcd_id') or r.get('ncd_id') or r['document_type']})"
            for r in references
        ) or "  • Clinical guidelines on file"

        # RAF section — only included when score is meaningful
        raf_section = ""
        if raf_impact and raf_impact.get("total_raf", 0) > 0:
            raf_details = "; ".join(
                f"{d['code']} → {d['hcc']} (weight {d['weight']})"
                for d in raf_impact.get("details", [])
            )
            raf_section = (
                f"\nClinical Complexity (CMS-HCC V28 Risk Adjustment):\n"
                f"The patient's documented conditions carry a combined V28 RAF score of "
                f"{raf_impact['total_raf']:.3f}, reflecting clinically significant comorbidity "
                f"burden that supports the medical necessity of the services billed.\n"
                f"  {raf_details}\n"
            )

        # Appeal path context from policy engine
        appeal_path_section = ""
        if coverage and coverage.get("appeal_path") and not coverage.get("covered", True):
            appeal_path_section = (
                f"\nCoverage Determination Context:\n"
                f"Per the 2026 policy check ({coverage.get('source_title') or coverage.get('source', 'CMS policy')}): "
                f"{coverage.get('reason', '')}. "
                f"Recommended appeal path: {coverage['appeal_path']}\n"
            )

        # V28 hierarchy violation notes (informational — not in letter body)
        v28_note = ""
        if v28_violations:
            v28_note = (
                f"\n[INTERNAL NOTE — V28 hierarchy violations detected: "
                + "; ".join(v["code"] for v in v28_violations)
                + " — verify documentation before submission]\n"
            )

        return (
            f"RE: Medical Necessity Appeal — Services Rendered {service_date}\n\n"
            f"To Whom It May Concern,\n\n"
            f"We are writing to appeal the denial of claim for services rendered by "
            f"{provider} on {service_date}.\n\n"
            f"Procedure codes billed: {', '.join(cpt_codes)}\n"
            f"Diagnosis codes: {', '.join(icd10_codes)}\n\n"
            f"Medical Necessity Justification:\n"
            f"The services rendered were medically necessary and clinically appropriate "
            f"for the documented diagnoses. The treating provider determined that these "
            f"services were required to achieve functional goals and improve the patient's "
            f"condition based on objective clinical findings.\n"
            f"{raf_section}"
            f"{appeal_path_section}"
            f"\nSupporting Policy References:\n"
            f"{ref_lines}\n\n"
            f"We respectfully request reconsideration of this claim and reprocessing "
            f"for payment in accordance with the patient's benefits.\n\n"
            f"Sincerely,\nCodeMed Denial Recovery Team"
            f"{v28_note}"
        )
