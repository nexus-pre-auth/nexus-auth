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

Revenue model: 20 % of every dollar recovered, zero upfront cost to the clinic.
"""

import logging
import uuid
from datetime import date
from typing import Optional

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FEE_PERCENTAGE = 0.20   # CodeMed takes 20 %

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
                SET status = 'fixed',
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
            "Fixed denial %s (%s) — estimated recovery $%.2f",
            denial_id, code, value["estimated_recovery"],
        )
        return {
            "denial_id": denial_id,
            "denial_code": code,
            "fixes_applied": fixes,
            "fix_notes": notes,
            "value": value,
            "status": "fixed",
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

        fixed = sum(1 for r in results if r.get("status") == "fixed")
        total_value = sum(
            r.get("value", {}).get("estimated_recovery", 0)
            for r in results
            if r.get("status") == "fixed"
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
        CO-50: generate a structured appeal letter asserting medical necessity
        with references to relevant LCDs / NCDs from the knowledge graph.
        """
        cpt_codes  = claim.get("cpt_codes") or []
        icd_codes  = claim.get("icd10_codes") or []
        provider   = claim.get("provider_id", "Provider")
        svc_date   = claim.get("service_date") or date.today().isoformat()

        # Pull relevant LCD / NCD references from knowledge_documents
        references = self._lookup_clinical_policies(cpt_codes, icd_codes)

        letter = self._render_appeal_letter(
            provider=provider,
            service_date=str(svc_date),
            cpt_codes=cpt_codes,
            icd10_codes=icd_codes,
            references=references,
        )

        fixes = [{
            "field": "appeal_letter",
            "value": letter,
            "source": "generated",
            "references": [r["title"] for r in references],
        }]
        notes = (
            f"Generated medical necessity appeal for CPT {', '.join(cpt_codes)} "
            f"with {len(references)} supporting policy reference(s)"
        )
        return fixes, notes

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
    # Value calculation
    # ------------------------------------------------------------------

    def _calculate_value(self, billed_amount: float, denial_code: str) -> dict:
        rate = SUCCESS_RATES.get(denial_code, 0.5)
        estimated = float(billed_amount or 0) * rate
        your_fee  = estimated * FEE_PERCENTAGE
        clinic_net = estimated - your_fee
        return {
            "billed_amount":       round(float(billed_amount or 0), 2),
            "success_probability": rate,
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

        # CO-50: no ICD-10 → CPT medical necessity linkage on high-risk codes
        HIGH_RISK_CPTS = {"97750", "97755", "97760", "97761", "97763"}
        if any(c in HIGH_RISK_CPTS for c in cpt_codes) and not icd_codes:
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
                  AND document_type IN ('lcd', 'ncd', 'clinical_policy', 'prior_auth_criteria')
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
    ) -> str:
        ref_lines = "\n".join(
            f"  • {r['title']} ({r.get('lcd_id') or r.get('ncd_id') or r['document_type']})"
            for r in references
        ) or "  • Clinical guidelines on file"

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
            f"condition based on objective clinical findings.\n\n"
            f"Supporting Policy References:\n"
            f"{ref_lines}\n\n"
            f"We respectfully request reconsideration of this claim and reprocessing "
            f"for payment in accordance with the patient's benefits.\n\n"
            f"Sincerely,\nCodeMed Denial Recovery Team"
        )
