"""
NexusAuth Policy Engine — 2026 CMS Coverage Architecture
=========================================================
Integrates 2026 CMS policy updates into prior auth and medical necessity
decisions. Understands the three-tier CMS coverage hierarchy:

  NCD  National Coverage Determination  — applies in all states
  LCD  Local Coverage Determination     — MAC jurisdiction-specific
  Articles (billing & coding)           — which ICD-10/CPT codes are covered

Also enforces the Self-Administered Drug (SAD) exclusion list for Part B.

Used by the denial recovery engine to:
  1. Determine the correct appeal path for CO-50 denials
  2. Check whether a diagnosis code is in the LCD's approved list
  3. Block CO-50 appeal generation when NCD categorically excludes the service
"""

import logging
from typing import Optional

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

# CPT prefixes that identify drug administration codes (HCPCS J-codes + CPT vaccine codes)
_DRUG_CODE_PREFIXES = ("J",)
_DRUG_CPT_RANGES = ((90281, 90399), (90460, 90474))  # Immunization admin ranges


class PolicyEngine2026:
    """
    Checks 2026 CMS coverage rules before generating a CO-50 appeal.
    Requires a live DB connection to query knowledge_documents.
    """

    def __init__(self, db_conn: psycopg2.extensions.connection):
        self._conn = db_conn
        self._sad_list: Optional[set] = None   # Lazy-loaded

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def check_coverage(
        self,
        procedure_code: str,
        diagnosis_code: str,
        jurisdiction: str,
    ) -> dict:
        """
        Check if a procedure is covered under 2026 rules.
        Returns a coverage result with denial reason and appeal path if denied.

        Evaluation order:
          1. NCD (national — hard stop if categorically excluded)
          2. LCD (local — check approved diagnosis list)
          3. SAD exclusion list (drugs only)
        """
        if not procedure_code:
            return {"covered": True, "confidence": "low", "notes": "No procedure code provided"}

        # Step 1: NCD check
        ncd_result = self.query_ncd(procedure_code, diagnosis_code)
        if ncd_result and not ncd_result.get("covered", True):
            return {
                "covered":      False,
                "reason":       f"NCD denial: {ncd_result.get('rationale', 'Non-covered service')}",
                "source":       ncd_result.get("ncd_id"),
                "source_title": ncd_result.get("title"),
                "appeal_path":  "National policy — appeal requires new clinical evidence or alternative diagnosis",
            }

        # Step 2: LCD check
        lcd_result = self.query_lcd(procedure_code, diagnosis_code, jurisdiction)
        if lcd_result:
            approved = self.get_lcd_code_list(lcd_result["lcd_id"])
            if diagnosis_code and approved and diagnosis_code not in approved:
                return {
                    "covered":          False,
                    "reason":           f"Diagnosis {diagnosis_code} not in LCD approved list",
                    "source":           lcd_result["lcd_id"],
                    "source_title":     lcd_result.get("title"),
                    "billing_article":  lcd_result.get("billing_article"),
                    "approved_codes":   sorted(approved)[:10],   # Sample for appeal context
                    "appeal_path": (
                        "Clinical justification with peer-reviewed evidence; "
                        "document that clinical presentation meets LCD criteria "
                        "even if primary ICD-10 is not in standard approved list"
                    ),
                }

        # Step 3: SAD exclusion (drugs only)
        if self.is_drug(procedure_code):
            sad = self._get_sad_list()
            if procedure_code in sad:
                return {
                    "covered":     False,
                    "reason":      "Drug on Self-Administered Drug exclusion list",
                    "source":      "SAD List 2026",
                    "appeal_path": (
                        "Document why patient cannot self-administer "
                        "(e.g. requires clinical supervision, injection technique)"
                    ),
                }

        return {
            "covered":    True,
            "confidence": "high" if ncd_result else "medium",
            "notes":      "Coverage confirmed per 2026 NCD/LCD policies",
        }

    def query_ncd(
        self, procedure_code: str, diagnosis_code: Optional[str]
    ) -> Optional[dict]:
        """
        Look up an active NCD for this procedure code from knowledge_documents.
        Returns the most relevant NCD row or None if no NCD governs the service.
        """
        cpt_filter = [procedure_code] if procedure_code else []
        icd_filter = [diagnosis_code] if diagnosis_code else []

        with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    title,
                    ncd_id,
                    content_text,
                    content_summary,
                    metadata
                FROM knowledge_documents
                WHERE document_type = 'ncd'
                  AND is_active = TRUE
                  AND (
                      (cpt_codes && %s AND array_length(%s::text[], 1) > 0)
                      OR (icd10_codes && %s AND array_length(%s::text[], 1) > 0)
                  )
                ORDER BY confidence_score DESC
                LIMIT 1
                """,
                (cpt_filter, cpt_filter, icd_filter, icd_filter),
            )
            row = cur.fetchone()

        if row is None:
            return None

        result = dict(row)
        # Parse coverage determination from metadata if stored there
        meta = result.get("metadata") or {}
        covered = meta.get("covered", True)
        result["covered"] = covered
        result["rationale"] = meta.get("rationale") or result.get("content_summary", "")
        return result

    def query_lcd(
        self, procedure_code: str, diagnosis_code: Optional[str], jurisdiction: str
    ) -> Optional[dict]:
        """
        Look up the most relevant active LCD for procedure + jurisdiction.
        Returns the LCD row including billing article reference, or None.
        """
        cpt_filter = [procedure_code] if procedure_code else []

        with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    title,
                    lcd_id,
                    content_text,
                    content_summary,
                    icd10_codes      AS approved_icd10,
                    metadata,
                    metadata->>'billing_article' AS billing_article
                FROM knowledge_documents
                WHERE document_type = 'lcd'
                  AND is_active = TRUE
                  AND (cpt_codes && %s OR array_length(%s::text[], 1) = 0)
                  AND (
                      metadata->>'jurisdiction' = %s
                      OR metadata->>'jurisdiction' IS NULL
                  )
                ORDER BY
                    CASE WHEN metadata->>'jurisdiction' = %s THEN 0 ELSE 1 END,
                    confidence_score DESC
                LIMIT 1
                """,
                (cpt_filter, cpt_filter, jurisdiction, jurisdiction),
            )
            row = cur.fetchone()

        return dict(row) if row else None

    def get_lcd_code_list(self, lcd_id: str) -> set:
        """
        Return the set of ICD-10 codes approved under a given LCD.
        Checks both the icd10_codes array and the billing article metadata.
        """
        with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT icd10_codes, metadata
                FROM knowledge_documents
                WHERE lcd_id = %s AND is_active = TRUE
                LIMIT 1
                """,
                (lcd_id,),
            )
            row = cur.fetchone()

        if not row:
            return set()

        codes: set[str] = set(row.get("icd10_codes") or [])

        # Also check billing article expanded code list in metadata
        meta = row.get("metadata") or {}
        article_codes = meta.get("approved_icd10_codes") or []
        codes.update(article_codes)

        return codes

    @staticmethod
    def is_drug(procedure_code: str) -> bool:
        """Return True if the procedure code represents a drug/biologic administration."""
        if not procedure_code:
            return False
        # HCPCS J-codes are always drugs
        if procedure_code.upper().startswith(_DRUG_CODE_PREFIXES):
            return True
        # CPT immunization admin ranges
        try:
            code_int = int(procedure_code)
            return any(lo <= code_int <= hi for lo, hi in _DRUG_CPT_RANGES)
        except ValueError:
            return False

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_sad_list(self) -> set:
        """
        Lazy-load the Self-Administered Drug exclusion list from knowledge_documents.
        Falls back to an empty set if no SAD document is loaded.
        """
        if self._sad_list is not None:
            return self._sad_list

        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT metadata->'sad_codes'
                FROM knowledge_documents
                WHERE document_type = 'coverage_determination'
                  AND metadata->>'document_subtype' = 'sad_exclusion_list'
                  AND is_active = TRUE
                ORDER BY effective_date DESC
                LIMIT 1
                """,
            )
            row = cur.fetchone()

        if row and row[0]:
            self._sad_list = set(row[0])
        else:
            logger.debug("No SAD exclusion list found in knowledge_documents")
            self._sad_list = set()

        return self._sad_list
