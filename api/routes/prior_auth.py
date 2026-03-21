"""
NexusAuth API — POST /v1/prior-auth/check
==========================================
Prior-authorization coverage decision engine.

Given CPT codes, ICD-10 codes, and a payer, searches the knowledge base
for matching policy documents and returns a coverage decision with
supporting evidence.

Decision logic:
  - Scores each result by: CPT overlap + ICD-10 overlap + semantic similarity
  - Thresholds from taxonomy.yaml: auto_accept ≥ 0.75, review 0.40–0.74
  - Returns the top supporting policy excerpts as evidence
"""

import logging
import os

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field

from api.auth import TenantDep
from api.db import get_conn

logger = logging.getLogger(__name__)
router = APIRouter()

# Coverage decision thresholds (mirrors taxonomy.yaml)
THRESHOLD_LIKELY_COVERED = 0.75
THRESHOLD_REVIEW_REQUIRED = 0.40


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class PriorAuthRequest(BaseModel):
    cpt_codes: list[str] = Field(..., min_length=1,
        description="CPT procedure codes to check, e.g. ['93224', '93225']")
    icd10_codes: list[str] = Field(default_factory=list,
        description="Supporting ICD-10 diagnosis codes, e.g. ['I49.0', 'R00.1']")
    payer: str = Field(...,
        description="Payer code: CMS, AETNA, UHC, BCBS")
    specialty: str | None = Field(None,
        description="Optional medical specialty for narrowing search, e.g. 'cardiology'")


class SupportingPolicy(BaseModel):
    document_id: str
    title: str | None
    document_type: str | None
    source_url: str | None
    excerpt: str
    similarity: float


class PriorAuthResponse(BaseModel):
    decision: str           # likely_covered | review_required | likely_denied
    confidence: float       # 0.0 – 1.0
    payer: str
    cpt_codes: list[str]
    icd10_codes: list[str]
    reasoning: str
    supporting_policies: list[SupportingPolicy]


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def _score_result(result: dict, cpt_set: set[str], icd10_set: set[str]) -> float:
    """
    Combine three signals into a single coverage score:
      - Semantic similarity from pgvector (weight 0.5)
      - CPT code overlap with policy (weight 0.3)
      - ICD-10 code overlap with policy (weight 0.2)
    """
    semantic = float(result.get("similarity", 0))

    policy_cpts = set(result.get("cpt_codes") or [])
    cpt_overlap = len(cpt_set & policy_cpts) / max(len(cpt_set), 1)

    policy_icd = set(result.get("icd10_codes") or [])
    icd_overlap = len(icd10_set & policy_icd) / max(len(icd10_set), 1) if icd10_set else 0.0

    return round(0.5 * semantic + 0.3 * cpt_overlap + 0.2 * icd_overlap, 4)


def _make_decision(score: float) -> tuple[str, str]:
    """Return (decision label, reasoning string) for a composite score."""
    if score >= THRESHOLD_LIKELY_COVERED:
        return (
            "likely_covered",
            f"Strong policy match found (score {score:.2f}). "
            "Procedure codes and diagnosis codes align with coverage criteria. "
            "Prior authorization is likely to be approved — verify clinical documentation.",
        )
    elif score >= THRESHOLD_REVIEW_REQUIRED:
        return (
            "review_required",
            f"Partial policy match found (score {score:.2f}). "
            "Coverage criteria partially align but clinical review is recommended. "
            "Submit with complete documentation to support medical necessity.",
        )
    else:
        return (
            "likely_denied",
            f"Weak or no policy match found (score {score:.2f}). "
            "No coverage criteria strongly align with the requested procedure. "
            "Consider alternative codes or obtain a pre-determination letter.",
        )


# ---------------------------------------------------------------------------
# Route
# ---------------------------------------------------------------------------

@router.post("", response_model=PriorAuthResponse, summary="Check prior-auth coverage")
async def check_prior_auth(body: PriorAuthRequest, tenant: TenantDep):
    """
    Determine the likely prior-authorization outcome for a procedure.

    Searches ingested payer policies for matching coverage criteria,
    scores results against the submitted codes, and returns a decision
    with supporting evidence excerpts.
    """
    if not os.environ.get("OPENAI_API_KEY"):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Embedding service not configured — OPENAI_API_KEY missing",
        )

    payer_code = body.payer.upper()
    cpt_set = {c.strip() for c in body.cpt_codes}
    icd10_set = {c.strip() for c in body.icd10_codes}

    # Build a rich natural-language query from the submitted codes
    query_parts = [f"prior authorization coverage criteria for CPT {', '.join(sorted(cpt_set))}"]
    if icd10_set:
        query_parts.append(f"diagnosis {', '.join(sorted(icd10_set))}")
    if body.specialty:
        query_parts.append(body.specialty)
    query = " ".join(query_parts)

    try:
        from openai import OpenAI
        client = OpenAI()
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                            detail=f"Embedding client error: {exc}")

    try:
        with get_conn() as conn:
            # Resolve payer
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM payers WHERE payer_code = %s LIMIT 1", (payer_code,))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Unknown payer '{payer_code}'. Valid: CMS, AETNA, UHC, BCBS",
                    )
                payer_id = str(row[0])

            # Pull candidate policies via semantic search
            # Search specifically for prior_auth and coverage docs
            from ingestion.embedder import semantic_search
            candidates = semantic_search(
                conn, query=query, limit=20,
                document_type="prior_auth_criteria",
                payer_id=payer_id, client=client,
            )

            # Also search LCDs/NCDs for CMS
            if payer_code == "CMS" and len(candidates) < 5:
                lcd_results = semantic_search(
                    conn, query=query, limit=10,
                    document_type="lcd",
                    payer_id=payer_id, client=client,
                )
                candidates.extend(lcd_results)

            # Enrich with CPT/ICD-10 arrays from knowledge_documents
            if candidates:
                doc_ids = tuple(str(r["id"]) for r in candidates)
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT id, cpt_codes, icd10_codes
                        FROM knowledge_documents
                        WHERE id = ANY(%s::uuid[])
                        """,
                        (list(doc_ids),),
                    )
                    code_map = {str(row[0]): {"cpt_codes": row[1], "icd10_codes": row[2]}
                                for row in cur.fetchall()}
                for r in candidates:
                    codes = code_map.get(str(r["id"]), {})
                    r["cpt_codes"] = codes.get("cpt_codes") or []
                    r["icd10_codes"] = codes.get("icd10_codes") or []

    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Prior-auth check failed for tenant %s: %s", tenant["tenant_name"], exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail="Prior-auth check failed — please try again")

    # Score and rank candidates
    scored = sorted(
        [{"result": r, "score": _score_result(r, cpt_set, icd10_set)} for r in candidates],
        key=lambda x: x["score"],
        reverse=True,
    )

    top_score = scored[0]["score"] if scored else 0.0
    decision, reasoning = _make_decision(top_score)

    supporting = [
        SupportingPolicy(
            document_id=str(s["result"]["id"]),
            title=s["result"].get("title"),
            document_type=s["result"].get("document_type"),
            source_url=s["result"].get("source_url"),
            excerpt=s["result"].get("chunk_text", "")[:500],
            similarity=round(s["score"], 4),
        )
        for s in scored[:5]  # Top 5 supporting policies
    ]

    logger.info(
        "Prior-auth check: payer=%s cpt=%s decision=%s confidence=%.2f (tenant=%s)",
        payer_code, list(cpt_set), decision, top_score, tenant["tenant_name"],
    )

    return PriorAuthResponse(
        decision=decision,
        confidence=top_score,
        payer=payer_code,
        cpt_codes=sorted(cpt_set),
        icd10_codes=sorted(icd10_set),
        reasoning=reasoning,
        supporting_policies=supporting,
    )
