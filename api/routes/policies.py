"""
NexusAuth API — GET /v1/policies
==================================
Paginated list and detail view of ingested knowledge_documents.
"""

import logging

from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel

from api.auth import TenantDep
from api.db import get_conn

logger = logging.getLogger(__name__)
router = APIRouter()

VALID_DOC_TYPES = {
    "lcd", "ncd", "prior_auth_criteria", "clinical_policy",
    "fee_schedule", "formulary", "billing_guidelines",
    "coverage_determination", "unknown",
}
VALID_PAYERS = {"CMS", "AETNA", "UHC", "BCBS"}


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------

class PolicySummary(BaseModel):
    id: str
    title: str | None
    document_type: str | None
    payer_code: str | None
    source_url: str | None
    source_domain: str | None
    specialties: list[str]
    cpt_codes: list[str]
    icd10_codes: list[str]
    confidence_score: float | None
    requires_review: bool
    effective_date: str | None
    created_at: str


class PoliciesResponse(BaseModel):
    policies: list[PolicySummary]
    total: int
    page: int
    page_size: int


class PolicyDetail(PolicySummary):
    content_text: str | None
    hcpcs_codes: list[str]
    routing_targets: list[str]


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("", response_model=PoliciesResponse, summary="List ingested policies")
async def list_policies(
    tenant: TenantDep,
    document_type: str | None = Query(None, description="Filter by document type"),
    payer: str | None = Query(None, description="Filter by payer code"),
    specialty: str | None = Query(None, description="Filter by medical specialty"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    """
    List ingested payer policy documents with optional filters.
    Results are ordered by confidence score descending.
    """
    if document_type and document_type not in VALID_DOC_TYPES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid document_type. Valid: {sorted(VALID_DOC_TYPES)}",
        )
    if payer and payer.upper() not in VALID_PAYERS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid payer. Valid: {sorted(VALID_PAYERS)}",
        )

    filters = ["kd.is_active = TRUE"]
    params: list = []

    if document_type:
        filters.append("kd.document_type = %s")
        params.append(document_type)
    if payer:
        filters.append("p.payer_code = %s")
        params.append(payer.upper())
    if specialty:
        filters.append("%s = ANY(kd.specialties)")
        params.append(specialty.lower())

    where = "WHERE " + " AND ".join(filters)
    offset = (page - 1) * page_size

    count_sql = f"""
        SELECT COUNT(*)
        FROM knowledge_documents kd
        LEFT JOIN payers p ON p.id = kd.payer_id
        {where}
    """
    list_sql = f"""
        SELECT
            kd.id::TEXT,
            kd.title,
            kd.document_type::TEXT,
            p.payer_code,
            kd.source_url,
            kd.source_domain,
            kd.specialties,
            kd.cpt_codes,
            kd.icd10_codes,
            kd.confidence_score,
            kd.requires_review,
            kd.effective_date::TEXT,
            kd.created_at::TEXT
        FROM knowledge_documents kd
        LEFT JOIN payers p ON p.id = kd.payer_id
        {where}
        ORDER BY kd.confidence_score DESC NULLS LAST
        LIMIT %s OFFSET %s
    """

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(count_sql, params)
                total = cur.fetchone()[0]

                cur.execute(list_sql, params + [page_size, offset])
                cols = [d[0] for d in cur.description]
                rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception as exc:
        logger.exception("list_policies failed: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail="Failed to fetch policies")

    policies = [
        PolicySummary(
            id=r["id"],
            title=r["title"],
            document_type=r["document_type"],
            payer_code=r["payer_code"],
            source_url=r["source_url"],
            source_domain=r["source_domain"],
            specialties=r["specialties"] or [],
            cpt_codes=r["cpt_codes"] or [],
            icd10_codes=r["icd10_codes"] or [],
            confidence_score=r["confidence_score"],
            requires_review=bool(r["requires_review"]),
            effective_date=r["effective_date"],
            created_at=r["created_at"],
        )
        for r in rows
    ]

    return PoliciesResponse(policies=policies, total=total, page=page, page_size=page_size)


@router.get("/{policy_id}", response_model=PolicyDetail, summary="Get policy detail")
async def get_policy(policy_id: str, tenant: TenantDep):
    """Retrieve full content and metadata for a single policy document."""
    sql = """
        SELECT
            kd.id::TEXT,
            kd.title,
            kd.document_type::TEXT,
            p.payer_code,
            kd.source_url,
            kd.source_domain,
            kd.specialties,
            kd.cpt_codes,
            kd.icd10_codes,
            kd.hcpcs_codes,
            kd.routing_targets,
            kd.confidence_score,
            kd.requires_review,
            kd.effective_date::TEXT,
            kd.content_text,
            kd.created_at::TEXT
        FROM knowledge_documents kd
        LEFT JOIN payers p ON p.id = kd.payer_id
        WHERE kd.id = %s::uuid AND kd.is_active = TRUE
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (policy_id,))
                row = cur.fetchone()
    except Exception as exc:
        logger.exception("get_policy failed for id=%s: %s", policy_id, exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail="Failed to fetch policy")

    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Policy {policy_id} not found")

    cols = [d[0] for d in cur.description] if False else [
        "id", "title", "document_type", "payer_code", "source_url", "source_domain",
        "specialties", "cpt_codes", "icd10_codes", "hcpcs_codes", "routing_targets",
        "confidence_score", "requires_review", "effective_date", "content_text", "created_at",
    ]
    r = dict(zip(cols, row))

    return PolicyDetail(
        id=r["id"],
        title=r["title"],
        document_type=r["document_type"],
        payer_code=r["payer_code"],
        source_url=r["source_url"],
        source_domain=r["source_domain"],
        specialties=r["specialties"] or [],
        cpt_codes=r["cpt_codes"] or [],
        icd10_codes=r["icd10_codes"] or [],
        hcpcs_codes=r["hcpcs_codes"] or [],
        routing_targets=r["routing_targets"] or [],
        confidence_score=r["confidence_score"],
        requires_review=bool(r["requires_review"]),
        effective_date=r["effective_date"],
        content_text=r["content_text"],
        created_at=r["created_at"],
    )
