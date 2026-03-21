"""
NexusAuth API — POST /v1/search
=================================
Semantic search over ingested policy documents using pgvector.
Wraps ingestion/embedder.py::semantic_search().
"""

import logging
import os
from typing import Annotated

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field

from api.auth import TenantDep
from api.db import get_conn

logger = logging.getLogger(__name__)
router = APIRouter()


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class SearchRequest(BaseModel):
    query: str = Field(..., min_length=3, max_length=1000,
                       description="Natural language query, e.g. 'cardiac monitoring prior auth criteria'")
    document_type: str | None = Field(None,
        description="Filter by type: lcd, ncd, prior_auth_criteria, clinical_policy, fee_schedule, formulary, billing_guidelines, coverage_determination")
    payer: str | None = Field(None,
        description="Filter by payer code: CMS, AETNA, UHC, BCBS")
    limit: int = Field(10, ge=1, le=50, description="Max results to return")


class SearchResult(BaseModel):
    knowledge_document_id: str
    title: str | None
    document_type: str | None
    source_url: str | None
    source_domain: str | None
    chunk_text: str
    chunk_index: int
    similarity: float
    specialties: list[str]
    routing_targets: list[str]


class SearchResponse(BaseModel):
    query: str
    results: list[SearchResult]
    total: int


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_payer_id(conn, payer_code: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM payers WHERE payer_code = %s LIMIT 1", (payer_code,))
        row = cur.fetchone()
        return str(row[0]) if row else None


def _openai_client():
    from openai import OpenAI
    return OpenAI()


# ---------------------------------------------------------------------------
# Route
# ---------------------------------------------------------------------------

@router.post("", response_model=SearchResponse, summary="Semantic policy search")
async def search_policies(body: SearchRequest, tenant: TenantDep):
    """
    Search ingested payer policy documents using semantic similarity.

    Returns ranked policy chunks with source metadata and similarity scores.
    Use `document_type` and `payer` to narrow results.
    """
    if not os.environ.get("OPENAI_API_KEY"):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Embedding service not configured — OPENAI_API_KEY missing",
        )

    try:
        client = _openai_client()
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                            detail=f"Embedding client error: {exc}")

    payer_id: str | None = None

    try:
        with get_conn() as conn:
            if body.payer:
                payer_id = _get_payer_id(conn, body.payer.upper())
                if not payer_id:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Unknown payer code '{body.payer}'. Valid: CMS, AETNA, UHC, BCBS",
                    )

            from ingestion.embedder import semantic_search
            raw_results = semantic_search(
                conn,
                query=body.query,
                limit=body.limit,
                document_type=body.document_type,
                payer_id=payer_id,
                client=client,
            )

    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Search failed for tenant %s: %s", tenant["tenant_name"], exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail="Search failed — please try again")

    results = [
        SearchResult(
            knowledge_document_id=str(r["id"]),
            title=r.get("title"),
            document_type=r.get("document_type"),
            source_url=r.get("source_url"),
            source_domain=r.get("source_domain"),
            chunk_text=r.get("chunk_text", ""),
            chunk_index=r.get("chunk_index", 0),
            similarity=round(float(r.get("similarity", 0)), 4),
            specialties=r.get("specialties") or [],
            routing_targets=r.get("routing_targets") or [],
        )
        for r in raw_results
    ]

    logger.info("Search '%s...' → %d results (tenant=%s)",
                body.query[:40], len(results), tenant["tenant_name"])

    return SearchResponse(query=body.query, results=results, total=len(results))
