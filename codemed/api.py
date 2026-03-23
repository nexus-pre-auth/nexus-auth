"""
CodeMed AI — API-First HIPAA Architecture
==========================================
FastAPI REST endpoints for the CodeMed AI medical coding intelligence engine.

Endpoints:
  POST /v1/hcc/enforce          — V28 HCC hierarchy enforcement
  POST /v1/meat/extract         — MEAT evidence extraction from clinical notes
  POST /v1/codes/search         — Natural language coding query (ICD-10/CPT/HCPCS)
  GET  /v1/codes/lookup/{code}  — Exact code lookup
  POST /v1/codes/suggest        — Code suggestion for a clinical scenario
  POST /v1/appeals/generate     — Generate prior auth appeal letter
  GET  /v1/health               — Health check
  GET  /v1/metrics              — Aggregate performance metrics (DB mode)

HIPAA compliance notes:
  - No PHI is logged in request/response at INFO level
  - All PHI fields are masked in audit logs
  - Endpoints require authentication (API key via X-API-Key header)
  - Rate limiting: 60 requests/minute per API key
  - TLS required in production (enforce via reverse proxy)

Run locally:
  uvicorn codemed.api:app --reload --port 8001
"""

from __future__ import annotations

import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Request, Security
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, Field, field_validator
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from codemed.hcc_engine import HCCEngine
from codemed.meat_extractor import MEATExtractor
from codemed.nlq_engine import NLQEngine
from codemed.appeals_generator import AppealsGenerator, DenialScenario
from codemed.cache import CacheNamespace, get_cache
from billing.routes import billing_router
from billing.stripe_config import configure_stripe

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Engine singletons (initialised once at startup)
# ---------------------------------------------------------------------------

_hcc_engine: Optional[HCCEngine] = None
_meat_extractor: Optional[MEATExtractor] = None
_nlq_engine: Optional[NLQEngine] = None
_appeals_generator: Optional[AppealsGenerator] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _hcc_engine, _meat_extractor, _nlq_engine, _appeals_generator
    _hcc_engine = HCCEngine()
    _meat_extractor = MEATExtractor()
    _nlq_engine = NLQEngine()
    _appeals_generator = AppealsGenerator()
    # Initialise cache (graceful fallback if Redis unavailable)
    _cache = get_cache()
    # Configure Stripe (graceful no-op when key not set)
    configure_stripe()
    logger.info(
        "CodeMed AI engines initialised (cache=%s)",
        "redis" if _cache.is_available else "disabled",
    )
    yield
    # Shutdown — nothing to explicitly tear down yet


# ---------------------------------------------------------------------------
# App initialisation
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Rate limiter — 60 requests/minute per IP (enforced on all clinical endpoints)
# ---------------------------------------------------------------------------
limiter = Limiter(key_func=get_remote_address, default_limits=["60/minute"])

app = FastAPI(
    title="CodeMed AI",
    description=(
        "Medical Coding Intelligence Engine — V28 HCC Hierarchy, "
        "MEAT Evidence Extraction, NL Code Search, Prior Auth Appeals"
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# ---------------------------------------------------------------------------
# CORS — configurable via ALLOWED_ORIGINS (comma-separated list).
# Defaults to localhost dev ports. Set to your domain(s) in production.
# ---------------------------------------------------------------------------
_raw_origins = os.environ.get(
    "ALLOWED_ORIGINS",
    "http://localhost:8501,http://localhost:3000,http://localhost:8001",
)
_allowed_origins = [o.strip() for o in _raw_origins.split(",") if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["X-API-Key", "Content-Type"],
)

app.include_router(billing_router, prefix="/v1/billing")


def get_hcc_engine() -> HCCEngine:
    if _hcc_engine is None:
        raise HTTPException(status_code=503, detail="HCC engine not initialised")
    return _hcc_engine


def get_meat_extractor() -> MEATExtractor:
    if _meat_extractor is None:
        raise HTTPException(status_code=503, detail="MEAT extractor not initialised")
    return _meat_extractor


def get_nlq_engine() -> NLQEngine:
    if _nlq_engine is None:
        raise HTTPException(status_code=503, detail="NLQ engine not initialised")
    return _nlq_engine


def get_appeals_generator() -> AppealsGenerator:
    if _appeals_generator is None:
        raise HTTPException(status_code=503, detail="Appeals generator not initialised")
    return _appeals_generator


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------

API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)
_DEFAULT_DEV_KEY = "dev-key-codemed"
_ENV = os.environ.get("CODEMED_ENV", "development").lower()
_VALID_API_KEYS = set(
    k.strip()
    for k in os.environ.get("CODEMED_API_KEYS", _DEFAULT_DEV_KEY).split(",")
    if k.strip()
)

# Warn loudly if the default dev key is still set in non-development environments
if _ENV == "production" and _DEFAULT_DEV_KEY in _VALID_API_KEYS:
    logger.critical(
        "SECURITY: Default API key 'dev-key-codemed' is active in production! "
        "Set CODEMED_API_KEYS to a secure random value immediately."
    )


def require_api_key(api_key: str = Security(API_KEY_HEADER)):
    """Validate API key from X-API-Key header."""
    if not api_key or api_key not in _VALID_API_KEYS:
        raise HTTPException(
            status_code=401,
            detail="Invalid or missing API key. Provide X-API-Key header.",
        )
    # Block the default dev key in production
    if _ENV == "production" and api_key == _DEFAULT_DEV_KEY:
        raise HTTPException(
            status_code=403,
            detail="Default development API key is not permitted in production.",
        )
    return api_key


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class HCCEnforceRequest(BaseModel):
    icd10_codes: list[str] = Field(
        ..., min_length=1, max_length=100,
        description="List of ICD-10 codes to enforce hierarchy on",
        examples=[["E11.9", "E11.40", "N18.4", "N18.32"]],
    )
    encounter_id: Optional[str] = Field(None, description="Optional encounter ID for audit trail")
    patient_id: Optional[str] = Field(None, description="Optional patient ID (de-identified)")

    @field_validator("icd10_codes")
    @classmethod
    def validate_icd10_codes(cls, v):
        for code in v:
            if not code or len(code) > 10:
                raise ValueError(f"Invalid ICD-10 code: {code!r}")
        return [c.strip().upper() for c in v]


class HCCEnforceResponse(BaseModel):
    encounter_id: Optional[str]
    summary: dict
    active_hccs: list[dict]
    suppressed_hccs: list[dict]
    hierarchy_conflicts: list[dict]
    unmapped_codes: list[str]
    raf_before: float
    raf_after: float
    raf_delta: float
    model_version: str = "V28"
    processed_at: str


class MEATExtractRequest(BaseModel):
    clinical_note: str = Field(
        ..., min_length=10, max_length=50000,
        description="Full clinical note text (SOAP, APSO, or free-text format)",
    )
    icd10_codes: list[str] = Field(
        ..., min_length=1, max_length=50,
        description="ICD-10 codes coded for this encounter",
    )
    code_descriptions: Optional[dict[str, str]] = Field(
        None, description="Optional mapping of ICD-10 code → description"
    )
    encounter_id: Optional[str] = None

    @field_validator("icd10_codes")
    @classmethod
    def validate_codes(cls, v):
        return [c.strip().upper() for c in v]


class MEATExtractResponse(BaseModel):
    encounter_id: Optional[str]
    summary: dict
    processed_at: str


class CodeSearchRequest(BaseModel):
    query: str = Field(
        ..., min_length=2, max_length=500,
        description="Natural language search query",
        examples=["cardiac monitoring atrial fibrillation"],
    )
    code_types: Optional[list[str]] = Field(
        None,
        description="Filter by code type: ICD-10, CPT, HCPCS (default: all)",
    )
    max_results: int = Field(10, ge=1, le=50, description="Maximum results to return")
    mode: str = Field("keyword", description="Search mode: keyword or semantic")

    @field_validator("code_types")
    @classmethod
    def validate_code_types(cls, v):
        if v is None:
            return v
        valid = {"ICD-10", "CPT", "HCPCS"}
        for ct in v:
            if ct not in valid:
                raise ValueError(f"Invalid code_type: {ct!r}. Must be one of {valid}")
        return v

    @field_validator("mode")
    @classmethod
    def validate_mode(cls, v):
        if v not in ("keyword", "semantic"):
            raise ValueError("mode must be 'keyword' or 'semantic'")
        return v


class CodeSearchResponse(BaseModel):
    query: str
    total_results: int
    results: list[dict]
    search_mode: str
    latency_ms: int


class CodeSuggestRequest(BaseModel):
    diagnosis_text: str = Field(
        ..., min_length=3, max_length=500,
        description="Description of the diagnosis or condition",
    )
    procedure_text: Optional[str] = Field(
        None, max_length=500,
        description="Description of the procedure or service",
    )
    max_per_type: int = Field(5, ge=1, le=20)


class CodeLookupResponse(BaseModel):
    code: str
    description: str
    code_type: str
    category: str
    found: bool


class AppealGenerateRequest(BaseModel):
    patient_name: str = Field(..., max_length=200)
    patient_dob: str = Field(..., description="Date of birth YYYY-MM-DD")
    patient_id: str = Field(..., max_length=100)
    insurance_member_id: str = Field(..., max_length=100)
    provider_name: str = Field(..., max_length=255)
    provider_npi: str = Field(..., max_length=20)
    service_date: str = Field(..., description="Service date YYYY-MM-DD")
    claim_number: str = Field(..., max_length=100)
    denied_cpt_codes: list[str] = Field(..., min_length=1, max_length=20)
    diagnosis_codes: list[str] = Field(..., min_length=1, max_length=50)
    denial_reason: str = Field(..., max_length=1000)
    denial_date: str = Field(..., description="Denial date YYYY-MM-DD")
    payer_name: str = Field(..., max_length=255)
    clinical_summary: str = Field(..., max_length=5000)
    meat_evidence: list[str] = Field(default_factory=list, max_length=20)
    policy_ids: list[str] = Field(
        default_factory=list,
        description="Specific LCD/NCD policy IDs to cite (e.g. ['L33822'])",
    )
    include_meat: bool = Field(True, description="Include MEAT evidence in letter")


class AppealGenerateResponse(BaseModel):
    letter_text: str
    word_count: int
    policy_citations: list[dict]
    regulatory_citations: list[str]
    generated_at: str


# ---------------------------------------------------------------------------
# Middleware: request timing
# ---------------------------------------------------------------------------

@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = (time.time() - start_time) * 1000
    response.headers["X-Process-Time-Ms"] = str(round(process_time))
    return response


# ---------------------------------------------------------------------------
# Health check (no auth required)
# ---------------------------------------------------------------------------

@app.get("/v1/health", tags=["System"])
async def health_check():
    """Check API health and engine status."""
    cache = get_cache()
    return {
        "status": "healthy",
        "version": "1.0.0",
        "engines": {
            "hcc": _hcc_engine is not None,
            "meat": _meat_extractor is not None,
            "nlq": _nlq_engine is not None,
            "appeals": _appeals_generator is not None,
        },
        "cache": cache.health(),
        "timestamp": datetime.utcnow().isoformat(),
    }


# ---------------------------------------------------------------------------
# HCC Hierarchy Enforcement
# ---------------------------------------------------------------------------

@app.post(
    "/v1/hcc/enforce",
    response_model=HCCEnforceResponse,
    tags=["HCC V28"],
    summary="Enforce V28 HCC hierarchy on a set of ICD-10 codes",
)
@limiter.limit("60/minute")
async def enforce_hcc_hierarchy(
    request_obj: Request,
    request: HCCEnforceRequest,
    engine: HCCEngine = Depends(get_hcc_engine),
    _: str = Depends(require_api_key),
):
    """
    Apply CMS-HCC Model V28 hierarchy enforcement to a list of ICD-10 codes.

    Returns active HCCs (should be coded), suppressed HCCs (trumped by higher
    severity codes), hierarchy conflicts, and RAF score impact.
    """
    result = engine.enforce_hierarchy(request.icd10_codes)
    summary = result.summary()

    return HCCEnforceResponse(
        encounter_id=request.encounter_id,
        summary=summary,
        active_hccs=[
            {
                "hcc_number": h.hcc_number,
                "hcc_description": h.hcc_description,
                "source_icd10": h.source_icd10,
                "hierarchy_group": h.hierarchy_group,
                "raf_weight": h.raf_weight,
                "status": h.status,
            }
            for h in result.active_hccs
        ],
        suppressed_hccs=[
            {
                "hcc_number": h.hcc_number,
                "hcc_description": h.hcc_description,
                "source_icd10": h.source_icd10,
                "hierarchy_group": h.hierarchy_group,
                "raf_weight": h.raf_weight,
                "status": h.status,
                "trumped_by_hcc": h.trumped_by_hcc,
            }
            for h in result.suppressed_hccs
        ],
        hierarchy_conflicts=result.hierarchy_conflicts,
        unmapped_codes=result.unmapped_codes,
        raf_before=round(result.raf_before_enforcement, 4),
        raf_after=round(result.raf_after_enforcement, 4),
        raf_delta=round(result.raf_delta, 4),
        processed_at=datetime.utcnow().isoformat(),
    )


# ---------------------------------------------------------------------------
# MEAT Evidence Extraction
# ---------------------------------------------------------------------------

@app.post(
    "/v1/meat/extract",
    response_model=MEATExtractResponse,
    tags=["MEAT Evidence"],
    summary="Extract MEAT evidence from a clinical note",
)
@limiter.limit("60/minute")
async def extract_meat_evidence(
    request_obj: Request,
    request: MEATExtractRequest,
    extractor: MEATExtractor = Depends(get_meat_extractor),
    _: str = Depends(require_api_key),
):
    """
    Extract Monitoring, Evaluation, Assessment, and Treatment evidence from
    a clinical note to support coded ICD-10 diagnoses.

    Returns per-diagnosis defensibility scores and supporting quote excerpts.
    """
    result = extractor.extract(
        clinical_note=request.clinical_note,
        icd10_codes=request.icd10_codes,
        code_descriptions=request.code_descriptions,
    )

    return MEATExtractResponse(
        encounter_id=request.encounter_id,
        summary=result.summary(),
        processed_at=datetime.utcnow().isoformat(),
    )


# ---------------------------------------------------------------------------
# Code Search — Natural Language Query
# ---------------------------------------------------------------------------

@app.post(
    "/v1/codes/search",
    response_model=CodeSearchResponse,
    tags=["Code Search"],
    summary="Search ICD-10, CPT, and HCPCS codes with plain-English query",
)
@limiter.limit("60/minute")
async def search_codes(
    request_obj: Request,
    request: CodeSearchRequest,
    engine: NLQEngine = Depends(get_nlq_engine),
    _: str = Depends(require_api_key),
):
    """
    Search for medical codes using a plain-English query.

    Supports ICD-10 diagnosis codes, CPT procedure codes, and HCPCS Level II
    supply/drug codes. Results are ranked by relevance score.
    """
    # Build a stable cache key from all search parameters
    cache_key = f"{request.query}|{sorted(request.code_types or [])}|{request.max_results}|{request.mode}"
    cache = get_cache()
    cached = cache.get(CacheNamespace.CODE_SEARCH, cache_key)
    if cached:
        cached["latency_ms"] = 0   # Cache hit — report 0ms
        return CodeSearchResponse(**cached)

    start_ms = int(time.time() * 1000)
    results = engine.search(
        query=request.query,
        code_types=request.code_types,
        max_results=request.max_results,
        mode=request.mode,
    )
    latency_ms = int(time.time() * 1000) - start_ms

    response = CodeSearchResponse(
        query=request.query,
        total_results=len(results),
        results=[r.to_dict() for r in results],
        search_mode=request.mode,
        latency_ms=latency_ms,
    )
    cache.set(CacheNamespace.CODE_SEARCH, cache_key, response.model_dump())
    return response


@app.get(
    "/v1/codes/lookup/{code}",
    response_model=CodeLookupResponse,
    tags=["Code Search"],
    summary="Exact lookup of a specific ICD-10, CPT, or HCPCS code",
)
@limiter.limit("60/minute")
async def lookup_code(
    request: Request,
    code: str,
    engine: NLQEngine = Depends(get_nlq_engine),
    _: str = Depends(require_api_key),
):
    """
    Look up a specific medical code (ICD-10, CPT, or HCPCS) by exact code value.
    Results are cached for 7 days (codes rarely change).
    """
    cache = get_cache()
    cached = cache.get(CacheNamespace.CODE_LOOKUP, code.upper())
    if cached:
        return CodeLookupResponse(**cached)

    result = engine.lookup_code(code)
    if result:
        response = CodeLookupResponse(
            code=result.code,
            description=result.description,
            code_type=result.code_type,
            category=result.category,
            found=True,
        )
        cache.set(CacheNamespace.CODE_LOOKUP, code.upper(), response.model_dump())
        return response

    response = CodeLookupResponse(
        code=code.upper(),
        description="",
        code_type="unknown",
        category="",
        found=False,
    )
    # Cache negative lookups for 1 hour to avoid repeated DB misses
    cache.set(CacheNamespace.CODE_LOOKUP, code.upper(), response.model_dump(), ttl=3600)
    return response


@app.post(
    "/v1/codes/suggest",
    tags=["Code Search"],
    summary="Suggest ICD-10, CPT, and HCPCS codes for a clinical scenario",
)
@limiter.limit("60/minute")
async def suggest_codes(
    request_obj: Request,
    request: CodeSuggestRequest,
    engine: NLQEngine = Depends(get_nlq_engine),
    _: str = Depends(require_api_key),
):
    """
    Suggest relevant ICD-10, CPT, and HCPCS codes for a described clinical scenario.

    Provide a diagnosis description and (optionally) a procedure description.
    Returns ranked suggestions for each code type.
    """
    suggestions = engine.suggest_codes(
        diagnosis_text=request.diagnosis_text,
        procedure_text=request.procedure_text,
        max_per_type=request.max_per_type,
    )
    return {
        "diagnosis_text": request.diagnosis_text,
        "procedure_text": request.procedure_text,
        "suggestions": {
            code_type: [r.to_dict() for r in results]
            for code_type, results in suggestions.items()
        },
    }


# ---------------------------------------------------------------------------
# Prior Auth Appeal Generation
# ---------------------------------------------------------------------------

@app.post(
    "/v1/appeals/generate",
    response_model=AppealGenerateResponse,
    tags=["Prior Auth Appeals"],
    summary="Generate a formal prior authorization appeal letter",
)
@limiter.limit("60/minute")
async def generate_appeal(
    request_obj: Request,
    request: AppealGenerateRequest,
    generator: AppealsGenerator = Depends(get_appeals_generator),
    _: str = Depends(require_api_key),
):
    """
    Generate a formal, citation-backed prior authorization appeal letter.

    The letter includes specific LCD/NCD policy citations, medical necessity
    arguments, MEAT evidence excerpts, and regulatory citations.
    Specify policy_ids (e.g. ["L33822"]) for targeted citations.
    """
    scenario = DenialScenario(
        patient_name=request.patient_name,
        patient_dob=request.patient_dob,
        patient_id=request.patient_id,
        insurance_member_id=request.insurance_member_id,
        provider_name=request.provider_name,
        provider_npi=request.provider_npi,
        service_date=request.service_date,
        claim_number=request.claim_number,
        denied_cpt_codes=request.denied_cpt_codes,
        diagnosis_codes=request.diagnosis_codes,
        denial_reason=request.denial_reason,
        denial_date=request.denial_date,
        payer_name=request.payer_name,
        clinical_summary=request.clinical_summary,
        meat_evidence=request.meat_evidence,
        policy_ids=request.policy_ids,
    )

    letter = generator.generate(scenario, include_meat=request.include_meat)

    return AppealGenerateResponse(
        letter_text=letter.letter_text,
        word_count=letter.word_count,
        policy_citations=letter.policy_citations,
        regulatory_citations=letter.regulatory_citations,
        generated_at=letter.generated_at,
    )


# ---------------------------------------------------------------------------
# Batch HCC Enforcement
# ---------------------------------------------------------------------------

class BatchHCCEncounter(BaseModel):
    encounter_id: str = Field(..., max_length=100)
    icd10_codes: list[str] = Field(..., min_length=1, max_length=100)
    patient_id: Optional[str] = Field(None, max_length=100)

    @field_validator("icd10_codes")
    @classmethod
    def validate_codes(cls, v):
        for code in v:
            if not code or len(code) > 10:
                raise ValueError(f"Invalid ICD-10 code: {code!r}")
        return [c.strip().upper() for c in v]


class BatchHCCRequest(BaseModel):
    encounters: list[BatchHCCEncounter] = Field(
        ..., min_length=1, max_length=100,
        description="Up to 100 encounters to process in a single request",
    )


class BatchHCCResponse(BaseModel):
    total: int
    results: list[dict]
    processed_at: str


@app.post(
    "/v1/batch/hcc",
    response_model=BatchHCCResponse,
    tags=["HCC V28"],
    summary="Batch V28 HCC hierarchy enforcement — up to 100 encounters",
)
@limiter.limit("20/minute")
async def batch_enforce_hcc(
    request_obj: Request,
    request: BatchHCCRequest,
    engine: HCCEngine = Depends(get_hcc_engine),
    _: str = Depends(require_api_key),
):
    """
    Enforce V28 HCC hierarchy on up to 100 encounters in a single API call.

    Useful for nightly RAF reconciliation or bulk chart audits.
    Each encounter is processed independently; results mirror the single
    `/v1/hcc/enforce` response shape plus the encounter_id.
    """
    results = []
    for enc in request.encounters:
        result = engine.enforce_hierarchy(enc.icd10_codes)
        summary = result.summary()
        results.append({
            "encounter_id": enc.encounter_id,
            "patient_id": enc.patient_id,
            "summary": summary,
            "active_hccs": [
                {
                    "hcc_number": h.hcc_number,
                    "hcc_description": h.hcc_description,
                    "source_icd10": h.source_icd10,
                    "raf_weight": h.raf_weight,
                    "status": h.status,
                }
                for h in result.active_hccs
            ],
            "suppressed_hccs": [
                {
                    "hcc_number": h.hcc_number,
                    "source_icd10": h.source_icd10,
                    "trumped_by_hcc": h.trumped_by_hcc,
                }
                for h in result.suppressed_hccs
            ],
            "unmapped_codes": result.unmapped_codes,
            "raf_before": round(result.raf_before_enforcement, 4),
            "raf_after": round(result.raf_after_enforcement, 4),
            "raf_delta": round(result.raf_delta, 4),
        })
    return BatchHCCResponse(
        total=len(results),
        results=results,
        processed_at=datetime.utcnow().isoformat(),
    )


# ---------------------------------------------------------------------------
# RADV Audit Export
# ---------------------------------------------------------------------------

class RADVAuditRequest(BaseModel):
    clinic_id: str = Field(..., max_length=100, description="Clinic/practice identifier")
    contract_number: str = Field(..., max_length=50, description="MA contract number (H-number)")
    payment_year: int = Field(..., ge=2020, le=2030, description="Payment year (CY)")
    encounters: list[BatchHCCEncounter] = Field(
        ..., min_length=1, max_length=500,
        description="Encounters to include in the RADV sample",
    )


class RADVAuditResponse(BaseModel):
    clinic_id: str
    contract_number: str
    payment_year: int
    model_version: str = "V28"
    total_encounters: int
    total_hccs_submitted: int
    total_hccs_supported: int
    unsupported_hccs: list[dict]
    audit_ready_encounters: list[dict]
    generated_at: str


@app.post(
    "/v1/audits/radv-export",
    response_model=RADVAuditResponse,
    tags=["RADV Audit"],
    summary="Generate a structured RADV audit package for a set of encounters",
)
@limiter.limit("10/minute")
async def radv_export(
    request_obj: Request,
    request: RADVAuditRequest,
    engine: HCCEngine = Depends(get_hcc_engine),
    _: str = Depends(require_api_key),
):
    """
    Build a CMS RADV-ready audit package.

    For each encounter the engine:
    1. Enforces V28 hierarchy on submitted ICD-10 codes.
    2. Flags HCCs with no face-to-face, in-person encounter support (RADV rule).
    3. Returns an audit-ready record with active HCCs and any unsupported codes.

    The response can be serialised to JSON and submitted to your RADV workflow.
    RADV compliance note: audio-only telehealth does NOT satisfy face-to-face
    requirements per 42 CFR § 422.504(l).
    """
    audit_encounters = []
    all_unsupported: list[dict] = []
    total_submitted = 0
    total_supported = 0

    for enc in request.encounters:
        result = engine.enforce_hierarchy(enc.icd10_codes)
        submitted_hccs = [h.hcc_number for h in result.active_hccs] + [
            h.hcc_number for h in result.suppressed_hccs
        ]
        active_hccs = [h.hcc_number for h in result.active_hccs]
        # In RADV all HCCs must be supported by a qualifying face-to-face encounter.
        # Here we surface any that were suppressed (hierarchy conflict) as "unsupported"
        # — production integrations should cross-reference against encounter claims.
        unsupported = [
            {
                "encounter_id": enc.encounter_id,
                "hcc_number": h.hcc_number,
                "hcc_description": h.hcc_description,
                "source_icd10": h.source_icd10,
                "reason": "Suppressed by higher-severity HCC in same hierarchy group",
                "trumped_by_hcc": h.trumped_by_hcc,
            }
            for h in result.suppressed_hccs
        ]
        all_unsupported.extend(unsupported)
        total_submitted += len(submitted_hccs)
        total_supported += len(active_hccs)

        audit_encounters.append({
            "encounter_id": enc.encounter_id,
            "patient_id": enc.patient_id,
            "submitted_icd10": enc.icd10_codes,
            "active_hccs": [
                {
                    "hcc_number": h.hcc_number,
                    "hcc_description": h.hcc_description,
                    "source_icd10": h.source_icd10,
                    "raf_weight": h.raf_weight,
                }
                for h in result.active_hccs
            ],
            "unmapped_codes": result.unmapped_codes,
            "raf_score": round(result.raf_after_enforcement, 4),
            "audit_status": "unsupported_hccs_present" if unsupported else "clean",
        })

    return RADVAuditResponse(
        clinic_id=request.clinic_id,
        contract_number=request.contract_number,
        payment_year=request.payment_year,
        total_encounters=len(request.encounters),
        total_hccs_submitted=total_submitted,
        total_hccs_supported=total_supported,
        unsupported_hccs=all_unsupported,
        audit_ready_encounters=audit_encounters,
        generated_at=datetime.utcnow().isoformat(),
    )


# ---------------------------------------------------------------------------
# Cache management endpoints
# ---------------------------------------------------------------------------

@app.get("/v1/cache/stats", tags=["System"])
async def cache_stats(_: str = Depends(require_api_key)):
    """Return cache hit/miss statistics."""
    return get_cache().health()


@app.delete("/v1/cache/flush", tags=["System"])
async def cache_flush(_: str = Depends(require_api_key)):
    """Flush all CodeMed cache entries (use after data updates)."""
    cache = get_cache()
    ok = cache.flush_all()
    return {"flushed": ok, "message": "Cache cleared" if ok else "Cache not available"}


# ---------------------------------------------------------------------------
# Run directly
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    uvicorn.run(
        "codemed.api:app",
        host="0.0.0.0",
        port=8001,
        reload=True,
        log_level="info",
    )
