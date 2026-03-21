"""
NexusAuth REST API
==================
FastAPI application exposing the NexusAuth knowledge layer.

Endpoints:
  POST /v1/search              — Semantic policy search
  POST /v1/prior-auth/check    — Prior-authorization coverage decision
  GET  /v1/policies            — List ingested policy documents
  GET  /v1/policies/{id}       — Policy detail
  GET  /health                 — Liveness check

Run locally:
  uvicorn api.main:app --reload --port 8000

Environment variables:
  DATABASE_URL     — PostgreSQL connection string
  OPENAI_API_KEY   — Required for /search and /prior-auth/check
"""

import logging
import os
import time

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from api.db import close_pool, init_pool
from api.routes import policies, prior_auth, search

logging.basicConfig(
    level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO")),
    format="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
)
logger = logging.getLogger("nexusauth.api")

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="NexusAuth API",
    description=(
        "Clinical NLP engine for prior-authorization automation. "
        "Search Medicare and commercial payer policies, and get instant "
        "coverage decisions for procedure + diagnosis code combinations."
    ),
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# ---------------------------------------------------------------------------
# CORS — allow the index.html frontend to call the API
# ---------------------------------------------------------------------------

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # Tighten to specific domain in production
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Startup / Shutdown
# ---------------------------------------------------------------------------

@app.on_event("startup")
async def startup():
    init_pool()
    logger.info("NexusAuth API started")


@app.on_event("shutdown")
async def shutdown():
    close_pool()
    logger.info("NexusAuth API stopped")

# ---------------------------------------------------------------------------
# Request timing middleware
# ---------------------------------------------------------------------------

@app.middleware("http")
async def add_timing_header(request: Request, call_next):
    start = time.monotonic()
    response = await call_next(request)
    elapsed_ms = int((time.monotonic() - start) * 1000)
    response.headers["X-Response-Time-Ms"] = str(elapsed_ms)
    return response

# ---------------------------------------------------------------------------
# Global exception handler
# ---------------------------------------------------------------------------

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled exception on %s %s", request.method, request.url.path)
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"},
    )

# ---------------------------------------------------------------------------
# Health check (no auth required)
# ---------------------------------------------------------------------------

@app.get("/health", tags=["System"], summary="Liveness check")
async def health():
    """Returns 200 when the API is running. DB connectivity is not checked here."""
    return {"status": "ok", "version": app.version}


@app.get("/health/db", tags=["System"], summary="DB connectivity check")
async def health_db():
    """Verifies the API can reach the PostgreSQL database."""
    try:
        from api.db import get_conn
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return {"status": "ok", "database": "connected"}
    except Exception as exc:
        return JSONResponse(
            status_code=503,
            content={"status": "error", "database": str(exc)},
        )

# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------

app.include_router(search.router,     prefix="/v1/search",       tags=["Search"])
app.include_router(prior_auth.router, prefix="/v1/prior-auth",   tags=["Prior Authorization"])
app.include_router(policies.router,   prefix="/v1/policies",     tags=["Policies"])
