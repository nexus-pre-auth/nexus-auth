"""
NexusAuth API — API Key Authentication
========================================
Validates X-API-Key header against the api_keys table.
Records usage for rate limiting and billing.
"""

import hashlib
import logging
import time
from typing import Annotated

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import APIKeyHeader

from api.db import get_conn

logger = logging.getLogger(__name__)

API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)


def _hash_key(raw_key: str) -> str:
    return hashlib.sha256(raw_key.encode()).hexdigest()


def _lookup_tenant(key_hash: str) -> dict | None:
    """Return tenant row for a valid, active key — or None."""
    sql = """
        SELECT
            t.id        AS tenant_id,
            t.name      AS tenant_name,
            t.plan_tier,
            t.monthly_limit,
            t.is_active AS tenant_active,
            k.id        AS key_id,
            k.is_active AS key_active,
            k.expires_at
        FROM api_keys k
        JOIN tenants t ON t.id = k.tenant_id
        WHERE k.key_hash = %s
        LIMIT 1
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (key_hash,))
            row = cur.fetchone()
            if not row:
                return None
            cols = [d[0] for d in cur.description]
            return dict(zip(cols, row))


def _record_usage(tenant_id: str, key_id: str, endpoint: str, method: str,
                  status_code: int, response_ms: int) -> None:
    sql = """
        INSERT INTO api_usage (tenant_id, api_key_id, endpoint, method, status_code, response_ms)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (tenant_id, key_id, endpoint, method, status_code, response_ms))
    except Exception as exc:
        logger.warning("Failed to record API usage: %s", exc)


def _update_last_used(key_id: str) -> None:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE api_keys SET last_used_at = NOW() WHERE id = %s",
                    (key_id,)
                )
    except Exception as exc:
        logger.warning("Failed to update last_used_at: %s", exc)


async def require_api_key(
    request: Request,
    raw_key: Annotated[str | None, Depends(API_KEY_HEADER)],
) -> dict:
    """
    FastAPI dependency — validates API key and returns tenant context.
    Raises 401 if missing or invalid, 403 if tenant/key is inactive.
    """
    if not raw_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing X-API-Key header",
        )

    tenant = _lookup_tenant(_hash_key(raw_key))

    if not tenant:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
        )

    if not tenant["tenant_active"] or not tenant["key_active"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="API key or tenant is inactive",
        )

    # Store start time for usage recording middleware
    request.state.tenant = tenant
    request.state.request_start = time.monotonic()
    return tenant


# Shorthand type alias for route dependencies
TenantDep = Annotated[dict, Depends(require_api_key)]
