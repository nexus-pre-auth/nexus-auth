"""
WebPT historical claims sync.

Queues a background RQ job that:
  1. Fetches claims from the WebPT Claims API (paginated)
  2. Inserts them into webpt_claims
  3. Updates connection status throughout

The job itself is designed to run inside an RQ worker process.
"""

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import psycopg2
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

WEBPT_API_BASE  = os.getenv("WEBPT_API_BASE", "https://api.webpt.com/v1")
CLAIMS_LOOKBACK_DAYS = int(os.getenv("WEBPT_LOOKBACK_DAYS", "365"))


# ---------------------------------------------------------------------------
# Job entrypoint — called by RQ worker
# ---------------------------------------------------------------------------

def run_historical_sync(connection_id: str, database_url: str) -> dict:
    """
    RQ job: pull historical claims from WebPT and store them.
    Returns a summary dict with counts.
    """
    import psycopg2
    from .token_store import load_tokens
    from .oauth import refresh_access_token

    conn = psycopg2.connect(database_url)
    try:
        _set_sync_status(conn, connection_id, "syncing")

        tokens = load_tokens(conn, connection_id)
        if tokens is None:
            raise RuntimeError(f"No tokens found for connection {connection_id}")

        if tokens.is_expired():
            if not tokens.refresh_token:
                raise RuntimeError("Access token expired and no refresh token available")
            tokens = refresh_access_token(tokens.refresh_token)
            from .token_store import store_tokens
            store_tokens(conn, connection_id, tokens)

        since = datetime.now(tz=timezone.utc) - timedelta(days=CLAIMS_LOOKBACK_DAYS)
        total_synced = _fetch_and_store_claims(conn, connection_id, tokens.access_token, since)

        _set_sync_completed(conn, connection_id, total_synced)
        logger.info("Historical sync complete for %s — %d claims", connection_id, total_synced)
        return {"connection_id": connection_id, "claims_synced": total_synced}

    except Exception as exc:
        logger.exception("Historical sync failed for %s", connection_id)
        _set_sync_error(conn, connection_id, str(exc))
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=30), reraise=True)
def _fetch_page(access_token: str, since: datetime, page: int) -> dict:
    resp = requests.get(
        f"{WEBPT_API_BASE}/claims",
        headers={"Authorization": f"Bearer {access_token}"},
        params={
            "page": page,
            "per_page": 100,
            "date_of_service_after": since.date().isoformat(),
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def _fetch_and_store_claims(
    conn: psycopg2.extensions.connection,
    connection_id: str,
    access_token: str,
    since: datetime,
) -> int:
    total = 0
    page = 1

    while True:
        data = _fetch_page(access_token, since, page)
        claims = data.get("claims") or data.get("data") or []
        if not claims:
            break

        _bulk_insert_claims(conn, connection_id, claims)
        total += len(claims)
        logger.debug("Synced page %d — %d claims (total: %d)", page, len(claims), total)

        # Update running count for progress tracking
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE webpt_connections SET claims_synced = %s WHERE id = %s",
                (total, connection_id),
            )
        conn.commit()

        if not data.get("next_page"):
            break
        page += 1

    return total


def _bulk_insert_claims(
    conn: psycopg2.extensions.connection,
    connection_id: str,
    claims: list,
) -> None:
    if not claims:
        return

    rows = []
    for c in claims:
        rows.append((
            connection_id,
            str(c.get("id") or c.get("claim_id", "")),
            c.get("patient_id"),
            c.get("provider_id"),
            c.get("service_date") or c.get("date_of_service"),
            c.get("cpt_codes") or [],
            c.get("icd10_codes") or c.get("diagnosis_codes") or [],
            c.get("status") or c.get("claim_status"),
            c.get("amount") or c.get("billed_amount"),
            psycopg2.extras.Json(c),
        ))

    import psycopg2.extras
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO webpt_claims
                (connection_id, webpt_claim_id, patient_id, provider_id,
                 service_date, cpt_codes, icd10_codes, claim_status, amount, raw_payload)
            VALUES %s
            ON CONFLICT (connection_id, webpt_claim_id) DO NOTHING
            """,
            rows,
        )
    conn.commit()


def _set_sync_status(conn, connection_id: str, status: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE webpt_connections
            SET sync_started_at = COALESCE(sync_started_at, NOW()),
                status = %s
            WHERE id = %s
            """,
            (status, connection_id),
        )
    conn.commit()


def _set_sync_completed(conn, connection_id: str, claims_synced: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE webpt_connections
            SET status = 'ready',
                sync_completed_at = NOW(),
                claims_synced = %s
            WHERE id = %s
            """,
            (claims_synced, connection_id),
        )
    conn.commit()


def _set_sync_error(conn, connection_id: str, error_message: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE webpt_connections
            SET status = 'error', error_message = %s
            WHERE id = %s
            """,
            (error_message, connection_id),
        )
    conn.commit()


# ---------------------------------------------------------------------------
# Job enqueueing helper
# ---------------------------------------------------------------------------

def enqueue_historical_sync(connection_id: str) -> str:
    """
    Enqueue a historical sync job in Redis Queue.
    Returns the RQ job ID.
    """
    import redis
    from rq import Queue

    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    database_url = os.getenv("DATABASE_URL", "")

    r = redis.from_url(redis_url)
    q = Queue("webpt_sync", connection=r)

    job = q.enqueue(
        run_historical_sync,
        connection_id,
        database_url,
        job_timeout=3600,
        result_ttl=86400,
    )
    logger.info("Enqueued historical sync job %s for connection %s", job.id, connection_id)
    return job.id
