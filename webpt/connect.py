"""
WebPT Connection Orchestrator.

Coordinates the full "Connect WebPT" flow shown in the sequence diagram:

  1.  build_oauth_redirect()   → URL to send the user to WebPT
  2.  handle_oauth_callback()  → receives code, exchanges tokens,
                                  stores encrypted tokens,
                                  queues historical sync,
                                  registers webhook
  3.  get_connection_status()  → returns current status for polling
  4.  finalize_after_sync()    → called when sync job completes;
                                  runs pattern detection on the
                                  Intelligence Graph
"""

import logging
import os
import uuid
from typing import Optional

import psycopg2
import psycopg2.extras

from .oauth import build_authorization_url, exchange_code_for_tokens, generate_state
from .token_store import store_tokens
from .sync import enqueue_historical_sync
from .webhooks import register_webhook
from .intelligence_graph import run_pattern_detection

logger = logging.getLogger(__name__)


class WebPTConnector:
    """High-level facade for the WebPT OAuth integration."""

    def __init__(self, database_url: Optional[str] = None):
        self._database_url = database_url or os.environ["DATABASE_URL"]

    # ------------------------------------------------------------------
    # Step 1: initiate OAuth
    # ------------------------------------------------------------------

    def build_oauth_redirect(self, clinic_id: str) -> tuple[str, str]:
        """
        Create (or reset) a pending connection record, generate a CSRF state,
        and return (authorization_url, state).

        The caller should:
          - store `state` in the user's session
          - redirect the user's browser to `authorization_url`
        """
        state = generate_state()
        conn = self._db()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO webpt_connections (clinic_id, oauth_state, status)
                    VALUES (%s, %s, 'pending')
                    ON CONFLICT (clinic_id)
                        WHERE status != 'disconnected'
                    DO UPDATE SET
                        oauth_state = EXCLUDED.oauth_state,
                        status = 'pending',
                        error_message = NULL
                    RETURNING id
                    """,
                    (clinic_id, state),
                )
                connection_id = cur.fetchone()[0]
            conn.commit()
        finally:
            conn.close()

        authorization_url = build_authorization_url(state)
        logger.info(
            "OAuth redirect built for clinic %s (connection %s)", clinic_id, connection_id
        )
        return authorization_url, state

    # ------------------------------------------------------------------
    # Step 2: handle callback from WebPT
    # ------------------------------------------------------------------

    def handle_oauth_callback(
        self,
        code: str,
        state: str,
        practice_id: Optional[str] = None,
    ) -> dict:
        """
        Process the OAuth callback.

        1. Validate state → look up connection
        2. Exchange code for tokens
        3. Store encrypted tokens
        4. Queue historical sync
        5. Register webhook
        6. Return status payload: {"connection_id": ..., "status": "connected"}
        """
        conn = self._db()
        try:
            connection_id = self._resolve_connection_by_state(conn, state)

            # Exchange code → tokens (calls WebPT token endpoint)
            tokens = exchange_code_for_tokens(code)
            logger.info("Tokens obtained for connection %s", connection_id)

            # Store encrypted tokens
            store_tokens(conn, connection_id, tokens)
            logger.info("Tokens stored (encrypted) for connection %s", connection_id)

            # Mark connected and record practice_id if provided
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE webpt_connections
                    SET status = 'connected',
                        connected_at = NOW(),
                        oauth_state = NULL,
                        webpt_practice_id = %s
                    WHERE id = %s
                    """,
                    (practice_id, connection_id),
                )
            conn.commit()

            # Queue historical sync (background RQ job)
            job_id = enqueue_historical_sync(str(connection_id))
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE webpt_connections SET sync_queued_at = NOW() WHERE id = %s",
                    (connection_id,),
                )
            conn.commit()
            logger.info("Historical sync queued (job %s) for connection %s", job_id, connection_id)

            # Register webhook with WebPT
            try:
                webhook_id = register_webhook(
                    tokens.access_token, practice_id=practice_id
                )
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE webpt_connections
                        SET webhook_id = %s, webhook_registered = TRUE
                        WHERE id = %s
                        """,
                        (webhook_id, connection_id),
                    )
                conn.commit()
                logger.info("Webhook registered (%s) for connection %s", webhook_id, connection_id)
            except Exception as exc:
                # Webhook failure is non-fatal — log and continue
                logger.warning("Webhook registration failed for %s: %s", connection_id, exc)

        finally:
            conn.close()

        return {
            "connection_id": str(connection_id),
            "status": "connected",
            "message": "Connected! Loading your data...",
        }

    # ------------------------------------------------------------------
    # Step 3: status polling
    # ------------------------------------------------------------------

    def get_connection_status(self, clinic_id: str) -> Optional[dict]:
        """Return the current connection + sync status for a clinic."""
        conn = self._db()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT id, status, claims_synced, sync_queued_at,
                           sync_started_at, sync_completed_at,
                           webhook_registered, error_message, connected_at
                    FROM webpt_connections
                    WHERE clinic_id = %s
                      AND status != 'disconnected'
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (clinic_id,),
                )
                row = cur.fetchone()
        finally:
            conn.close()

        if row is None:
            return None

        result = dict(row)
        result["id"] = str(result["id"])

        # Human-readable message for the UI
        status = result["status"]
        if status == "pending":
            result["message"] = "Awaiting authorization..."
        elif status == "connected":
            result["message"] = "Connected! Loading your data..."
        elif status == "syncing":
            claims = result.get("claims_synced") or 0
            result["message"] = f"Syncing historical claims... ({claims:,} loaded so far)"
        elif status == "ready":
            result["message"] = "Your dashboard is ready!"
        elif status == "error":
            result["message"] = f"Connection error: {result.get('error_message', 'unknown')}"
        return result

    # ------------------------------------------------------------------
    # Step 4: finalise after sync completes (called from RQ worker)
    # ------------------------------------------------------------------

    def finalize_after_sync(self, connection_id: str) -> dict:
        """
        Called (e.g. by a post-sync RQ job or webhook) once the historical
        sync is done.  Runs pattern detection and marks the connection 'ready'.
        """
        conn = self._db()
        try:
            pattern_count = run_pattern_detection(conn, connection_id)
            logger.info(
                "Pattern detection complete for %s — %d patterns found",
                connection_id,
                pattern_count,
            )
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE webpt_connections SET status = 'ready' WHERE id = %s",
                    (connection_id,),
                )
            conn.commit()
        finally:
            conn.close()

        return {
            "connection_id": connection_id,
            "status": "ready",
            "patterns_found": pattern_count,
            "message": "Your dashboard is ready!",
        }

    # ------------------------------------------------------------------
    # Disconnect
    # ------------------------------------------------------------------

    def disconnect(self, clinic_id: str) -> None:
        """Revoke the connection for a clinic (soft delete)."""
        from .webhooks import delete_webhook
        from .token_store import load_tokens

        conn = self._db()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT id, webhook_id
                    FROM webpt_connections
                    WHERE clinic_id = %s AND status != 'disconnected'
                    ORDER BY created_at DESC LIMIT 1
                    """,
                    (clinic_id,),
                )
                row = cur.fetchone()

            if row is None:
                return

            connection_id = str(row["id"])
            webhook_id = row.get("webhook_id")

            if webhook_id:
                tokens = load_tokens(conn, connection_id)
                if tokens and not tokens.is_expired():
                    try:
                        delete_webhook(tokens.access_token, webhook_id)
                    except Exception as exc:
                        logger.warning("Could not delete webhook %s: %s", webhook_id, exc)

            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE webpt_connections
                    SET status = 'disconnected', disconnected_at = NOW()
                    WHERE id = %s
                    """,
                    (row["id"],),
                )
            conn.commit()
            logger.info("Disconnected clinic %s (connection %s)", clinic_id, connection_id)
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _db(self) -> psycopg2.extensions.connection:
        return psycopg2.connect(self._database_url)

    def _resolve_connection_by_state(
        self, conn: psycopg2.extensions.connection, state: str
    ) -> uuid.UUID:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id FROM webpt_connections
                WHERE oauth_state = %s AND status = 'pending'
                """,
                (state,),
            )
            row = cur.fetchone()
        if row is None:
            raise ValueError(f"Invalid or expired OAuth state: {state!r}")
        return row[0]
