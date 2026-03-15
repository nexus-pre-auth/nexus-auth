"""
Encrypted token storage for WebPT OAuth credentials.

Tokens are encrypted with Fernet (AES-128-CBC + HMAC-SHA256) before being
written to the database.  The encryption key is loaded from the environment
variable TOKEN_ENCRYPTION_KEY (a URL-safe base64-encoded 32-byte key).

Generate a key with:
    python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
"""

import logging
import os
from datetime import datetime, timezone
from typing import Optional

import psycopg2
from cryptography.fernet import Fernet, InvalidToken

from .oauth import TokenSet

logger = logging.getLogger(__name__)

_fernet: Optional[Fernet] = None


def _get_fernet() -> Fernet:
    global _fernet
    if _fernet is None:
        key = os.environ.get("TOKEN_ENCRYPTION_KEY", "")
        if not key:
            raise RuntimeError(
                "TOKEN_ENCRYPTION_KEY environment variable is not set. "
                "Generate one with: python -c \"from cryptography.fernet import Fernet; "
                "print(Fernet.generate_key().decode())\""
            )
        _fernet = Fernet(key.encode())
    return _fernet


def _encrypt(plaintext: str) -> str:
    return _get_fernet().encrypt(plaintext.encode()).decode()


def _decrypt(ciphertext: str) -> str:
    try:
        return _get_fernet().decrypt(ciphertext.encode()).decode()
    except InvalidToken as exc:
        raise ValueError("Token decryption failed — key mismatch or corrupted data") from exc


def store_tokens(conn: psycopg2.extensions.connection, connection_id: str, tokens: TokenSet) -> None:
    """Upsert encrypted tokens for a WebPT connection."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO webpt_tokens
                (connection_id, access_token_enc, refresh_token_enc,
                 token_type, expires_at, scope)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (connection_id) DO UPDATE SET
                access_token_enc  = EXCLUDED.access_token_enc,
                refresh_token_enc = EXCLUDED.refresh_token_enc,
                token_type        = EXCLUDED.token_type,
                expires_at        = EXCLUDED.expires_at,
                scope             = EXCLUDED.scope,
                updated_at        = NOW()
            """,
            (
                connection_id,
                _encrypt(tokens.access_token),
                _encrypt(tokens.refresh_token) if tokens.refresh_token else None,
                tokens.token_type,
                tokens.expires_at,
                tokens.scope,
            ),
        )
    conn.commit()
    logger.info("Stored encrypted tokens for connection %s", connection_id)


def load_tokens(conn: psycopg2.extensions.connection, connection_id: str) -> Optional[TokenSet]:
    """Load and decrypt tokens for a WebPT connection. Returns None if not found."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT access_token_enc, refresh_token_enc, token_type, expires_at, scope
            FROM webpt_tokens
            WHERE connection_id = %s
            """,
            (connection_id,),
        )
        row = cur.fetchone()

    if row is None:
        return None

    access_enc, refresh_enc, token_type, expires_at, scope = row
    return TokenSet(
        access_token=_decrypt(access_enc),
        refresh_token=_decrypt(refresh_enc) if refresh_enc else None,
        token_type=token_type or "Bearer",
        expires_at=expires_at if expires_at else datetime.now(tz=timezone.utc),
        scope=scope,
    )
