"""
WebPT OAuth 2.0 client.

Handles the authorization code flow:
  1. build_authorization_url()  → redirect user to WebPT
  2. exchange_code_for_tokens() → trade auth code for access/refresh tokens
  3. refresh_access_token()     → use refresh token when access token expires
"""

import os
import secrets
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests
from tenacity import retry, stop_after_attempt, wait_exponential


WEBPT_AUTH_URL   = os.getenv("WEBPT_AUTH_URL",   "https://auth.webpt.com/oauth/authorize")
WEBPT_TOKEN_URL  = os.getenv("WEBPT_TOKEN_URL",  "https://auth.webpt.com/oauth/token")
WEBPT_CLIENT_ID  = os.getenv("WEBPT_CLIENT_ID",  "")
WEBPT_CLIENT_SECRET = os.getenv("WEBPT_CLIENT_SECRET", "")
WEBPT_REDIRECT_URI  = os.getenv("WEBPT_REDIRECT_URI",  "")
WEBPT_SCOPES = os.getenv(
    "WEBPT_SCOPES",
    "openid profile claims:read webhooks:write",
)


@dataclass
class TokenSet:
    access_token: str
    refresh_token: Optional[str]
    token_type: str
    expires_at: datetime
    scope: Optional[str]

    @classmethod
    def from_response(cls, data: dict) -> "TokenSet":
        expires_in = int(data.get("expires_in", 3600))
        expires_at = datetime.now(tz=timezone.utc) + timedelta(seconds=expires_in)
        return cls(
            access_token=data["access_token"],
            refresh_token=data.get("refresh_token"),
            token_type=data.get("token_type", "Bearer"),
            expires_at=expires_at,
            scope=data.get("scope"),
        )

    def is_expired(self, buffer_seconds: int = 60) -> bool:
        return datetime.now(tz=timezone.utc) >= (
            self.expires_at - timedelta(seconds=buffer_seconds)
        )


def generate_state() -> str:
    """Generate a cryptographically random OAuth state parameter (CSRF nonce)."""
    return secrets.token_urlsafe(32)


def build_authorization_url(state: str) -> str:
    """Return the WebPT OAuth authorization URL the user should be redirected to."""
    params = {
        "response_type": "code",
        "client_id": WEBPT_CLIENT_ID,
        "redirect_uri": WEBPT_REDIRECT_URI,
        "scope": WEBPT_SCOPES,
        "state": state,
    }
    return f"{WEBPT_AUTH_URL}?{urllib.parse.urlencode(params)}"


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    reraise=True,
)
def exchange_code_for_tokens(code: str) -> TokenSet:
    """Exchange an authorization code for access + refresh tokens."""
    response = requests.post(
        WEBPT_TOKEN_URL,
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": WEBPT_REDIRECT_URI,
            "client_id": WEBPT_CLIENT_ID,
            "client_secret": WEBPT_CLIENT_SECRET,
        },
        timeout=15,
    )
    response.raise_for_status()
    return TokenSet.from_response(response.json())


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    reraise=True,
)
def refresh_access_token(refresh_token: str) -> TokenSet:
    """Use a refresh token to obtain a new access token."""
    response = requests.post(
        WEBPT_TOKEN_URL,
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": WEBPT_CLIENT_ID,
            "client_secret": WEBPT_CLIENT_SECRET,
        },
        timeout=15,
    )
    response.raise_for_status()
    return TokenSet.from_response(response.json())
