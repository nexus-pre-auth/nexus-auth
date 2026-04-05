"""
Tests for webpt/oauth.py.

Covers:
  - TokenSet.from_response() — OAuth response parsing
  - TokenSet.is_expired()    — expiry logic with time mocking
  - generate_state()         — CSRF nonce generation
  - build_authorization_url() — URL construction
  - exchange_code_for_tokens() — token exchange via mocked HTTP
  - refresh_access_token()     — token refresh via mocked HTTP
"""

import urllib.parse
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from webpt.oauth import (
    TokenSet,
    build_authorization_url,
    exchange_code_for_tokens,
    generate_state,
    refresh_access_token,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_token_response(**overrides) -> dict:
    base = {
        "access_token": "at-abc123",
        "refresh_token": "rt-xyz789",
        "token_type": "Bearer",
        "expires_in": 3600,
        "scope": "openid profile claims:read",
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# TokenSet.from_response
# ---------------------------------------------------------------------------

class TestTokenSetFromResponse:
    def test_parses_access_token(self):
        ts = TokenSet.from_response(_make_token_response())
        assert ts.access_token == "at-abc123"

    def test_parses_refresh_token(self):
        ts = TokenSet.from_response(_make_token_response())
        assert ts.refresh_token == "rt-xyz789"

    def test_refresh_token_optional(self):
        ts = TokenSet.from_response(_make_token_response(refresh_token=None))
        assert ts.refresh_token is None

    def test_parses_scope(self):
        ts = TokenSet.from_response(_make_token_response())
        assert ts.scope == "openid profile claims:read"

    def test_defaults_token_type_to_bearer(self):
        data = _make_token_response()
        del data["token_type"]
        ts = TokenSet.from_response(data)
        assert ts.token_type == "Bearer"

    def test_expires_at_is_in_future(self):
        ts = TokenSet.from_response(_make_token_response(expires_in=3600))
        assert ts.expires_at > datetime.now(tz=timezone.utc)

    def test_expires_at_approximates_expires_in(self):
        ts = TokenSet.from_response(_make_token_response(expires_in=7200))
        expected_min = datetime.now(tz=timezone.utc) + timedelta(seconds=7100)
        expected_max = datetime.now(tz=timezone.utc) + timedelta(seconds=7300)
        assert expected_min < ts.expires_at < expected_max

    def test_defaults_expires_in_to_3600(self):
        data = _make_token_response()
        del data["expires_in"]
        ts = TokenSet.from_response(data)
        # expires_at should be ~1 hour from now
        delta = ts.expires_at - datetime.now(tz=timezone.utc)
        assert 3500 < delta.total_seconds() < 3700


# ---------------------------------------------------------------------------
# TokenSet.is_expired
# ---------------------------------------------------------------------------

class TestTokenSetIsExpired:
    def _make_ts(self, seconds_until_expiry: int) -> TokenSet:
        expires_at = datetime.now(tz=timezone.utc) + timedelta(seconds=seconds_until_expiry)
        return TokenSet(
            access_token="at",
            refresh_token=None,
            token_type="Bearer",
            expires_at=expires_at,
            scope=None,
        )

    def test_not_expired_when_plenty_of_time_left(self):
        ts = self._make_ts(seconds_until_expiry=600)
        assert ts.is_expired() is False

    def test_expired_when_past_expiry(self):
        ts = self._make_ts(seconds_until_expiry=-10)
        assert ts.is_expired() is True

    def test_expired_within_buffer(self):
        # Token expires in 30 seconds but buffer is 60 → should be considered expired
        ts = self._make_ts(seconds_until_expiry=30)
        assert ts.is_expired(buffer_seconds=60) is True

    def test_not_expired_outside_buffer(self):
        # Token expires in 120 seconds with a 60 second buffer → still valid
        ts = self._make_ts(seconds_until_expiry=120)
        assert ts.is_expired(buffer_seconds=60) is False

    def test_default_buffer_is_60_seconds(self):
        # Exactly at the 60-second boundary
        ts = self._make_ts(seconds_until_expiry=59)
        assert ts.is_expired() is True


# ---------------------------------------------------------------------------
# generate_state
# ---------------------------------------------------------------------------

class TestGenerateState:
    def test_returns_nonempty_string(self):
        state = generate_state()
        assert isinstance(state, str)
        assert len(state) > 0

    def test_returns_url_safe_characters(self):
        state = generate_state()
        # URL-safe base64: letters, digits, -, _
        assert all(c in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_=" for c in state)

    def test_returns_unique_values(self):
        # Two successive calls should not produce the same state
        assert generate_state() != generate_state()

    def test_minimum_length(self):
        # secrets.token_urlsafe(32) produces ~43 chars
        assert len(generate_state()) >= 40


# ---------------------------------------------------------------------------
# build_authorization_url
# ---------------------------------------------------------------------------

class TestBuildAuthorizationUrl:
    def test_contains_client_id(self):
        with patch("webpt.oauth.WEBPT_CLIENT_ID", "my-client-id"):
            url = build_authorization_url("test-state")
        assert "my-client-id" in url

    def test_contains_state(self):
        url = build_authorization_url("csrf-nonce-123")
        assert "csrf-nonce-123" in url

    def test_response_type_is_code(self):
        url = build_authorization_url("s")
        parsed = urllib.parse.urlparse(url)
        params = urllib.parse.parse_qs(parsed.query)
        assert params["response_type"] == ["code"]

    def test_uses_configured_auth_url(self):
        with patch("webpt.oauth.WEBPT_AUTH_URL", "https://auth.example.com/oauth/authorize"):
            url = build_authorization_url("s")
        assert url.startswith("https://auth.example.com/oauth/authorize")

    def test_contains_redirect_uri(self):
        with patch("webpt.oauth.WEBPT_REDIRECT_URI", "https://app.example.com/callback"):
            url = build_authorization_url("s")
        assert "https%3A%2F%2Fapp.example.com%2Fcallback" in url or "https://app.example.com/callback" in url

    def test_contains_scope(self):
        with patch("webpt.oauth.WEBPT_SCOPES", "openid claims:read"):
            url = build_authorization_url("s")
        assert "openid" in url


# ---------------------------------------------------------------------------
# exchange_code_for_tokens
# ---------------------------------------------------------------------------

class TestExchangeCodeForTokens:
    def _mock_response(self, data: dict, status_code: int = 200):
        resp = MagicMock()
        resp.status_code = status_code
        resp.json.return_value = data
        resp.raise_for_status = MagicMock()
        return resp

    def test_returns_token_set_on_success(self):
        mock_resp = self._mock_response(_make_token_response())
        with patch("webpt.oauth.requests.post", return_value=mock_resp):
            ts = exchange_code_for_tokens("auth-code-123")
        assert isinstance(ts, TokenSet)
        assert ts.access_token == "at-abc123"

    def test_sends_grant_type_authorization_code(self):
        mock_resp = self._mock_response(_make_token_response())
        with patch("webpt.oauth.requests.post", return_value=mock_resp) as mock_post:
            exchange_code_for_tokens("auth-code-123")
        call_kwargs = mock_post.call_args
        sent_data = call_kwargs[1]["data"] if "data" in call_kwargs[1] else call_kwargs[0][1]
        assert sent_data["grant_type"] == "authorization_code"

    def test_sends_authorization_code(self):
        mock_resp = self._mock_response(_make_token_response())
        with patch("webpt.oauth.requests.post", return_value=mock_resp) as mock_post:
            exchange_code_for_tokens("my-auth-code")
        sent_data = mock_post.call_args[1]["data"]
        assert sent_data["code"] == "my-auth-code"

    def test_raises_on_http_error(self):
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = Exception("401 Unauthorized")
        with patch("webpt.oauth.requests.post", return_value=mock_resp):
            with pytest.raises(Exception):
                exchange_code_for_tokens("bad-code")


# ---------------------------------------------------------------------------
# refresh_access_token
# ---------------------------------------------------------------------------

class TestRefreshAccessToken:
    def _mock_response(self, data: dict) -> MagicMock:
        resp = MagicMock()
        resp.json.return_value = data
        resp.raise_for_status = MagicMock()
        return resp

    def test_returns_new_token_set(self):
        new_data = _make_token_response(access_token="new-at-999")
        mock_resp = self._mock_response(new_data)
        with patch("webpt.oauth.requests.post", return_value=mock_resp):
            ts = refresh_access_token("rt-old")
        assert ts.access_token == "new-at-999"

    def test_sends_grant_type_refresh_token(self):
        mock_resp = self._mock_response(_make_token_response())
        with patch("webpt.oauth.requests.post", return_value=mock_resp) as mock_post:
            refresh_access_token("my-refresh-token")
        sent_data = mock_post.call_args[1]["data"]
        assert sent_data["grant_type"] == "refresh_token"

    def test_sends_refresh_token_value(self):
        mock_resp = self._mock_response(_make_token_response())
        with patch("webpt.oauth.requests.post", return_value=mock_resp) as mock_post:
            refresh_access_token("my-refresh-token")
        sent_data = mock_post.call_args[1]["data"]
        assert sent_data["refresh_token"] == "my-refresh-token"

    def test_raises_on_http_error(self):
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = Exception("400 Bad Request")
        with patch("webpt.oauth.requests.post", return_value=mock_resp):
            with pytest.raises(Exception):
                refresh_access_token("expired-rt")
