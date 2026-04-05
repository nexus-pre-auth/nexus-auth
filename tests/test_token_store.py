"""
Tests for webpt/token_store.py.

Covers:
  - _encrypt / _decrypt round-trip with a real Fernet key
  - _decrypt raises ValueError on tampered ciphertext
  - _get_fernet raises RuntimeError when key is absent
  - store_tokens — verifies upsert SQL is called with encrypted values
  - load_tokens  — verifies row is decrypted into a TokenSet
  - load_tokens  — returns None when no row found
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, call, patch

import pytest
from cryptography.fernet import Fernet

import webpt.token_store as ts_module
from webpt.oauth import TokenSet


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def reset_fernet_singleton(monkeypatch):
    """
    The module caches a Fernet instance in a global.
    Reset it before and after every test to prevent key bleed between tests.
    """
    monkeypatch.setattr(ts_module, "_fernet", None)
    yield
    monkeypatch.setattr(ts_module, "_fernet", None)


@pytest.fixture
def fernet_key(monkeypatch) -> str:
    """Set a valid TOKEN_ENCRYPTION_KEY env var and return the key string."""
    key = Fernet.generate_key().decode()
    monkeypatch.setenv("TOKEN_ENCRYPTION_KEY", key)
    return key


@pytest.fixture
def sample_token_set() -> TokenSet:
    return TokenSet(
        access_token="access-token-value",
        refresh_token="refresh-token-value",
        token_type="Bearer",
        expires_at=datetime(2030, 1, 1, tzinfo=timezone.utc),
        scope="openid claims:read",
    )


# ---------------------------------------------------------------------------
# _get_fernet
# ---------------------------------------------------------------------------

class TestGetFernet:
    def test_raises_when_key_missing(self, monkeypatch):
        monkeypatch.delenv("TOKEN_ENCRYPTION_KEY", raising=False)
        with pytest.raises(RuntimeError, match="TOKEN_ENCRYPTION_KEY"):
            ts_module._get_fernet()

    def test_returns_fernet_with_valid_key(self, fernet_key):
        f = ts_module._get_fernet()
        assert isinstance(f, Fernet)

    def test_is_cached(self, fernet_key):
        f1 = ts_module._get_fernet()
        f2 = ts_module._get_fernet()
        assert f1 is f2


# ---------------------------------------------------------------------------
# _encrypt / _decrypt
# ---------------------------------------------------------------------------

class TestEncryptDecrypt:
    def test_round_trip(self, fernet_key):
        plaintext = "super-secret-access-token"
        ciphertext = ts_module._encrypt(plaintext)
        assert ts_module._decrypt(ciphertext) == plaintext

    def test_ciphertext_differs_from_plaintext(self, fernet_key):
        plaintext = "my-token"
        ciphertext = ts_module._encrypt(plaintext)
        assert ciphertext != plaintext

    def test_each_encryption_produces_different_ciphertext(self, fernet_key):
        # Fernet uses random IV per call
        ct1 = ts_module._encrypt("same-value")
        ct2 = ts_module._encrypt("same-value")
        assert ct1 != ct2

    def test_decrypt_raises_on_tampered_data(self, fernet_key):
        ciphertext = ts_module._encrypt("original")
        tampered = ciphertext[:-4] + "XXXX"
        with pytest.raises(ValueError, match="decryption failed"):
            ts_module._decrypt(tampered)

    def test_decrypt_raises_on_wrong_key(self, monkeypatch):
        key1 = Fernet.generate_key().decode()
        key2 = Fernet.generate_key().decode()

        monkeypatch.setenv("TOKEN_ENCRYPTION_KEY", key1)
        ciphertext = ts_module._encrypt("secret")

        # Reset singleton and switch to a different key
        monkeypatch.setattr(ts_module, "_fernet", None)
        monkeypatch.setenv("TOKEN_ENCRYPTION_KEY", key2)

        with pytest.raises(ValueError):
            ts_module._decrypt(ciphertext)


# ---------------------------------------------------------------------------
# store_tokens
# ---------------------------------------------------------------------------

class TestStoreTokens:
    def test_calls_execute_with_connection_id(self, fernet_key, mock_conn, mock_cursor, sample_token_set):
        ts_module.store_tokens(mock_conn, "conn-abc", sample_token_set)
        assert mock_cursor.execute.called
        sql, params = mock_cursor.execute.call_args[0]
        assert "INSERT INTO webpt_tokens" in sql
        assert params[0] == "conn-abc"

    def test_access_token_is_encrypted(self, fernet_key, mock_conn, mock_cursor, sample_token_set):
        ts_module.store_tokens(mock_conn, "conn-abc", sample_token_set)
        _, params = mock_cursor.execute.call_args[0]
        encrypted_access = params[1]
        # Must decrypt back to the original value
        assert ts_module._decrypt(encrypted_access) == "access-token-value"

    def test_refresh_token_is_encrypted(self, fernet_key, mock_conn, mock_cursor, sample_token_set):
        ts_module.store_tokens(mock_conn, "conn-abc", sample_token_set)
        _, params = mock_cursor.execute.call_args[0]
        encrypted_refresh = params[2]
        assert ts_module._decrypt(encrypted_refresh) == "refresh-token-value"

    def test_null_refresh_token_stored_as_none(self, fernet_key, mock_conn, mock_cursor, sample_token_set):
        sample_token_set.refresh_token = None
        ts_module.store_tokens(mock_conn, "conn-abc", sample_token_set)
        _, params = mock_cursor.execute.call_args[0]
        assert params[2] is None

    def test_commits_after_insert(self, fernet_key, mock_conn, sample_token_set):
        ts_module.store_tokens(mock_conn, "conn-abc", sample_token_set)
        mock_conn.commit.assert_called_once()


# ---------------------------------------------------------------------------
# load_tokens
# ---------------------------------------------------------------------------

class TestLoadTokens:
    def test_returns_none_when_not_found(self, fernet_key, mock_conn, mock_cursor):
        mock_cursor.fetchone.return_value = None
        result = ts_module.load_tokens(mock_conn, "conn-missing")
        assert result is None

    def test_returns_token_set_when_found(self, fernet_key, mock_conn, mock_cursor):
        expires = datetime(2030, 1, 1, tzinfo=timezone.utc)
        enc_access  = ts_module._encrypt("loaded-access-token")
        enc_refresh = ts_module._encrypt("loaded-refresh-token")
        mock_cursor.fetchone.return_value = (enc_access, enc_refresh, "Bearer", expires, "openid")

        result = ts_module.load_tokens(mock_conn, "conn-xyz")

        assert isinstance(result, TokenSet)
        assert result.access_token  == "loaded-access-token"
        assert result.refresh_token == "loaded-refresh-token"
        assert result.token_type    == "Bearer"
        assert result.scope         == "openid"

    def test_null_refresh_token_in_db_returns_none(self, fernet_key, mock_conn, mock_cursor):
        expires = datetime(2030, 1, 1, tzinfo=timezone.utc)
        enc_access = ts_module._encrypt("at")
        mock_cursor.fetchone.return_value = (enc_access, None, "Bearer", expires, None)

        result = ts_module.load_tokens(mock_conn, "conn-xyz")

        assert result.refresh_token is None

    def test_queries_by_connection_id(self, fernet_key, mock_conn, mock_cursor):
        mock_cursor.fetchone.return_value = None
        ts_module.load_tokens(mock_conn, "my-connection-id")

        sql, params = mock_cursor.execute.call_args[0]
        assert "WHERE connection_id = %s" in sql
        assert params == ("my-connection-id",)
