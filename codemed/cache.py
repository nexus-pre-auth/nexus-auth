"""
CodeMed AI — Redis Caching Layer
=================================
Provides a drop-in caching layer for the CodeMed API endpoints.

Design:
  - Synchronous Redis client (matches the sync FastAPI handlers)
  - Automatic JSON serialization / deserialization
  - TTL per cache key family:
      * Code lookups  → 7 days  (CMS codes change rarely)
      * Code search   → 1 hour  (query results may improve with new embeddings)
      * HCC crosswalk → 30 days (V28 model updates annually)
      * MEAT patterns → 1 day   (pattern library updates)
  - Graceful fallback: if Redis is unavailable, every call executes normally
  - SHA-256 key hashing for safe, deterministic cache keys
  - Namespace prefixes to avoid collisions between API versions

Usage (in api.py):
    from codemed.cache import get_cache, CacheNamespace

    cache = get_cache()

    @app.get("/v1/codes/lookup/{code}")
    def lookup_code(code: str):
        cached = cache.get(CacheNamespace.CODE_LOOKUP, code)
        if cached:
            return cached
        result = engine.lookup_code(code)
        if result:
            cache.set(CacheNamespace.CODE_LOOKUP, code, result.to_dict())
        return result

Environment variables:
    REDIS_URL          — Redis connection URL (default: redis://localhost:6379/0)
    CODEMED_CACHE_TTL_* — Override default TTLs (seconds)
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# TTL defaults (seconds)
# ---------------------------------------------------------------------------

TTL_CODE_LOOKUP   = int(os.environ.get("CODEMED_CACHE_TTL_LOOKUP",  7 * 86400))   # 7 days
TTL_CODE_SEARCH   = int(os.environ.get("CODEMED_CACHE_TTL_SEARCH",  3600))        # 1 hour
TTL_HCC_CROSSWALK = int(os.environ.get("CODEMED_CACHE_TTL_HCC",     30 * 86400))  # 30 days
TTL_POLICY_INDEX  = int(os.environ.get("CODEMED_CACHE_TTL_POLICY",  86400))       # 1 day

# ---------------------------------------------------------------------------
# Cache namespaces
# ---------------------------------------------------------------------------

class CacheNamespace:
    """Key namespace prefixes — prevents collision between cache families."""
    CODE_LOOKUP   = "cm:lookup:v1"
    CODE_SEARCH   = "cm:search:v1"
    CODE_SUGGEST  = "cm:suggest:v1"
    HCC_ENFORCE   = "cm:hcc:v1"
    POLICY_LOOKUP = "cm:policy:v1"


# ---------------------------------------------------------------------------
# Cache stats (in-process counters)
# ---------------------------------------------------------------------------

@dataclass
class CacheStats:
    hits: int = 0
    misses: int = 0
    errors: int = 0
    skips: int = 0   # Redis unavailable

    @property
    def hit_rate(self) -> float:
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0

    def __repr__(self) -> str:
        return (
            f"CacheStats(hits={self.hits}, misses={self.misses}, "
            f"errors={self.errors}, hit_rate={self.hit_rate:.1%})"
        )


# ---------------------------------------------------------------------------
# Cache client
# ---------------------------------------------------------------------------

class CodeMedCache:
    """
    Synchronous Redis cache with JSON serialization and graceful fallback.

    All methods are safe to call even when Redis is unavailable — they will
    log a warning and return None / no-op rather than raising exceptions.
    """

    def __init__(self, redis_url: str | None = None):
        self._redis_url = redis_url or os.environ.get("REDIS_URL", "redis://localhost:6379/0")
        self._client = None
        self._available = False
        self.stats = CacheStats()
        self._connect()

    def _connect(self) -> None:
        """Attempt to connect to Redis. Silently fails if unavailable."""
        try:
            import redis as redis_lib
            client = redis_lib.from_url(
                self._redis_url,
                decode_responses=True,
                socket_connect_timeout=2,
                socket_timeout=2,
            )
            # Validate connection
            client.ping()
            self._client = client
            self._available = True
            logger.info("Redis cache connected: %s", self._redis_url.split("@")[-1])
        except ImportError:
            logger.warning(
                "redis package not installed — caching disabled. "
                "Install with: pip install redis"
            )
        except Exception as exc:
            logger.warning(
                "Redis unavailable (%s) — running without cache. "
                "Install Redis or set REDIS_URL env var.",
                exc,
            )

    @property
    def is_available(self) -> bool:
        return self._available and self._client is not None

    # ── Core operations ───────────────────────────────────────────────────

    def get(self, namespace: str, key: str) -> Optional[Any]:
        """
        Retrieve a cached value.

        Args:
            namespace: CacheNamespace prefix
            key:       Cache key (will be hashed)

        Returns:
            Deserialized value or None on miss / error
        """
        if not self.is_available:
            self.stats.skips += 1
            return None

        cache_key = self._make_key(namespace, key)
        try:
            raw = self._client.get(cache_key)
            if raw is not None:
                self.stats.hits += 1
                logger.debug("Cache HIT: %s", cache_key[:40])
                return json.loads(raw)
            self.stats.misses += 1
            logger.debug("Cache MISS: %s", cache_key[:40])
            return None
        except Exception as exc:
            self.stats.errors += 1
            logger.warning("Cache GET error (%s): %s", cache_key[:40], exc)
            return None

    def set(self, namespace: str, key: str, value: Any, ttl: int | None = None) -> bool:
        """
        Store a value in the cache.

        Args:
            namespace: CacheNamespace prefix
            key:       Cache key (will be hashed)
            value:     JSON-serializable value to cache
            ttl:       Time-to-live in seconds (uses namespace default if None)

        Returns:
            True on success, False on error / unavailable
        """
        if not self.is_available:
            return False

        cache_key = self._make_key(namespace, key)
        ttl = ttl or self._default_ttl(namespace)

        try:
            serialized = json.dumps(value, default=str)
            self._client.setex(cache_key, ttl, serialized)
            logger.debug("Cache SET: %s (ttl=%ds)", cache_key[:40], ttl)
            return True
        except Exception as exc:
            self.stats.errors += 1
            logger.warning("Cache SET error (%s): %s", cache_key[:40], exc)
            return False

    def delete(self, namespace: str, key: str) -> bool:
        """Delete a specific cache entry."""
        if not self.is_available:
            return False
        cache_key = self._make_key(namespace, key)
        try:
            self._client.delete(cache_key)
            return True
        except Exception as exc:
            logger.warning("Cache DELETE error: %s", exc)
            return False

    def invalidate_namespace(self, namespace: str) -> int:
        """
        Delete all cache keys in a namespace.
        Returns number of keys deleted.
        """
        if not self.is_available:
            return 0
        pattern = f"{namespace}:*"
        try:
            keys = self._client.keys(pattern)
            if keys:
                return self._client.delete(*keys)
            return 0
        except Exception as exc:
            logger.warning("Cache invalidate error (%s): %s", pattern, exc)
            return 0

    def flush_all(self) -> bool:
        """Clear ALL CodeMed cache keys (all namespaces)."""
        if not self.is_available:
            return False
        try:
            keys = self._client.keys("cm:*")
            if keys:
                self._client.delete(*keys)
            logger.info("Cache flushed: %d keys removed", len(keys))
            return True
        except Exception as exc:
            logger.warning("Cache flush error: %s", exc)
            return False

    def health(self) -> dict:
        """Return cache health info for the /v1/health endpoint."""
        return {
            "available": self.is_available,
            "redis_url": self._redis_url.split("@")[-1],   # Strip credentials
            "stats": {
                "hits": self.stats.hits,
                "misses": self.stats.misses,
                "errors": self.stats.errors,
                "hit_rate": round(self.stats.hit_rate, 3),
            },
        }

    # ── Private ───────────────────────────────────────────────────────────

    @staticmethod
    def _make_key(namespace: str, raw_key: str) -> str:
        """
        Create a safe, deterministic cache key.
        Keys are SHA-256 hashed to handle arbitrary-length inputs
        (long search queries, complex nested keys, etc.).
        """
        key_hash = hashlib.sha256(raw_key.encode()).hexdigest()[:16]
        return f"{namespace}:{key_hash}"

    @staticmethod
    def _default_ttl(namespace: str) -> int:
        """Return the default TTL for a given namespace."""
        ttl_map = {
            CacheNamespace.CODE_LOOKUP:   TTL_CODE_LOOKUP,
            CacheNamespace.CODE_SEARCH:   TTL_CODE_SEARCH,
            CacheNamespace.CODE_SUGGEST:  TTL_CODE_SEARCH,
            CacheNamespace.HCC_ENFORCE:   TTL_HCC_CROSSWALK,
            CacheNamespace.POLICY_LOOKUP: TTL_POLICY_INDEX,
        }
        return ttl_map.get(namespace, TTL_CODE_SEARCH)


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_cache_instance: Optional[CodeMedCache] = None


def get_cache() -> CodeMedCache:
    """
    Return the module-level cache singleton.
    Thread-safe for read access; initialised once on first call.
    """
    global _cache_instance
    if _cache_instance is None:
        _cache_instance = CodeMedCache()
    return _cache_instance


def reset_cache() -> None:
    """Reset the singleton (used in tests to force re-initialisation)."""
    global _cache_instance
    _cache_instance = None


# ---------------------------------------------------------------------------
# CLI diagnostic
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    cache = CodeMedCache()
    print(f"\nRedis available: {cache.is_available}")
    print(f"Health: {cache.health()}")

    if cache.is_available:
        # Round-trip test
        cache.set(CacheNamespace.CODE_LOOKUP, "E11.9", {
            "code": "E11.9",
            "description": "Type 2 diabetes mellitus without complications",
            "code_type": "ICD-10",
        })
        result = cache.get(CacheNamespace.CODE_LOOKUP, "E11.9")
        print(f"\nRound-trip test: {'PASS' if result else 'FAIL'}")
        print(f"Cached value: {result}")
        print(f"Stats: {cache.stats}")
    else:
        print("\nRedis not available — install Redis and try again")
        print("  macOS: brew install redis && brew services start redis")
        print("  Linux: sudo apt-get install redis-server")
        print("  Docker: docker run -p 6379:6379 redis:alpine")
