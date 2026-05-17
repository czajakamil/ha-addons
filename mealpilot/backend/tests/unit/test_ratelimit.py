"""Unit tests for backend/app/ratelimit.py.

SlidingWindowLimiter uses time.monotonic() internally, so we patch it
directly instead of relying on freezegun (which only patches wall-clock time).
IdempotencyCache also uses time.monotonic(), patched the same way.
"""
from __future__ import annotations

import time
import threading
from unittest.mock import patch

import pytest

from app.ratelimit import (
    SlidingWindowLimiter,
    ConvInflightStore,
    IdempotencyCache,
    login_limiter,
    ai_limiter,
    setup_limiter,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_limiter(max_attempts: int = 3, window_seconds: float = 60.0) -> SlidingWindowLimiter:
    return SlidingWindowLimiter(max_attempts=max_attempts, window_seconds=window_seconds)


# ---------------------------------------------------------------------------
# SlidingWindowLimiter
# ---------------------------------------------------------------------------

class TestSlidingWindowLimiter:
    def test_first_n_calls_allowed(self):
        limiter = _make_limiter(max_attempts=3, window_seconds=60.0)
        for _ in range(3):
            allowed, retry = limiter.check("user1")
            assert allowed is True
            assert retry == 0.0

    def test_n_plus_one_call_blocked(self):
        limiter = _make_limiter(max_attempts=3, window_seconds=60.0)
        for _ in range(3):
            limiter.check("user1")
        allowed, retry = limiter.check("user1")
        assert allowed is False
        assert retry >= 1.0

    def test_window_expiry_resets_limit(self):
        limiter = _make_limiter(max_attempts=2, window_seconds=10.0)
        base = 1_000_000.0

        with patch("app.ratelimit.time") as mock_time:
            mock_time.monotonic.return_value = base
            limiter.check("user1")
            limiter.check("user1")

            # Should be blocked now
            allowed, _ = limiter.check("user1")
            assert allowed is False

            # Advance time past the window
            mock_time.monotonic.return_value = base + 11.0
            allowed, retry = limiter.check("user1")
            assert allowed is True
            assert retry == 0.0

    def test_different_users_have_independent_limits(self):
        limiter = _make_limiter(max_attempts=1, window_seconds=60.0)
        allowed_a, _ = limiter.check("alice")
        assert allowed_a is True

        # alice is now blocked
        allowed_a2, _ = limiter.check("alice")
        assert allowed_a2 is False

        # bob is unaffected
        allowed_b, _ = limiter.check("bob")
        assert allowed_b is True

    def test_call_just_before_window_end_is_counted(self):
        limiter = _make_limiter(max_attempts=2, window_seconds=10.0)
        base = 1_000_000.0

        with patch("app.ratelimit.time") as mock_time:
            mock_time.monotonic.return_value = base
            limiter.check("user1")  # t=0

            # Second call just before window ends
            mock_time.monotonic.return_value = base + 9.9
            limiter.check("user1")  # t=9.9 — still inside window

            # Third call — should be blocked (2 events in window)
            mock_time.monotonic.return_value = base + 9.95
            allowed, retry = limiter.check("user1")
            assert allowed is False

    def test_reset_clears_bucket(self):
        limiter = _make_limiter(max_attempts=1, window_seconds=60.0)
        limiter.check("user1")
        allowed, _ = limiter.check("user1")
        assert allowed is False

        limiter.reset("user1")
        allowed, _ = limiter.check("user1")
        assert allowed is True

    def test_retry_after_is_reasonable(self):
        limiter = _make_limiter(max_attempts=1, window_seconds=30.0)
        limiter.check("user1")
        _, retry = limiter.check("user1")
        # retry_after should be at most window + 1 second of slack
        assert 1.0 <= retry <= 31.0


# ---------------------------------------------------------------------------
# ConvInflightStore
# ---------------------------------------------------------------------------

class TestConvInflightStore:
    def test_first_acquire_returns_true(self):
        store = ConvInflightStore()
        assert store.acquire("conv-1") is True

    def test_second_acquire_same_key_returns_false(self):
        store = ConvInflightStore()
        store.acquire("conv-1")
        assert store.acquire("conv-1") is False

    def test_after_release_can_acquire_again(self):
        store = ConvInflightStore()
        store.acquire("conv-1")
        store.release("conv-1")
        assert store.acquire("conv-1") is True

    def test_release_nonexistent_key_does_not_raise(self):
        store = ConvInflightStore()
        store.release("does-not-exist")  # must not raise

    def test_different_keys_are_independent(self):
        store = ConvInflightStore()
        store.acquire("conv-a")
        assert store.acquire("conv-b") is True

    def test_thread_safety_concurrent_acquire(self):
        """Only one thread should win the acquire for the same key."""
        store = ConvInflightStore()
        results = []

        def _try_acquire():
            results.append(store.acquire("shared-conv"))

        threads = [threading.Thread(target=_try_acquire) for _ in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Exactly one True expected
        assert results.count(True) == 1
        assert results.count(False) == 19


# ---------------------------------------------------------------------------
# IdempotencyCache
# ---------------------------------------------------------------------------

class TestIdempotencyCache:
    def test_store_and_get_roundtrip(self):
        cache = IdempotencyCache(ttl_seconds=60.0)
        base = 1_000_000.0
        with patch("app.ratelimit.time") as mock_time:
            mock_time.monotonic.return_value = base
            cache.set("key1", {"result": 42})

            mock_time.monotonic.return_value = base + 1.0
            result = cache.get("key1")
        assert result == {"result": 42}

    def test_expired_entry_returns_none(self):
        cache = IdempotencyCache(ttl_seconds=10.0)
        base = 1_000_000.0
        with patch("app.ratelimit.time") as mock_time:
            mock_time.monotonic.return_value = base
            cache.set("key1", "value")

            mock_time.monotonic.return_value = base + 11.0
            result = cache.get("key1")
        assert result is None

    def test_missing_key_returns_none(self):
        cache = IdempotencyCache(ttl_seconds=60.0)
        with patch("app.ratelimit.time") as mock_time:
            mock_time.monotonic.return_value = 1_000_000.0
            result = cache.get("nonexistent")
        assert result is None

    def test_entry_not_expired_within_ttl(self):
        cache = IdempotencyCache(ttl_seconds=300.0)
        base = 1_000_000.0
        with patch("app.ratelimit.time") as mock_time:
            mock_time.monotonic.return_value = base
            cache.set("key1", "still-valid")

            mock_time.monotonic.return_value = base + 299.0
            result = cache.get("key1")
        assert result == "still-valid"

    def test_set_overwrites_existing_key(self):
        cache = IdempotencyCache(ttl_seconds=60.0)
        base = 1_000_000.0
        with patch("app.ratelimit.time") as mock_time:
            mock_time.monotonic.return_value = base
            cache.set("key1", "first")
            cache.set("key1", "second")

            mock_time.monotonic.return_value = base + 1.0
            result = cache.get("key1")
        assert result == "second"


# ---------------------------------------------------------------------------
# Global instances — parameter sanity checks
# ---------------------------------------------------------------------------

class TestGlobalInstances:
    def test_login_limiter_params(self):
        assert login_limiter.max_attempts == 10
        assert login_limiter.window == 15 * 60

    def test_setup_limiter_params(self):
        assert setup_limiter.max_attempts == 5
        assert setup_limiter.window == 15 * 60

    def test_ai_limiter_params(self):
        assert ai_limiter.max_attempts == 5
        assert ai_limiter.window == 60
