"""Simple in-memory sliding-window rate limiter.

Sufficient for a single-instance deploy (MealPilot runs on one host).
Multi-replica setups would need Redis-backed storage instead.
"""
from __future__ import annotations

import threading
import time
from collections import defaultdict, deque
from typing import Deque, Dict, Tuple


class SlidingWindowLimiter:
    def __init__(self, max_attempts: int, window_seconds: float):
        self.max_attempts = max_attempts
        self.window = window_seconds
        self._buckets: Dict[str, Deque[float]] = defaultdict(deque)
        self._lock = threading.Lock()

    def check(self, key: str) -> Tuple[bool, float]:
        """Returns (allowed, retry_after_seconds)."""
        now = time.monotonic()
        with self._lock:
            bucket = self._buckets[key]
            cutoff = now - self.window
            while bucket and bucket[0] < cutoff:
                bucket.popleft()
            if len(bucket) >= self.max_attempts:
                retry_after = self.window - (now - bucket[0])
                return False, max(retry_after, 1.0)
            bucket.append(now)
            return True, 0.0

    def reset(self, key: str) -> None:
        with self._lock:
            self._buckets.pop(key, None)


# 10 attempts per 15 minutes per (IP, username). Argon2 further slows each attempt.
login_limiter = SlidingWindowLimiter(max_attempts=10, window_seconds=15 * 60)

# 5 attempts per 15 minutes per IP. Setup is one-shot — after success the endpoint
# returns 403, but hash_password is expensive, so this closes the DoS/brute-force vector.
setup_limiter = SlidingWindowLimiter(max_attempts=5, window_seconds=15 * 60)

# 5 LLM calls per minute per user — guards against rapid monthly quota exhaustion
# and DoS against the AI provider.
ai_limiter = SlidingWindowLimiter(max_attempts=5, window_seconds=60)


class ConvInflightStore:
    """Prevents concurrent /run calls on the same conversation (double-click guard)."""

    def __init__(self) -> None:
        self._running: set[str] = set()
        self._lock = threading.Lock()

    def acquire(self, key: str) -> bool:
        with self._lock:
            if key in self._running:
                return False
            self._running.add(key)
            return True

    def release(self, key: str) -> None:
        with self._lock:
            self._running.discard(key)


class IdempotencyCache:
    """Short-lived result cache keyed by user-supplied Idempotency-Key header."""

    def __init__(self, ttl_seconds: float = 300) -> None:
        self.ttl = ttl_seconds
        self._cache: Dict[str, Tuple[object, float]] = {}
        self._lock = threading.Lock()

    def get(self, key: str) -> object | None:
        now = time.monotonic()
        with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                return None
            result, exp = entry
            if exp < now:
                del self._cache[key]
                return None
            return result

    def set(self, key: str, result: object) -> None:
        now = time.monotonic()
        with self._lock:
            self._evict(now)
            self._cache[key] = (result, now + self.ttl)

    def _evict(self, now: float) -> None:
        expired = [k for k, (_, exp) in self._cache.items() if exp < now]
        for k in expired:
            del self._cache[k]


conv_inflight = ConvInflightStore()
idempotency_cache = IdempotencyCache(ttl_seconds=300)
