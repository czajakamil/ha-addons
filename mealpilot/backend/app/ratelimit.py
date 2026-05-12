"""Bardzo prosty in-memory rate limiter, sliding window.

Sensowny dla single-instance deploya (MealPilot to single-user app na jednym hoście).
Dla wielu replik trzeba by przepiąć na Redis.
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
        """Zwraca (allowed, retry_after_seconds)."""
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


# 10 prób na 15 minut per (IP, username). Argon2 dodatkowo spowalnia każdą próbę.
login_limiter = SlidingWindowLimiter(max_attempts=10, window_seconds=15 * 60)
