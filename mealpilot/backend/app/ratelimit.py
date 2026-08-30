"""Bardzo prosty in-memory rate limiter, sliding window.

Sensowny dla single-instance deploya (MealPilot to single-user app na jednym hoście).
Dla wielu replik trzeba by przepiąć na Redis.
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict, deque


class SlidingWindowLimiter:
    def __init__(self, max_attempts: int, window_seconds: float):
        self.max_attempts = max_attempts
        self.window = window_seconds
        self._buckets: dict[str, deque[float]] = defaultdict(deque)
        self._lock = threading.Lock()

    def check(self, key: str) -> tuple[bool, float]:
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

# /mcp/* przyjmuje ruch nieuwierzytelniony aż do sprawdzenia klucza i robi przy tym
# zapytanie do bazy per żądanie — dlatego limitujemy per IP zarówno handshake,
# kanał wiadomości, jak i same nieudane próby uwierzytelnienia.
mcp_session_limiter = SlidingWindowLimiter(max_attempts=30, window_seconds=5 * 60)
mcp_message_limiter = SlidingWindowLimiter(max_attempts=600, window_seconds=60)
mcp_auth_limiter = SlidingWindowLimiter(max_attempts=20, window_seconds=5 * 60)

# Rejestracja klienta OAuth (RFC 7591) jest z definicji nieuwierzytelniona i
# tworzy wiersz w bazie — bez limitu każdy mógłby zapchać tabelę. 20/h na IP
# starcza na wielokrotne podpinanie konektora i pomyłki po drodze.
oauth_register_limiter = SlidingWindowLimiter(max_attempts=20, window_seconds=60 * 60)
# /oauth/token przyjmuje ruch nieuwierzytelniony i robi zapytanie do bazy per
# żądanie; limit jest hojny, bo odświeżanie tokenu to normalny ruch klienta.
oauth_token_limiter = SlidingWindowLimiter(max_attempts=120, window_seconds=5 * 60)
