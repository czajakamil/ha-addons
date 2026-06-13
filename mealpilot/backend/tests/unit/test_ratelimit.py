import pytest

from app.ratelimit import SlidingWindowLimiter

pytestmark = pytest.mark.unit


def test_allows_up_to_max_then_blocks():
    lim = SlidingWindowLimiter(max_attempts=3, window_seconds=60)
    for _ in range(3):
        allowed, retry = lim.check("k")
        assert allowed is True
        assert retry == 0.0
    allowed, retry = lim.check("k")
    assert allowed is False
    assert retry >= 1.0


def test_keys_are_independent():
    lim = SlidingWindowLimiter(max_attempts=1, window_seconds=60)
    assert lim.check("a")[0] is True
    assert lim.check("b")[0] is True
    assert lim.check("a")[0] is False


def test_reset_clears_bucket():
    lim = SlidingWindowLimiter(max_attempts=1, window_seconds=60)
    assert lim.check("k")[0] is True
    assert lim.check("k")[0] is False
    lim.reset("k")
    assert lim.check("k")[0] is True


def test_window_expiry_with_monkeypatched_clock(monkeypatch):
    import app.ratelimit as rl

    t = {"now": 1000.0}
    monkeypatch.setattr(rl.time, "monotonic", lambda: t["now"])
    lim = SlidingWindowLimiter(max_attempts=2, window_seconds=10)
    assert lim.check("k")[0] is True
    assert lim.check("k")[0] is True
    assert lim.check("k")[0] is False
    # Przesuń zegar poza okno — stare próby wypadają.
    t["now"] += 11
    assert lim.check("k")[0] is True
