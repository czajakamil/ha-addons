import pytest

from app.security import (
    dummy_verify,
    hash_password,
    password_needs_rehash,
    verify_password,
)

pytestmark = pytest.mark.unit


def test_hash_is_salted_and_verifies():
    h1 = hash_password("CorrectHorse123")
    h2 = hash_password("CorrectHorse123")
    assert h1 != h2  # różna sól
    assert verify_password("CorrectHorse123", h1)
    assert verify_password("CorrectHorse123", h2)


def test_verify_rejects_wrong_password():
    h = hash_password("CorrectHorse123")
    assert verify_password("zła-próba", h) is False


def test_verify_handles_garbage_hash():
    assert verify_password("cokolwiek", "to-nie-jest-argon2-hash") is False


def test_password_needs_rehash_false_for_fresh_hash():
    assert password_needs_rehash(hash_password("CorrectHorse123")) is False


def test_password_needs_rehash_true_for_invalid_hash():
    assert password_needs_rehash("śmieci") is True


def test_dummy_verify_never_raises():
    # Używane do wyrównania czasu odpowiedzi gdy user nie istnieje.
    dummy_verify("cokolwiek")
