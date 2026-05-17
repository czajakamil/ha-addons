"""Unit tests for backend/app/security.py.

All tests are pure unit tests — no database required.
"""
import pytest

from app.security import (
    hash_password,
    verify_password,
    dummy_verify,
    password_needs_rehash,
)


class TestHashPassword:
    def test_returns_non_empty_string(self):
        result = hash_password("mysecret")
        assert isinstance(result, str)
        assert len(result) > 0

    def test_two_calls_produce_different_hashes(self):
        """Argon2 uses a random salt, so two hashes for the same password differ."""
        h1 = hash_password("samepassword")
        h2 = hash_password("samepassword")
        assert h1 != h2

    def test_hash_does_not_contain_plaintext_password(self):
        password = "supersecret123"
        h = hash_password(password)
        assert password not in h


class TestVerifyPassword:
    def test_correct_password_returns_true(self):
        password = "correct-horse-battery-staple"
        h = hash_password(password)
        assert verify_password(password, h) is True

    def test_wrong_password_returns_false(self):
        h = hash_password("rightpassword")
        assert verify_password("wrongpassword", h) is False

    def test_empty_password_against_non_empty_hash_returns_false(self):
        h = hash_password("notempty")
        assert verify_password("", h) is False

    def test_verify_against_empty_hash_returns_false(self):
        assert verify_password("anypassword", "") is False

    def test_verify_against_garbage_hash_returns_false(self):
        assert verify_password("anypassword", "not-a-valid-argon2-hash") is False

    def test_roundtrip_with_special_characters(self):
        password = "p@$$w0rd!äöü€"
        h = hash_password(password)
        assert verify_password(password, h) is True

    def test_different_hash_for_same_password_still_verifies(self):
        """Both hashes should verify against the same plaintext despite differing."""
        password = "shared"
        h1 = hash_password(password)
        h2 = hash_password(password)
        assert h1 != h2
        assert verify_password(password, h1) is True
        assert verify_password(password, h2) is True


class TestDummyVerify:
    def test_does_not_raise(self):
        """dummy_verify must swallow all exceptions."""
        dummy_verify("any_password")

    def test_does_not_raise_with_empty_string(self):
        dummy_verify("")

    def test_returns_none(self):
        """dummy_verify has no return value — implicitly returns None."""
        result = dummy_verify("something")
        assert result is None

    def test_never_grants_access(self):
        """dummy_verify always returns None (not True), so it never authorizes."""
        result = dummy_verify("correct-horse-battery-staple")
        assert not result


class TestPasswordNeedsRehash:
    def test_fresh_hash_does_not_need_rehash(self):
        h = hash_password("fresh")
        assert password_needs_rehash(h) is False

    def test_invalid_hash_needs_rehash(self):
        """A garbage string is treated as invalid → rehash is required."""
        assert password_needs_rehash("not-a-real-hash") is True

    def test_empty_string_needs_rehash(self):
        assert password_needs_rehash("") is True
