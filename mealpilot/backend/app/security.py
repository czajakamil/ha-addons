from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError, InvalidHashError, VerificationError

_hasher = PasswordHasher()

# Constant hash used for timing equalization when the user does not exist.
# The value is irrelevant — only the verification time matters.
_DUMMY_HASH = _hasher.hash("dummy-password-for-timing-equalization")


def hash_password(password: str) -> str:
    return _hasher.hash(password)


def verify_password(password: str, password_hash: str) -> bool:
    try:
        return _hasher.verify(password_hash, password)
    except (VerifyMismatchError, InvalidHashError, VerificationError):
        return False


def password_needs_rehash(password_hash: str) -> bool:
    try:
        return _hasher.check_needs_rehash(password_hash)
    except InvalidHashError:
        return True


def dummy_verify(password: str) -> None:
    """Runs Argon2 against a dummy hash to equalize response time when user does not exist."""
    try:
        _hasher.verify(_DUMMY_HASH, password)
    except (VerifyMismatchError, InvalidHashError, VerificationError):
        pass
