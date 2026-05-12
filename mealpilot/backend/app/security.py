from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError, InvalidHashError, VerificationError

_hasher = PasswordHasher()

# Stały hash używany do równoważenia czasu odpowiedzi gdy user nie istnieje.
# Wartość nieistotna — liczy się tylko czas weryfikacji.
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
    """Wywołanie z dummy hashem żeby zrównać czas odpowiedzi gdy user nie istnieje."""
    try:
        _hasher.verify(_DUMMY_HASH, password)
    except (VerifyMismatchError, InvalidHashError, VerificationError):
        pass
