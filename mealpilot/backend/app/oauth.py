"""OAuth 2.1 authorization server — the part of MealPilot that claude.ai can talk to.

Why this exists
---------------
``X-MealPilot-Token`` works for any client whose configuration file *you* write:
Claude Desktop and Claude Code let you pin a custom header, so a static API key
is enough. A remote connector added on claude.ai has no such file. It is handed
a URL and nothing else, and the only credential it can obtain is one it
negotiates itself — which in the MCP spec means OAuth. Without the endpoints in
this module, that client reaches ``/mcp``, gets 401, and reports the server as
having no tools. The static-key path is untouched and still works.

Shape of the flow
-----------------
1. ``GET /mcp`` with no token answers 401 + ``WWW-Authenticate`` pointing at
   ``/.well-known/oauth-protected-resource`` (RFC 9728).
2. The client reads that, finds the authorization server, and reads
   ``/.well-known/oauth-authorization-server`` (RFC 8414).
3. It registers itself at ``/oauth/register`` (RFC 7591) — nobody wants to mint
   a client id by hand for every connector.
4. The user's browser lands on ``/oauth/authorize``, signs in, approves.
5. The client redeems the code at ``/oauth/token`` with its PKCE verifier and
   gets an access token (+ a refresh token).
6. Every later request carries ``Authorization: Bearer <token>``.

What "2.1" changes versus 2.0
-----------------------------
PKCE is mandatory for *every* authorization-code flow, not just public clients;
the implicit and password grants are gone; redirect URIs match exactly rather
than by prefix; refresh tokens for public clients are rotated on every use.
This module implements that set, and additionally binds tokens to an RFC 8707
``resource`` so a token minted for a different MCP server cannot be replayed
here — the "confused deputy" problem the MCP spec calls out explicitly.

Everything secret is stored as SHA-256 and never in plaintext, matching how
``models.ApiKey`` already treats API keys. Tokens are opaque and looked up in
the database rather than being self-describing JWTs, so revocation is immediate:
changing a password or deleting an account kills live sessions on the spot.
"""

from __future__ import annotations

import base64
import hashlib
import os
import re
import secrets
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from urllib.parse import urlparse, urlsplit, urlunsplit

from sqlalchemy.orm import Session

from . import models
from .security import dummy_verify, verify_password

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #

TOKEN_PREFIX = "mpo_at_"
REFRESH_PREFIX = "mpo_rt_"
CODE_PREFIX = "mpo_ac_"
CLIENT_ID_PREFIX = "mpc_"
CLIENT_SECRET_PREFIX = "mps_"

# Short enough that a leaked code is near-worthless, long enough to survive a
# slow browser redirect. The spec's own advice is "maximum of 10 minutes"; the
# code is single-use and PKCE-bound on top of this.
CODE_TTL = timedelta(minutes=5)
ACCESS_TOKEN_TTL = timedelta(hours=1)
REFRESH_TOKEN_TTL = timedelta(days=30)

# Deliberately the same vocabulary as `models.ApiKey.scope`, so one Principal
# type serves both credential kinds and `read` means exactly what it already
# means everywhere else in the codebase.
SUPPORTED_SCOPES = ("read", "write")
DEFAULT_SCOPE = "write"

GRANT_TYPES = ("authorization_code", "refresh_token")
RESPONSE_TYPES = ("code",)
# S256 only. OAuth 2.1 keeps `plain` alive solely for platforms that cannot do
# SHA-256; anything talking to an HTTP MCP server can.
CODE_CHALLENGE_METHODS = ("S256",)

_CODE_VERIFIER_RE = re.compile(r"^[A-Za-z0-9\-._~]{43,128}$")


class OAuthError(Exception):
    """An RFC 6749 §5.2 error. Carries the code the spec wants on the wire."""

    def __init__(self, error: str, description: str, status_code: int = 400):
        super().__init__(description)
        self.error = error
        self.description = description
        self.status_code = status_code

    def as_dict(self) -> dict[str, str]:
        return {"error": self.error, "error_description": self.description}


# --------------------------------------------------------------------------- #
# Hashing / random
# --------------------------------------------------------------------------- #


def sha256_hex(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _new_secret(prefix: str) -> str:
    return prefix + secrets.token_urlsafe(32)


def _now() -> datetime:
    return datetime.now(UTC)


def _aware(value: datetime | None) -> datetime | None:
    """SQLite hands back naive datetimes; every comparison here is against an aware `now`."""
    if value is None:
        return None
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


# --------------------------------------------------------------------------- #
# Issuer / resource identity
# --------------------------------------------------------------------------- #


def _normalise_base(url: str) -> str:
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, parts.path.rstrip("/"), "", ""))


def public_base_url(request) -> str:
    """The externally reachable origin of this server, without a trailing slash.

    Behind Home Assistant's ingress or any reverse proxy, what the app sees and
    what the browser typed are different URLs — and OAuth metadata that names
    the wrong one produces a flow that dead-ends at the redirect. Operators
    running behind a proxy should set ``MEALPILOT_PUBLIC_URL``; the request's
    own view is the fallback, which is correct for a direct deployment.
    """
    configured = os.environ.get("MEALPILOT_PUBLIC_URL", "").strip()
    if configured:
        return _normalise_base(configured)
    return _normalise_base(str(request.base_url))


def canonical_resource(request) -> str:
    """The RFC 8707 resource identifier of the MCP endpoint tokens are minted for."""
    return f"{public_base_url(request)}/mcp"


def resource_matches(requested: str | None, canonical: str) -> bool:
    """Compare a client-supplied ``resource`` with ours, ignoring cosmetic differences.

    Case in scheme/host and a trailing slash are not meaningful distinctions in
    a URI, and clients differ on both; the path is what actually identifies the
    resource, so that is compared exactly.
    """
    if requested is None:
        return True  # RFC 8707 makes the parameter optional
    want, have = urlsplit(requested), urlsplit(canonical)
    return (
        want.scheme.lower() == have.scheme.lower()
        and want.netloc.lower() == have.netloc.lower()
        and want.path.rstrip("/") == have.path.rstrip("/")
    )


# --------------------------------------------------------------------------- #
# Scopes
# --------------------------------------------------------------------------- #


def normalise_scope(requested: str | None) -> str:
    """Collapse a scope string to the single effective scope, defaulting to write.

    ``read`` and ``write`` are a ladder, not a set: asking for both means write.
    An unknown scope is dropped rather than rejected, which is what RFC 6749
    §3.3 allows and what keeps a client that speculatively asks for extra
    scopes from failing outright.
    """
    if requested is None:
        return DEFAULT_SCOPE
    asked = {s for s in requested.replace(",", " ").split() if s in SUPPORTED_SCOPES}
    if not asked:
        return DEFAULT_SCOPE
    return "write" if "write" in asked else "read"


# --------------------------------------------------------------------------- #
# PKCE
# --------------------------------------------------------------------------- #


def verify_pkce(verifier: str, challenge: str, method: str = "S256") -> bool:
    """Check an RFC 7636 verifier against the stored challenge.

    Only S256 is accepted. The comparison is constant-time — the challenge is a
    secret-derived value and a timing oracle on it would leak the verifier.
    """
    if method != "S256":
        return False
    if not verifier or not _CODE_VERIFIER_RE.match(verifier):
        return False
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    expected = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
    return secrets.compare_digest(expected, challenge)


# --------------------------------------------------------------------------- #
# Redirect URIs
# --------------------------------------------------------------------------- #


def is_loopback(uri: str) -> bool:
    host = (urlparse(uri).hostname or "").lower()
    return host in {"localhost", "127.0.0.1", "::1"}


def validate_redirect_uri(uri: str) -> str:
    """Accept a redirect URI for registration, or explain why not.

    HTTPS is required except on loopback (a native client cannot get a
    certificate for 127.0.0.1), and a fragment is forbidden because the
    authorization response appends its own query and a fragment would swallow
    it. Custom schemes are allowed for native clients.
    """
    parts = urlparse(uri)
    if not parts.scheme:
        raise OAuthError("invalid_redirect_uri", f"Redirect URI musi być absolutny: {uri}")
    if parts.fragment:
        raise OAuthError("invalid_redirect_uri", f"Redirect URI nie może zawierać fragmentu: {uri}")
    if parts.scheme == "http" and not is_loopback(uri):
        raise OAuthError(
            "invalid_redirect_uri",
            f"Redirect URI po http jest dozwolony tylko dla loopbacku: {uri}",
        )
    return uri


def redirect_uri_allowed(client: models.OAuthClient, uri: str) -> bool:
    """Exact string match, as OAuth 2.1 requires.

    Prefix or wildcard matching is how open redirects — and with them, code
    theft — get in. The one concession is loopback: RFC 8252 says the port of a
    native client's temporary listener is not knowable at registration time, so
    for ``http://127.0.0.1`` the port is compared loosely and everything else
    still has to match exactly.
    """
    registered = client.redirect_uris or []
    if uri in registered:
        return True
    if not is_loopback(uri):
        return False
    want = urlparse(uri)
    for candidate in registered:
        have = urlparse(candidate)
        if not is_loopback(candidate):
            continue
        if (want.scheme, want.hostname, want.path) == (have.scheme, have.hostname, have.path):
            return True
    return False


# --------------------------------------------------------------------------- #
# Client registration / authentication
# --------------------------------------------------------------------------- #


def register_client(
    db: Session,
    *,
    client_name: str,
    redirect_uris: list[str],
    grant_types: list[str] | None = None,
    scope: str | None = None,
    token_endpoint_auth_method: str = "none",
) -> tuple[models.OAuthClient, str | None]:
    """Create a client row. Returns (client, plaintext secret or None for public clients)."""
    if not redirect_uris:
        raise OAuthError("invalid_redirect_uri", "Wymagany co najmniej jeden redirect_uri.")
    for uri in redirect_uris:
        validate_redirect_uri(uri)

    requested_grants = grant_types or ["authorization_code", "refresh_token"]
    unsupported = [g for g in requested_grants if g not in GRANT_TYPES]
    if unsupported:
        raise OAuthError(
            "invalid_client_metadata",
            f"Nieobsługiwane grant_types: {', '.join(unsupported)}. Dozwolone: {', '.join(GRANT_TYPES)}.",
        )

    if token_endpoint_auth_method not in {"none", "client_secret_post", "client_secret_basic"}:
        raise OAuthError(
            "invalid_client_metadata",
            f"Nieobsługiwana metoda uwierzytelnienia klienta: {token_endpoint_auth_method}.",
        )

    secret: str | None = None
    secret_hash: str | None = None
    if token_endpoint_auth_method != "none":
        secret = _new_secret(CLIENT_SECRET_PREFIX)
        secret_hash = sha256_hex(secret)

    client = models.OAuthClient(
        client_id=_new_secret(CLIENT_ID_PREFIX),
        client_secret_hash=secret_hash,
        client_name=(client_name or "").strip()[:200],
        redirect_uris=list(redirect_uris),
        grant_types=requested_grants,
        scope=normalise_scope(scope),
        token_endpoint_auth_method=token_endpoint_auth_method,
    )
    db.add(client)
    db.commit()
    db.refresh(client)
    return client, secret


def get_client(db: Session, client_id: str | None) -> models.OAuthClient:
    if not client_id:
        raise OAuthError("invalid_client", "Brak client_id.", status_code=401)
    client = db.query(models.OAuthClient).filter(models.OAuthClient.client_id == client_id).one_or_none()
    if client is None:
        raise OAuthError("invalid_client", "Nieznany client_id.", status_code=401)
    return client


def authenticate_client(db: Session, client_id: str | None, client_secret: str | None) -> models.OAuthClient:
    """Resolve the client on the token endpoint and check its secret if it has one."""
    client = get_client(db, client_id)
    if client.client_secret_hash is None:
        # Public client: PKCE is the proof, and a secret sent anyway is ignored
        # rather than trusted — accepting it would let a caller pick which
        # authentication method to be judged by.
        return client
    if not client_secret or not secrets.compare_digest(sha256_hex(client_secret), client.client_secret_hash):
        raise OAuthError("invalid_client", "Nieprawidłowy client_secret.", status_code=401)
    return client


# --------------------------------------------------------------------------- #
# Authorization codes
# --------------------------------------------------------------------------- #


def issue_code(
    db: Session,
    *,
    client: models.OAuthClient,
    user_id: int,
    redirect_uri: str,
    code_challenge: str,
    code_challenge_method: str,
    scope: str,
    resource: str | None,
) -> str:
    code = _new_secret(CODE_PREFIX)
    db.add(
        models.OAuthAuthCode(
            code_hash=sha256_hex(code),
            client_id=client.client_id,
            user_id=user_id,
            redirect_uri=redirect_uri,
            code_challenge=code_challenge,
            code_challenge_method=code_challenge_method,
            scope=scope,
            resource=resource,
            expires_at=_now() + CODE_TTL,
        )
    )
    db.commit()
    return code


@dataclass(frozen=True)
class CodeGrant:
    """A redeemed authorization code, read out before the row was destroyed."""

    user_id: int
    client_id: str
    redirect_uri: str
    code_challenge: str
    code_challenge_method: str
    scope: str
    resource: str | None
    expires_at: datetime


def consume_code(
    db: Session,
    *,
    code: str,
    client: models.OAuthClient,
    redirect_uri: str | None,
    code_verifier: str | None,
) -> CodeGrant:
    """Redeem an authorization code exactly once.

    The row is deleted before anything is issued from it, so two concurrent
    redemptions cannot both succeed. Every binding the code carries is checked:
    the client that redeems it must be the one it was issued to, the redirect
    URI must be the one the user actually saw, and the PKCE verifier must match.

    The fields are copied out *before* the delete: committing a deletion expires
    the instance and detaches it, so reading an attribute afterwards would raise
    instead of validating.
    """
    row = (
        db.query(models.OAuthAuthCode).filter(models.OAuthAuthCode.code_hash == sha256_hex(code)).one_or_none()
        if code
        else None
    )
    if row is None:
        raise OAuthError("invalid_grant", "Kod autoryzacyjny jest nieprawidłowy lub został już użyty.")

    grant = CodeGrant(
        user_id=row.user_id,
        client_id=row.client_id,
        redirect_uri=row.redirect_uri,
        code_challenge=row.code_challenge,
        code_challenge_method=row.code_challenge_method,
        scope=row.scope,
        resource=row.resource,
        expires_at=_aware(row.expires_at),
    )
    # Whatever happens next, this code is spent.
    db.delete(row)
    db.commit()

    if grant.expires_at < _now():
        raise OAuthError("invalid_grant", "Kod autoryzacyjny wygasł.")
    if grant.client_id != client.client_id:
        raise OAuthError("invalid_grant", "Kod autoryzacyjny został wydany innemu klientowi.")
    if redirect_uri is not None and redirect_uri != grant.redirect_uri:
        raise OAuthError("invalid_grant", "redirect_uri nie zgadza się z tym użytym przy autoryzacji.")
    if not verify_pkce(code_verifier or "", grant.code_challenge, grant.code_challenge_method):
        raise OAuthError("invalid_grant", "Nieprawidłowy code_verifier (PKCE).")
    return grant


# --------------------------------------------------------------------------- #
# Tokens
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class IssuedTokens:
    access_token: str
    refresh_token: str
    expires_in: int
    scope: str


def issue_tokens(
    db: Session,
    *,
    client_id: str,
    user_id: int,
    scope: str,
    resource: str | None,
) -> IssuedTokens:
    access = _new_secret(TOKEN_PREFIX)
    refresh = _new_secret(REFRESH_PREFIX)
    now = _now()
    db.add(
        models.OAuthToken(
            token_hash=sha256_hex(access),
            kind="access",
            client_id=client_id,
            user_id=user_id,
            scope=scope,
            resource=resource,
            expires_at=now + ACCESS_TOKEN_TTL,
        )
    )
    db.add(
        models.OAuthToken(
            token_hash=sha256_hex(refresh),
            kind="refresh",
            client_id=client_id,
            user_id=user_id,
            scope=scope,
            resource=resource,
            expires_at=now + REFRESH_TOKEN_TTL,
        )
    )
    db.commit()
    return IssuedTokens(
        access_token=access,
        refresh_token=refresh,
        expires_in=int(ACCESS_TOKEN_TTL.total_seconds()),
        scope=scope,
    )


def rotate_refresh_token(
    db: Session,
    *,
    refresh_token: str,
    client: models.OAuthClient,
    scope_request: str | None,
) -> IssuedTokens:
    """Exchange a refresh token for a new pair, invalidating the old one.

    Rotation is mandatory for public clients in OAuth 2.1: the token lives in a
    client that cannot keep a secret, so the only defence left is making a
    stolen copy useless the moment the legitimate holder uses theirs.

    A downgrade (write -> read) is honoured; an upgrade is refused, because a
    refresh must never widen what the user originally approved.
    """
    row = (
        db.query(models.OAuthToken)
        .filter(
            models.OAuthToken.token_hash == sha256_hex(refresh_token),
            models.OAuthToken.kind == "refresh",
        )
        .one_or_none()
        if refresh_token
        else None
    )
    if row is None:
        raise OAuthError("invalid_grant", "Refresh token jest nieprawidłowy lub został już użyty.")

    # Read before deleting: the commit below detaches the instance.
    owner_id, owner_client, granted_scope = row.user_id, row.client_id, row.scope
    resource, expires_at = row.resource, _aware(row.expires_at)

    db.delete(row)
    db.commit()

    if expires_at < _now():
        raise OAuthError("invalid_grant", "Refresh token wygasł.")
    if owner_client != client.client_id:
        raise OAuthError("invalid_grant", "Refresh token należy do innego klienta.")

    scope = granted_scope
    if scope_request is not None:
        requested = normalise_scope(scope_request)
        if requested == "write" and granted_scope == "read":
            raise OAuthError("invalid_scope", "Odświeżenie nie może rozszerzyć zakresu.")
        scope = requested

    return issue_tokens(
        db,
        client_id=owner_client,
        user_id=owner_id,
        scope=scope,
        resource=resource,
    )


@dataclass(frozen=True)
class AccessGrant:
    """A validated access token, reduced to what the MCP layer needs."""

    user_id: int
    scope: str
    client_id: str


_LAST_USED_THROTTLE = timedelta(seconds=60)


def verify_access_token(db: Session, token: str, *, resource: str) -> AccessGrant:
    """Resolve a bearer token to its owner, or raise ``invalid_token``.

    ``resource`` is the audience this call arrived at. A token minted for a
    different MCP server is rejected here even if it is otherwise perfectly
    valid — that check is the whole point of RFC 8707 and the reason a
    malicious server cannot forward a token it collected to this one.
    """
    row = (
        db.query(models.OAuthToken)
        .filter(models.OAuthToken.token_hash == sha256_hex(token), models.OAuthToken.kind == "access")
        .one_or_none()
        if token
        else None
    )
    if row is None:
        raise OAuthError("invalid_token", "Nieprawidłowy token dostępu.", status_code=401)
    if _aware(row.expires_at) < _now():
        db.delete(row)
        db.commit()
        raise OAuthError("invalid_token", "Token dostępu wygasł.", status_code=401)
    if not resource_matches(row.resource, resource):
        raise OAuthError("invalid_token", "Token został wydany dla innego zasobu.", status_code=401)

    last_used = _aware(row.last_used_at)
    now = _now()
    if last_used is None or (now - last_used) > _LAST_USED_THROTTLE:
        row.last_used_at = now
        db.commit()

    return AccessGrant(user_id=row.user_id, scope=row.scope or DEFAULT_SCOPE, client_id=row.client_id)


def revoke_token(db: Session, token: str) -> bool:
    """Drop a token by value, whichever kind it is. Idempotent, per RFC 7009."""
    digest = sha256_hex(token) if token else ""
    deleted = db.query(models.OAuthToken).filter(models.OAuthToken.token_hash == digest).delete()
    db.commit()
    return bool(deleted)


def revoke_all_for_user(db: Session, user_id: int) -> None:
    """Cut every OAuth grant a user has. Used on password change and account deletion."""
    db.query(models.OAuthToken).filter(models.OAuthToken.user_id == user_id).delete(synchronize_session=False)
    db.query(models.OAuthAuthCode).filter(models.OAuthAuthCode.user_id == user_id).delete(synchronize_session=False)


def purge_expired(db: Session) -> int:
    """Housekeeping: forget codes and tokens that can no longer be used."""
    now = _now()
    removed = db.query(models.OAuthAuthCode).filter(models.OAuthAuthCode.expires_at < now).delete()
    removed += db.query(models.OAuthToken).filter(models.OAuthToken.expires_at < now).delete()
    db.commit()
    return removed


# --------------------------------------------------------------------------- #
# End-user authentication for the consent screen
# --------------------------------------------------------------------------- #


def authenticate_user(db: Session, username: str, password: str) -> models.User | None:
    """Password check for the consent screen, timing-equalised like ``/api/auth/login``.

    Without the dummy hash on the miss path, the response time distinguishes
    "no such account" from "wrong password" — and this endpoint is reachable by
    anyone who can start an authorization flow.
    """
    user = db.query(models.User).filter(models.User.username == username.strip()).one_or_none()
    if not user or not user.is_active:
        dummy_verify(password)
        return None
    if not verify_password(password, user.password_hash):
        return None
    return user
