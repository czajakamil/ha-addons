"""HTTP surface of the OAuth 2.1 authorization server. Logic lives in ``app/oauth.py``.

Endpoint map
------------
``GET  /.well-known/oauth-protected-resource[/mcp]``  RFC 9728 — "who authorizes me"
``GET  /.well-known/oauth-authorization-server``      RFC 8414 — "here are my endpoints"
``POST /oauth/register``                              RFC 7591 — dynamic client registration
``GET  /oauth/authorize``                             login + consent screen
``POST /oauth/authorize``                             consent submitted -> redirect with code
``POST /oauth/token``                                 code / refresh -> access token
``POST /oauth/revoke``                                RFC 7009

Error handling on ``/authorize`` follows RFC 6749 §4.1.2.1, which splits on
whether the redirect target can be trusted. If ``client_id`` or ``redirect_uri``
is bad, the error is rendered *to the user* — bouncing it to an unverified URI
is how an open redirect turns into a phishing primitive. Everything else is
returned to the client as query parameters on its registered redirect URI.

The consent screen always asks for the password, even when a MealPilot session
cookie is already present. The flow is started by a third party, and what it
hands out is a credential that outlives the browser session; a deliberate
re-authentication is the point at which the user actually sees who is asking.
"""

from __future__ import annotations

import base64
import binascii
import html
import secrets
from dataclasses import dataclass
from datetime import UTC
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, Form, HTTPException, Request, status
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from sqlalchemy.orm import Session

from .. import models, oauth
from ..db import get_db
from ..ratelimit import login_limiter, oauth_register_limiter, oauth_token_limiter

router = APIRouter(tags=["oauth"])

CSRF_SESSION_KEY = "oauth_csrf"


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _client_ip(request: Request) -> str:
    cf = request.headers.get("cf-connecting-ip")
    if cf:
        return cf.strip()
    fwd = request.headers.get("x-forwarded-for")
    if fwd:
        return fwd.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def _throttle(limiter, key: str, what: str) -> None:
    allowed, retry_after = limiter.check(key)
    if not allowed:
        raise HTTPException(
            status.HTTP_429_TOO_MANY_REQUESTS,
            f"Za dużo żądań ({what}). Spróbuj ponownie za {int(retry_after)} s.",
            headers={"Retry-After": str(int(retry_after))},
        )


def _oauth_error_response(exc: oauth.OAuthError) -> JSONResponse:
    """Token/registration errors are JSON with no-store, per RFC 6749 §5.1."""
    headers = {"Cache-Control": "no-store", "Pragma": "no-cache"}
    if exc.status_code == 401:
        headers["WWW-Authenticate"] = 'Basic realm="MealPilot"'
    return JSONResponse(exc.as_dict(), status_code=exc.status_code, headers=headers)


_NO_STORE = {"Cache-Control": "no-store", "Pragma": "no-cache"}
_METADATA_CACHE = {"Cache-Control": "public, max-age=3600"}


# --------------------------------------------------------------------------- #
# Discovery
# --------------------------------------------------------------------------- #


def _protected_resource_metadata(request: Request) -> dict:
    return {
        "resource": oauth.canonical_resource(request),
        "authorization_servers": [oauth.public_base_url(request)],
        "scopes_supported": list(oauth.SUPPORTED_SCOPES),
        "bearer_methods_supported": ["header"],
        "resource_name": "MealPilot MCP",
    }


@router.get("/.well-known/oauth-protected-resource")
def protected_resource_metadata(request: Request):
    return JSONResponse(_protected_resource_metadata(request), headers=_METADATA_CACHE)


@router.get("/.well-known/oauth-protected-resource/mcp")
def protected_resource_metadata_for_mcp(request: Request):
    """RFC 9728 puts the resource's own path *after* the well-known segment.

    The resource is ``<base>/mcp``, so its metadata canonically lives at
    ``<base>/.well-known/oauth-protected-resource/mcp`` — that is the URL the
    401 from ``/mcp`` advertises, and the one a spec-following client fetches.
    """
    return JSONResponse(_protected_resource_metadata(request), headers=_METADATA_CACHE)


def _authorization_server_metadata(request: Request) -> dict:
    base = oauth.public_base_url(request)
    return {
        "issuer": base,
        "authorization_endpoint": f"{base}/oauth/authorize",
        "token_endpoint": f"{base}/oauth/token",
        "registration_endpoint": f"{base}/oauth/register",
        "revocation_endpoint": f"{base}/oauth/revoke",
        "scopes_supported": list(oauth.SUPPORTED_SCOPES),
        "response_types_supported": list(oauth.RESPONSE_TYPES),
        "grant_types_supported": list(oauth.GRANT_TYPES),
        "code_challenge_methods_supported": list(oauth.CODE_CHALLENGE_METHODS),
        "token_endpoint_auth_methods_supported": ["none", "client_secret_post", "client_secret_basic"],
        "revocation_endpoint_auth_methods_supported": ["none", "client_secret_post", "client_secret_basic"],
    }


@router.get("/.well-known/oauth-authorization-server")
def authorization_server_metadata(request: Request):
    return JSONResponse(_authorization_server_metadata(request), headers=_METADATA_CACHE)


@router.get("/.well-known/openid-configuration")
def openid_configuration(request: Request):
    """Not an OpenID provider — but several MCP clients probe here first.

    Serving the same document costs nothing and saves a discovery round trip
    that would otherwise 404 and, in some clients, abort the flow outright.
    """
    return JSONResponse(_authorization_server_metadata(request), headers=_METADATA_CACHE)


# --------------------------------------------------------------------------- #
# Dynamic client registration (RFC 7591)
# --------------------------------------------------------------------------- #


@router.post("/oauth/register", status_code=status.HTTP_201_CREATED)
async def register(request: Request, db: Session = Depends(get_db)):
    """Open registration: anyone who can reach this can mint a client id.

    That is the intended posture — it is what lets claude.ai enrol itself — and
    it grants nothing on its own. A client id is not a credential: it can read
    nothing until a human has signed in on the consent screen and approved it.
    The rate limit is what stops the table being flooded.
    """
    _throttle(oauth_register_limiter, _client_ip(request), "rejestracji klientów OAuth")

    try:
        payload = await request.json()
    except (ValueError, UnicodeDecodeError):
        payload = None
    if not isinstance(payload, dict):
        return _oauth_error_response(oauth.OAuthError("invalid_client_metadata", "Body musi być obiektem JSON."))

    redirect_uris = payload.get("redirect_uris")
    if not isinstance(redirect_uris, list) or not all(isinstance(u, str) for u in redirect_uris):
        return _oauth_error_response(
            oauth.OAuthError("invalid_redirect_uri", "Pole redirect_uris musi być listą adresów URL.")
        )

    grant_types = payload.get("grant_types")
    if grant_types is not None and (
        not isinstance(grant_types, list) or not all(isinstance(g, str) for g in grant_types)
    ):
        return _oauth_error_response(oauth.OAuthError("invalid_client_metadata", "grant_types musi być listą."))

    try:
        client, secret = oauth.register_client(
            db,
            client_name=str(payload.get("client_name") or ""),
            redirect_uris=redirect_uris,
            grant_types=grant_types,
            scope=payload.get("scope"),
            token_endpoint_auth_method=str(payload.get("token_endpoint_auth_method") or "none"),
        )
    except oauth.OAuthError as exc:
        return _oauth_error_response(exc)

    issued_at = client.created_at
    body = {
        "client_id": client.client_id,
        "client_name": client.client_name,
        "redirect_uris": client.redirect_uris,
        "grant_types": client.grant_types,
        "response_types": list(oauth.RESPONSE_TYPES),
        "token_endpoint_auth_method": client.token_endpoint_auth_method,
        "scope": " ".join(oauth.SUPPORTED_SCOPES),
        "client_id_issued_at": int(
            (issued_at if issued_at.tzinfo else issued_at.replace(tzinfo=UTC)).timestamp(),
        ),
    }
    if secret is not None:
        body["client_secret"] = secret
        body["client_secret_expires_at"] = 0  # 0 = never expires (RFC 7591 §3.2.1)
    return JSONResponse(body, status_code=status.HTTP_201_CREATED, headers=_NO_STORE)


# --------------------------------------------------------------------------- #
# Authorization endpoint
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class _AuthorizeRequest:
    """The validated parameters of one authorization request."""

    client: models.OAuthClient
    redirect_uri: str
    scope: str
    state: str | None
    challenge: str
    method: str
    resource: str | None


class _UntrustedRedirectError(Exception):
    """The redirect target could not be verified, so the error must be shown to the user."""

    def __init__(self, detail: str):
        super().__init__(detail)
        self.detail = detail


class _RedirectableError(Exception):
    """An error the client is entitled to receive on its verified redirect URI."""

    def __init__(self, redirect_uri: str, state: str | None, error: oauth.OAuthError):
        super().__init__(error.description)
        self.redirect_uri = redirect_uri
        self.state = state
        self.error = error


def _resolve_client_and_redirect(db: Session, params) -> tuple:
    """First half of validation: the part whose failure must NOT be redirected."""
    try:
        client = oauth.get_client(db, params.get("client_id"))
    except oauth.OAuthError as exc:
        raise _UntrustedRedirectError(exc.description) from exc

    redirect_uri = params.get("redirect_uri")
    if not redirect_uri:
        registered = client.redirect_uris or []
        if len(registered) != 1:
            raise _UntrustedRedirectError("Wymagany parametr redirect_uri (klient ma zarejestrowanych kilka adresów).")
        redirect_uri = registered[0]
    if not oauth.redirect_uri_allowed(client, redirect_uri):
        raise _UntrustedRedirectError("Podany redirect_uri nie jest zarejestrowany dla tego klienta.")
    return client, redirect_uri


def _validate_authorize(request: Request, db: Session, params) -> _AuthorizeRequest:
    """Second half: failures here are safe to hand back to the client's redirect URI."""
    client, redirect_uri = _resolve_client_and_redirect(db, params)
    state = params.get("state")

    def fail(error: str, description: str):
        return oauth.OAuthError(error, description)

    if params.get("response_type") != "code":
        raise _RedirectableError(redirect_uri, state, fail("unsupported_response_type", "Obsługiwany jest tylko code."))

    challenge = params.get("code_challenge")
    method = params.get("code_challenge_method") or "plain"
    if not challenge:
        raise _RedirectableError(
            redirect_uri,
            state,
            fail("invalid_request", "PKCE jest wymagane — brak code_challenge."),
        )
    if method not in oauth.CODE_CHALLENGE_METHODS:
        raise _RedirectableError(
            redirect_uri,
            state,
            fail("invalid_request", f"Obsługiwane code_challenge_method: {', '.join(oauth.CODE_CHALLENGE_METHODS)}."),
        )

    resource = params.get("resource")
    if not oauth.resource_matches(resource, oauth.canonical_resource(request)):
        raise _RedirectableError(
            redirect_uri,
            state,
            fail("invalid_target", "Parametr resource nie wskazuje na ten serwer MCP."),
        )

    if "authorization_code" not in (client.grant_types or []):
        raise _RedirectableError(
            redirect_uri,
            state,
            fail("unauthorized_client", "Klient nie ma zarejestrowanego grantu authorization_code."),
        )

    return _AuthorizeRequest(
        client=client,
        redirect_uri=redirect_uri,
        scope=oauth.normalise_scope(params.get("scope")),
        state=state,
        challenge=challenge,
        method=method,
        resource=resource,
    )


def _redirect_with(redirect_uri: str, params: dict, state: str | None) -> RedirectResponse:
    if state:
        params = {**params, "state": state}
    joiner = "&" if "?" in redirect_uri else "?"
    return RedirectResponse(
        f"{redirect_uri}{joiner}{urlencode(params)}",
        status_code=status.HTTP_303_SEE_OTHER,
        headers=_NO_STORE,
    )


@router.get("/oauth/authorize")
def authorize_form(request: Request, db: Session = Depends(get_db)):
    params = dict(request.query_params)
    try:
        parsed = _validate_authorize(request, db, params)
    except _UntrustedRedirectError as exc:
        return _error_page("Nie można autoryzować", exc.detail)
    except _RedirectableError as exc:
        return _redirect_with(
            exc.redirect_uri,
            {"error": exc.error.error, "error_description": exc.error.description},
            exc.state,
        )

    csrf = secrets.token_urlsafe(32)
    request.session[CSRF_SESSION_KEY] = csrf
    return _consent_page(request, parsed, csrf, params)


@router.post("/oauth/authorize")
def authorize_submit(
    request: Request,
    db: Session = Depends(get_db),
    username: str = Form(default=""),
    password: str = Form(default=""),
    csrf: str = Form(default=""),
    action: str = Form(default="approve"),
    client_id: str = Form(default=""),
    redirect_uri: str = Form(default=""),
    response_type: str = Form(default="code"),
    scope: str = Form(default=""),
    state: str = Form(default=""),
    code_challenge: str = Form(default=""),
    code_challenge_method: str = Form(default="S256"),
    resource: str = Form(default=""),
):
    params = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "response_type": response_type,
        "scope": scope or None,
        "state": state or None,
        "code_challenge": code_challenge,
        "code_challenge_method": code_challenge_method,
        "resource": resource or None,
    }

    try:
        parsed = _validate_authorize(request, db, params)
    except _UntrustedRedirectError as exc:
        return _error_page("Nie można autoryzować", exc.detail)
    except _RedirectableError as exc:
        return _redirect_with(
            exc.redirect_uri,
            {"error": exc.error.error, "error_description": exc.error.description},
            exc.state,
        )

    expected = request.session.get(CSRF_SESSION_KEY)
    if not expected or not csrf or not secrets.compare_digest(csrf, expected):
        return _error_page(
            "Sesja formularza wygasła",
            "Otwórz link autoryzacyjny ponownie — token formularza jest nieaktualny.",
            status_code=status.HTTP_400_BAD_REQUEST,
        )

    if action != "approve":
        request.session.pop(CSRF_SESSION_KEY, None)
        return _redirect_with(
            parsed.redirect_uri,
            {"error": "access_denied", "error_description": "Użytkownik odrzucił dostęp."},
            parsed.state,
        )

    ip = _client_ip(request)
    rate_key = f"{ip}::{username.strip().lower()}"
    allowed, retry_after = login_limiter.check(rate_key)
    if not allowed:
        return _consent_page(
            request,
            parsed,
            csrf,
            params,
            error=f"Za dużo prób logowania. Spróbuj ponownie za {int(retry_after)} s.",
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        )

    user = oauth.authenticate_user(db, username, password)
    if user is None:
        return _consent_page(
            request,
            parsed,
            csrf,
            params,
            error="Nieprawidłowa nazwa użytkownika lub hasło.",
            status_code=status.HTTP_401_UNAUTHORIZED,
        )

    login_limiter.reset(rate_key)
    request.session.pop(CSRF_SESSION_KEY, None)

    code = oauth.issue_code(
        db,
        client=parsed.client,
        user_id=user.id,
        redirect_uri=parsed.redirect_uri,
        code_challenge=parsed.challenge,
        code_challenge_method=parsed.method,
        scope=parsed.scope,
        resource=parsed.resource or oauth.canonical_resource(request),
    )
    return _redirect_with(parsed.redirect_uri, {"code": code}, parsed.state)


# --------------------------------------------------------------------------- #
# Token endpoint
# --------------------------------------------------------------------------- #


def _basic_auth(request: Request) -> tuple[str | None, str | None]:
    """Client credentials from an ``Authorization: Basic`` header, if present."""
    header = request.headers.get("authorization", "")
    if not header.lower().startswith("basic "):
        return None, None
    try:
        raw = base64.b64decode(header[6:].strip(), validate=True).decode("utf-8")
    except (binascii.Error, UnicodeDecodeError, ValueError):
        return None, None
    client_id, _, client_secret = raw.partition(":")
    return client_id or None, client_secret or None


@router.post("/oauth/token")
def token(
    request: Request,
    db: Session = Depends(get_db),
    grant_type: str = Form(default=""),
    code: str = Form(default=""),
    redirect_uri: str = Form(default=""),
    code_verifier: str = Form(default=""),
    refresh_token: str = Form(default=""),
    client_id: str = Form(default=""),
    client_secret: str = Form(default=""),
    scope: str = Form(default=""),
    resource: str = Form(default=""),
):
    _throttle(oauth_token_limiter, _client_ip(request), "żądań tokenu OAuth")

    basic_id, basic_secret = _basic_auth(request)
    effective_id = client_id or basic_id
    effective_secret = client_secret or basic_secret

    try:
        client = oauth.authenticate_client(db, effective_id, effective_secret)

        if grant_type == "authorization_code":
            grant = oauth.consume_code(
                db,
                code=code,
                client=client,
                redirect_uri=redirect_uri or None,
                code_verifier=code_verifier,
            )
            if resource and not oauth.resource_matches(resource, oauth.canonical_resource(request)):
                raise oauth.OAuthError("invalid_target", "Parametr resource nie wskazuje na ten serwer MCP.")
            issued = oauth.issue_tokens(
                db,
                client_id=client.client_id,
                user_id=grant.user_id,
                scope=grant.scope,
                resource=grant.resource,
            )
        elif grant_type == "refresh_token":
            if "refresh_token" not in (client.grant_types or []):
                raise oauth.OAuthError("unauthorized_client", "Klient nie ma zarejestrowanego grantu refresh_token.")
            issued = oauth.rotate_refresh_token(
                db,
                refresh_token=refresh_token,
                client=client,
                scope_request=scope or None,
            )
        else:
            raise oauth.OAuthError(
                "unsupported_grant_type",
                f"Obsługiwane grant_type: {', '.join(oauth.GRANT_TYPES)}.",
            )
    except oauth.OAuthError as exc:
        return _oauth_error_response(exc)

    return JSONResponse(
        {
            "access_token": issued.access_token,
            "token_type": "Bearer",
            "expires_in": issued.expires_in,
            "refresh_token": issued.refresh_token,
            "scope": issued.scope,
        },
        headers=_NO_STORE,
    )


@router.post("/oauth/revoke")
def revoke(
    request: Request,
    db: Session = Depends(get_db),
    token: str = Form(default=""),
    client_id: str = Form(default=""),
    client_secret: str = Form(default=""),
):
    """RFC 7009. Always 200, even for an unknown token — the caller learns nothing."""
    _throttle(oauth_token_limiter, _client_ip(request), "żądań tokenu OAuth")
    basic_id, basic_secret = _basic_auth(request)
    try:
        oauth.authenticate_client(db, client_id or basic_id, client_secret or basic_secret)
    except oauth.OAuthError as exc:
        return _oauth_error_response(exc)
    oauth.revoke_token(db, token)
    return JSONResponse({}, headers=_NO_STORE)


# --------------------------------------------------------------------------- #
# Pages
# --------------------------------------------------------------------------- #

_PAGE = """<!doctype html>
<html lang="pl"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{title} — MealPilot</title>
<style>
  :root {{ color-scheme: light dark; --bg:#f6f7f9; --card:#fff; --fg:#16181d;
           --muted:#5c6370; --line:#dfe3e8; --accent:#2f6f4f; --err:#b3261e; }}
  @media (prefers-color-scheme: dark) {{
    :root {{ --bg:#14161a; --card:#1c1f25; --fg:#e8eaed; --muted:#9aa0a8;
             --line:#2c3038; --accent:#5fbf8c; --err:#f2b8b5; }}
  }}
  * {{ box-sizing:border-box; }}
  body {{ margin:0; min-height:100vh; display:flex; align-items:center;
          justify-content:center; padding:24px; background:var(--bg); color:var(--fg);
          font:15px/1.5 system-ui,-apple-system,"Segoe UI",sans-serif; }}
  .card {{ width:100%; max-width:420px; background:var(--card); border:1px solid var(--line);
           border-radius:14px; padding:28px; }}
  h1 {{ margin:0 0 6px; font-size:20px; }}
  p {{ margin:0 0 16px; color:var(--muted); }}
  .scopes {{ border:1px solid var(--line); border-radius:10px; padding:12px 14px; margin:0 0 18px; }}
  .scopes li {{ margin:2px 0; }}
  ul {{ margin:6px 0 0; padding-left:20px; }}
  label {{ display:block; font-size:13px; color:var(--muted); margin:12px 0 4px; }}
  input[type=text], input[type=password] {{ width:100%; padding:9px 11px; border-radius:8px;
    border:1px solid var(--line); background:var(--bg); color:var(--fg); font-size:15px; }}
  .row {{ display:flex; gap:10px; margin-top:22px; }}
  button {{ flex:1; padding:10px 14px; border-radius:8px; border:1px solid var(--line);
            font-size:15px; cursor:pointer; background:transparent; color:var(--fg); }}
  button.primary {{ background:var(--accent); border-color:var(--accent); color:#fff; font-weight:600; }}
  .err {{ color:var(--err); font-size:14px; margin:12px 0 0; }}
  code {{ font-size:13px; word-break:break-all; }}
</style></head><body><div class="card">{body}</div></body></html>
"""


def _error_page(title: str, detail: str, status_code: int = status.HTTP_400_BAD_REQUEST) -> HTMLResponse:
    body = f"<h1>{html.escape(title)}</h1><p class='err'>{html.escape(detail)}</p>"
    return HTMLResponse(_PAGE.format(title=html.escape(title), body=body), status_code=status_code, headers=_NO_STORE)


_SCOPE_TEXT = {
    "read": ["przeglądać przepisy, plan tygodnia i listę zakupów"],
    "write": [
        "przeglądać przepisy, plan tygodnia i listę zakupów",
        "dodawać i zmieniać przepisy, plan oraz listę zakupów",
    ],
}


def _hidden(name: str, value) -> str:
    if value in (None, ""):
        return ""
    return f'<input type="hidden" name="{html.escape(name)}" value="{html.escape(str(value))}">'


def _consent_page(
    request: Request,
    parsed: _AuthorizeRequest,
    csrf: str,
    params: dict,
    error: str | None = None,
    status_code: int = status.HTTP_200_OK,
) -> HTMLResponse:
    # Absolute, and built from the same base the metadata advertises. A relative
    # action would resolve against /oauth/authorize and post to /oauth/oauth/authorize.
    action_url = f"{oauth.public_base_url(request)}/oauth/authorize"
    name = parsed.client.client_name or parsed.client.client_id
    permissions = "".join(f"<li>{html.escape(p)}</li>" for p in _SCOPE_TEXT[parsed.scope])
    hidden = "".join(
        [
            _hidden("csrf", csrf),
            _hidden("client_id", parsed.client.client_id),
            _hidden("redirect_uri", parsed.redirect_uri),
            _hidden("response_type", "code"),
            _hidden("scope", parsed.scope),
            _hidden("state", params.get("state")),
            _hidden("code_challenge", parsed.challenge),
            _hidden("code_challenge_method", parsed.method),
            _hidden("resource", parsed.resource),
        ]
    )
    error_html = f"<p class='err'>{html.escape(error)}</p>" if error else ""
    body = f"""
      <h1>Połącz z MealPilot</h1>
      <p><strong>{html.escape(name)}</strong> prosi o dostęp do Twojego konta.</p>
      <div class="scopes">Aplikacja będzie mogła:<ul>{permissions}</ul></div>
      <form method="post" action="{html.escape(action_url)}" autocomplete="off">
        {hidden}
        <label for="u">Nazwa użytkownika</label>
        <input id="u" name="username" type="text" autocomplete="username" autofocus required>
        <label for="p">Hasło</label>
        <input id="p" name="password" type="password" autocomplete="current-password" required>
        {error_html}
        <div class="row">
          <button type="submit" name="action" value="deny">Odrzuć</button>
          <button type="submit" name="action" value="approve" class="primary">Zezwól</button>
        </div>
      </form>
    """
    return HTMLResponse(
        _PAGE.format(title="Autoryzacja", body=body),
        status_code=status_code,
        headers=_NO_STORE,
    )
