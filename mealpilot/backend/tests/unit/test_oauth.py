"""Prymitywy OAuth 2.1: PKCE, zakresy, redirect_uri, dopasowanie zasobu.

To są rzeczy, które w przepływie E2E przechodzą tylko „ścieżką szczęśliwą",
a każda z nich sama w sobie jest bramką bezpieczeństwa — dlatego mają własne
testy na krawędzie, a nie tylko pokrycie z testu integracyjnego.
"""

import base64
import hashlib

import pytest

from app import oauth

pytestmark = pytest.mark.unit


def _challenge(verifier: str) -> str:
    digest = hashlib.sha256(verifier.encode()).digest()
    return base64.urlsafe_b64encode(digest).decode().rstrip("=")


# --------------------------------------------------------------------------- #
# PKCE
# --------------------------------------------------------------------------- #

VERIFIER = "a" * 64


def test_pkce_accepts_the_matching_verifier():
    assert oauth.verify_pkce(VERIFIER, _challenge(VERIFIER)) is True


def test_pkce_rejects_a_different_verifier():
    assert oauth.verify_pkce("b" * 64, _challenge(VERIFIER)) is False


def test_pkce_rejects_the_plain_method():
    """`plain` przechodzi przez podsłuchany kanał — OAuth 2.1 zostawia tylko S256."""
    assert oauth.verify_pkce(VERIFIER, VERIFIER, method="plain") is False


def test_pkce_rejects_an_empty_verifier():
    assert oauth.verify_pkce("", _challenge(VERIFIER)) is False


@pytest.mark.parametrize("verifier", ["short", "x" * 42, "x" * 129, "ma spacje" + "x" * 40])
def test_pkce_rejects_verifiers_outside_the_rfc_grammar(verifier):
    """RFC 7636 wymaga 43–128 znaków z ograniczonego alfabetu; krótki verifier to słaby sekret."""
    assert oauth.verify_pkce(verifier, _challenge(verifier)) is False


def test_pkce_challenge_is_unpadded_base64url():
    """Padding `=` w challenge to najczęstszy błąd implementacji — musi się nie zgadzać."""
    padded = base64.urlsafe_b64encode(hashlib.sha256(VERIFIER.encode()).digest()).decode()
    assert padded.endswith("=")
    assert oauth.verify_pkce(VERIFIER, padded) is False


# --------------------------------------------------------------------------- #
# Zakresy
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("requested", "expected"),
    [
        (None, "write"),
        ("", "write"),
        ("read", "read"),
        ("write", "write"),
        ("read write", "write"),
        ("write read", "write"),
        ("read,write", "write"),
        ("cokolwiek", "write"),
        ("read cokolwiek", "read"),
    ],
)
def test_scope_normalisation(requested, expected):
    assert oauth.normalise_scope(requested) == expected


# --------------------------------------------------------------------------- #
# Redirect URI
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "uri",
    [
        "https://claude.ai/api/mcp/auth_callback",
        "http://127.0.0.1:33418/callback",
        "http://localhost:8080/cb",
        "mealpilot://oauth/callback",
    ],
)
def test_valid_redirect_uris_are_accepted(uri):
    assert oauth.validate_redirect_uri(uri) == uri


@pytest.mark.parametrize(
    "uri",
    [
        "http://evil.example/cb",  # http poza loopbackiem
        "/tylko/sciezka",  # nie jest absolutny
        "https://ok.example/cb#fragment",  # fragment zjadłby query z kodem
    ],
)
def test_invalid_redirect_uris_are_rejected(uri):
    with pytest.raises(oauth.OAuthError):
        oauth.validate_redirect_uri(uri)


class _Client:
    def __init__(self, uris):
        self.redirect_uris = uris


def test_redirect_uri_must_match_exactly():
    client = _Client(["https://claude.ai/api/mcp/auth_callback"])
    assert oauth.redirect_uri_allowed(client, "https://claude.ai/api/mcp/auth_callback") is True
    # Prefiks to klasyczny open redirect — musi odpaść.
    assert oauth.redirect_uri_allowed(client, "https://claude.ai/api/mcp/auth_callback/../evil") is False
    assert oauth.redirect_uri_allowed(client, "https://claude.ai.evil.test/api/mcp/auth_callback") is False
    assert oauth.redirect_uri_allowed(client, "https://claude.ai/api/mcp/auth_callback?x=1") is False


def test_loopback_port_may_differ_but_nothing_else_may():
    """RFC 8252: natywny klient nie zna swojego portu w chwili rejestracji."""
    client = _Client(["http://127.0.0.1:1234/callback"])
    assert oauth.redirect_uri_allowed(client, "http://127.0.0.1:55555/callback") is True
    assert oauth.redirect_uri_allowed(client, "http://127.0.0.1:55555/inna-sciezka") is False
    assert oauth.redirect_uri_allowed(client, "http://localhost:1234/callback") is False


# --------------------------------------------------------------------------- #
# Powiązanie z zasobem (RFC 8707)
# --------------------------------------------------------------------------- #

CANONICAL = "https://dom.example/mcp"


@pytest.mark.parametrize(
    "requested",
    [None, CANONICAL, "https://dom.example/mcp/", "https://DOM.example/mcp", "HTTPS://dom.example/mcp"],
)
def test_resource_matches_ignores_cosmetic_differences(requested):
    assert oauth.resource_matches(requested, CANONICAL) is True


@pytest.mark.parametrize(
    "requested",
    [
        "https://inny.example/mcp",  # inny serwer MCP
        "http://dom.example/mcp",  # inny schemat
        "https://dom.example/inne",  # inna ścieżka
    ],
)
def test_resource_mismatch_is_detected(requested):
    assert oauth.resource_matches(requested, CANONICAL) is False


# --------------------------------------------------------------------------- #
# Adres publiczny
# --------------------------------------------------------------------------- #


class _Request:
    def __init__(self, base_url):
        self.base_url = base_url


def test_public_base_url_prefers_the_configured_value(monkeypatch):
    """Za reverse proxy adres widziany przez aplikację nie jest tym, który wpisał użytkownik."""
    monkeypatch.setenv("MEALPILOT_PUBLIC_URL", "https://mealpilot.example/")
    assert oauth.public_base_url(_Request("http://wewnetrzny:8000/")) == "https://mealpilot.example"


def test_public_base_url_falls_back_to_the_request(monkeypatch):
    monkeypatch.delenv("MEALPILOT_PUBLIC_URL", raising=False)
    assert oauth.public_base_url(_Request("http://testserver/")) == "http://testserver"


def test_canonical_resource_is_the_mcp_endpoint(monkeypatch):
    monkeypatch.delenv("MEALPILOT_PUBLIC_URL", raising=False)
    assert oauth.canonical_resource(_Request("http://testserver/")) == "http://testserver/mcp"
