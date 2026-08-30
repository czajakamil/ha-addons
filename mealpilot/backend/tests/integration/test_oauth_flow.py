"""OAuth 2.1 od discovery do wywołania MCP — czyli to, co robi konektor na claude.ai.

Powód istnienia całej tej ścieżki: konektor dodany w przeglądarce dostaje sam
URL i nie ma jak dołożyć nagłówka `X-MealPilot-Token`, więc bez OAuth widzi 401
i raportuje „brak narzędzi". Test `test_pelny_przeplyw_*` przechodzi dokładnie
te kroki co klient, w tej samej kolejności.

Reszta pliku to własności bezpieczeństwa, których szczęśliwa ścieżka nie dotyka:
jednorazowość kodu, PKCE, rotacja refresh tokenu, powiązanie z zasobem
i odcięcie grantów przy zmianie hasła.
"""

import base64
import hashlib
import re
import secrets
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse

import pytest

from app import models, oauth

pytestmark = pytest.mark.integration

REDIRECT_URI = "https://claude.ai/api/mcp/auth_callback"
ADMIN_USERNAME = "admin"
ADMIN_PASSWORD = "AdminPass1234"


# --------------------------------------------------------------------------- #
# Pomocnicze: kroki przepływu
# --------------------------------------------------------------------------- #


def _verifier() -> str:
    return secrets.token_urlsafe(48)


def _challenge(verifier: str) -> str:
    digest = hashlib.sha256(verifier.encode()).digest()
    return base64.urlsafe_b64encode(digest).decode().rstrip("=")


def _register(client, **overrides) -> dict:
    payload = {"client_name": "Claude", "redirect_uris": [REDIRECT_URI]}
    payload.update(overrides)
    r = client.post("/oauth/register", json=payload)
    assert r.status_code == 201, r.text
    return r.json()


def _authorize_params(client_id: str, verifier: str, **overrides) -> dict:
    params = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": REDIRECT_URI,
        "code_challenge": _challenge(verifier),
        "code_challenge_method": "S256",
        "state": "stan-123",
    }
    params.update(overrides)
    return {k: v for k, v in params.items() if v is not None}


def _consent(client, params) -> tuple[str, dict]:
    """GET formularza zgody -> (csrf, ukryte pola)."""
    r = client.get("/oauth/authorize", params=params)
    assert r.status_code == 200, r.text
    csrf = re.search(r'name="csrf" value="([^"]+)"', r.text)
    assert csrf, r.text
    return csrf.group(1), params


def _approve(client, params, csrf, username=ADMIN_USERNAME, password=ADMIN_PASSWORD, action="approve"):
    form = {**params, "csrf": csrf, "username": username, "password": password, "action": action}
    return client.post("/oauth/authorize", data=form, follow_redirects=False)


def _code_from(response) -> str:
    assert response.status_code == 303, response.text
    query = parse_qs(urlparse(response.headers["location"]).query)
    assert "error" not in query, query
    return query["code"][0]


def _exchange(client, client_id: str, code: str, verifier: str, **overrides):
    form = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": REDIRECT_URI,
        "code_verifier": verifier,
        "client_id": client_id,
    }
    form.update(overrides)
    return client.post("/oauth/token", data=form)


def _bearer_request(access_token: str) -> SimpleNamespace:
    """Minimalna atrapa `Request` — tyle, ile czyta `_authenticate`."""
    return SimpleNamespace(
        base_url="http://testserver/",
        headers={"authorization": f"Bearer {access_token}"},
        client=None,
    )


def _full_flow(client, scope=None, **credentials) -> tuple[dict, str]:
    """Cały przepływ aż do tokenów. Zwraca (body /oauth/token, client_id)."""
    registered = _register(client)
    verifier = _verifier()
    params = _authorize_params(registered["client_id"], verifier, scope=scope)
    csrf, params = _consent(client, params)
    code = _code_from(_approve(client, params, csrf, **credentials))
    r = _exchange(client, registered["client_id"], code, verifier)
    assert r.status_code == 200, r.text
    return r.json(), registered["client_id"]


# --------------------------------------------------------------------------- #
# Discovery
# --------------------------------------------------------------------------- #


def test_niezalogowane_mcp_wskazuje_gdzie_sie_autoryzowac(client):
    """Bez tego nagłówka konektor na claude.ai pokazuje „brak narzędzi" zamiast logowania."""
    r = client.post("/mcp")
    assert r.status_code == 401
    challenge = r.headers["www-authenticate"]
    assert challenge.startswith("Bearer ")
    assert 'resource_metadata="http://testserver/.well-known/oauth-protected-resource/mcp"' in challenge


def test_metadane_zasobu_wskazuja_serwer_autoryzacji(client):
    r = client.get("/.well-known/oauth-protected-resource/mcp")
    assert r.status_code == 200
    body = r.json()
    assert body["resource"] == "http://testserver/mcp"
    assert body["authorization_servers"] == ["http://testserver"]
    assert set(body["scopes_supported"]) == {"read", "write"}


def test_metadane_serwera_autoryzacji_opisuja_oauth_21(client):
    body = client.get("/.well-known/oauth-authorization-server").json()
    assert body["issuer"] == "http://testserver"
    assert body["authorization_endpoint"] == "http://testserver/oauth/authorize"
    assert body["token_endpoint"] == "http://testserver/oauth/token"
    assert body["registration_endpoint"] == "http://testserver/oauth/register"
    # OAuth 2.1: bez implicit, bez password, PKCE wyłącznie S256.
    assert set(body["grant_types_supported"]) == {"authorization_code", "refresh_token"}
    assert body["response_types_supported"] == ["code"]
    assert body["code_challenge_methods_supported"] == ["S256"]


def test_metadane_respektuja_publiczny_adres(client, monkeypatch):
    """Za ingressem HA metadane muszą podawać adres z przeglądarki, nie wewnętrzny."""
    monkeypatch.setenv("MEALPILOT_PUBLIC_URL", "https://mealpilot.example")
    body = client.get("/.well-known/oauth-authorization-server").json()
    assert body["issuer"] == "https://mealpilot.example"
    assert body["token_endpoint"] == "https://mealpilot.example/oauth/token"


# --------------------------------------------------------------------------- #
# Rejestracja klienta (RFC 7591)
# --------------------------------------------------------------------------- #


def test_klient_moze_zarejestrowac_sie_sam(client):
    body = _register(client)
    assert body["client_id"].startswith("mpc_")
    assert body["redirect_uris"] == [REDIRECT_URI]
    # Klient publiczny: żadnego sekretu do wysyłki, PKCE zamiast niego.
    assert "client_secret" not in body


def test_klient_poufny_dostaje_sekret(client):
    body = _register(client, token_endpoint_auth_method="client_secret_post")
    assert body["client_secret"].startswith("mps_")
    assert body["client_secret_expires_at"] == 0


def test_rejestracja_odrzuca_niebezpieczny_redirect_uri(client):
    r = client.post("/oauth/register", json={"client_name": "Zły", "redirect_uris": ["http://evil.example/cb"]})
    assert r.status_code == 400
    assert r.json()["error"] == "invalid_redirect_uri"


def test_rejestracja_wymaga_listy_adresow(client):
    r = client.post("/oauth/register", json={"client_name": "Bez adresu"})
    assert r.status_code == 400
    assert r.json()["error"] == "invalid_redirect_uri"


# --------------------------------------------------------------------------- #
# Pełny przepływ
# --------------------------------------------------------------------------- #


def test_pelny_przeplyw_konczy_sie_dzialajacym_tokenem(admin_client, db_session):
    tokens, _ = _full_flow(admin_client)

    assert tokens["token_type"] == "Bearer"
    assert tokens["access_token"].startswith("mpo_at_")
    assert tokens["refresh_token"].startswith("mpo_rt_")
    assert tokens["scope"] == "write"
    assert tokens["expires_in"] == 3600

    # Token rozwiązuje się na właściciela konta i jego zakres.
    grant = oauth.verify_access_token(db_session, tokens["access_token"], resource="http://testserver/mcp")
    admin = db_session.query(models.User).filter(models.User.username == ADMIN_USERNAME).one()
    assert grant.user_id == admin.id
    assert grant.scope == "write"


def test_token_z_oauth_zwraca_liste_narzedzi_mcp(admin_client):
    """Puenta całej zmiany: po OAuth `tools/list` zwraca narzędzia, a nie pustkę.

    To dokładnie ten krok, który dziś na claude.ai kończy się komunikatem
    „This connector has no tools available" — bo bez Bearera transport odpowiada
    401 jeszcze przed `initialize`.
    """
    tokens, _ = _full_flow(admin_client)
    headers = {
        "Authorization": f"Bearer {tokens['access_token']}",
        "Accept": "application/json, text/event-stream",
    }

    handshake = admin_client.post(
        "/mcp",
        headers=headers,
        json={
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "test", "version": "1"},
            },
        },
    )
    assert handshake.status_code == 200, handshake.text

    listing = admin_client.post("/mcp", headers=headers, json={"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
    assert listing.status_code == 200, listing.text

    names = set(re.findall(r'"name":"([a-z_]+)"', listing.text))
    assert {"list_recipes", "create_recipe", "get_week_plan", "get_shopping_list"} <= names


def test_bearer_daje_ten_sam_principal_co_klucz_api(admin_client):
    import app.routers.mcp_sse as mcp_sse

    tokens, _ = _full_flow(admin_client)

    principal = mcp_sse._authenticate(_bearer_request(tokens["access_token"]), None)
    assert principal.scope == "write"


def test_przeplyw_z_zakresem_read_daje_token_tylko_do_odczytu(admin_client, db_session):
    tokens, _ = _full_flow(admin_client, scope="read")
    assert tokens["scope"] == "read"
    grant = oauth.verify_access_token(db_session, tokens["access_token"], resource="http://testserver/mcp")
    assert grant.scope == "read"


def test_state_wraca_nietkniety(admin_client):
    """Bez odesłania `state` klient nie odróżni swojej odpowiedzi od podrzuconej."""
    registered = _register(admin_client)
    verifier = _verifier()
    params = _authorize_params(registered["client_id"], verifier, state="unikalny-stan-xyz")
    csrf, params = _consent(admin_client, params)
    r = _approve(admin_client, params, csrf)
    query = parse_qs(urlparse(r.headers["location"]).query)
    assert query["state"] == ["unikalny-stan-xyz"]


def test_odrzucenie_zgody_nie_wydaje_kodu(admin_client):
    registered = _register(admin_client)
    verifier = _verifier()
    params = _authorize_params(registered["client_id"], verifier)
    csrf, params = _consent(admin_client, params)

    r = _approve(admin_client, params, csrf, action="deny")

    query = parse_qs(urlparse(r.headers["location"]).query)
    assert query["error"] == ["access_denied"]
    assert "code" not in query


# --------------------------------------------------------------------------- #
# Uwierzytelnienie użytkownika na ekranie zgody
# --------------------------------------------------------------------------- #


def test_zle_haslo_nie_wydaje_kodu(admin_client):
    registered = _register(admin_client)
    verifier = _verifier()
    params = _authorize_params(registered["client_id"], verifier)
    csrf, params = _consent(admin_client, params)

    r = _approve(admin_client, params, csrf, password="ZupelnieZle9999")

    assert r.status_code == 401
    assert "Nieprawidłowa nazwa użytkownika lub hasło" in r.text
    assert "location" not in r.headers


def test_zalogowana_sesja_nie_zastepuje_hasla(admin_client):
    """`admin_client` ma ważne ciasteczko — mimo to zgoda wymaga podania hasła.

    Przepływ startuje strona trzecia, a wydawany jest token żyjący dłużej niż
    sesja przeglądarki; to moment, w którym użytkownik ma świadomie potwierdzić.
    """
    registered = _register(admin_client)
    verifier = _verifier()
    params = _authorize_params(registered["client_id"], verifier)
    csrf, params = _consent(admin_client, params)

    form = {**params, "csrf": csrf, "action": "approve"}  # bez username/password
    r = admin_client.post("/oauth/authorize", data=form, follow_redirects=False)

    assert r.status_code == 401
    assert "location" not in r.headers


def test_formularz_bez_tokenu_csrf_jest_odrzucany(admin_client):
    registered = _register(admin_client)
    verifier = _verifier()
    params = _authorize_params(registered["client_id"], verifier)
    _consent(admin_client, params)

    r = _approve(admin_client, params, csrf="podrobiony-token")

    assert r.status_code == 400
    assert "location" not in r.headers


# --------------------------------------------------------------------------- #
# Walidacja żądania autoryzacji
# --------------------------------------------------------------------------- #


def test_nieznany_klient_nie_powoduje_przekierowania(client):
    """Błąd client_id renderujemy użytkownikowi — przekierowanie byłoby open redirectem."""
    r = client.get(
        "/oauth/authorize",
        params={"response_type": "code", "client_id": "mpc_nieistnieje", "redirect_uri": "https://evil.example/cb"},
        follow_redirects=False,
    )
    assert r.status_code == 400
    assert "location" not in r.headers


def test_niezarejestrowany_redirect_uri_nie_powoduje_przekierowania(client):
    registered = _register(client)
    r = client.get(
        "/oauth/authorize",
        params=_authorize_params(
            registered["client_id"], _verifier(), redirect_uri="https://napastnik.example/zbieram"
        ),
        follow_redirects=False,
    )
    assert r.status_code == 400
    assert "location" not in r.headers


def test_brak_pkce_jest_odrzucany(client):
    """W OAuth 2.1 PKCE jest obowiązkowe także dla klientów poufnych."""
    registered = _register(client)
    params = _authorize_params(registered["client_id"], _verifier())
    del params["code_challenge"]

    r = client.get("/oauth/authorize", params=params, follow_redirects=False)

    assert r.status_code == 303
    query = parse_qs(urlparse(r.headers["location"]).query)
    assert query["error"] == ["invalid_request"]


def test_metoda_plain_jest_odrzucana(client):
    registered = _register(client)
    params = _authorize_params(registered["client_id"], _verifier(), code_challenge_method="plain")

    r = client.get("/oauth/authorize", params=params, follow_redirects=False)

    query = parse_qs(urlparse(r.headers["location"]).query)
    assert query["error"] == ["invalid_request"]


def test_obcy_resource_jest_odrzucany(client):
    """Token ma być powiązany z tym serwerem — inaczej da się go podstawić gdzie indziej."""
    registered = _register(client)
    params = _authorize_params(registered["client_id"], _verifier(), resource="https://inny-serwer.example/mcp")

    r = client.get("/oauth/authorize", params=params, follow_redirects=False)

    query = parse_qs(urlparse(r.headers["location"]).query)
    assert query["error"] == ["invalid_target"]


def test_nieobslugiwany_response_type_jest_odrzucany(client):
    registered = _register(client)
    params = _authorize_params(registered["client_id"], _verifier(), response_type="token")

    r = client.get("/oauth/authorize", params=params, follow_redirects=False)

    query = parse_qs(urlparse(r.headers["location"]).query)
    assert query["error"] == ["unsupported_response_type"]


# --------------------------------------------------------------------------- #
# Wymiana kodu na token
# --------------------------------------------------------------------------- #


def test_kod_dziala_tylko_raz(admin_client):
    registered = _register(admin_client)
    verifier = _verifier()
    params = _authorize_params(registered["client_id"], verifier)
    csrf, params = _consent(admin_client, params)
    code = _code_from(_approve(admin_client, params, csrf))

    assert _exchange(admin_client, registered["client_id"], code, verifier).status_code == 200
    powtorka = _exchange(admin_client, registered["client_id"], code, verifier)
    assert powtorka.status_code == 400
    assert powtorka.json()["error"] == "invalid_grant"


def test_zly_code_verifier_nie_wymienia_kodu(admin_client):
    """Sedno PKCE: sam przechwycony kod jest bezużyteczny."""
    registered = _register(admin_client)
    verifier = _verifier()
    params = _authorize_params(registered["client_id"], verifier)
    csrf, params = _consent(admin_client, params)
    code = _code_from(_approve(admin_client, params, csrf))

    r = _exchange(admin_client, registered["client_id"], code, _verifier())

    assert r.status_code == 400
    assert r.json()["error"] == "invalid_grant"


def test_kod_nie_dziala_dla_innego_klienta(admin_client):
    """Kod wydany klientowi A nie może być wymieniony przez klienta B."""
    ofiara = _register(admin_client)
    napastnik = _register(admin_client, client_name="Napastnik")
    verifier = _verifier()
    params = _authorize_params(ofiara["client_id"], verifier)
    csrf, params = _consent(admin_client, params)
    code = _code_from(_approve(admin_client, params, csrf))

    r = _exchange(admin_client, napastnik["client_id"], code, verifier)

    assert r.status_code == 400
    assert r.json()["error"] == "invalid_grant"


def test_niezgodny_redirect_uri_przy_wymianie_jest_odrzucany(admin_client):
    registered = _register(admin_client)
    verifier = _verifier()
    params = _authorize_params(registered["client_id"], verifier)
    csrf, params = _consent(admin_client, params)
    code = _code_from(_approve(admin_client, params, csrf))

    r = _exchange(admin_client, registered["client_id"], code, verifier, redirect_uri="https://claude.ai/inny_callback")

    assert r.status_code == 400
    assert r.json()["error"] == "invalid_grant"


def test_nieznany_grant_type_jest_odrzucany(admin_client):
    registered = _register(admin_client)
    r = admin_client.post(
        "/oauth/token",
        data={"grant_type": "password", "client_id": registered["client_id"], "username": "a", "password": "b"},
    )
    assert r.status_code == 400
    assert r.json()["error"] == "unsupported_grant_type"


def test_odpowiedz_tokenu_nie_jest_cacheowana(admin_client):
    registered = _register(admin_client)
    verifier = _verifier()
    params = _authorize_params(registered["client_id"], verifier)
    csrf, params = _consent(admin_client, params)
    code = _code_from(_approve(admin_client, params, csrf))

    r = _exchange(admin_client, registered["client_id"], code, verifier)

    assert r.headers["cache-control"] == "no-store"


def test_klient_poufny_musi_podac_sekret(admin_client):
    registered = _register(admin_client, token_endpoint_auth_method="client_secret_post")
    verifier = _verifier()
    params = _authorize_params(registered["client_id"], verifier)
    csrf, params = _consent(admin_client, params)
    code = _code_from(_approve(admin_client, params, csrf))

    bez = _exchange(admin_client, registered["client_id"], code, verifier)
    assert bez.status_code == 401
    assert bez.json()["error"] == "invalid_client"


# --------------------------------------------------------------------------- #
# Refresh token
# --------------------------------------------------------------------------- #


def test_refresh_wydaje_nowa_pare_i_uniewaznia_stara(admin_client):
    """Rotacja: skradziona kopia refresh tokenu przestaje działać po użyciu przez właściciela."""
    registered = _register(admin_client)
    verifier = _verifier()
    params = _authorize_params(registered["client_id"], verifier)
    csrf, params = _consent(admin_client, params)
    code = _code_from(_approve(admin_client, params, csrf))
    first = _exchange(admin_client, registered["client_id"], code, verifier).json()

    r = admin_client.post(
        "/oauth/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": first["refresh_token"],
            "client_id": registered["client_id"],
        },
    )
    assert r.status_code == 200
    second = r.json()
    assert second["access_token"] != first["access_token"]
    assert second["refresh_token"] != first["refresh_token"]

    powtorka = admin_client.post(
        "/oauth/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": first["refresh_token"],
            "client_id": registered["client_id"],
        },
    )
    assert powtorka.status_code == 400
    assert powtorka.json()["error"] == "invalid_grant"


def test_refresh_nie_moze_rozszerzyc_zakresu(admin_client):
    """Odświeżenie nie jest okazją do wzięcia więcej, niż użytkownik zatwierdził."""
    tokens, client_id = _full_flow(admin_client, scope="read")

    r = admin_client.post(
        "/oauth/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": tokens["refresh_token"],
            "client_id": client_id,
            "scope": "write",
        },
    )

    assert r.status_code == 400
    assert r.json()["error"] == "invalid_scope"


def test_refresh_moze_zawezic_zakres(admin_client):
    tokens, client_id = _full_flow(admin_client)
    assert tokens["scope"] == "write"

    r = admin_client.post(
        "/oauth/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": tokens["refresh_token"],
            "client_id": client_id,
            "scope": "read",
        },
    )

    assert r.status_code == 200
    assert r.json()["scope"] == "read"


# --------------------------------------------------------------------------- #
# Powiązanie tokenu z zasobem i cykl życia
# --------------------------------------------------------------------------- #


def test_token_wydany_dla_innego_zasobu_nie_dziala(admin_client, db_session):
    tokens, _ = _full_flow(admin_client)
    with pytest.raises(oauth.OAuthError) as exc:
        oauth.verify_access_token(db_session, tokens["access_token"], resource="https://inny.example/mcp")
    assert exc.value.error == "invalid_token"


def test_wygasly_token_jest_odrzucany(admin_client, db_session):
    from datetime import UTC, datetime, timedelta

    tokens, _ = _full_flow(admin_client)
    row = (
        db_session.query(models.OAuthToken)
        .filter(models.OAuthToken.token_hash == oauth.sha256_hex(tokens["access_token"]))
        .one()
    )
    row.expires_at = datetime.now(UTC) - timedelta(seconds=1)
    db_session.commit()

    with pytest.raises(oauth.OAuthError):
        oauth.verify_access_token(db_session, tokens["access_token"], resource="http://testserver/mcp")


def test_revoke_uniewaznia_token(admin_client, db_session):
    registered = _register(admin_client)
    verifier = _verifier()
    params = _authorize_params(registered["client_id"], verifier)
    csrf, params = _consent(admin_client, params)
    code = _code_from(_approve(admin_client, params, csrf))
    tokens = _exchange(admin_client, registered["client_id"], code, verifier).json()

    r = admin_client.post(
        "/oauth/revoke",
        data={"token": tokens["access_token"], "client_id": registered["client_id"]},
    )
    assert r.status_code == 200

    with pytest.raises(oauth.OAuthError):
        oauth.verify_access_token(db_session, tokens["access_token"], resource="http://testserver/mcp")


def test_zmiana_hasla_odcina_granty_oauth(admin_client, db_session):
    """Access token żyje godzinę, refresh miesiąc — bez tego zmiana hasła nic by nie dała."""
    tokens, _ = _full_flow(admin_client)

    r = admin_client.post(
        "/api/auth/change-password",
        json={"old_password": ADMIN_PASSWORD, "new_password": "NoweHaslo987654"},
    )
    assert r.status_code == 204

    db_session.expire_all()
    with pytest.raises(oauth.OAuthError):
        oauth.verify_access_token(db_session, tokens["access_token"], resource="http://testserver/mcp")


def test_usuniecie_konta_kasuje_jego_tokeny(admin_client, make_user, db_session):
    ofiara, uid = make_user("oauth_usuwany")
    registered = _register(ofiara)
    verifier = _verifier()
    params = _authorize_params(registered["client_id"], verifier)
    csrf, params = _consent(ofiara, params)
    code = _code_from(_approve(ofiara, params, csrf, username="oauth_usuwany", password="UserPass1234"))
    _exchange(ofiara, registered["client_id"], code, verifier)

    assert db_session.query(models.OAuthToken).filter(models.OAuthToken.user_id == uid).count() > 0
    assert admin_client.delete(f"/api/admin/users/{uid}").status_code == 204

    db_session.expire_all()
    assert db_session.query(models.OAuthToken).filter(models.OAuthToken.user_id == uid).count() == 0
    assert db_session.query(models.OAuthAuthCode).filter(models.OAuthAuthCode.user_id == uid).count() == 0


def test_nieaktywne_konto_nie_przechodzi_bramki_mcp(admin_client, make_user, db_session):
    import app.routers.mcp_sse as mcp_sse

    ofiara, uid = make_user("oauth_dezaktywowany")
    registered = _register(ofiara)
    verifier = _verifier()
    params = _authorize_params(registered["client_id"], verifier)
    csrf, params = _consent(ofiara, params)
    code = _code_from(_approve(ofiara, params, csrf, username="oauth_dezaktywowany", password="UserPass1234"))
    tokens = _exchange(ofiara, registered["client_id"], code, verifier).json()

    admin_client.patch(f"/api/admin/users/{uid}", json={"is_active": False})

    with pytest.raises(Exception) as exc:  # HTTPException 401
        mcp_sse._authenticate(_bearer_request(tokens["access_token"]), None)
    assert getattr(exc.value, "status_code", None) == 401


# --------------------------------------------------------------------------- #
# Współistnienie z kluczem API
# --------------------------------------------------------------------------- #


def test_klucz_api_dalej_dziala_obok_oauth(make_user, client):
    """Konfiguracje Claude Desktop / Claude Code nie mogą paść przez dodanie OAuth."""
    owner, _ = make_user("oauth_i_klucz")
    raw = owner.post("/api/auth/api-keys", json={"name": "desktop"}).json()["key"]

    r = client.post("/mcp", headers={"X-MealPilot-Token": raw})

    assert r.status_code != 401


def test_zly_bearer_dostaje_wyzwanie_www_authenticate(client):
    r = client.post("/mcp", headers={"Authorization": "Bearer mpo_at_nieistnieje"})
    assert r.status_code == 401
    assert "resource_metadata=" in r.headers["www-authenticate"]
