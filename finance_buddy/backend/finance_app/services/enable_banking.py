"""Klient Enable Banking (PSD2): JWT, wywołania HTTP i normalizacja transakcji.

Bez SDK i nowych zależności — HTTP ze stdlib, podpis RS256 przez `cryptography`.
Wnioski z testu na PKO BP i decyzje: docs/design/enable-banking.md.
"""

from __future__ import annotations

import base64
import datetime as dt
import hashlib
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from . import bank_descriptions as descriptions

BASE_URL = "https://api.enablebanking.com"
JWT_TTL_SECONDS = 3600
REQUEST_TIMEOUT_SECONDS = 30
MAX_PAGES = 100
DEFAULT_CONSENT_DAYS = 90
MAX_CONSENT_DAYS = 180


class EnableBankingError(RuntimeError):
    """Błąd wywołania API. `status` = kod HTTP albo None przy błędzie sieci."""

    def __init__(self, message: str, status: int | None = None, code: str | None = None) -> None:
        super().__init__(message)
        self.status = status
        self.code = code

    @property
    def session_invalid(self) -> bool:
        """Zgoda wygasła albo została cofnięta — trzeba połączyć ponownie."""
        if self.status in (401, 403):
            return True
        return (self.code or "").upper() in {
            "EXPIRED_SESSION",
            "CLOSED_SESSION",
            "REVOKED_SESSION",
            "SESSION_DOES_NOT_EXIST",
        }


# --- Klucz i JWT -----------------------------------------------------------------


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def load_private_key(pem: str | bytes) -> rsa.RSAPrivateKey:
    raw = pem.encode() if isinstance(pem, str) else pem
    try:
        key = serialization.load_pem_private_key(raw, password=None)
    except (ValueError, TypeError) as exc:
        raise EnableBankingError("To nie jest klucz prywatny w formacie PEM.") from exc
    if not isinstance(key, rsa.RSAPrivateKey):
        raise EnableBankingError("Klucz musi być kluczem RSA.")
    return key


def make_jwt(app_id: str, key: rsa.RSAPrivateKey, now: int | None = None) -> str:
    """JWT RS256, jakiego oczekuje Enable Banking: `kid` = Application ID."""
    issued = int(time.time()) if now is None else now
    header = {"typ": "JWT", "alg": "RS256", "kid": app_id}
    payload = {
        "iss": "enablebanking.com",
        "aud": "api.enablebanking.com",
        "iat": issued,
        "exp": issued + JWT_TTL_SECONDS,
    }
    signing_input = ".".join(
        _b64url(json.dumps(part, separators=(",", ":")).encode()) for part in (header, payload)
    )
    signature = key.sign(signing_input.encode("ascii"), padding.PKCS1v15(), hashes.SHA256())
    return f"{signing_input}.{_b64url(signature)}"


# --- HTTP --------------------------------------------------------------------------


def _error_from_http(method: str, path: str, exc: urllib.error.HTTPError) -> EnableBankingError:
    body = exc.read().decode("utf-8", "replace")
    code = message = None
    try:
        parsed = json.loads(body)
        if isinstance(parsed, dict):
            code = parsed.get("error") or parsed.get("code")
            message = parsed.get("message") or parsed.get("detail")
    except ValueError:
        pass
    text = message or body.strip() or exc.reason
    return EnableBankingError(
        f"Enable Banking: {method} {path.split('?')[0]} → HTTP {exc.code}: {text}",
        status=exc.code,
        code=str(code) if code else None,
    )


@dataclass
class Client:
    app_id: str
    key: rsa.RSAPrivateKey
    # Nagłówki PSU tylko przy akcji użytkownika — bez nich bank liczy pobranie
    # do limitu 4 na dobę (RTS 2018/389 art. 36 ust. 5).
    psu_headers: dict[str, str] = field(default_factory=dict)

    def request(self, method: str, path: str, body: dict | None = None) -> Any:  # noqa: ANN401 — JSON z API
        headers = {
            "Authorization": f"Bearer {make_jwt(self.app_id, self.key)}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            **self.psu_headers,
        }
        data = json.dumps(body).encode() if body is not None else None
        # Stały host z BASE_URL, nie adres od użytkownika.
        req = urllib.request.Request(  # noqa: S310
            BASE_URL + path, data=data, headers=headers, method=method
        )
        try:
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_SECONDS) as resp:  # noqa: S310
                return json.loads(resp.read() or b"null")
        except urllib.error.HTTPError as exc:
            raise _error_from_http(method, path, exc) from exc
        except (urllib.error.URLError, TimeoutError) as exc:
            raise EnableBankingError(f"Enable Banking nie odpowiada: {exc}") from exc

    def application(self) -> dict:
        return self.request("GET", "/application")

    def aspsps(self, country: str) -> list[dict]:
        query = urllib.parse.urlencode({"country": country})
        return self.request("GET", f"/aspsps?{query}").get("aspsps") or []

    def start_auth(
        self,
        *,
        aspsp_name: str,
        country: str,
        valid_until: dt.datetime,
        state: str,
        redirect_url: str,
        psu_type: str,
    ) -> str:
        resp = self.request(
            "POST",
            "/auth",
            {
                "access": {"valid_until": valid_until.isoformat()},
                "aspsp": {"name": aspsp_name, "country": country},
                "state": state,
                "redirect_url": redirect_url,
                "psu_type": psu_type,
            },
        )
        return resp["url"]

    def create_session(self, code: str) -> dict:
        return self.request("POST", "/sessions", {"code": code})

    def get_session(self, session_id: str) -> dict:
        return self.request("GET", f"/sessions/{urllib.parse.quote(session_id)}")

    def account_details(self, account_uid: str) -> dict:
        return self.request("GET", f"/accounts/{urllib.parse.quote(account_uid)}/details")

    def delete_session(self, session_id: str) -> None:
        self.request("DELETE", f"/sessions/{urllib.parse.quote(session_id)}")

    def balances(self, account_uid: str) -> list[dict]:
        path = f"/accounts/{urllib.parse.quote(account_uid)}/balances"
        return self.request("GET", path).get("balances") or []

    def transactions(self, account_uid: str, date_from: dt.date, date_to: dt.date) -> list[dict]:
        found: list[dict] = []
        key: str | None = None
        for _ in range(MAX_PAGES):
            query = {"date_from": date_from.isoformat(), "date_to": date_to.isoformat()}
            if key:
                query["continuation_key"] = key
            path = (
                f"/accounts/{urllib.parse.quote(account_uid)}/transactions?"
                f"{urllib.parse.urlencode(query)}"
            )
            page = self.request("GET", path)
            found.extend(page.get("transactions") or [])
            next_key = page.get("continuation_key")
            # Zabezpieczenie z Actual: część banków potrafi oddać ten sam klucz.
            if not next_key or next_key == key:
                break
            key = next_key
        return found


def parse_redirect(value: str) -> tuple[str, str | None]:
    """Wklejony adres po powrocie z banku (albo sam kod) → (code, state)."""
    value = value.strip()
    if not value:
        raise EnableBankingError("Wklej adres z paska przeglądarki po powrocie z banku.")
    if "code=" not in value and "error=" not in value:
        return value, None
    query = urllib.parse.parse_qs(urllib.parse.urlsplit(value).query)
    if "error" in query:
        description = (query.get("error_description") or [""])[0]
        raise EnableBankingError(f"Bank przerwał zgodę: {query['error'][0]} {description}".strip())
    codes = query.get("code")
    if not codes:
        raise EnableBankingError("W adresie nie ma parametru code.")
    states = query.get("state")
    return codes[0], states[0] if states else None


def consent_days(aspsp: dict) -> int:
    """Najdłuższa zgoda, na jaką pozwala bank, ale nie dłużej niż 180 dni."""
    seconds = aspsp.get("maximum_consent_validity") or DEFAULT_CONSENT_DAYS * 86400
    return max(1, min(MAX_CONSENT_DAYS, int(seconds) // 86400))


# --- Konta i salda -----------------------------------------------------------------


def iban_of(account: dict) -> str:
    return "".join(str((account.get("account_id") or {}).get("iban") or "").split()).upper()


def mask_iban(iban: str) -> str | None:
    """Ostatnie 4 cyfry wystarczą, żeby odróżnić konta; pełny IBAN nie trafia do bazy."""
    if not iban:
        return None
    return f"…{iban[-4:]}" if len(iban) > 4 else "****"


def iban_hash(iban: str) -> str | None:
    """Odcisk IBAN-u — po ponownej zgodzie konta dostają nowe `uid`, a IBAN zostaje."""
    if not iban:
        return None
    return hashlib.sha256(iban.encode("utf-8")).hexdigest()


_BALANCE_PREFERENCE = ("ITBD", "CLBD", "ITAV", "CLAV", "XPCD", "OTHR")


def booked_balance_cents(balances: list[dict]) -> int | None:
    """Saldo zaksięgowane (`ITBD`, potem `CLBD`) — porównywalne z sumą transakcji."""
    by_type = {b.get("balance_type"): b for b in balances}
    for kind in _BALANCE_PREFERENCE:
        balance = by_type.get(kind)
        if balance is None:
            continue
        cents = _to_cents((balance.get("balance_amount") or {}).get("amount"))
        if cents is not None:
            return cents
    return None


# --- Transakcje --------------------------------------------------------------------


@dataclass
class ApiTransaction:
    """Transakcja z API sprowadzona do pól, które rozumie import."""

    external_id: str
    date: dt.date
    amount_cents: int
    currency: str | None
    payee: str | None
    memo: str | None


def _to_cents(raw: object) -> int | None:
    if raw is None:
        return None
    try:
        value = Decimal(str(raw).strip())
    except InvalidOperation:
        return None
    return int((value * 100).to_integral_value())


def _signed_cents(tx: dict) -> int | None:
    cents = _to_cents((tx.get("transaction_amount") or {}).get("amount"))
    if cents is None:
        return None
    indicator = tx.get("credit_debit_indicator")
    if indicator == "DBIT":
        return -abs(cents)
    if indicator == "CRDT":
        return abs(cents)
    return cents


def _iso_date(raw: object) -> dt.date | None:
    if not isinstance(raw, str):
        return None
    try:
        return dt.date.fromisoformat(raw[:10])
    except ValueError:
        return None


def _counterparty(tx: dict) -> str | None:
    creditor = ((tx.get("creditor") or {}).get("name") or "").strip()
    debtor = ((tx.get("debtor") or {}).get("name") or "").strip()
    indicator = tx.get("credit_debit_indicator")
    if indicator == "DBIT":
        return creditor or None
    if indicator == "CRDT":
        return debtor or None
    return creditor or debtor or None


def _remittance(tx: dict) -> list[str]:
    return [s.strip() for s in tx.get("remittance_information") or [] if s and s.strip()]


def _fingerprint(tx: dict) -> str:
    # Surowe pola bez `status` i `booking_date` — inaczej odcisk zmieniałby się
    # przy zaksięgowaniu (lekcja z Actual). Saldo po operacji odróżnia dwie
    # identyczne płatności tego samego dnia.
    content = json.dumps(
        [
            tx.get("transaction_date") or tx.get("value_date") or tx.get("booking_date") or "",
            (tx.get("transaction_amount") or {}).get("amount"),
            (tx.get("transaction_amount") or {}).get("currency"),
            tx.get("credit_debit_indicator") or "",
            (tx.get("creditor") or {}).get("name") or "",
            (tx.get("debtor") or {}).get("name") or "",
            tx.get("remittance_information") or [],
            ((tx.get("balance_after_transaction") or {}).get("balance_amount") or {}).get("amount"),
        ],
        ensure_ascii=False,
    )
    return hashlib.sha256(content.encode("utf-8")).hexdigest()[:32]


def normalize(transactions: list[dict]) -> list[ApiTransaction]:
    """Zaksięgowane transakcje w postaci do importu; oczekujące i wadliwe pomija.

    Oczekujących nie bierzemy wcale (decyzja w docs §4) — wrócą jako
    zaksięgowane. Wiersz bez daty albo kwoty pomijamy zamiast przerywać
    synchronizację (lekcja z Actual).
    """
    result: list[ApiTransaction] = []
    fingerprints: Counter[str] = Counter()
    cities = descriptions.learned_cities(
        location
        for tx in transactions
        if (location := descriptions.card_location_of(_remittance(tx), _counterparty(tx)))
    )
    for tx in transactions:
        if (tx.get("status") or "BOOK") != "BOOK":
            continue
        # Data operacji, gdy bank ją poda; PKO BP zwraca tylko datę księgowania,
        # więc płatność kartą z piątku bywa w aplikacji w poniedziałek.
        day = _iso_date(
            tx.get("transaction_date") or tx.get("booking_date") or tx.get("value_date")
        )
        cents = _signed_cents(tx)
        if day is None or cents is None:
            continue
        bank_id = tx.get("entry_reference") or tx.get("transaction_id")
        if bank_id:
            external_id = f"eb:{bank_id}"
        else:
            base = _fingerprint(tx)
            count = fingerprints[base]
            fingerprints[base] += 1
            external_id = f"eb-fp:{base}" if count == 0 else f"eb-fp:{base}-{count}"
        described = descriptions.describe(_counterparty(tx), _remittance(tx), cities)
        result.append(
            ApiTransaction(
                external_id=external_id,
                date=day,
                amount_cents=cents,
                currency=(tx.get("transaction_amount") or {}).get("currency"),
                payee=described.payee,
                memo=described.memo,
            )
        )
    return result
