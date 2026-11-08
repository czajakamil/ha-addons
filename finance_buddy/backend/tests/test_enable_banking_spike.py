from __future__ import annotations

import base64
import io
import json
from pathlib import Path

import pytest
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from backend.scripts import enable_banking_spike as eb


@pytest.fixture(scope="module")
def key() -> rsa.RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _unb64(part: str) -> bytes:
    return base64.urlsafe_b64decode(part + "=" * (-len(part) % 4))


def _tx(**overrides: object) -> dict:
    tx = {
        "entry_reference": "REF1",
        "transaction_amount": {"amount": "12.34", "currency": "PLN"},
        "credit_debit_indicator": "DBIT",
        "status": "BOOK",
        "booking_date": "2026-10-02",
        "value_date": "2026-10-02",
        "transaction_date": "2026-10-01",
        "creditor": {"name": "Biedronka"},
        "debtor": {"name": "Kamil"},
        "remittance_information": ["Zakupy kartą"],
    }
    tx.update(overrides)
    return tx


def test_jwt_has_expected_claims_and_valid_signature(key: rsa.RSAPrivateKey) -> None:
    token = eb.make_jwt("app-123", key, now=1_000)
    header_b64, payload_b64, sig_b64 = token.split(".")
    assert json.loads(_unb64(header_b64)) == {"typ": "JWT", "alg": "RS256", "kid": "app-123"}
    assert json.loads(_unb64(payload_b64)) == {
        "iss": "enablebanking.com",
        "aud": "api.enablebanking.com",
        "iat": 1_000,
        "exp": 1_000 + eb.JWT_TTL_SECONDS,
    }
    key.public_key().verify(
        _unb64(sig_b64),
        f"{header_b64}.{payload_b64}".encode(),
        padding.PKCS1v15(),
        hashes.SHA256(),
    )


def test_load_private_key_reads_pem(tmp_path: Path, key: rsa.RSAPrivateKey) -> None:
    pem = tmp_path / "k.pem"
    pem.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    assert eb.load_private_key(pem).key_size == 2048


def test_parse_redirect_accepts_url_or_bare_code() -> None:
    url = "https://czajakamil.github.io/ha-addons/eb-callback?state=abc&code=XYZ"
    assert eb.parse_redirect(url) == ("XYZ", "abc")
    assert eb.parse_redirect("  XYZ ") == ("XYZ", None)
    with pytest.raises(eb.SpikeError):
        eb.parse_redirect("https://x.example/cb?error=access_denied&code=")


def test_normalize_sign_payee_and_date() -> None:
    debit, credit, unsigned = eb.normalize(
        [
            _tx(),
            _tx(
                entry_reference="REF2",
                credit_debit_indicator="CRDT",
                transaction_amount={"amount": "100", "currency": "PLN"},
            ),
            _tx(
                entry_reference="REF3",
                credit_debit_indicator=None,
                transaction_amount={"amount": "-5.5", "currency": "PLN"},
                creditor=None,
            ),
        ]
    )
    assert (debit.amount_cents, debit.payee, str(debit.date)) == (-1234, "Biedronka", "2026-10-02")
    assert (credit.amount_cents, credit.payee) == (10000, "Kamil")
    assert unsigned.amount_cents == -550
    assert "brak credit_debit_indicator" in unsigned.problems
    assert debit.external_id == "eb:REF1"


def test_derived_ids_ignore_booking_and_number_duplicates() -> None:
    pending = _tx(entry_reference=None, status="PDNG", booking_date=None)
    booked = _tx(entry_reference=None, status="BOOK", booking_date="2026-10-05")
    # Ta sama transakcja przed i po zaksięgowaniu → ten sam odcisk.
    assert eb.normalize([pending])[0].derived_id == eb.normalize([booked])[0].derived_id

    rows = eb.normalize([pending, booked, booked])
    # Zaksięgowane numerowane pierwsze, oczekująca dostaje kolejny numer.
    base = rows[1].derived_id
    assert base is not None
    assert rows[2].derived_id == f"{base}-1"
    assert rows[0].derived_id == f"{base}-2"
    assert rows[0].external_id == f"eb-fp:{base}-2"


def test_analyze_account_reports_id_coverage() -> None:
    account = {
        "uid": "u1",
        "name": "Konto",
        "currency": "PLN",
        "pages": 1,
        "transactions": [_tx(), _tx(entry_reference=None), _tx(entry_reference="REF1")],
        "balances": [
            {"balance_type": "CLBD", "balance_amount": {"amount": "10", "currency": "PLN"}}
        ],
    }
    text = "\n".join(eb.analyze_account(account))
    assert "entry_reference: 2/3" in text
    assert "bez żadnego id:  1/3" in text
    assert "powtórzone id" in text
    assert "saldo CLBD: 10" in text


def test_compare_with_csv(tmp_path: Path) -> None:
    csv_path = tmp_path / "wyciag.csv"
    csv_path.write_text(
        "Data operacji;Kwota;Numer referencyjny;Opis\n"
        "01.10.2026;-12,34;REF1;Biedronka\n"
        "03.10.2026;100,00;XX;Przelew\n"
        "20.09.2026;-1,00;OLD;Poza zakresem\n",
        encoding="utf-8",
    )
    args = eb.build_parser().parse_args(
        [
            "compare",
            "d.json",
            str(csv_path),
            "--date-col",
            "Data operacji",
            "--amount-col",
            "Kwota",
            "--id-col",
            "Numer referencyjny",
        ]
    )
    csv_rows = eb.read_csv(csv_path, args)
    api = eb.normalize(
        [
            _tx(),
            _tx(
                entry_reference="REF2",
                credit_debit_indicator="CRDT",
                transaction_amount={"amount": "100", "currency": "PLN"},
                booking_date="2026-10-03",
                transaction_date="2026-10-03",
            ),
        ]
    )
    text = "\n".join(eb.compare(api, csv_rows))
    assert "sparowanych: 2" in text
    assert "data CSV == transaction_date: 2/2" in text
    assert "id z CSV == entry_reference: 1/2" in text


def test_stability_detects_changed_ids() -> None:
    old = {
        "accounts": [
            {"uid": "u1", "transactions": [_tx(), _tx(entry_reference="P1", status="PDNG")]}
        ]
    }
    new = {"accounts": [{"uid": "u1", "transactions": [_tx(), _tx(entry_reference="B1")]}]}
    text = "\n".join(eb.stability(old, new))
    assert "stare id obecne w nowym zrzucie: 1/2" in text
    assert "PDNG" in text
    assert "→ BOOK eb:B1" in text


class _FakeResponse(io.BytesIO):
    def __enter__(self) -> _FakeResponse:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def test_dump_end_to_end_with_pagination(
    tmp_path: Path,
    key: rsa.RSAPrivateKey,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    pem = tmp_path / "k.pem"
    pem.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    state = tmp_path / "state"
    eb._write_json(
        state / "session.json", {"session_id": "s", "accounts": [{"uid": "u1", "currency": "PLN"}]}
    )

    calls: list[str] = []

    def fake_urlopen(req: object, timeout: int) -> _FakeResponse:  # noqa: ARG001
        url = req.full_url  # type: ignore[attr-defined]
        calls.append(url)
        assert req.headers["Authorization"].startswith("Bearer ")  # type: ignore[attr-defined]
        if url.endswith("/balances"):
            body = {"balances": []}
        elif "continuation_key=K2" in url:
            body = {"transactions": [_tx(entry_reference="REF2")]}
        else:
            body = {"transactions": [_tx()], "continuation_key": "K2"}
        return _FakeResponse(json.dumps(body).encode())

    monkeypatch.setattr(eb.urllib.request, "urlopen", fake_urlopen)
    code = eb.main(
        ["--app-id", "a", "--key", str(pem), "--state-dir", str(state), "dump", "--days", "30"]
    )
    assert code == 0
    assert len(calls) == 3
    dumps = list((state / "dumps").glob("*.json"))
    assert len(dumps) == 1
    saved = json.loads(dumps[0].read_text())
    assert [t["entry_reference"] for t in saved["accounts"][0]["transactions"]] == ["REF1", "REF2"]
    assert "transakcji: 2" in capsys.readouterr().out


def test_stability_detects_shifted_numbering() -> None:
    old = {
        "accounts": [
            {
                "uid": "u1",
                "transactions": [
                    _tx(entry_reference="O;2"),
                    _tx(
                        entry_reference="O;1",
                        booking_date="2026-10-03",
                        transaction_amount={"amount": "1.00", "currency": "PLN"},
                    ),
                ],
            }
        ]
    }
    # Okno historii przesunęło się o jedną operację: numery wskazują inne transakcje.
    new = {
        "accounts": [
            {
                "uid": "u1",
                "transactions": [
                    _tx(entry_reference="O;1"),
                    _tx(
                        entry_reference="O;2",
                        booking_date="2026-10-03",
                        transaction_amount={"amount": "1.00", "currency": "PLN"},
                    ),
                ],
            }
        ]
    }
    text = "\n".join(eb.stability(old, new))
    assert "stare id obecne w nowym zrzucie: 2/2" in text
    assert "to samo id, INNA transakcja: 2" in text


def test_iban_is_masked_in_output() -> None:
    assert eb.mask_iban("PL82 1020 3466 0000 9002 0235 8398") == "…8398"
    assert eb.mask_iban(None) == "?"
    text = "\n".join(
        eb.analyze_account(
            {"uid": "u1", "iban": "PL82102034660000900202358398", "transactions": []}
        )
    )
    assert "…8398" in text
    assert "PL8210" not in text
