"""Klucz z konfiguracji HA ma pierwszeństwo przed zapisanym, a panel dostaje o tym flagę."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from backend.finance_app.services import ynab as ynab_service

USER = SimpleNamespace(id="u1")


@pytest.fixture(autouse=True)
def stored_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ynab_service, "_get_keychain_api_key", lambda _user_id: "stored")


def test_config_key_wins_over_stored_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("YNAB_API_KEY", "from-ha")

    assert ynab_service._resolve_ynab_api_key(None, USER) == "from-ha"
    settings = ynab_service._serialize_ynab_settings(None, USER.id)
    assert settings.api_key_set is True
    assert settings.api_key_from_config is True


def test_stored_key_used_without_config_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("YNAB_API_KEY", raising=False)

    assert ynab_service._resolve_ynab_api_key(None, USER) == "stored"
    settings = ynab_service._serialize_ynab_settings(None, USER.id)
    assert settings.api_key_set is True
    assert settings.api_key_from_config is False
