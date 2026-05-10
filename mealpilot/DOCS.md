# MealPilot

Add-on serwuje aplikację MealPilot (FastAPI + React) na porcie 8000, dostępną przez ingress
Home Assistanta (panel boczny "MealPilot") oraz bezpośrednio na `http://<host>:8000`.

## Opcje

- `cors_origins` – lista originów CORS oddzielona przecinkami (`*` = wszystkie). Pod ingress można zostawić `*`.
- `require_cf_access` – wymuszanie nagłówka Cloudflare Access JWT (zostaw `false` dla użytku lokalnego/ingress).
- `cookie_secure` – ustawia ciasteczko sesyjne jako Secure (włącz, jeśli wystawiasz aplikację po HTTPS).

## Trwałe dane

Baza SQLite oraz obrazki cache są zapisywane w `/data` (montowane przez Home Assistanta).
