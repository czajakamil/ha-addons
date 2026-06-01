# Kamil's Home Assistant Add-ons

A collection of custom [Home Assistant](https://www.home-assistant.io/) add-ons — each solving a real problem at home, built to production quality rather than "it works on my machine."

[![HA Add-on store](https://img.shields.io/badge/HA-Add--on%20store-41BDF5?logo=home-assistant&logoColor=white)](https://www.home-assistant.io/hassio/installing_third_party_addons/)

---

## Add this repository to Home Assistant

**Settings → Add-ons → Add-on Store → ⋮ → Repositories**, paste:

```
https://github.com/czajakamil/ha-addons
```

---

## Add-ons

### [MealPilot](./mealpilot) — _most complete_

**Full-stack meal planning with an AI assistant.**

FastAPI backend + React/TypeScript frontend, served via HA Ingress. Features a weekly meal planner, recipe library with drag-and-drop, AI chat agent (any OpenAI-compatible endpoint) and a typed MCP tool layer, step-by-step cooking mode with timers, multi-user households with per-user AI cost limits, and Cloudflare Access support.

`Python 3.12` `FastAPI` `React` `TypeScript` `SQLite` `MCP` `Docker multi-arch (amd64/aarch64)`

→ [Full documentation](./mealpilot/README.md)

---

### [CatPrint](./catprint)

**REST print server for the GOTOOGO C15 / MXW01 BLE thermal printer.**

Exposes a simple HTTP API so any HA automation can trigger a print. Implements a persistent print queue — jobs submitted while the printer is off are stored and flushed automatically when it reconnects.

`Python` `BLE` `FastAPI` `print queue`

---

### [Cookidoo Today](./cookidoo_today)

**Pulls today's and this week's recipe plan from Cookidoo and exposes it as HA sensor data.**

Scrapes the Cookidoo web interface and fetches recipe images, making the data available to dashboards and automations (e.g. "show tonight's recipe on the kitchen display").

`Python` `web scraping` `REST`

---

### [Health App API](./health_managment)

**FastAPI backend for logging personal health metrics, backed by PostgreSQL.**

Designed to receive data from HA automations or mobile shortcuts and store it in a structured, queryable form.

`Python` `FastAPI` `PostgreSQL`

---

## About

Each one of these add-ons started as a personal need.

If you find them useful, feel free to open an issue or PR.
