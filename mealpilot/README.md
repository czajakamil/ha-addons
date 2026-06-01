# MealPilot

> A full-stack meal-planning application built as a **Home Assistant Add-on** — weekly plan, AI meal assistant, shopping list, and step-by-step cooking mode, all served from your self-hosted smart-home hub.

![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-0.115-009688?logo=fastapi&logoColor=white)
![React](https://img.shields.io/badge/React-18-61DAFB?logo=react&logoColor=black)
![TypeScript](https://img.shields.io/badge/TypeScript-5-3178C6?logo=typescript&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-multi--arch-2496ED?logo=docker&logoColor=white)
![SQLite](https://img.shields.io/badge/SQLite-3-003B57?logo=sqlite&logoColor=white)

---

## Why this project exists

Most meal-planning apps are SaaS with a monthly fee, a mobile-only UX, and no local data ownership. MealPilot runs entirely on your own hardware inside Home Assistant, costs nothing to host, and gives you an LLM assistant that actually knows your recipe library.

---

## Feature overview

| Area | What it does |
|------|-------------|
| **Recipe library** | Create/edit recipes with ingredients, macros, tags, images, ratings (1–5 ★), and private notes. Drag-and-drop reordering of steps and ingredients. |
| **Weekly planner** | Drag meals onto a 7-day calendar, generate a shopping list from the plan in one click. |
| **AI assistant** | Chat interface backed by any OpenAI-compatible endpoint (OpenAI, Anthropic, local Ollama, etc.). The agent can read your recipes, build plans, rate dishes, and add shopping items — via a typed MCP tool layer, not free-form text parsing. |
| **Streaming responses** | Agent replies stream token-by-token; tool invocations are surfaced live in the UI. |
| **Cooking mode** | Step-by-step view with per-step countdown timers and an audio alert when each stage ends. |
| **Shopping list** | Auto-generated from the week's plan; manually editable with checkboxes. |
| **Multi-user households** | Users belong to households and share recipes/plans. Admins control per-user AI access and monthly token/cost limits. |
| **API keys** | Users can generate long-lived API tokens for automation or external integrations (e.g. calling the agent from an HA automation). |

---

## Local development

```bash
# clone and start the dev stack
git clone https://github.com/czajakamil/ha-addons
cd ha-addons/mealpilot
docker compose -f docker-compose.dev.yml up

# frontend hot-reload (separate terminal)
cd frontend && npm install && npm run dev
```

Backend auto-reloads on file changes via Uvicorn's `--reload` flag.  
The dev compose mounts `./data` so the SQLite database survives restarts.

---

## Installation (Home Assistant)

1. **Add repository** in HA → Settings → Add-ons → Add-on Store → ⋮ → Repositories:
   ```
   https://github.com/czajakamil/ha-addons
   ```
2. Install **MealPilot**, configure `admin_username` / `admin_password` and (optionally) `ai_api_url` + `ai_api_key`
3. Start the add-on — it appears in your HA sidebar immediately via Ingress

### Configuration options

| Key | Description |
|-----|-------------|
| `admin_username` / `admin_password` | Bootstrap admin account |
| `ai_api_url` | Any OpenAI-compatible base URL (e.g. `https://api.anthropic.com/v1`) |
| `ai_api_key` | API key for the LLM provider |
| `cors_origins` | Extra allowed origins (comma-separated) |
| `require_cf_access` | Enable Cloudflare Access JWT verification |
| `cookie_secure` | Set the `Secure` flag on session cookies (enable behind HTTPS) |

---

## AI agent — MCP tool layer

The agent does not rely on free-form instructions to manipulate data. Every action is a **typed MCP tool** registered at startup:

```
list_recipes        get_recipe          create_recipe       update_recipe
delete_recipe       rate_recipe         get_plan            set_plan_slot
list_shopping       add_shopping_item   check_shopping_item clear_shopping_list
get_macros_summary  …
```

Full tool specification: [`AGENT_MCP_SPEC.md`](AGENT_MCP_SPEC.md)

