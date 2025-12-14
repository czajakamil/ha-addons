import asyncio
import json
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any
import contextlib

import aiohttp
from fastapi import FastAPI, Response
from fastapi.responses import FileResponse, JSONResponse
from PIL import Image

from cookidoo_api import Cookidoo
from cookidoo_api.types import CookidooConfig
from cookidoo_api.helpers import get_localization_options


DATA_DIR = Path("/data")
IMG_DIR = DATA_DIR / "images"
TODAY_JSON = DATA_DIR / "today.json"
WEEK_JSON = DATA_DIR / "week.json"
TODAY_JPG = DATA_DIR / "today.jpg"
WEEK_JPG = DATA_DIR / "week.jpg"
OPTIONS_JSON = DATA_DIR / "options.json"

IMG_DIR.mkdir(parents=True, exist_ok=True)


@dataclass
class Settings:
    email: str
    password: str
    country: str = "pl"
    refresh_minutes: int = 15


def load_settings() -> Settings:
    raw = json.loads(OPTIONS_JSON.read_text(encoding="utf-8"))
    return Settings(
        email=raw["email"],
        password=raw["password"],
        country=raw.get("country", "pl"),
        refresh_minutes=int(raw.get("refresh_minutes", 15)),
    )


def cookidoo_base_and_lang(localization_url: str, lang: str) -> tuple[str, str]:
    from urllib.parse import urlsplit

    sp = urlsplit(localization_url)
    base = f"{sp.scheme}://{sp.netloc}"
    return base, lang


async def scrape_recipe_photo_url(
    session: aiohttp.ClientSession, base: str, lang: str, recipe_id: str
) -> str | None:
    url = f"{base}/recipes/recipe/{lang}/{recipe_id}"
    async with session.get(url) as r:
        if r.status != 200:
            return None
        html = await r.text()

    m = re.search(
        r"(https://assets\.tmecosys\.com/image/upload/t_web_rdp_recipe[^\"']+\.jpg)",
        html,
    )
    return m.group(1) if m else None


async def download_if_needed(session: aiohttp.ClientSession, url: str, out_path: Path) -> None:
    if out_path.exists() and out_path.stat().st_size > 10_000:
        return
    async with session.get(url) as r:
        r.raise_for_status()
        out_path.write_bytes(await r.read())


def make_collage(image_paths: list[Path], out_path: Path) -> None:
    if not image_paths:
        return

    imgs = [Image.open(p).convert("RGB") for p in image_paths[:4]]

    if len(imgs) == 1:
        imgs[0].save(out_path, "JPEG", quality=90)
        return

    target_w, target_h = 640, 480
    imgs = [im.resize((target_w, target_h)) for im in imgs]

    if len(imgs) == 2:
        canvas = Image.new("RGB", (target_w * 2, target_h))
        canvas.paste(imgs[0], (0, 0))
        canvas.paste(imgs[1], (target_w, 0))
    else:
        canvas = Image.new("RGB", (target_w * 2, target_h * 2))
        canvas.paste(imgs[0], (0, 0))
        canvas.paste(imgs[1], (target_w, 0))
        canvas.paste(imgs[2], (0, target_h))
        if len(imgs) >= 4:
            canvas.paste(imgs[3], (target_w, target_h))

    canvas.save(out_path, "JPEG", quality=90)


async def refresh_week() -> dict[str, Any]:
    s = load_settings()

    locs = await get_localization_options(country=s.country)
    if not locs:
        raise RuntimeError(f"Brak lokalizacji dla country={s.country}")

    loc = next((l for l in locs if l.language.lower().startswith("pl")), locs[0])
    cfg = CookidooConfig(localization=loc, email=s.email, password=s.password)

    async with aiohttp.ClientSession() as session:
        api = Cookidoo(session, cfg)
        await api.login()

        today = date.today()
        days = await api.get_recipes_in_calendar_week(today)

        base, lang = cookidoo_base_and_lang(cfg.localization.url, cfg.localization.language)

        week_days: list[dict[str, Any]] = []
        week_image_pool: list[Path] = []

        # Zbierz unikalne recipe_id z tygodnia -> mniej scrapowania
        all_recipes = {}
        for d in days:
            for r in (getattr(d, "recipes", None) or []):
                all_recipes[r.id] = r

        # Scrapuj URL obrazków (po jednym na przepis)
        photo_urls: dict[str, str | None] = {}
        for rid in all_recipes.keys():
            photo_urls[rid] = await scrape_recipe_photo_url(session, base, lang, rid)

        # Pobierz obrazki i zbuduj payload dzienny/tygodniowy
        for d in days:
            day_id = getattr(d, "id", None) or ""
            recipes = getattr(d, "recipes", None) or []

            day_img_paths: list[Path] = []
            out_recipes: list[dict[str, Any]] = []

            for r in recipes:
                rid = r.id
                photo_url = photo_urls.get(rid)
                local_path = IMG_DIR / f"{rid}.jpg"

                if photo_url:
                    await download_if_needed(session, photo_url, local_path)
                    if local_path.exists():
                        day_img_paths.append(local_path)
                        week_image_pool.append(local_path)

                out_recipes.append(
                    {
                        "id": rid,
                        "name": r.name,
                        "total_time": getattr(r, "total_time", None),
                        "recipe_url": f"{base}/recipes/recipe/{lang}/{rid}",
                        "image_local": f"/api/image/{rid}.jpg" if local_path.exists() else None,
                        "image_remote": photo_url,
                    }
                )

            # opcjonalna dzienna kolażówka (jak chcesz później do dashboardu)
            day_jpg = IMG_DIR / f"day_{day_id}.jpg"
            if day_img_paths:
                make_collage(day_img_paths, day_jpg)

            week_days.append(
                {
                    "date": day_id,
                    "recipes": out_recipes,
                    "day_image_local": f"/api/day/{day_id}.jpg" if day_jpg.exists() else None,
                }
            )

        payload = {
            "generated_at": date.today().isoformat(),
            "days": week_days,
        }

        WEEK_JSON.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

        # kolaż tygodniowy (pierwsze 4 obrazki z tygodnia)
        if week_image_pool:
            make_collage(week_image_pool, WEEK_JPG)

        # today.json jako wycinek z week.json
        today_id = date.today().isoformat()
        today_obj = next((x for x in week_days if x["date"] == today_id), {"date": today_id, "recipes": []})
        TODAY_JSON.write_text(json.dumps(today_obj, ensure_ascii=False), encoding="utf-8")

        # (celowo) brak api.logout() bo biblioteka go nie ma
        return payload


_cache_week: dict[str, Any] = {"days": []}
_cache_today: dict[str, Any] = {"date": None, "recipes": []}


async def _refresh_loop(stop: asyncio.Event) -> None:
    while not stop.is_set():
        try:
            data = await refresh_week()
            _cache_week.update(data)
            if TODAY_JSON.exists():
                _cache_today.update(json.loads(TODAY_JSON.read_text(encoding="utf-8")))
        except Exception as e:
            print("Refresh error:", repr(e))
            if WEEK_JSON.exists():
                _cache_week.update(json.loads(WEEK_JSON.read_text(encoding="utf-8")))
            if TODAY_JSON.exists():
                _cache_today.update(json.loads(TODAY_JSON.read_text(encoding="utf-8")))

        s = load_settings()
        sleep_s = max(60, int(s.refresh_minutes) * 60)
        try:
            await asyncio.wait_for(stop.wait(), timeout=sleep_s)
        except asyncio.TimeoutError:
            pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    stop = asyncio.Event()
    task = asyncio.create_task(_refresh_loop(stop))
    try:
        yield
    finally:
        stop.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


app = FastAPI(title="Cookidoo Today", lifespan=lifespan)


@app.get("/")
async def root() -> JSONResponse:
    return JSONResponse(
        {
            "ok": True,
            "endpoints": [
                "/api/today",
                "/api/week",
                "/api/today.jpg",
                "/api/week.jpg",
                "/api/image/<recipe_id>.jpg",
                "/api/day/<YYYY-MM-DD>.jpg",
            ],
        }
    )


@app.get("/api/today")
async def api_today() -> JSONResponse:
    if TODAY_JSON.exists():
        return JSONResponse(json.loads(TODAY_JSON.read_text(encoding="utf-8")))
    return JSONResponse(_cache_today)


@app.get("/api/week")
async def api_week() -> JSONResponse:
    if WEEK_JSON.exists():
        return JSONResponse(json.loads(WEEK_JSON.read_text(encoding="utf-8")))
    return JSONResponse(_cache_week)


@app.get("/api/today.jpg")
async def api_today_jpg() -> Response:
    if TODAY_JPG.exists():
        return FileResponse(TODAY_JPG, media_type="image/jpeg")
    return Response(status_code=404)


@app.get("/api/week.jpg")
async def api_week_jpg() -> Response:
    if WEEK_JPG.exists():
        return FileResponse(WEEK_JPG, media_type="image/jpeg")
    return Response(status_code=404)


@app.get("/api/image/{recipe_id}.jpg")
async def api_recipe_jpg(recipe_id: str) -> Response:
    p = IMG_DIR / f"{recipe_id}.jpg"
    if p.exists():
        return FileResponse(p, media_type="image/jpeg")
    return Response(status_code=404)


@app.get("/api/day/{day}.jpg")
async def api_day_jpg(day: str) -> Response:
    p = IMG_DIR / f"day_{day}.jpg"
    if p.exists():
        return FileResponse(p, media_type="image/jpeg")
    return Response(status_code=404)
