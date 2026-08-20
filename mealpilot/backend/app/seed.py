"""Seed initial recipes and a starting week plan if the DB is empty."""

from sqlalchemy.orm import Session

from . import models

RECIPES = [
    {
        "id": "kurczak_z_ryzem",
        "title": "Kurczak z ryżem i warzywami",
        "tags": ["lunch", "high-protein", "meal-prep"],
        "servings": 4,
        "prep_time": 15,
        "cook_time": 30,
        "kcal": 612,
        "p": 48,
        "f": 14,
        "c": 72,
        "hue": 38,
        "ingredients": [
            {"name": "pierś kurczaka", "qty": 600, "unit": "g"},
            {"name": "ryż basmati", "qty": 300, "unit": "g"},
            {"name": "papryka czerwona", "qty": 2, "unit": "szt"},
            {"name": "cebula", "qty": 1, "unit": "szt"},
            {"name": "czosnek", "qty": 3, "unit": "ząbek"},
            {"name": "oliwa", "qty": 2, "unit": "łyżka"},
            {"name": "sól", "qty": 1, "unit": "łyżeczka"},
            {"name": "papryka słodka", "qty": 1, "unit": "łyżeczka"},
        ],
        "steps": [
            "Pokrój pierś kurczaka w kostkę 2 cm i zamarynuj w oliwie z papryką.",
            "Ugotuj ryż basmati w 600 ml osolonej wody (15 min).",
            "Podsmaż cebulę i czosnek, dodaj kurczaka, smaż 8–10 min.",
            "Dodaj pokrojoną paprykę, duś kolejne 5 min.",
            "Wymieszaj z ryżem, dopraw.",
        ],
    },
    {
        "id": "owsianka_z_owocami",
        "title": "Owsianka z owocami i orzechami",
        "tags": ["śniadanie", "vege", "szybkie"],
        "servings": 1,
        "prep_time": 5,
        "cook_time": 8,
        "kcal": 420,
        "p": 16,
        "f": 12,
        "c": 62,
        "hue": 70,
        "ingredients": [
            {"name": "płatki owsiane", "qty": 60, "unit": "g"},
            {"name": "mleko", "qty": 250, "unit": "ml"},
            {"name": "banan", "qty": 1, "unit": "szt"},
            {"name": "orzechy włoskie", "qty": 20, "unit": "g"},
            {"name": "miód", "qty": 1, "unit": "łyżeczka"},
            {"name": "cynamon", "qty": 1, "unit": "szczypta"},
        ],
        "steps": [
            "Zagotuj mleko z cynamonem.",
            "Dodaj płatki, gotuj 5 min mieszając.",
            "Pokrój banana, dodaj orzechy i miód.",
        ],
    },
    {
        "id": "salatka_grecka",
        "title": "Sałatka grecka z fetą",
        "tags": ["kolacja", "vege", "letnie"],
        "servings": 2,
        "prep_time": 15,
        "cook_time": 0,
        "kcal": 380,
        "p": 14,
        "f": 28,
        "c": 18,
        "hue": 130,
        "ingredients": [
            {"name": "ogórek", "qty": 1, "unit": "szt"},
            {"name": "pomidor", "qty": 3, "unit": "szt"},
            {"name": "feta", "qty": 200, "unit": "g"},
            {"name": "oliwki czarne", "qty": 100, "unit": "g"},
            {"name": "cebula czerwona", "qty": 0.5, "unit": "szt"},
            {"name": "oliwa", "qty": 3, "unit": "łyżka"},
            {"name": "oregano", "qty": 1, "unit": "łyżeczka"},
        ],
        "steps": [
            "Pokrój pomidory i ogórki w grube kawałki.",
            "Posiekaj cebulę, dodaj oliwki.",
            "Pokrusz fetę, polej oliwą, posyp oregano.",
        ],
    },
    {
        "id": "losos_pieczony",
        "title": "Łosoś pieczony z brokułem",
        "tags": ["obiad", "high-protein", "omega-3"],
        "servings": 2,
        "prep_time": 10,
        "cook_time": 25,
        "kcal": 540,
        "p": 42,
        "f": 28,
        "c": 22,
        "hue": 12,
        "ingredients": [
            {"name": "filet z łososia", "qty": 400, "unit": "g"},
            {"name": "brokuł", "qty": 1, "unit": "szt"},
            {"name": "ziemniaki młode", "qty": 400, "unit": "g"},
            {"name": "cytryna", "qty": 1, "unit": "szt"},
            {"name": "oliwa", "qty": 2, "unit": "łyżka"},
            {"name": "koperek", "qty": 1, "unit": "łyżka"},
        ],
        "steps": [
            "Rozgrzej piekarnik do 200°C.",
            "Ułóż łososia na blasze, polej oliwą, sokiem z cytryny.",
            "Dodaj różyczki brokuła i pokrojone ziemniaki.",
            "Piecz 22 min, posyp koperkiem.",
        ],
    },
    {
        "id": "kasza_z_warzywami",
        "title": "Kasza gryczana z pieczarkami",
        "tags": ["obiad", "vege", "tanio"],
        "servings": 3,
        "prep_time": 10,
        "cook_time": 25,
        "kcal": 410,
        "p": 14,
        "f": 10,
        "c": 64,
        "hue": 50,
        "ingredients": [
            {"name": "kasza gryczana", "qty": 250, "unit": "g"},
            {"name": "pieczarki", "qty": 300, "unit": "g"},
            {"name": "cebula", "qty": 1, "unit": "szt"},
            {"name": "marchew", "qty": 2, "unit": "szt"},
            {"name": "masło", "qty": 1, "unit": "łyżka"},
            {"name": "tymianek", "qty": 1, "unit": "łyżeczka"},
        ],
        "steps": [
            "Ugotuj kaszę w 500 ml wody (15 min).",
            "Podsmaż cebulę z marchewką na maśle.",
            "Dodaj pieczarki, smaż do złotego koloru.",
            "Wymieszaj z kaszą, dopraw tymiankiem.",
        ],
    },
    {
        "id": "zupa_pomidorowa",
        "title": "Zupa pomidorowa z ryżem",
        "tags": ["obiad", "vege", "comfort"],
        "servings": 4,
        "prep_time": 10,
        "cook_time": 30,
        "kcal": 280,
        "p": 8,
        "f": 8,
        "c": 42,
        "hue": 25,
        "ingredients": [
            {"name": "passata pomidorowa", "qty": 700, "unit": "g"},
            {"name": "bulion warzywny", "qty": 1, "unit": "l"},
            {"name": "ryż basmati", "qty": 100, "unit": "g"},
            {"name": "śmietana 18%", "qty": 200, "unit": "ml"},
            {"name": "marchew", "qty": 1, "unit": "szt"},
            {"name": "cebula", "qty": 1, "unit": "szt"},
        ],
        "steps": [
            "Podsmaż cebulę i marchew.",
            "Dodaj passatę i bulion, gotuj 15 min.",
            "Ugotuj ryż osobno.",
            "Wmieszaj śmietanę, podawaj z ryżem.",
        ],
    },
    {
        "id": "tofu_stir_fry",
        "title": "Tofu stir-fry z makaronem",
        "tags": ["kolacja", "vege", "azjatycka"],
        "servings": 2,
        "prep_time": 15,
        "cook_time": 15,
        "kcal": 520,
        "p": 24,
        "f": 16,
        "c": 68,
        "hue": 95,
        "ingredients": [
            {"name": "tofu", "qty": 250, "unit": "g"},
            {"name": "makaron ryżowy", "qty": 200, "unit": "g"},
            {"name": "papryka czerwona", "qty": 1, "unit": "szt"},
            {"name": "cebula dymka", "qty": 4, "unit": "szt"},
            {"name": "sos sojowy", "qty": 3, "unit": "łyżka"},
            {"name": "imbir", "qty": 1, "unit": "łyżeczka"},
            {"name": "olej sezamowy", "qty": 1, "unit": "łyżka"},
        ],
        "steps": [
            "Pokrój tofu w kostkę, podsmaż na rozgrzanym oleju.",
            "Ugotuj makaron według opakowania.",
            "Dodaj paprykę i imbir, smaż 3 min.",
            "Wymieszaj z makaronem i sosem sojowym.",
        ],
    },
    {
        "id": "jajecznica",
        "title": "Jajecznica z awokado",
        "tags": ["śniadanie", "high-protein", "szybkie"],
        "servings": 1,
        "prep_time": 5,
        "cook_time": 5,
        "kcal": 380,
        "p": 22,
        "f": 28,
        "c": 8,
        "hue": 80,
        "ingredients": [
            {"name": "jajko", "qty": 3, "unit": "szt"},
            {"name": "awokado", "qty": 0.5, "unit": "szt"},
            {"name": "masło", "qty": 1, "unit": "łyżeczka"},
            {"name": "szczypiorek", "qty": 1, "unit": "łyżka"},
            {"name": "chleb żytni", "qty": 2, "unit": "kromka"},
        ],
        "steps": [
            "Rozbełtaj jajka z solą.",
            "Smaż na maśle na małym ogniu, mieszając.",
            "Podawaj z awokado i chlebem.",
        ],
    },
]

WEEK_START = "2026-05-04"
PLAN = [
    (0, "Śniadanie", "owsianka_z_owocami"),
    (0, "Obiad", "kurczak_z_ryzem"),
    (0, "Kolacja", "salatka_grecka"),
    (1, "Śniadanie", "jajecznica"),
    (1, "Obiad", "kasza_z_warzywami"),
    (1, "Kolacja", "tofu_stir_fry"),
    (2, "Śniadanie", "owsianka_z_owocami"),
    (2, "Obiad", "losos_pieczony"),
    (3, "Śniadanie", "jajecznica"),
    (3, "Obiad", "zupa_pomidorowa"),
    (3, "Kolacja", "salatka_grecka"),
    (4, "Obiad", "kurczak_z_ryzem"),
    (4, "Kolacja", "kasza_z_warzywami"),
    (5, "Śniadanie", "owsianka_z_owocami"),
    (5, "Obiad", "losos_pieczony"),
]


def seed_for_user(db: Session, user_id: int) -> None:
    """Seed demo recipes and a starter plan for a fresh user account."""
    has_recipes = db.query(models.Recipe).filter(models.Recipe.created_by == user_id).count() > 0
    if not has_recipes:
        for r in RECIPES:
            data = {**r, "id": f"u{user_id}_{r['id']}"}
            db.add(models.Recipe(created_by=user_id, owner_user_id=user_id, **data))
        db.commit()

    has_plan = db.query(models.MealPlanEntry).filter(models.MealPlanEntry.created_by == user_id).count() > 0
    if not has_plan:
        for day, meal, recipe_id in PLAN:
            db.add(
                models.MealPlanEntry(
                    created_by=user_id,
                    owner_user_id=user_id,
                    week_start=WEEK_START,
                    day=day,
                    meal=meal,
                    recipe_id=f"u{user_id}_{recipe_id}",
                    servings=1,
                )
            )
        db.commit()
