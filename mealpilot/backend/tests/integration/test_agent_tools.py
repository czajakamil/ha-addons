"""Narzędzia agenta napędzane przez `services.registry.invoke` — czyli dokładnie
tą samą drogą, którą chodzi agent w aplikacji i serwer MCP.

Testy są synchroniczne (brak pytest-asyncio w środowisku), więc pojedyncze
wywołanie korutyny opakowujemy w `asyncio.run`.
"""

import asyncio

import pytest

from app import models
from app.services import registry
from app.services.errors import Invalid, NotFound

pytestmark = pytest.mark.integration

WEEK = "2026-07-06"  # poniedziałek
NEXT_WEEK = "2026-07-13"


@pytest.fixture
def call(admin_client, db_session):
    """Wywołuje narzędzie jako admin, na wyczyszczonej z seedu bibliotece.

    Seed daje każdemu nowemu użytkownikowi 8 przepisów i startowy plan; kasujemy je,
    żeby `total` / `has_more` dało się asertować dokładnie.
    """
    user = db_session.query(models.User).filter(models.User.username == "admin").one()
    for model in (models.ShoppingItemRecipe, models.ShoppingItem, models.MealPlanEntry, models.Recipe):
        db_session.query(model).delete()
    db_session.commit()

    def _call(name, args=None):
        return asyncio.run(registry.invoke(db_session, user, name, args or {}))

    return _call


def _make(call, title, **kwargs):
    payload = {"title": title, "servings": 1, "ingredients": [], "steps": ["Krok"], **kwargs}
    return call("create_recipe", payload)


# --------------------------------------------------------------------------- #
# search_recipes
# --------------------------------------------------------------------------- #


def test_search_finds_by_title_tag_and_ingredient(call):
    by_title = _make(call, "Łupacz po kaszubsku")
    by_tag = _make(call, "Danie drugie", tags=["Wegetariańskie"])
    by_ingredient = _make(call, "Danie trzecie", ingredients=[{"name": "ryż basmati", "qty": 100, "unit": "g"}])

    assert [i["id"] for i in call("search_recipes", {"query": "łupacz"})["items"]] == [by_title["id"]]
    assert [i["id"] for i in call("search_recipes", {"query": "wegetariańskie"})["items"]] == [by_tag["id"]]
    assert [i["id"] for i in call("search_recipes", {"query": "basmati"})["items"]] == [by_ingredient["id"]]


def test_search_is_case_and_accent_insensitive(call):
    lupacz = _make(call, "Łupacz po kaszubsku")
    rice = _make(call, "Danie z ryżem", ingredients=[{"name": "ryż", "qty": 100, "unit": "g"}])

    assert [i["id"] for i in call("search_recipes", {"query": "lupacz"})["items"]] == [lupacz["id"]]
    assert [i["id"] for i in call("search_recipes", {"query": "ŁUPACZ"})["items"]] == [lupacz["id"]]
    assert [i["id"] for i in call("search_recipes", {"query": "RYZ"})["items"]] == [rice["id"]]


def test_search_requires_every_token(call):
    _make(call, "Kurczak curry")
    _make(call, "Łupacz po kaszubsku")

    assert call("search_recipes", {"query": "kurczak curry"})["total"] == 1
    # Oba słowa muszą trafić w JEDEN przepis — nie w sumę wyników.
    assert call("search_recipes", {"query": "kurczak lupacz"})["total"] == 0


def test_search_ranks_title_above_ingredient(call):
    in_title = _make(call, "Sałatka z tuńczykiem")
    in_ingredient = _make(call, "Zapiekanka", ingredients=[{"name": "tuńczyk", "qty": 1, "unit": "szt"}])

    items = call("search_recipes", {"query": "tuńczyk"})["items"]
    assert [i["id"] for i in items] == [in_title["id"], in_ingredient["id"]]


def test_search_respects_limit_and_reports_pagination(call):
    for suffix in ("A", "B", "C"):
        _make(call, f"Zupa {suffix}")

    page = call("search_recipes", {"query": "zupa", "limit": 2})
    assert set(page) >= {"items", "total", "limit", "offset", "has_more", "query"}
    assert len(page["items"]) == 2
    assert page["total"] == 3
    assert page["has_more"] is True
    assert page["query"] == "zupa"

    full = call("search_recipes", {"query": "zupa"})
    assert len(full["items"]) == 3
    assert full["has_more"] is False


def test_search_rejects_empty_query(call):
    with pytest.raises(Invalid):
        call("search_recipes", {"query": "   "})


# --------------------------------------------------------------------------- #
# list_recipes / filter_recipes
# --------------------------------------------------------------------------- #


def test_list_returns_summaries_not_full_recipes(call):
    _make(
        call,
        "Skrót",
        ingredients=[{"name": "sól", "qty": 1, "unit": "g"}, {"name": "pieprz", "qty": 2, "unit": "g"}],
        steps=["a", "b", "c"],
    )
    item = call("list_recipes")["items"][0]
    assert "ingredients" not in item
    assert "steps" not in item
    assert item["ingredients_count"] == 2
    assert item["steps_count"] == 3


def test_list_paginates_with_total_and_has_more(call):
    for n in range(5):
        _make(call, f"Przepis {n}")

    page = call("list_recipes", {"limit": 2, "offset": 2})
    assert page["total"] == 5
    assert page["limit"] == 2
    assert page["offset"] == 2
    assert len(page["items"]) == 2
    assert page["has_more"] is True

    last = call("list_recipes", {"limit": 2, "offset": 4})
    assert len(last["items"]) == 1
    assert last["has_more"] is False

    # Strony nie zachodzą na siebie i razem dają komplet.
    ids = [i["id"] for i in call("list_recipes", {"limit": 2, "offset": 0})["items"]]
    ids += [i["id"] for i in page["items"]] + [i["id"] for i in last["items"]]
    assert len(set(ids)) == 5


def test_filter_recipes_also_returns_summaries(call):
    _make(call, "Filtrowany", ingredients=[{"name": "sól", "qty": 1, "unit": "g"}])
    item = call("filter_recipes")["items"][0]
    assert "ingredients" not in item and "steps" not in item
    assert item["ingredients_count"] == 1


def test_filter_tags_are_and(call):
    both = _make(call, "Oba tagi", tags=["szybkie", "vege"])
    _make(call, "Jeden tag", tags=["szybkie"])

    page = call("filter_recipes", {"tags": ["szybkie", "vege"]})
    assert [i["id"] for i in page["items"]] == [both["id"]]
    assert call("filter_recipes", {"tags": ["szybkie"]})["total"] == 2


def test_filter_meal_types_are_or(call):
    obiad = _make(call, "Na obiad", meal_types=["Obiad"])
    kolacja = _make(call, "Na kolację", meal_types=["Kolacja"])
    _make(call, "Bez typu")

    page = call("filter_recipes", {"meal_types": ["Obiad", "Kolacja"]})
    assert {i["id"] for i in page["items"]} == {obiad["id"], kolacja["id"]}


def test_filter_max_total_time_sums_prep_and_cook(call):
    fast = _make(call, "Szybkie", prep_time=5, cook_time=10)
    _make(call, "Wolne", prep_time=30, cook_time=30)

    page = call("filter_recipes", {"max_total_time": 15})
    assert [i["id"] for i in page["items"]] == [fast["id"]]
    # Granica jest domknięta (<=), a 16 min już nie mieści się w 15.
    assert call("filter_recipes", {"max_total_time": 14})["total"] == 0


def test_filter_is_meal_prep_both_ways(call):
    prep = _make(call, "Na zapas", is_meal_prep=True, meal_prep_days=3)
    normal = _make(call, "Zwykłe")

    assert [i["id"] for i in call("filter_recipes", {"is_meal_prep": True})["items"]] == [prep["id"]]
    assert [i["id"] for i in call("filter_recipes", {"is_meal_prep": False})["items"]] == [normal["id"]]


# --------------------------------------------------------------------------- #
# create_recipe
# --------------------------------------------------------------------------- #


def test_create_derives_and_deduplicates_the_slug(call):
    first = _make(call, "Sernik Babci")
    second = _make(call, "Sernik Babci")
    third = _make(call, "Sernik Babci")

    assert first["id"] == "sernik-babci"
    assert second["id"] == "sernik-babci-2"
    assert third["id"] == "sernik-babci-3"
    # Oba istnieją niezależnie.
    assert call("get_recipe", {"recipe_id": second["id"]})["title"] == "Sernik Babci"


def test_create_accepts_steps_as_strings_and_as_objects(call):
    plain = _make(call, "Kroki tekstem", steps=["Ugotuj", "Podaj"])
    rich = _make(
        call,
        "Kroki obiektami",
        steps=[{"text": "Ugotuj", "duration_minutes": 12}, {"text": "Podaj", "duration_minutes": None}],
    )

    assert plain["steps"] == [
        {"text": "Ugotuj", "duration_minutes": None},
        {"text": "Podaj", "duration_minutes": None},
    ]
    assert rich["steps"] == [
        {"text": "Ugotuj", "duration_minutes": 12},
        {"text": "Podaj", "duration_minutes": None},
    ]
    # Kształt jest ten sam także po ponownym odczycie z bazy.
    assert call("get_recipe", {"recipe_id": plain["id"]})["steps"] == plain["steps"]
    assert call("get_recipe", {"recipe_id": rich["id"]})["steps"] == rich["steps"]


def test_create_round_trips_meal_prep_fields(call):
    created = _make(
        call,
        "Chili na zapas",
        is_meal_prep=True,
        meal_prep_days=4,
        meal_prep_steps=[{"text": "Rozłóż do pojemników", "duration_minutes": 10}, "Zamroź"],
    )
    fetched = call("get_recipe", {"recipe_id": created["id"]})

    assert fetched["is_meal_prep"] is True
    assert fetched["meal_prep_days"] == 4
    assert fetched["meal_prep_steps"] == [
        {"text": "Rozłóż do pojemników", "duration_minutes": 10},
        {"text": "Zamroź", "duration_minutes": None},
    ]


def test_create_requires_a_title(call):
    with pytest.raises(Invalid):
        call("create_recipe", {"title": "  "})


# --------------------------------------------------------------------------- #
# rate_recipe / set_recipe_note
# --------------------------------------------------------------------------- #


def test_rate_recipe_sets_updates_and_clears(call):
    rid = _make(call, "Do oceny")["id"]

    set_ = call("rate_recipe", {"recipe_id": rid, "rating": 4})
    assert (set_["my_rating"], set_["rating_count"], set_["avg_rating"]) == (4, 1, 4.0)

    updated = call("rate_recipe", {"recipe_id": rid, "rating": 2})
    assert (updated["my_rating"], updated["rating_count"], updated["avg_rating"]) == (2, 1, 2.0)

    cleared = call("rate_recipe", {"recipe_id": rid, "rating": 0})
    assert cleared["my_rating"] is None
    assert cleared["rating_count"] == 0
    assert cleared["avg_rating"] is None


def test_rate_recipe_rejects_out_of_range(call):
    rid = _make(call, "Zakres")["id"]
    with pytest.raises(Invalid):
        call("rate_recipe", {"recipe_id": rid, "rating": 6})


def test_set_recipe_note_sets_and_clears(call):
    rid = _make(call, "Z notatką")["id"]

    assert call("set_recipe_note", {"recipe_id": rid, "note": "Za słone"})["my_note"] == "Za słone"
    assert call("set_recipe_note", {"recipe_id": rid, "note": "Lepiej z czosnkiem"})["my_note"] == "Lepiej z czosnkiem"
    assert call("set_recipe_note", {"recipe_id": rid, "note": ""})["my_note"] is None


# --------------------------------------------------------------------------- #
# get_week_nutrition_summary
# --------------------------------------------------------------------------- #


def test_nutrition_summary_scales_by_servings_and_totals_the_week(call):
    rid = _make(call, "Makro", servings=2, kcal=1000, p=100, f=50, c=200)["id"]
    # 1 porcja z przepisu na 2 → połowa; 4 porcje → podwójnie.
    call("add_plan_entry", {"week_start": WEEK, "day": 0, "meal": "Obiad", "recipe_id": rid, "servings": 1})
    call("add_plan_entry", {"week_start": WEEK, "day": 2, "meal": "Obiad", "recipe_id": rid, "servings": 4})

    summary = call("get_week_nutrition_summary", {"week_start": WEEK})
    assert summary["week_start"] == WEEK
    assert summary["days"]["0"] == {"kcal": 500.0, "p": 50.0, "f": 25.0, "c": 100.0}
    assert summary["days"]["2"] == {"kcal": 2000.0, "p": 200.0, "f": 100.0, "c": 400.0}
    assert summary["days"]["1"] == {"kcal": 0.0, "p": 0.0, "f": 0.0, "c": 0.0}
    assert set(summary["days"]) == {str(d) for d in range(7)}
    assert summary["week_total"] == {"kcal": 2500.0, "p": 250.0, "f": 125.0, "c": 500.0}


def test_nutrition_summary_rejects_a_non_monday(call):
    with pytest.raises(Invalid):
        call("get_week_nutrition_summary", {"week_start": "2026-07-07"})


# --------------------------------------------------------------------------- #
# Szablony tygodnia
# --------------------------------------------------------------------------- #


def test_week_template_round_trip(call):
    a = _make(call, "Pierwszy")["id"]
    b = _make(call, "Drugi")["id"]
    call("add_plan_entry", {"week_start": WEEK, "day": 0, "meal": "Obiad", "recipe_id": a, "servings": 2})
    call("add_plan_entry", {"week_start": WEEK, "day": 1, "meal": "Kolacja", "recipe_id": b, "servings": 1})

    saved = call("save_week_as_template", {"week_start": WEEK, "name": "Mój tydzień"})
    assert saved["name"] == "Mój tydzień"
    assert saved["entry_count"] == 2

    listed = call("list_week_templates")
    assert listed["total"] == 1
    assert listed["templates"][0]["id"] == saved["id"]

    applied = call("apply_week_template", {"template_id": saved["id"], "week_start": NEXT_WEEK})
    assert applied["week_start"] == NEXT_WEEK
    assert applied["applied_template"] == "Mój tydzień"
    assert applied["skipped_recipe_ids"] == []
    assert {(e["day"], e["meal"], e["recipe_id"], e["servings"]) for e in applied["entries"]} == {
        (0, "Obiad", a, 2),
        (1, "Kolacja", b, 1),
    }


def test_apply_template_skips_recipes_that_no_longer_exist(call):
    a = _make(call, "Zostaje")["id"]
    b = _make(call, "Zniknie")["id"]
    call("add_plan_entry", {"week_start": WEEK, "day": 0, "meal": "Obiad", "recipe_id": a, "servings": 1})
    call("add_plan_entry", {"week_start": WEEK, "day": 1, "meal": "Obiad", "recipe_id": b, "servings": 1})
    saved = call("save_week_as_template", {"week_start": WEEK, "name": "Z dziurą"})

    call("delete_recipe", {"recipe_id": b})

    applied = call("apply_week_template", {"template_id": saved["id"], "week_start": NEXT_WEEK})
    assert applied["skipped_recipe_ids"] == [b]
    assert [e["recipe_id"] for e in applied["entries"]] == [a]


def test_save_template_rejects_an_empty_week(call):
    with pytest.raises(Invalid):
        call("save_week_as_template", {"week_start": NEXT_WEEK, "name": "Pusty"})


# --------------------------------------------------------------------------- #
# Dyspozytor
# --------------------------------------------------------------------------- #


def test_unknown_tool_name_raises_not_found(call):
    with pytest.raises(NotFound):
        call("wyczaruj_obiad", {})


def test_alias_dispatches_to_the_real_tool(call):
    rid = _make(call, "Do listy")["id"]
    item = call("add_shopping_item", {"week_start": WEEK, "name": "Masło", "qty": 1, "unit": "szt", "recipe_id": rid})
    assert call("remove_shopping_item", {"item_id": item["id"]}) == {"deleted": item["id"]}
