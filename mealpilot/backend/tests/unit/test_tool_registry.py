"""Parity między jedynym rejestrem narzędzi a wszystkimi powierzchniami.

Dryf między `app/services/registry.py` a listami w agencie / MCP był źródłem
błędów, których nie łapał żaden test. Te asercje pilnują, że lista jest jedna.
"""

import jsonschema
import pytest

from app.agent.tools import TOOL_DEFS, TOOL_DEFS_OPENAI
from app.mcpserver.server import TOOLS as MCP_TOOLS
from app.mcpserver.server import _annotations
from app.services import registry
from app.services.errors import NotFound

pytestmark = pytest.mark.unit

# Jedyne narzędzie, które nie jest read_only, a mimo to nic nie zapisuje w bazie
# (woła LLM i zwraca oszacowanie) — dlatego wolno mu mieć puste `changed`.
NON_MUTATING_WRITE_TOOLS = {"estimate_recipe_macros"}

GROUP_NAMES = {name for name, _emoji in registry.GROUP_ORDER}


def test_registry_is_not_empty():
    assert len(registry.TOOL_SPECS) == len(registry.TOOL_NAMES) > 0


@pytest.mark.parametrize("name", registry.TOOL_NAMES)
def test_every_name_is_dispatchable(name):
    assert registry.get_spec(name).name == name


def test_every_alias_resolves_to_a_real_spec():
    aliases = {alias: spec for spec in registry.TOOL_SPECS for alias in spec.aliases}
    assert aliases, "brak aliasów — jeśli to celowe, usuń ten test razem z polem `aliases`"
    for alias, spec in aliases.items():
        assert alias not in registry.TOOL_NAMES, f"alias {alias} koliduje z prawdziwą nazwą"
        assert registry.get_spec(alias) is spec


def test_unknown_tool_name_raises_not_found():
    with pytest.raises(NotFound):
        registry.get_spec("nie_ma_takiego_narzedzia")


def test_agent_tool_defs_match_registry():
    assert {t["name"] for t in TOOL_DEFS} == set(registry.TOOL_NAMES)


def test_openai_tool_defs_match_registry():
    assert {t["function"]["name"] for t in TOOL_DEFS_OPENAI} == set(registry.TOOL_NAMES)


def test_mcp_tools_match_registry():
    assert {t.name for t in MCP_TOOLS} == set(registry.TOOL_NAMES)


@pytest.mark.parametrize("spec", registry.TOOL_SPECS, ids=lambda s: s.name)
def test_schemas_are_valid_json_schema(spec):
    jsonschema.Draft202012Validator.check_schema(spec.input_schema)
    assert spec.input_schema["type"] == "object"
    if spec.output_schema is not None:
        jsonschema.Draft202012Validator.check_schema(spec.output_schema)


@pytest.mark.parametrize("spec", registry.TOOL_SPECS, ids=lambda s: s.name)
def test_spec_metadata_is_filled_in(spec):
    assert spec.title.strip()
    assert spec.summary.strip()
    assert spec.description.strip()
    assert spec.group in GROUP_NAMES


@pytest.mark.parametrize("spec", registry.TOOL_SPECS, ids=lambda s: s.name)
def test_changed_domains_match_read_only_flag(spec):
    if spec.read_only:
        assert spec.changed == (), "narzędzie tylko-do-odczytu nie może deklarować zmienionych domen"
        assert not spec.destructive
    elif spec.name not in NON_MUTATING_WRITE_TOOLS:
        assert spec.changed, "narzędzie zapisujące musi zadeklarować co najmniej jedną domenę `changed`"


@pytest.mark.parametrize("spec", registry.TOOL_SPECS, ids=lambda s: s.name)
def test_scope_follows_read_only(spec):
    assert spec.scope == ("read" if spec.read_only else "write")


def test_mcp_annotations_mirror_the_spec():
    by_name = {t.name: t for t in MCP_TOOLS}
    for spec in registry.TOOL_SPECS:
        tool = by_name[spec.name]
        assert tool.title == spec.title
        assert tool.description == spec.description
        assert tool.inputSchema == spec.input_schema
        assert tool.outputSchema == spec.output_schema
        ann = tool.annotations
        assert ann is not None
        assert ann.title == spec.title
        assert ann.readOnlyHint is spec.read_only
        assert ann.destructiveHint is spec.destructive
        assert ann.idempotentHint is spec.idempotent
        assert ann.openWorldHint is False


def test_annotations_helper_reflects_flags():
    spec = registry.get_spec("delete_recipe")
    ann = _annotations(spec)
    assert (ann.readOnlyHint, ann.destructiveHint) == (False, True)


def test_describe_is_json_serializable():
    import json

    for spec in registry.TOOL_SPECS:
        payload = registry.describe(spec)
        assert payload["name"] == spec.name
        assert payload["changed"] == list(spec.changed)
        json.dumps(payload)


def test_write_and_destructive_sets_agree_with_specs():
    assert set(registry.WRITE_TOOLS) == {s.name for s in registry.TOOL_SPECS if not s.read_only}
    assert set(registry.DESTRUCTIVE_TOOLS) == {s.name for s in registry.TOOL_SPECS if s.destructive}
    assert registry.DESTRUCTIVE_TOOLS <= registry.WRITE_TOOLS
