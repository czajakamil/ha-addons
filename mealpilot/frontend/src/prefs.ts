import type { MacroTarget } from './types';
import { apiFetch } from './data';

export interface UiPrefs {
  recipesGrouped: boolean;
  macroTargets: MacroTarget;
  favoriteRecipeIds: string[];
}

const DEFAULTS: UiPrefs = {
  recipesGrouped: false,
  macroTargets: { kcal: 2200, p: 130, f: 70, c: 260 },
  favoriteRecipeIds: [],
};

interface ServerShape {
  recipes_grouped?: boolean;
  macro_targets?: { kcal?: number; p?: number; f?: number; c?: number };
  favorite_recipe_ids?: string[];
}

let cache: UiPrefs = { ...DEFAULTS, macroTargets: { ...DEFAULTS.macroTargets }, favoriteRecipeIds: [] };
let loaded = false;

function fromServer(s: ServerShape): UiPrefs {
  const mt = s.macro_targets ?? {};
  return {
    recipesGrouped: s.recipes_grouped ?? DEFAULTS.recipesGrouped,
    macroTargets: {
      kcal: mt.kcal ?? DEFAULTS.macroTargets.kcal,
      p: mt.p ?? DEFAULTS.macroTargets.p,
      f: mt.f ?? DEFAULTS.macroTargets.f,
      c: mt.c ?? DEFAULTS.macroTargets.c,
    },
    favoriteRecipeIds: s.favorite_recipe_ids ?? [],
  };
}

export async function fetchUiPrefs(): Promise<UiPrefs> {
  const res = await apiFetch('/settings/ui');
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  cache = fromServer((await res.json()) as ServerShape);
  loaded = true;
  return cache;
}

export async function patchUiPrefs(patch: Partial<UiPrefs>): Promise<void> {
  const body: Record<string, unknown> = {};
  if (patch.recipesGrouped !== undefined) body.recipes_grouped = patch.recipesGrouped;
  if (patch.macroTargets !== undefined) body.macro_targets = patch.macroTargets;
  if (patch.favoriteRecipeIds !== undefined) body.favorite_recipe_ids = patch.favoriteRecipeIds;

  const res = await apiFetch('/settings/ui', {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  cache = fromServer((await res.json()) as ServerShape);
}

export function getUiPrefs(): UiPrefs {
  return cache;
}

export function isUiPrefsLoaded(): boolean {
  return loaded;
}

export function resetUiPrefsCache(): void {
  cache = { ...DEFAULTS, macroTargets: { ...DEFAULTS.macroTargets }, favoriteRecipeIds: [] };
  loaded = false;
}
