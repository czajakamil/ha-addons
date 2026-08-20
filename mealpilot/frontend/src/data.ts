import type { Ingredient, PlanEntry, Recipe, ShoppingItem, WeekTemplate } from './types';

export const DAYS = ['Pon', 'Wt', 'Śr', 'Czw', 'Pt', 'Sob', 'Ndz'] as const;
export const MEALS = ['Śniadanie', 'Obiad', 'Kolacja'] as const;
export const MEAL_TYPES_ALL = [
  { id: 'Śniadanie', default: true },
  { id: 'II Śniadanie', default: false },
  { id: 'Obiad', default: true },
  { id: 'Przekąska', default: false },
  { id: 'Kolacja', default: true },
] as const;

export const TARGETS_CHANGED = 'mp:targets-changed';
export const emitTargetsChanged = (): void => {
  window.dispatchEvent(new Event(TARGETS_CHANGED));
};

export function categoryOf(name: string): string {
  const n = name.toLowerCase();
  if (/kurczak|łosos|łoso|tofu|jajko|wołowin|indyk|szynk/.test(n)) return 'Mięso, ryby, białko';
  if (/mleko|śmietan|feta|jogurt|masł|ser/.test(n)) return 'Nabiał';
  if (
    /papryka|cebul|ogórek|pomidor|marchew|ziemniak|brokuł|pieczark|czosnek|awokado|cytryn|szczypior|koperek|dymka|imbir/.test(
      n,
    )
  )
    return 'Warzywa i owoce';
  if (/banan|owoc/.test(n)) return 'Warzywa i owoce';
  if (/ryż|kasza|owsian|płatk|makaron|chleb|kromka/.test(n)) return 'Suche i zboża';
  if (/oliw|olej|sos|miód|przyp|tymianek|cynamon|oregano|sól|papryka słodka/.test(n))
    return 'Tłuszcze i przyprawy';
  if (/bulion|passata|oliwk|orzech/.test(n)) return 'Spiżarnia';
  return 'Inne';
}

export function currentWeekStart(): string {
  const today = new Date();
  const day = today.getDay(); // 0=Sun, 1=Mon, ...
  const offset = day === 0 ? 6 : day - 1; // days since Monday
  const monday = new Date(today);
  monday.setDate(today.getDate() - offset);
  return monday.toISOString().slice(0, 10);
}

export const WEEK_START = currentWeekStart();

interface State {
  recipes: Recipe[];
  plan: Record<string, PlanEntry[]>;
  shopping: Record<string, ShoppingItem[]>;
}

const state: State = { recipes: [], plan: { [WEEK_START]: [] }, shopping: {} };
export const apiBase = ((import.meta.env.VITE_API_BASE as string | undefined) ?? 'api').replace(
  /\/$/,
  '',
);

export async function apiFetch(path: string, init: RequestInit = {}): Promise<Response> {
  return fetch(`${apiBase}${path}`, { credentials: 'include', ...init });
}

async function jsonOrThrow<T>(res: Response): Promise<T> {
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json() as Promise<T>;
}

export async function loadRecipes(): Promise<void> {
  state.recipes = await jsonOrThrow<Recipe[]>(await apiFetch('/recipes'));
}

export async function loadPlan(ws: string = WEEK_START): Promise<void> {
  const data = await jsonOrThrow<{ entries?: PlanEntry[] }>(await apiFetch(`/plan/${ws}`));
  state.plan[ws] = data.entries ?? [];
}

export async function loadAll(): Promise<void> {
  await Promise.all([loadRecipes(), loadPlan(WEEK_START)]);
}

export function resetClientState(): void {
  state.recipes = [];
  state.plan = { [WEEK_START]: [] };
  state.shopping = {};
}

export async function savePlan(ws: string, entries: PlanEntry[]): Promise<PlanEntry[]> {
  const data = await jsonOrThrow<{ entries?: PlanEntry[] }>(
    await apiFetch(`/plan/${ws}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(entries),
    }),
  );
  state.plan[ws] = data.entries ?? [];
  return state.plan[ws];
}

export async function createRecipe(payload: Recipe): Promise<Recipe> {
  const r = await jsonOrThrow<Recipe>(
    await apiFetch('/recipes', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    }),
  );
  state.recipes = [...state.recipes, r];
  return r;
}

export async function updateRecipe(id: string, payload: Omit<Recipe, 'id'>): Promise<Recipe> {
  const r = await jsonOrThrow<Recipe>(
    await apiFetch(`/recipes/${encodeURIComponent(id)}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    }),
  );
  state.recipes = state.recipes.map((x) => (x.id === id ? r : x));
  return r;
}

export async function updateRecipeOwnership(
  id: string,
  shareWithHousehold: boolean,
): Promise<Recipe> {
  const r = await jsonOrThrow<Recipe>(
    await apiFetch(`/recipes/${encodeURIComponent(id)}/ownership`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ share_with_household: shareWithHousehold }),
    }),
  );
  state.recipes = state.recipes.map((x) => (x.id === id ? r : x));
  return r;
}

export function recipeImageUrl(recipe: Pick<Recipe, 'image_filename'>): string | null {
  if (!recipe.image_filename) return null;
  return `/images/${encodeURIComponent(recipe.image_filename)}`;
}

export async function uploadRecipeImage(id: string, file: File): Promise<Recipe> {
  const fd = new FormData();
  fd.append('file', file);
  const r = await jsonOrThrow<Recipe>(
    await apiFetch(`/recipes/${encodeURIComponent(id)}/image`, {
      method: 'POST',
      body: fd,
    }),
  );
  state.recipes = state.recipes.map((x) => (x.id === id ? r : x));
  return r;
}

export async function refreshRecipe(id: string): Promise<Recipe> {
  const r = await jsonOrThrow<Recipe>(await apiFetch(`/recipes/${encodeURIComponent(id)}`));
  state.recipes = state.recipes.map((x) => (x.id === id ? r : x));
  return r;
}

export async function rateRecipe(id: string, rating: number): Promise<void> {
  const res = await apiFetch(`/recipes/${encodeURIComponent(id)}/rating`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ rating }),
  });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
}

export async function deleteRating(id: string): Promise<void> {
  const res = await apiFetch(`/recipes/${encodeURIComponent(id)}/rating`, { method: 'DELETE' });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
}

export async function saveRecipeNote(id: string, note: string): Promise<void> {
  const res = await apiFetch(`/recipes/${encodeURIComponent(id)}/note`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ note }),
  });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  state.recipes = state.recipes.map((r) => (r.id === id ? { ...r, my_note: note } : r));
}

export async function deleteRecipeNote(id: string): Promise<void> {
  const res = await apiFetch(`/recipes/${encodeURIComponent(id)}/note`, { method: 'DELETE' });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  state.recipes = state.recipes.map((r) => (r.id === id ? { ...r, my_note: null } : r));
}

export async function deleteRecipeImage(id: string): Promise<Recipe> {
  const r = await jsonOrThrow<Recipe>(
    await apiFetch(`/recipes/${encodeURIComponent(id)}/image`, {
      method: 'DELETE',
    }),
  );
  state.recipes = state.recipes.map((x) => (x.id === id ? r : x));
  return r;
}

export async function deleteRecipe(id: string): Promise<void> {
  const res = await apiFetch(`/recipes/${encodeURIComponent(id)}`, { method: 'DELETE' });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  state.recipes = state.recipes.filter((r) => r.id !== id);
  for (const ws of Object.keys(state.plan)) {
    state.plan[ws] = (state.plan[ws] ?? []).filter((p) => p.recipe_id !== id);
  }
}

export const getRecipes = (): Recipe[] => state.recipes;
export const getPlan = (ws: string = WEEK_START): PlanEntry[] => state.plan[ws] ?? [];
export const getPlanMap = (): Record<string, PlanEntry[]> => state.plan;
export const recipeBy = (id: string): Recipe | undefined => state.recipes.find((r) => r.id === id);

export interface MacroEstimate {
  kcal: number;
  p: number;
  f: number;
  c: number;
}

export async function estimateMacros(payload: {
  title: string;
  servings: number;
  ingredients: Ingredient[];
}): Promise<MacroEstimate> {
  const res = await apiFetch('/recipes/estimate-macros', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  if (!res.ok) {
    const body = await res.text().catch(() => res.statusText);
    let msg = body;
    try {
      msg = (JSON.parse(body) as { detail?: string }).detail ?? body;
    } catch {
      /* ignore */
    }
    throw new Error(msg);
  }
  return res.json() as Promise<MacroEstimate>;
}

export async function listTemplates(): Promise<WeekTemplate[]> {
  return jsonOrThrow<WeekTemplate[]>(await apiFetch('/templates'));
}

export async function createTemplate(name: string, entries: PlanEntry[]): Promise<WeekTemplate> {
  return jsonOrThrow<WeekTemplate>(
    await apiFetch('/templates', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name, entries }),
    }),
  );
}

export async function deleteTemplate(id: number): Promise<void> {
  const res = await apiFetch(`/templates/${id}`, { method: 'DELETE' });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
}

export async function applyTemplate(id: number, weekStart: string): Promise<PlanEntry[]> {
  const data = await jsonOrThrow<{ entries?: PlanEntry[] }>(
    await apiFetch(`/templates/${id}/apply/${weekStart}`, { method: 'POST' }),
  );
  const entries = data.entries ?? [];
  state.plan[weekStart] = entries;
  return entries;
}

export const RECIPES_CHANGED = 'mp:recipes-changed';
export const emitRecipesChanged = (): void => {
  window.dispatchEvent(new Event(RECIPES_CHANGED));
};

export const PLAN_CHANGED = 'mp:plan-changed';
export const emitPlanChanged = (): void => {
  window.dispatchEvent(new Event(PLAN_CHANGED));
};

export const SHOPPING_CHANGED = 'mp:shopping-changed';
export const emitShoppingChanged = (): void => {
  window.dispatchEvent(new Event(SHOPPING_CHANGED));
};

export const getShopping = (ws: string = WEEK_START): ShoppingItem[] =>
  state.shopping[ws] ?? [];

export async function loadShopping(ws: string = WEEK_START): Promise<ShoppingItem[]> {
  const items = await jsonOrThrow<ShoppingItem[]>(await apiFetch(`/shopping/${ws}`));
  state.shopping[ws] = items;
  return items;
}

export async function regenerateShopping(ws: string = WEEK_START): Promise<ShoppingItem[]> {
  const items = await jsonOrThrow<ShoppingItem[]>(
    await apiFetch(`/shopping/${ws}/generate`, { method: 'POST' }),
  );
  state.shopping[ws] = items;
  return items;
}

export async function setShoppingChecked(
  ws: string,
  id: number,
  checked: boolean,
): Promise<ShoppingItem> {
  const item = await jsonOrThrow<ShoppingItem>(
    await apiFetch(`/shopping/${ws}/items/${id}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ checked }),
    }),
  );
  state.shopping[ws] = (state.shopping[ws] ?? []).map((it) => (it.id === id ? item : it));
  return item;
}

export async function addShoppingItem(
  ws: string,
  payload: { name: string; qty: number; unit: string; category?: string; recipe_id?: string },
): Promise<ShoppingItem> {
  const item = await jsonOrThrow<ShoppingItem>(
    await apiFetch(`/shopping/${ws}/items`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    }),
  );
  const list = state.shopping[ws] ?? [];
  const idx = list.findIndex((it) => it.id === item.id);
  state.shopping[ws] = idx >= 0 ? list.map((it) => (it.id === item.id ? item : it)) : [...list, item];
  return item;
}

export async function deleteShoppingItem(ws: string, id: number): Promise<void> {
  const res = await apiFetch(`/shopping/${ws}/items/${id}`, { method: 'DELETE' });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  state.shopping[ws] = (state.shopping[ws] ?? []).filter((it) => it.id !== id);
}

export async function clearShopping(ws: string = WEEK_START): Promise<void> {
  const res = await apiFetch(`/shopping/${ws}`, { method: 'DELETE' });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  state.shopping[ws] = [];
}
