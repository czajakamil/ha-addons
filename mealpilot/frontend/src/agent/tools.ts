import {
  apiFetch,
  emitPlanChanged,
  emitRecipesChanged,
  emitShoppingChanged,
  loadPlan,
  loadRecipes,
  loadShopping,
} from '../data';

export interface ToolDef {
  name: string;
  description: string;
  parameters: Record<string, unknown>;
  group: ToolGroup;
  handler: (args: Record<string, unknown>) => Promise<unknown>;
}

export type ToolGroup = 'Przepisy' | 'Plan tygodnia' | 'Lista zakupów';

export const GROUP_ORDER: { label: ToolGroup; icon: string }[] = [
  { label: 'Przepisy', icon: '📖' },
  { label: 'Plan tygodnia', icon: '📅' },
  { label: 'Lista zakupów', icon: '🛒' },
];

async function req(method: string, path: string, body?: unknown): Promise<unknown> {
  const init: RequestInit = { method };
  if (body !== undefined) {
    init.headers = { 'Content-Type': 'application/json' };
    init.body = JSON.stringify(body);
  }
  const res = await apiFetch(path, init);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`HTTP ${res.status}: ${text}`);
  }
  if (res.status === 204) return null;
  const ctype = res.headers.get('content-type') || '';
  if (ctype.includes('application/json')) return res.json();
  return res.text();
}

function mondayOf(d: Date): string {
  const day = d.getDay();
  const offset = day === 0 ? -6 : 1 - day;
  const m = new Date(d);
  m.setDate(d.getDate() + offset);
  return m.toISOString().slice(0, 10);
}

function currentWeekStart(): string {
  return mondayOf(new Date());
}

interface Recipe {
  id: string;
  title: string;
  servings: number;
  kcal: number;
  p: number;
  f: number;
  c: number;
}
interface PlanEntry {
  day: number;
  meal: string;
  recipe_id: string;
  servings: number;
}
interface WeekPlan {
  week_start: string;
  entries: PlanEntry[];
}

async function getRecipes(): Promise<Recipe[]> {
  return (await req('GET', '/recipes')) as Recipe[];
}

async function getPlanEntries(ws: string): Promise<PlanEntry[]> {
  const plan = (await req('GET', `/plan/${ws}`)) as WeekPlan;
  return plan.entries || [];
}

async function putPlan(ws: string, entries: PlanEntry[]): Promise<unknown> {
  const result = await req('PUT', `/plan/${ws}`, entries);
  await loadPlan(ws);
  emitPlanChanged();
  return result;
}

async function enrichPlan(plan: WeekPlan): Promise<WeekPlan & { entries: (PlanEntry & { recipe_title: string })[] }> {
  const recipes = await getRecipes();
  const titles = new Map(recipes.map((r) => [r.id, r.title]));
  return {
    ...plan,
    entries: (plan.entries || []).map((e) => ({ ...e, recipe_title: titles.get(e.recipe_id) || '' })),
  };
}

export const TOOLS: ToolDef[] = [
  {
    name: 'list_recipes',
    description: 'Zwraca wszystkie przepisy zalogowanego użytkownika.',
    group: 'Przepisy',
    parameters: { type: 'object', properties: {} },
    handler: () => req('GET', '/recipes'),
  },
  {
    name: 'get_recipe',
    description: 'Szczegóły jednego przepisu (składniki, kroki, makro).',
    group: 'Przepisy',
    parameters: {
      type: 'object',
      properties: { recipe_id: { type: 'string' } },
      required: ['recipe_id'],
    },
    handler: (a) => req('GET', `/recipes/${String(a.recipe_id)}`),
  },
  {
    name: 'list_tags',
    description:
      'Wszystkie unikalne tagi używane w bibliotece przepisów. Wywołaj jako pierwsze, by wiedzieć jakimi wartościami operuje użytkownik.',
    group: 'Przepisy',
    parameters: { type: 'object', properties: {} },
    handler: () => req('GET', '/recipes/meta/tags'),
  },
  {
    name: 'list_meal_types',
    description: 'Wszystkie unikalne typy posiłków zdefiniowane w przepisach.',
    group: 'Przepisy',
    parameters: { type: 'object', properties: {} },
    handler: () => req('GET', '/recipes/meta/meal_types'),
  },
  {
    name: 'filter_recipes',
    description:
      "Zwraca przepisy spełniające kryteria. Użyj gdy user chce np. 'azjatyckie na obiad' lub 'lekkie śniadanie poniżej 400 kcal'.",
    group: 'Przepisy',
    parameters: {
      type: 'object',
      properties: {
        tags: { type: 'array', items: { type: 'string' } },
        meal_types: { type: 'array', items: { type: 'string' } },
        max_kcal: { type: 'number' },
        min_protein: { type: 'number' },
      },
    },
    handler: (a) => {
      const params = new URLSearchParams();
      if (Array.isArray(a.tags) && a.tags.length) params.set('tags', (a.tags as string[]).join(','));
      if (Array.isArray(a.meal_types) && a.meal_types.length)
        params.set('meal_types', (a.meal_types as string[]).join(','));
      if (a.max_kcal !== undefined) params.set('max_kcal', String(a.max_kcal));
      if (a.min_protein !== undefined) params.set('min_protein', String(a.min_protein));
      const q = params.toString();
      return req('GET', `/recipes${q ? `?${q}` : ''}`);
    },
  },
  {
    name: 'create_recipe',
    description:
      'Dodaje nowy przepis do biblioteki. Użyj gdy user opisuje nowy przepis lub wkleja go z internetu. ' +
      'Wygeneruj slug id (małe litery, polskie znaki → ASCII, spacje → "-"). Jeśli user nie podał makro/kcal — oszacuj na podstawie składników i zaznacz w podglądzie, że są szacunkowe. ' +
      'Zawsze pokaż użytkownikowi podgląd i czekaj na potwierdzenie przed wywołaniem.',
    group: 'Przepisy',
    parameters: {
      type: 'object',
      properties: {
        id: { type: 'string' },
        title: { type: 'string' },
        tags: { type: 'array', items: { type: 'string' } },
        meal_types: { type: 'array', items: { type: 'string' } },
        servings: { type: 'integer' },
        prep_time: { type: 'integer' },
        cook_time: { type: 'integer' },
        kcal: { type: 'number' },
        p: { type: 'number' },
        f: { type: 'number' },
        c: { type: 'number' },
        hue: { type: 'integer', minimum: 0, maximum: 360 },
        ingredients: {
          type: 'array',
          items: {
            type: 'object',
            properties: {
              name: { type: 'string' },
              qty: { type: 'number' },
              unit: { type: 'string' },
            },
            required: ['name', 'qty', 'unit'],
          },
        },
        steps: { type: 'array', items: { type: 'string' } },
      },
      required: ['id', 'title'],
    },
    handler: async (a) => {
      const result = await req('POST', '/recipes', a);
      await loadRecipes();
      emitRecipesChanged();
      return result;
    },
  },
  {
    name: 'update_recipe',
    description: 'Aktualizuje wybrane pola istniejącego przepisu. Pokaż podgląd zmian i czekaj na potwierdzenie.',
    group: 'Przepisy',
    parameters: {
      type: 'object',
      properties: {
        recipe_id: { type: 'string' },
        title: { type: 'string' },
        tags: { type: 'array', items: { type: 'string' } },
        meal_types: { type: 'array', items: { type: 'string' } },
        servings: { type: 'integer' },
        prep_time: { type: 'integer' },
        cook_time: { type: 'integer' },
        kcal: { type: 'number' },
        p: { type: 'number' },
        f: { type: 'number' },
        c: { type: 'number' },
        hue: { type: 'integer', minimum: 0, maximum: 360 },
        ingredients: {
          type: 'array',
          items: {
            type: 'object',
            properties: {
              name: { type: 'string' },
              qty: { type: 'number' },
              unit: { type: 'string' },
            },
            required: ['name', 'qty', 'unit'],
          },
        },
        steps: { type: 'array', items: { type: 'string' } },
      },
      required: ['recipe_id'],
    },
    handler: async (a) => {
      const { recipe_id, ...rest } = a;
      const result = await req('PUT', `/recipes/${String(recipe_id)}`, rest);
      await loadRecipes();
      emitRecipesChanged();
      return result;
    },
  },
  {
    name: 'delete_recipe',
    description: 'Usuwa przepis i powiązane wpisy w planie tygodnia. Zawsze potwierdź z użytkownikiem przed wywołaniem.',
    group: 'Przepisy',
    parameters: {
      type: 'object',
      properties: { recipe_id: { type: 'string' } },
      required: ['recipe_id'],
    },
    handler: async (a) => {
      const result = await req('DELETE', `/recipes/${String(a.recipe_id)}`);
      await Promise.all([loadRecipes(), loadPlan()]);
      emitRecipesChanged();
      emitPlanChanged();
      return result;
    },
  },
  {
    name: 'get_week_plan',
    description: 'Plan posiłków na dany tydzień z tytułami przepisów. week_start = poniedziałek (YYYY-MM-DD).',
    group: 'Plan tygodnia',
    parameters: {
      type: 'object',
      properties: { week_start: { type: 'string' } },
      required: ['week_start'],
    },
    handler: async (a) => {
      const plan = (await req('GET', `/plan/${String(a.week_start)}`)) as WeekPlan;
      return enrichPlan(plan);
    },
  },
  {
    name: 'get_current_week_plan',
    description: 'Plan na bieżący tydzień (week_start liczony automatycznie).',
    group: 'Plan tygodnia',
    parameters: { type: 'object', properties: {} },
    handler: async () => {
      const ws = currentWeekStart();
      const plan = (await req('GET', `/plan/${ws}`)) as WeekPlan;
      return enrichPlan(plan);
    },
  },
  {
    name: 'set_week_plan',
    description:
      'Zastępuje cały plan tygodnia. UWAGA: zawsze najpierw pokaż użytkownikowi podgląd i poczekaj na potwierdzenie.',
    group: 'Plan tygodnia',
    parameters: {
      type: 'object',
      properties: {
        week_start: { type: 'string' },
        entries: {
          type: 'array',
          items: {
            type: 'object',
            properties: {
              day: { type: 'integer', minimum: 0, maximum: 6 },
              meal: { type: 'string' },
              recipe_id: { type: 'string' },
              servings: { type: 'integer' },
            },
            required: ['day', 'meal', 'recipe_id', 'servings'],
          },
        },
      },
      required: ['week_start', 'entries'],
    },
    handler: (a) => putPlan(String(a.week_start), a.entries as PlanEntry[]),
  },
  {
    name: 'add_plan_entry',
    description: 'Dodaje jeden slot do planu (zastępując ewentualny istniejący na tym dniu/posiłku).',
    group: 'Plan tygodnia',
    parameters: {
      type: 'object',
      properties: {
        week_start: { type: 'string' },
        day: { type: 'integer', minimum: 0, maximum: 6 },
        meal: { type: 'string' },
        recipe_id: { type: 'string' },
        servings: { type: 'integer' },
      },
      required: ['week_start', 'day', 'meal', 'recipe_id', 'servings'],
    },
    handler: async (a) => {
      const ws = String(a.week_start);
      const entries = await getPlanEntries(ws);
      const filtered = entries.filter((e) => !(e.day === a.day && e.meal === a.meal));
      filtered.push({
        day: Number(a.day),
        meal: String(a.meal),
        recipe_id: String(a.recipe_id),
        servings: Number(a.servings),
      });
      return putPlan(ws, filtered);
    },
  },
  {
    name: 'remove_plan_entry',
    description: 'Usuwa slot (dzień + posiłek) z planu.',
    group: 'Plan tygodnia',
    parameters: {
      type: 'object',
      properties: {
        week_start: { type: 'string' },
        day: { type: 'integer', minimum: 0, maximum: 6 },
        meal: { type: 'string' },
      },
      required: ['week_start', 'day', 'meal'],
    },
    handler: async (a) => {
      const ws = String(a.week_start);
      const entries = await getPlanEntries(ws);
      return putPlan(
        ws,
        entries.filter((e) => !(e.day === a.day && e.meal === a.meal)),
      );
    },
  },
  {
    name: 'get_week_nutrition_summary',
    description: 'Suma kcal/białka/tłuszczu/węglowodanów dla każdego dnia tygodnia.',
    group: 'Plan tygodnia',
    parameters: {
      type: 'object',
      properties: { week_start: { type: 'string' } },
      required: ['week_start'],
    },
    handler: async (a) => {
      const ws = String(a.week_start);
      const [plan, recipes] = await Promise.all([
        req('GET', `/plan/${ws}`) as Promise<WeekPlan>,
        getRecipes(),
      ]);
      const rmap = new Map(recipes.map((r) => [r.id, r]));
      const out: Record<number, { kcal: number; p: number; f: number; c: number }> = {};
      for (let d = 0; d < 7; d++) out[d] = { kcal: 0, p: 0, f: 0, c: 0 };
      for (const e of plan.entries || []) {
        const r = rmap.get(e.recipe_id);
        if (!r || !r.servings) continue;
        const scale = e.servings / r.servings;
        out[e.day].kcal += (r.kcal || 0) * scale;
        out[e.day].p += (r.p || 0) * scale;
        out[e.day].f += (r.f || 0) * scale;
        out[e.day].c += (r.c || 0) * scale;
      }
      const round = (v: number) => Math.round(v * 100) / 100;
      return Object.fromEntries(
        Object.entries(out).map(([k, v]) => [
          k,
          { kcal: round(v.kcal), p: round(v.p), f: round(v.f), c: round(v.c) },
        ]),
      );
    },
  },
  {
    name: 'get_shopping_list',
    description: 'Aktualna lista zakupów na dany tydzień.',
    group: 'Lista zakupów',
    parameters: {
      type: 'object',
      properties: { week_start: { type: 'string' } },
      required: ['week_start'],
    },
    handler: (a) => req('GET', `/shopping/${String(a.week_start)}`),
  },
  {
    name: 'generate_shopping_list',
    description:
      'Regeneruje listę zakupów z planu tygodnia. Pozycje własne (dodane ręcznie) i odhaczenia są zachowane.',
    group: 'Lista zakupów',
    parameters: {
      type: 'object',
      properties: { week_start: { type: 'string' } },
      required: ['week_start'],
    },
    handler: async (a) => {
      const ws = String(a.week_start);
      const result = await req('POST', `/shopping/${ws}/generate`);
      await loadShopping(ws);
      emitShoppingChanged();
      return result;
    },
  },
  {
    name: 'check_shopping_item',
    description: 'Oznacza pozycję jako kupioną lub odznacza.',
    group: 'Lista zakupów',
    parameters: {
      type: 'object',
      properties: {
        week_start: { type: 'string' },
        item_id: { type: 'integer' },
        checked: { type: 'boolean' },
      },
      required: ['week_start', 'item_id', 'checked'],
    },
    handler: async (a) => {
      const ws = String(a.week_start);
      const result = await req('PATCH', `/shopping/${ws}/items/${Number(a.item_id)}`, {
        checked: Boolean(a.checked),
      });
      await loadShopping(ws);
      emitShoppingChanged();
      return result;
    },
  },
  {
    name: 'add_shopping_item',
    description:
      'Dodaje własną pozycję do listy zakupów (np. papier toaletowy, mleko poza planem). Jeśli pozycja o tej nazwie i jednostce już istnieje, ilość zostanie zsumowana.',
    group: 'Lista zakupów',
    parameters: {
      type: 'object',
      properties: {
        week_start: { type: 'string' },
        name: { type: 'string' },
        qty: { type: 'number' },
        unit: { type: 'string' },
        category: { type: 'string' },
      },
      required: ['week_start', 'name'],
    },
    handler: async (a) => {
      const ws = String(a.week_start);
      const body: Record<string, unknown> = { name: String(a.name) };
      if (a.qty !== undefined) body.qty = Number(a.qty);
      if (a.unit !== undefined) body.unit = String(a.unit);
      if (a.category !== undefined) body.category = String(a.category);
      const result = await req('POST', `/shopping/${ws}/items`, body);
      await loadShopping(ws);
      emitShoppingChanged();
      return result;
    },
  },
  {
    name: 'remove_shopping_item',
    description: 'Usuwa pojedynczą pozycję z listy zakupów (po item_id z get_shopping_list).',
    group: 'Lista zakupów',
    parameters: {
      type: 'object',
      properties: {
        week_start: { type: 'string' },
        item_id: { type: 'integer' },
      },
      required: ['week_start', 'item_id'],
    },
    handler: async (a) => {
      const ws = String(a.week_start);
      const result = await req('DELETE', `/shopping/${ws}/items/${Number(a.item_id)}`);
      await loadShopping(ws);
      emitShoppingChanged();
      return result;
    },
  },
  {
    name: 'clear_shopping_list',
    description: 'Usuwa wszystkie pozycje listy zakupów danego tygodnia (także własne).',
    group: 'Lista zakupów',
    parameters: {
      type: 'object',
      properties: { week_start: { type: 'string' } },
      required: ['week_start'],
    },
    handler: async (a) => {
      const ws = String(a.week_start);
      const result = await req('DELETE', `/shopping/${ws}`);
      await loadShopping(ws);
      emitShoppingChanged();
      return result;
    },
  },
];

export const TOOLS_BY_NAME: Map<string, ToolDef> = new Map(TOOLS.map((t) => [t.name, t]));
