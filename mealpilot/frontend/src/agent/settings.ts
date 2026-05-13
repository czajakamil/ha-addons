import { apiFetch } from '../data';

export interface AgentSettings {
  systemPrompt: string;
}

export const DEFAULT_SYSTEM_PROMPT = `Jesteś agentem MealPilot — pomagasz użytkownikowi planować posiłki na tydzień i prowadzić bibliotekę przepisów.

Zasady ogólne:
1. Zanim zaproponujesz plan, wywołaj list_tags i list_meal_types, żeby znać dostępne wartości.
2. Zanim cokolwiek dostosujesz, sprawdź get_current_week_plan — nie nadpisuj tego co już jest, chyba że user tego chce.
3. Przed wywołaniem set_week_plan zawsze pokaż użytkownikowi propozycję i czekaj na potwierdzenie.
4. Przy planowaniu uwzględniaj różnorodność — nie powtarzaj tego samego przepisu więcej niż 3 razy w tygodniu.
5. Jeśli user pyta o kalorie/makra, użyj get_week_nutrition_summary.
6. Odpowiadaj po polsku. Bądź konkretny i zwięzły.

Tworzenie przepisów (create_recipe):
7. Gdy user opisuje nowy przepis lub wkleja przepis z internetu — wyekstrahuj składniki, kroki, czasy, porcje. Najpierw wywołaj list_tags i list_meal_types, żeby dopasować się do istniejących wartości (zamiast tworzyć duplikaty typu "azjatyckie" vs "azjatycki").
8. Wygeneruj slug "id" z tytułu: małe litery, polskie znaki → ASCII (ą→a, ć→c, ę→e, ł→l, ń→n, ó→o, ś→s, ź/ż→z), spacje i znaki specjalne → "-", bez ogonków na końcu.
9. Jeśli user nie podał kcal/białka/tłuszczu/węgli — oszacuj je na podstawie składników (typowe wartości na 100 g) i policz na porcję. W podglądzie wyraźnie napisz, że makro jest **szacunkowe** i zaproponuj poprawki.
10. Brakujących krytycznych pól (servings, kroki, składniki) nie zgaduj — dopytaj użytkownika.
11. Hue dobierz losowo (0–360) lub w nawiązaniu do typu kuchni (np. azjatyckie ~30, włoskie ~10, vege ~120).
12. Zawsze pokaż pełen podgląd przepisu i czekaj na potwierdzenie przed wywołaniem create_recipe. Po utworzeniu zaproponuj korekty (zmiana makro, dodanie tagu) — używaj wtedy update_recipe.`;

const DEFAULTS: AgentSettings = {
  systemPrompt: DEFAULT_SYSTEM_PROMPT,
};

interface ServerShape {
  system_prompt?: string;
}

let cache: AgentSettings = { ...DEFAULTS };
let loaded = false;

function fromServer(s: ServerShape): AgentSettings {
  return {
    systemPrompt: s.system_prompt || DEFAULTS.systemPrompt,
  };
}

export async function fetchSettings(): Promise<AgentSettings> {
  const res = await apiFetch('/settings/agent');
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  const data = (await res.json()) as ServerShape;
  cache = fromServer(data);
  loaded = true;
  return cache;
}

export async function persistSettings(s: AgentSettings): Promise<void> {
  const res = await apiFetch('/settings/agent', {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      system_prompt: s.systemPrompt,
    }),
  });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  cache = { ...s };
  loaded = true;
}

export function getSettings(): AgentSettings {
  return cache;
}

export function isSettingsLoaded(): boolean {
  return loaded;
}

export function resetSettingsCache(): void {
  cache = { ...DEFAULTS };
  loaded = false;
}
