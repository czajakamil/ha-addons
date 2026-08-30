import { apiFetch } from '../data';

export interface AgentSettings {
  model: string;
  /**
   * Nadpisanie promptu systemowego przez użytkownika.
   * Pusty string = "użyj domyślnego" — backend podstawia wtedy swój
   * DEFAULT_SYSTEM_PROMPT. Frontend NIE trzyma własnej kopii promptu,
   * żeby nie przypiąć do bazy przestarzałej wersji.
   */
  systemPrompt: string;
  /** Domyślny prompt z backendu (tylko do podglądu / przywracania). */
  defaultSystemPrompt: string;
}

const DEFAULT_MODEL = 'claude-haiku-4-5-20251001';

const DEFAULTS: AgentSettings = {
  model: DEFAULT_MODEL,
  systemPrompt: '',
  defaultSystemPrompt: '',
};

interface ServerShape {
  model?: string;
  system_prompt?: string;
  default_system_prompt?: string;
}

let cache: AgentSettings = { ...DEFAULTS };
let loaded = false;

function fromServer(s: ServerShape): AgentSettings {
  return {
    model: s.model || DEFAULT_MODEL,
    systemPrompt: s.system_prompt ?? '',
    defaultSystemPrompt: s.default_system_prompt ?? '',
  };
}

/** Prompt pokazywany w UI: nadpisanie użytkownika albo domyślny z serwera. */
export function effectiveSystemPrompt(s: AgentSettings): string {
  return s.systemPrompt || s.defaultSystemPrompt;
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
      model: s.model,
      // Nie odsyłamy domyślnego promptu — pusty string znaczy
      // "użyj domyślnego z backendu".
      system_prompt: s.systemPrompt.trim() === s.defaultSystemPrompt.trim() ? '' : s.systemPrompt,
    }),
  });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  cache = fromServer((await res.json()) as ServerShape);
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

export function isConfigured(s: AgentSettings): boolean {
  return Boolean(s.model);
}
