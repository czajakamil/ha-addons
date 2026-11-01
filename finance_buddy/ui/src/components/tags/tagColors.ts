import type { Category } from '../../api';

/**
 * Paleta kolorów tagów. Przechowujemy *klucz* (np. "teal"), nie surowy hex —
 * dzięki temu kolory podmieniają się z motywem jasny/ciemny i są spójne z
 * awatarami kont (ta sama rodzina zmiennych `--cat-*`).
 */
export type TagColorOption = { id: string; label: string; value: string };

export const TAG_COLOR_OPTIONS: TagColorOption[] = [
  { id: 'indigo', label: 'Indygo', value: 'var(--text-muted)' },
  { id: 'teal', label: 'Morski', value: 'var(--text-muted)' },
  { id: 'green', label: 'Zielony', value: 'var(--positive)' },
  { id: 'coral', label: 'Koralowy', value: 'var(--negative)' },
  { id: 'amber', label: 'Bursztynowy', value: 'var(--warning)' },
  { id: 'pink', label: 'Różowy', value: 'var(--text-muted)' },
  { id: 'blue', label: 'Niebieski', value: 'var(--accent)' },
  { id: 'violet', label: 'Fioletowy', value: 'var(--text-muted)' }
];

const TAG_COLOR_MAP = new Map(TAG_COLOR_OPTIONS.map((option) => [option.id, option]));

/** Neutralny kolor dla tagów bez własnego i bez pokolorowanego przodka. */
export const NEUTRAL_TAG_COLOR = 'var(--text-muted)';

const HEX_COLOR_RE = /^#[0-9a-fA-F]{6}$/;

/** Kolor spoza gotowej palety, wybrany suwakami HSL i zapisany jako hex (np. "#3b82f6"). */
export const isCustomTagColor = (id?: string | null): id is string => !!id && HEX_COLOR_RE.test(id);

/** Zamienia klucz palety (lub własny hex) na wartość CSS; nieznany klucz → neutralny. */
export const getTagColorValue = (id?: string | null): string => {
  if (!id) return NEUTRAL_TAG_COLOR;
  const preset = TAG_COLOR_MAP.get(id)?.value;
  if (preset) return preset;
  return isCustomTagColor(id) ? id : NEUTRAL_TAG_COLOR;
};

export type Hsl = { h: number; s: number; l: number };

/** Domyślny punkt startowy suwaków, gdy tag nie ma jeszcze własnego koloru niestandardowego. */
export const DEFAULT_CUSTOM_HSL: Hsl = { h: 220, s: 75, l: 58 };

/** HSL (0-360, 0-100, 0-100) → hex "#rrggbb". */
export const hslToHex = ({ h, s, l }: Hsl): string => {
  const sNorm = s / 100;
  const lNorm = l / 100;
  const k = (n: number) => (n + h / 30) % 12;
  const a = sNorm * Math.min(lNorm, 1 - lNorm);
  const f = (n: number) => lNorm - a * Math.max(-1, Math.min(k(n) - 3, Math.min(9 - k(n), 1)));
  const toHex = (n: number) => Math.round(f(n) * 255).toString(16).padStart(2, '0');
  return `#${toHex(0)}${toHex(8)}${toHex(4)}`;
};

/** hex "#rrggbb" → HSL; nieprawidłowy wpis spada na domyślny punkt startowy. */
export const hexToHsl = (hex: string): Hsl => {
  const value = HEX_COLOR_RE.test(hex) ? hex : hslToHex(DEFAULT_CUSTOM_HSL);
  const r = parseInt(value.slice(1, 3), 16) / 255;
  const g = parseInt(value.slice(3, 5), 16) / 255;
  const b = parseInt(value.slice(5, 7), 16) / 255;
  const max = Math.max(r, g, b);
  const min = Math.min(r, g, b);
  const l = (max + min) / 2;
  let h = 0;
  let s = 0;
  if (max !== min) {
    const d = max - min;
    s = l > 0.5 ? d / (2 - max - min) : d / (max + min);
    if (max === r) h = (g - b) / d + (g < b ? 6 : 0);
    else if (max === g) h = (b - r) / d + 2;
    else h = (r - g) / d + 4;
    h *= 60;
  }
  return { h: Math.round(h), s: Math.round(s * 100), l: Math.round(l * 100) };
};

export type ResolvedTagColor = {
  /** Klucz efektywnego koloru (własny lub odziedziczony); null gdy nikt nie ma koloru. */
  colorId: string | null;
  /** Wartość CSS gotowa do wstrzyknięcia (zawsze niepusta — neutralny w ostateczności). */
  value: string;
  /** Czy kolor pochodzi od przodka (tag nie ma własnego). */
  inherited: boolean;
};

/**
 * Wylicza efektywny kolor tagu: własny, a przy jego braku — najbliższego
 * przodka z ustawionym kolorem. `byId` to mapa wszystkich kategorii (do wspinania
 * się po `parent_id`). Odporne na cykle w uszkodzonych danych.
 */
export const resolveTagColor = (
  category: Pick<Category, 'color' | 'parent_id'> | undefined | null,
  byId: Map<string, Category>
): ResolvedTagColor => {
  if (!category) {
    return { colorId: null, value: NEUTRAL_TAG_COLOR, inherited: false };
  }
  if (category.color) {
    return { colorId: category.color, value: getTagColorValue(category.color), inherited: false };
  }

  const visited = new Set<string>();
  let current = category.parent_id ? byId.get(category.parent_id) : undefined;
  while (current) {
    if (visited.has(current.id)) break; // obrona przed cyklem
    visited.add(current.id);
    if (current.color) {
      return { colorId: current.color, value: getTagColorValue(current.color), inherited: true };
    }
    current = current.parent_id ? byId.get(current.parent_id) : undefined;
  }

  return { colorId: null, value: NEUTRAL_TAG_COLOR, inherited: false };
};

/** Buduje mapę id → kategoria (pomocnik dla resolveTagColor). */
export const buildCategoryMap = (categories: Category[]): Map<string, Category> =>
  new Map(categories.map((category) => [category.id, category]));
