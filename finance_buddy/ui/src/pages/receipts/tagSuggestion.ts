/**
 * Skąd wziąć tag transakcji, gdy użytkownik go nie wpisał.
 *
 * Dwa źródła, w tej kolejności:
 *  1. sklep — `automatic_category_id`, czyli tag zapamiętany przy poprzednim
 *     paragonie z tej sieci,
 *  2. pozycje — tag produktów, które już są w bazie.
 *
 * Przy pozycjach nie wystarczy „weź pierwszy z brzegu": paragon spożywczy ma
 * pozycje z kilku gałęzi. Dlatego albo jedna gałąź bierze większość wartości
 * (Biedronka: jedzenie + jedna żarówka to nadal paragon spożywczy), albo
 * cofamy się do wspólnego przodka — na to pozwala dopiero drzewo tagów,
 * płaskie kategorie nie mają czego uogólniać.
 */

import type { Category, Item, Store } from '../../api';
import { NEW_PRODUCT, NO_PRODUCT, inputToCents, type ItemRow } from './model';

export type TagSuggestionSource = 'store' | 'items';

export interface TagSuggestion {
  path: string;
  source: TagSuggestionSource;
  /** Ile pozycji miało tag (tylko dla źródła „items") — do napisu pod polem. */
  tagged?: number;
  /** Ile pozycji w ogóle brało udział w liczeniu. */
  total?: number;
}

/** Udział wartości, przy którym jedna gałąź wygrywa zamiast uogólniania do przodka. */
export const DOMINANT_SHARE = 0.8;

const segmentsOf = (path: string) => path.split('/').filter(Boolean);

/** Najgłębszy wspólny przodek: „zakupy/nabiał" + „zakupy/pieczywo" → „zakupy". */
export const commonTagPath = (paths: string[]): string => {
  if (!paths.length) return '';
  let common = segmentsOf(paths[0]);
  for (const path of paths.slice(1)) {
    const other = segmentsOf(path);
    let shared = 0;
    while (shared < common.length && shared < other.length && common[shared] === other[shared]) {
      shared += 1;
    }
    common = common.slice(0, shared);
    if (!common.length) break;
  }
  return common.join('/');
};

/**
 * Jeden tag dla zbioru otagowanych pozycji, ważony ich wartością. Pozycje bez
 * tagu są pomijane, a nie liczone jako głos przeciw — świeżo zakładany produkt
 * nie ma jeszcze skąd wiedzieć, gdzie należy.
 */
export const dominantTagPath = (entries: { path: string; weight: number }[]): string => {
  const weightByPath = new Map<string, number>();
  let total = 0;
  for (const entry of entries) {
    const path = entry.path.trim();
    if (!path) continue;
    const weight = Math.abs(entry.weight) || 0;
    weightByPath.set(path, (weightByPath.get(path) ?? 0) + weight);
    total += weight;
  }
  if (!weightByPath.size) return '';
  if (total > 0) {
    for (const [path, weight] of weightByPath) {
      if (weight / total >= DOMINANT_SHARE) return path;
    }
  }
  return commonTagPath([...weightByPath.keys()]);
};

/**
 * Tag wiersza: dla istniejącego produktu z bazy (bo tam mieszka prawda), dla
 * nowego — z pola w tabeli pozycji.
 */
export const rowTagPath = (row: ItemRow, pathByProductId: Map<string, string>): string =>
  row.productId === NEW_PRODUCT
    ? row.categoryPath.trim()
    : pathByProductId.get(row.productId) ?? row.categoryPath.trim();

export interface TagSuggestionInput {
  store: Store | null;
  rows: ItemRow[];
  products: Item[];
  categories: Category[];
}

export const suggestTransactionTag = ({
  store,
  rows,
  products,
  categories
}: TagSuggestionInput): TagSuggestion | null => {
  const pathByCategoryId = new Map(categories.map((category) => [category.id, category.path]));

  if (store?.automatically_categorize_payee && store.automatic_category_id) {
    const path = pathByCategoryId.get(store.automatic_category_id);
    if (path) return { path, source: 'store' };
  }

  const pathByProductId = new Map<string, string>();
  for (const product of products) {
    const path = product.category_id ? pathByCategoryId.get(product.category_id) : undefined;
    if (path) pathByProductId.set(product.id, path);
  }

  // Wiersze pominięte (rabaty, śmieci z OCR) nie są towarem i nie mają głosu.
  const counted = rows.filter((row) => row.rawName.trim() && row.productId !== NO_PRODUCT);
  const entries = counted.map((row) => ({
    path: rowTagPath(row, pathByProductId),
    weight: inputToCents(row.lineTotal) ?? 0
  }));

  const path = dominantTagPath(entries);
  if (!path) return null;
  return {
    path,
    source: 'items',
    tagged: entries.filter((entry) => entry.path.trim()).length,
    total: counted.length
  };
};
