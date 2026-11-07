// Szczegóły transakcji: podział pozycji na kategorie. Bez stanu i bez JSX.
import type { Category, Item, TransactionItem } from '../../api';

export const UNCATEGORIZED_KEY = '__none__';

// Kolory podziału idą po kolei z palety wykresów, żeby pasek dało się odczytać;
// ta sama kropka stoi przy pozycji w tabeli.
const SPLIT_COLORS = [1, 2, 3, 4, 5, 6, 7, 8].map((index) => `var(--chart-${index})`);
export const UNCATEGORIZED_COLOR = 'var(--text-faint)';

export type CategorySplitEntry = {
  key: string;
  categoryId: string | null;
  path: string;
  cents: number;
  share: number;
  color: string;
};

export const itemTotalCents = (item: Pick<TransactionItem, 'unit_price_cents' | 'quantity'>) =>
  Math.round(item.unit_price_cents * item.quantity);

/** Kategoria pozycji to kategoria produktu — paragon nie ma własnej. */
export const itemCategoryId = (item: Pick<TransactionItem, 'product_id'>, productsById: ReadonlyMap<string, Item>) =>
  productsById.get(item.product_id)?.category_id ?? null;

/** Sumy kategorii od największej; udział liczony od sumy pozycji. */
export const buildCategorySplit = (
  items: readonly TransactionItem[],
  productsById: ReadonlyMap<string, Item>,
  categoriesById: ReadonlyMap<string, Category>
): CategorySplitEntry[] => {
  const sums = new Map<string, number>();
  items.forEach((item) => {
    const categoryId = itemCategoryId(item, productsById);
    const key = categoryId && categoriesById.has(categoryId) ? categoryId : UNCATEGORIZED_KEY;
    sums.set(key, (sums.get(key) ?? 0) + itemTotalCents(item));
  });
  const total = [...sums.values()].reduce((sum, cents) => sum + Math.abs(cents), 0);
  let colorIndex = 0;
  return [...sums.entries()]
    .sort((a, b) => Math.abs(b[1]) - Math.abs(a[1]))
    .map(([key, cents]) => {
      const uncategorized = key === UNCATEGORIZED_KEY;
      const color = uncategorized ? UNCATEGORIZED_COLOR : SPLIT_COLORS[colorIndex++ % SPLIT_COLORS.length];
      return {
        key,
        categoryId: uncategorized ? null : key,
        path: uncategorized ? '' : categoriesById.get(key)?.path ?? '',
        cents,
        share: total ? Math.abs(cents) / total : 0,
        color
      };
    });
};

/** Czy suma pozycji pokrywa kwotę transakcji (znak pomijamy: wydatek jest ujemny). */
export const itemsMatchTotal = (items: readonly TransactionItem[], totalCents: number) =>
  Math.abs(items.reduce((sum, item) => sum + itemTotalCents(item), 0)) === Math.abs(totalCents);
