// Ekran „Nowa transakcja" na telefonie: podpowiedzi z ostatnich transakcji i lista kategorii.
// Bez stanu i bez JSX.
import type { Category, Store, Transaction } from '../../api';
import { buildCategoryTree, type CategoryNode } from '../../pages/categories/model';

export const storeDisplayName = (store: Pick<Store, 'name' | 'group_name'>) =>
  (store.group_name ?? '').trim() || (store.name ?? '').trim() || '—';

/** Sklepy z ostatnich transakcji, od najświeższego, bez powtórzeń. Przelewy nie mają sklepu. */
export const recentStoreIds = (transactions: readonly Transaction[], limit = 6) => {
  const ids: string[] = [];
  for (const tx of transactions) {
    if (ids.length >= limit) break;
    if (tx.transfer_account_id || !tx.store_id || ids.includes(tx.store_id)) continue;
    ids.push(tx.store_id);
  }
  return ids;
};

/** Ostatnio używane kategorie, od najświeższej, bez powtórzeń. */
export const recentCategoryIds = (transactions: readonly Transaction[], limit = 3) => {
  const ids: string[] = [];
  for (const tx of transactions) {
    if (ids.length >= limit) break;
    if (!tx.category_id || ids.includes(tx.category_id)) continue;
    ids.push(tx.category_id);
  }
  return ids;
};

/**
 * Kategoria „jak zwykle" dla sklepu: automatyczna kategoria sklepu, a bez niej ta z jego
 * ostatniej transakcji. Usunięta kategoria nie jest podpowiedzią.
 */
export const suggestedCategoryId = (
  store: Pick<Store, 'id' | 'automatic_category_id'> | undefined,
  transactions: readonly Transaction[],
  categoryIds: ReadonlySet<string>
) => {
  if (!store) return null;
  if (store.automatic_category_id && categoryIds.has(store.automatic_category_id)) return store.automatic_category_id;
  const last = transactions.find((tx) => tx.store_id === store.id && tx.category_id && categoryIds.has(tx.category_id));
  return last?.category_id ?? null;
};

/** „Jedzenie › Przekąski › " — nazwy przodków do wyszarzonego przedrostka; pusty dla korzenia. */
export const categoryParentPrefix = (
  category: Pick<Category, 'parent_id'>,
  byId: ReadonlyMap<string, Pick<Category, 'name' | 'parent_id'>>
) => {
  const names: string[] = [];
  const seen = new Set<string>();
  let parentId = category.parent_id;
  while (parentId && !seen.has(parentId)) {
    seen.add(parentId);
    const parent = byId.get(parentId);
    if (!parent) break;
    names.unshift(parent.name);
    parentId = parent.parent_id;
  }
  return names.length ? `${names.join(' › ')} › ` : '';
};

export type CategoryPickerRow = {
  id: string;
  label: string;
  /** Przodkowie przed nazwą — tylko w wynikach wyszukiwania, gdzie wcięcia nie ma. */
  prefix: string;
  depth: number;
  /** Rodzic z podkategoriami to nagłówek grupy, nie kategoria do wyboru. */
  selectable: boolean;
};

export const categoryPickerRows = (categories: readonly Category[], search: string): CategoryPickerRow[] => {
  const tree = buildCategoryTree(categories, null);
  const byId = new Map(categories.map((category) => [category.id, category]));
  const query = search.trim().toLocaleLowerCase('pl');
  const rows: CategoryPickerRow[] = [];
  const visit = (node: CategoryNode) => {
    const isGroup = node.children.length > 0;
    if (!query) {
      rows.push({ id: node.category.id, label: node.category.name, prefix: '', depth: node.depth, selectable: !isGroup });
    } else if (!isGroup && node.category.name.toLocaleLowerCase('pl').includes(query)) {
      rows.push({
        id: node.category.id,
        label: node.category.name,
        prefix: categoryParentPrefix(node.category, byId),
        depth: 0,
        selectable: true
      });
    }
    node.children.forEach(visit);
  };
  tree.forEach(visit);
  return rows;
};
