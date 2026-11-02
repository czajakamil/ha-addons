// Skład majątku na koniec okresu: salda kont zgrupowane według typu albo tagu.
// Czysta logika, żeby suma listy dało się sprawdzić względem „Netto” bez renderowania.
import type { Category, NetWorthAccountBalance } from '../../api';
import {
  ACCOUNT_UNCATEGORIZED_ID,
  ACCOUNT_UNCATEGORIZED_NAME,
  getAccountKindLabel,
  type BreakdownChartEntry
} from './model';

type Bucket = { name: string; total: number; categoryId?: string };

const toEntries = (buckets: Map<string, Bucket>): BreakdownChartEntry[] => {
  const entries = Array.from(buckets, ([key, bucket]) => ({
    key,
    name: bucket.name,
    categoryId: bucket.categoryId,
    total: bucket.total,
    value: Math.abs(bucket.total),
    percentage: 0,
    color: ''
  }))
    .filter((entry) => entry.value > 0)
    .sort((a, b) => b.value - a.value);
  const percentBase = entries.reduce((sum, entry) => sum + entry.value, 0);
  return entries.map((entry) => ({ ...entry, percentage: entry.value / percentBase }));
};

const addToBucket = (
  buckets: Map<string, Bucket>,
  key: string,
  name: string,
  amount: number,
  categoryId?: string
) => {
  const bucket = buckets.get(key) ?? { name, total: 0, categoryId };
  bucket.total += amount;
  buckets.set(key, bucket);
};

export const buildKindEntries = (balances: readonly NetWorthAccountBalance[]) => {
  const buckets = new Map<string, Bucket>();
  balances.forEach((balance) =>
    addToBucket(
      buckets,
      balance.account_kind,
      getAccountKindLabel(balance.account_kind),
      balance.balance_cents
    )
  );
  return toEntries(buckets);
};

// Tag konta i jego przodkowie, od liścia do korzenia
const tagChain = (categoryId: string | null | undefined, byId: Map<string, Category>) => {
  const chain: Category[] = [];
  let current = categoryId ? byId.get(categoryId) : undefined;
  while (current) {
    chain.push(current);
    current = current.parent_id ? byId.get(current.parent_id) : undefined;
  }
  return chain;
};

export const buildTagEntries = (
  balances: readonly NetWorthAccountBalance[],
  categories: readonly Category[],
  activeParentId: string | null
) => {
  const byId = new Map(categories.map((category) => [category.id, category]));
  const buckets = new Map<string, Bucket>();

  balances.forEach((balance) => {
    const chain = tagChain(balance.category_id, byId);
    const amount = balance.balance_cents;

    if (!activeParentId) {
      const root = chain[chain.length - 1];
      if (root) {
        addToBucket(buckets, root.id, root.name, amount, root.id);
      } else {
        addToBucket(buckets, ACCOUNT_UNCATEGORIZED_ID, ACCOUNT_UNCATEGORIZED_NAME, amount);
      }
      return;
    }

    const activeIndex = chain.findIndex((category) => category.id === activeParentId);
    if (activeIndex === 0) {
      addToBucket(buckets, `${activeParentId}::self`, 'Bez podtagu', amount);
    } else if (activeIndex > 0) {
      const child = chain[activeIndex - 1];
      addToBucket(buckets, child.id, child.name, amount, child.id);
    }
  });

  return toEntries(buckets);
};

// Tagi, w które można wejść: mają podtag z niezerową sumą
export const findDrillableTagIds = (
  balances: readonly NetWorthAccountBalance[],
  categories: readonly Category[]
) => {
  const byId = new Map(categories.map((category) => [category.id, category]));
  const totals = new Map<string, number>();
  balances.forEach((balance) =>
    tagChain(balance.category_id, byId).forEach((category) =>
      totals.set(category.id, (totals.get(category.id) ?? 0) + balance.balance_cents)
    )
  );

  const ids = new Set<string>();
  categories.forEach((category) => {
    if (category.parent_id && totals.get(category.id)) ids.add(category.parent_id);
  });
  return ids;
};

export const tagPath = (categoryId: string, categories: readonly Category[]) => {
  const byId = new Map(categories.map((category) => [category.id, category]));
  return tagChain(categoryId, byId)
    .reverse()
    .map((category) => category.name)
    .join(' / ');
};
