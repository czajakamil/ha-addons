// Grupowanie sklepów po nazwie grupowej i statystyki liczone z listy transakcji. Bez stanu i bez JSX.
import type { Store, Transaction } from '../../api';
import { toIsoDate } from '../../utils/date';

const MONTHS_IN_WINDOW = 12;
const MAIN_CURRENCY = 'PLN';
const INTERNAL_STORE_RE = /reconciliation balance adjustment|^transfer ?:|przenies.*konto/i;

export type StoreStat = { count: number; spentCents: number; lastDate: string | null };

export type StoreGroup = {
  /** Sklep z największą liczbą transakcji. Jego id otwiera stronę, a NIP i adres zapisują się właśnie w nim. */
  id: string;
  key: string;
  name: string;
  members: Store[];
  categoryId: string | null;
  isArchived: boolean;
  count: number;
  spentCents: number;
  lastDate: string | null;
};

export type MonthlySpend = { month: string; cents: number };

const EMPTY_STAT: StoreStat = { count: 0, spentCents: 0, lastDate: null };

export const getGroupLabel = (store: Store) => store.group_name?.trim() || store.name.trim();

const groupKey = (store: Store) => getGroupLabel(store).toLocaleLowerCase('pl-PL').replace(/\s+/g, ' ');

/** Pierwszy dzień okna: bieżący miesiąc i 11 poprzednich, tak samo jak na wykresie. */
export const spendWindowStart = (today: Date) =>
  toIsoDate(new Date(today.getFullYear(), today.getMonth() - (MONTHS_IN_WINDOW - 1), 1));

const spendWindowEnd = (today: Date) => toIsoDate(new Date(today.getFullYear(), today.getMonth() + 1, 0));

const txDate = (tx: Transaction) => tx.date ?? tx.recorded_at?.slice(0, 10) ?? null;

// Te same wykluczenia co w raportach: przelewy i korekty salda nie są wydatkami. Zwrot pomniejsza sumę.
const spendCents = (tx: Transaction, store: Store | undefined) => {
  if (tx.transfer_account_id || tx.currency !== MAIN_CURRENCY) return 0;
  if (store && INTERNAL_STORE_RE.test(getGroupLabel(store))) return 0;
  return -tx.total_cents;
};

export const aggregateStoreStats = (
  transactions: readonly Transaction[],
  stores: readonly Store[],
  today = new Date()
) => {
  const storeById = new Map(stores.map((store) => [store.id, store]));
  const start = spendWindowStart(today);
  const end = spendWindowEnd(today);
  const stats = new Map<string, StoreStat>();
  transactions.forEach((tx) => {
    if (!tx.store_id) return;
    const stat = stats.get(tx.store_id) ?? { ...EMPTY_STAT };
    const date = txDate(tx);
    stat.count += 1;
    if (date && (!stat.lastDate || date > stat.lastDate)) stat.lastDate = date;
    if (date && date >= start && date <= end) stat.spentCents += spendCents(tx, storeById.get(tx.store_id));
    stats.set(tx.store_id, stat);
  });
  return stats;
};

export const buildStoreGroups = (stores: readonly Store[], stats: ReadonlyMap<string, StoreStat>): StoreGroup[] => {
  const buckets = new Map<string, Store[]>();
  stores.forEach((store) => {
    // Zarchiwizowany sklep nie dokleja się do aktywnej grupy: importer już go nie dopasowuje
    const key = `${store.is_deleted ? 'archived' : 'active'}:${groupKey(store)}`;
    buckets.set(key, [...(buckets.get(key) ?? []), store]);
  });
  return Array.from(buckets, ([key, bucket]) => {
    const statOf = (store: Store) => stats.get(store.id) ?? EMPTY_STAT;
    const members = bucket
      .slice()
      .sort((a, b) => statOf(b).count - statOf(a).count || a.name.localeCompare(b.name, 'pl'));
    const lastDates = members.map((store) => statOf(store).lastDate).filter((date): date is string => Boolean(date));
    return {
      id: members[0].id,
      key,
      name: getGroupLabel(members[0]),
      members,
      categoryId: members.find((store) => store.automatic_category_id)?.automatic_category_id ?? null,
      isArchived: members[0].is_deleted,
      count: members.reduce((sum, store) => sum + statOf(store).count, 0),
      spentCents: members.reduce((sum, store) => sum + statOf(store).spentCents, 0),
      lastDate: lastDates.length ? lastDates.reduce((latest, date) => (date > latest ? date : latest)) : null
    };
  });
};

/** Grupa bez transakcji z ostatnich 12 miesięcy. Daty przyszłe liczą się jako aktywność. */
export const isInactiveGroup = (group: StoreGroup, today = new Date()) =>
  !group.lastDate || group.lastDate < spendWindowStart(today);

export type GroupSort = 'spent' | 'name' | 'count' | 'last';

export const sortStoreGroups = (groups: readonly StoreGroup[], sort: GroupSort) => {
  const byName = (a: StoreGroup, b: StoreGroup) => a.name.localeCompare(b.name, 'pl');
  const compare: Record<GroupSort, (a: StoreGroup, b: StoreGroup) => number> = {
    spent: (a, b) => b.spentCents - a.spentCents || byName(a, b),
    name: byName,
    count: (a, b) => b.count - a.count || byName(a, b),
    last: (a, b) => (b.lastDate ?? '').localeCompare(a.lastDate ?? '') || byName(a, b)
  };
  return groups.slice().sort(compare[sort]);
};

/** Wydatki w 12 miesiącach, od najstarszego; ostatni to bieżący. Miesiąc z nadwyżką zwrotów to 0. */
export const buildMonthlySpend = (
  transactions: readonly Transaction[],
  stores: readonly Store[],
  today = new Date()
): MonthlySpend[] => {
  const storeById = new Map(stores.map((store) => [store.id, store]));
  const months = Array.from({ length: MONTHS_IN_WINDOW }, (_, index) =>
    toIsoDate(new Date(today.getFullYear(), today.getMonth() - (MONTHS_IN_WINDOW - 1 - index), 1)).slice(0, 7)
  );
  const totals = new Map(months.map((month) => [month, 0]));
  transactions.forEach((tx) => {
    const month = txDate(tx)?.slice(0, 7);
    if (!month || !totals.has(month)) return;
    totals.set(month, totals.get(month)! + spendCents(tx, tx.store_id ? storeById.get(tx.store_id) : undefined));
  });
  return months.map((month) => ({ month, cents: Math.max(0, totals.get(month)!) }));
};
