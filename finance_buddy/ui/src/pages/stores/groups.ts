// Grupowanie sklepów po nazwie grupowej, ze statystykami z serwera. Bez stanu i bez JSX.
import type { Store, StoreStatsRow } from '../../api';
import { toIsoDate } from '../../utils/date';

const MONTHS_IN_WINDOW = 12;

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

const EMPTY_STAT: StoreStat = { count: 0, spentCents: 0, lastDate: null };

export const getGroupLabel = (store: Store) => store.group_name?.trim() || store.name.trim();

const groupKey = (store: Store) => getGroupLabel(store).toLocaleLowerCase('pl-PL').replace(/\s+/g, ' ');

/** Pierwszy dzień okna: bieżący miesiąc i 11 poprzednich, tak samo jak na serwerze. */
export const spendWindowStart = (today: Date) =>
  toIsoDate(new Date(today.getFullYear(), today.getMonth() - (MONTHS_IN_WINDOW - 1), 1));

export const toStatsMap = (rows: readonly StoreStatsRow[]) =>
  new Map<string, StoreStat>(
    rows.map((row) => [row.store_id, { count: row.count, spentCents: row.spent_cents_12m, lastDate: row.last_date ?? null }])
  );

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
