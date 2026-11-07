import { describe, expect, it } from 'vitest';
import type { Store } from '../../api';
import {
  buildStoreGroups,
  isInactiveGroup,
  sortStoreGroups,
  spendWindowStart,
  toStatsMap
} from './groups';

const TODAY = new Date(2026, 10, 15);

const store = (id: string, name: string, extra: Partial<Store> = {}): Store => ({
  id,
  name,
  include_in_payee_list: true,
  automatically_categorize_payee: false,
  renaming_rules: [],
  is_deleted: false,
  ...extra
});

describe('buildStoreGroups', () => {
  const stores = [
    store('b1', 'Biedronka', { group_name: 'Biedronka' }),
    store('b2', 'BIEDRONKA 4521 KRAKOW', { group_name: ' biedronka ', automatic_category_id: 'food' }),
    store('i', 'Ikea')
  ];
  const stats = toStatsMap([
    { store_id: 'b2', count: 2, spent_cents_12m: 500, last_date: '2026-10-02' },
    { store_id: 'b1', count: 1, spent_cents_12m: 100, last_date: '2026-09-01' }
  ]);

  it('łączy nazwy grupowe bez względu na wielkość liter, a głównym sklepem jest ten z największą liczbą transakcji', () => {
    const groups = buildStoreGroups(stores, stats);
    expect(groups).toHaveLength(2);
    const group = groups.find((entry) => entry.id === 'b2')!;
    expect(group.members.map((member) => member.id)).toEqual(['b2', 'b1']);
    expect(group).toMatchObject({ count: 3, spentCents: 600, lastDate: '2026-10-02', categoryId: 'food' });
  });

  it('nie dokleja zarchiwizowanego sklepu do aktywnej grupy', () => {
    const groups = buildStoreGroups([store('a', 'Lidl'), store('b', 'Lidl', { is_deleted: true })], new Map());
    expect(groups.map((group) => group.isArchived).sort()).toEqual([false, true]);
  });

  it('sortuje malejąco po wydatkach', () => {
    const sorted = sortStoreGroups(buildStoreGroups(stores, stats), 'spent');
    expect(sorted[0].id).toBe('b2');
  });

  it('odwraca kierunek, a sklepy bez transakcji zostawia na końcu', () => {
    const groups = buildStoreGroups(stores, stats);
    const ascending = sortStoreGroups(groups, 'spent', 'asc');
    expect(ascending[ascending.length - 1].id).toBe('b2');
    expect(sortStoreGroups(groups, 'last', 'asc').map((group) => group.id)).toEqual(['b2', 'i']);
    expect(sortStoreGroups(groups, 'last', 'desc').map((group) => group.id)).toEqual(['b2', 'i']);
  });

  it('nazwy sortuje alfabetycznie w obie strony', () => {
    const groups = buildStoreGroups(stores, stats);
    const names = sortStoreGroups(groups, 'name').map((group) => group.name);
    expect(sortStoreGroups(groups, 'name', 'desc').map((group) => group.name)).toEqual(names.slice().reverse());
  });
});

describe('isInactiveGroup', () => {
  const make = (lastDate: string | null) =>
    ({ lastDate } as Parameters<typeof isInactiveGroup>[0]);

  it('granicą jest początek okna 12 miesięcy', () => {
    expect(spendWindowStart(TODAY)).toBe('2025-12-01');
    expect(isInactiveGroup(make('2025-12-01'), TODAY)).toBe(false);
    expect(isInactiveGroup(make('2025-11-30'), TODAY)).toBe(true);
    expect(isInactiveGroup(make(null), TODAY)).toBe(true);
  });
});
