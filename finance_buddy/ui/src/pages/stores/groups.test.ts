import { describe, expect, it } from 'vitest';
import type { Store, Transaction } from '../../api';
import {
  aggregateStoreStats,
  buildMonthlySpend,
  buildStoreGroups,
  isInactiveGroup,
  sortStoreGroups,
  spendWindowStart
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

const tx = (storeId: string | null, date: string, total: number, extra: Partial<Transaction> = {}): Transaction => ({
  id: `${storeId}-${date}-${total}`,
  account_id: 'a',
  store_id: storeId,
  date,
  currency: 'PLN',
  cleared: true,
  reconciled: false,
  total_cents: total,
  items: [],
  ...extra
});

describe('aggregateStoreStats', () => {
  const stores = [store('s', 'Lidl')];

  it('liczy całą historię, a wydatki tylko z okna 12 miesięcy', () => {
    const stat = aggregateStoreStats(
      [tx('s', '2026-11-01', -1000), tx('s', '2025-12-01', -500), tx('s', '2025-11-30', -9900)],
      stores,
      TODAY
    ).get('s');
    expect(stat).toEqual({ count: 3, spentCents: 1500, lastDate: '2026-11-01' });
  });

  it('odejmuje zwroty, pomija przelewy i inne waluty, ale je zlicza', () => {
    const stat = aggregateStoreStats(
      [
        tx('s', '2026-10-01', -1000),
        tx('s', '2026-10-02', 300),
        tx('s', '2026-10-03', -5000, { transfer_account_id: 'b' }),
        tx('s', '2026-10-04', -7000, { currency: 'EUR' })
      ],
      stores,
      TODAY
    ).get('s');
    expect(stat).toMatchObject({ count: 4, spentCents: 700 });
  });

  it('nie traktuje korekty salda jako wydatku', () => {
    const adjustment = [store('r', 'Reconciliation Balance Adjustment')];
    expect(aggregateStoreStats([tx('r', '2026-10-01', -1000)], adjustment, TODAY).get('r')?.spentCents).toBe(0);
  });
});

describe('buildStoreGroups', () => {
  const stores = [
    store('b1', 'Biedronka', { group_name: 'Biedronka' }),
    store('b2', 'BIEDRONKA 4521 KRAKOW', { group_name: ' biedronka ', automatic_category_id: 'food' }),
    store('i', 'Ikea')
  ];
  const stats = aggregateStoreStats([tx('b2', '2026-10-01', -200), tx('b2', '2026-10-02', -300), tx('b1', '2026-09-01', -100)], stores, TODAY);

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

describe('buildMonthlySpend', () => {
  it('zwraca 12 miesięcy z bieżącym na końcu i nie schodzi poniżej zera', () => {
    const months = buildMonthlySpend([tx('s', '2026-11-02', -800), tx('s', '2026-10-02', 500)], [], TODAY);
    expect(months).toHaveLength(12);
    expect(months[11]).toEqual({ month: '2026-11', cents: 800 });
    expect(months[10]).toEqual({ month: '2026-10', cents: 0 });
    expect(months[0].month).toBe('2025-12');
  });
});
