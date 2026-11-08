import { describe, expect, it } from 'vitest';
import type { Account, Category, Item, Store, Transaction } from '../../api';
import { buildMobileDays, buildMobileRow, categoriesLabel, type MobileListContext } from './mobileList';

const tx = (overrides: Partial<Transaction>): Transaction => ({
  id: 't',
  account_id: 'main',
  currency: 'PLN',
  cleared: true,
  reconciled: false,
  total_cents: -1000,
  date: '2026-11-01',
  items: [],
  ...overrides
});

const category = (id: string, name: string): Category => ({ id, name, color: 'green', parent_id: null }) as Category;

const ctx = (): MobileListContext => ({
  accountMap: new Map([
    ['main', { id: 'main', name: 'Main Account' } as Account],
    ['saver', { id: 'saver', name: 'Smart Saver' } as Account]
  ]),
  storeMap: new Map([
    ['zabka', { id: 'zabka', name: 'ZABKA Z1234', group_name: 'Żabka' } as Store],
    ['ynab', { id: 'ynab', name: 'Reconciliation Balance Adjustment' } as Store]
  ]),
  categoryMap: new Map([
    ['food', category('food', 'Jedzenie')],
    ['home', category('home', 'Dom')],
    ['fun', category('fun', 'Rozrywka')]
  ]),
  productMap: new Map([
    ['p1', { id: 'p1', category_id: 'food' } as Item],
    ['p2', { id: 'p2', category_id: 'home' } as Item],
    ['p3', { id: 'p3', category_id: null } as Item]
  ])
});

const item = (id: string, productId: string) => ({ id, product_id: productId, quantity: 1, unit_price_cents: 100 }) as Transaction['items'][number];

describe('buildMobileRow', () => {
  it('pokazuje sklep z grupy, kategorię i konto', () => {
    const row = buildMobileRow(tx({ store_id: 'zabka', category_id: 'food', cleared: false }), ctx());
    expect(row).toMatchObject({
      kind: 'regular',
      name: 'Żabka',
      categoryLabel: 'Jedzenie',
      categoryMissing: false,
      account: 'Main Account',
      unapproved: true
    });
    expect(row.dots).toHaveLength(1);
  });

  it('liczy kategorie pozycji paragonu, a produkt bez kategorii bierze kategorię transakcji', () => {
    const row = buildMobileRow(
      tx({ category_id: 'fun', items: [item('i1', 'p1'), item('i2', 'p2'), item('i3', 'p3')] }),
      ctx()
    );
    expect(row.categoryLabel).toBe('3 kategorie');
    expect(row.dots).toHaveLength(3);
  });

  it('oznacza wydatek bez kategorii', () => {
    expect(buildMobileRow(tx({ store_id: 'zabka' }), ctx())).toMatchObject({
      categoryLabel: 'Bez kategorii',
      categoryMissing: true
    });
  });

  it('opisuje przelew kierunkiem między kontami', () => {
    expect(buildMobileRow(tx({ transfer_account_id: 'saver', total_cents: -200 }), ctx())).toMatchObject({
      kind: 'transfer',
      name: 'Main Account → Smart Saver',
      categoryLabel: 'Przelew',
      account: null
    });
  });

  it('rozpoznaje korektę salda z YNAB', () => {
    expect(buildMobileRow(tx({ store_id: 'ynab', cleared: false }), ctx())).toMatchObject({
      kind: 'adjustment',
      name: 'Korekta salda',
      categoryLabel: null,
      unapproved: false
    });
  });

  it('na stronie konta nie powtarza konta', () => {
    expect(buildMobileRow(tx({ store_id: 'zabka' }), { ...ctx(), hideAccount: true }).account).toBeNull();
  });
});

describe('buildMobileDays', () => {
  it('grupuje po dacie i sumuje tylko zwykłe transakcje, gdy są co najmniej dwie', () => {
    const days = buildMobileDays(
      [
        tx({ id: 'a', store_id: 'zabka', total_cents: -1000 }),
        tx({ id: 'b', store_id: 'zabka', total_cents: -500 }),
        tx({ id: 'c', transfer_account_id: 'saver', total_cents: -9999 }),
        tx({ id: 'd', date: '2026-10-31', store_id: 'zabka' })
      ],
      ctx()
    );
    expect(days.map((day) => day.date)).toEqual(['2026-11-01', '2026-10-31']);
    expect(days[0].sumCents).toBe(-1500);
    expect(days[1].sumCents).toBeNull();
  });
});

describe('categoriesLabel', () => {
  it('odmienia liczebnik', () => {
    expect(categoriesLabel(2)).toBe('2 kategorie');
    expect(categoriesLabel(5)).toBe('5 kategorii');
    expect(categoriesLabel(12)).toBe('12 kategorii');
    expect(categoriesLabel(22)).toBe('22 kategorie');
  });
});
