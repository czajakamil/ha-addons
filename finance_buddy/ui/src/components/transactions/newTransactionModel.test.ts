import { describe, expect, it } from 'vitest';
import type { Category, Store, Transaction } from '../../api';
import {
  categoryParentPrefix,
  categoryPickerRows,
  recentCategoryIds,
  recentStoreIds,
  storeDisplayName,
  suggestedCategoryId
} from './newTransactionModel';

const tx = (overrides: Partial<Transaction>): Transaction => ({
  id: 't',
  account_id: 'a',
  currency: 'PLN',
  cleared: true,
  reconciled: false,
  total_cents: -100,
  items: [],
  ...overrides
});

const cat = (id: string, name: string, path: string, parent_id: string | null = null): Category => ({
  id,
  name,
  path,
  parent_id
});

const categories = [
  cat('food', 'Jedzenie', 'jedzenie'),
  cat('groceries', 'Zakupy spożywcze', 'jedzenie/zakupy spożywcze', 'food'),
  cat('snacks', 'Przekąski', 'jedzenie/przekąski', 'food'),
  cat('fuel', 'Paliwo', 'paliwo')
];

describe('recentStoreIds / recentCategoryIds', () => {
  const list = [
    tx({ store_id: 's1', category_id: 'c1' }),
    tx({ store_id: 's1', category_id: 'c2' }),
    tx({ transfer_account_id: 'b', store_id: 'sx' }),
    tx({ store_id: 's2', category_id: 'c1' })
  ];

  it('zwraca unikalne, od najświeższego, bez przelewów', () => {
    expect(recentStoreIds(list)).toEqual(['s1', 's2']);
    expect(recentCategoryIds(list)).toEqual(['c1', 'c2']);
  });

  it('przestrzega limitu', () => {
    expect(recentCategoryIds(list, 1)).toEqual(['c1']);
  });
});

describe('suggestedCategoryId', () => {
  const ids = new Set(['groceries', 'fuel']);

  it('woli automatyczną kategorię sklepu', () => {
    const store = { id: 's1', automatic_category_id: 'fuel' } as Store;
    expect(suggestedCategoryId(store, [tx({ store_id: 's1', category_id: 'groceries' })], ids)).toBe('fuel');
  });

  it('bez niej bierze kategorię z ostatniej transakcji sklepu', () => {
    const store = { id: 's1', automatic_category_id: null } as Store;
    expect(suggestedCategoryId(store, [tx({ store_id: 's2', category_id: 'fuel' }), tx({ store_id: 's1', category_id: 'groceries' })], ids)).toBe(
      'groceries'
    );
  });

  it('pomija usunięte kategorie', () => {
    const store = { id: 's1', automatic_category_id: 'gone' } as Store;
    expect(suggestedCategoryId(store, [], ids)).toBeNull();
  });
});

describe('categoryPickerRows', () => {
  it('bez frazy: drzewo (rodzeństwo alfabetycznie), rodzic z dziećmi jest nagłówkiem', () => {
    const rows = categoryPickerRows(categories, '');
    expect(rows.map((row) => [row.id, row.depth, row.selectable])).toEqual([
      ['food', 0, false],
      ['snacks', 1, true],
      ['groceries', 1, true],
      ['fuel', 0, true]
    ]);
  });

  it('z frazą: płaska lista liści z przodkami w przedrostku', () => {
    expect(categoryPickerRows(categories, 'przek')).toEqual([
      { id: 'snacks', label: 'Przekąski', prefix: 'Jedzenie › ', depth: 0, selectable: true }
    ]);
  });
});

describe('etykiety', () => {
  it('przedrostek przodków', () => {
    const byId = new Map(categories.map((category) => [category.id, category]));
    expect(categoryParentPrefix(categories[2], byId)).toBe('Jedzenie › ');
    expect(categoryParentPrefix(categories[3], byId)).toBe('');
  });

  it('nazwa sklepu z grupy', () => {
    expect(storeDisplayName({ name: 'ZABKA Z1', group_name: 'Żabka' })).toBe('Żabka');
    expect(storeDisplayName({ name: 'Orlen', group_name: null })).toBe('Orlen');
  });
});
