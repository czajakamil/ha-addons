import { describe, expect, it } from 'vitest';
import type { Category, Item, TransactionItem } from '../../api';
import { UNCATEGORIZED_KEY, buildCategorySplit, itemCategory } from './model';

const category = (id: string, path: string) => ({ id, name: path.split('/').pop(), path }) as Category;
const categoriesById = new Map([category('food', 'Jedzenie'), category('junk', 'Jedzenie/Śmieciowe')].map((c) => [c.id, c]));
const productsById = new Map(
  [
    { id: 'chips', name: 'Chipsy', category_id: 'junk' },
    { id: 'bread', name: 'Chleb', category_id: null },
    { id: 'gone', name: 'Stary', category_id: 'deleted' }
  ].map((p) => [p.id, p as Item])
);
const item = (product_id: string, unit_price_cents: number) =>
  ({ id: product_id, product_id, unit_price_cents, quantity: 1 }) as TransactionItem;

describe('itemCategory', () => {
  it('bierze kategorię produktu', () => {
    expect(itemCategory(item('chips', 100), productsById, categoriesById, 'food')).toEqual({ categoryId: 'junk', inherited: false });
  });

  it('produkt bez kategorii (albo z usuniętą) dziedziczy kategorię transakcji', () => {
    expect(itemCategory(item('bread', 100), productsById, categoriesById, 'food')).toEqual({ categoryId: 'food', inherited: true });
    expect(itemCategory(item('gone', 100), productsById, categoriesById, 'food')).toEqual({ categoryId: 'food', inherited: true });
  });

  it('bez kategorii transakcji pozycja zostaje bez kategorii', () => {
    expect(itemCategory(item('bread', 100), productsById, categoriesById, null)).toEqual({ categoryId: null, inherited: false });
  });
});

describe('buildCategorySplit', () => {
  it('sumuje pozycje bez kategorii produktu do kategorii transakcji', () => {
    const items = [item('chips', 300), item('bread', 500), item('gone', 200)];
    const split = buildCategorySplit(items, productsById, categoriesById, 'food');
    expect(split.map((entry) => [entry.key, entry.cents])).toEqual([
      ['food', 700],
      ['junk', 300]
    ]);
    expect(split[0].share).toBeCloseTo(0.7);
  });

  it('bez kategorii transakcji pokazuje „Bez kategorii”', () => {
    const split = buildCategorySplit([item('bread', 500)], productsById, categoriesById, null);
    expect(split[0].key).toBe(UNCATEGORIZED_KEY);
  });
});
