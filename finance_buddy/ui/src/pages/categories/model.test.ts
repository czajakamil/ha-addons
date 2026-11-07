import { describe, expect, it } from 'vitest';
import type { Category } from '../../api';
import {
  buildCategoryTree,
  countUnused,
  flattenVisible,
  planMove,
  planShift,
  sortTree,
  toStatsMap,
  windowStart
} from './model';

const cat = (id: string, name: string, parent: string | null, order: number): Category => ({
  id,
  name,
  parent_id: parent,
  sort_order: order,
  path: name
});

const categories = [
  cat('food', 'Jedzenie', null, 0),
  cat('home', 'Dom', null, 1),
  cat('shop', 'Zakupy', 'food', 0),
  cat('out', 'Na mieście', 'food', 1),
  cat('rest', 'Restauracja', 'out', 0),
  cat('rent', 'Czynsz', 'home', 0)
];

const recent = windowStart();
const months = (value: number) => Array.from({ length: 12 }, (_, index) => (index === 11 ? value : 0));
const stats = toStatsMap([
  { category_id: 'shop', count: 3, spent_cents_12m: 3000, last_date: recent, monthly_cents: months(3000) },
  { category_id: 'rest', count: 1, spent_cents_12m: 500, last_date: recent, monthly_cents: months(500) },
  { category_id: 'out', count: 1, spent_cents_12m: 200, last_date: recent, monthly_cents: months(200) }
]);

const tree = buildCategoryTree(categories, stats);

describe('buildCategoryTree', () => {
  it('sumuje gałąź razem z własnymi transakcjami rodzica', () => {
    const [food] = tree;
    expect(food.total.count).toBe(5);
    expect(food.total.spentCents).toBe(3700);
    expect(food.total.monthly[11]).toBe(3700);
    expect(food.children.map((node) => node.category.name)).toEqual(['Zakupy', 'Na mieście']);
  });
});

describe('flattenVisible', () => {
  it('chowa nieużywane gałęzie, chyba że są świeżo dodane', () => {
    const base = { expanded: new Set<string>(), query: '', hideUnused: true };
    expect(flattenVisible(tree, { ...base, keep: new Set() }).map((row) => row.category.id)).toEqual(['food']);
    expect(flattenVisible(tree, { ...base, keep: new Set(['rent']) }).map((row) => row.category.id)).toEqual([
      'food',
      'home'
    ]);
    expect(countUnused(tree)).toBe(1);
  });

  it('wyszukiwanie pokazuje przodków dopasowania i rozwija je', () => {
    const rows = flattenVisible(tree, { expanded: new Set(), query: 'resta', hideUnused: true, keep: new Set() });
    expect(rows.map((row) => row.category.id)).toEqual(['food', 'out', 'rest']);
  });
});

describe('planMove', () => {
  it('wstawia przed rodzeństwem', () => {
    expect(planMove(tree, 'out', 'shop', 'before')).toEqual({ parentId: 'food', orderedIds: ['out', 'shop'] });
  });

  it('przenosi do środka innej kategorii na koniec', () => {
    expect(planMove(tree, 'rest', 'home', 'inside')).toEqual({ parentId: 'home', orderedIds: ['rent', 'rest'] });
  });

  it('nie wpina gałęzi w nią samą i pomija ruch bez zmian', () => {
    expect(planMove(tree, 'food', 'rest', 'inside')).toBeNull();
    expect(planMove(tree, 'shop', 'out', 'before')).toBeNull();
  });

  it('planShift zamienia z sąsiadem i zatrzymuje się na brzegu', () => {
    expect(planShift(tree, 'home', -1)).toEqual({ parentId: null, orderedIds: ['home', 'food'] });
    expect(planShift(tree, 'food', -1)).toBeNull();
  });
});

describe('sortTree', () => {
  it('sortuje rodzeństwo na każdym poziomie, a kategorie bez transakcji idą na koniec', () => {
    const bySpent = sortTree(tree, { key: 'spent', direction: 'asc' });
    expect(bySpent.map((node) => node.category.id)).toEqual(['home', 'food']);
    expect(bySpent[1].children.map((node) => node.category.id)).toEqual(['out', 'shop']);
    const byLast = sortTree(tree, { key: 'last', direction: 'asc' });
    expect(byLast.map((node) => node.category.id)).toEqual(['food', 'home']);
    const byName = sortTree(tree, { key: 'name', direction: 'asc' });
    expect(byName.map((node) => node.category.name)).toEqual(['Dom', 'Jedzenie']);
    expect(sortTree(tree, null).map((node) => node.category.id)).toEqual(['food', 'home']);
  });
});
