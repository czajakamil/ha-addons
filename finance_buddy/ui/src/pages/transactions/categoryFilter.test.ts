import { describe, expect, it } from 'vitest';
import type { Category } from '../../api';
import {
  UNCATEGORIZED_ID,
  allFilterIds,
  buildFilterTree,
  categoryFilterOptions,
  expandCategoryParam,
  selectedCategoryNames,
  toggleCategory
} from './categoryFilter';

const cat = (id: string, name: string, parent_id: string | null = null, sort_order = 0): Category => ({
  id,
  name,
  parent_id,
  path: name,
  sort_order
});

const categories = [
  cat('food', '🍔 Jedzenie', null, 0),
  cat('catering', 'Catering', 'food', 0),
  cat('sweets', 'Ciastka', 'food', 1),
  cat('car', '🚗 Samochód', null, 1),
  cat('fuel', 'Paliwo', 'car', 0)
];
const tree = buildFilterTree(categories);
const NONE = 'Bez kategorii';

describe('categoryFilterOptions', () => {
  it('lists the tree in custom order with depth', () => {
    const options = categoryFilterOptions(tree, '', [], NONE);
    expect(options.map((option) => [option.label, option.depth ?? 0])).toEqual([
      [NONE, 0],
      ['🍔 Jedzenie', 0],
      ['Catering', 1],
      ['Ciastka', 1],
      ['🚗 Samochód', 0],
      ['Paliwo', 1]
    ]);
  });

  it('shows a matching child under its parent and a matching parent with all children', () => {
    expect(categoryFilterOptions(tree, 'pali', [], NONE).map((option) => option.id)).toEqual(['car', 'fuel']);
    expect(categoryFilterOptions(tree, 'jedz', [], NONE).map((option) => option.id)).toEqual([
      'food',
      'catering',
      'sweets'
    ]);
  });

  it('marks a partly selected branch', () => {
    const food = categoryFilterOptions(tree, '', ['catering'], NONE).find((option) => option.id === 'food');
    expect(food?.indeterminate).toBe(true);
  });
});

describe('toggleCategory', () => {
  it('selects and clears a whole branch', () => {
    const selected = toggleCategory(tree, [], 'food');
    expect(selected.sort()).toEqual(['catering', 'food', 'sweets']);
    expect(toggleCategory(tree, selected, 'food')).toEqual([]);
  });

  it('drops the parent when a child is cleared and restores it when the last child is picked', () => {
    const withoutSweets = toggleCategory(tree, ['food', 'catering', 'sweets'], 'sweets');
    expect(withoutSweets).toEqual(['catering']);
    expect(toggleCategory(tree, withoutSweets, 'sweets').sort()).toEqual(['catering', 'food', 'sweets']);
  });

  it('toggles uncategorized on its own', () => {
    expect(toggleCategory(tree, [], UNCATEGORIZED_ID)).toEqual([UNCATEGORIZED_ID]);
  });
});

describe('selectedCategoryNames', () => {
  it('names a full branch once and loose children separately', () => {
    expect(selectedCategoryNames(tree, ['food', 'catering', 'sweets', 'fuel'], NONE)).toEqual([
      '🍔 Jedzenie',
      'Paliwo'
    ]);
    expect(selectedCategoryNames(tree, [UNCATEGORIZED_ID], NONE)).toEqual([NONE]);
  });
});

describe('helpers', () => {
  it('counts every option including uncategorized', () => {
    expect(allFilterIds(tree)).toHaveLength(6);
  });

  it('expands a parent from the link to its branch', () => {
    expect(expandCategoryParam(categories, 'car').sort()).toEqual(['car', 'fuel']);
    expect(expandCategoryParam(categories, 'fuel')).toEqual(['fuel']);
    expect(expandCategoryParam(categories, '')).toEqual([]);
  });
});
