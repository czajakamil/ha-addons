import { describe, expect, it } from 'vitest';
import type { Category, NetWorthAccountBalance } from '../../api';
import {
  buildKindEntries,
  buildTagEntries,
  findDrillableTagIds,
  tagPath
} from './netWorthComposition';

const categories = [
  { id: 'savings', name: 'Oszczędności', parent_id: null },
  { id: 'retire', name: 'Emerytura', parent_id: 'savings' },
  { id: 'ike', name: 'IKE', parent_id: 'retire' },
  { id: 'daily', name: 'Codzienne', parent_id: null }
] as Category[];
const balance = (
  balance_cents: number,
  category_id: string | null,
  account_kind = 'checking'
): NetWorthAccountBalance => ({
  account_id: `${category_id}-${balance_cents}`,
  account_kind,
  category_id,
  balance_cents
});
const balances = [
  balance(100_00, 'ike', 'savings'),
  balance(50_00, 'savings', 'savings'),
  balance(200_00, 'daily'),
  balance(-30_00, null, 'creditCard')
];
const sum = (entries: { total: number }[]) => entries.reduce((total, e) => total + e.total, 0);

describe('skład majątku', () => {
  it('suma typów i tagów jest równa sumie sald, także z długiem', () => {
    expect(sum(buildKindEntries(balances))).toBe(320_00);
    expect(sum(buildTagEntries(balances, categories, null))).toBe(320_00);
  });

  it('procent liczy z wielkości kwot, więc dług nie zawyża udziałów ponad 100%', () => {
    const entries = buildTagEntries(balances, categories, null);
    expect(entries.map((entry) => entry.key)).toEqual(['daily', 'savings', '__account_uncategorized__']);
    expect(entries.reduce((total, entry) => total + entry.percentage, 0)).toBeCloseTo(1);
    expect(entries[2].total).toBe(-30_00);
  });

  it('konto z nieznanym tagiem trafia do „Bez kategorii”', () => {
    const entries = buildTagEntries([balance(10_00, 'skasowany')], categories, null);
    expect(entries[0].name).toBe('Bez kategorii');
  });

  it('po wejściu w tag grupuje według podtagów i konta bezpośrednio na tagu', () => {
    const entries = buildTagEntries(balances, categories, 'savings');
    expect(entries.map((entry) => [entry.name, entry.total])).toEqual([
      ['Emerytura', 100_00],
      ['Bez podkategorii', 50_00]
    ]);
  });

  it('pozwala wejść tylko w tagi z niezerowym podtagiem', () => {
    expect(findDrillableTagIds(balances, categories)).toEqual(new Set(['savings', 'retire']));
    const netZero = [balance(10_00, 'ike'), balance(-10_00, 'ike')];
    expect(findDrillableTagIds(netZero, categories).size).toBe(0);
  });

  it('buduje ścieżkę od korzenia', () => {
    expect(tagPath('ike', categories)).toBe('Oszczędności / Emerytura / IKE');
  });
});
