import { describe, expect, it } from 'vitest';
import type { Item, ItemMergeResult } from '../../api';
import { describeMerge, sortItems, toItemStatsMap } from './model';

const result = (patch: Partial<ItemMergeResult>): ItemMergeResult => ({
  target_id: 't',
  merged_count: 1,
  items_moved: 0,
  aliases_moved: 0,
  aliases_added: 0,
  dry_run: true,
  ...patch
});

describe('describeMerge', () => {
  it('liczy pozycje i nazwy z paragonów z polską odmianą', () => {
    expect(describeMerge(result({ items_moved: 12, aliases_moved: 1, aliases_added: 1 }), 'Vifon', 'Zupka chińska')).toBe(
      'Do „Zupka chińska” przejdzie 12 pozycji i 2 nazwy z paragonów. „Vifon” zniknie z listy.'
    );
  });

  it('pomija nazwy, gdy nic nie przechodzi', () => {
    expect(describeMerge(result({ items_moved: 1 }), 'A', 'B')).toBe('Do „B” przejdzie 1 pozycja. „A” zniknie z listy.');
  });
});

describe('sortItems po statystykach', () => {
  const items: Item[] = [
    { id: 'a', name: 'Banany' },
    { id: 'b', name: 'Arbuz' },
    { id: 'c', name: 'Cebula' },
    { id: 'd', name: 'Mleko' }
  ];
  const stats = toItemStatsMap([
    { item_id: 'c', count: 5, spent_cents_12m: 900, last_date: '2026-10-01' },
    { item_id: 'd', count: 2, spent_cents_12m: 4000, last_date: '2026-11-03' }
  ]);
  const order = (sort: Parameters<typeof sortItems>[1]) => sortItems(items, sort, stats).map((item) => item.id);

  it('malejąco, a niekupione alfabetycznie na końcu', () => {
    expect(order('spent_desc')).toEqual(['d', 'c', 'b', 'a']);
    expect(order('count_desc')).toEqual(['c', 'd', 'b', 'a']);
    expect(order('last_desc')).toEqual(['d', 'c', 'b', 'a']);
  });
});
