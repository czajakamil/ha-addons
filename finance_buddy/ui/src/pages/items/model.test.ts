import { describe, expect, it } from 'vitest';
import type { ItemMergeResult } from '../../api';
import { describeMerge } from './model';

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
