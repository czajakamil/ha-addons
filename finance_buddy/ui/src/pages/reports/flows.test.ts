import { describe, expect, it } from 'vitest';
import type { Category, Transaction } from '../../api';
import { buildFlows } from './flows';

const categories = [{ id: 'food', name: 'Zakupy', path: 'Dom/Zakupy' }] as Category[];
const tx = (total_cents: number, category_id: string | null = 'food') =>
  ({ total_cents, category_id, account_id: 'a', transfer_account_id: null }) as unknown as Transaction;
const build = (...transactions: Transaction[]) =>
  buildFlows({
    transactions,
    categories,
    categoryNameLookup: new Map([['food', 'Dom/Zakupy']]),
    accountIds: null,
    categoryIds: null
  });
const linkValue = (flows: ReturnType<typeof build>, source: string, target: string) =>
  flows.data.links.find((link) => link.source === source && link.target === target)?.value;

describe('buildFlows', () => {
  it('przy deficycie dodaje „Z oszczędności” i nie skaluje przychodów', () => {
    const flows = build(tx(100_00, null), tx(-150_00));

    expect(flows.fromSavings).toBe(50_00);
    expect(linkValue(flows, 'income', 'spending')).toBe(100_00);
    expect(linkValue(flows, 'savings', 'spending')).toBe(50_00);
    expect(linkValue(flows, 'spending', 'group:Dom')).toBe(150_00);
    expect(flows.data.nodes.find((node) => node.id === 'savings')?.name).toBe('Z oszczędności');
  });

  it('bez przychodów jedynym źródłem są oszczędności', () => {
    const flows = build(tx(-150_00));

    expect(flows.data.nodes.some((node) => node.id === 'income')).toBe(false);
    expect(linkValue(flows, 'savings', 'spending')).toBe(150_00);
  });

  it('przy nadwyżce nie ma węzła oszczędności', () => {
    const flows = build(tx(200_00, null), tx(-150_00));

    expect(flows.fromSavings).toBe(0);
    expect(flows.data.nodes.some((node) => node.id === 'savings')).toBe(false);
    expect(linkValue(flows, 'income', 'group:Dom')).toBe(150_00);
    expect(linkValue(flows, 'income', 'surplus')).toBe(50_00);
  });

  it('równe przychody i wydatki nie tworzą ani nadwyżki, ani oszczędności', () => {
    const ids = build(tx(150_00, null), tx(-150_00)).data.nodes.map((node) => node.id);

    expect(ids).not.toContain('savings');
    expect(ids).not.toContain('surplus');
  });
});
