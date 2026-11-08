import { describe, expect, it } from 'vitest';
import type { Transaction } from '../../api';
import { categoryForStoreChange, getToggleReconciledPayload, getToggleStatusPayload } from './model';

const tx = (cleared: boolean, reconciled: boolean) => ({ cleared, reconciled }) as Transaction;

describe('getToggleStatusPayload', () => {
  it('zatwierdza niezatwierdzoną', () => {
    expect(getToggleStatusPayload(tx(false, false))).toEqual({ cleared: true, reconciled: false });
  });

  it('cofa zatwierdzenie', () => {
    expect(getToggleStatusPayload(tx(true, false))).toEqual({ cleared: false, reconciled: false });
  });

  it('z uzgodnionej schodzi tylko do zatwierdzonej, nigdy do niezatwierdzonej', () => {
    expect(getToggleStatusPayload(tx(true, true))).toEqual({ cleared: true, reconciled: false });
  });
});

describe('getToggleReconciledPayload', () => {
  it('uzgadnia niezatwierdzoną i zatwierdzoną (uzgodniona jest zawsze zatwierdzona)', () => {
    expect(getToggleReconciledPayload(tx(false, false))).toEqual({ cleared: true, reconciled: true });
    expect(getToggleReconciledPayload(tx(true, false))).toEqual({ cleared: true, reconciled: true });
  });

  it('cofa uzgodnienie do zatwierdzonej', () => {
    expect(getToggleReconciledPayload(tx(true, true))).toEqual({ cleared: true, reconciled: false });
  });
});

describe('categoryForStoreChange', () => {
  const base = { category_id: null, items: [] } as unknown as Transaction;

  it('dopisuje podpowiedź do transakcji bez kategorii', () => {
    expect(categoryForStoreChange(base, 'fuel')).toBe('fuel');
  });

  it('nie nadpisuje ustawionej kategorii', () => {
    expect(categoryForStoreChange({ ...base, category_id: 'groceries' }, 'fuel')).toBeNull();
  });

  it('pomija transakcje z pozycjami', () => {
    expect(categoryForStoreChange({ ...base, items: [{}] } as unknown as Transaction, 'fuel')).toBeNull();
  });

  it('bez podpowiedzi zostawia pustą', () => {
    expect(categoryForStoreChange(base, null)).toBeNull();
  });
});
