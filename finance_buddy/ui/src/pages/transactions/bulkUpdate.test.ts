import { describe, expect, it, vi } from 'vitest';
import { ApiError, type Transaction } from '../../api';
import { BULK_CONCURRENCY, describeBulkError, runBulkUpdate, type BulkAction } from './bulkUpdate';

const tx = (id: string, extra: Partial<Transaction> = {}) => ({ id, ...extra }) as Transaction;

const setCategory: BulkAction = {
  label: 'Zmiana kategorii',
  skippedLabel: 'przelewy',
  payload: (transaction) => (transaction.transfer_account_id ? null : { category_id: 'c1' })
};

describe('runBulkUpdate', () => {
  it('rozdziela udane, pominięte i nieudane', async () => {
    const update = vi.fn(async (id: string) => {
      if (id === 'bad') throw new ApiError(500, 'boom', null);
      return tx(id, { category_id: 'c1' });
    });

    const report = await runBulkUpdate(
      [tx('a'), tx('bad'), tx('transfer', { transfer_account_id: 'x' }), tx('b')],
      setCategory,
      { update }
    );

    expect(report.updatedCount).toBe(2);
    expect(report.skippedCount).toBe(1);
    expect(report.failures.map((failure) => failure.transaction.id)).toEqual(['bad']);
    expect(update).not.toHaveBeenCalledWith('transfer', expect.anything());
  });

  it('nie przerywa kolejnych porcji po błędzie i raportuje postęp', async () => {
    const ids = Array.from({ length: BULK_CONCURRENCY + 2 }, (_, index) => `t${index}`);
    const onProgress = vi.fn();
    const update = vi.fn(async (id: string) => {
      if (id === 't0') throw new TypeError('Failed to fetch');
      return tx(id);
    });

    const report = await runBulkUpdate(ids.map((id) => tx(id)), setCategory, { update, onProgress });

    expect(update).toHaveBeenCalledTimes(ids.length);
    expect(report.updatedCount).toBe(ids.length - 1);
    expect(onProgress.mock.calls.map(([done]) => done)).toEqual([BULK_CONCURRENCY, ids.length]);
  });

  it('ponowienie dotyka tylko przekazanych transakcji', async () => {
    const update = vi.fn(async (id: string) => tx(id));
    const first = await runBulkUpdate([tx('ok'), tx('bad')], setCategory, {
      update: async (id) => {
        if (id === 'bad') throw new ApiError(500, '', null);
        return tx(id);
      }
    });

    const retry = await runBulkUpdate(
      first.failures.map((failure) => failure.transaction),
      first.action,
      { update }
    );

    expect(update.mock.calls.map(([id]) => id)).toEqual(['bad']);
    expect(retry.failures).toEqual([]);
    expect(retry.updatedCount).toBe(1);
  });
});

describe('describeBulkError', () => {
  it('nie pokazuje surowej odpowiedzi serwera', () => {
    const raw = '{"detail":"account_id cannot be null"}';
    expect(describeBulkError(new ApiError(422, raw, null))).not.toContain('account_id');
    expect(describeBulkError(new ApiError(404, raw, null))).not.toBe(describeBulkError(new ApiError(503, raw, null)));
    expect(describeBulkError(new TypeError('Failed to fetch'))).not.toContain('fetch');
  });
});
