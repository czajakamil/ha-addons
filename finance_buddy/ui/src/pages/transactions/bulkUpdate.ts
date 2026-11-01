// Akcja zbiorcza na transakcjach. API nie ma zapisu zbiorczego, więc idziemy porcjami
// pojedynczych żądań i zapisujemy wynik każdej transakcji osobno.
import { ApiError, type Transaction, type TransactionUpdate } from '../../api';
import { copy } from '../../copy/pl';

const labels = copy.transactions.bulk;

export const BULK_CONCURRENCY = 5;

export type BulkAction = {
  label: string;
  skippedLabel: string;
  // null oznacza, że ta transakcja nie wymaga zmiany
  payload: (transaction: Transaction) => TransactionUpdate | null;
};

export type BulkFailure = {
  transaction: Transaction;
  reason: string;
};

export type BulkReport = {
  action: BulkAction;
  updatedCount: number;
  skippedCount: number;
  failures: BulkFailure[];
};

export const describeBulkError = (error: unknown): string => {
  if (!(error instanceof ApiError)) return labels.errors.network;
  if (error.status === 404) return labels.errors.notFound;
  if (error.status >= 500) return labels.errors.server;
  return labels.errors.rejected;
};

type RunBulkUpdateOptions = {
  update: (id: string, payload: TransactionUpdate) => Promise<Transaction>;
  onProgress?: (done: number, updated: Transaction[]) => void;
};

export const runBulkUpdate = async (
  targets: readonly Transaction[],
  action: BulkAction,
  { update, onProgress }: RunBulkUpdateOptions
): Promise<BulkReport> => {
  const report: BulkReport = { action, updatedCount: 0, skippedCount: 0, failures: [] };

  for (let start = 0; start < targets.length; start += BULK_CONCURRENCY) {
    const batch = targets.slice(start, start + BULK_CONCURRENCY);
    const results = await Promise.allSettled(
      batch.map(async (transaction) => {
        const payload = action.payload(transaction);
        return payload ? update(transaction.id, payload) : null;
      })
    );

    const updated: Transaction[] = [];
    results.forEach((result, index) => {
      if (result.status === 'rejected') {
        report.failures.push({ transaction: batch[index], reason: describeBulkError(result.reason) });
      } else if (result.value) {
        updated.push(result.value);
      } else {
        report.skippedCount += 1;
      }
    });
    report.updatedCount += updated.length;
    onProgress?.(start + batch.length, updated);
  }

  return report;
};
