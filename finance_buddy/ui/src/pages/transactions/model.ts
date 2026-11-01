// Typy, stałe i czyste helpery strony transakcji — bez stanu i bez JSX.
import { Transaction } from '../../api';
import { DATA_TABLE_ROW_HEIGHT } from '../../components/DataTable';

export type FilterState = {
  startDate: string;
  endDate: string;
  accountId: string;
};

export type TransactionColumnId =
  | 'open'
  | 'date'
  | 'account'
  | 'store'
  | 'category'
  | 'comment'
  | 'outflow'
  | 'inflow'
  | 'status';

export type TransactionStatusState = 'pending' | 'cleared' | 'reconciled';

export const STATUS_PRESENTATION: Record<
  TransactionStatusState,
  { short: string; label: string; nextAction: string }
> = {
  pending: { short: 'P', label: 'Przetwarzana', nextAction: 'oznaczyć jako Cleared' },
  cleared: { short: 'C', label: 'Cleared', nextAction: 'oznaczyć jako Reconciled' },
  reconciled: { short: 'R', label: 'Reconciled', nextAction: 'oznaczyć jako Przetwarzana' }
};

export const getTransactionStatusState = (transaction: Transaction): TransactionStatusState => {
  if (transaction.reconciled) {
    return 'reconciled';
  }
  if (transaction.cleared) {
    return 'cleared';
  }
  return 'pending';
};

export type StatusUpdatePayload = { cleared: boolean; reconciled: boolean };

export const getNextStatusPayload = (transaction: Transaction): StatusUpdatePayload => {
  if (transaction.reconciled) {
    return { cleared: false, reconciled: false };
  }
  if (transaction.cleared) {
    return { cleared: true, reconciled: true };
  }
  return { cleared: true, reconciled: false };
};

export const MIN_COLUMN_WIDTH = 10;
export const DEFAULT_COLUMN_WIDTHS: Partial<Record<TransactionColumnId, number>> = {
  open: 54,
  date: 100,
  account: 110,
  store: 180,
  category: 170,
  comment: 450,
  outflow: 120,
  inflow: 120,
  status: 40
};
export const COLUMN_WIDTH_TABLE_ID = 'transactions';
export const COLUMN_WIDTH_PERSIST_DELAY = 400;
const TRANSACTIONS_GRID_MIN_HEIGHT = 280;
const TRANSACTIONS_GRID_MAX_HEIGHT = 720;
export const TRANSACTION_COLUMN_ORDER: readonly TransactionColumnId[] = [
  'open',
  'date',
  'account',
  'store',
  'category',
  'comment',
  'outflow',
  'inflow',
  'status'
];

export const TRANSACTION_COLUMN_IDS = new Set<TransactionColumnId>(TRANSACTION_COLUMN_ORDER);

export const getTransactionsGridHeight = (rowCount: number) => {
  if (rowCount <= 0) {
    return TRANSACTIONS_GRID_MIN_HEIGHT;
  }
  const estimated = 64 + DATA_TABLE_ROW_HEIGHT * rowCount;
  return Math.min(TRANSACTIONS_GRID_MAX_HEIGHT, Math.max(TRANSACTIONS_GRID_MIN_HEIGHT, estimated));
};

export const sanitizeStoredColumnWidths = (
  widths: Record<string, unknown>
): Partial<Record<TransactionColumnId, number>> => {
  const sanitized: Partial<Record<TransactionColumnId, number>> = {};

  Object.entries(widths).forEach(([columnId, value]) => {
    const typedId = columnId as TransactionColumnId;

    if (!TRANSACTION_COLUMN_IDS.has(typedId)) return;
    if (typeof value !== 'number' || Number.isNaN(value)) return;

    sanitized[typedId] = Math.max(MIN_COLUMN_WIDTH, Math.round(value));
  });

  return sanitized;
};

export const toPersistableColumnWidths = (
  widths: Partial<Record<TransactionColumnId, number>>
): Record<string, number> => {
  const payload: Record<string, number> = {};

  (Object.entries(widths) as [TransactionColumnId, number | undefined][]).forEach(([columnId, value]) => {
    if (!TRANSACTION_COLUMN_IDS.has(columnId)) return;
    if (typeof value !== 'number' || Number.isNaN(value)) return;

    payload[columnId] = Math.max(MIN_COLUMN_WIDTH, Math.round(value));
  });

  return payload;
};

export type EditableLookupField = 'account' | 'store' | 'category';
export type EditableAmountField = 'outflow' | 'inflow';
