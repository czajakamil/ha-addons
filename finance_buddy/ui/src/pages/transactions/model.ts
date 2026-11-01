// Typy, stałe i czyste helpery strony transakcji — bez stanu i bez JSX.
import { Transaction } from '../../api';
import { toIsoDate } from '../../utils/date';

export type FilterState = {
  startDate: string;
  endDate: string;
  accountId: string;
  categoryId: string;
  // Strona sklepu prowadzi tu z gotowym filtrem (?stores=)
  storeIds?: string[];
};

export type DatePreset = 'all' | 'thisMonth' | 'previousMonth' | 'last90' | 'year' | 'custom';

// Zakres dat presetu. „custom" nie ma tu zakresu: daty wybiera użytkownik.
export const datePresetRange = (preset: DatePreset, now = new Date()) => {
  const year = now.getFullYear();
  const month = now.getMonth();
  const today = toIsoDate(now);
  switch (preset) {
    case 'thisMonth':
      return { startDate: toIsoDate(new Date(year, month, 1)), endDate: today };
    case 'previousMonth':
      return { startDate: toIsoDate(new Date(year, month - 1, 1)), endDate: toIsoDate(new Date(year, month, 0)) };
    case 'last90':
      return { startDate: toIsoDate(new Date(year, month, now.getDate() - 90)), endDate: today };
    case 'year':
      return { startDate: toIsoDate(new Date(year, 0, 1)), endDate: today };
    default:
      return { startDate: '', endDate: '' };
  }
};

export type TransactionColumnId =
  | 'select'
  | 'open'
  | 'account'
  | 'store'
  | 'category'
  | 'comment'
  | 'amount'
  | 'status';

export type TransactionStatusState = 'pending' | 'cleared' | 'reconciled';

export const STATUS_PRESENTATION: Record<
  TransactionStatusState,
  { short: string; label: string; nextAction: string }
> = {
  pending: { short: 'N', label: 'Niezatwierdzona', nextAction: 'zatwierdzić' },
  cleared: { short: 'Z', label: 'Zatwierdzona', nextAction: 'oznaczyć jako uzgodnioną' },
  reconciled: { short: 'U', label: 'Uzgodniona', nextAction: 'cofnąć zatwierdzenie' }
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

export const DEFAULT_COLUMN_WIDTHS: Partial<Record<TransactionColumnId, number>> = {
  select: 40,
  open: 44,
  store: 260,
  account: 160,
  category: 200,
  amount: 128,
  status: 40
};
export type EditableLookupField = 'account' | 'store' | 'category';
