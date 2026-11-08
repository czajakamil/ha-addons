// Typy, stałe i czyste helpery strony transakcji — bez stanu i bez JSX.
import { Transaction } from '../../api';
import { toIsoDate } from '../../utils/date';

export type FilterState = {
  startDate: string;
  endDate: string;
  // Puste = wszystkie konta
  accountIds: string[];
  // Puste = wszystkie kategorie; dokładne id (rodzic nie obejmuje sam podkategorii —
  // zaznacza je menu), plus `__uncategorized__` dla transakcji bez kategorii
  categoryIds: string[];
  // Strona sklepu prowadzi tu z gotowym filtrem (?stores=)
  storeIds?: string[];
  // Tylko niezatwierdzone — kolejka „Do zatwierdzenia"
  unclearedOnly?: boolean;
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
  | 'time'
  | 'account'
  | 'store'
  | 'category'
  | 'comment'
  | 'amount'
  | 'outflow'
  | 'inflow'
  | 'status';

export type TransactionStatusState = 'pending' | 'cleared' | 'reconciled';

// Kliknięcie w status przełącza tylko niezatwierdzona ↔ zatwierdzona. Uzgodnienie to świadoma
// decyzja (menu kontekstowe), a jego cofnięcie wymaga potwierdzenia — przypadkowe kliknięcie
// nie może zdjąć statusu „sprawdzone z wyciągiem”.
export const STATUS_PRESENTATION: Record<TransactionStatusState, { label: string; clickHint: string }> = {
  pending: { label: 'Niezatwierdzona', clickHint: 'Kliknij, aby zatwierdzić.' },
  cleared: { label: 'Zatwierdzona', clickHint: 'Kliknij, aby cofnąć zatwierdzenie.' },
  reconciled: { label: 'Uzgodniona', clickHint: 'Kliknij, aby cofnąć uzgodnienie.' }
};

export const UNRECONCILE_CONFIRM = 'Cofnąć uzgodnienie tej transakcji? Pozostanie zatwierdzona.';

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

/** Payload dla kliknięcia w ikonę statusu. Z uzgodnionej schodzi tylko do zatwierdzonej. */
export const getToggleStatusPayload = (transaction: Transaction): StatusUpdatePayload => {
  if (transaction.reconciled) {
    return { cleared: true, reconciled: false };
  }
  return { cleared: !transaction.cleared, reconciled: false };
};

/** Payload dla „Oznacz jako uzgodnioną” / „Cofnij uzgodnienie” z menu kontekstowego. */
export const getToggleReconciledPayload = (transaction: Transaction): StatusUpdatePayload =>
  transaction.reconciled ? { cleared: true, reconciled: false } : { cleared: true, reconciled: true };

export const DEFAULT_COLUMN_WIDTHS: Partial<Record<TransactionColumnId, number>> = {
  select: 40,
  open: 44,
  time: 64,
  store: 196,
  account: 160,
  category: 200,
  outflow: 120,
  inflow: 120,
  status: 40
};
export type EditableLookupField = 'account' | 'store' | 'category';
export type EditableAmountField = 'outflow' | 'inflow';
