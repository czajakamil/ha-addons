// Logika ekranu startowego bez Reacta — co trafia do „Do zrobienia" i jak to
// opisać. Kolejność listy to kolejność pilności: najpierw to, co psuje budżet
// albo grozi utratą danych, potem zaległości, na końcu pieniądze do rozdzielenia.
import type { HomeEnvelope, HomeSummary } from '../../api';
import type { BackupHealth } from '../panel/backupStatus';
import { formatSignedMoney } from '../../components/ui/Money';
import { copy } from '../../copy/pl';
import { formatMoney, pluralPL } from '../../utils/format';

export const HOME_CURRENCY = 'PLN';

export type TodoTone = 'danger' | 'warning' | 'info';
export type TodoKind = 'backup' | 'overspent' | 'stale' | 'untagged' | 'receipts' | 'assign';

export type TodoItem = {
  key: string;
  kind: TodoKind;
  tone: TodoTone;
  label: string;
  value?: string;
  to: string;
};

const days = (n: number) => `${n} ${pluralPL(n, 'dzień', 'dni', 'dni')}`;

/** Pełne dni między datami ISO (YYYY-MM-DD), bez wpływu strefy czasowej. */
export const daysBetween = (fromIso: string, toIso: string) =>
  Math.round((Date.parse(toIso) - Date.parse(fromIso)) / 86_400_000);

const BACKUP_LABEL: Partial<Record<BackupHealth, { tone: TodoTone; label: string }>> = {
  error: { tone: 'danger', label: 'Kopia zapasowa nie powiodła się' },
  stale: { tone: 'warning', label: 'Kopia zapasowa jest nieaktualna' },
  missing: { tone: 'warning', label: 'Nie ma jeszcze żadnej kopii zapasowej' }
};

export const buildTodoItems = (summary: HomeSummary, backup: BackupHealth | null): TodoItem[] => {
  const items: TodoItem[] = [];

  const backupItem = backup ? BACKUP_LABEL[backup] : undefined;
  if (backupItem) {
    items.push({ key: 'backup', kind: 'backup', ...backupItem, to: '/panel' });
  }

  if (summary.overspent_count > 0) {
    items.push({
      key: 'overspent',
      kind: 'overspent',
      tone: 'danger',
      label: pluralPL(summary.overspent_count, 'Kategoria ponad budżet', 'Kategorie ponad budżet', 'Kategorii ponad budżet'),
      value: String(summary.overspent_count),
      to: '/budget?filter=overspent'
    });
  }

  if (summary.ready_to_assign_cents < 0) {
    items.push({
      key: 'assign',
      kind: 'assign',
      tone: 'danger',
      label: 'Przypisano więcej niż masz',
      value: formatMoney(-summary.ready_to_assign_cents, HOME_CURRENCY),
      to: '/budget?filter=overspent'
    });
  }

  for (const account of summary.stale_accounts) {
    items.push({
      key: `stale-${account.account_id}`,
      kind: 'stale',
      tone: 'warning',
      label: `${account.account_name}: brak danych od ${days(account.days_since)}`,
      to: '/bank-import'
    });
  }

  // Dopóki żadne konto nie ma historii importów, jedynym znakiem, że dane
  // przestały spływać, jest data ostatniej transakcji w ogóle.
  if (!summary.stale_accounts.length && summary.last_transaction_date) {
    const age = daysBetween(summary.last_transaction_date, summary.today);
    if (age > summary.stale_threshold_days) {
      items.push({
        key: 'stale-all',
        kind: 'stale',
        tone: 'warning',
        label: `Ostatnia transakcja ${days(age)} temu`,
        to: '/bank-import'
      });
    }
  }

  if (summary.untagged_count > 0) {
    items.push({
      key: 'untagged',
      kind: 'untagged',
      tone: 'warning',
      label: 'Transakcje bez kategorii',
      value: String(summary.untagged_count),
      to: '/transactions/uncategorized'
    });
  }

  if (summary.receipts_todo > 0) {
    items.push({
      key: 'receipts',
      kind: 'receipts',
      tone: 'warning',
      label: 'Paragony do przejrzenia',
      value: String(summary.receipts_todo),
      to: '/receipts'
    });
  }

  if (summary.ready_to_assign_cents > 0) {
    items.push({
      key: 'assign',
      kind: 'assign',
      tone: 'info',
      label: 'Do przypisania',
      value: formatMoney(summary.ready_to_assign_cents, HOME_CURRENCY),
      to: '/budget'
    });
  }

  return items;
};

/** Zmiana wydatków wobec tego samego odcinka poprzedniego miesiąca, w procentach. */
export const spendingChangePct = (current: number, previous: number): number | null => {
  if (previous <= 0) return null;
  return ((current - previous) / previous) * 100;
};

export const formatChangePct = (pct: number) => {
  const rounded = Math.round(pct * 10) / 10;
  const sign = rounded > 0 ? '+' : rounded < 0 ? '−' : '';
  return `${sign}${Math.abs(rounded).toLocaleString('pl-PL', { maximumFractionDigits: 1 })}%`;
};

/** Procent użycia kategorii dla paska; powyżej 100 oznacza przekroczenie. */
export const envelopeUsage = (envelope: HomeEnvelope) => {
  if (envelope.available_cents < 0) return 101;
  if (envelope.funds_cents <= 0) return 100;
  return Math.min(100, (envelope.spent_cents / envelope.funds_cents) * 100);
};

export const envelopeNote = (envelope: HomeEnvelope) =>
  envelope.available_cents < 0
    ? copy.home.overLimit(formatSignedMoney(-envelope.available_cents, HOME_CURRENCY))
    : copy.home.left(formatSignedMoney(envelope.available_cents, HOME_CURRENCY));

/** Podpis pod główną liczbą: ile dni zostało do końca miesiąca z daty ISO z backendu. */
export const describeDaysLeft = (todayIso: string) => {
  const [year, month, day] = todayIso.split('-').map(Number);
  const left = new Date(year, month, 0).getDate() - day;
  return left === 0 ? copy.home.lastDay : copy.home.daysLeft(days(left));
};
