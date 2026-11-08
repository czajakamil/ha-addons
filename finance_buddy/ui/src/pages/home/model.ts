// Logika ekranu startowego bez Reacta — co trafia do „Do zrobienia" i jak to
// opisać. Kolejność listy to kolejność pilności: najpierw to, co psuje budżet
// albo grozi utratą danych, potem zaległości, na końcu pieniądze do rozdzielenia.
import type { HomeEnvelope, HomePace, HomeSummary } from '../../api';
import type { BackupHealth } from '../panel/backupStatus';
import { formatSignedMoney } from '../../components/ui/Money';
import { copy } from '../../copy/pl';
import { pluralPL } from '../../utils/format';

export const HOME_CURRENCY = 'PLN';

export type TodoTone = 'danger' | 'warning' | 'info';
export type TodoKind = 'backup' | 'stale' | 'untagged' | 'receipts';

export type TodoItem = {
  key: string;
  kind: TodoKind;
  tone: TodoTone;
  label: string;
  detail?: string;
  /** Przycisk w wierszu; bez niego cały wiersz jest linkiem. */
  action?: string;
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

/**
 * Wiersze „Do zrobienia" poza kopertami (te mają własny, rozwijany wiersz)
 * i poza RTA (to pokazuje główna liczba na górze).
 */
export const buildTodoItems = (summary: HomeSummary, backup: BackupHealth | null): TodoItem[] => {
  const items: TodoItem[] = [];

  const backupItem = backup ? BACKUP_LABEL[backup] : undefined;
  if (backupItem) {
    items.push({ key: 'backup', kind: 'backup', ...backupItem, detail: 'Sprawdź w Panelu', to: '/panel' });
  }

  for (const account of summary.stale_accounts) {
    items.push({
      key: `stale-${account.account_id}`,
      kind: 'stale',
      tone: 'warning',
      label: `${account.account_name}: brak danych od ${days(account.days_since)}`,
      detail: 'Zaimportuj nowszy wyciąg',
      action: 'Importuj',
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
        detail: 'Zaimportuj nowszy wyciąg',
        action: 'Importuj',
        to: '/bank-import'
      });
    }
  }

  if (summary.untagged_count > 0) {
    const n = summary.untagged_count;
    items.push({
      key: 'untagged',
      kind: 'untagged',
      tone: 'warning',
      label: `${n} ${pluralPL(n, 'transakcja', 'transakcje', 'transakcji')} bez kategorii`,
      detail: 'Bez kategorii nie trafiają do żadnej koperty',
      action: 'Kategoryzuj',
      to: '/transactions/uncategorized'
    });
  }

  if (summary.receipts_todo > 0) {
    const n = summary.receipts_todo;
    items.push({
      key: 'receipts',
      kind: 'receipts',
      tone: 'warning',
      label: `${n} ${pluralPL(n, 'paragon', 'paragony', 'paragonów')} do przejrzenia`,
      detail: 'Odczytane, czekają na zatwierdzenie',
      action: 'Przejrzyj',
      to: '/receipts'
    });
  }

  return items;
};

/** Podpis rozwijanego wiersza kopert, np. „1 kategoria ponad budżet, 2 blisko limitu". */
export const describeEnvelopes = (overspentCount: number, envelopes: HomeEnvelope[]) => {
  const near = envelopes.filter((envelope) => envelope.available_cents > 0).length;
  const parts: string[] = [];
  if (overspentCount > 0) {
    parts.push(
      `${overspentCount} ${pluralPL(overspentCount, 'kategoria', 'kategorie', 'kategorii')} ponad budżet`
    );
  }
  if (near > 0) parts.push(`${near} blisko limitu`);
  return parts.join(', ');
};

/** Ile dni miesiąca zostało do wydania, licząc dzisiejszy. */
export const daysLeftIncludingToday = (todayIso: string) => {
  const [year, month, day] = todayIso.split('-').map(Number);
  return new Date(year, month, 0).getDate() - day + 1;
};

/** Średnio na dzień do końca miesiąca; null, gdy nie ma czego dzielić. */
export const perDay = (spendable: number, todayIso: string) => {
  if (spendable <= 0) return null;
  const left = daysLeftIncludingToday(todayIso);
  return { cents: Math.floor(spendable / left), days: left, label: days(left) };
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

/** Podpis pod główną liczbą: ile dni miesiąca zostało, licząc dzisiejszy — tak samo jak `perDay`. */
export const describeDaysLeft = (todayIso: string) => {
  const left = daysLeftIncludingToday(todayIso);
  return left === 1 ? copy.home.lastDay : copy.home.daysLeft(days(left));
};

/** Punkty polilinii dla wykresu tempa; wspólna skala Y dla obu linii, z zapasem 8% u góry. */
export const paceGeometry = (pace: HomePace, width: number, height: number) => {
  const lastDay = Math.max(1, pace.days_in_month - 1);
  const top = Math.max(1, ...pace.current_cents, ...pace.average_cents) * 1.08;
  const x = (day: number) => Math.round(((day - 1) / lastDay) * width * 10) / 10;
  const y = (cents: number) => Math.round((height - (cents / top) * height) * 10) / 10;
  const points = (values: number[]) => values.map((cents, i) => `${x(i + 1)},${y(cents)}`).join(' ');
  return {
    x,
    y,
    current: points(pace.current_cents),
    average: pace.average_cents.length ? points(pace.average_cents) : null
  };
};

// Poniżej tej różnicy względem średniej tempo uznajemy za zwykłe.
const PACE_TOLERANCE = 0.05;

/** Jedno zdanie pod wykresem: szybciej, wolniej czy jak zwykle do dzisiejszego dnia. */
export const paceVerdict = (pace: HomePace) => {
  const day = pace.current_cents.length;
  if (!day || !pace.average_cents.length) return null;
  const current = pace.current_cents[day - 1];
  const average = pace.average_cents[day - 1];
  const diff = current - average;
  if (average > 0 && Math.abs(diff) <= average * PACE_TOLERANCE) return copy.home.paceUsual;
  const amount = formatSignedMoney(Math.abs(diff), HOME_CURRENCY);
  return diff > 0 ? copy.home.paceMore(amount) : copy.home.paceLess(amount);
};
