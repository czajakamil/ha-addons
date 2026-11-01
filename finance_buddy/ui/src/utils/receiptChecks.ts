/**
 * Kontrole spójności etykiety paragonu — bliźniak `backend/finance_app/receipt_checks.py`.
 *
 * Reguła żyje w dwóch językach świadomie. Formularz ocenia pola przy każdym
 * naciśnięciu klawisza i round-trip do serwera na każdą literę byłby marnotrawstwem;
 * serwer i tak sprawdza wszystko przy zatwierdzeniu, bo blokada, którą da się
 * ominąć, nie jest blokadą. Rozjazd między implementacjami to dokładnie ten
 * rodzaj błędu, który te kontrole naprawiają — dlatego trzyma je razem wspólny
 * zestaw przypadków `shared/receipt_checks_cases.json`, sprawdzany po obu
 * stronach (`receiptChecks.test.ts` i `backend/tests/test_receipt_checks.py`).
 *
 * Tu siedzi wyłącznie **mechanizm**. Progi (okno skanu, tolerancja sumy) przychodzą
 * z serwera razem z listą szkiców, więc zmiana wartości nie wymaga nowego builda.
 */

import { isValidNip } from './nip';

export type IssueLevel = 'bad' | 'warn';

export interface ReceiptCheckIssue {
  /** 'bad' — wartość niemożliwa przy tych danych; 'warn' — możliwa, ale podejrzana. */
  level: IssueLevel;
  /** Stabilny identyfikator reguły; tym samym kodem potwierdza się blokadę przy zapisie. */
  code: string;
  message: string;
  suggestion?: string | null;
  field?: string | null;
}

export interface ReceiptCheckRules {
  scan_window_days: number;
  item_sum_tolerance_ratio: number;
  item_sum_tolerance_min_cents: number;
}

/** Wartości awaryjne na czas, zanim wróci lista szkiców; źródłem prawdy jest serwer. */
export const DEFAULT_RULES: ReceiptCheckRules = {
  scan_window_days: 60,
  item_sum_tolerance_ratio: 0.01,
  item_sum_tolerance_min_cents: 2
};

export interface CheckableItem {
  line_total_cents?: number | null;
  discount_cents?: number | null;
}

export interface CheckableLabel {
  store_name?: string | null;
  nip?: string | null;
  happened_at?: string | null;
  total_cents?: number | null;
  items?: CheckableItem[] | null;
  receipt_discount_cents?: number | null;
  discounts_declared_cents?: number | null;
}

const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;

interface CivilDate {
  year: number;
  month: number;
  day: number;
}

const asDate = (year: number, month: number, day: number): CivilDate | null => {
  if (month < 1 || month > 12 || day < 1 || day > 31) return null;
  const probe = new Date(Date.UTC(2000, month - 1, day));
  // Rok podstawiamy przestępny, żeby 29 lutego nie wypadał zależnie od roku
  // etykiety; sam rok sprawdzać nie ma po co, bo bierzemy go wprost z tekstu.
  if (probe.getUTCMonth() + 1 !== month || probe.getUTCDate() !== day) return null;
  return { year, month, day };
};

const toIso = (date: CivilDate) =>
  `${String(date.year).padStart(4, '0')}-${String(date.month).padStart(2, '0')}-${String(
    date.day
  ).padStart(2, '0')}`;

const toEpochDay = (date: CivilDate) => Date.UTC(date.year, date.month - 1, date.day) / 86400000;

const daysBetween = (from: CivilDate, to: CivilDate) => toEpochDay(to) - toEpochDay(from);

/** Sama data z `happened_at` (`2024-06-19T20:44` → `2024-06-19`). */
export const datePart = (value: string | null | undefined): string | null => {
  if (!value) return null;
  const head = value.trim().slice(0, 10);
  return DATE_RE.test(head) ? head : null;
};

const parseDayText = (dayText: string): CivilDate | null =>
  asDate(Number(dayText.slice(0, 4)), Number(dayText.slice(5, 7)), Number(dayText.slice(8, 10)));

const swapDayAndMonth = (date: CivilDate) => asDate(date.year, date.day, date.month);

const withDate = (timestamp: string, date: CivilDate) => `${toIso(date)}${timestamp.slice(10)}`;

/**
 * Ocena daty wobec daty skanu; `null`, gdy bez zastrzeżeń.
 *
 * Paragon nie może być późniejszy niż moment, w którym trafił do systemu, ani
 * wyprzedzać go o więcej niż okno skanu. Reguła jest niezależna od OCR: patrzy
 * wyłącznie na wpisaną datę i na datę skanu.
 */
export const inspectDate = (
  happenedAt: string | null | undefined,
  scanDate: string | null | undefined,
  rules: ReceiptCheckRules = DEFAULT_RULES
): ReceiptCheckIssue | null => {
  const dayText = datePart(happenedAt);
  if (!dayText || !happenedAt) return null;
  const current = parseDayText(dayText);
  if (!current) return null;

  const scannedText = datePart(scanDate);
  const scanned = scannedText ? parseDayText(scannedText) : null;

  if (!scanned) {
    // Bez daty skanu zostaje sam zdrowy rozsądek co do roku.
    const maxYear = new Date().getFullYear() + 1;
    if (current.year < 2000 || current.year > maxYear) {
      return {
        level: 'bad',
        code: 'date_year_out_of_range',
        message: `rok ${current.year} jest niemożliwy`,
        suggestion: null
      };
    }
    return null;
  }

  const inWindow = (date: CivilDate) => {
    const delta = daysBetween(date, scanned);
    return delta >= 0 && delta <= rules.scan_window_days;
  };

  const swapped = swapDayAndMonth(current);

  if (inWindow(current)) {
    if (swapped && inWindow(swapped) && daysBetween(swapped, scanned) < daysBetween(current, scanned)) {
      return {
        level: 'warn',
        code: 'date_day_month_ambiguous',
        message:
          `obie daty możliwe, ale ${toIso(swapped)} leży bliżej skanu ` +
          `(${toIso(scanned)}) niż ${toIso(current)}`,
        suggestion: withDate(happenedAt, swapped)
      };
    }
    return null;
  }

  const reason =
    daysBetween(scanned, current) > 0
      ? `${toIso(current)} jest po dacie skanu (${toIso(scanned)})`
      : `${daysBetween(current, scanned)} dni różnicy między podaną datą (${toIso(
          current
        )}) a datą skanu (${toIso(scanned)})`;

  // Najpierw zamiana dnia z miesiącem (najczęstsza przyczyna), potem podmiana
  // roku. Kandydatów jest garść i przyjmujemy tylko tego, który wpada w okno.
  if (swapped && inWindow(swapped)) {
    return {
      level: 'bad',
      code: 'date_outside_scan_window',
      message: `${reason} — wygląda na zamienione dzień z miesiącem`,
      suggestion: withDate(happenedAt, swapped)
    };
  }
  for (const candidateYear of [scanned.year, scanned.year - 1]) {
    const fixed = asDate(candidateYear, current.month, current.day);
    if (fixed && inWindow(fixed)) {
      return {
        level: 'bad',
        code: 'date_outside_scan_window',
        message: `${reason} — wygląda na przekłamany rok`,
        suggestion: withDate(happenedAt, fixed)
      };
    }
  }
  return { level: 'bad', code: 'date_outside_scan_window', message: reason, suggestion: null };
};

/**
 * Zastrzeżenie, gdy NIP nie przechodzi sumy kontrolnej.
 *
 * Świadomie **bez podpowiedzi**, choć parser potrafi naprawić jedną cyfrę:
 * poprawek dających poprawną sumę kontrolną jest zwykle kilka, a podstawiony
 * numer wygląda potem na zweryfikowany i nikt do niego nie wróci. Właściwą
 * poprawką jest odczytanie NIP-u z obrazu.
 */
export const inspectNip = (nip: string | null | undefined): ReceiptCheckIssue | null => {
  const digits = (nip ?? '').replace(/\D/g, '');
  if (!digits) return null;
  if (digits.length !== 10) {
    return {
      level: 'bad',
      code: 'nip_length',
      message: `NIP ma ${digits.length} cyfr zamiast 10`,
      suggestion: null
    };
  }
  if (isValidNip(digits)) return null;
  return {
    level: 'bad',
    code: 'nip_checksum',
    message: `NIP ${digits} nie przechodzi sumy kontrolnej — sprawdź cyfry na paragonie`,
    suggestion: null
  };
};

/**
 * Do czego sumują się pozycje razem z rabatami; `null`, gdy nie da się policzyć.
 *
 *     Σ(wartość pozycji + rabat pozycji) + rabat na paragon = suma
 *
 * `discounts_declared_cents` („udzielono łącznie rabatów") do rachunku nie wchodzi
 * — to suma kontrolna rabatów już przypisanych do pozycji, a nie kolejny rabat.
 */
export const itemsBalanceCents = (label: CheckableLabel): number | null => {
  const items = label.items ?? [];
  if (!items.length) return null;
  if (items.some((item) => item.line_total_cents == null)) return null;

  let balance = items.reduce((sum, item) => sum + (item.line_total_cents ?? 0), 0);
  for (const item of items) {
    if (item.discount_cents) balance -= Math.abs(item.discount_cents);
  }
  if (label.receipt_discount_cents) balance -= Math.abs(label.receipt_discount_cents);
  return balance;
};

/**
 * Zastrzeżenie, gdy pozycje nie domykają się z sumą paragonu.
 *
 * Poziom "warn", nie "bad": rozjazd bywa legalny (rabat, którego nie da się
 * przypisać do pozycji), więc zapisu nie blokuje — w odróżnieniu od NIP-u, gdzie
 * zła suma kontrolna nie ma legalnego wytłumaczenia.
 */
export const inspectItemSum = (
  label: CheckableLabel,
  rules: ReceiptCheckRules = DEFAULT_RULES
): ReceiptCheckIssue | null => {
  const total = label.total_cents;
  const balance = itemsBalanceCents(label);
  if (total == null || balance == null) return null;

  // Bez zaokrąglania — patrz komentarz przy tej samej linii po stronie Pythona.
  const tolerance = Math.max(
    rules.item_sum_tolerance_min_cents,
    Math.abs(total) * rules.item_sum_tolerance_ratio
  );
  const difference = balance - total;
  if (Math.abs(difference) <= tolerance) return null;

  const zloty = (cents: number) => (cents / 100).toFixed(2);
  return {
    level: 'warn',
    code: 'items_sum_mismatch',
    message:
      `pozycje sumują się do ${zloty(balance)}, a suma etykiety to ${zloty(total)} ` +
      `(różnica ${difference > 0 ? '+' : ''}${zloty(difference)}) — rabat niezapisany ` +
      'w polach rabatowych albo błędna wartość którejś pozycji',
    suggestion: null
  };
};

// Braki, których żadna reguła nie wyliczy — trzeba zajrzeć w obraz.
const MISSING_CHECKS: { field: string; code: string; message: string }[] = [
  { field: 'store_name', code: 'missing_store', message: 'brak nazwy sklepu' },
  { field: 'happened_at', code: 'missing_date', message: 'brak daty' },
  { field: 'total_cents', code: 'missing_total', message: 'brak sumy' },
  { field: 'items', code: 'missing_items', message: 'brak pozycji' }
];

const isMissing = (label: CheckableLabel, field: string) => {
  if (field === 'store_name') return !(label.store_name ?? '').trim();
  if (field === 'happened_at') return !datePart(label.happened_at);
  if (field === 'total_cents') return label.total_cents == null;
  return !(label.items ?? []).length;
};

/** Wszystkie zastrzeżenia do etykiety, w kolejności, w jakiej pokazuje je formularz. */
export const labelIssues = (
  label: CheckableLabel,
  scanDate: string | null | undefined,
  rules: ReceiptCheckRules = DEFAULT_RULES
): ReceiptCheckIssue[] => {
  const issues: ReceiptCheckIssue[] = MISSING_CHECKS.filter(({ field }) =>
    isMissing(label, field)
  ).map(({ field, code, message }) => ({ level: 'warn', code, message, field, suggestion: null }));

  const checks: [string, ReceiptCheckIssue | null][] = [
    ['happened_at', inspectDate(label.happened_at, scanDate, rules)],
    ['nip', inspectNip(label.nip)],
    ['items', inspectItemSum(label, rules)]
  ];
  for (const [field, issue] of checks) {
    if (issue) issues.push({ ...issue, field });
  }
  return issues;
};

/** Same zastrzeżenia wymagające świadomej decyzji przed zapisem. */
export const blockingIssues = (issues: ReceiptCheckIssue[]) =>
  issues.filter((issue) => issue.level === 'bad');

/* --- Pole daty ----------------------------------------------------------
 * Tekstowe, a nie `datetime-local`: ten drugi pokazuje datę w formacie ustawień
 * systemu (u nas DD.MM.RRRR) i nie da się tego nadpisać ani atrybutem, ani CSS-em.
 * Przy przepisywaniu z paragonu liczy się, żeby na ekranie stało dokładnie to
 * samo, co na wydruku: RRRR-MM-DD.
 */

const DATE_FIELD_RE =
  /^\s*(\d{4})[-./](\d{1,2})[-./](\d{1,2})(?:[\sT,]+(\d{1,2})[:.](\d{2}))?\s*$/;

const pad2 = (value: string | number) => String(value).padStart(2, '0');

/**
 * Zawartość pola → `2024-06-10T18:04`; `null`, gdy pusto lub nie do odczytania.
 *
 * Wyrozumiałe wobec separatorów, ale rok zawsze idzie pierwszy: `10-06-2024`
 * jest odrzucane zamiast zgadywane, bo przy dacie dwuznacznej lepiej zapytać
 * niż strzelić.
 */
export const parseDateField = (text: string): string | null => {
  const match = DATE_FIELD_RE.exec(text ?? '');
  if (!match) return null;
  const [, year, month, day, hour, minute] = match;
  if (!asDate(Number(year), Number(month), Number(day))) return null;
  const time = hour === undefined ? '00:00' : `${pad2(hour)}:${minute}`;
  return `${year}-${pad2(month)}-${pad2(day)}T${time}`;
};

/** `2024-06-10T18:04` → `2024-06-10 18:04` (tak, jak drukuje paragon). */
export const formatDateField = (isoValue: string | null | undefined) =>
  isoValue ? isoValue.slice(0, 16).replace('T', ' ') : '';

/** Czy w polu stoi coś, czego nie umiemy odczytać jako daty. */
export const isDateFieldGarbled = (text: string) =>
  (text ?? '').trim() !== '' && parseDateField(text) === null;
