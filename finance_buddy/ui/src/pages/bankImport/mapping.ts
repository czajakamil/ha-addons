/**
 * Czysta logika mapowania kolumn wyciągu CSV — bez Reacta.
 *
 * Tu NIE parsujemy kwot ani dat: robi to wyłącznie backend
 * (services/bank_csv.py), żeby podgląd nie pokazał czegoś innego, niż trafi
 * do bazy. Projekt: docs/design/import-csv.md §8.
 */

export type ColumnRole =
  | 'ignore'
  | 'date'
  | 'amount'
  | 'credit'
  | 'debit'
  | 'payee'
  | 'memo'
  | 'currency'
  | 'fee'
  | 'status'
  | 'external_id';

export type AmountMode = 'signed' | 'signed_inverted' | 'split';

export type RowStatus =
  | 'new'
  | 'already_imported'
  | 'match'
  | 'transfer'
  | 'other_currency'
  | 'skipped'
  | 'invalid';

export type RowAction = 'create' | 'merge' | 'transfer' | 'skip';

export interface Column {
  index: number;
  name: string;
  samples: string[];
}

export interface Mapping {
  /** Rola po INDEKSIE kolumny (ING ma cztery kolumny „Waluta"). */
  columns: Record<string, ColumnRole>;
  amount_mode: AmountMode;
  status_accepted: string[];
}

export interface PreviewRow {
  index: number;
  status: RowStatus;
  amount_cents?: number | null;
  suggested_action: RowAction;
  match?: { transaction_id: string; account_id: string } | null;
}

export interface RowOverride {
  action: RowAction;
  transfer_account_id?: string | null;
}

export interface Decision {
  row: number;
  action: RowAction;
  target_transaction_id?: string | null;
  transfer_account_id?: string | null;
}

// Kolejność wzorców ma znaczenie: pierwszy pasujący wygrywa.
const ROLE_PATTERNS: Array<[ColumnRole, RegExp]> = [
  ['external_id', /(nr|numer|id)\s*transakcji|transaction\s*id|reference/],
  ['fee', /^(fee|opłata|prowizja)/],
  ['credit', /uznani|wpływ|credit|money\s*in/],
  ['debit', /obciążeni|wypływ|debit|money\s*out/],
  ['amount', /^kwota(?!.*blokad)|^amount|^wartość/],
  ['date', /^data(?!.*wygenerowania)|date$|^date|^dzień/],
  ['currency', /^waluta|^currency/],
  ['status', /^(state|status|stan)$/],
  ['payee', /kontrahent|odbiorc|nadawc|payee|merchant|^description$|^opis$/],
  ['memo', /tytuł|title|szczegóły|details|^type$|^typ$|memo|notatka/],
];

const SINGLE_ROLES: ColumnRole[] = [
  'date',
  'amount',
  'credit',
  'debit',
  'payee',
  'currency',
  'fee',
  'status',
  'external_id',
];

const normalize = (name: string) => name.trim().toLowerCase().replace(/\s+/g, ' ');

/** Podpowiedź ról z nazw kolumn. Dla ról pojedynczych wygrywa pierwsza kolumna. */
export function suggestMapping(columns: Column[]): Mapping {
  const result: Record<string, ColumnRole> = {};
  const taken = new Set<ColumnRole>();
  for (const column of columns) {
    const name = normalize(column.name);
    if (!name) continue;
    const hit = ROLE_PATTERNS.find(([, re]) => re.test(name));
    if (!hit) continue;
    const [role] = hit;
    if (SINGLE_ROLES.includes(role) && taken.has(role)) continue;
    taken.add(role);
    result[String(column.index)] = role;
  }
  const hasSplit = taken.has('credit') || taken.has('debit');
  const amount_mode: AmountMode = !taken.has('amount') && hasSplit ? 'split' : 'signed';
  const statusIndex = Object.entries(result).find(([, r]) => r === 'status')?.[0];
  const statusSamples = statusIndex
    ? columns.find((c) => String(c.index) === statusIndex)?.samples ?? []
    : [];
  const status_accepted = statusSamples.some((s) => s.toUpperCase() === 'COMPLETED')
    ? ['COMPLETED']
    : [];
  return { columns: result, amount_mode, status_accepted };
}

/** Błędy mapowania — te same reguły co `Mapping.errors` w backendzie. */
export function validateMapping(mapping: Mapping): string[] {
  const counts = new Map<ColumnRole, number>();
  for (const role of Object.values(mapping.columns)) {
    if (role === 'ignore') continue;
    counts.set(role, (counts.get(role) ?? 0) + 1);
  }
  const errors: string[] = [];
  for (const [role, count] of counts) {
    if (count > 1 && SINGLE_ROLES.includes(role)) {
      errors.push(`Rola „${role}” przypisana do więcej niż jednej kolumny.`);
    }
  }
  if (!counts.has('date')) errors.push('Wskaż kolumnę z datą.');
  if (mapping.amount_mode === 'split') {
    if (!counts.has('credit') && !counts.has('debit')) {
      errors.push('Wskaż kolumny uznań i obciążeń.');
    }
  } else if (!counts.has('amount')) {
    errors.push('Wskaż kolumnę z kwotą.');
  }
  return errors;
}

const IMPORTABLE: RowStatus[] = ['new', 'match', 'transfer'];

/** Decyzje do zapisu: sugestia serwera, chyba że użytkownik ją zmienił. */
export function buildDecisions(
  rows: PreviewRow[],
  overrides: Record<number, RowOverride> = {},
): Decision[] {
  const decisions: Decision[] = [];
  for (const row of rows) {
    if (!IMPORTABLE.includes(row.status)) continue;
    const override = overrides[row.index];
    const action = override?.action ?? row.suggested_action;
    if (action === 'skip') continue;
    const decision: Decision = { row: row.index, action };
    if (action === 'merge' && row.match) decision.target_transaction_id = row.match.transaction_id;
    if (action === 'transfer') {
      decision.transfer_account_id =
        override?.transfer_account_id ?? (row.status === 'transfer' ? row.match?.account_id : null);
    }
    decisions.push(decision);
  }
  return decisions;
}

export interface Summary {
  create: number;
  merge: number;
  transfer: number;
  skip: number;
  totalCents: number;
}

/** Liczniki i suma do paska podsumowania (suma = tylko nowe transakcje). */
export function summarize(rows: PreviewRow[], decisions: Decision[]): Summary {
  const byRow = new Map(decisions.map((d) => [d.row, d]));
  const summary: Summary = { create: 0, merge: 0, transfer: 0, skip: 0, totalCents: 0 };
  for (const row of rows) {
    if (row.status === 'skipped' || row.status === 'invalid') continue;
    const decision = byRow.get(row.index);
    if (!decision) {
      summary.skip += 1;
      continue;
    }
    summary[decision.action] += 1;
    if (decision.action !== 'merge') summary.totalCents += row.amount_cents ?? 0;
  }
  return summary;
}
