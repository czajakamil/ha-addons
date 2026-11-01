// Typy, stałe i czyste helpery strony kont — bez stanu i bez JSX.
import type { KeyboardEvent as ReactKeyboardEvent } from 'react';
import { Account, AccountKind } from '../../api';

export const currencyOptions = [
  { code: 'PLN', name: 'Polski złoty' },
  { code: 'USD', name: 'Dolar amerykański' },
  { code: 'EUR', name: 'Euro' },
  { code: 'GBP', name: 'Funt szterling' },
  { code: 'CHF', name: 'Frank szwajcarski' },
  { code: 'CAD', name: 'Dolar kanadyjski' },
  { code: 'AUD', name: 'Dolar australijski' },
  { code: 'JPY', name: 'Jen japoński' },
  { code: 'CZK', name: 'Korona czeska' },
  { code: 'NOK', name: 'Korona norweska' },
  { code: 'SEK', name: 'Korona szwedzka' },
  { code: 'DKK', name: 'Korona duńska' },
  { code: 'HUF', name: 'Forint węgierski' },
  { code: 'RUB', name: 'Rubel rosyjski' },
  { code: 'UAH', name: 'Hrywna ukraińska' },
  { code: 'CNY', name: 'Juan chiński' },
  { code: 'INR', name: 'Rupia indyjska' },
  { code: 'BRL', name: 'Real brazylijski' },
  { code: 'MXN', name: 'Peso meksykańskie' },
  { code: 'ZAR', name: 'Rand południowoafrykański' }
];

export type EditableField = 'name' | 'currency' | 'comment';

export type EditingCell = {
  accountId: string;
  field: EditableField;
};

export const ACCOUNT_COLUMN_KEYS = ['name', 'category', 'currency', 'balance', 'kind', 'budget', 'comment'] as const;
export type AccountColumn = (typeof ACCOUNT_COLUMN_KEYS)[number];

export const isAccountColumnKey = (value: string): value is AccountColumn =>
  ACCOUNT_COLUMN_KEYS.includes(value as AccountColumn);

export const isEditableFieldColumn = (column: AccountColumn): column is EditableField =>
  column === 'name' || column === 'currency' || column === 'comment';

const ACCOUNT_GRID_MIN_HEIGHT = 260;
const ACCOUNT_GRID_MAX_HEIGHT = 720;
export const ACCOUNT_GRID_ROW_HEIGHT = 52;
export const ACCOUNT_GRID_HEADER_HEIGHT = 44;

export const getAccountsGridHeight = (rowCount: number) => {
  if (rowCount <= 0) {
    return ACCOUNT_GRID_MIN_HEIGHT;
  }
  const estimated = ACCOUNT_GRID_HEADER_HEIGHT + 16 + ACCOUNT_GRID_ROW_HEIGHT * rowCount;
  return Math.min(ACCOUNT_GRID_MAX_HEIGHT, Math.max(ACCOUNT_GRID_MIN_HEIGHT, estimated));
};

export type BudgetLabelValue = 'on' | 'off';

export const normalizeAccountKind = (value?: string | null) =>
  (value ?? '').trim().toLowerCase().replace(/\s+/g, '').replace(/_/g, '');

export type AccountKindOption = { value: AccountKind; label: string };

// Typ konta niesie tylko jedną oś znaczenia: aktywo / kredyt odnawialny /
// zobowiązanie. `creditcard` uruchamia specjalną logikę budżetu (kategoria
// spłaty, overspent_credit); `liability` to strona długów net worth; `cash`
// zbiera wszystkie aktywa. Rozróżnianie konta osobistego, oszczędnościowego
// czy gotówki to organizacja — od tego są nazwa i tagi.
export const ACCOUNT_KIND_OPTIONS: AccountKindOption[] = [
  { value: 'cash', label: 'Aktywo' },
  { value: 'creditcard', label: 'Karta kredytowa' },
  { value: 'liability', label: 'Zobowiązanie' }
];

const ACCOUNT_KIND_LABELS = new Map<string, string>(
  ACCOUNT_KIND_OPTIONS.map((option) => [normalizeAccountKind(option.value), option.label])
);

export const getAccountKindLabel = (value?: string | null) => {
  const normalized = normalizeAccountKind(value);
  if (!normalized) {
    return 'Nieznane';
  }
  return ACCOUNT_KIND_LABELS.get(normalized) ?? value ?? 'Nieznane';
};

export const isCreditAccountKind = (value?: string | null) =>
  normalizeAccountKind(value) === 'creditcard';

// Konta, dla których ma sens widok długu: oprocentowanie, rata i harmonogram.
// Karty kredytowe świadomie NIE — YNAB nie wypełnia dla nich pól długu, mają
// za to własną ścieżkę z kategorią spłaty.
export const isLiabilityAccountKind = (value?: string | null) =>
  normalizeAccountKind(value) === 'liability';

export type SortOption = 'name_asc' | 'name_desc' | 'balance_asc' | 'balance_desc';

export const SORT_OPTIONS: { value: SortOption; label: string }[] = [
  { value: 'name_asc', label: 'Alfabetycznie (A-Z)' },
  { value: 'name_desc', label: 'Alfabetycznie (Z-A)' },
  { value: 'balance_desc', label: 'Saldo malejąco' },
  { value: 'balance_asc', label: 'Saldo rosnąco' }
];

export const NAVIGATION_KEYS = new Set<string>([
  'ArrowLeft',
  'ArrowRight',
  'ArrowUp',
  'ArrowDown',
  'Home',
  'End',
  'PageUp',
  'PageDown'
]);

export const stopNavigationKeyPropagation = (
  event: ReactKeyboardEvent<HTMLInputElement | HTMLTextAreaElement>
) => {
  if (NAVIGATION_KEYS.has(event.key)) {
    // Zatrzymaj event zanim dotrze do react-data-grid
    event.stopPropagation();
  }
};

export const focusClosestGridCell = (target: EventTarget | null) => {
  if (!(target instanceof HTMLElement)) return;
  const cell = target.closest<HTMLElement>('.rdg-cell, [role="gridcell"]');
  cell?.focus();
};

export type BalanceAdjustmentModalState = {
  account: Account;
  targetBalanceCents: number;
};

// Znormalizowane porównywanie ścieżek kategorii ("A / B" ~ "a/b") przy wyszukiwaniu.
export const normalizeCategoryLabel = (value: string) =>
  value.replace(/\s*\/\s*/g, '/').trim().toLowerCase();
