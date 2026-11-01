// Typy, stałe i czyste helpery strony kont — bez stanu i bez JSX.
import { Account, AccountKind } from '../../api';

// Kolejność w selekcie: najczęstsze na górze, reszta alfabetycznie.
export const TOP_CURRENCIES = ['PLN', 'EUR', 'USD'];

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

export type BudgetLabelValue = 'on' | 'off';

export const normalizeAccountKind = (value?: string | null) =>
  (value ?? '').trim().toLowerCase().replace(/\s+/g, '').replace(/_/g, '');

export type AccountKindOption = { value: AccountKind; label: string; description: string };

// Typ konta niesie tylko jedną oś znaczenia: aktywo / kredyt odnawialny /
// zobowiązanie. `creditcard` uruchamia specjalną logikę budżetu (kategoria
// spłaty, overspent_credit); `liability` to strona długów net worth; `cash`
// zbiera wszystkie aktywa. Rozróżnianie konta osobistego, oszczędnościowego
// czy gotówki to organizacja — od tego są nazwa i tagi.
export const ACCOUNT_KIND_OPTIONS: AccountKindOption[] = [
  { value: 'cash', label: 'Aktywo', description: 'Pieniądze, które masz: rachunek, oszczędności, gotówka.' },
  { value: 'creditcard', label: 'Karta kredytowa', description: 'Kredyt odnawialny: budżet śledzi kategorię spłaty.' },
  { value: 'liability', label: 'Zobowiązanie', description: 'Dług: kredyt lub pożyczka, z widokiem rat i harmonogramu.' }
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

// Znormalizowane porównywanie ścieżek kategorii ("A / B" ~ "a/b") przy wyszukiwaniu.
export const normalizeCategoryLabel = (value: string) =>
  value.replace(/\s*\/\s*/g, '/').trim().toLowerCase();

const DORMANT_AFTER_DAYS = 90;
export const MAIN_CURRENCY = 'PLN';

/** Konto z zerowym saldem i bez ruchu od 90 dni. Brak daty oznacza, że aktywność jeszcze się wczytuje. */
export const isDormantAccount = (account: Account, lastActivity: string | null | undefined) => {
  if (account.balance_cents !== 0 || lastActivity === undefined) return false;
  if (lastActivity === null) return true;
  return Date.now() - Date.parse(lastActivity) > DORMANT_AFTER_DAYS * 86_400_000;
};

// Suma grupy obejmuje konta w głównej walucie; kursów nie przeliczamy.
export const sumInMainCurrency = (accounts: Account[]) =>
  accounts
    .filter((account) => account.currency === MAIN_CURRENCY)
    .reduce((sum, account) => sum + account.balance_cents, 0);
