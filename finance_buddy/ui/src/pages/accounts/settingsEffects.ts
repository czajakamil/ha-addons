// Opis skutku zmian, które wymagają potwierdzenia. Teksty wynikają z tego, jak
// liczy backend: Do przypisania to saldo kont budżetowych minus dostępne środki
// (services/budget.py), a PATCH waluty nie przelicza kwot ani transakcji.
import type { Account } from '../../api';
import { formatMoney } from '../../utils/format';
import { MAIN_CURRENCY, normalizeAccountKind } from './model';

export const describeBudgetStatusChange = (account: Account, nextOnBudget: boolean) => {
  const amount = formatMoney(Math.abs(account.balance_cents), account.currency);
  const raises = nextOnBudget === account.balance_cents > 0;
  const direct = account.balance_cents === 0 ? 'Saldo konta to 0, więc samo saldo nie zmieni Do przypisania.'
    : `Do przypisania ${raises ? 'zwiększy' : 'zmniejszy'} się o ${amount}.`;
  return nextOnBudget
    ? `${direct} Wydatki z tego konta zaczną obciążać kategorie budżetu, co może dodatkowo zmienić tę kwotę.`
    : `${direct} Wydatki z tego konta przestaną obciążać kategorie budżetu, co może dodatkowo zmienić tę kwotę.`;
};

export const describeCurrencyChange = (account: Account, nextCurrency: string) => {
  const from = formatMoney(account.balance_cents, account.currency);
  const to = formatMoney(account.balance_cents, nextCurrency);
  const sum =
    nextCurrency === MAIN_CURRENCY
      ? 'Konto wejdzie do sumy grupy na liście kont.'
      : account.currency === MAIN_CURRENCY
        ? 'Konto wypadnie z sumy grupy na liście kont.'
        : '';
  return `Kwoty nie zostaną przeliczone: saldo ${from} będzie pokazywane jako ${to}. Istniejące transakcje zachowają swoją walutę, a import CSV będzie przyjmował wiersze w ${nextCurrency}. ${sum}`.trim();
};

export const describeKindChange = (account: Account, nextKind: string) => {
  const wasCard = normalizeAccountKind(account.account_kind) === 'creditcard';
  const next = normalizeAccountKind(nextKind);
  if (next === 'creditcard') {
    return account.is_on_budget
      ? 'Budżet założy kategorię spłaty tej karty, a wydatki z niej będą przesuwać środki do tej kategorii.'
      : 'Konto jest śledzone, więc budżet się nie zmieni. Kategoria spłaty powstanie dopiero, gdy konto stanie się budżetowe.';
  }
  const base = wasCard ? 'Wydatki z konta przestaną zasilać kategorię spłaty karty. ' : '';
  return next === 'liability'
    ? `${base}Konto zostanie zobowiązaniem, a w menu na liście kont pojawią się szczegóły długu.`
    : `${base}Konto zostanie aktywem.`;
};
