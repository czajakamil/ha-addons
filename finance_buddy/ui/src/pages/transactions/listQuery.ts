// Czysta logika listy transakcji: zapytanie o stronę, doklejanie kolejnych stron
// i opisy liczników. Filtrowanie i wyszukiwanie robi serwer (/transactions/page),
// więc tu nie ma już żadnego przeszukiwania danych w przeglądarce.
import type { Transaction, TransactionPageParams } from '../../api';
import { pluralPL } from '../../utils/format';
import type { FilterState } from './model';

export const TRANSACTIONS_PAGE_SIZE = 100;
// Wyszukiwarka pyta serwer dopiero po chwili bez pisania, nie po każdym znaku.
export const SEARCH_DEBOUNCE_MS = 300;

export const buildTransactionsPageQuery = (
  filters: FilterState,
  search: string,
  offset = 0
): TransactionPageParams => {
  const needle = search.trim();
  return {
    start_date: filters.startDate || undefined,
    end_date: filters.endDate || undefined,
    account_id: filters.accountId || undefined,
    search: needle || undefined,
    offset,
    limit: TRANSACTIONS_PAGE_SIZE
  };
};

/**
 * Dokleja kolejną stronę do już wczytanych transakcji.
 *
 * Jeśli między stronami ktoś dodał transakcję, przesunięcie `offset` powtórzy
 * ostatni wiersz poprzedniej strony — pomijamy takie duplikaty, bo zdublowany
 * klucz wiersza psuje siatkę.
 */
export const appendTransactionsPage = (
  loaded: readonly Transaction[],
  page: readonly Transaction[]
): Transaction[] => {
  const seen = new Set(loaded.map((tx) => tx.id));
  return [...loaded, ...page.filter((tx) => !seen.has(tx.id))];
};

export const remainingTransactions = (loadedCount: number, total: number) =>
  Math.max(0, total - loadedCount);

export const formatOperationsCount = (count: number) =>
  `${count} ${pluralPL(count, 'operacja', 'operacje', 'operacji')}`;

export const formatLoadMoreLabel = (loadedCount: number, total: number) => {
  const next = Math.min(TRANSACTIONS_PAGE_SIZE, remainingTransactions(loadedCount, total));
  return `Załaduj ${next} ${pluralPL(next, 'następną', 'następne', 'następnych')}`;
};
