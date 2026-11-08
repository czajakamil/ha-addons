// Logika listy transakcji: zapytanie o stronę, doklejanie kolejnych stron i opisy
// liczników. Filtrowanie i wyszukiwanie robi serwer, więc tu nie ma żadnego
// przeszukiwania danych w przeglądarce.
import { getTransactionsPage, type Transaction, type TransactionPageParams } from '../../api';
import { pluralPL } from '../../utils/format';
import type { FilterState } from './model';

export const TRANSACTIONS_PAGE_SIZE = 100;
// Wyszukiwarka pyta serwer dopiero po chwili bez pisania, nie po każdym znaku.
export const SEARCH_DEBOUNCE_MS = 300;

export const fetchTransactionsPage = async (
  filters: FilterState,
  search: string,
  offset = 0
): Promise<{ items: Transaction[]; total: number | null }> => {
  const page = await getTransactionsPage(buildTransactionsPageQuery(filters, search, offset));
  return { items: page.items, total: page.total };
};

export const buildTransactionsPageQuery = (
  filters: FilterState,
  search: string,
  offset = 0
): TransactionPageParams => {
  const needle = search.trim();
  return {
    start_date: filters.startDate || undefined,
    end_date: filters.endDate || undefined,
    account_ids: filters.accountIds.length ? filters.accountIds : undefined,
    store_ids: filters.storeIds?.length ? filters.storeIds : undefined,
    category_ids: filters.categoryIds.length ? filters.categoryIds : undefined,
    search: needle || undefined,
    uncleared_only: filters.unclearedOnly || undefined,
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

export const formatOperationsCount = (count: number) =>
  `${count} ${pluralPL(count, 'operacja', 'operacje', 'operacji')}`;
