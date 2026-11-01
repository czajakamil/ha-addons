// Logika listy transakcji: zapytanie o stronę, doklejanie kolejnych stron i opisy
// liczników. Filtrowanie i wyszukiwanie robi serwer, więc tu nie ma żadnego
// przeszukiwania danych w przeglądarce.
import { getTransactions, getTransactionsPage, type Transaction, type TransactionPageParams } from '../../api';
import { pluralPL } from '../../utils/format';
import type { FilterState } from './model';

export const TRANSACTIONS_PAGE_SIZE = 100;
// Wyszukiwarka pyta serwer dopiero po chwili bez pisania, nie po każdym znaku.
export const SEARCH_DEBOUNCE_MS = 300;

// /transactions/page nie filtruje po kategorii. Z filtrem kategorii lista idzie przez
// /transactions, który nie zwraca liczby wszystkich wyników; `total` jest wtedy znany
// dopiero po wczytaniu ostatniej, niepełnej strony.
export const fetchTransactionsPage = async (
  filters: FilterState,
  search: string,
  offset = 0
): Promise<{ items: Transaction[]; total: number | null }> => {
  const query = buildTransactionsPageQuery(filters, search, offset);
  if (!filters.categoryId) {
    const page = await getTransactionsPage(query);
    return { items: page.items, total: page.total };
  }
  const items = await getTransactions({ ...query, category_id: filters.categoryId });
  return { items, total: items.length < TRANSACTIONS_PAGE_SIZE ? offset + items.length : null };
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

export const formatOperationsCount = (count: number) =>
  `${count} ${pluralPL(count, 'operacja', 'operacje', 'operacji')}`;
