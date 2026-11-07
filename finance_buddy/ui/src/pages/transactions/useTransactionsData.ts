// Warstwa danych strony transakcji: bootstrap słowników (konta/sklepy/kategorie)
// oraz stronicowana lista transakcji. Filtry i wyszukiwanie wykonuje serwer —
// lista pokazuje kolejne strony („Załaduj więcej”), a licznik całość pasujących.
// Celowo nie używa useApi — fetcher listy przyjmuje argumenty filtrów.
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  Account,
  Category,
  Item,
  Store,
  Transaction,
  getAccounts,
  getAllCategories,
  getAllItems,
  getAllStores
} from '../../api';
import { type EditableLookupField, type FilterState } from './model';
import { SEARCH_DEBOUNCE_MS, appendTransactionsPage, fetchTransactionsPage } from './listQuery';

const EMPTY_FILTERS: FilterState = { startDate: '', endDate: '', accountIds: [], categoryId: '' };

// `scopedAccountId`: lista na stronie konta, stale zawężona do tego konta (także zarchiwizowanego).
export const useTransactionsData = (scopedAccountId?: string) => {
  const [accounts, setAccounts] = useState<Account[]>([]);
  const [stores, setStores] = useState<Store[]>([]);
  const [categories, setCategories] = useState<Category[]>([]);
  // Produkty są potrzebne dopiero po rozwinięciu transakcji, ale lista zwraca
  // pozycje z samym product_id — bez słownika nie ma czego pokazać.
  const [products, setProducts] = useState<Item[]>([]);
  const [transactions, setTransactions] = useState<Transaction[]>([]);
  // Liczba wszystkich pasujących transakcji; null, gdy nie jest jeszcze znana (filtr kategorii).
  const [totalCount, setTotalCount] = useState<number | null>(0);
  // Szczegóły konta, sklepu i kategorii prowadzą tu z gotowym filtrem (?account=, ?stores=, ?category=)
  const [initialFilters] = useState<FilterState>(() => {
    const params = new URLSearchParams(window.location.search);
    const storeIds = scopedAccountId ? [] : (params.get('stores') ?? '').split(',').filter(Boolean);
    return {
      ...EMPTY_FILTERS,
      accountIds: [scopedAccountId ?? params.get('account')].filter((id): id is string => Boolean(id)),
      categoryId: scopedAccountId ? '' : params.get('category') ?? '',
      ...(storeIds.length ? { storeIds } : {})
    };
  });
  const [filters, setFilters] = useState<FilterState>(initialFilters);
  const [searchQuery, setSearchQuery] = useState('');
  const [loading, setLoading] = useState(true);
  // Odświeżanie przy już pokazanej liście (fraza, filtry) — siatka zostaje na miejscu.
  const [refreshing, setRefreshing] = useState(false);
  const [loadingMore, setLoadingMore] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Ostatnio zastosowane filtry i fraza, dla doładowania kolejnych stron.
  const appliedFiltersRef = useRef<FilterState>(EMPTY_FILTERS);
  const appliedSearchRef = useRef('');
  const loadedCountRef = useRef(0);
  // Numer ostatniego zapytania — odpowiedź na starsze (np. wolniejsze, z
  // poprzednią frazą) nie może nadpisać nowszej.
  const requestSeqRef = useRef(0);

  useEffect(() => {
    loadedCountRef.current = transactions.length;
  }, [transactions]);

  const fetchFirstPage = useCallback(async (currentFilters: FilterState, search: string) => {
    const seq = ++requestSeqRef.current;
    appliedFiltersRef.current = currentFilters;
    appliedSearchRef.current = search;
    try {
      setRefreshing(true);
      const page = await fetchTransactionsPage(currentFilters, search);
      if (seq !== requestSeqRef.current) return;
      setTransactions(page.items);
      setTotalCount(page.total);
      setError(null);
    } catch (err) {
      if (seq !== requestSeqRef.current) return;
      console.error(err);
      setError('Nie udało się wczytać transakcji.');
    } finally {
      if (seq === requestSeqRef.current) setRefreshing(false);
    }
  }, []);

  const loadTransactions = useCallback(
    (currentFilters: FilterState) => fetchFirstPage(currentFilters, appliedSearchRef.current),
    [fetchFirstPage]
  );

  // Filtry działają od razu, bez przycisku.
  const applyFilters = useCallback(
    (next: FilterState) => {
      setFilters(next);
      void fetchFirstPage(next, appliedSearchRef.current);
    },
    [fetchFirstPage]
  );

  const loadMoreTransactions = useCallback(async () => {
    const seq = ++requestSeqRef.current;
    try {
      setLoadingMore(true);
      const page = await fetchTransactionsPage(
        appliedFiltersRef.current,
        appliedSearchRef.current,
        loadedCountRef.current
      );
      if (seq !== requestSeqRef.current) return;
      setTransactions((prev) => appendTransactionsPage(prev, page.items));
      setTotalCount(page.total);
    } catch (err) {
      if (seq !== requestSeqRef.current) return;
      console.error(err);
      setError('Nie udało się wczytać kolejnych transakcji.');
    } finally {
      setLoadingMore(false);
    }
  }, []);

  // Wyszukiwarka pyta serwer z opóźnieniem; pierwsze wczytanie robi bootstrap.
  useEffect(() => {
    if (searchQuery.trim() === appliedSearchRef.current.trim()) return;
    const timer = window.setTimeout(() => {
      void fetchFirstPage(appliedFiltersRef.current, searchQuery);
    }, SEARCH_DEBOUNCE_MS);
    return () => window.clearTimeout(timer);
  }, [searchQuery, fetchFirstPage]);

  useEffect(() => {
    const bootstrap = async () => {
      try {
        setLoading(true);
        const [{ accounts: accountsData }, storesData, categoriesData, productsData] =
          await Promise.all([
            getAccounts(),
            getAllStores(),
            getAllCategories(200, { for_accounts: false }),
            getAllItems()
          ]);
        setAccounts(accountsData);
        setStores(storesData);
        setCategories(categoriesData);
        setProducts(productsData);
        await loadTransactions(initialFilters);
        setError(null);
      } catch (err) {
        console.error(err);
        setError('Nie udało się zainicjalizować danych.');
      } finally {
        setLoading(false);
      }
    };

    void bootstrap();
  }, [initialFilters, loadTransactions]);

  const activeAccounts = useMemo(() => accounts.filter((account) => !account.is_closed), [accounts]);
  const accountMap = useMemo(() => new Map(accounts.map((account) => [account.id, account])), [accounts]);
  const storeMap = useMemo(() => new Map(stores.map((store) => [store.id, store])), [stores]);
  const categoryMap = useMemo(() => new Map(categories.map((category) => [category.id, category])), [categories]);
  const productMap = useMemo(() => new Map(products.map((product) => [product.id, product])), [products]);

  // Konta wybrane w filtrze mogły zostać zamknięte — pomijamy je (wyliczenie zamiast
  // synchronizowania stanu w efekcie).
  const effectiveFilters = useMemo(() => {
    if (!filters.accountIds.length || scopedAccountId || !accounts.length) {
      return filters;
    }
    const activeIds = new Set(activeAccounts.map((account) => account.id));
    const accountIds = filters.accountIds.filter((id) => activeIds.has(id));
    return accountIds.length === filters.accountIds.length ? filters : { ...filters, accountIds };
  }, [filters, accounts.length, activeAccounts, scopedAccountId]);

  const formatTransferLabel = useCallback(
    (tx: Transaction) => {
      if (!tx.transfer_account_id) return null;
      const counterparty = accountMap.get(tx.transfer_account_id);
      if (!counterparty) return 'Przelew';
      const currencyLabel = counterparty.currency ? ` (${counterparty.currency})` : '';
      return `Przelew: ${counterparty.name}${currencyLabel}`;
    },
    [accountMap]
  );

  const getLookupDisplayValue = useCallback(
    (tx: Transaction, field: EditableLookupField): string => {
      if (field === 'account') {
        return accountMap.get(tx.account_id)?.name ?? '';
      }
      if (field === 'store') {
        const transferLabel = formatTransferLabel(tx);
        if (transferLabel) return transferLabel;
        const store = tx.store_id ? storeMap.get(tx.store_id) : undefined;
        if (!store) return '';
        const group = (store.group_name ?? '').trim();
        const name = (store.name ?? '').trim();
        return group || name;
      }
      if (!tx.category_id) return '';
      return categoryMap.get(tx.category_id)?.name ?? '';
    },
    [accountMap, storeMap, categoryMap, formatTransferLabel]
  );

  return {
    accounts,
    stores,
    categories,
    products,
    setProducts,
    transactions,
    setTransactions,
    totalCount,
    filters: effectiveFilters,
    applyFilters,
    searchQuery,
    setSearchQuery,
    loading,
    refreshing,
    loadingMore,
    error,
    loadTransactions,
    loadMoreTransactions,
    activeAccounts,
    accountMap,
    storeMap,
    categoryMap,
    productMap,
    formatTransferLabel,
    getLookupDisplayValue
  };
};

export type TransactionsData = ReturnType<typeof useTransactionsData>;
