// Controller hook for ReportsPage. Owns the entangled filter + data + fetch
// layer: filter state drives fetches, while the fetch-triggering effects read
// filter state, so the two cannot be cleanly separated into sibling hooks.
// Tab-only view state (view toggles, drill-down) lives in the owning tab; the
// inline-edit sub-module lives in useBreakdownEditing.
import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import {
  Account,
  Category,
  Store,
  Transaction,
  getAccounts,
  getAllCategories,
  getAllStores,
  getReportsDashboard,
  getTransactions,
  ReportsDashboardResponse
} from '../../api';
import {
  DEFAULT_QUICK_RANGE,
  QUICK_RANGE_OPTIONS,
  REPORT_TABS,
  SNAKEY_MAX_TRANSACTIONS,
  SNAKEY_PAGE_SIZE,
  UNCATEGORIZED_CATEGORY_ID,
  createInitialRange,
  expandCategoryIdsWithDescendants,
  monthEndDate,
  type QuickRangeId,
  type ReportFilters,
  type ReportTabId
} from './model';

export const useReports = () => {
  const initialRange = useMemo(() => createInitialRange(), []);
  const [startMonth, setStartMonth] = useState(initialRange.start);
  const [endMonth, setEndMonth] = useState(initialRange.end);
  const [activeQuickRange, setActiveQuickRange] = useState<QuickRangeId>(DEFAULT_QUICK_RANGE);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [data, setData] = useState<ReportsDashboardResponse | null>(null);
  const [activeTab, setActiveTab] = useState<ReportTabId>(REPORT_TABS[0].id);
  const [snakeyTransactions, setSnakeyTransactions] = useState<Transaction[]>([]);
  const [snakeyLoading, setSnakeyLoading] = useState(false);
  const [snakeyError, setSnakeyError] = useState<string | null>(null);
  const [snakeyLimitReached, setSnakeyLimitReached] = useState(false);
  const [accounts, setAccounts] = useState<Account[]>([]);
  const [categories, setCategories] = useState<Category[]>([]);
  const [accountCategories, setAccountCategories] = useState<Category[]>([]);
  const [stores, setStores] = useState<Store[]>([]);
  const [selectedAccountIds, setSelectedAccountIds] = useState<string[]>([]);
  const [selectedCategoryIds, setSelectedCategoryIds] = useState<string[]>([]);
  const [pendingAccountIds, setPendingAccountIds] = useState<string[]>([]);
  const [pendingCategoryIds, setPendingCategoryIds] = useState<string[]>([]);
  const [accountSearch, setAccountSearch] = useState('');
  const [categorySearch, setCategorySearch] = useState('');
  const [accountsMenuOpen, setAccountsMenuOpen] = useState(false);
  const [categoriesMenuOpen, setCategoriesMenuOpen] = useState(false);
  const [dateMenuOpen, setDateMenuOpen] = useState(false);
  const [topbarHidden, setTopbarHidden] = useState(false);
  const [topbarHeight, setTopbarHeight] = useState(0);
  const topbarRef = useRef<HTMLElement | null>(null);

  const accountLookup = useMemo(
    () => new Map(accounts.map((account) => [account.id, account])),
    [accounts]
  );
  const storeLookup = useMemo(() => new Map(stores.map((store) => [store.id, store])), [stores]);
  const categoryNameLookup = useMemo(
    () => new Map(categories.map((category) => [category.id, category.name])),
    [categories]
  );
  const allAccountOptionIds = useMemo(() => accounts.map((account) => account.id), [accounts]);
  const allCategoryOptionIds = useMemo(
    () => [UNCATEGORIZED_CATEGORY_ID, ...categories.map((category) => category.id)],
    [categories]
  );

  const fetchReports = useCallback(async (start: string, end: string, filters?: ReportFilters) => {
    try {
      setLoading(true);
      const payload = await getReportsDashboard({
        start_date: `${start}-01`,
        end_date: monthEndDate(end),
        account_ids: filters?.accountIds,
        category_ids: filters?.categoryIds
      });
      setData(payload);
      setError(null);
    } catch (err) {
      console.error(err);
      setError(err instanceof Error ? err.message : 'Nie udało się pobrać raportów.');
    } finally {
      setLoading(false);
    }
  }, []);

  const fetchSnakeyTransactions = useCallback(async (startDate: string, endDate: string) => {
    try {
      setSnakeyLoading(true);
      setSnakeyError(null);
      setSnakeyLimitReached(false);
      const collected: Transaction[] = [];
      let offset = 0;

      while (collected.length < SNAKEY_MAX_TRANSACTIONS) {
        const limit = Math.min(SNAKEY_PAGE_SIZE, SNAKEY_MAX_TRANSACTIONS - collected.length);
        const chunk = await getTransactions({
          start_date: startDate,
          end_date: endDate,
          offset,
          limit
        });
        collected.push(...chunk);
        if (chunk.length < limit) {
          break;
        }
        offset += chunk.length;
      }

      setSnakeyTransactions(collected);
      if (collected.length >= SNAKEY_MAX_TRANSACTIONS) {
        setSnakeyLimitReached(true);
      }
    } catch (err) {
      console.error(err);
      setSnakeyTransactions([]);
      setSnakeyError(err instanceof Error ? err.message : 'Nie udało się pobrać danych do diagramu.');
    } finally {
      setSnakeyLoading(false);
    }
  }, []);

  useEffect(() => {
    let mounted = true;
    const loadFilters = async () => {
      try {
        const [{ accounts: accountsResponse }, categoriesResponse, accountCategoriesResponse, storesResponse] =
          await Promise.all([
            getAccounts(),
            getAllCategories(200, { for_accounts: false }),
            getAllCategories(200, { for_accounts: true }),
            getAllStores(200)
          ]);
        if (!mounted) return;
        setAccounts(
          accountsResponse
            .filter((account) => !account.is_closed)
            .sort((a, b) => a.name.localeCompare(b.name, 'pl'))
        );
        setCategories(
          categoriesResponse.slice().sort((a, b) => a.name.localeCompare(b.name, 'pl'))
        );
        setAccountCategories(
          accountCategoriesResponse.slice().sort((a, b) => a.name.localeCompare(b.name, 'pl'))
        );
        setStores(storesResponse.slice().sort((a, b) => a.name.localeCompare(b.name, 'pl')));
      } catch (err) {
        console.error(err);
      }
    };

    void loadFilters();
    return () => {
      mounted = false;
    };
  }, []);

  useEffect(() => {
    void fetchReports(initialRange.start, initialRange.end, {
      accountIds: selectedAccountIds,
      categoryIds: selectedCategoryIds
    });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [fetchReports, initialRange.end, initialRange.start]);

  useLayoutEffect(() => {
    if (!topbarRef.current) return;

    const element = topbarRef.current;
    const updateHeight = () => setTopbarHeight(element.getBoundingClientRect().height);

    updateHeight();

    let resizeObserver: ResizeObserver | null = null;

    if (typeof ResizeObserver !== 'undefined') {
      resizeObserver = new ResizeObserver(updateHeight);
      resizeObserver.observe(element);
    }
    window.addEventListener('resize', updateHeight);

    return () => {
      if (resizeObserver) {
        resizeObserver.disconnect();
      }
      window.removeEventListener('resize', updateHeight);
    };
  }, []);

  useEffect(() => {
    let lastY = window.scrollY;
    let ticking = false;

    const handleScroll = () => {
      const currentY = window.scrollY;

      if (!ticking) {
        window.requestAnimationFrame(() => {
          const delta = currentY - lastY;
          const scrolledPastBuffer = currentY > 80;

          if (delta > 6 && scrolledPastBuffer) {
            setTopbarHidden(true);
          } else if (delta < -6 || currentY < 40) {
            setTopbarHidden(false);
          }

          lastY = currentY;
          ticking = false;
        });

        ticking = true;
      }
    };

    window.addEventListener('scroll', handleScroll, { passive: true });
    return () => window.removeEventListener('scroll', handleScroll);
  }, []);

  const resolveAccountFilterIds = useCallback(
    (ids: string[]) => {
      if (ids.length === 0 || ids.length === allAccountOptionIds.length) {
        return undefined;
      }
      return ids;
    },
    [allAccountOptionIds]
  );

  const resolveCategoryFilterIds = useCallback(
    (ids: string[]) => {
      if (ids.length === 0 || ids.length === allCategoryOptionIds.length) {
        return undefined;
      }
      // Rollup pod-drzewa: wybór taga-rodzica obejmuje też jego potomków.
      return expandCategoryIdsWithDescendants(ids, categories);
    },
    [allCategoryOptionIds, categories]
  );

  useEffect(() => {
    if (activeTab !== 'snakey') {
      return;
    }

    const startDate = data?.start_date ?? `${startMonth}-01`;
    const endDate = data?.end_date ?? monthEndDate(endMonth);
    void fetchSnakeyTransactions(startDate, endDate);
  }, [activeTab, data, endMonth, fetchSnakeyTransactions, startMonth]);

  const handleQuickRangeSelect = (rangeId: QuickRangeId) => {
    const option = QUICK_RANGE_OPTIONS.find((item) => item.id === rangeId);
    if (!option) return;
    const range = option.getRange(data?.earliest_transaction_date);
    setStartMonth(range.start);
    setEndMonth(range.end);
    setActiveQuickRange(rangeId);
    setError(null);
    setDateMenuOpen(false);
    void fetchReports(range.start, range.end, {
      accountIds: resolveAccountFilterIds(selectedAccountIds),
      categoryIds: resolveCategoryFilterIds(selectedCategoryIds)
    });
  };

  const applyAccountSelection = useCallback(
    (nextIds: string[], options: { closeMenu?: boolean } = {}) => {
      setSelectedAccountIds(nextIds);
      setPendingAccountIds(nextIds);
      void fetchReports(startMonth, endMonth, {
        accountIds: resolveAccountFilterIds(nextIds),
        categoryIds: resolveCategoryFilterIds(selectedCategoryIds)
      });
      if (options.closeMenu) {
        setAccountsMenuOpen(false);
      }
    },
    [
      endMonth,
      fetchReports,
      resolveAccountFilterIds,
      resolveCategoryFilterIds,
      selectedCategoryIds,
      startMonth
    ]
  );

  const togglePendingAccount = (accountId: string) => {
    setPendingAccountIds((prev) =>
      prev.includes(accountId) ? prev.filter((id) => id !== accountId) : [...prev, accountId]
    );
  };

  const handleSelectAllAccounts = () => {
    setPendingAccountIds(allAccountOptionIds);
  };

  const handleClearAllAccounts = () => {
    setPendingAccountIds([]);
  };

  const handleAccountMenuCancel = () => {
    setPendingAccountIds(selectedAccountIds);
    setAccountSearch('');
    setAccountsMenuOpen(false);
  };

  const handleAccountMenuApply = () => {
    applyAccountSelection(pendingAccountIds, { closeMenu: true });
  };

  const applyCategorySelection = useCallback(
    (nextIds: string[], options: { closeMenu?: boolean } = {}) => {
      setSelectedCategoryIds(nextIds);
      setPendingCategoryIds(nextIds);
      void fetchReports(startMonth, endMonth, {
        accountIds: resolveAccountFilterIds(selectedAccountIds),
        categoryIds: resolveCategoryFilterIds(nextIds)
      });
      if (options.closeMenu) {
        setCategoriesMenuOpen(false);
      }
    },
    [
      endMonth,
      fetchReports,
      resolveAccountFilterIds,
      resolveCategoryFilterIds,
      selectedAccountIds,
      startMonth
    ]
  );

  const handleCategoryChipToggle = (categoryId: string) => {
    const next = selectedCategoryIds.includes(categoryId)
      ? selectedCategoryIds.filter((id) => id !== categoryId)
      : [...selectedCategoryIds, categoryId];
    applyCategorySelection(next, { closeMenu: false });
  };

  const togglePendingCategory = (categoryId: string) => {
    setPendingCategoryIds((prev) =>
      prev.includes(categoryId) ? prev.filter((id) => id !== categoryId) : [...prev, categoryId]
    );
  };

  const handleSelectAllCategories = () => {
    setPendingCategoryIds(allCategoryOptionIds);
  };

  const handleClearAllCategories = () => {
    setPendingCategoryIds([]);
  };

  const handleCategoryMenuCancel = () => {
    setPendingCategoryIds(selectedCategoryIds);
    setCategorySearch('');
    setCategoriesMenuOpen(false);
  };

  const handleCategoryMenuApply = () => {
    applyCategorySelection(pendingCategoryIds, { closeMenu: true });
  };

  const handleDateMenuOpenChange = useCallback((nextOpen: boolean) => {
    setDateMenuOpen(nextOpen);
    if (nextOpen) {
      setAccountsMenuOpen(false);
      setCategoriesMenuOpen(false);
    }
  }, []);

  const handleAccountsMenuOpenChange = useCallback(
    (nextOpen: boolean) => {
      setAccountsMenuOpen(nextOpen);
      if (nextOpen) {
        setPendingAccountIds(selectedAccountIds);
        setAccountSearch('');
        setCategoriesMenuOpen(false);
        setDateMenuOpen(false);
      }
    },
    [selectedAccountIds]
  );

  const handleCategoriesMenuOpenChange = useCallback(
    (nextOpen: boolean) => {
      setCategoriesMenuOpen(nextOpen);
      if (nextOpen) {
        setPendingCategoryIds(selectedCategoryIds);
        setCategorySearch('');
        setAccountsMenuOpen(false);
        setDateMenuOpen(false);
      }
    },
    [selectedCategoryIds]
  );

  return {
    // date range
    startMonth,
    endMonth,
    activeQuickRange,
    handleQuickRangeSelect,
    // data + status
    data,
    loading,
    error,
    // tabs
    activeTab,
    setActiveTab,
    // snakey
    snakeyTransactions,
    snakeyLoading,
    snakeyError,
    snakeyLimitReached,
    // reference data
    accounts,
    categories,
    accountCategories,
    stores,
    // lookups
    accountLookup,
    storeLookup,
    categoryNameLookup,
    allAccountOptionIds,
    allCategoryOptionIds,
    // selection state
    selectedAccountIds,
    selectedCategoryIds,
    pendingAccountIds,
    pendingCategoryIds,
    accountSearch,
    setAccountSearch,
    categorySearch,
    setCategorySearch,
    accountsMenuOpen,
    categoriesMenuOpen,
    dateMenuOpen,
    // fetchers
    fetchReports,
    resolveAccountFilterIds,
    resolveCategoryFilterIds,
    // account filter handlers
    togglePendingAccount,
    handleSelectAllAccounts,
    handleClearAllAccounts,
    handleAccountMenuCancel,
    handleAccountMenuApply,
    // category filter handlers
    handleCategoryChipToggle,
    togglePendingCategory,
    handleSelectAllCategories,
    handleClearAllCategories,
    handleCategoryMenuCancel,
    handleCategoryMenuApply,
    // menu open change
    handleDateMenuOpenChange,
    handleAccountsMenuOpenChange,
    handleCategoriesMenuOpenChange,
    // topbar chrome
    topbarRef,
    topbarHeight,
    topbarHidden
  };
};

export type ReportsController = ReturnType<typeof useReports>;
