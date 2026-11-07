// Pasek filtrów: szukaj, zakres dat, konto, kategoria. Zmiany działają od razu; aktywne filtry to chipy.
import { useMemo, useState } from 'react';
import { Search, X } from 'lucide-react';
import type { Account, Category } from '../../../api';
import CheckboxFilterMenu from '../../../components/CheckboxFilterMenu';
import { DateInput } from '../../../components/ui/DateTimeInput';
import { copy } from '../../../copy/pl';
import { formatShortDate } from '../../../utils/date';
import {
  allFilterIds,
  buildFilterTree,
  categoryFilterOptions,
  selectedCategoryNames,
  toggleCategory
} from '../categoryFilter';
import { datePresetRange, type DatePreset, type FilterState } from '../model';

const labels = copy.transactions;
const PRESETS = Object.keys(labels.presets) as DatePreset[];
const NO_FILTERS: FilterState = { startDate: '', endDate: '', accountIds: [], categoryIds: [] };

type TransactionsFiltersProps = {
  filters: FilterState;
  onChange: (filters: FilterState) => void;
  searchQuery: string;
  onSearchChange: (value: string) => void;
  activeAccounts: Account[];
  categories: Category[];
  storeLabel?: string;
  // Lista stale zawężona do konta: bez wyboru konta, a czyszczenie filtrów go nie zdejmuje
  accountLocked?: boolean;
};

const TransactionsFilters = ({
  filters,
  onChange,
  searchQuery,
  onSearchChange,
  activeAccounts,
  categories,
  storeLabel,
  accountLocked = false
}: TransactionsFiltersProps) => {
  const [preset, setPreset] = useState<DatePreset>('all');
  // Menu kont: wybór roboczy, który trafia do filtrów dopiero po „Zatwierdź"
  const [accountsMenuOpen, setAccountsMenuOpen] = useState(false);
  const [pendingAccountIds, setPendingAccountIds] = useState<string[]>([]);
  const [accountSearch, setAccountSearch] = useState('');
  // Menu kategorii działa tak samo: drzewo, wyszukiwarka i „Zatwierdź"
  const [categoriesMenuOpen, setCategoriesMenuOpen] = useState(false);
  const [pendingCategoryIds, setPendingCategoryIds] = useState<string[]>([]);
  const [categorySearch, setCategorySearch] = useState('');
  const categoryTree = useMemo(() => buildFilterTree(categories), [categories]);
  const categoryOptions = useMemo(
    () => categoryFilterOptions(categoryTree, categorySearch, pendingCategoryIds, labels.uncategorized),
    [categoryTree, categorySearch, pendingCategoryIds]
  );

  const handleCategoriesMenuOpenChange = (nextOpen: boolean) => {
    if (nextOpen) {
      setPendingCategoryIds(filters.categoryIds);
      setCategorySearch('');
    }
    setCategoriesMenuOpen(nextOpen);
  };

  const applyCategories = () => {
    // Wszystkie zaznaczone = bez filtra
    const categoryIds = pendingCategoryIds.length === allFilterIds(categoryTree).length ? [] : pendingCategoryIds;
    setCategoriesMenuOpen(false);
    onChange({ ...filters, categoryIds });
  };

  const categoryNames = selectedCategoryNames(categoryTree, filters.categoryIds, labels.uncategorized);
  const categoriesLabel =
    filters.categoryIds.length === 0
      ? labels.allCategories
      : categoryNames.length === 1
        ? categoryNames[0]
        : labels.selectedCategories(categoryNames.length || filters.categoryIds.length);

  const accountOptions = useMemo(() => {
    const query = accountSearch.trim().toLowerCase();
    return activeAccounts
      .filter((account) => !query || account.name.toLowerCase().includes(query))
      .map((account) => ({ id: account.id, label: account.name }));
  }, [accountSearch, activeAccounts]);

  const handleAccountsMenuOpenChange = (nextOpen: boolean) => {
    if (nextOpen) {
      setPendingAccountIds(filters.accountIds);
      setAccountSearch('');
    }
    setAccountsMenuOpen(nextOpen);
  };

  const applyAccounts = () => {
    // Wszystkie zaznaczone = bez filtra
    const accountIds = pendingAccountIds.length === activeAccounts.length ? [] : pendingAccountIds;
    setAccountsMenuOpen(false);
    onChange({ ...filters, accountIds });
  };

  const accountNames = filters.accountIds
    .map((id) => activeAccounts.find((account) => account.id === id)?.name)
    .filter((name): name is string => Boolean(name));
  const accountsLabel =
    accountNames.length === 0
      ? labels.allAccounts
      : accountNames.length === 1
        ? accountNames[0]
        : labels.selectedAccounts(accountNames.length);

  const handlePresetChange = (next: DatePreset) => {
    setPreset(next);
    if (next !== 'custom') {
      onChange({ ...filters, ...datePresetRange(next) });
    }
  };

  const clearDates = () => {
    setPreset('all');
    onChange({ ...filters, startDate: '', endDate: '' });
  };

  const hasDates = Boolean(filters.startDate || filters.endDate);
  const dateChip =
    preset !== 'custom' && preset !== 'all'
      ? labels.presets[preset]
      : [filters.startDate && formatShortDate(filters.startDate), filters.endDate && formatShortDate(filters.endDate)]
          .filter(Boolean)
          .join(' – ');

  const chips = [
    filters.storeIds?.length
      ? { key: 'store', label: storeLabel ?? labels.storeFilter, onRemove: () => onChange({ ...filters, storeIds: undefined }) }
      : null,
    hasDates ? { key: 'dates', label: dateChip, onRemove: clearDates } : null,
    accountNames.length && !accountLocked
      ? { key: 'account', label: accountsLabel, onRemove: () => onChange({ ...filters, accountIds: [] }) }
      : null,
    filters.categoryIds.length
      ? { key: 'category', label: categoriesLabel, onRemove: () => onChange({ ...filters, categoryIds: [] }) }
      : null
  ].filter((chip) => chip !== null);

  return (
    <div className="transactions-filters">
      <div className="transactions-filters__bar">
        <div className="transactions-filters__search">
          <Search size={16} aria-hidden="true" />
          <input
            type="search"
            className="form-control"
            aria-label={labels.searchLabel}
            placeholder={labels.search}
            value={searchQuery}
            onChange={(event) => onSearchChange(event.target.value)}
          />
        </div>
        <select
          className="form-select"
          aria-label={labels.dateRange}
          value={preset}
          onChange={(event) => handlePresetChange(event.target.value as DatePreset)}
        >
          {PRESETS.map((value) => (
            <option key={value} value={value}>
              {labels.presets[value]}
            </option>
          ))}
        </select>
        {preset === 'custom' && (
          <>
            <DateInput
              className="form-control"
              aria-label={labels.dateFrom}
              value={filters.startDate}
              onChange={(startDate) => onChange({ ...filters, startDate })}
            />
            <DateInput
              className="form-control"
              aria-label={labels.dateTo}
              value={filters.endDate}
              onChange={(endDate) => onChange({ ...filters, endDate })}
            />
          </>
        )}
        {!accountLocked && (
          <CheckboxFilterMenu
            id="transactions-accounts-filter"
            label={labels.account}
            triggerClassName="transactions-filters__menu-trigger"
            triggerLabel={accountsLabel}
            searchPlaceholder={labels.searchAccounts}
            emptyText={labels.noAccountsFound}
            options={accountOptions}
            search={accountSearch}
            onSearchChange={setAccountSearch}
            pendingIds={pendingAccountIds}
            onToggle={(id) =>
              setPendingAccountIds((prev) => (prev.includes(id) ? prev.filter((item) => item !== id) : [...prev, id]))
            }
            onSelectAll={() => setPendingAccountIds(activeAccounts.map((account) => account.id))}
            onClearAll={() => setPendingAccountIds([])}
            onCancel={() => setAccountsMenuOpen(false)}
            onApply={applyAccounts}
            isOpen={accountsMenuOpen}
            onOpenChange={handleAccountsMenuOpenChange}
          />
        )}
        <CheckboxFilterMenu
          id="transactions-categories-filter"
          label={labels.category}
          triggerClassName="transactions-filters__menu-trigger"
          triggerLabel={categoriesLabel}
          searchPlaceholder={labels.searchCategories}
          emptyText={labels.noCategoriesFound}
          options={categoryOptions}
          search={categorySearch}
          onSearchChange={setCategorySearch}
          pendingIds={pendingCategoryIds}
          onToggle={(id) => setPendingCategoryIds((prev) => toggleCategory(categoryTree, prev, id))}
          onSelectAll={() => setPendingCategoryIds(allFilterIds(categoryTree))}
          onClearAll={() => setPendingCategoryIds([])}
          onCancel={() => setCategoriesMenuOpen(false)}
          onApply={applyCategories}
          isOpen={categoriesMenuOpen}
          onOpenChange={handleCategoriesMenuOpenChange}
        />
      </div>

      {chips.length > 0 && (
        <div className="transactions-filters__chips">
          {chips.map((chip) => (
            <button
              key={chip.key}
              type="button"
              className="filter-chip"
              aria-label={labels.removeFilter(chip.label)}
              onClick={chip.onRemove}
            >
              {chip.label}
              <X size={12} aria-hidden="true" />
            </button>
          ))}
          <button
            type="button"
            className="text-button"
            onClick={() => {
              setPreset('all');
              onChange(accountLocked ? { ...NO_FILTERS, accountIds: filters.accountIds } : NO_FILTERS);
            }}
          >
            {labels.clearFilters}
          </button>
        </div>
      )}
    </div>
  );
};

export default TransactionsFilters;
