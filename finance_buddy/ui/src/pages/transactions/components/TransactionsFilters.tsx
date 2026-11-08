// Pasek filtrów: szukaj, zakres dat, konto, kategoria. Zmiany działają od razu; aktywne filtry to chipy.
// Na telefonie zostaje wyszukiwarka i przycisk „Filtry", a reszta siedzi w panelu od dołu.
import { useMemo, useState } from 'react';
import { ListFilter, Search, X } from 'lucide-react';
import type { Account, Category } from '../../../api';
import CheckboxFilterMenu from '../../../components/CheckboxFilterMenu';
import BottomSheet from '../../../components/ui/BottomSheet';
import { DateInput } from '../../../components/ui/DateTimeInput';
import { copy } from '../../../copy/pl';
import { formatShortDate } from '../../../utils/date';
import {
  allFilterIds,
  buildFilterTree,
  categoryFilterOptions,
  selectedCategoryNames,
  toggleCategory,
  UNCATEGORIZED_ID
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
  // „mobile": wyszukiwarka + przycisk otwierający panel z filtrami
  variant?: 'bar' | 'mobile';
};

const TransactionsFilters = ({
  filters,
  onChange,
  searchQuery,
  onSearchChange,
  activeAccounts,
  categories,
  storeLabel,
  accountLocked = false,
  variant = 'bar'
}: TransactionsFiltersProps) => {
  const [preset, setPreset] = useState<DatePreset>('all');
  const [sheetOpen, setSheetOpen] = useState(false);
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
    filters.unclearedOnly
      ? { key: 'uncleared', label: labels.unclearedOnly, onRemove: () => onChange({ ...filters, unclearedOnly: false }) }
      : null,
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

  const clearAll = () => {
    setPreset('all');
    onChange(accountLocked ? { ...NO_FILTERS, accountIds: filters.accountIds } : NO_FILTERS);
  };

  const searchField = (
    <div className="transactions-filters__search">
      <Search size={16} aria-hidden="true" />
      <input
        type="search"
        className="form-control"
        aria-label={labels.searchLabel}
        placeholder={variant === 'mobile' ? labels.searchShort : labels.search}
        value={searchQuery}
        onChange={(event) => onSearchChange(event.target.value)}
      />
    </div>
  );

  const customDates = (
    <>
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
    </>
  );

  const menus = (
    <>
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
    </>
  );

  if (variant === 'mobile') {
    const onlyUncategorized = filters.categoryIds.length === 1 && filters.categoryIds[0] === UNCATEGORIZED_ID;
    return (
      <div className="transactions-filters transactions-filters--mobile">
        <div className="transactions-filters__bar">
          {searchField}
          <button
            type="button"
            className="transactions-filters__sheet-trigger is-focusable"
            aria-haspopup="dialog"
            onClick={() => setSheetOpen(true)}
          >
            <ListFilter size={16} aria-hidden="true" />
            {labels.filters}
            {chips.length > 0 && (
              <span className="transactions-filters__badge" aria-label={labels.activeFilters(chips.length)}>
                {chips.length}
              </span>
            )}
          </button>
        </div>
        {chips.length > 0 && (
          <div className="transactions-filters__chips transactions-filters__chips--scroll">
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
          </div>
        )}
        <BottomSheet
          isOpen={sheetOpen}
          onClose={() => setSheetOpen(false)}
          title={labels.filters}
          closeLabel={labels.closeFilters}
          headerActions={
            <button type="button" className="text-button" onClick={clearAll}>
              {labels.clearFilters}
            </button>
          }
        >
          <div className="transactions-sheet">
            <fieldset className="transactions-sheet__group">
              <legend className="transactions-sheet__label">{labels.dateRange}</legend>
              <div className="transactions-sheet__pills">
                {PRESETS.map((value) => (
                  <button
                    key={value}
                    type="button"
                    className="transactions-sheet__pill"
                    aria-pressed={preset === value}
                    onClick={() => handlePresetChange(value)}
                  >
                    {labels.presets[value]}
                  </button>
                ))}
              </div>
              {preset === 'custom' && <div className="transactions-sheet__dates">{customDates}</div>}
            </fieldset>
            <div className="transactions-sheet__group">
              <span className="transactions-sheet__label">{labels.accountAndCategory}</span>
              {menus}
            </div>
            <div className="transactions-sheet__switches">
              <button
                type="button"
                role="switch"
                aria-checked={onlyUncategorized}
                className="transactions-sheet__switch"
                onClick={() => onChange({ ...filters, categoryIds: onlyUncategorized ? [] : [UNCATEGORIZED_ID] })}
              >
                {labels.onlyUncategorized}
                <span className="transactions-sheet__track" aria-hidden="true" />
              </button>
              <button
                type="button"
                role="switch"
                aria-checked={Boolean(filters.unclearedOnly)}
                className="transactions-sheet__switch"
                onClick={() => onChange({ ...filters, unclearedOnly: !filters.unclearedOnly })}
              >
                {labels.unclearedOnly}
                <span className="transactions-sheet__track" aria-hidden="true" />
              </button>
            </div>
          </div>
        </BottomSheet>
      </div>
    );
  }

  return (
    <div className="transactions-filters">
      <div className="transactions-filters__bar">
        {searchField}
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
        {customDates}
        {menus}
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
            onClick={clearAll}
          >
            {labels.clearFilters}
          </button>
        </div>
      )}
    </div>
  );
};

export default TransactionsFilters;
