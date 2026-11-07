import { CSSProperties, useCallback, useMemo, useState } from 'react';
import { ChevronDown, SlidersHorizontal } from 'lucide-react';
import CheckboxFilterMenu, { type CheckboxFilterOption } from '../../../components/CheckboxFilterMenu';
import PopoverFilter from '../../../components/PopoverFilter';
import { useMediaQuery } from '../../../hooks/useMediaQuery';
import {
  QUICK_RANGE_OPTIONS,
  UNCATEGORIZED_CATEGORY_ID,
  UNCATEGORIZED_CATEGORY_NAME,
  quickRangeSummary,
  stripParentPrefix,
  type CategoryMenuEntry,
  type CategoryTreeNode
} from '../model';
import type { ReportsController } from '../useReports';
import ReportsFiltersSheet from './ReportsFiltersSheet';
import ReportsTabBar from './ReportsTabBar';

type ReportsTopbarProps = {
  controller: ReportsController;
};

const ReportsTopbar = ({ controller }: ReportsTopbarProps) => {
  const {
    topbarRef,
    topbarHeight,
    topbarHidden,
    activeTab,
    setActiveTab,
    startMonth,
    endMonth,
    activeQuickRange,
    handleQuickRangeSelect,
    dateMenuOpen,
    handleDateMenuOpenChange,
    accounts,
    categories,
    selectedAccountIds,
    selectedCategoryIds,
    allCategoryOptionIds,
    pendingAccountIds,
    pendingCategoryIds,
    accountSearch,
    setAccountSearch,
    categorySearch,
    setCategorySearch,
    accountsMenuOpen,
    categoriesMenuOpen,
    handleAccountsMenuOpenChange,
    handleCategoriesMenuOpenChange,
    togglePendingAccount,
    handleSelectAllAccounts,
    handleClearAllAccounts,
    handleAccountMenuCancel,
    handleAccountMenuApply,
    togglePendingCategory,
    handleSelectAllCategories,
    handleClearAllCategories,
    handleCategoryMenuCancel,
    handleCategoryMenuApply
  } = controller;

  const isCompact = useMediaQuery('(max-width: 699px)');
  const activeRangeLabel = QUICK_RANGE_OPTIONS.find((option) => option.id === activeQuickRange)?.label ?? '';
  const [filtersSheetOpen, setFiltersSheetOpen] = useState(false);
  const hasAccountFilter = selectedAccountIds.length > 0 && selectedAccountIds.length !== accounts.length;
  const hasCategoryFilter =
    selectedCategoryIds.length > 0 && selectedCategoryIds.length !== allCategoryOptionIds.length;
  const activeFilterCount = Number(hasAccountFilter) + Number(hasCategoryFilter);
  const closeFiltersSheet = () => {
    handleAccountsMenuOpenChange(false);
    handleCategoriesMenuOpenChange(false);
    setFiltersSheetOpen(false);
  };

  const categoryTree = useMemo<CategoryTreeNode[]>(() => {
    if (!categories.length) {
      return [];
    }
    const byId = new Map(categories.map((category) => [category.id, category]));
    const topLevelByName = new Map(
      categories
        .filter((category) => !category.parent_id)
        .map((category) => [category.name.toLowerCase(), category])
    );
    const childrenByParent = new Map<string | null, typeof categories>();
    categories.forEach((category) => {
      let parentKey = category.parent_id ?? null;
      if (!parentKey && category.name.includes('/')) {
        const prefix = category.name.split('/')[0]?.trim().toLowerCase();
        const fallbackParent = topLevelByName.get(prefix);
        if (fallbackParent && fallbackParent.id !== category.id) {
          parentKey = fallbackParent.id;
        }
      }
      const bucket = childrenByParent.get(parentKey) ?? [];
      bucket.push(category);
      childrenByParent.set(parentKey, bucket);
    });

    const topLevel = (childrenByParent.get(null) ?? [])
      .slice()
      .sort((a, b) => a.name.localeCompare(b.name, 'pl'));

    const nodes: CategoryTreeNode[] = topLevel.map((parent) => {
      const children = (childrenByParent.get(parent.id) ?? [])
        .slice()
        .sort((a, b) => a.name.localeCompare(b.name, 'pl'));
      return { parent, children };
    });

    childrenByParent.forEach((childList, parentId) => {
      if (!parentId) {
        return;
      }
      const parent = byId.get(parentId);
      if (!parent) {
        return;
      }
      const alreadyAdded = nodes.some((node) => node.parent.id === parentId);
      if (!alreadyAdded) {
        nodes.push({
          parent,
          children: childList.slice().sort((a, b) => a.name.localeCompare(b.name, 'pl'))
        });
      }
    });

    const orphanChildren = categories.filter(
      (category) => category.parent_id && !byId.has(category.parent_id)
    );
    orphanChildren.forEach((orphan) => {
      const alreadyAdded = nodes.some((node) => node.parent.id === orphan.id);
      if (!alreadyAdded) {
        nodes.push({ parent: orphan, children: [] });
      }
    });

    return nodes.sort((a, b) => a.parent.name.localeCompare(b.parent.name, 'pl'));
  }, [categories]);

  const categoryMenuEntries = useMemo<CategoryMenuEntry[]>(() => {
    const query = categorySearch.trim().toLowerCase();
    const matches = (value: string) => value.toLowerCase().includes(query);
    const entries: CategoryMenuEntry[] = [];
    if (!query || matches(UNCATEGORIZED_CATEGORY_NAME)) {
      entries.push({ type: 'uncategorized' });
    }

    categoryTree.forEach((node) => {
      const parentMatches = !query || matches(node.parent.name);
      const matchingChildren = node.children.filter((child) => {
        const displayName = stripParentPrefix(child.name, node.parent.name);
        return matches(child.name) || matches(displayName);
      });
      const hasChildMatches = matchingChildren.length > 0;
      if (query && !parentMatches && !hasChildMatches) {
        return;
      }

      const childrenToShow = parentMatches || !query ? node.children : matchingChildren;

      entries.push({
        type: 'parent',
        category: node.parent,
        hasChildren: node.children.length > 0
      });

      childrenToShow.forEach((child) => {
        entries.push({ type: 'child', category: child, parentId: node.parent.id });
      });
    });

    return entries;
  }, [categorySearch, categoryTree]);

  const filteredAccounts = useMemo(() => {
    const query = accountSearch.trim().toLowerCase();
    if (!query) return accounts;
    return accounts.filter((account) => account.name.toLowerCase().includes(query));
  }, [accountSearch, accounts]);

  const resolveCategoryLabel = useCallback(
    (categoryId: string) => {
      if (categoryId === UNCATEGORIZED_CATEGORY_ID) {
        return UNCATEGORIZED_CATEGORY_NAME;
      }
      return categories.find((item) => item.id === categoryId)?.name ?? 'Kategoria';
    },
    [categories]
  );

  const accountOptions = useMemo<CheckboxFilterOption[]>(
    () => filteredAccounts.map((account) => ({ id: account.id, label: account.name })),
    [filteredAccounts]
  );

  const categoryOptions = useMemo<CheckboxFilterOption[]>(
    () =>
      categoryMenuEntries.map((entry) => {
        if (entry.type === 'uncategorized') {
          return { id: UNCATEGORIZED_CATEGORY_ID, label: UNCATEGORIZED_CATEGORY_NAME };
        }
        if (entry.type === 'parent') {
          return { id: entry.category.id, label: entry.category.name };
        }
        return {
          id: entry.category.id,
          label: stripParentPrefix(entry.category.name, resolveCategoryLabel(entry.parentId)),
          nested: true
        };
      }),
    [categoryMenuEntries, resolveCategoryLabel]
  );

  // Konta i kategorie: w belce na desktopie, w panelu od dołu na telefonie
  const extraFilters = (
    <>
      <CheckboxFilterMenu
        id="reports-accounts-filter"
        label="Wybierz konta"
        className="reports-tag-input"
        triggerClassName="reports-tag-input__control"
        triggerLabel={hasAccountFilter ? `Wybrane konta: ${selectedAccountIds.length}` : 'Wszystkie konta'}
        searchPlaceholder="Szukaj kont"
        emptyText="Brak kont pasujących do wyszukiwania."
        options={accountOptions}
        search={accountSearch}
        onSearchChange={setAccountSearch}
        pendingIds={pendingAccountIds}
        onToggle={togglePendingAccount}
        onSelectAll={handleSelectAllAccounts}
        onClearAll={handleClearAllAccounts}
        onCancel={handleAccountMenuCancel}
        onApply={handleAccountMenuApply}
        isOpen={accountsMenuOpen}
        onOpenChange={handleAccountsMenuOpenChange}
      />
      <CheckboxFilterMenu
        id="reports-categories-filter"
        label="Wybierz kategorie wydatków"
        className="reports-tag-input"
        triggerClassName="reports-tag-input__control"
        triggerLabel={
          hasCategoryFilter ? `Wybrane kategorie: ${selectedCategoryIds.length}` : 'Wszystkie kategorie'
        }
        searchPlaceholder="Szukaj kategorii"
        emptyText="Brak kategorii pasujących do wyszukiwania."
        options={categoryOptions}
        search={categorySearch}
        onSearchChange={setCategorySearch}
        pendingIds={pendingCategoryIds}
        onToggle={togglePendingCategory}
        onSelectAll={handleSelectAllCategories}
        onClearAll={handleClearAllCategories}
        onCancel={handleCategoryMenuCancel}
        onApply={handleCategoryMenuApply}
        isOpen={categoriesMenuOpen}
        onOpenChange={handleCategoriesMenuOpenChange}
      />
    </>
  );

  return (
    <header
      ref={topbarRef}
      style={
        {
          ['--reports-topbar-height' as const]: topbarHeight ? `${topbarHeight}px` : undefined
        } as CSSProperties
      }
      className={`reports-topbar${topbarHidden ? ' is-hidden' : ''}`}
    >
      <div className="reports-topbar__inner">
        <h1 className="reports-topbar__title">Raporty</h1>
        <ReportsTabBar activeTab={activeTab} onSelect={setActiveTab} />

        <div className="reports-topbar__filters">
          <div className="reports-filter-row reports-filter-row--topbar">
            {isCompact ? (
              <>
                <PopoverFilter
                  id="reports-date-filter"
                  label="Wybór okresu raportów"
                  className="reports-date-control"
                  panelClassName="reports-date-menu"
                  isOpen={dateMenuOpen}
                  onOpenChange={handleDateMenuOpenChange}
                  trigger={({ ref, panelId, isOpen, onToggle }) => (
                    <button
                      ref={ref}
                      type="button"
                      className="reports-date-trigger is-focusable"
                      aria-haspopup="dialog"
                      aria-expanded={isOpen}
                      aria-controls={panelId}
                      onClick={onToggle}
                    >
                      <span className="reports-date-trigger__value">
                        {quickRangeSummary(activeRangeLabel, startMonth, endMonth)}
                      </span>
                      <ChevronDown className="reports-tag-input__chevron" size={14} aria-hidden="true" />
                    </button>
                  )}
                >
                  <div className="reports-date-menu__options">
                    {QUICK_RANGE_OPTIONS.map((option) => (
                      <button
                        key={option.id}
                        type="button"
                        className={`reports-date-option${activeQuickRange === option.id ? ' is-active' : ''}`}
                        onClick={() => handleQuickRangeSelect(option.id)}
                      >
                        {option.label}
                      </button>
                    ))}
                  </div>
                </PopoverFilter>
                <button
                  type="button"
                  className="reports-filters-button is-focusable"
                  aria-haspopup="dialog"
                  aria-label={activeFilterCount ? `Filtry, aktywne: ${activeFilterCount}` : 'Filtry'}
                  onClick={() => setFiltersSheetOpen(true)}
                >
                  <SlidersHorizontal size={16} aria-hidden="true" />
                  Filtry{activeFilterCount > 0 && ` · ${activeFilterCount}`}
                </button>
                <ReportsFiltersSheet isOpen={filtersSheetOpen} onClose={closeFiltersSheet}>
                  {extraFilters}
                </ReportsFiltersSheet>
              </>
            ) : (
              <>
                <div className="reports-range-chips" role="group" aria-label="Okres">
                  {QUICK_RANGE_OPTIONS.map((option) => (
                    <button
                      key={option.id}
                      type="button"
                      className="reports-range-chip is-focusable"
                      aria-pressed={activeQuickRange === option.id}
                      onClick={() => handleQuickRangeSelect(option.id)}
                    >
                      {option.label}
                    </button>
                  ))}
                </div>
                <span className="reports-filter-divider" aria-hidden="true" />
                <div className="reports-filter-tags reports-filter-tags--topbar" aria-label="Filtry dodatkowe">
                  {extraFilters}
                </div>
              </>
            )}
          </div>
        </div>
      </div>
    </header>
  );
};

export default ReportsTopbar;
