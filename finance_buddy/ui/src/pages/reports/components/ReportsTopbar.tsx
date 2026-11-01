import { CSSProperties, useCallback, useMemo } from 'react';
import { Calendar } from 'lucide-react';
import Button from '../../../components/Button';
import PopoverFilter from '../../../components/PopoverFilter';
import {
  QUICK_RANGE_OPTIONS,
  UNCATEGORIZED_CATEGORY_ID,
  UNCATEGORIZED_CATEGORY_NAME,
  formatMonthLabel,
  stripParentPrefix,
  type CategoryMenuEntry,
  type CategoryTreeNode
} from '../model';
import type { ReportsController } from '../useReports';
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
    customRangeMode,
    setActiveQuickRange,
    setCustomRangeMode,
    handleQuickRangeSelect,
    handleCustomRangeChange,
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
      return categories.find((item) => item.id === categoryId)?.name ?? 'Tag';
    },
    [categories]
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
        <ReportsTabBar activeTab={activeTab} onSelect={setActiveTab} />

        <div className="reports-topbar__filters">
          <div className="reports-filter-row reports-filter-row--topbar">
            <PopoverFilter
              id="reports-date-filter"
              label="Wybór zakresu dat raportów"
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
                  <span className="reports-date-trigger__icon" aria-hidden="true">
                    <Calendar size={16} />
                  </span>
                  <div className="reports-date-trigger__labels">
                    <span className="reports-date-trigger__value">
                      {startMonth && endMonth
                        ? `${formatMonthLabel(startMonth)} – ${formatMonthLabel(endMonth)}`
                        : 'Wybierz zakres'}
                    </span>
                  </div>
                  <span className="reports-date-trigger__chevron" aria-hidden="true">
                    ▾
                  </span>
                </button>
              )}
            >
              <div className="reports-date-menu__options">
                {QUICK_RANGE_OPTIONS.map((option) => (
                  <button
                    key={option.id}
                    type="button"
                    className={`reports-date-option${
                      activeQuickRange === option.id ? ' is-active' : ''
                    }`}
                    onClick={() => handleQuickRangeSelect(option.id)}
                  >
                    {option.label}
                  </button>
                ))}
                <button
                  type="button"
                  className={`reports-date-option reports-date-option--custom${
                    customRangeMode ? ' is-active' : ''
                  }`}
                  onClick={() => {
                    setActiveQuickRange(null);
                    setCustomRangeMode(true);
                  }}
                >
                  Zakres niestandardowy
                </button>
              </div>

              {customRangeMode && (
                <div className="reports-date-custom">
                  <label className="reports-date-custom__label">
                    Data początkowa
                    <input
                      type="month"
                      value={startMonth}
                      onChange={(event) => handleCustomRangeChange(event.target.value, 'start')}
                    />
                  </label>
                  <label className="reports-date-custom__label">
                    Data końcowa
                    <input
                      type="month"
                      value={endMonth}
                      onChange={(event) => handleCustomRangeChange(event.target.value, 'end')}
                    />
                  </label>
                </div>
              )}
            </PopoverFilter>
            <div className="reports-filter-tags reports-filter-tags--topbar" aria-label="Filtry dodatkowe">
              <PopoverFilter
                id="reports-accounts-filter"
                label="Filtr kont"
                className="reports-tag-input"
                panelClassName="reports-tag-input__menu reports-tag-input__menu--wide category-menu"
                isOpen={accountsMenuOpen}
                onOpenChange={handleAccountsMenuOpenChange}
                trigger={({ ref, panelId, isOpen, onToggle }) => (
                  <button
                    ref={ref}
                    type="button"
                    className="reports-tag-input__control is-focusable"
                    aria-label="Wybierz konta"
                    aria-haspopup="dialog"
                    aria-expanded={isOpen}
                    aria-controls={panelId}
                    onClick={onToggle}
                  >
                    <div className="reports-tag-input__value">
                      {selectedAccountIds.length === 0 ||
                      selectedAccountIds.length === accounts.length ? (
                        <span className="reports-tag-input__placeholder">Wszystkie konta</span>
                      ) : (
                        <span className="reports-selection-pill">
                          <span className="reports-selection-pill__label">
                            {selectedAccountIds.length} wybranych kont
                          </span>
                        </span>
                      )}
                    </div>
                    <span className="reports-tag-input__chevron">▾</span>
                  </button>
                )}
              >
                <div className="category-menu__search">
                  <input
                    type="search"
                    placeholder="Szukaj kont"
                    value={accountSearch}
                    onChange={(event) => setAccountSearch(event.target.value)}
                  />
                </div>
                <div className="category-menu__list" aria-label="Wybierz konta">
                  {filteredAccounts.map((account) => (
                    <label key={account.id} className="category-menu__item">
                      <input
                        type="checkbox"
                        checked={pendingAccountIds.includes(account.id)}
                        onChange={() => togglePendingAccount(account.id)}
                      />
                      <span className="category-menu__name">{account.name}</span>
                    </label>
                  ))}
                  {filteredAccounts.length === 0 && (
                    <p className="reports-tag-input__empty">Brak kont pasujących do wyszukiwania.</p>
                  )}
                </div>
                <div className="category-menu__footer">
                  <button type="button" className="category-menu__action" onClick={handleSelectAllAccounts}>
                    Wybierz wszystko
                  </button>
                  <button type="button" className="category-menu__action" onClick={handleClearAllAccounts}>
                    Odznacz wszystko
                  </button>
                  <div className="category-menu__footer-actions">
                    <Button variant="outline-secondary" size="sm" onClick={handleAccountMenuCancel}>
                      Anuluj
                    </Button>
                    <Button variant="primary" size="sm" onClick={handleAccountMenuApply}>
                      Zatwierdź
                    </Button>
                  </div>
                </div>
              </PopoverFilter>

              <PopoverFilter
                id="reports-categories-filter"
                label="Filtr kategorii wydatków"
                className="reports-tag-input"
                panelClassName="reports-tag-input__menu reports-tag-input__menu--wide category-menu"
                isOpen={categoriesMenuOpen}
                onOpenChange={handleCategoriesMenuOpenChange}
                trigger={({ ref, panelId, isOpen, onToggle }) => (
                  <button
                    ref={ref}
                    type="button"
                    className="reports-tag-input__control is-focusable"
                    aria-label="Wybierz kategorie wydatków"
                    aria-haspopup="dialog"
                    aria-expanded={isOpen}
                    aria-controls={panelId}
                    onClick={onToggle}
                  >
                    <div className="reports-tag-input__value">
                      {(selectedCategoryIds.length === 0 ||
                        selectedCategoryIds.length === allCategoryOptionIds.length) && (
                        <span className="reports-tag-input__placeholder">Wszystkie kategorie</span>
                      )}
                      {selectedCategoryIds.length > 0 &&
                        selectedCategoryIds.length !== allCategoryOptionIds.length && (
                          <span className="reports-selection-pill">
                            <span className="reports-selection-pill__label">
                              {selectedCategoryIds.length} wybranych kategorii
                            </span>
                          </span>
                        )}
                    </div>
                    <span className="reports-tag-input__chevron">▾</span>
                  </button>
                )}
              >
                <div className="category-menu__search">
                  <input
                    type="search"
                    placeholder="Szukaj kategorii"
                    value={categorySearch}
                    onChange={(event) => setCategorySearch(event.target.value)}
                  />
                </div>
                <div className="category-menu__list" aria-label="Wybierz kategorie">
                  {categoryMenuEntries.map((entry) => {
                    if (entry.type === 'uncategorized') {
                      return (
                        <label key="uncategorized" className="category-menu__item">
                          <input
                            type="checkbox"
                            checked={pendingCategoryIds.includes(UNCATEGORIZED_CATEGORY_ID)}
                            onChange={() => togglePendingCategory(UNCATEGORIZED_CATEGORY_ID)}
                          />
                          <span className="category-menu__name">{UNCATEGORIZED_CATEGORY_NAME}</span>
                        </label>
                      );
                    }

                    if (entry.type === 'parent') {
                      const checked = pendingCategoryIds.includes(entry.category.id);
                      return (
                        <label key={entry.category.id} className="category-menu__item">
                          <input
                            type="checkbox"
                            checked={checked}
                            onChange={() => togglePendingCategory(entry.category.id)}
                          />
                          <span className="category-menu__name">{entry.category.name}</span>
                        </label>
                      );
                    }

                    return (
                      <label
                        key={`${entry.parentId}-${entry.category.id}`}
                        className="category-menu__item category-menu__item--child"
                      >
                        <input
                          type="checkbox"
                          checked={pendingCategoryIds.includes(entry.category.id)}
                          onChange={() => togglePendingCategory(entry.category.id)}
                        />
                        <span className="category-menu__name">
                          {stripParentPrefix(entry.category.name, resolveCategoryLabel(entry.parentId))}
                        </span>
                      </label>
                    );
                  })}
                  {categoryMenuEntries.length === 0 && (
                    <p className="reports-tag-input__empty">Brak kategorii pasujących do wyszukiwania.</p>
                  )}
                </div>

                <div className="category-menu__footer">
                  <button type="button" className="category-menu__action" onClick={handleSelectAllCategories}>
                    Wybierz wszystko
                  </button>
                  <button type="button" className="category-menu__action" onClick={handleClearAllCategories}>
                    Odznacz wszystko
                  </button>
                  <div className="category-menu__footer-actions">
                    <Button variant="outline-secondary" size="sm" onClick={handleCategoryMenuCancel}>
                      Anuluj
                    </Button>
                    <Button variant="primary" size="sm" onClick={handleCategoryMenuApply}>
                      Zatwierdź
                    </Button>
                  </div>
                </div>
              </PopoverFilter>
            </div>
          </div>
        </div>
      </div>
    </header>
  );
};

export default ReportsTopbar;
