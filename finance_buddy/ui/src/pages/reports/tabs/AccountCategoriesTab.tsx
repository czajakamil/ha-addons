import { CSSProperties, useMemo, useState } from 'react';
import { ResponsivePie } from '@nivo/pie';
import type { Category } from '../../../api';
import {
  ACCOUNT_UNCATEGORIZED_ID,
  ACCOUNT_UNCATEGORIZED_NAME,
  OTHER_ACCOUNT_KINDS_LABEL,
  OTHER_CATEGORIES_LABEL,
  getAccountKindLabel,
  groupBreakdownEntries,
  moneyLabel,
  percentLabel,
  type BreakdownChartEntry,
  type BreakdownPieDatum
} from '../model';
import { BreakdownPieTooltip } from '../tooltips';
import { useChartTheme } from '../useChartTheme';
import { chartColor } from '../../../utils/tokens';
import type { ReportsController } from '../useReports';

type AccountCategoriesTabProps = {
  controller: ReportsController;
};

const AccountCategoriesTab = ({ controller }: AccountCategoriesTabProps) => {
  const {
    accounts,
    categories,
    accountCategories,
    accountCategorySummary,
    accountCategoryLoading,
    accountCategoryError,
    selectedAccountIds
  } = controller;
  const chartTheme = useChartTheme();
  const [accountCategoryView, setAccountCategoryView] = useState<'types' | 'categories'>('types');
  const [activeAccountCategoryId, setActiveAccountCategoryId] = useState<string | null>(null);

  const accountCategoryById = useMemo(
    () => new Map(accountCategories.map((category) => [category.id, category])),
    [accountCategories]
  );
  const accountCategoryLookup = useMemo(() => {
    const map = new Map<string, string>();
    accountCategories.forEach((category) => map.set(category.id, category.name));
    categories.forEach((category) => {
      if (!map.has(category.id)) {
        map.set(category.id, category.name);
      }
    });
    return map;
  }, [accountCategories, categories]);
  const accountCategoryPathMap = useMemo(() => {
    const cache = new Map<string, string>();

    const buildPath = (category: Category | undefined): string => {
      if (!category) return '';
      const cached = cache.get(category.id);
      if (cached) return cached;
      const parent = category.parent_id ? accountCategoryById.get(category.parent_id) : undefined;
      const parentPath = parent ? buildPath(parent) : '';
      const path = parentPath ? `${parentPath} / ${category.name}` : category.name;
      cache.set(category.id, path);
      return path;
    };

    accountCategories.forEach((category) => buildPath(category));
    return cache;
  }, [accountCategories, accountCategoryById]);
  const accountCategoryRootMap = useMemo(() => {
    const cache = new Map<string, Category | undefined>();

    const resolveRoot = (categoryId: string): Category | undefined => {
      if (cache.has(categoryId)) return cache.get(categoryId);
      let current = accountCategoryById.get(categoryId);
      if (!current) {
        cache.set(categoryId, undefined);
        return undefined;
      }
      while (current?.parent_id) {
        const parent = accountCategoryById.get(current.parent_id);
        if (!parent) break;
        current = parent;
      }
      cache.set(categoryId, current);
      return current;
    };

    const map = new Map<string, { id: string; name: string }>();
    accountCategories.forEach((category) => {
      const root = resolveRoot(category.id);
      if (root) {
        map.set(category.id, { id: root.id, name: root.name });
      }
    });
    return map;
  }, [accountCategories, accountCategoryById]);
  const accountCategoryChildrenMap = useMemo(() => {
    const map = new Map<string, Category[]>();
    accountCategories.forEach((category) => {
      if (!category.parent_id) return;
      const list = map.get(category.parent_id) ?? [];
      list.push(category);
      map.set(category.parent_id, list);
    });
    map.forEach((list) => list.sort((a, b) => a.name.localeCompare(b.name, 'pl')));
    return map;
  }, [accountCategories]);
  const filteredAccountsForCategories = useMemo(
    () =>
      selectedAccountIds.length
        ? accounts.filter((account) => selectedAccountIds.includes(account.id))
        : accounts,
    [accounts, selectedAccountIds]
  );
  const accountCategoryTotalsById = useMemo(() => {
    const totals = new Map<string, number>();
    if (!filteredAccountsForCategories.length) {
      return totals;
    }
    filteredAccountsForCategories.forEach((account) => {
      const categoryId = account.category_id;
      if (!categoryId) return;
      let current = accountCategoryById.get(categoryId);
      if (!current) return;
      const amount = account.balance_cents;
      while (current) {
        totals.set(current.id, (totals.get(current.id) ?? 0) + amount);
        if (!current.parent_id) break;
        current = accountCategoryById.get(current.parent_id);
      }
    });
    return totals;
  }, [filteredAccountsForCategories, accountCategoryById]);
  const accountCategoryDrillableIds = useMemo(() => {
    const ids = new Set<string>();
    accountCategoryChildrenMap.forEach((children, parentId) => {
      const hasData = children.some(
        (child) => Math.abs(accountCategoryTotalsById.get(child.id) ?? 0) > 0
      );
      if (hasData) ids.add(parentId);
    });
    return ids;
  }, [accountCategoryChildrenMap, accountCategoryTotalsById]);
  const accountCategoryCategoriesData = useMemo<BreakdownChartEntry[]>(() => {
    if (!filteredAccountsForCategories.length) {
      return [];
    }

    const totals = new Map<string, { name: string; total: number; categoryId?: string }>();
    const activeParentId = activeAccountCategoryId;

    const addToBucket = (bucketId: string, name: string, amount: number, categoryId?: string) => {
      const bucket = totals.get(bucketId) ?? { name, total: 0, categoryId };
      bucket.total += amount;
      if (categoryId) {
        bucket.categoryId = categoryId;
      }
      totals.set(bucketId, bucket);
    };

    filteredAccountsForCategories.forEach((account) => {
      const categoryId = account.category_id ?? null;
      const amount = account.balance_cents;

      if (!activeParentId) {
        if (!categoryId) {
          addToBucket(ACCOUNT_UNCATEGORIZED_ID, ACCOUNT_UNCATEGORIZED_NAME, amount);
          return;
        }
        const root = accountCategoryRootMap.get(categoryId);
        const bucketId = root?.id ?? categoryId;
        const bucketName =
          root?.name ??
          accountCategoryPathMap.get(categoryId) ??
          accountCategoryLookup.get(categoryId) ??
          ACCOUNT_UNCATEGORIZED_NAME;
        addToBucket(bucketId, bucketName, amount, bucketId);
        return;
      }

      if (!categoryId) {
        return;
      }

      if (categoryId === activeParentId) {
        addToBucket(`${activeParentId}::self`, 'Bez podkategorii', amount);
        return;
      }

      let current: Category | undefined = accountCategoryById.get(categoryId);
      if (!current) {
        return;
      }
      while (current && current.parent_id && current.parent_id !== activeParentId) {
        const parent: Category | undefined = accountCategoryById.get(current.parent_id);
        if (!parent) {
          return;
        }
        current = parent;
      }
      if (!current || current.parent_id !== activeParentId) {
        return;
      }
      addToBucket(current.id, current.name, amount, current.id);
    });

    const entries = Array.from(totals.entries())
      .map(([key, meta]) => ({
        key,
        name: meta.name,
        total: meta.total,
        value: Math.abs(meta.total),
        categoryId: meta.categoryId
      }))
      .filter((entry) => entry.value > 0)
      .sort((a, b) => b.value - a.value);

    const percentBase = entries.reduce((sum, entry) => sum + entry.value, 0);

    return entries.map((entry, index) => ({
      ...entry,
      percentage: percentBase > 0 ? entry.value / percentBase : 0,
      color: chartColor(index)
    }));
  }, [
    accountCategoryLookup,
    accountCategoryPathMap,
    accountCategoryRootMap,
    accountCategoryById,
    filteredAccountsForCategories,
    activeAccountCategoryId
  ]);

  const accountCategoryTypeData = useMemo<BreakdownChartEntry[]>(() => {
    if (!accountCategorySummary.length) {
      return [];
    }

    const entries = accountCategorySummary
      .map((entry) => ({
        key: entry.account_kind || 'unknown',
        name: getAccountKindLabel(entry.account_kind),
        total: entry.total_cents,
        value: Math.abs(entry.total_cents),
        percentage: 0,
        color: ''
      }))
      .filter((entry) => entry.value > 0)
      .sort((a, b) => b.value - a.value);

    const percentBase = entries.reduce((sum, entry) => sum + entry.value, 0);

    return entries.map((entry, index) => ({
      ...entry,
      percentage: percentBase > 0 ? entry.value / percentBase : 0,
      color: chartColor(index)
    }));
  }, [accountCategorySummary]);

  const accountCategoryData =
    accountCategoryView === 'types' ? accountCategoryTypeData : accountCategoryCategoriesData;

  const accountCategoryAccountsCount = useMemo(() => {
    if (accountCategoryView === 'types') {
      return accountCategorySummary.reduce((sum, entry) => sum + entry.accounts_count, 0);
    }
    return filteredAccountsForCategories.length;
  }, [accountCategorySummary, accountCategoryView, filteredAccountsForCategories.length]);

  const accountCategoryPercentBase = useMemo(
    () => accountCategoryData.reduce((sum, entry) => sum + entry.value, 0),
    [accountCategoryData]
  );

  const groupedAccountCategoryData = useMemo<BreakdownChartEntry[]>(
    () =>
      groupBreakdownEntries(
        accountCategoryData,
        accountCategoryPercentBase,
        accountCategoryView === 'types' ? OTHER_ACCOUNT_KINDS_LABEL : OTHER_CATEGORIES_LABEL
      ),
    [accountCategoryData, accountCategoryPercentBase, accountCategoryView]
  );

  const accountCategoryPieData = useMemo<BreakdownPieDatum[]>(
    () =>
      groupedAccountCategoryData.map((entry) => ({
        ...entry,
        id: entry.key,
        label: entry.name
      })),
    [groupedAccountCategoryData]
  );
  const accountCategoryPieTotal = useMemo(
    () => accountCategoryPieData.reduce((sum, entry) => sum + entry.value, 0),
    [accountCategoryPieData]
  );
  const accountBreakdownCenterLayer = useMemo(
    () =>
      ({ centerX, centerY }: { centerX: number; centerY: number }) => (
        <>
          <text
            x={centerX}
            y={centerY - 8}
            textAnchor="middle"
            dominantBaseline="central"
            fill={chartTheme.text}
            fontSize={20}
            fontWeight={700}
          >
            {moneyLabel(accountCategoryPieTotal)}
          </text>
          <text
            x={centerX}
            y={centerY + 14}
            textAnchor="middle"
            dominantBaseline="central"
            fill={chartTheme.muted}
            fontSize={12}
            fontWeight={600}
          >
            Łączna wartość kont
          </text>
        </>
      ),
    [accountCategoryPieTotal, chartTheme]
  );
  const accountCategoryNetTotal = useMemo(
    () => groupedAccountCategoryData.reduce((sum, entry) => sum + entry.total, 0),
    [groupedAccountCategoryData]
  );
  const hasAccountCategoryData =
    groupedAccountCategoryData.length > 0 && accountCategoryPieTotal > 0;

  const accountCategoryActiveLabel = useMemo(() => {
    if (!activeAccountCategoryId) return null;
    return (
      accountCategoryPathMap.get(activeAccountCategoryId) ??
      accountCategoryLookup.get(activeAccountCategoryId) ??
      activeAccountCategoryId
    );
  }, [activeAccountCategoryId, accountCategoryLookup, accountCategoryPathMap]);

  const handleAccountCategoryEntryClick = (entry: BreakdownChartEntry) => {
    if (accountCategoryView !== 'categories') return;
    if (entry.isOther || !entry.categoryId) return;
    if (!accountCategoryDrillableIds.has(entry.categoryId)) return;
    setActiveAccountCategoryId(entry.categoryId);
  };
  const handleAccountCategoryBack = () => {
    if (!activeAccountCategoryId) return;
    const parentId = accountCategoryById.get(activeAccountCategoryId)?.parent_id ?? null;
    setActiveAccountCategoryId(parentId);
  };

  return (
    <section className="card report-card">
      <div className="report-card__header">
        <div>
          <h3>Tagi kont</h3>
          <p className="report-card__subtitle">
            Rozkład sald kont według typu lub kategorii.
          </p>
        </div>
        <div className="report-summary-grid">
          <div>
            <p>Łączne saldo</p>
            <strong>{moneyLabel(accountCategoryNetTotal)}</strong>
          </div>
          <div>
            <p>Uwzględnione konta</p>
            <strong>{accountCategoryAccountsCount}</strong>
          </div>
        </div>
      </div>
      {accountCategoryView === 'types' && accountCategoryLoading ? (
        <p className="report-empty">Ładowanie danych o kontach...</p>
      ) : accountCategoryView === 'types' && accountCategoryError ? (
        <p className="report-empty">{accountCategoryError}</p>
      ) : !hasAccountCategoryData ? (
        <p className="report-empty">Brak danych o saldach kont.</p>
      ) : (
        <div className="breakdown-layout">
          {accountCategoryPieData.length <= 5 && (
          <div
            className="breakdown-chart"
            aria-label={`Udział ${accountCategoryView === 'types' ? 'typów' : 'kategorii'} kont według salda`}
          >
            <div className="breakdown-chart__pie">
              <div className="report-chart-canvas">
                <ResponsivePie
                  data={accountCategoryPieData}
                  margin={{ top: 16, right: 16, bottom: 16, left: 16 }}
                  innerRadius={0.78}
                  padAngle={0.7}
                  cornerRadius={45}
                  activeOuterRadiusOffset={8}
                  colors={{ datum: 'data.color' }}
                  enableArcLinkLabels={false}
                  arcLabelsSkipAngle={16}
                  arcLabel={(datum) => percentLabel(datum.data.percentage)}
                  arcLabelsTextColor={chartTheme.surface}
                  theme={chartTheme.nivoTheme}
                  valueFormat={(value) => moneyLabel(Number(value))}
                  tooltip={(props) => <BreakdownPieTooltip {...props} />}
                  onClick={(datum) => handleAccountCategoryEntryClick(datum.data)}
                  layers={['arcs', 'arcLabels', accountBreakdownCenterLayer]}
                />
              </div>
            </div>
          </div>
          )}
          <aside
            className="breakdown-layout__categories"
            aria-label={
              accountCategoryView === 'types'
                ? 'Typy kont w wybranym zestawieniu'
                : 'Tagi kont w wybranym zestawieniu'
            }
          >
            <div className="breakdown-layout__header breakdown-layout__header--stacked">
              <h4 className="breakdown-layout__title">
                {accountCategoryView === 'types' ? 'Typy kont' : 'Tagi kont'}
              </h4>
              <p className="text-secondary small">Salda netto z wybranych kont</p>
              <div
                className="breakdown-toggle"
                role="group"
                aria-label="Wybierz widok kont"
                style={{ ['--toggle-index' as const]: accountCategoryView === 'types' ? 0 : 1 } as CSSProperties}
              >
                <span className="breakdown-toggle__thumb" aria-hidden />
                <button
                  type="button"
                  className={`breakdown-toggle__option${
                    accountCategoryView === 'types' ? ' is-active' : ''
                  }`}
                  onClick={() => {
                    setAccountCategoryView('types');
                    setActiveAccountCategoryId(null);
                  }}
                >
                  Typy
                </button>
                <button
                  type="button"
                  className={`breakdown-toggle__option${
                    accountCategoryView === 'categories' ? ' is-active' : ''
                  }`}
                  onClick={() => setAccountCategoryView('categories')}
                >
                  Tagi
                </button>
              </div>
            </div>
            {accountCategoryView === 'categories' && accountCategoryActiveLabel && (
              <div className="breakdown-filter">
                <span className="breakdown-filter__label">Ścieżka tagu:</span>
                <span className="breakdown-filter__value">{accountCategoryActiveLabel}</span>
                <button
                  type="button"
                  className="breakdown-filter__clear"
                  onClick={handleAccountCategoryBack}
                >
                  Wróć
                </button>
              </div>
            )}
            <ul className="report-breakdown__list">
              {groupedAccountCategoryData.map((entry) => {
                const isDrillable =
                  accountCategoryView === 'categories' &&
                  !!entry.categoryId &&
                  accountCategoryDrillableIds.has(entry.categoryId) &&
                  !entry.isOther;
                return (
                  <li
                    key={entry.key}
                    className={`breakdown-card${isDrillable ? ' is-clickable' : ''}`}
                    style={{ ['--category-color' as const]: entry.color } as CSSProperties}
                    role={isDrillable ? 'button' : undefined}
                    tabIndex={isDrillable ? 0 : undefined}
                    onClick={() => {
                      if (!isDrillable) return;
                      handleAccountCategoryEntryClick(entry);
                    }}
                    onKeyDown={(event) => {
                      if (!isDrillable) return;
                      if (event.key === 'Enter' || event.key === ' ') {
                        event.preventDefault();
                        handleAccountCategoryEntryClick(entry);
                      }
                    }}
                  >
                    <div className="breakdown-card__pill">
                      <span className="breakdown-card__dot" />
                    </div>
                    <div className="breakdown-card__content">
                      <p className="breakdown-card__name">{entry.name}</p>
                      <p className="breakdown-card__meta">
                        {percentLabel(entry.percentage)} • {moneyLabel(entry.total)}
                      </p>
                    </div>
                  </li>
                );
              })}
            </ul>
          </aside>
        </div>
      )}
    </section>
  );
};

export default AccountCategoriesTab;
