import { CSSProperties, useMemo, useState } from 'react';
import Button from '../../../components/Button';
import DataTable from '../../../components/DataTable';
import {
  UNCATEGORIZED_CATEGORY_ID,
  UNCATEGORIZED_CATEGORY_NAME,
  formatDate,
  OTHER_CATEGORIES_LABEL,
  BREAKDOWN_TOP_LIMIT,
  otherCategoryColor,
  moneyLabel,
  normalizeCategoryName,
  percentLabel,
  type BreakdownChartEntry
} from '../model';
import { useBreakdownEditing } from '../useBreakdownEditing';
import { chartColor } from '../../../utils/tokens';
import type { ReportsController } from '../useReports';

type SpendingBreakdownTabProps = {
  controller: ReportsController;
};

const SpendingBreakdownTab = ({ controller }: SpendingBreakdownTabProps) => {
  const {
    data,
    startMonth,
    endMonth,
    selectedAccountIds,
    selectedCategoryIds,
    stores,
    storeLookup,
    accountLookup,
    fetchReports,
    resolveAccountFilterIds,
    resolveCategoryFilterIds
  } = controller;

  const [breakdownView, setBreakdownView] = useState<'categories' | 'groups'>('categories');
  const [activeGroupFilter, setActiveGroupFilter] = useState<string | null>(null);

  const {
    selectedBreakdownCategory,
    setSelectedBreakdownCategory,
    breakdownTransactions,
    breakdownTransactionsLoading,
    breakdownTransactionsError,
    breakdownEditError,
    breakdownGridColumns,
    breakdownGridHeight,
    breakdownRowKeyGetter,
    handleBreakdownGridDoubleClick
  } = useBreakdownEditing({
    data,
    startMonth,
    endMonth,
    selectedAccountIds,
    selectedCategoryIds,
    stores,
    storeLookup,
    accountLookup,
    fetchReports,
    resolveAccountFilterIds,
    resolveCategoryFilterIds
  });

  const rangeLabel =
    data !== null ? `${formatDate(data.start_date)} – ${formatDate(data.end_date)}` : '';
  const breakdownEntries = data?.spending_breakdown.entries ?? [];
  const totalSpending = data?.spending_breakdown.total_spending_cents ?? 0;

  const filteredBreakdownEntries = useMemo(
    () =>
      breakdownEntries.filter((entry) => {
        const { groupName, categoryName } = normalizeCategoryName(entry.category_name);
        const normalizedGroup = groupName.toLowerCase();
        const normalizedCategory = categoryName.toLowerCase();
        if (
          normalizedGroup === 'internal master category' ||
          normalizedCategory === 'internal master category'
        ) {
          return false;
        }
        if (activeGroupFilter) {
          return normalizedGroup === activeGroupFilter.toLowerCase();
        }
        return true;
      }),
    [breakdownEntries, activeGroupFilter]
  );
  const visibleBreakdownTotal = useMemo(
    () => filteredBreakdownEntries.reduce((sum, entry) => sum + Math.abs(entry.total_cents), 0),
    [filteredBreakdownEntries]
  );
  const breakdownPercentBase = visibleBreakdownTotal || Math.abs(totalSpending);
  const hasBreakdownEntries = filteredBreakdownEntries.length > 0;
  const breakdownCategoriesData = useMemo<BreakdownChartEntry[]>(
    () =>
      filteredBreakdownEntries.map((entry, index) => {
        const { categoryName } = normalizeCategoryName(entry.category_name);
        const resolvedName = entry.category_id ? categoryName : UNCATEGORIZED_CATEGORY_NAME;
        const absoluteValue = Math.abs(entry.total_cents);
        const percentage =
          breakdownPercentBase > 0
            ? absoluteValue / breakdownPercentBase
            : entry.percentage ?? 0;
        return {
          key: entry.category_id ?? `uncategorized-${index}`,
          name: resolvedName,
          categoryId: entry.category_id ?? UNCATEGORIZED_CATEGORY_ID,
          percentage,
          total: absoluteValue,
          value: absoluteValue,
          color: chartColor(index)
        };
      }),
    [filteredBreakdownEntries, breakdownPercentBase]
  );

  const breakdownGroupData = useMemo<BreakdownChartEntry[]>(() => {
    const totals = new Map<string, number>();

    filteredBreakdownEntries.forEach((entry) => {
      const { groupName } = normalizeCategoryName(entry.category_name);
      totals.set(groupName, (totals.get(groupName) ?? 0) + Math.abs(entry.total_cents));
    });

    return Array.from(totals.entries())
      .sort((a, b) => b[1] - a[1])
      .map(([name, total], index) => ({
        key: `group-${name || 'uncategorized'}-${index}`,
        name: !name || name === 'Uncategorized' ? UNCATEGORIZED_CATEGORY_NAME : name,
        percentage: breakdownPercentBase > 0 ? total / breakdownPercentBase : 0,
        total,
        value: total,
        color: chartColor(index)
      }));
  }, [filteredBreakdownEntries, breakdownPercentBase]);

  const effectiveBreakdownView: 'categories' | 'groups' =
    activeGroupFilter ? 'categories' : breakdownView;

  const breakdownChartData =
    effectiveBreakdownView === 'groups' ? breakdownGroupData : breakdownCategoriesData;

  // Pierwsza dziesiątka malejąco, reszta zbiorczo jako „Pozostałe"
  const topBreakdownEntries = useMemo<BreakdownChartEntry[]>(() => {
    const sorted = breakdownChartData.slice().sort((a, b) => b.total - a.total);
    const rest = sorted.slice(BREAKDOWN_TOP_LIMIT);
    if (!rest.length) return sorted;
    const restTotal = rest.reduce((sum, entry) => sum + entry.total, 0);
    return [
      ...sorted.slice(0, BREAKDOWN_TOP_LIMIT),
      {
        key: 'other-categories',
        name: OTHER_CATEGORIES_LABEL,
        percentage: breakdownPercentBase > 0 ? restTotal / breakdownPercentBase : 0,
        total: restTotal,
        value: restTotal,
        color: otherCategoryColor(),
        isOther: true
      }
    ];
  }, [breakdownChartData, breakdownPercentBase]);
  const breakdownPieTotal = useMemo(
    () => topBreakdownEntries.reduce((sum, entry) => sum + entry.value, 0),
    [topBreakdownEntries]
  );
  const breakdownToggleIndex = effectiveBreakdownView === 'categories' ? 0 : 1;

  const monthsInRange = data?.spending_trends?.months?.length ?? 0;
  const averagePerMonth = monthsInRange > 0 ? Math.round(breakdownPieTotal / monthsInRange) : 0;
  const daysInRange = useMemo(() => {
    if (!data) return 0;
    const start = new Date(data.start_date).getTime();
    const end = new Date(data.end_date).getTime();
    if (Number.isNaN(start) || Number.isNaN(end) || end < start) return 0;
    return Math.round((end - start) / 86_400_000) + 1;
  }, [data]);
  const averagePerDay = daysInRange > 0 ? Math.round(breakdownPieTotal / daysInRange) : 0;
  const mostFrequentEntry = useMemo(() => {
    let best: { name: string; count: number } | null = null;
    for (const entry of filteredBreakdownEntries) {
      const count = entry.transaction_count ?? 0;
      if (count > 0 && (best === null || count > best.count)) {
        const { categoryName } = normalizeCategoryName(entry.category_name);
        best = { name: entry.category_id ? categoryName : UNCATEGORIZED_CATEGORY_NAME, count };
      }
    }
    return best;
  }, [filteredBreakdownEntries]);
  const largestOutflow = data?.spending_breakdown.largest_outflow ?? null;
  const largestOutflowLabel = largestOutflow
    ? largestOutflow.store_name ||
      largestOutflow.comment ||
      largestOutflow.category_name ||
      'Bez opisu'
    : null;

  const handleBreakdownEntryClick = (entry: BreakdownChartEntry) => {
    if (!entry.name || entry.isOther) return;
    if (effectiveBreakdownView === 'groups') {
      setActiveGroupFilter(entry.name);
      setBreakdownView('categories');
      setSelectedBreakdownCategory(null);
      return;
    }
    setSelectedBreakdownCategory({
      id: entry.categoryId ?? UNCATEGORIZED_CATEGORY_ID,
      name: entry.name,
      color: entry.color
    });
  };

  return (
    <div className="breakdown-stack">
    {selectedBreakdownCategory && (
    <section className="card report-card">
        <div className="breakdown-transactions">
          <div className="breakdown-transactions__header">
            <div>
              <div className="breakdown-transactions__title">
                <span
                  className="breakdown-transactions__dot"
                  style={{ backgroundColor: selectedBreakdownCategory.color ?? otherCategoryColor() }}
                  aria-hidden
                />
                <span>Transakcje: {selectedBreakdownCategory.name}</span>
              </div>
              <p className="breakdown-transactions__meta">
                {rangeLabel || 'Wybrany zakres'}
                {breakdownTransactions.length > 0 ? ` • ${breakdownTransactions.length} pozycji` : ''}
              </p>
            </div>
            <Button variant="light" size="sm" onClick={() => setSelectedBreakdownCategory(null)}>
              Zamknij
            </Button>
          </div>
          {breakdownEditError && (
            <p className="breakdown-transactions__error">{breakdownEditError}</p>
          )}
          {breakdownTransactionsLoading ? (
            <p className="report-empty">Ładowanie transakcji...</p>
          ) : breakdownTransactionsError ? (
            <p className="report-empty">{breakdownTransactionsError}</p>
          ) : breakdownTransactions.length === 0 ? (
            <p className="report-empty">Brak transakcji w tej kategorii.</p>
          ) : (
            <DataTable
              aria-label="Transakcje z wybranej kategorii"
              className="transactions-grid breakdown-transactions__grid"
              columns={breakdownGridColumns}
              rows={breakdownTransactions}
              rowKeyGetter={breakdownRowKeyGetter}
              height={breakdownGridHeight}
              onCellDoubleClick={handleBreakdownGridDoubleClick}
              noRowsFallback={
                <div className="data-grid__empty text-secondary text-center">
                  Brak transakcji w wybranej kategorii.
                </div>
              }
            />
          )}
        </div>
    </section>
    )}
    {!hasBreakdownEntries ? (
      <section className="card report-card">
        <p className="report-empty">Brak wydatków w wybranym okresie.</p>
      </section>
    ) : (
      <div className="breakdown-columns">
        <section className="card report-card breakdown-categories-card">
          <aside
            className="breakdown-layout__categories breakdown-layout__categories--spending"
            aria-label={
              effectiveBreakdownView === 'groups'
                ? 'Grupy wydatków w wybranym okresie'
                : 'Tagi wydatków w wybranym okresie'
            }
          >
            <div className="breakdown-layout__header breakdown-layout__header--stacked">
              <h4 className="breakdown-layout__title">
                {effectiveBreakdownView === 'groups' ? 'Grupy' : 'Kategorie'}
              </h4>
              <div
                className="breakdown-toggle"
                role="group"
                aria-label="Wybierz widok wydatków"
                style={{ ['--toggle-index' as const]: breakdownToggleIndex } as CSSProperties}
              >
                <span className="breakdown-toggle__thumb" aria-hidden />
                <button
                  type="button"
                  className={`breakdown-toggle__option${
                    effectiveBreakdownView === 'categories' ? ' is-active' : ''
                  }`}
                  onClick={() => setBreakdownView('categories')}
                >
                  Kategorie
                </button>
                <button
                  type="button"
                  className={`breakdown-toggle__option${
                    effectiveBreakdownView === 'groups' ? ' is-active' : ''
                  }`}
                  onClick={() => {
                    setActiveGroupFilter(null);
                    setBreakdownView('groups');
                    setSelectedBreakdownCategory(null);
                  }}
                >
                  Grupy
                </button>
              </div>
            </div>
            {activeGroupFilter && (
              <div className="breakdown-filter">
                <span className="breakdown-filter__label">Filtrowane po grupie:</span>
                <span className="breakdown-filter__value">{activeGroupFilter}</span>
                <button
                  type="button"
                  className="breakdown-filter__clear"
                  onClick={() => setActiveGroupFilter(null)}
                >
                  Wyczyść
                </button>
              </div>
            )}
            <div className="breakdown-list">
              <div className="breakdown-list__header">
                <span>{effectiveBreakdownView === 'groups' ? 'Grupy' : 'Kategorie'}</span>
                <span>Łączne wydatki</span>
              </div>
              <ul className="breakdown-list__items">
                {topBreakdownEntries.map((entry) => {
                  const isGroupMode = effectiveBreakdownView === 'groups';
                  const isCategoryMode = effectiveBreakdownView === 'categories';
                  const isClickable = (isGroupMode || isCategoryMode) && !entry.isOther;
                  const barPercent = Math.min(Math.max(entry.percentage * 100, 0), 100);
                  const barWidth = barPercent > 0 ? Math.max(barPercent, 2) : 0;
                  return (
                    <li
                      key={entry.key}
                      className={`breakdown-list__item${isClickable ? ' is-clickable' : ''}`}
                      style={
                        {
                          ['--category-color' as const]: entry.color,
                          ['--bar-percent' as const]: `${barWidth}%`
                        } as CSSProperties
                      }
                      role={isClickable ? 'button' : undefined}
                      tabIndex={isClickable ? 0 : undefined}
                      onClick={() => {
                        if (!isClickable) return;
                        handleBreakdownEntryClick(entry);
                      }}
                      onKeyDown={(event) => {
                        if (!isClickable) return;
                        if (event.key === 'Enter' || event.key === ' ') {
                          event.preventDefault();
                          handleBreakdownEntryClick(entry);
                        }
                      }}
                    >
                      <div className="breakdown-list__row">
                        <div className="breakdown-list__name-row">
                          <span className="breakdown-list__name">{entry.name}</span>
                        </div>
                        <div className="breakdown-list__amount">{moneyLabel(entry.total)}</div>
                      </div>
                      <div className="breakdown-list__bar-row">
                        <div className="breakdown-list__bar-wrap">
                          <div className="breakdown-list__bar" aria-hidden>
                            <span className="breakdown-list__bar-fill" />
                          </div>
                        </div>
                        <div className="breakdown-list__percent">
                          {percentLabel(entry.percentage)}
                        </div>
                      </div>
                    </li>
                  );
                })}
              </ul>
            </div>
          </aside>
        </section>
      </div>
    )}
    {hasBreakdownEntries && (
      <section className="report-kpi-row">
        <div className="report-kpis">
          <div className="report-kpi">
            <span className="report-kpi__label">Średnio miesięcznie</span>
            <strong className="report-kpi__value">{moneyLabel(averagePerMonth)}</strong>
            <span className="report-kpi__hint">
              {monthsInRange > 0 ? `${monthsInRange} mies. w zakresie` : 'Brak danych'}
            </span>
          </div>
          <div className="report-kpi">
            <span className="report-kpi__label">Średnio dziennie</span>
            <strong className="report-kpi__value">{moneyLabel(averagePerDay)}</strong>
            <span className="report-kpi__hint">
              {daysInRange > 0 ? `${daysInRange} dni w zakresie` : 'Brak danych'}
            </span>
          </div>
          {mostFrequentEntry && (
            <div className="report-kpi">
              <span className="report-kpi__label">Najczęstsza kategoria</span>
              <strong className="report-kpi__value">{mostFrequentEntry.name}</strong>
              <span className="report-kpi__hint">
                {mostFrequentEntry.count}{' '}
                {mostFrequentEntry.count === 1 ? 'transakcja' : 'transakcji'}
              </span>
            </div>
          )}
          {largestOutflow && (
            <div className="report-kpi">
              <span className="report-kpi__label">Największy wydatek</span>
              <strong className="report-kpi__value">{largestOutflowLabel}</strong>
              <span className="report-kpi__hint">
                {moneyLabel(largestOutflow.total_cents)} • {formatDate(largestOutflow.date)}
              </span>
            </div>
          )}
        </div>
      </section>
    )}
    </div>
  );
};

export default SpendingBreakdownTab;
