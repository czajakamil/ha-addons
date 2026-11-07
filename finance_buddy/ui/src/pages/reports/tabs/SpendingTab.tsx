import { useMemo, useState } from 'react';
import { ChevronDown } from 'lucide-react';
import Button from '../../../components/Button';
import DataTable from '../../../components/DataTable';
import SegmentedControl from '../../../components/ui/SegmentedControl';
import { useMediaQuery } from '../../../hooks/useMediaQuery';
import { pluralPL } from '../../../utils/format';
import { monthDate } from '../../../utils/months';
import { chartColor } from '../../../utils/tokens';
import Sparkline from '../components/Sparkline';
import SpendingChart from '../components/SpendingChart';
import {
  UNCATEGORIZED_CATEGORY_ID,
  formatDate,
  moneyLabel,
  otherCategoryColor,
  percentLabel,
  reportMonthLabel
} from '../model';
import { buildSpending, wholeMoneyLabel, type SpendingSeries, type SpendingView } from '../spending';
import { useBreakdownEditing } from '../useBreakdownEditing';
import type { ReportsController } from '../useReports';

type SpendingTabProps = {
  controller: ReportsController;
};

const longMonth = new Intl.DateTimeFormat('pl-PL', { month: 'long' });
const shortMonth = new Intl.DateTimeFormat('pl-PL', { month: 'short' });
const capitalize = (value: string) => value.charAt(0).toUpperCase() + value.slice(1);

const monthKey = (date: Date) =>
  `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;

const changeLabel = (change: number | null) => {
  if (change === null) return '—';
  return `${change >= 0 ? '↑' : '↓'} ${Math.abs(Math.round(change * 100))}%`;
};

const SpendingTab = ({ controller }: SpendingTabProps) => {
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

  const isCompact = useMediaQuery('(max-width: 699px)');
  const [view, setView] = useState<SpendingView>('categories');
  const [groupFilter, setGroupFilter] = useState<string | null>(null);
  // Rozwinięcie dotyczy jednego podziału; po zmianie widoku w „Pozostałych” jest co innego
  const othersScope = `${view}:${groupFilter ?? ''}`;
  const [expandedScope, setExpandedScope] = useState<string | null>(null);
  const othersExpanded = expandedScope === othersScope;

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

  const today = useMemo(() => new Date(), []);
  const model = useMemo(
    () =>
      buildSpending({
        months: data?.income_vs_expense.months ?? [],
        view,
        groupFilter,
        currentMonth: monthKey(today),
        colorOf: chartColor,
        otherColor: otherCategoryColor()
      }),
    [data?.income_vs_expense.months, view, groupFilter, today]
  );

  const rangeLabel =
    data !== null ? `${formatDate(data.start_date)} – ${formatDate(data.end_date)}` : '';
  const monthCount = model.months.length;
  const partial = model.partialMonth;
  const daysInMonth = new Date(today.getFullYear(), today.getMonth() + 1, 0).getDate();
  const compareLabel = model.compareMonth
    ? capitalize(shortMonth.format(monthDate(model.compareMonth)))
    : null;

  // „Pozostałe” potrafią być większe od największej kategorii
  const barBase = Math.max(...model.rows.map((row) => row.total), 1);

  const openRow = (row: SpendingSeries) => {
    if (row.isOther) return;
    if (view === 'groups') {
      setGroupFilter(row.groupName ?? row.name);
      setView('categories');
      setSelectedBreakdownCategory(null);
      return;
    }
    setSelectedBreakdownCategory({
      id: row.categoryId ?? UNCATEGORIZED_CATEGORY_ID,
      name: row.name,
      color: row.color
    });
  };

  const changeView = (next: SpendingView) => {
    setView(next);
    setSelectedBreakdownCategory(null);
    if (next === 'groups') setGroupFilter(null);
  };

  const transactionsPanel = selectedBreakdownCategory && (
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
        {breakdownEditError && <p className="breakdown-transactions__error">{breakdownEditError}</p>}
        {breakdownTransactionsLoading ? (
          <p className="report-empty">Ładowanie transakcji...</p>
        ) : breakdownTransactionsError ? (
          <p className="report-empty">{breakdownTransactionsError}</p>
        ) : breakdownTransactions.length === 0 ? (
          <p className="report-empty">Brak transakcji w tej kategorii.</p>
        ) : (
          <DataTable
            tableId="report-breakdown"
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
  );

  if (model.rows.length === 0) {
    return (
      <div className="spending-tab">
        {transactionsPanel}
        {groupFilter && (
          <div className="breakdown-filter">
            <span className="breakdown-filter__label">Filtrowane po grupie:</span>
            <span className="breakdown-filter__value">{groupFilter}</span>
            <button type="button" className="breakdown-filter__clear" onClick={() => setGroupFilter(null)}>
              Wyczyść
            </button>
          </div>
        )}
        <section className="card report-card">
          <p className="report-empty">Brak wydatków w wybranym okresie.</p>
        </section>
      </div>
    );
  }

  return (
    <div className="spending-tab">
      {transactionsPanel}

      <section className="spending-kpis" aria-label="Podsumowanie wydatków">
        <div className="spending-kpi spending-kpi--main">
          <span className="spending-kpi__label">Wydano w {monthCount} mies.</span>
          <strong className="spending-kpi__value">{moneyLabel(model.total)}</strong>
        </div>
        <div className="spending-kpi">
          <span className="spending-kpi__label">{isCompact ? 'Średnio / mies.' : 'Średnio miesięcznie'}</span>
          <strong className="spending-kpi__value">
            {model.average === null ? '—' : wholeMoneyLabel(model.average)}
          </strong>
          <span className="spending-kpi__hint">
            {model.completeMonths === 0
              ? 'brak pełnego miesiąca w zakresie'
              : partial
                ? `${model.completeMonths} ${pluralPL(model.completeMonths, 'pełny miesiąc', 'pełne miesiące', 'pełnych miesięcy')}, bez bieżącego`
                : `${model.completeMonths} mies. w zakresie`}
          </span>
        </div>
        {partial && (
          <div className="spending-kpi">
            <span className="spending-kpi__label">
              {capitalize(longMonth.format(monthDate(partial.month)))} · w toku
            </span>
            <strong className="spending-kpi__value">{moneyLabel(partial.total)}</strong>
            <span className="spending-kpi__hint">
              dzień {today.getDate()} z {daysInMonth}
            </span>
          </div>
        )}
      </section>

      <SpendingChart model={model} onSelect={openRow} />

      <section className="spending-breakdown">
        <div className="spending-breakdown__toolbar">
          <SegmentedControl
            label="Podział wydatków"
            value={view}
            options={[
              { value: 'categories', label: 'Kategorie' },
              { value: 'groups', label: 'Grupy' }
            ]}
            onChange={changeView}
          />
          <span className="spending-breakdown__hint">
            {view === 'groups' ? 'Kliknij grupę, aby zobaczyć jej kategorie' : 'Kliknij wiersz, aby zobaczyć transakcje'}
          </span>
        </div>
        {groupFilter && (
          <div className="breakdown-filter">
            <span className="breakdown-filter__label">Filtrowane po grupie:</span>
            <span className="breakdown-filter__value">{groupFilter}</span>
            <button type="button" className="breakdown-filter__clear" onClick={() => setGroupFilter(null)}>
              Wyczyść
            </button>
          </div>
        )}
        <div className="card spending-table">
          <div className="spending-table__scroll">
            <div className="spending-table__inner">
              <div className="spending-row spending-row--head" aria-hidden="true">
                <span>{view === 'groups' ? 'Grupa' : 'Kategoria'}</span>
                <span>Udział</span>
                <span>Kwota</span>
                <span>%</span>
                <span>{compareLabel ? `${compareLabel} vs śr.` : 'vs śr.'}</span>
                <span>{monthCount} mies.</span>
              </div>
              <ul className="spending-table__list">
                {model.rows.flatMap((row) => {
                  const renderRow = (item: SpendingSeries, isChild: boolean) => {
                    const content = (
                      <>
                        <span className="spending-row__name">
                          <span className="spending-row__dot" style={{ background: item.color }} aria-hidden="true" />
                          <span className="spending-row__label">{item.name}</span>
                          {item.isOther && (
                            <ChevronDown
                              className={`spending-row__chevron${othersExpanded ? ' is-open' : ''}`}
                              size={14}
                              aria-hidden="true"
                            />
                          )}
                        </span>
                        <span className="spending-row__bar" aria-hidden="true">
                          <span
                            style={{
                              width: `${Math.max(1, (item.total / barBase) * 100)}%`,
                              background: item.color
                            }}
                          />
                        </span>
                        <span className="spending-row__amount">{moneyLabel(item.total)}</span>
                        <span className="spending-row__share">
                          {percentLabel(item.share)}
                          <span className="spending-row__suffix"> wydatków</span>
                        </span>
                        <span className="spending-row__change">
                          {compareLabel && (
                            <span className="spending-row__suffix">{compareLabel.toLowerCase()} vs śr. </span>
                          )}
                          {changeLabel(item.change)}
                        </span>
                        <Sparkline months={model.months} values={item.values} color={item.color} />
                      </>
                    );
                    return (
                      <li key={item.key}>
                        <button
                          type="button"
                          className={`spending-row is-clickable${isChild ? ' spending-row--child' : ''}`}
                          aria-expanded={item.isOther ? othersExpanded : undefined}
                          onClick={() =>
                            item.isOther
                              ? setExpandedScope(othersExpanded ? null : othersScope)
                              : openRow(item)
                          }
                        >
                          {content}
                        </button>
                      </li>
                    );
                  };
                  const children = othersExpanded ? (row.children ?? []) : [];
                  return [renderRow(row, false), ...children.map((child) => renderRow(child, true))];
                })}
              </ul>
            </div>
          </div>
        </div>
        <p className="spending-breakdown__note">
          Bez przelewów między kontami i korekt salda.
          {partial && ` Miesiąc w toku (${reportMonthLabel(partial.month)}) nie liczy się do średniej.`}
        </p>
      </section>
    </div>
  );
};

export default SpendingTab;
