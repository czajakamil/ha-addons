import { CSSProperties, useMemo, useState } from 'react';
import MonthBars from '../../../components/ui/MonthBars';
import { monthRangeLabel } from '../../../utils/months';
import {
  SPENDING_TRENDS_CATEGORY_LIMIT,
  SPENDING_TRENDS_OTHER_LABEL,
  UNCATEGORIZED_CATEGORY_NAME,
  isExcludedCategory,
  moneyLabel,
  percentLabel,
  reportMonthLabel,
  resolveTrendBucket
} from '../model';
import ReportBarList, { type ReportBarRow } from '../components/ReportBarList';
import type { ReportsController } from '../useReports';
import ResizableTable from '../../../components/ui/ResizableTable';
import { useVisibleMonths } from '../../../hooks/useVisibleMonths';

type SpendingTrendsTabProps = {
  controller: ReportsController;
};

const currentMonthKey = () => {
  const now = new Date();
  return `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
};

const SpendingTrendsTab = ({ controller }: SpendingTrendsTabProps) => {
  const { data } = controller;
  const [spendingTrendView, setSpendingTrendView] = useState<'categories' | 'groups'>('categories');
  const [selectedMonth, setSelectedMonth] = useState<string | null>(null);

  const averageMonthlySpendingCents = data?.spending_trends.average_monthly_spending_cents ?? 0;
  const totalSpendingCents = data?.spending_trends.total_spending_cents ?? 0;
  const trendAverage = data?.spending_trends.average_monthly_spending_cents ?? 0;

  const monthBars = useMemo(
    () =>
      (data?.spending_trends?.months ?? []).map((point) => ({
        month: point.month,
        cents: Math.abs(point.total_spending_cents)
      })),
    [data?.spending_trends?.months]
  );
  const visibleBars = useVisibleMonths(monthBars);
  // Zmiana filtrów może zabrać wybrany miesiąc z zakresu; wtedy lista wraca do całego okresu
  const activeMonth = visibleBars.some((bar) => bar.month === selectedMonth) ? selectedMonth : null;
  const hasTrendChartData = visibleBars.length > 0;
  const spendingToggleIndex = spendingTrendView === 'categories' ? 0 : 1;

  // Kwoty sumowane z pełnego zakresu albo z wybranego miesiąca, po tagach lub grupach
  const structureRows = useMemo<ReportBarRow[]>(() => {
    const totals = new Map<string, { name: string; total: number }>();
    (data?.income_vs_expense?.months ?? [])
      .filter((month) => !activeMonth || month.month === activeMonth)
      .forEach((month) => {
        month.expense_categories.forEach((category) => {
          if (isExcludedCategory(category.category_name)) return;
          const value = Math.abs(category.total_cents);
          if (!value) return;
          const { key, label } = resolveTrendBucket(category, spendingTrendView);
          const existing = totals.get(key) ?? { name: label || UNCATEGORIZED_CATEGORY_NAME, total: 0 };
          existing.total += value;
          totals.set(key, existing);
        });
      });

    const sorted = Array.from(totals.entries()).sort((a, b) => b[1].total - a[1].total);
    const sum = sorted.reduce((acc, [, entry]) => acc + entry.total, 0);
    const toRow = (key: string, name: string, total: number) => ({
      key,
      name,
      total,
      percentage: sum > 0 ? total / sum : 0
    });
    const rows = sorted
      .slice(0, SPENDING_TRENDS_CATEGORY_LIMIT)
      .map(([key, entry]) => toRow(key, entry.name, entry.total));
    const restTotal = sorted
      .slice(SPENDING_TRENDS_CATEGORY_LIMIT)
      .reduce((acc, [, entry]) => acc + entry.total, 0);
    return restTotal > 0 ? [...rows, toRow('other', SPENDING_TRENDS_OTHER_LABEL, restTotal)] : rows;
  }, [data?.income_vs_expense?.months, activeMonth, spendingTrendView]);

  const spendingTrendRows = useMemo(() => {
    if (!data?.spending_trends?.months?.length) return [];

    return data.spending_trends.months
      .map((point) => {
        const total = Math.abs(point.total_spending_cents);
        const diff = total - trendAverage;
        const percent = trendAverage ? diff / trendAverage : 0;
        return { ...point, total, diff, percent };
      })
      .sort((a, b) => b.month.localeCompare(a.month));
  }, [data?.spending_trends?.months, trendAverage]);

  return (
    <section className="card report-card report-card--full">
      <div className="report-card__header">
        <div>
          <h3>Trendy wydatków</h3>
          <p className="report-card__subtitle">
            Suma wydatków w każdym miesiącu. Wybierz słupek, żeby zobaczyć jego strukturę.
          </p>
        </div>
        <div className="spending-trends-summary">
          <div className="spending-trends-summary__item">
            <p>Średnie wydatki / mies.</p>
            <strong>{moneyLabel(averageMonthlySpendingCents)}</strong>
          </div>
          <div className="spending-trends-summary__item">
            <p>Łączne wydatki</p>
            <strong>{moneyLabel(totalSpendingCents)}</strong>
          </div>
        </div>
      </div>
      {!hasTrendChartData ? (
        <p className="report-empty">Brak trendów do pokazania.</p>
      ) : (
        <>
          <p className="report-chart-range">
            {monthRangeLabel(visibleBars[0].month, visibleBars[visibleBars.length - 1].month)}
          </p>
          <MonthBars
            months={visibleBars}
            highlighted={activeMonth ?? currentMonthKey()}
            selected={activeMonth}
            onSelect={(month) => setSelectedMonth(month === activeMonth ? null : month)}
            ariaLabel="Wybierz miesiąc"
          />
          <div className="spending-trends-structure">
            <div className="spending-trends-controls">
              <h4 className="breakdown-layout__title">
                {activeMonth ? `Struktura: ${reportMonthLabel(activeMonth)}` : 'Struktura: cały zakres'}
              </h4>
              <div
                className="breakdown-toggle"
                role="group"
                aria-label="Wybierz widok wydatków"
                style={{ ['--toggle-index' as const]: spendingToggleIndex } as CSSProperties}
              >
                <span className="breakdown-toggle__thumb" aria-hidden />
                <button
                  type="button"
                  className={`breakdown-toggle__option${
                    spendingTrendView === 'categories' ? ' is-active' : ''
                  }`}
                  onClick={() => setSpendingTrendView('categories')}
                >
                  Tagi
                </button>
                <button
                  type="button"
                  className={`breakdown-toggle__option${
                    spendingTrendView === 'groups' ? ' is-active' : ''
                  }`}
                  onClick={() => setSpendingTrendView('groups')}
                >
                  Grupy
                </button>
              </div>
            </div>
            {structureRows.length > 0 ? (
              <ReportBarList
                title={spendingTrendView === 'categories' ? 'Tagi' : 'Grupy'}
                rows={structureRows}
              />
            ) : (
              <p className="report-empty">Brak wydatków w tym miesiącu.</p>
            )}
          </div>
          <div className="table-responsive">
            <ResizableTable tableId="report-trends" className="table report-table spending-trends-table">
              <thead>
                <tr>
                  <th>Miesiąc</th>
                  <th>Suma wydatków</th>
                  <th>
                    Porównanie do średniej
                    <span className="spending-trends-note">
                      Średnia: {moneyLabel(trendAverage)}
                    </span>
                  </th>
                </tr>
              </thead>
              <tbody>
                {spendingTrendRows.map((row) => {
                  const isAbove = row.diff >= 0;
                  return (
                    <tr key={row.month}>
                      <td>{reportMonthLabel(row.month)}</td>
                      <td>{moneyLabel(row.total)}</td>
                      <td>
                        {row.diff === 0 ? (
                          <span className="spending-trends-diff is-neutral">—</span>
                        ) : (
                          <span className="spending-trends-diff">
                            <span aria-hidden="true">{isAbove ? '↑' : '↓'} </span>
                            {moneyLabel(Math.abs(row.diff))} ({percentLabel(Math.abs(row.percent))})
                          </span>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </ResizableTable>
          </div>
        </>
      )}
    </section>
  );
};

export default SpendingTrendsTab;
