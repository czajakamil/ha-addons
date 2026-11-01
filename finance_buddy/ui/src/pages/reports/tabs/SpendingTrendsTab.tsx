import { CSSProperties, useCallback, useMemo, useState } from 'react';
import { ResponsiveBar, type BarCustomLayerProps } from '@nivo/bar';
import type { IncomeExpenseMonthRow } from '../../../api';
import {
  BREAKDOWN_CHART_COLORS,
  OTHER_CATEGORY_COLOR,
  SPENDING_TRENDS_CATEGORY_LIMIT,
  SPENDING_TRENDS_OTHER_KEY,
  SPENDING_TRENDS_OTHER_LABEL,
  UNCATEGORIZED_CATEGORY_NAME,
  isExcludedCategory,
  moneyLabel,
  resolveTrendBucket,
  type TrendBarDatum,
  type TrendCategoryLegend
} from '../model';
import { SpendingTrendsTooltip } from '../tooltips';
import { useChartTheme } from '../useChartTheme';
import type { ReportsController } from '../useReports';

type SpendingTrendsTabProps = {
  controller: ReportsController;
};

const SpendingTrendsTab = ({ controller }: SpendingTrendsTabProps) => {
  const { data } = controller;
  const chartTheme = useChartTheme();
  const [spendingTrendView, setSpendingTrendView] = useState<'categories' | 'groups'>('categories');

  const averageMonthlySpendingCents = data?.spending_trends.average_monthly_spending_cents ?? 0;
  const totalSpendingCents = data?.spending_trends.total_spending_cents ?? 0;
  const trendAverage = data?.spending_trends.average_monthly_spending_cents ?? 0;

  const spendingTrendCategoryTotals = useMemo(() => {
    const totals = new Map<string, { name: string; total: number }>();
    if (!data?.income_vs_expense?.months) {
      return totals;
    }

    data.income_vs_expense.months.forEach((month) => {
      month.expense_categories.forEach((category) => {
        if (isExcludedCategory(category.category_name)) {
          return;
        }
        const { key, label } = resolveTrendBucket(category, spendingTrendView);
        const value = Math.abs(category.total_cents);
        if (!value) return;
        const existing = totals.get(key) ?? { name: label, total: 0 };
        existing.total += value;
        totals.set(key, existing);
      });
    });

    return totals;
  }, [data?.income_vs_expense?.months, spendingTrendView]);

  const spendingTrendChart = useMemo(() => {
    if (!data?.spending_trends?.months?.length) {
      return { data: [] as TrendBarDatum[], keys: [] as string[], legend: [] as TrendCategoryLegend[], maxValue: 0 };
    }

    const expenseMonths = data.income_vs_expense?.months ?? [];
    const monthLookup = new Map<string, IncomeExpenseMonthRow>(
      expenseMonths.map((month) => [month.month, month])
    );

    const sortedCategories = Array.from(spendingTrendCategoryTotals.entries()).sort(
      (a, b) => b[1].total - a[1].total
    );
    const baseCategories = sortedCategories.slice(0, SPENDING_TRENDS_CATEGORY_LIMIT);

    const legend: TrendCategoryLegend[] = baseCategories.map(([key, meta], index) => ({
      key,
      label: meta.name || UNCATEGORIZED_CATEGORY_NAME,
      color: BREAKDOWN_CHART_COLORS[index % BREAKDOWN_CHART_COLORS.length]
    }));
    const keys = legend.map((entry) => entry.key);

    const dataPoints: TrendBarDatum[] = data.spending_trends.months.map((point) => {
      const monthRow = monthLookup.get(point.month);
      const datum: TrendBarDatum = {
        month: point.month,
        label: point.label,
        total: 0
      };
      let otherTotal = 0;
      let stackTotal = 0;

      monthRow?.expense_categories.forEach((category) => {
        if (isExcludedCategory(category.category_name)) {
          return;
        }
        const bucket = resolveTrendBucket(category, spendingTrendView);
        const value = Math.abs(category.total_cents);
        if (!value) return;
        if (keys.includes(bucket.key)) {
          datum[bucket.key] = value;
        } else {
          otherTotal += value;
        }
        stackTotal += value;
      });

      if (otherTotal > 0) {
        datum[SPENDING_TRENDS_OTHER_KEY] = otherTotal;
        stackTotal += otherTotal;
      }

      datum.total = stackTotal;

      return datum;
    });

    if (
      dataPoints.some(
        (item) => Number((item as Record<string, number | string>)[SPENDING_TRENDS_OTHER_KEY] ?? 0) > 0
      )
    ) {
      legend.push({
        key: SPENDING_TRENDS_OTHER_KEY,
        label: SPENDING_TRENDS_OTHER_LABEL,
        color: OTHER_CATEGORY_COLOR
      });
      keys.push(SPENDING_TRENDS_OTHER_KEY);
    }

    if (keys.length === 0) {
      const fallbackKey = 'wydatki';
      dataPoints.forEach((item) => {
        item[fallbackKey] = item.total;
      });
      legend.push({
        key: fallbackKey,
        label: 'Wydatki',
        color: BREAKDOWN_CHART_COLORS[0]
      });
      keys.push(fallbackKey);
    }

    const maxValue = dataPoints.reduce(
      (max, item) => Math.max(max, Number(item.total) || 0),
      0
    );

    return { data: dataPoints, keys, legend, maxValue };
  }, [
    data?.income_vs_expense?.months,
    data?.spending_trends?.months,
    spendingTrendCategoryTotals,
    spendingTrendView
  ]);

  const spendingTrendLegendMap = useMemo(
    () => new Map(spendingTrendChart.legend.map((entry) => [entry.key, entry])),
    [spendingTrendChart.legend]
  );
  const spendingTrendsMaxValue = spendingTrendChart.maxValue || 0;
  const spendingTrendsChartMax = spendingTrendsMaxValue > 0 ? spendingTrendsMaxValue * 1.12 : 'auto';
  const hasTrendChartData = spendingTrendChart.data.length > 0;
  const spendingToggleIndex = spendingTrendView === 'categories' ? 0 : 1;
  const spendingTrendColors = useCallback(
    (bar: { id: string | number }) =>
      spendingTrendLegendMap.get(String(bar.id))?.color ?? OTHER_CATEGORY_COLOR,
    [spendingTrendLegendMap]
  );
  const renderTrendLineLayer = useCallback(
    ({ bars }: BarCustomLayerProps<TrendBarDatum>) => {
      if (!bars.length) {
        return null;
      }

      const topPoints = new Map<string, { x: number; y: number }>();
      bars.forEach((bar) => {
        const key = String(bar.data.indexValue);
        const top = { x: bar.x + bar.width / 2, y: bar.y };
        const existing = topPoints.get(key);
        if (!existing || top.y < existing.y) {
          topPoints.set(key, top);
        }
      });

      const points = Array.from(topPoints.values()).sort((a, b) => a.x - b.x);
      if (!points.length) return null;

      const pathD = points
        .map((point, index) => `${index === 0 ? 'M' : 'L'} ${point.x} ${point.y}`)
        .join(' ');

      return (
        <g>
          <path d={pathD} fill="none" stroke={chartTheme.muted} strokeWidth={2} />
          {points.map((point, index) => (
            <g key={`${point.x}-${index}`}>
              <circle
                cx={point.x}
                cy={point.y}
                r={6.5}
                fill={chartTheme.surface}
                stroke={chartTheme.muted}
                strokeWidth={2}
              />
              <circle cx={point.x} cy={point.y} r={2.6} fill={chartTheme.text} />
            </g>
          ))}
        </g>
      );
    },
    [chartTheme]
  );

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
            Miesięczne wydatki według kategorii z linią sumy.
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
          <div className="spending-trends-controls" role="group" aria-label="Wybierz widok wydatków">
            <div
              className="breakdown-toggle"
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
          <div className="spending-trends-chart" role="img" aria-label="Miesięczne wydatki w czasie">
            <ResponsiveBar
              data={spendingTrendChart.data}
              keys={spendingTrendChart.keys}
              indexBy="label"
              margin={{ top: 24, right: 16, bottom: 48, left: 86 }}
              padding={0.32}
              maxValue={spendingTrendsChartMax}
              valueScale={{ type: 'linear' }}
              indexScale={{ type: 'band', round: true }}
              colors={spendingTrendColors}
              enableLabel={false}
              enableGridX={false}
              enableGridY
              gridYValues={6}
              axisLeft={{
                tickSize: 0,
                tickPadding: 10,
                format: (value) => moneyLabel(Number(value)),
                legendOffset: -70
              }}
              axisBottom={{
                tickSize: 0,
                tickPadding: 12,
                tickRotation: -15
              }}
              theme={chartTheme.nivoTheme}
              tooltip={(props) => (
                <SpendingTrendsTooltip {...props} legendLookup={spendingTrendLegendMap} />
              )}
              valueFormat={(value) => moneyLabel(Number(value))}
              layers={[
                'grid',
                'axes',
                'bars',
                (layerProps) => renderTrendLineLayer(layerProps),
                'markers',
                'legends'
              ]}
            />
          </div>
          <div className="spending-trends-legend" aria-label="Legenda kategorii wydatków">
            {spendingTrendChart.legend.map((entry) => (
              <span key={entry.key} className="spending-trends-legend__item">
                <span
                  className="spending-trends-legend__dot"
                  style={{ backgroundColor: entry.color }}
                  aria-hidden
                />
                <span className="spending-trends-legend__label">{entry.label}</span>
              </span>
            ))}
          </div>
          <div className="table-responsive">
            <table className="table report-table spending-trends-table">
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
                  const isPositive = row.diff >= 0;
                  const diffLabel = moneyLabel(Math.abs(row.diff));
                  const percentText = row.percent
                    ? `${(Math.abs(row.percent) * 100).toFixed(1)}%`
                    : '0.0%';
                  return (
                    <tr key={row.month}>
                      <td>{row.label}</td>
                      <td>{moneyLabel(row.total)}</td>
                      <td>
                        {row.diff === 0 ? (
                          <span className="spending-trends-diff is-neutral">—</span>
                        ) : (
                          <span className={`spending-trends-diff ${isPositive ? 'is-positive' : 'is-negative'}`}>
                            <span aria-hidden="true">{isPositive ? '↑' : '↓'} </span>
                            {diffLabel} ({percentText})
                          </span>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </>
      )}
    </section>
  );
};

export default SpendingTrendsTab;
