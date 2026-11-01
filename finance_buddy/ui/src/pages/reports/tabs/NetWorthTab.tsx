import { useCallback, useMemo } from 'react';
import { ResponsiveBar, type BarCustomLayerProps } from '@nivo/bar';
import {
  NET_ASSETS_COLOR,
  NET_DEBTS_COLOR,
  moneyLabel,
  type NetWorthBarDatum
} from '../model';
import { NetWorthTooltip } from '../tooltips';
import { useChartTheme } from '../useChartTheme';
import type { ReportsController } from '../useReports';

type NetWorthTabProps = {
  controller: ReportsController;
};

const NetWorthTab = ({ controller }: NetWorthTabProps) => {
  const { data } = controller;
  const chartTheme = useChartTheme();
  const netWorth = data?.net_worth;
  const netWorthMonths = netWorth?.months ?? [];

  const netWorthColors = useCallback(
    (bar: { id: string | number }) => (String(bar.id) === 'assets' ? NET_ASSETS_COLOR : NET_DEBTS_COLOR),
    []
  );
  const netWorthChart = useMemo(() => {
    if (!netWorthMonths.length) {
      return {
        data: [] as NetWorthBarDatum[],
        minValue: 'auto' as const,
        maxValue: 'auto' as const
      };
    }

    const dataPoints: NetWorthBarDatum[] = netWorthMonths.map((point) => ({
      month: point.month,
      label: point.label,
      assets: point.assets_cents,
      debts: point.debts_cents,
      net: point.net_worth_cents
    }));

    const values = dataPoints.flatMap((item) => [item.assets, item.debts, item.net]);
    const max = Math.max(...values, 0);
    const min = Math.min(...values, 0);
    const pad = Math.max(Math.abs(max), Math.abs(min)) * 0.12;

    return {
      data: dataPoints,
      minValue: min - pad,
      maxValue: max + pad
    };
  }, [netWorthMonths]);

  const renderNetWorthLineLayer = useCallback(
    ({ xScale, yScale }: BarCustomLayerProps<NetWorthBarDatum>) => {
      const bandwidth = typeof (xScale as any).bandwidth === 'function' ? (xScale as any).bandwidth() : 0;
      const points = netWorthChart.data
        .map((datum) => {
          const rawX = typeof (xScale as any) === 'function' ? (xScale as any)(datum.label) : null;
          const x = typeof rawX === 'number' ? rawX + bandwidth / 2 : null;
          const rawY = typeof (yScale as any) === 'function' ? (yScale as any)(datum.net) : null;
          const y = typeof rawY === 'number' ? rawY : null;
          if (x === null || y === null) return null;
          return { x, y };
        })
        .filter(Boolean) as { x: number; y: number }[];

      if (!points.length) return null;

      const pathD = points
        .map((point, index) => `${index === 0 ? 'M' : 'L'} ${point.x} ${point.y}`)
        .join(' ');

      return (
        <g>
          <path d={pathD} fill="none" stroke={chartTheme.text} strokeWidth={2} />
          {points.map((point, index) => (
            <g key={`${point.x}-${index}`}>
              <circle
                cx={point.x}
                cy={point.y}
                r={6.5}
                fill={chartTheme.surface}
                stroke={chartTheme.text}
                strokeWidth={2}
              />
              <circle cx={point.x} cy={point.y} r={2.6} fill={chartTheme.text} />
            </g>
          ))}
        </g>
      );
    },
    [netWorthChart.data, chartTheme]
  );

  return (
    <section className="card report-card report-card--full">
      <div className="report-card__header">
        <div>
          <h3>Majątek netto</h3>
          <p className="report-card__subtitle">Bilans aktywów i zobowiązań miesiąc po miesiącu.</p>
        </div>
        <div className="report-summary-grid">
          <div>
            <p>Aktywa</p>
            <strong>{moneyLabel(netWorth?.latest_assets_cents ?? 0)}</strong>
          </div>
          <div>
            <p>Zobowiązania</p>
            <strong>{moneyLabel(netWorth?.latest_debts_cents ?? 0)}</strong>
          </div>
          <div>
            <p>Netto</p>
            <strong>{moneyLabel(netWorth?.latest_net_worth_cents ?? 0)}</strong>
          </div>
        </div>
      </div>
      {netWorth?.months.length ? (
        <>
          <div className="net-worth-chart" role="img" aria-label="Netto, aktywa i zobowiązania w czasie">
            <ResponsiveBar
              data={netWorthChart.data}
              keys={['assets', 'debts']}
              indexBy="label"
              groupMode="grouped"
              padding={0.26}
              innerPadding={6}
              margin={{ top: 24, right: 16, bottom: 48, left: 86 }}
              maxValue={netWorthChart.maxValue}
              minValue={netWorthChart.minValue}
              valueScale={{ type: 'linear' }}
              indexScale={{ type: 'band', round: true }}
              colors={netWorthColors}
              enableLabel={false}
              enableGridX={false}
              enableGridY
              gridYValues={6}
              borderRadius={4}
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
              tooltip={(props) => <NetWorthTooltip {...props} />}
              valueFormat={(value) => moneyLabel(Number(value))}
              layers={['grid', 'axes', 'bars', renderNetWorthLineLayer, 'markers', 'legends']}
            />
          </div>
          <div className="spending-trends-legend" aria-label="Legenda net worth">
            <span className="spending-trends-legend__item">
              <span className="spending-trends-legend__dot" style={{ backgroundColor: NET_ASSETS_COLOR }} aria-hidden />
              <span className="spending-trends-legend__label">Aktywa</span>
            </span>
            <span className="spending-trends-legend__item">
              <span className="spending-trends-legend__dot" style={{ backgroundColor: NET_DEBTS_COLOR }} aria-hidden />
              <span className="spending-trends-legend__label">Zobowiązania</span>
            </span>
            <span className="spending-trends-legend__item">
              <span
                className="spending-trends-legend__dot spending-trends-legend__dot--line"
                style={{ backgroundColor: chartTheme.text, color: chartTheme.text }}
                aria-hidden
              />
              <span className="spending-trends-legend__label">Netto</span>
            </span>
          </div>
          <div className="table-responsive">
            <table className="table report-table">
              <thead>
                <tr>
                  <th>Miesiąc</th>
                  <th>Aktywa</th>
                  <th>Zobowiązania</th>
                  <th>Net worth</th>
                  <th>Zmiana</th>
                </tr>
              </thead>
              <tbody>
                {netWorth.months.map((point) => (
                  <tr key={point.month}>
                    <td>{point.label}</td>
                    <td>{moneyLabel(point.assets_cents)}</td>
                    <td>{moneyLabel(point.debts_cents)}</td>
                    <td>{moneyLabel(point.net_worth_cents)}</td>
                    <td className={point.change_cents >= 0 ? 'is-positive' : 'is-negative'}>
                      {moneyLabel(point.change_cents)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      ) : (
        <p className="report-empty">Brak danych o wartości netto.</p>
      )}
    </section>
  );
};

export default NetWorthTab;
