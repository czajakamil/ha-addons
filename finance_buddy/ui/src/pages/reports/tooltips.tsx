// Chart tooltip components for ReportsPage (nivo pie/bar). Pure presentational —
// extracted from ReportsPage.tsx alongside the model layer.
import type { BarTooltipProps } from '@nivo/bar';
import type { PieTooltipProps } from '@nivo/pie';
import {
  moneyLabel,
  percentLabel,
  netAssetsColor,
  otherCategoryColor,
  netDebtsColor,
  type BreakdownPieDatum,
  type NetWorthBarDatum,
  type TrendBarDatum,
  type TrendCategoryLegend
} from './model';

export const BreakdownPieTooltip = ({ datum }: PieTooltipProps<BreakdownPieDatum>) => (
  <div className="report-tooltip report-tooltip--pie">
    <div className="report-tooltip__title report-tooltip__title--sm">{datum.label}</div>
    <div>{moneyLabel(datum.data.total)}</div>
    <div className="report-tooltip__muted report-tooltip__muted--sm">{percentLabel(datum.data.percentage)}</div>
  </div>
);

export type SpendingTrendsTooltipProps = BarTooltipProps<TrendBarDatum> & {
  legendLookup: Map<string, TrendCategoryLegend>;
};

export const SpendingTrendsTooltip = ({ data, legendLookup }: SpendingTrendsTooltipProps) => {
  const datum = data;
  const total = Number(datum.total) || 0;
  const breakdownKeys = Array.from(legendLookup.keys());

  const entries = breakdownKeys
    .map((key) => {
      const value = Number((datum as Record<string, number | string>)[key] ?? 0);
      if (value <= 0) return null;
      const meta = legendLookup.get(key);
      return {
        key,
        label: meta?.label ?? key,
        color: meta?.color ?? otherCategoryColor(),
        value
      };
    })
    .filter(Boolean)
    .sort((a, b) => (b?.value ?? 0) - (a?.value ?? 0)) as {
    key: string;
    label: string;
    color: string;
    value: number;
  }[];

  return (
    <div className="report-tooltip report-tooltip--trends">
      <div className="report-tooltip__title">{datum.label}</div>
      <div className="report-tooltip__subtotal">Łącznie {moneyLabel(total)}</div>
      <ul className="report-tooltip__list">
        {entries.map((entry) => (
          <li key={entry.key} className="report-tooltip__row">
            <span aria-hidden className="report-tooltip__dot" style={{ background: entry.color }} />
            <span className="report-tooltip__row-label">{entry.label}</span>
            <span className="report-tooltip__row-value">{moneyLabel(entry.value)}</span>
          </li>
        ))}
        {entries.length === 0 && <li className="report-tooltip__muted">Brak wydatków.</li>}
      </ul>
    </div>
  );
};

export const NetWorthTooltip = ({ data }: BarTooltipProps<NetWorthBarDatum>) => {
  const assets = Number(data.assets) || 0;
  const debts = Number(data.debts) || 0;
  const net = Number(data.net) || assets + debts;

  return (
    <div className="report-tooltip report-tooltip--networth">
      <div className="report-tooltip__title">{data.label}</div>
      <ul className="report-tooltip__list">
        <li className="report-tooltip__row report-tooltip__row--between">
          <span>Aktywa</span>
          <strong style={{ color: netAssetsColor() }}>{moneyLabel(assets)}</strong>
        </li>
        <li className="report-tooltip__row report-tooltip__row--between">
          <span>Zobowiązania</span>
          <strong style={{ color: netDebtsColor() }}>{moneyLabel(debts)}</strong>
        </li>
        <li className="report-tooltip__row report-tooltip__row--between">
          <span>Netto</span>
          <strong>{moneyLabel(net)}</strong>
        </li>
      </ul>
    </div>
  );
};
