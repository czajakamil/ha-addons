// Zakładka Wydatki: miesięczne serie wydatków według tagów lub grup, bez React i DOM.
import type { IncomeExpenseMonthRow } from '../../api';
import { pluralPL } from '../../utils/format';
import {
  OTHER_CATEGORIES_LABEL,
  UNCATEGORIZED_CATEGORY_ID,
  isExcludedCategory,
  normalizeCategoryName,
  resolveTrendBucket
} from './model';

export type SpendingView = 'categories' | 'groups';

export const SPENDING_CHART_SERIES = 5;
export const SPENDING_TABLE_ROWS = 10;

export type SpendingSeries = {
  key: string;
  name: string;
  color: string;
  /** Kwoty w groszach, po jednej na miesiąc modelu. */
  values: number[];
  total: number;
  share: number;
  /** Ostatni pełny miesiąc względem średniej tej serii; null, gdy nie ma z czym porównać. */
  change: number | null;
  /** Wiersz zbiorczy „Pozostałe”: nie da się go otworzyć. */
  isOther?: boolean;
  categoryId?: string;
  groupName?: string;
};

export type SpendingMonth = { month: string; total: number; partial: boolean };

export type SpendingModel = {
  months: SpendingMonth[];
  /** Najwięksi plus zbiorcze „Pozostałe”: to, co widać na słupkach i w legendzie. */
  chartSeries: SpendingSeries[];
  rows: SpendingSeries[];
  total: number;
  /** Średnia z pełnych miesięcy; bieżący jest niepełny i zaniżałby ją. */
  average: number | null;
  completeMonths: number;
  partialMonth: SpendingMonth | null;
  compareMonth: string | null;
};

type BuildOptions = {
  months: readonly IncomeExpenseMonthRow[];
  view: SpendingView;
  /** Zawęża do jednej grupy (po kliknięciu grupy). */
  groupFilter: string | null;
  currentMonth: string;
  colorOf: (index: number) => string;
  otherColor: string;
};

const sum = (values: readonly number[]) => values.reduce((acc, value) => acc + value, 0);

const average = (values: readonly number[], complete: readonly boolean[]) => {
  const picked = values.filter((_, index) => complete[index]);
  return picked.length ? sum(picked) / picked.length : null;
};

export const buildSpending = ({
  months,
  view,
  groupFilter,
  currentMonth,
  colorOf,
  otherColor
}: BuildOptions): SpendingModel => {
  const count = months.length;
  const complete = months.map((entry) => entry.month !== currentMonth);
  const lastComplete = complete.lastIndexOf(true);
  const filter = groupFilter?.toLowerCase() ?? null;

  const byKey = new Map<string, SpendingSeries>();
  months.forEach((entry, index) => {
    entry.expense_categories.forEach((category) => {
      if (isExcludedCategory(category.category_name)) return;
      const value = Math.abs(category.total_cents);
      if (!value) return;
      const { groupName } = normalizeCategoryName(category.category_name);
      if (filter && groupName.toLowerCase() !== filter) return;
      const { key, label } = resolveTrendBucket(category, view);
      let series = byKey.get(key);
      if (!series) {
        series = {
          key,
          name: label,
          color: otherColor,
          values: new Array<number>(count).fill(0),
          total: 0,
          share: 0,
          change: null,
          categoryId: category.category_id ?? UNCATEGORIZED_CATEGORY_ID,
          groupName
        };
        byKey.set(key, series);
      }
      series.values[index] += value;
      series.total += value;
    });
  });

  const ranked = Array.from(byKey.values()).sort((a, b) => b.total - a.total);
  const total = sum(ranked.map((series) => series.total));
  ranked.forEach((series, index) => {
    if (index < SPENDING_CHART_SERIES) series.color = colorOf(index);
  });

  const finish = (series: SpendingSeries): SpendingSeries => {
    const mean = average(series.values, complete);
    return {
      ...series,
      share: total ? series.total / total : 0,
      change:
        mean && lastComplete >= 0 ? (series.values[lastComplete] - mean) / mean : null
    };
  };

  const aggregate = (rest: SpendingSeries[], name: string): SpendingSeries =>
    finish({
      key: 'other',
      name,
      color: otherColor,
      values: months.map((_, index) => sum(rest.map((series) => series.values[index]))),
      total: sum(rest.map((series) => series.total)),
      share: 0,
      change: null,
      isOther: true
    });

  const chartRest = ranked.slice(SPENDING_CHART_SERIES);
  const chartSeries = ranked.slice(0, SPENDING_CHART_SERIES).map(finish);
  if (chartRest.length) chartSeries.push(aggregate(chartRest, OTHER_CATEGORIES_LABEL));

  const tableRest = ranked.slice(SPENDING_TABLE_ROWS);
  const rows = ranked.slice(0, SPENDING_TABLE_ROWS).map(finish);
  if (tableRest.length) {
    const n = tableRest.length;
    const word =
      view === 'groups'
        ? pluralPL(n, 'grupa', 'grupy', 'grup')
        : pluralPL(n, 'kategoria', 'kategorie', 'kategorii');
    rows.push(aggregate(tableRest, `${OTHER_CATEGORIES_LABEL} · ${n} ${word}`));
  }

  const monthTotals = months.map((_, index) => sum(ranked.map((series) => series.values[index])));
  const spendingMonths = months.map<SpendingMonth>((entry, index) => ({
    month: entry.month,
    total: monthTotals[index],
    partial: !complete[index]
  }));

  return {
    months: spendingMonths,
    chartSeries,
    rows,
    total,
    average: average(monthTotals, complete),
    completeMonths: complete.filter(Boolean).length,
    partialMonth: spendingMonths.find((entry) => entry.partial) ?? null,
    compareMonth: lastComplete >= 0 ? months[lastComplete].month : null
  };
};

export const wholeAmount = new Intl.NumberFormat('pl-PL', {
  maximumFractionDigits: 0,
  useGrouping: 'always' as unknown as boolean
});

export const wholeMoneyLabel = (cents: number) => `${wholeAmount.format(cents / 100)} zł`;

const NICE_STEPS = [1, 2, 2.5, 5, 10];

/** Skala osi: krok z „ładnych” liczb, tak by osi wystarczyło ok. 3–4 linii. */
export const niceScale = (maxCents: number) => {
  const max = Math.max(maxCents, 1);
  const raw = max / 3;
  const magnitude = 10 ** Math.floor(Math.log10(raw));
  const step = (NICE_STEPS.find((factor) => factor * magnitude >= raw) ?? 10) * magnitude;
  const top = Math.ceil(max / step) * step;
  const ticks: number[] = [];
  for (let value = 0; value <= top; value += step) ticks.push(value);
  return { top, ticks };
};
