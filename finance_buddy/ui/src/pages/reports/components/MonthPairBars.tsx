// Dwie wielkości w każdym miesiącu: para słupków, a nad nią wynik. Opcjonalnie wynik jako linia z kropkami.
import type { CSSProperties } from 'react';
import { useVisibleMonths } from '../../../hooks/useVisibleMonths';
import { monthDate, fullMonthName } from '../../../utils/months';
import { moneyLabel } from '../model';

const MAX_LABELED_MONTHS = 14;

const monthName = new Intl.DateTimeFormat('pl-PL', { month: 'short' });
const wholeAmount = new Intl.NumberFormat('pl-PL', {
  maximumFractionDigits: 0,
  useGrouping: 'always' as unknown as boolean
});

export type MonthPairPoint = { month: string; first_cents: number; second_cents: number; result_cents: number };

export type MonthPairSeries = { label: string; modifier?: 'income' | 'negative' };

type MonthPairBarsProps = {
  months: readonly MonthPairPoint[];
  first: MonthPairSeries;
  second: MonthPairSeries;
  resultLabel: string;
  showResultLine?: boolean;
};

const resultText = (cents: number) => {
  if (!cents) return '0';
  return `${cents > 0 ? '+' : '−'}${wholeAmount.format(Math.abs(cents) / 100)} zł`;
};

const MonthPairBars = ({ months, first, second, resultLabel, showResultLine = false }: MonthPairBarsProps) => {
  const visible = useVisibleMonths(months);
  const max = Math.max(...visible.flatMap((entry) => [Math.abs(entry.first_cents), Math.abs(entry.second_cents)]), 1);
  // Ujemny wynik schodzi poniżej zera, więc oś musi zrobić dla niego miejsce pod słupkami
  const min = showResultLine ? Math.min(...visible.map((entry) => entry.result_cents), 0) : 0;
  const range = max - min;
  const belowZero = -min / range;
  const resultPoint = (entry: MonthPairPoint, index: number) => ({
    x: ((index + 0.5) / visible.length) * 100,
    y: ((max - entry.result_cents) / range) * 100
  });
  const resultPoints = showResultLine ? visible.map(resultPoint) : [];
  const isDense = visible.length > MAX_LABELED_MONTHS;
  const step = Math.ceil(visible.length / MAX_LABELED_MONTHS);
  const barHeight = (cents: number) => (cents ? `max(2px, ${(Math.abs(cents) / max) * 100}%)` : 0);
  const titleOf = (entry: MonthPairPoint) =>
    `${fullMonthName.format(monthDate(entry.month))}: ${first.label.toLowerCase()} ${moneyLabel(Math.abs(entry.first_cents))}, ${second.label.toLowerCase()} ${moneyLabel(Math.abs(entry.second_cents))}`;
  const seriesClass = (base: string, series: MonthPairSeries) =>
    series.modifier ? `${base} ${base}--${series.modifier}` : base;

  return (
    <>
      <div
        className="month-bars"
        role="img"
        aria-label={visible.map(titleOf).join('; ')}
        style={{ ['--bars-below-zero' as const]: belowZero } as CSSProperties}
      >
        {visible.map((entry, index) => (
          <div key={entry.month} className="month-bars__column" title={titleOf(entry)}>
            {!isDense && (
              <span className={`month-bars__value${entry.result_cents < 0 ? ' is-negative' : ''}`}>
                {resultText(entry.result_cents)}
              </span>
            )}
            <span className="month-bars__pair">
              <span className={seriesClass('month-bars__bar', first)} style={{ height: barHeight(entry.first_cents) }} />
              <span className={seriesClass('month-bars__bar', second)} style={{ height: barHeight(entry.second_cents) }} />
            </span>
            {belowZero > 0 && <span className="month-bars__below-zero" />}
            <span className={`month-bars__month${(visible.length - 1 - index) % step === 0 ? '' : ' is-hidden'}`}>
              {monthName.format(monthDate(entry.month))}
            </span>
          </div>
        ))}
        {showResultLine && (
          <div className="month-bars__result" aria-hidden>
            <svg viewBox="0 0 100 100" preserveAspectRatio="none">
              {belowZero > 0 && (
                <line className="month-bars__zero-line" x1="0" x2="100" y1={100 - belowZero * 100} y2={100 - belowZero * 100} />
              )}
              <polyline points={resultPoints.map(({ x, y }) => `${x},${y}`).join(' ')} />
            </svg>
            {resultPoints.map(({ x, y }, index) => (
              <i key={visible[index].month} style={{ left: `${x}%`, top: `${y}%` }} />
            ))}
          </div>
        )}
      </div>
      <div className="month-bars__legend" aria-hidden>
        <span><i className={seriesClass('month-bars__dot', first)} />{first.label}</span>
        <span><i className={seriesClass('month-bars__dot', second)} />{second.label}</span>
        {showResultLine && <span><i className="month-bars__dot month-bars__dot--result" />Kropki i linia: {resultLabel}</span>}
        {!isDense && <span>Nad słupkami: {resultLabel}</span>}
      </div>
    </>
  );
};

export default MonthPairBars;
