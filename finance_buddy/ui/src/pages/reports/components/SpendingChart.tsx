// Miesięczne wydatki jako słupki ułożone z największych pozycji, z linią średniej.
import { useMediaQuery } from '../../../hooks/useMediaQuery';
import { pluralPL } from '../../../utils/format';
import { monthDate } from '../../../utils/months';
import { moneyLabel, reportMonthLabel, reportMonthShort } from '../model';
import { niceScale, wholeAmount, wholeMoneyLabel, type SpendingModel } from '../spending';

const COMPACT_MONTHS = 6;
const MAX_LABELED_MONTHS = 14;

const monthName = new Intl.DateTimeFormat('pl-PL', { month: 'short' });
const tickLabel = (cents: number, compact: boolean) => {
  if (cents === 0) return '0';
  const zloty = cents / 100;
  if (!compact || zloty < 1000) return wholeAmount.format(zloty);
  return `${String(zloty / 1000).replace('.', ',')}k`;
};

type SpendingChartProps = {
  model: SpendingModel;
};

const SpendingChart = ({ model }: SpendingChartProps) => {
  const isCompact = useMediaQuery('(max-width: 699px)');
  // Na telefonie mieści się tylko ostatnie pół roku
  const offset = isCompact ? Math.max(0, model.months.length - COMPACT_MONTHS) : 0;
  const months = model.months.slice(offset);
  const series = model.chartSeries.map((entry) => ({ ...entry, values: entry.values.slice(offset) }));

  const { top, ticks } = niceScale(Math.max(...months.map((entry) => entry.total), model.average ?? 0));
  const at = (cents: number) => `${(cents / top) * 100}%`;
  const isDense = months.length > MAX_LABELED_MONTHS;
  const labelStep = Math.ceil(months.length / MAX_LABELED_MONTHS);

  const summary = months
    .map((entry) => `${reportMonthLabel(entry.month)}: ${moneyLabel(entry.total)}`)
    .join(', ');

  const legend = (
    <ul className="spending-chart__legend" aria-label="Legenda">
      {series.map((entry) => (
        <li key={entry.key}>
          <span className="spending-chart__swatch" style={{ background: entry.color }} aria-hidden="true" />
          {entry.name}
        </li>
      ))}
    </ul>
  );

  return (
    <section className="card report-card spending-chart" aria-label="Wydatki miesięcznie">
      <div className="spending-chart__header">
        <h3>
          {isCompact
            ? `Ostatnie ${months.length} ${pluralPL(months.length, 'miesiąc', 'miesiące', 'miesięcy')}`
            : 'Wydatki miesięcznie'}
        </h3>
        {isCompact ? (
          model.average !== null && (
            <span className="spending-chart__average-note">śr. {wholeMoneyLabel(model.average)}</span>
          )
        ) : (
          legend
        )}
      </div>
      <div className="spending-chart__body">
        <div className="spending-chart__axis" aria-hidden="true">
          {ticks.map((tick) => (
            <span key={tick} style={{ bottom: at(tick) }}>
              {tickLabel(tick, isCompact)}
            </span>
          ))}
        </div>
        <div className="spending-chart__main">
          <div className="spending-chart__plot" role="img" aria-label={summary}>
            {ticks.map((tick) => (
              <div key={tick} className="spending-chart__grid" style={{ bottom: at(tick) }} />
            ))}
            <div
              className="spending-chart__bars"
              style={{
                gridTemplateColumns: `repeat(${months.length}, minmax(0, 1fr))`,
                gap: isDense ? 3 : undefined
              }}
            >
              {months.map((entry, index) => (
                <div
                  key={entry.month}
                  className={`spending-chart__column${entry.partial ? ' is-partial' : ''}`}
                  title={`${reportMonthLabel(entry.month)}: ${moneyLabel(entry.total)}${entry.partial ? ' (w toku)' : ''}`}
                >
                  {series.map((item) =>
                    item.values[index] > 0 ? (
                      <span
                        key={item.key}
                        className="spending-chart__segment"
                        style={{ height: at(item.values[index]), background: item.color }}
                      />
                    ) : null
                  )}
                </div>
              ))}
            </div>
            {model.average !== null && (
              <>
                <div className="spending-chart__average" style={{ bottom: at(model.average) }} />
                {!isCompact && (
                  <span className="spending-chart__average-label" style={{ bottom: at(model.average) }}>
                    średnio {wholeMoneyLabel(model.average)}
                  </span>
                )}
              </>
            )}
          </div>
          <div
            className="spending-chart__months"
            style={{
              gridTemplateColumns: `repeat(${months.length}, minmax(0, 1fr))`,
              gap: isDense ? 3 : undefined
            }}
            aria-hidden="true"
          >
            {months.map((entry, index) => {
              const shown = (months.length - 1 - index) % labelStep === 0;
              const base = isDense ? reportMonthShort(entry.month) : monthName.format(monthDate(entry.month));
              const suffix = entry.partial ? (isCompact || isDense ? '*' : ' · w toku') : '';
              return (
                <span key={entry.month} className={shown ? undefined : 'is-hidden'}>
                  {base}
                  {suffix}
                </span>
              );
            })}
          </div>
        </div>
      </div>
      {isCompact && legend}
    </section>
  );
};

export default SpendingChart;
