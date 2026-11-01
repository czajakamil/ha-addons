// Wydatki miesięcznie: neutralne słupki z wartością nad każdym, bieżący miesiąc wyróżniony.
import { formatSignedMoney } from '../../../components/ui/Money';
import { copy } from '../../../copy/pl';
import { useMediaQuery } from '../../../hooks/useMediaQuery';
import type { MonthlySpend } from '../groups';

const labels = copy.stores;
const COMPACT_MONTHS = 6;
// Wartość nad słupkiem i miesiąc pod nim z odstępami; reszta wysokości należy do słupka.
const LABELS_HEIGHT_PX = 44;

const monthName = new Intl.DateTimeFormat('pl-PL', { month: 'short' });
const rangeName = new Intl.DateTimeFormat('pl-PL', { month: 'short', year: 'numeric' });
const wholeAmount = new Intl.NumberFormat('pl-PL', { maximumFractionDigits: 0 });

const toDate = (month: string) => {
  const [year, index] = month.split('-').map(Number);
  return new Date(year, index - 1, 1);
};

const StoreSpendChart = ({ months }: { months: MonthlySpend[] }) => {
  const isCompact = useMediaQuery('(max-width: 699px)');
  const visible = isCompact ? months.slice(-COMPACT_MONTHS) : months;
  const max = Math.max(...visible.map((entry) => entry.cents), 1);
  const summary = visible
    .map((entry) => `${monthName.format(toDate(entry.month))}: ${formatSignedMoney(entry.cents)}`)
    .join(', ');

  return (
    <section className="account-details__chart-card" aria-label={labels.chartTitle}>
      <div className="account-details__chart-head">
        <h2 className="home-block__title">{labels.chartTitle}</h2>
        <span className="stores-muted stores-chart__range">
          {rangeName.format(toDate(visible[0].month))} – {rangeName.format(toDate(visible[visible.length - 1].month))}
        </span>
      </div>
      <div className="stores-chart" role="img" aria-label={summary}>
        {visible.map((entry, index) => (
          <div key={entry.month} className="stores-chart__column">
            <span className="stores-chart__value">{entry.cents ? `${wholeAmount.format(entry.cents / 100)} zł` : '0'}</span>
            <div
              className={`stores-chart__bar${index === visible.length - 1 ? ' is-current' : ''}`}
              style={{ height: `max(2px, calc((100% - ${LABELS_HEIGHT_PX}px) * ${entry.cents / max}))` }}
            />
            <span className="stores-chart__month">{monthName.format(toDate(entry.month))}</span>
          </div>
        ))}
      </div>
    </section>
  );
};

export default StoreSpendChart;
