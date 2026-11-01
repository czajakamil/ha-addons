// Sumy miesięczne: neutralne słupki z wartością nad każdym, jeden miesiąc wyróżniony.
import { fullMonthName, monthDate } from '../../utils/months';

const MAX_LABELED_MONTHS = 14;
// Wartość nad słupkiem i miesiąc pod nim z odstępami; reszta wysokości należy do słupka.
const LABELS_HEIGHT_PX = 44;

const monthName = new Intl.DateTimeFormat('pl-PL', { month: 'short' });
const wholeAmount = new Intl.NumberFormat('pl-PL', { maximumFractionDigits: 0, useGrouping: 'always' as unknown as boolean });

export type MonthBar = { month: string; cents: number };

type MonthBarsProps = {
  months: readonly MonthBar[];
  highlighted?: string | null;
  selected?: string | null;
  onSelect?: (month: string) => void;
  ariaLabel?: string;
};

const MonthBars = ({ months, highlighted, selected, onSelect, ariaLabel }: MonthBarsProps) => {
  // Miesiąc z nadwyżką zwrotów nie ma słupka w dół
  const visible = months.map((entry) => ({ ...entry, cents: Math.max(0, entry.cents) }));
  const max = Math.max(...visible.map((entry) => entry.cents), 1);
  // Przy długim zakresie etykiety by na siebie nachodziły: kwota zostaje w podpowiedzi, a miesiące co kilka słupków
  const isDense = visible.length > MAX_LABELED_MONTHS;
  const step = Math.ceil(visible.length / MAX_LABELED_MONTHS);
  const amountOf = (cents: number) => (cents ? `${wholeAmount.format(cents / 100)} zł` : '0');
  const summary = visible
    .map((entry) => `${fullMonthName.format(monthDate(entry.month))}: ${amountOf(entry.cents)}`)
    .join(', ');

  return (
    <div
      className="month-bars"
      role={onSelect ? 'group' : 'img'}
      aria-label={onSelect ? ariaLabel : summary}
    >
      {visible.map((entry, index) => {
        const isHighlighted = entry.month === highlighted;
        const hasLabel = (visible.length - 1 - index) % step === 0;
        const content = (
          <>
            {!isDense && <span className="month-bars__value">{amountOf(entry.cents)}</span>}
            <span
              className={`month-bars__bar${isHighlighted ? ' is-current' : ''}`}
              style={{ height: `max(2px, calc((100% - ${LABELS_HEIGHT_PX}px) * ${entry.cents / max}))` }}
            />
            <span className={`month-bars__month${hasLabel ? '' : ' is-hidden'}`}>
              {monthName.format(monthDate(entry.month))}
            </span>
          </>
        );
        const title = `${fullMonthName.format(monthDate(entry.month))}: ${amountOf(entry.cents)}`;
        return onSelect ? (
          <button
            key={entry.month}
            type="button"
            className="month-bars__column month-bars__column--button"
            aria-pressed={entry.month === selected}
            aria-label={title}
            title={title}
            onClick={() => onSelect(entry.month)}
          >
            {content}
          </button>
        ) : (
          <div key={entry.month} className="month-bars__column" title={title}>
            {content}
          </div>
        );
      })}
    </div>
  );
};

export default MonthBars;
