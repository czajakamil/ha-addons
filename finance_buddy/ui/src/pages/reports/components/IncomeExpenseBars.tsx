// Przychody i wydatki w każdym miesiącu: para słupków, a nad nią bilans.
import { useVisibleMonths } from '../../../hooks/useVisibleMonths';
import { monthDate, fullMonthName } from '../../../utils/months';
import { moneyLabel } from '../model';

const MAX_LABELED_MONTHS = 14;

const monthName = new Intl.DateTimeFormat('pl-PL', { month: 'short' });
const wholeAmount = new Intl.NumberFormat('pl-PL', {
  maximumFractionDigits: 0,
  useGrouping: 'always' as unknown as boolean
});

type IncomeExpenseMonth = { month: string; income_cents: number; expense_cents: number; net_cents: number };

const netLabel = (cents: number) => {
  if (!cents) return '0';
  return `${cents > 0 ? '+' : '−'}${wholeAmount.format(Math.abs(cents) / 100)} zł`;
};

const IncomeExpenseBars = ({ months }: { months: readonly IncomeExpenseMonth[] }) => {
  const visible = useVisibleMonths(months);
  const max = Math.max(...visible.flatMap((entry) => [entry.income_cents, Math.abs(entry.expense_cents)]), 1);
  const isDense = visible.length > MAX_LABELED_MONTHS;
  const step = Math.ceil(visible.length / MAX_LABELED_MONTHS);
  const barHeight = (cents: number) => (cents ? `max(2px, ${(Math.abs(cents) / max) * 100}%)` : 0);
  const titleOf = (entry: IncomeExpenseMonth) =>
    `${fullMonthName.format(monthDate(entry.month))}: przychody ${moneyLabel(entry.income_cents)}, wydatki ${moneyLabel(Math.abs(entry.expense_cents))}`;

  return (
    <>
      <div className="month-bars" role="img" aria-label={visible.map(titleOf).join('; ')}>
        {visible.map((entry, index) => (
          <div key={entry.month} className="month-bars__column" title={titleOf(entry)}>
            {!isDense && (
              <span className={`month-bars__value${entry.net_cents < 0 ? ' is-negative' : ''}`}>
                {netLabel(entry.net_cents)}
              </span>
            )}
            <span className="month-bars__pair">
              <span className="month-bars__bar month-bars__bar--income" style={{ height: barHeight(entry.income_cents) }} />
              <span className="month-bars__bar" style={{ height: barHeight(entry.expense_cents) }} />
            </span>
            <span className={`month-bars__month${(visible.length - 1 - index) % step === 0 ? '' : ' is-hidden'}`}>
              {monthName.format(monthDate(entry.month))}
            </span>
          </div>
        ))}
      </div>
      <div className="month-bars__legend" aria-hidden>
        <span><i className="month-bars__dot month-bars__dot--income" />Przychody</span>
        <span><i className="month-bars__dot" />Wydatki</span>
        {!isDense && <span>Nad słupkami: bilans</span>}
      </div>
    </>
  );
};

export default IncomeExpenseBars;
