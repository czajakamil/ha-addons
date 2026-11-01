// Wydatki miesięcznie: neutralne słupki z wartością nad każdym, bieżący miesiąc wyróżniony.
import MonthBars from '../../../components/ui/MonthBars';
import { monthRangeLabel } from '../../../utils/months';
import { copy } from '../../../copy/pl';
import { useVisibleMonths } from '../../../hooks/useVisibleMonths';
import type { StoreMonthlySpend } from '../../../api';

const labels = copy.stores;

const StoreSpendChart = ({ months }: { months: StoreMonthlySpend[] }) => {
  const visible = useVisibleMonths(months);
  const lastMonth = visible[visible.length - 1].month;

  return (
    <section className="account-details__chart-card" aria-label={labels.chartTitle}>
      <div className="account-details__chart-head">
        <h2 className="home-block__title">{labels.chartTitle}</h2>
        <span className="stores-muted stores-chart__range">{monthRangeLabel(visible[0].month, lastMonth)}</span>
      </div>
      <MonthBars months={visible} highlighted={lastMonth} />
    </section>
  );
};

export default StoreSpendChart;
