// Górny pasek budżetu: wybór miesiąca, baner „Do przypisania" i filtry statusu.
import {
  BUDGET_STATUS_FILTERS,
  FILTER_LABELS,
  moneyLabel,
  signClass,
  type BudgetStatusFilter
} from '../model';
import BudgetMonthNav from './BudgetMonthNav';

type BudgetToolbarProps = {
  activeMonth: string;
  activeMonthDate: Date;
  onMonthChange: (monthKey: string) => void;
  rtaValue: number;
  statusFilter: BudgetStatusFilter;
  onStatusFilterChange: (filter: BudgetStatusFilter) => void;
  filterCounts: Record<BudgetStatusFilter, number>;
};

const BudgetToolbar = ({
  activeMonth,
  activeMonthDate,
  onMonthChange,
  rtaValue,
  statusFilter,
  onStatusFilterChange,
  filterCounts
}: BudgetToolbarProps) => {
  // „Napraw" przy ujemnym „Do przypisania" pokazuje przekroczone kategorie
  const handleFixThisMonth = () => {
    onStatusFilterChange(rtaValue < 0 ? 'overspent' : 'all');
  };

  return (
    <section className="card budget-planner-toolbar" aria-label="Kontrolki budżetu">
      <div className="budget-planner-toolbar__top">
        <BudgetMonthNav activeMonth={activeMonth} activeMonthDate={activeMonthDate} onMonthChange={onMonthChange} />
        <div className={`budget-rta-banner ${signClass(rtaValue)}`}>
          <div className="budget-rta-banner__copy">
            <div className="budget-rta-banner__label">Do przypisania</div>
            <strong className="budget-rta-banner__value">{moneyLabel(rtaValue)}</strong>
            <div className="budget-rta-banner__text">
              {rtaValue < 0 ? 'Masz przypisane więcej niż dostępne środki.' : 'Budżet jest zbilansowany.'}
            </div>
          </div>
          <button type="button" className="budget-rta-banner__action" onClick={handleFixThisMonth}>
            Napraw
          </button>
        </div>
      </div>

      <div className="budget-filter-row" role="tablist" aria-label="Filtry tagów">
        {BUDGET_STATUS_FILTERS.map((filterKey) => (
          <button
            key={filterKey}
            type="button"
            role="tab"
            aria-selected={statusFilter === filterKey}
            className={`budget-filter-chip${statusFilter === filterKey ? ' is-active' : ''}`}
            onClick={() => onStatusFilterChange(filterKey)}
          >
            <span>{FILTER_LABELS[filterKey]}</span>
            <span className="budget-filter-chip__count">{filterCounts[filterKey]}</span>
          </button>
        ))}
      </div>
    </section>
  );
};

export default BudgetToolbar;
