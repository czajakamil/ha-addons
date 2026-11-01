// Górny pasek budżetu: miesiąc, główna liczba „Do przypisania" i filtry statusu.
import { Link } from 'react-router-dom';
import Money from '../../../components/ui/Money';
import SegmentedControl from '../../../components/ui/SegmentedControl';
import { copy } from '../../../copy/pl';
import { BUDGET_STATUS_FILTERS, FILTER_LABELS, type BudgetStatusFilter } from '../model';
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
}: BudgetToolbarProps) => (
  <>
    <header className="budget-top">
      <BudgetMonthNav activeMonth={activeMonth} activeMonthDate={activeMonthDate} onMonthChange={onMonthChange} />
      <div className="budget-rta">
        {rtaValue < 0 && (
          <button
            type="button"
            className="btn btn-primary"
            title={copy.budget.overassigned}
            onClick={() => onStatusFilterChange('overspent')}
          >
            {copy.budget.fix}
          </button>
        )}
        <div className="budget-rta__figure">
          <span className="budget-rta__label">{copy.budget.readyToAssign}</span>
          <Money cents={rtaValue} tone={rtaValue < 0 ? 'negative' : 'positive'} className="budget-rta__value" />
        </div>
      </div>
    </header>

    <div className="budget-filters">
      <SegmentedControl
        label={copy.budget.filtersLabel}
        value={statusFilter}
        onChange={onStatusFilterChange}
        options={BUDGET_STATUS_FILTERS.map((value) => ({
          value,
          label: FILTER_LABELS[value],
          count: filterCounts[value]
        }))}
      />
      <Link to="/tags" className="text-button">
        {copy.budget.addGroup}
      </Link>
    </div>
  </>
);

export default BudgetToolbar;
