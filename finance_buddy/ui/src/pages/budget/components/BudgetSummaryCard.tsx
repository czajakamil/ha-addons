// Zwijany panel podsumowania miesiąca. Na wąskich ekranach startuje zwinięty, jako pasek nad tabelą.
import { useState } from 'react';
import { ChevronDown } from 'lucide-react';
import type { BudgetMonthSummary } from '../../../api';
import Money from '../../../components/ui/Money';
import { copy } from '../../../copy/pl';

const NARROW_LAYOUT_QUERY = '(max-width: 1099px)';

type BudgetSummaryCardProps = {
  summary: BudgetMonthSummary | undefined;
};

const BudgetSummaryCard = ({ summary }: BudgetSummaryCardProps) => {
  const [collapsed, setCollapsed] = useState(() => window.matchMedia(NARROW_LAYOUT_QUERY).matches);
  const labels = copy.budget.summary;

  return (
    <section className="budget-side">
      <button
        type="button"
        className="budget-side__toggle"
        aria-expanded={!collapsed}
        aria-controls="budget-summary-panel"
        onClick={() => setCollapsed((prev) => !prev)}
      >
        <span>{labels.title}</span>
        <ChevronDown size={16} aria-hidden="true" className={`budget-side__chevron${collapsed ? '' : ' is-open'}`} />
      </button>
      {!collapsed && (
        <dl id="budget-summary-panel" className="budget-side__list">
          <div>
            <dt>{labels.assigned}</dt>
            <dd>
              <Money cents={summary?.assigned_cents ?? 0} tone="neutral" />
            </dd>
          </div>
          <div>
            <dt>{labels.activity}</dt>
            <dd>
              <Money cents={summary?.activity_cents ?? 0} />
            </dd>
          </div>
          <div>
            <dt>{labels.available}</dt>
            <dd>
              <Money
                cents={summary?.available_cents ?? 0}
                tone={(summary?.available_cents ?? 0) < 0 ? 'negative' : 'neutral'}
              />
            </dd>
          </div>
          <div>
            <dt>{labels.onBudget}</dt>
            <dd>
              <Money cents={summary?.on_budget_balance_cents ?? 0} tone="neutral" />
            </dd>
          </div>
        </dl>
      )}
    </section>
  );
};

export default BudgetSummaryCard;
