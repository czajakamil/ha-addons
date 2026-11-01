// Zwijana karta podsumowania miesiąca w panelu bocznym budżetu.
import { useState } from 'react';
import { IconChevronDown } from '@tabler/icons-react';
import type { BudgetMonthSummary } from '../../../api';
import { clampPercent, moneyLabel, signClass } from '../model';

type BudgetSummaryCardProps = {
  monthLabel: string;
  summary: BudgetMonthSummary | undefined;
};

const BudgetSummaryCard = ({ monthLabel, summary }: BudgetSummaryCardProps) => {
  const [summaryCollapsed, setSummaryCollapsed] = useState(false);
  const rtaValue = summary?.rta_cents ?? 0;
  const assignedValue = summary?.assigned_cents ?? 0;
  const activityValue = summary?.activity_cents ?? 0;
  const availableValue = summary?.available_cents ?? 0;
  const onBudgetValue = summary?.on_budget_balance_cents ?? 0;
  const assignedShare =
    onBudgetValue !== 0 ? clampPercent((Math.abs(assignedValue) / Math.abs(onBudgetValue)) * 100) : 0;
  const activityShare =
    assignedValue !== 0 ? clampPercent((Math.abs(activityValue) / Math.abs(assignedValue)) * 100) : 0;

  return (
    <section className={`card budget-side-card budget-side-card--summary${summaryCollapsed ? ' is-collapsed' : ''}`}>
      <button
        type="button"
        className="budget-side-card__toggle"
        aria-expanded={!summaryCollapsed}
        aria-controls="budget-summary-panel"
        onClick={() => setSummaryCollapsed((prev) => !prev)}
      >
        <span className="budget-side-card__toggle-title">{monthLabel} - podsumowanie</span>
        <IconChevronDown
          size={16}
          aria-hidden="true"
          className={`budget-side-card__toggle-icon${summaryCollapsed ? ' is-collapsed' : ''}`}
        />
      </button>
      {!summaryCollapsed && (
        <div id="budget-summary-panel" className="budget-side-card__body">
          <ul className="budget-side-list">
            <li>
              <span>Do przypisania</span>
              <strong className={signClass(rtaValue)}>{moneyLabel(rtaValue)}</strong>
            </li>
            <li>
              <span>Przypisane</span>
              <strong className={signClass(assignedValue)}>{moneyLabel(assignedValue)}</strong>
            </li>
            <li>
              <span>Aktywność</span>
              <strong className={signClass(activityValue)}>{moneyLabel(activityValue)}</strong>
            </li>
            <li>
              <span>Dostępne</span>
              <strong className={signClass(availableValue)}>{moneyLabel(availableValue)}</strong>
            </li>
            <li>
              <span>Na kontach budżetowych</span>
              <strong>{moneyLabel(onBudgetValue)}</strong>
            </li>
          </ul>
          <div className="budget-side-progress">
            <div className="budget-side-progress__row">
              <span>Miesięczne przypisania</span>
              <strong>{Math.round(assignedShare)}%</strong>
            </div>
            <div className="budget-side-progress__track">
              <span
                className="budget-side-progress__fill budget-side-progress__fill--assigned"
                style={{ width: `${assignedShare}%` }}
              />
            </div>
          </div>
          <div className="budget-side-progress">
            <div className="budget-side-progress__row">
              <span>Wydatki z przypisań</span>
              <strong>{Math.round(activityShare)}%</strong>
            </div>
            <div className="budget-side-progress__track">
              <span
                className="budget-side-progress__fill budget-side-progress__fill--activity"
                style={{ width: `${activityShare}%` }}
              />
            </div>
          </div>
          <p className="budget-side-note">Możesz przejść do kolejnego miesiąca, aby zaplanować cele z wyprzedzeniem.</p>
        </div>
      )}
    </section>
  );
};

export default BudgetSummaryCard;
