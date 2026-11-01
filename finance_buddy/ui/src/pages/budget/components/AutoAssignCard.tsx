// Rozwijana karta Auto-Assign: każda opcja ustawia przypisania wszystkich kategorii naraz.
import { IconBolt, IconChevronDown } from '@tabler/icons-react';
import { AUTO_ASSIGN_OPTION_GROUPS, moneyLabel } from '../model';
import type { BudgetEditing } from '../useBudgetEditing';

type AutoAssignCardProps = {
  editing: BudgetEditing;
};

const AutoAssignCard = ({ editing }: AutoAssignCardProps) => {
  const { autoAssignMenuOpen, setAutoAssignMenuOpen, autoAssignTotals, autoAssignDisabled, applyAutoAssign } =
    editing;

  return (
    <section className="card budget-side-card budget-side-card--action">
      <button
        type="button"
        className="budget-side-card__toggle budget-auto-assign__toggle"
        aria-expanded={autoAssignMenuOpen}
        aria-controls="budget-auto-assign-panel"
        onClick={() => setAutoAssignMenuOpen((prev) => !prev)}
      >
        <span className="budget-auto-assign__toggle-title">
          <IconBolt size={16} aria-hidden="true" />
          <span>Auto-Assign</span>
        </span>
        <IconChevronDown
          size={16}
          aria-hidden="true"
          className={`budget-side-card__toggle-icon budget-auto-assign__toggle-icon${autoAssignMenuOpen ? ' is-open' : ''}`}
        />
      </button>
      {autoAssignMenuOpen && (
        <div id="budget-auto-assign-panel" className="budget-side-card__body budget-auto-assign__body">
          {AUTO_ASSIGN_OPTION_GROUPS.map((options, groupIndex) => (
            <div
              key={groupIndex}
              className={`budget-auto-assign__group${groupIndex > 0 ? ' budget-auto-assign__group--secondary' : ''}`}
            >
              {options.map(({ mode, label }) => (
                <button
                  key={mode}
                  type="button"
                  className="budget-auto-assign__option"
                  onClick={() => void applyAutoAssign(mode)}
                  disabled={autoAssignDisabled}
                >
                  <span>{label}</span>
                  <strong>{moneyLabel(autoAssignTotals[mode])}</strong>
                </button>
              ))}
            </div>
          ))}
        </div>
      )}
    </section>
  );
};

export default AutoAssignCard;
