// Rozwijana sekcja „Przypisz automatycznie": każda opcja ustawia przypisania wszystkich kategorii naraz.
import { ChevronDown } from 'lucide-react';
import Money from '../../../components/ui/Money';
import { copy } from '../../../copy/pl';
import { AUTO_ASSIGN_OPTION_GROUPS } from '../model';
import type { BudgetEditing } from '../useBudgetEditing';

type AutoAssignCardProps = {
  editing: BudgetEditing;
};

const AutoAssignCard = ({ editing }: AutoAssignCardProps) => {
  const { autoAssignMenuOpen, setAutoAssignMenuOpen, autoAssignTotals, autoAssignDisabled, applyAutoAssign } =
    editing;

  return (
    <section className="budget-side">
      <button
        type="button"
        className="budget-side__toggle"
        aria-expanded={autoAssignMenuOpen}
        aria-controls="budget-auto-assign-panel"
        onClick={() => setAutoAssignMenuOpen((prev) => !prev)}
      >
        <span>{copy.budget.autoAssign.title}</span>
        <ChevronDown
          size={16}
          aria-hidden="true"
          className={`budget-side__chevron${autoAssignMenuOpen ? ' is-open' : ''}`}
        />
      </button>
      {autoAssignMenuOpen && (
        <div id="budget-auto-assign-panel" className="budget-auto">
          {AUTO_ASSIGN_OPTION_GROUPS.map((options, groupIndex) => (
            <div key={groupIndex} className="budget-auto__group">
              {options.map(({ mode, label }) => (
                <button
                  key={mode}
                  type="button"
                  className="budget-auto__option"
                  onClick={() => void applyAutoAssign(mode)}
                  disabled={autoAssignDisabled}
                >
                  <span>{label}</span>
                  <Money cents={autoAssignTotals[mode]} tone="neutral" />
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
