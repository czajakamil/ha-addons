// Nawigacja miesięczna budżetu: strzałki ‹ › i popover z siatką miesięcy roku.
// Stan popovera (otwarcie, przeglądany rok) żyje tutaj — strona zna tylko wybrany miesiąc.
import { useState } from 'react';
import { ChevronLeft, ChevronRight } from 'lucide-react';
import PopoverFilter from '../../../components/PopoverFilter';
import { copy } from '../../../copy/pl';
import { MONTH_SHORT_LABELS, buildMonthKey, capitalizeLabel, parseMonthKey, shiftMonth, toMonthKey } from '../model';

type BudgetMonthNavProps = {
  activeMonth: string;
  activeMonthDate: Date;
  onMonthChange: (monthKey: string) => void;
};

const BudgetMonthNav = ({ activeMonth, activeMonthDate, onMonthChange }: BudgetMonthNavProps) => {
  const [calendarMenuOpen, setCalendarMenuOpen] = useState(false);
  const [calendarViewYear, setCalendarViewYear] = useState<number>(new Date().getFullYear());
  const activeMonthYear = activeMonthDate.getFullYear();
  const activeMonthIndex = activeMonthDate.getMonth();

  const applyMonthSelection = (monthKey: string) => {
    onMonthChange(monthKey);
    setCalendarMenuOpen(false);
  };

  const handleShiftActiveMonth = (offset: number) => {
    const shiftedMonth = shiftMonth(activeMonthDate, offset);
    applyMonthSelection(toMonthKey(shiftedMonth));
  };

  // Otwarcie popovera zawsze pokazuje rok aktualnie wybranego miesiąca
  const handleCalendarOpenChange = (nextOpen: boolean) => {
    setCalendarMenuOpen(nextOpen);
    if (nextOpen) {
      const nextView = parseMonthKey(activeMonth);
      setCalendarViewYear(nextView.getFullYear());
    }
  };

  const calendarSelectionLabel = capitalizeLabel(
    new Date(activeMonthYear, activeMonthIndex, 1).toLocaleDateString('pl-PL', {
      month: 'long',
      year: 'numeric'
    })
  );

  const handleCalendarMonthPick = (monthIndex: number) => {
    applyMonthSelection(buildMonthKey(calendarViewYear, monthIndex));
  };

  return (
    <div className="budget-month-nav" role="group" aria-label={copy.budget.month.navigation}>
      <button
        type="button"
        className="budget-month-nav__arrow"
        onClick={() => handleShiftActiveMonth(-1)}
        aria-label={copy.budget.month.previous}
      >
        <ChevronLeft size={16} aria-hidden="true" />
      </button>
      <PopoverFilter
        id="budget-month-calendar"
        label={copy.budget.month.picker}
        className="budget-month-calendar"
        panelClassName="budget-month-calendar__menu"
        isOpen={calendarMenuOpen}
        onOpenChange={handleCalendarOpenChange}
        trigger={({ ref, panelId, isOpen, onToggle }) => (
          <button
            ref={ref}
            type="button"
            className={`budget-month-calendar__trigger${isOpen ? ' is-open' : ''}`}
            aria-haspopup="dialog"
            aria-expanded={isOpen}
            aria-controls={panelId}
            onClick={onToggle}
          >
            {calendarSelectionLabel}
          </button>
        )}
      >
        <div className="budget-month-calendar__year-row">
          <button
            type="button"
            className="budget-month-calendar__arrow"
            aria-label={copy.budget.month.previousYear}
            onClick={() => setCalendarViewYear((prev) => prev - 1)}
          >
            <ChevronLeft size={16} aria-hidden="true" />
          </button>
          <span className="budget-month-calendar__year">{calendarViewYear}</span>
          <button
            type="button"
            className="budget-month-calendar__arrow"
            aria-label={copy.budget.month.nextYear}
            onClick={() => setCalendarViewYear((prev) => prev + 1)}
          >
            <ChevronRight size={16} aria-hidden="true" />
          </button>
        </div>

        <div className="budget-month-calendar__grid" role="listbox" aria-label="Wybór miesiąca">
          {MONTH_SHORT_LABELS.map((label, monthIndex) => {
            const isSelected = activeMonthYear === calendarViewYear && activeMonthIndex === monthIndex;
            return (
              <button
                key={label}
                type="button"
                role="option"
                aria-selected={isSelected}
                className={`budget-month-calendar__month${isSelected ? ' is-selected' : ''}`}
                onClick={() => handleCalendarMonthPick(monthIndex)}
              >
                {label}
              </button>
            );
          })}
        </div>
      </PopoverFilter>
      <button
        type="button"
        className="budget-month-nav__arrow"
        onClick={() => handleShiftActiveMonth(1)}
        aria-label={copy.budget.month.next}
      >
        <ChevronRight size={16} aria-hidden="true" />
      </button>
    </div>
  );
};

export default BudgetMonthNav;
