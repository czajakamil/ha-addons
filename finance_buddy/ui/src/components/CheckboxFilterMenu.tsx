// Rozwijane menu filtra z wyszukiwarką i listą checkboxów (konta, kategorie). Wybór
// działa dopiero po „Zatwierdź" — stan roboczy (pendingIds) trzyma wywołujący.
import { CSSProperties, ReactNode, useLayoutEffect, useRef } from 'react';
import { ChevronDown } from 'lucide-react';
import Button from './Button';
import PopoverFilter from './PopoverFilter';

export type CheckboxFilterOption = {
  id: string;
  label: string;
  // Podkategoria: wcięta pod rodzicem
  nested?: boolean;
  // Poziom w drzewie (0 = korzeń); ma pierwszeństwo przed `nested`
  depth?: number;
  // Gałąź zaznaczona częściowo
  indeterminate?: boolean;
};

type CheckboxFilterMenuProps = {
  id: string;
  label: string;
  triggerLabel: ReactNode;
  // Klasa przycisku otwierającego — każdy ekran dopasowuje go do swojej belki
  triggerClassName?: string;
  className?: string;
  searchPlaceholder: string;
  emptyText: string;
  options: CheckboxFilterOption[];
  search: string;
  onSearchChange: (value: string) => void;
  pendingIds: string[];
  onToggle: (id: string) => void;
  onSelectAll: () => void;
  onClearAll: () => void;
  onCancel: () => void;
  onApply: () => void;
  isOpen: boolean;
  onOpenChange: (nextOpen: boolean) => void;
};

// Menu rozwija się w prawo od przycisku; gdy tam brakuje miejsca, przylega do jego prawej krawędzi
export const FilterMenuBody = ({ children }: { children: ReactNode }) => {
  const ref = useRef<HTMLDivElement>(null);

  useLayoutEffect(() => {
    const panel = ref.current?.parentElement;
    if (panel && panel.getBoundingClientRect().right > document.documentElement.clientWidth - 8) {
      panel.classList.add('filter-menu--end');
    }
  }, []);

  return (
    <div ref={ref} className="filter-menu__body">
      {children}
    </div>
  );
};

const CheckboxFilterMenu = ({
  id,
  label,
  triggerLabel,
  triggerClassName = 'filter-menu-trigger',
  className,
  searchPlaceholder,
  emptyText,
  options,
  search,
  onSearchChange,
  pendingIds,
  onToggle,
  onSelectAll,
  onClearAll,
  onCancel,
  onApply,
  isOpen,
  onOpenChange
}: CheckboxFilterMenuProps) => (
  <PopoverFilter
    id={id}
    label={label}
    className={`filter-menu-anchor${className ? ` ${className}` : ''}`}
    panelClassName="filter-menu"
    isOpen={isOpen}
    onOpenChange={onOpenChange}
    trigger={({ ref, panelId, isOpen: open, onToggle: toggle }) => (
      <button
        ref={ref}
        type="button"
        className={`${triggerClassName} is-focusable`}
        aria-label={label}
        aria-haspopup="dialog"
        aria-expanded={open}
        aria-controls={panelId}
        onClick={toggle}
      >
        <span className="filter-menu-trigger__value">{triggerLabel}</span>
        <ChevronDown className="filter-menu-trigger__chevron" size={14} aria-hidden="true" />
      </button>
    )}
  >
    <FilterMenuBody>
      <input
        type="search"
        className="filter-menu__search"
        placeholder={searchPlaceholder}
        aria-label={searchPlaceholder}
        value={search}
        onChange={(event) => onSearchChange(event.target.value)}
      />
      <div className="filter-menu__list">
        {options.map((option) => (
          <label
            key={option.id}
            className={`filter-menu__item${option.nested || option.depth ? ' filter-menu__item--nested' : ''}`}
            style={option.depth ? ({ '--filter-depth': option.depth } as CSSProperties) : undefined}
          >
            <input
              type="checkbox"
              checked={pendingIds.includes(option.id)}
              ref={(input) => {
                if (input) input.indeterminate = Boolean(option.indeterminate) && !pendingIds.includes(option.id);
              }}
              onChange={() => onToggle(option.id)}
            />
            <span className="filter-menu__name">{option.label}</span>
          </label>
        ))}
        {options.length === 0 && <p className="filter-menu__empty">{emptyText}</p>}
      </div>
      <div className="filter-menu__footer">
        <div className="filter-menu__bulk">
          <button type="button" className="filter-menu__action" onClick={onSelectAll}>
            Wybierz wszystko
          </button>
          <button type="button" className="filter-menu__action" onClick={onClearAll}>
            Odznacz wszystko
          </button>
        </div>
        <div className="filter-menu__footer-actions">
          <Button variant="outline-secondary" size="sm" onClick={onCancel}>
            Anuluj
          </Button>
          <Button variant="primary" size="sm" onClick={onApply}>
            Zatwierdź
          </Button>
        </div>
      </div>
    </FilterMenuBody>
  </PopoverFilter>
);

export default CheckboxFilterMenu;
