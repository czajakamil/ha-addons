// Wybór jednej kategorii z drzewa z wyszukiwarką — kliknięcie od razu wybiera i zamyka menu.
// Pozycje (z wcięciami) przygotowuje wywołujący.
import { CSSProperties, ReactNode, useState } from 'react';
import { ChevronDown } from 'lucide-react';
import { FilterMenuBody, type CheckboxFilterOption } from './CheckboxFilterMenu';
import PopoverFilter from './PopoverFilter';

type CategoryPickerMenuProps = {
  id: string;
  label: string;
  triggerLabel: ReactNode;
  triggerClassName?: string;
  searchPlaceholder: string;
  emptyText: string;
  disabled?: boolean;
  // Pozycje dla bieżącej frazy
  options: (search: string) => CheckboxFilterOption[];
  onSelect: (id: string) => void;
};

const CategoryPickerMenu = ({
  id,
  label,
  triggerLabel,
  triggerClassName = 'filter-menu-trigger',
  searchPlaceholder,
  emptyText,
  disabled = false,
  options,
  onSelect
}: CategoryPickerMenuProps) => {
  const [isOpen, setIsOpen] = useState(false);
  const [search, setSearch] = useState('');
  const visible = isOpen ? options(search) : [];

  const handleOpenChange = (nextOpen: boolean) => {
    if (nextOpen) setSearch('');
    setIsOpen(nextOpen);
  };

  const pick = (optionId: string) => {
    setIsOpen(false);
    onSelect(optionId);
  };

  return (
    <PopoverFilter
      id={id}
      label={label}
      className="filter-menu-anchor"
      panelClassName="filter-menu"
      isOpen={isOpen}
      onOpenChange={handleOpenChange}
      trigger={({ ref, panelId, isOpen: open, onToggle }) => (
        <button
          ref={ref}
          type="button"
          className={`${triggerClassName} is-focusable`}
          aria-label={label}
          aria-haspopup="dialog"
          aria-expanded={open}
          aria-controls={panelId}
          disabled={disabled}
          onClick={onToggle}
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
          autoFocus
          onChange={(event) => setSearch(event.target.value)}
          onKeyDown={(event) => {
            // Enter wybiera pierwszą pasującą kategorię
            if (event.key === 'Enter' && visible.length) {
              event.preventDefault();
              pick(visible[0].id);
            }
          }}
        />
        <div className="filter-menu__list">
          {visible.map((option) => (
            <button
              key={option.id}
              type="button"
              className="filter-menu__item filter-menu__item--pick"
              style={option.depth ? ({ '--filter-depth': option.depth } as CSSProperties) : undefined}
              onClick={() => pick(option.id)}
            >
              <span className="filter-menu__name">{option.label}</span>
            </button>
          ))}
          {visible.length === 0 && <p className="filter-menu__empty">{emptyText}</p>}
        </div>
      </FilterMenuBody>
    </PopoverFilter>
  );
};

export default CategoryPickerMenu;
