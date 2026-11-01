// Select w Ustawieniach konta: opcje z opisami, opcjonalnie z wyszukiwaniem.
// Lista jest w cmdk, który daje nawigację strzałkami i filtrowanie.
import { useEffect, useRef, useState, type ReactNode } from 'react';
import { Command } from 'cmdk';
import { Check, ChevronDown, Search } from 'lucide-react';

export type AccountSelectOption = {
  value: string;
  label: ReactNode;
  // Tekst, po którym szuka filtr cmdk
  keywords?: string[];
  description?: string;
};

type AccountSelectProps = {
  id: string;
  label: string;
  value: string;
  displayValue: ReactNode;
  groups: AccountSelectOption[][];
  searchPlaceholder?: string;
  noResults?: string;
  onChange: (value: string) => void;
};

const AccountSelect = ({
  id,
  label,
  value,
  displayValue,
  groups,
  searchPlaceholder,
  noResults,
  onChange
}: AccountSelectProps) => {
  const [open, setOpen] = useState(false);
  const rootRef = useRef<HTMLDivElement | null>(null);
  const triggerRef = useRef<HTMLButtonElement | null>(null);

  useEffect(() => {
    if (!open) return;
    const closeOnOutside = (event: MouseEvent) => {
      if (!rootRef.current?.contains(event.target as Node)) setOpen(false);
    };
    document.addEventListener('mousedown', closeOnOutside);
    return () => document.removeEventListener('mousedown', closeOnOutside);
  }, [open]);

  const close = () => {
    setOpen(false);
    triggerRef.current?.focus();
  };

  return (
    <div className="account-select" ref={rootRef}>
      <button
        id={id}
        ref={triggerRef}
        type="button"
        className="account-select__trigger"
        aria-haspopup="listbox"
        aria-expanded={open}
        onClick={() => setOpen((prev) => !prev)}
      >
        <span>{displayValue}</span>
        <ChevronDown size={14} aria-hidden="true" />
      </button>
      {open && (
        <Command
          className="account-select__popover"
          label={label}
          defaultValue={value}
          onKeyDown={(event) => {
            if (event.key === 'Escape') {
              event.stopPropagation();
              close();
            }
          }}
          // Bez wyszukiwarki fokus i tak musi trafić do listy, żeby działały strzałki
          tabIndex={-1}
          ref={(node) => {
            if (node && !searchPlaceholder) node.focus();
          }}
        >
          {searchPlaceholder && (
            <div className="account-select__search">
              <Search size={14} aria-hidden="true" />
              <Command.Input autoFocus placeholder={searchPlaceholder} />
            </div>
          )}
          <Command.List className="account-select__list">
            {noResults && <Command.Empty className="account-select__empty">{noResults}</Command.Empty>}
            {groups.map((options, index) => (
              <Command.Group key={index} className="account-select__group">
                {options.map((option) => (
                  <Command.Item
                    key={option.value}
                    value={option.value}
                    keywords={option.keywords}
                    className="account-select__option"
                    onSelect={() => {
                      close();
                      if (option.value !== value) onChange(option.value);
                    }}
                  >
                    <span>
                      <span className="account-select__option-label">{option.label}</span>
                      {option.description && <span className="account-hint">{option.description}</span>}
                    </span>
                    {option.value === value && <Check size={14} aria-hidden="true" />}
                  </Command.Item>
                ))}
              </Command.Group>
            ))}
          </Command.List>
        </Command>
      )}
    </div>
  );
};

export default AccountSelect;
