import {
  useEffect,
  useMemo,
  useRef,
  useState,
  type ChangeEvent,
  type KeyboardEvent as ReactKeyboardEvent,
  type FocusEvent as ReactFocusEvent,
  type MouseEvent as ReactMouseEvent
} from 'react';
import { captureGridCellAnchor, refocusGridCell } from '../../utils/gridFocus';


type LookupAutocompleteInputProps = {
  inputValue: string;
  suggestions: string[];
  disabled?: boolean;
  placeholder?: string;
  inputId?: string;
  onInputChange: (value: string) => void;
  onCommit: (value: string) => void; // zapis + wyjście z edycji
  onCancel: () => void;              // anulowanie (Esc)
};

const NAVIGATION_KEYS = new Set<string>([
  'ArrowLeft',
  'ArrowRight',
  'ArrowUp',
  'ArrowDown',
  'Home',
  'End',
  'PageUp',
  'PageDown'
]);

const LookupAutocompleteInput = ({
  inputValue,
  suggestions,
  disabled,
  placeholder,
  inputId,
  onInputChange,
  onCommit,
  onCancel
}: LookupAutocompleteInputProps) => {
  const [isOpen, setIsOpen] = useState(false);
  const [highlightedIndex, setHighlightedIndex] = useState(0);
  const containerRef = useRef<HTMLDivElement | null>(null);
  const activeOptionRef = useRef<HTMLButtonElement | null>(null);
  // Po commitcie/anulowaniu przywracamy fokus komórce, co odpala blur inputa
  // jeszcze przed odmontowaniem edytora — ta flaga wyłącza wtedy commit z blura.
  const settledRef = useRef(false);

  const filteredOptions = useMemo(() => {
    const needle = inputValue.trim().toLowerCase();
    if (!needle) return suggestions;
    return suggestions.filter((opt) => opt.toLowerCase().includes(needle));
  }, [inputValue, suggestions]);

  const openMenu = () => {
    settledRef.current = false;
    if (!filteredOptions.length) return;
    setIsOpen(true);
    setHighlightedIndex(0);
  };

  const closeMenu = () => {
    setIsOpen(false);
  };

  const commit = (value?: string, opts?: { focusCell?: boolean }) => {
    if (settledRef.current) return;
    settledRef.current = true;
    // Kotwica przed commitem — po nim edytor się odmontowuje.
    const anchor = captureGridCellAnchor(containerRef.current);
    const effective = (value ?? inputValue).trim();
    closeMenu();
    onCommit(effective);
    if (opts?.focusCell !== false) {
      refocusGridCell(anchor);
    }
  };

  useEffect(() => {
    if (!isOpen) return;
    if (!activeOptionRef.current) return;

    activeOptionRef.current.scrollIntoView({
      block: 'nearest'
    });
  }, [highlightedIndex, isOpen, filteredOptions.length]);

  const handleInputChange = (event: ChangeEvent<HTMLInputElement>) => {
    settledRef.current = false;
    onInputChange(event.target.value);
    setIsOpen(true);
    setHighlightedIndex(0);
  };

  const handleKeyDown = (event: ReactKeyboardEvent<HTMLInputElement>) => {
    const key = event.key;

    if (key === 'Escape') {
      event.preventDefault();
      event.stopPropagation();
      settledRef.current = true;
      const anchor = captureGridCellAnchor(containerRef.current);
      closeMenu();
      onCancel();
      refocusGridCell(anchor);
      return;
    }

    if (key === 'Enter') {
      event.preventDefault();
      event.stopPropagation();

      const hasOptions = filteredOptions.length > 0;
      const chosen =
        hasOptions &&
        highlightedIndex >= 0 &&
        highlightedIndex < filteredOptions.length
          ? filteredOptions[highlightedIndex]
          : undefined;

      commit(chosen, { focusCell: true });
      return;
    }

    if (key === 'ArrowDown' || key === 'ArrowUp') {
      event.stopPropagation();
      if (!filteredOptions.length) return;
      event.preventDefault();

      setIsOpen(true);
      setHighlightedIndex((prev) => {
        const delta = key === 'ArrowDown' ? 1 : -1;
        const len = filteredOptions.length;
        return (prev + delta + len) % len; // zawijanie góra/dół
      });
      return;
    }

    if (NAVIGATION_KEYS.has(key)) {
      event.stopPropagation();
    }
  };

  const handleBlur = (event: ReactFocusEvent<HTMLDivElement>) => {
    // jeśli fokus przechodzi na element w środku (np. opcja) – nic
    if (containerRef.current?.contains(event.relatedTarget as Node)) {
      return;
    }
    // blur najczęściej po kliknięciu gdzieś indziej (np. inna komórka / toolbar),
    // nie przejmujemy fokusu na siłę
    commit(undefined, { focusCell: false });
  };

  const handleOptionMouseDown = (
    option: string,
    event: ReactMouseEvent<HTMLButtonElement>
  ) => {
    // żeby blur nie zjadł kliknięcia
    event.preventDefault();
    commit(option, { focusCell: true });
  };

  return (
    <div
      ref={containerRef}
      className="lookup-autocomplete position-relative"
      onBlur={handleBlur}
    >
      <input
        type="text"
        id={inputId}
        className="form-control form-control-sm"
        value={inputValue}
        disabled={disabled}
        placeholder={placeholder}
        autoFocus
        onChange={handleInputChange}
        onKeyDown={handleKeyDown}
        onFocus={openMenu}
      />

      {isOpen && filteredOptions.length > 0 && (
        <div className="lookup-autocomplete__menu">
          {filteredOptions.map((option, index) => (
            <button
              key={`${option}-${index}`}
              type="button"
              ref={index === highlightedIndex ? activeOptionRef : undefined}
              className={
                'lookup-autocomplete__option' +
                (index === highlightedIndex ? ' lookup-autocomplete__option--active' : '')
              }
              onMouseDown={(event) => handleOptionMouseDown(option, event)}
            >
              {option}
            </button>
          ))}
        </div>
      )}
    </div>
  );
};

export default LookupAutocompleteInput;
