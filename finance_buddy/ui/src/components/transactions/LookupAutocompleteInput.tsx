import {
  Fragment,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ChangeEvent,
  type KeyboardEvent as ReactKeyboardEvent,
  type FocusEvent as ReactFocusEvent,
  type MouseEvent as ReactMouseEvent
} from 'react';
import { ArrowLeftRight } from 'lucide-react';
import { captureGridCellAnchor, refocusGridCell } from '../../utils/gridFocus';
import {
  defaultLookupHighlight,
  filterLookupOptions,
  findSelectedLookupIndex,
  resolveTypedLookup,
  splitLookupMatch,
  type LookupOption
} from './lookupOptions';


type LookupAutocompleteInputProps = {
  inputValue: string;
  options: LookupOption[];
  disabled?: boolean;
  placeholder?: string;
  inputId?: string;
  onInputChange: (value: string) => void;
  onCommit: (option: LookupOption | null) => void; // zapis + wyjście z edycji; null = wyczyszczone pole
  onCancel: () => void;              // anulowanie (Esc lub niepasujący tekst)
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

const SECTION_LABELS: Record<LookupOption['kind'], string> = {
  entry: 'Sklepy',
  transfer: 'Przelew na konto'
};

const LookupAutocompleteInput = ({
  inputValue,
  options,
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

  // Świeże pole pokazuje całą listę z obecną wartością; filtrujemy dopiero po pisaniu.
  const [dirty, setDirty] = useState(false);
  const [initialValue] = useState(inputValue);

  const filteredOptions = useMemo(
    () => (dirty ? filterLookupOptions(options, inputValue) : options),
    [dirty, inputValue, options]
  );
  const selectedOption = options[findSelectedLookupIndex(options, initialValue)];
  const hasSections = options.some((option) => option.kind === 'transfer');

  const openMenu = () => {
    settledRef.current = false;
    if (!filteredOptions.length) return;
    setIsOpen(true);
    setHighlightedIndex(
      dirty
        ? defaultLookupHighlight(filteredOptions, inputValue)
        : findSelectedLookupIndex(options, inputValue)
    );
  };

  const closeMenu = () => {
    setIsOpen(false);
  };

  const commit = (option: LookupOption | null, opts?: { focusCell?: boolean }) => {
    if (settledRef.current) return;
    settledRef.current = true;
    // Kotwica przed commitem — po nim edytor się odmontowuje.
    const anchor = captureGridCellAnchor(containerRef.current);
    closeMenu();
    onCommit(option);
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
    const value = event.target.value;
    setDirty(true);
    onInputChange(value);
    setIsOpen(true);
    setHighlightedIndex(defaultLookupHighlight(filterLookupOptions(options, value), value));
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

      const chosen = filteredOptions[highlightedIndex];
      if (chosen) {
        commit(chosen, { focusCell: true });
      } else if (!inputValue.trim()) {
        commit(null, { focusCell: true });
      }
      // Tekst bez żadnej pasującej pozycji: zostajemy w edycji, można go poprawić.
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
        if (prev < 0) return key === 'ArrowDown' ? 0 : len - 1;
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
    const typed = resolveTypedLookup(options, inputValue);
    if (typed.kind === 'none') {
      if (settledRef.current) return;
      settledRef.current = true;
      closeMenu();
      onCancel();
      return;
    }
    commit(typed.kind === 'match' ? typed.option : null, { focusCell: false });
  };

  const handleOptionMouseDown = (
    option: LookupOption,
    event: ReactMouseEvent<HTMLButtonElement>
  ) => {
    // żeby blur nie zjadł kliknięcia
    event.preventDefault();
    commit(option, { focusCell: true });
  };

  const renderRowLabel = (option: LookupOption) => {
    const text = option.rowLabel ?? option.label;
    const parts = dirty ? splitLookupMatch(text, inputValue) : null;
    return (
      <span className="lookup-autocomplete__label" title={text}>
        {parts ? (
          <>
            {parts.before}
            <mark>{parts.match}</mark>
            {parts.after}
          </>
        ) : (
          text
        )}
      </span>
    );
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
        autoComplete="off"
        onFocus={(event) => {
          // Zaznaczony tekst: pierwsze wpisane znaki zastępują obecną wartość.
          event.target.select();
          openMenu();
        }}
      />

      {isOpen && filteredOptions.length > 0 && (
        <div className="lookup-autocomplete__menu lookup-autocomplete__menu--rows">
          {filteredOptions.map((option, index) => {
            const isSelected = option === selectedOption;
            const startsSection = hasSections && filteredOptions[index - 1]?.kind !== option.kind;
            return (
              <Fragment key={`${option.kind}-${option.id}`}>
                {startsSection && (
                  <div className="lookup-autocomplete__section">{SECTION_LABELS[option.kind]}</div>
                )}
                <button
                  type="button"
                  ref={index === highlightedIndex ? activeOptionRef : undefined}
                  aria-current={isSelected ? 'true' : undefined}
                  className={
                    'lookup-autocomplete__option' +
                    (isSelected ? ' lookup-autocomplete__option--selected' : '') +
                    (index === highlightedIndex ? ' lookup-autocomplete__option--active' : '')
                  }
                  onMouseDown={(event) => handleOptionMouseDown(option, event)}
                >
                  {option.kind === 'transfer' && <ArrowLeftRight size={14} aria-hidden="true" />}
                  {renderRowLabel(option)}
                </button>
              </Fragment>
            );
          })}
          <div className="lookup-autocomplete__hint">Enter wybiera · Esc anuluje</div>
        </div>
      )}
    </div>
  );
};

export default LookupAutocompleteInput;
