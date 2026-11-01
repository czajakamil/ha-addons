import {
  useMemo,
  useRef,
  useState,
  type ChangeEvent,
  type FocusEvent as ReactFocusEvent,
  type KeyboardEvent as ReactKeyboardEvent,
  type MouseEvent as ReactMouseEvent
} from 'react';

export type EntityOption = { id: string; label: string; hint?: string };

type EntityAutocompleteProps = {
  /** Zaznaczone id ('' oznacza brak wyboru). */
  value: string;
  options: EntityOption[];
  onSelect: (id: string) => void;
  /** Gdy podane, wpisanie nowej nazwy pozwala utworzyć pozycję ("+ Utwórz…"). */
  onCreate?: (label: string) => void;
  /** Etykieta opcji czyszczącej wybór (np. "Brak"); brak = pole wymagane. */
  emptyOptionLabel?: string;
  /** Tekst pokazywany, gdy `value` nie pasuje do żadnej opcji (np. nowy produkt). */
  fallbackLabel?: string;
  placeholder?: string;
  disabled?: boolean;
  inputId?: string;
};

type Entry =
  | { kind: 'clear' }
  | { kind: 'option'; option: EntityOption }
  | { kind: 'create'; label: string };

/**
 * Wpisywalny combobox dla pól słownikowych w formularzach (konto/sklep/produkt).
 * Pozwala filtrować listę pisząc, wybrać istniejącą pozycję albo — gdy podano
 * `onCreate` — utworzyć nową z wpisanej nazwy. W odróżnieniu od
 * LookupAutocompleteInput nie zależy od fokusu siatki i operuje na id.
 */
const EntityAutocomplete = ({
  value,
  options,
  onSelect,
  onCreate,
  emptyOptionLabel,
  fallbackLabel,
  placeholder,
  disabled,
  inputId
}: EntityAutocompleteProps) => {
  const [query, setQuery] = useState('');
  const [focused, setFocused] = useState(false);
  const [isOpen, setIsOpen] = useState(false);
  const [highlighted, setHighlighted] = useState(0);
  // Czy użytkownik pisał od chwili wejścia w pole — dopiero wtedy filtrujemy listę
  // (świeży fokus pokazuje wszystkie opcje, mimo że w polu jest aktualna wartość).
  const [dirty, setDirty] = useState(false);
  const containerRef = useRef<HTMLDivElement | null>(null);
  const inputRef = useRef<HTMLInputElement | null>(null);

  const selected = useMemo(
    () => options.find((option) => option.id === value) ?? null,
    [options, value]
  );

  const entries = useMemo<Entry[]>(() => {
    const needle = query.trim().toLowerCase();
    const filtered =
      dirty && needle
        ? options.filter((option) => option.label.toLowerCase().includes(needle))
        : options;

    const result: Entry[] = [];
    if (emptyOptionLabel) {
      result.push({ kind: 'clear' });
    }
    filtered.forEach((option) => result.push({ kind: 'option', option }));

    const trimmed = query.trim();
    const exact = options.some((option) => option.label.trim().toLowerCase() === needle);
    if (onCreate && trimmed && !exact) {
      result.push({ kind: 'create', label: trimmed });
    }
    return result;
  }, [options, query, dirty, emptyOptionLabel, onCreate]);

  const displayValue = focused ? query : selected?.label ?? fallbackLabel ?? '';

  // "Wyczyść" ląduje w `entries` zawsze na pozycji 0 (patrz entries powyżej) —
  // domyślny highlight ma je pomijać, inaczej samo Enter po wpisaniu tekstu
  // czyści pole zamiast wybrać/utworzyć to, co użytkownik właśnie wpisał.
  const defaultHighlight = () => (emptyOptionLabel ? 1 : 0);

  const openMenu = () => {
    setIsOpen(true);
    setHighlighted(defaultHighlight());
  };

  const commitEntry = (entry: Entry) => {
    if (entry.kind === 'clear') {
      onSelect('');
    } else if (entry.kind === 'option') {
      onSelect(entry.option.id);
    } else {
      onCreate?.(entry.label);
    }
    setDirty(false);
    setQuery('');
    setFocused(false);
    setIsOpen(false);
    inputRef.current?.blur();
  };

  const handleFocus = () => {
    setFocused(true);
    setDirty(false);
    setQuery(selected?.label ?? fallbackLabel ?? '');
    openMenu();
  };

  const handleChange = (event: ChangeEvent<HTMLInputElement>) => {
    setDirty(true);
    setQuery(event.target.value);
    setIsOpen(true);
    setHighlighted(defaultHighlight());
  };

  const handleKeyDown = (event: ReactKeyboardEvent<HTMLInputElement>) => {
    const key = event.key;

    if (key === 'Escape') {
      event.preventDefault();
      event.stopPropagation();
      setDirty(false);
      setIsOpen(false);
      setFocused(false);
      inputRef.current?.blur();
      return;
    }

    if (key === 'ArrowDown' || key === 'ArrowUp') {
      event.preventDefault();
      if (!entries.length) return;
      setIsOpen(true);
      setHighlighted((prev) => {
        const delta = key === 'ArrowDown' ? 1 : -1;
        return (prev + delta + entries.length) % entries.length;
      });
      return;
    }

    if (key === 'Enter') {
      if (!isOpen || !entries.length) return;
      event.preventDefault();
      event.stopPropagation();
      const entry = entries[highlighted] ?? entries[0];
      commitEntry(entry);
    }
  };

  const handleBlur = (event: ReactFocusEvent<HTMLDivElement>) => {
    if (containerRef.current?.contains(event.relatedTarget as Node)) {
      return;
    }
    // Klik poza polem — porzucamy niedokończony tekst i wracamy do wyboru.
    setDirty(false);
    setFocused(false);
    setIsOpen(false);
  };

  const handleOptionMouseDown = (entry: Entry, event: ReactMouseEvent<HTMLButtonElement>) => {
    event.preventDefault();
    commitEntry(entry);
  };

  const renderEntryLabel = (entry: Entry) => {
    if (entry.kind === 'clear') {
      return <span className="text-secondary">{emptyOptionLabel}</span>;
    }
    if (entry.kind === 'create') {
      return <span>+ Utwórz „{entry.label}”</span>;
    }
    return (
      <span>
        {entry.option.label}
        {entry.option.hint ? <small className="text-secondary ms-1">{entry.option.hint}</small> : null}
      </span>
    );
  };

  return (
    <div ref={containerRef} className="lookup-autocomplete position-relative" onBlur={handleBlur}>
      <input
        ref={inputRef}
        type="text"
        id={inputId}
        className="form-control"
        value={displayValue}
        disabled={disabled}
        placeholder={placeholder}
        autoComplete="off"
        onFocus={handleFocus}
        onChange={handleChange}
        onKeyDown={handleKeyDown}
      />

      {isOpen && entries.length > 0 && (
        <div className="lookup-autocomplete__menu">
          {entries.map((entry, index) => {
            const key =
              entry.kind === 'option'
                ? entry.option.id
                : entry.kind === 'create'
                ? `create:${entry.label}`
                : 'clear';
            return (
              <button
                key={key}
                type="button"
                className={
                  'lookup-autocomplete__option' +
                  (index === highlighted ? ' lookup-autocomplete__option--active' : '')
                }
                onMouseDown={(mouseEvent) => handleOptionMouseDown(entry, mouseEvent)}
              >
                {renderEntryLabel(entry)}
              </button>
            );
          })}
        </div>
      )}
    </div>
  );
};

export default EntityAutocomplete;
