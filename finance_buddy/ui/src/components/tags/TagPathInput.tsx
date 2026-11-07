import {
  useEffect,
  useMemo,
  useRef,
  useState,
  type ChangeEvent,
  type FocusEvent as ReactFocusEvent,
  type KeyboardEvent as ReactKeyboardEvent,
  type MouseEvent as ReactMouseEvent
} from 'react';
import type { Category } from '../../api';
import { captureGridCellAnchor, refocusGridCell } from '../../utils/gridFocus';

type TagPathInputProps = {
  value: string;
  categories: Category[];
  /** Ogranicza podpowiedzi do tagów kont (true) albo tagów wydatków (false). */
  forAccounts?: boolean;
  disabled?: boolean;
  placeholder?: string;
  inputId?: string;
  autoFocus?: boolean;
  /**
   * Czy wyjście z pola zapisuje wartość (domyślnie tak — tak działa edycja w
   * komórce). Kolejka „do otagowania" wyłącza to: tam klik „Pomiń" przy
   * otwartym polu nie może po drodze otagować transakcji.
   */
  commitOnBlur?: boolean;
  onChange: (value: string) => void;
  onCommit?: (value: string) => void;
  onCancel?: () => void;
};

const NAVIGATION_KEYS = new Set<string>([
  'ArrowLeft',
  'ArrowRight',
  'Home',
  'End',
  'PageUp',
  'PageDown'
]);

/**
 * Puste pole → tagi najwyższego poziomu. Wpisany tekst → dopasowanie fragmentu
 * w CAŁEJ ścieżce (np. "mleko" znajdzie "Codzienne wydatki/Spożywcze/Mleko"),
 * dzięki czemu można szukać po dowolnym segmencie, nie tylko po górnej gałęzi.
 */
const getSuggestions = (value: string, categories: Category[], forAccounts?: boolean) => {
  const needle = value.trim().toLowerCase();

  return categories
    .filter((category) => (forAccounts === undefined ? true : (category.is_for_accounts ?? false) === forAccounts))
    .filter((category) =>
      needle.length ? category.path.toLowerCase().includes(needle) : !category.path.includes('/')
    )
    .sort((a, b) => a.path.localeCompare(b.path, 'pl'));
};

/** Podpowiedź: przodkowie przygaszeni, sama kategoria normalnie — „Rachunki › Czynsz”. */
const CategoryPathLabel = ({ path }: { path: string }) => {
  const segments = path.split('/').filter(Boolean);
  const leaf = segments.pop() ?? path;
  return (
    <span className="lookup-autocomplete__path">
      {segments.length > 0 && <span className="lookup-autocomplete__ancestors">{segments.join(' › ')} › </span>}
      {leaf}
    </span>
  );
};

/** Segmentowy autocomplete ścieżki tagu (np. "dom/remont/lazienka") — pozwala wybrać istniejącą gałąź albo wpisać nową. */
const TagPathInput = ({
  value,
  categories,
  forAccounts,
  disabled,
  placeholder,
  inputId,
  autoFocus,
  commitOnBlur = true,
  onChange,
  onCommit,
  onCancel
}: TagPathInputProps) => {
  const [isOpen, setIsOpen] = useState(false);
  const [highlightedIndex, setHighlightedIndex] = useState(0);
  const containerRef = useRef<HTMLDivElement | null>(null);
  const activeOptionRef = useRef<HTMLButtonElement | null>(null);
  // Po commitcie/anulowaniu przywracamy fokus komórce, co odpala blur inputa
  // jeszcze przed odmontowaniem edytora — ta flaga wyłącza wtedy commit z blura.
  const settledRef = useRef(false);

  const suggestions = useMemo(
    () => getSuggestions(value, categories, forAccounts),
    [value, categories, forAccounts]
  );

  const openMenu = () => {
    settledRef.current = false;
    if (!suggestions.length) return;
    setIsOpen(true);
    setHighlightedIndex(0);
  };

  const closeMenu = () => setIsOpen(false);

  useEffect(() => {
    if (!isOpen || !activeOptionRef.current) return;
    activeOptionRef.current.scrollIntoView({ block: 'nearest' });
  }, [highlightedIndex, isOpen, suggestions.length]);

  // Po commitcie/anulowaniu edytor w komórce tabeli się odmontowuje — bez
  // ręcznego przywrócenia fokusu klawiatura przestałaby działać w siatce.
  // Poza tabelą (modale, formularze) kotwica jest pusta i nic się nie dzieje.
  const commit = (path: string, opts?: { refocusCell?: boolean }) => {
    if (settledRef.current) return;
    settledRef.current = true;
    const anchor = captureGridCellAnchor(containerRef.current);
    closeMenu();
    onChange(path);
    onCommit?.(path);
    if (opts?.refocusCell !== false) {
      refocusGridCell(anchor);
    }
  };

  const handleInputChange = (event: ChangeEvent<HTMLInputElement>) => {
    // Poza tabelą komponent nie odmontowuje się po commicie — nowa interakcja
    // znów uzbraja zapis.
    settledRef.current = false;
    onChange(event.target.value);
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
      onCancel?.();
      refocusGridCell(anchor);
      return;
    }

    if (key === 'Enter') {
      event.preventDefault();
      event.stopPropagation();
      const highlighted =
        isOpen && suggestions.length && highlightedIndex >= 0 && highlightedIndex < suggestions.length
          ? suggestions[highlightedIndex]
          : null;
      // Wybór podpowiedzi nie kończy edycji — uzupełnia pole, żeby dało się
      // pisać dalej (np. dopisać "/podtag"). Zapis dopiero, gdy Enter trafia
      // na wartość, która już jest w polu, albo gdy nic nie jest podświetlone.
      if (highlighted && highlighted.path !== value.trim()) {
        onChange(highlighted.path);
        setHighlightedIndex(0);
        return;
      }
      commit(highlighted ? highlighted.path : value);
      return;
    }

    if (key === 'ArrowDown' || key === 'ArrowUp') {
      event.stopPropagation();
      if (!suggestions.length) return;
      event.preventDefault();
      setIsOpen(true);
      setHighlightedIndex((prev) => {
        const delta = key === 'ArrowDown' ? 1 : -1;
        const len = suggestions.length;
        return (prev + delta + len) % len;
      });
      return;
    }

    if (NAVIGATION_KEYS.has(key)) {
      event.stopPropagation();
    }
  };

  const handleBlur = (event: ReactFocusEvent<HTMLDivElement>) => {
    if (containerRef.current?.contains(event.relatedTarget as Node)) {
      return;
    }
    // Klik gdzie indziej — zapis bez odbierania fokusu nowemu miejscu.
    if (settledRef.current) return;
    closeMenu();
    if (!commitOnBlur) return;
    settledRef.current = true;
    onCommit?.(value);
  };

  // Klik działa jak Enter: wstawia ścieżkę i pozwala pisać dalej; ponowny
  // wybór tej samej wartości (albo blur/Enter) zapisuje.
  const handleOptionMouseDown = (path: string, event: ReactMouseEvent<HTMLButtonElement>) => {
    event.preventDefault();
    if (path !== value.trim()) {
      onChange(path);
      setHighlightedIndex(0);
      return;
    }
    commit(path);
  };

  return (
    <div ref={containerRef} className="lookup-autocomplete position-relative" onBlur={handleBlur}>
      <input
        type="text"
        id={inputId}
        className="form-control form-control-sm"
        value={value}
        disabled={disabled}
        placeholder={placeholder}
        autoFocus={autoFocus}
        onChange={handleInputChange}
        onKeyDown={handleKeyDown}
        onFocus={openMenu}
      />

      {isOpen && suggestions.length > 0 && (
        <div className="lookup-autocomplete__menu">
          {suggestions.map((category, index) => (
            <button
              key={category.id}
              type="button"
              ref={index === highlightedIndex ? activeOptionRef : undefined}
              className={
                'lookup-autocomplete__option' +
                (index === highlightedIndex ? ' lookup-autocomplete__option--active' : '')
              }
              onMouseDown={(event) => handleOptionMouseDown(category.path, event)}
            >
              <CategoryPathLabel path={category.path} />
            </button>
          ))}
        </div>
      )}
    </div>
  );
};

export default TagPathInput;
