import { useEffect, useRef } from 'react';
import type {
  ChangeEvent,
  KeyboardEvent as ReactKeyboardEvent,
  FocusEvent as ReactFocusEvent
} from 'react';
import { captureGridCellAnchor, refocusGridCell } from '../../utils/gridFocus';

type AmountEditCellProps = {
  value: string;
  disabled?: boolean;
  isSaving?: boolean;
  onChange: (value: string) => void;
  onCommit: () => void | Promise<void>;
  onCancel: () => void;
  // Ten sam edytor służy też do godziny: inna klawiatura na telefonie i wyrównanie
  inputMode?: 'decimal' | 'numeric' | 'text';
  align?: 'start' | 'end';
  placeholder?: string;
  ariaLabel?: string;
};

const AmountEditCell = ({
  value,
  disabled,
  isSaving,
  onChange,
  onCommit,
  onCancel,
  inputMode = 'decimal',
  align = 'end',
  placeholder,
  ariaLabel
}: AmountEditCellProps) => {
  const inputRef = useRef<HTMLInputElement | null>(null);

  // Po wejściu w edycję – kursor na końcu wartości
  useEffect(() => {
    const el = inputRef.current;
    if (!el) return;

    const len = el.value.length;
    el.focus();
    el.setSelectionRange(len, len);
  }, []);

  const handleChange = (event: ChangeEvent<HTMLInputElement>) => {
    onChange(event.target.value);
  };

  const handleKeyDown = (event: ReactKeyboardEvent<HTMLInputElement>) => {
    const { key } = event;

    // Żaden key z inputa nie może wpaść do react-data-grid:
    // inaczej grid zjada strzałki.
    event.stopPropagation();

    if (key === 'Escape') {
      event.preventDefault();
      const anchor = captureGridCellAnchor(inputRef.current);
      onCancel();
      refocusGridCell(anchor);
      return;
    }

    if (key === 'Enter') {
      event.preventDefault();

      // Kotwica PRZED commit, bo po nim komponent się odmontuje.
      const anchor = captureGridCellAnchor(inputRef.current);

      void Promise.resolve(onCommit()).then(() => {
        refocusGridCell(anchor);
      });

      return;
    }

    // Strzałki / Cmd+Strzałka / Home / End itp.:
    // brak preventDefault -> normalne zachowanie inputa.
  };

  const handleBlur = (_event: ReactFocusEvent<HTMLInputElement>) => {
    // klik gdzieś indziej – zapisujemy,
    // ale nie przejmujemy na siłę fokusu z powrotem na tabelę
    void Promise.resolve(onCommit());
  };

  return (
    <input
      ref={inputRef}
      type="text"
      inputMode={inputMode}
      className={`form-control form-control-sm text-${align}`}
      placeholder={placeholder}
      aria-label={ariaLabel}
      autoFocus
      value={value}
      disabled={disabled || isSaving}
      onChange={handleChange}
      onBlur={handleBlur}
      onKeyDown={handleKeyDown}
    />
  );
};

export default AmountEditCell;
