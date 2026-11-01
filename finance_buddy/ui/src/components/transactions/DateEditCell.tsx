import { useEffect, useRef } from 'react';
import type {
  ChangeEvent,
  FocusEvent as ReactFocusEvent,
  KeyboardEvent as ReactKeyboardEvent
} from 'react';
import { captureGridCellAnchor, refocusGridCell } from '../../utils/gridFocus';

type DateEditCellProps = {
  value: string;
  disabled?: boolean;
  isSaving?: boolean;
  onChange: (value: string) => void;
  onCommit: () => void | Promise<void>;
  onCancel: () => void;
};

const DateEditCell = ({
  value,
  disabled,
  isSaving,
  onChange,
  onCommit,
  onCancel
}: DateEditCellProps) => {
  const inputRef = useRef<HTMLInputElement | null>(null);

  useEffect(() => {
    const el = inputRef.current;
    if (!el) return;

    el.focus();
  }, []);

  const handleChange = (event: ChangeEvent<HTMLInputElement>) => {
    onChange(event.target.value);
  };

  const handleKeyDown = (event: ReactKeyboardEvent<HTMLInputElement>) => {
    const { key } = event;

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

      // Kotwica PRZED onCommit, bo po nim komponent się unmountuje.
      const anchor = captureGridCellAnchor(inputRef.current);

      void Promise.resolve(onCommit()).then(() => {
        refocusGridCell(anchor);
      });
    }
  };

  const handleBlur = (_event: ReactFocusEvent<HTMLInputElement>) => {
    void Promise.resolve(onCommit());
  };

  return (
    <input
      ref={inputRef}
      type="date"
      className="form-control form-control-sm"
      autoFocus
      value={value}
      disabled={disabled || isSaving}
      onChange={handleChange}
      onBlur={handleBlur}
      onKeyDown={handleKeyDown}
    />
  );
};

export default DateEditCell;
