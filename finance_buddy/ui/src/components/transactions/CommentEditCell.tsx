import { useEffect, useRef } from 'react';
import type {
  ChangeEvent,
  KeyboardEvent as ReactKeyboardEvent,
  FocusEvent as ReactFocusEvent
} from 'react';
import { captureGridCellAnchor, refocusGridCell } from '../../utils/gridFocus';

type CommentEditCellProps = {
  value: string;
  disabled?: boolean;
  rows?: number;
  isSaving?: boolean;
  onChange: (value: string) => void;
  onCommit: () => void | Promise<void>;
  onCancel: () => void;
};

const CommentEditCell = ({
  value,
  disabled,
  rows = 3,
  isSaving,
  onChange,
  onCommit,
  onCancel
}: CommentEditCellProps) => {
  const textareaRef = useRef<HTMLTextAreaElement | null>(null);
  // Esc i Cmd/Ctrl+Enter oddają fokus komórce, co odpala blur textarea jeszcze
  // przed odmontowaniem — bez tej flagi blur zapisałby anulowany tekst.
  const settledRef = useRef(false);

  // po wejściu w edycję – kursor na końcu tekstu
  useEffect(() => {
    const el = textareaRef.current;
    if (!el) return;

    const len = el.value.length;
    el.focus();
    el.setSelectionRange(len, len);
  }, []);

  const handleChange = (event: ChangeEvent<HTMLTextAreaElement>) => {
    onChange(event.target.value);
  };

  const handleKeyDown = (event: ReactKeyboardEvent<HTMLTextAreaElement>) => {
    const { key, metaKey, ctrlKey } = event;

    // żadnego eventu z tego textarea nie puszczamy do react-data-grid
    event.stopPropagation();

    if (key === 'Escape') {
      event.preventDefault();
      settledRef.current = true;

      const anchor = captureGridCellAnchor(textareaRef.current);
      onCancel();
      refocusGridCell(anchor);
      return;
    }

    if ((metaKey || ctrlKey) && key === 'Enter') {
      event.preventDefault();
      settledRef.current = true;

      // Kotwica PRZED onCommit, bo po nim komponent się unmountuje.
      const anchor = captureGridCellAnchor(textareaRef.current);

      void Promise.resolve(onCommit()).then(() => {
        refocusGridCell(anchor);
      });

      return;
    }

    // reszta klawiszy (strzałki, Cmd+Strzałka, Home/End itd.)
    // normalnie działa w textarea, bo NIE robimy preventDefault.
  };

  const handleBlur = (_event: ReactFocusEvent<HTMLTextAreaElement>) => {
    // kliknięcie gdzieś indziej – zapisujemy, ale
    // NIE przejmujemy z powrotem fokusu na tabelę
    if (settledRef.current) return;
    settledRef.current = true;
    void Promise.resolve(onCommit());
  };

  return (
    <>
      <textarea
        ref={textareaRef}
        rows={rows}
        autoFocus
        className="form-control form-control-sm"
        value={value}
        disabled={disabled}
        onChange={handleChange}
        onBlur={handleBlur}
        onKeyDown={handleKeyDown}
      />
      <div className="editable-cell__meta">
        {isSaving ? 'Zapisywanie…' : 'Cmd/Ctrl + Enter aby zapisać'}
      </div>
    </>
  );
};

export default CommentEditCell;
