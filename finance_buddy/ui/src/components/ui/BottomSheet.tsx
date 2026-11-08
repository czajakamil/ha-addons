// Panel od dołu na telefonie. Natywny <dialog> daje warstwę nad stroną, pułapkę fokusu
// i zamykanie klawiszem Esc; dotknięcie tła też zamyka.
import { ReactNode, useEffect, useId, useRef } from 'react';
import { X } from 'lucide-react';

type BottomSheetProps = {
  isOpen: boolean;
  onClose: () => void;
  title: string;
  closeLabel: string;
  /** Akcje obok tytułu, np. „Wyczyść". */
  headerActions?: ReactNode;
  children: ReactNode;
};

const BottomSheet = ({ isOpen, onClose, title, closeLabel, headerActions, children }: BottomSheetProps) => {
  const dialogRef = useRef<HTMLDialogElement>(null);
  const titleId = useId();

  useEffect(() => {
    const dialog = dialogRef.current;
    if (!dialog) return;
    if (isOpen && !dialog.open) dialog.showModal();
    if (!isOpen && dialog.open) dialog.close();
  }, [isOpen]);

  return (
    <dialog
      ref={dialogRef}
      className="bottom-sheet"
      aria-labelledby={titleId}
      onClose={onClose}
      onClick={(event) => {
        // Dotknięcie tła trafia w sam <dialog>, a treść leży w jego dziecku
        if (event.target === dialogRef.current) onClose();
      }}
    >
      <div className="bottom-sheet__content">
        <span className="bottom-sheet__grabber" aria-hidden="true" />
        <div className="bottom-sheet__head">
          <h2 id={titleId} className="bottom-sheet__title">{title}</h2>
          <div className="bottom-sheet__actions">
            {headerActions}
            <button type="button" className="bottom-sheet__close is-focusable" aria-label={closeLabel} onClick={onClose}>
              <X size={20} aria-hidden="true" />
            </button>
          </div>
        </div>
        {isOpen && children}
      </div>
    </dialog>
  );
};

export default BottomSheet;
