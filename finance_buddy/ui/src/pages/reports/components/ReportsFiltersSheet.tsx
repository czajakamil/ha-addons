// Panel od dołu z filtrami raportu na telefonie. Natywny <dialog> daje warstwę nad
// resztą strony, pułapkę fokusu i zamykanie klawiszem Esc.
import { ReactNode, useEffect, useRef } from 'react';
import { X } from 'lucide-react';

type ReportsFiltersSheetProps = {
  isOpen: boolean;
  onClose: () => void;
  children: ReactNode;
};

const ReportsFiltersSheet = ({ isOpen, onClose, children }: ReportsFiltersSheetProps) => {
  const dialogRef = useRef<HTMLDialogElement>(null);

  useEffect(() => {
    const dialog = dialogRef.current;
    if (!dialog) return;
    if (isOpen && !dialog.open) dialog.showModal();
    if (!isOpen && dialog.open) dialog.close();
  }, [isOpen]);

  return (
    <dialog
      ref={dialogRef}
      className="reports-filters-sheet"
      aria-labelledby="reports-filters-sheet-title"
      onClose={onClose}
      onClick={(event) => {
        // Kliknięcie w tło trafia w sam <dialog>, a treść leży w jego dziecku
        if (event.target === dialogRef.current) onClose();
      }}
    >
      <div className="reports-filters-sheet__content">
        <div className="reports-filters-sheet__head">
          <h2 id="reports-filters-sheet-title" className="reports-filters-sheet__title">Filtry</h2>
          <button type="button" className="reports-filters-sheet__close is-focusable" aria-label="Zamknij filtry" onClick={onClose}>
            <X size={20} aria-hidden="true" />
          </button>
        </div>
        {isOpen && children}
      </div>
    </dialog>
  );
};

export default ReportsFiltersSheet;
