// Modal potwierdzenia trwałego usunięcia sklepu. Usunięcie jest nieodwracalne,
// więc — tak jak przy kasowaniu konta — wymagamy świadomego potwierdzenia:
// użytkownik musi wpisać nazwę sklepu, żeby odblokować przycisk usuwania.
// Transakcje NIE są kasowane: tracą jedynie powiązanie ze sklepem (store_id → NULL),
// więc pozostają na kontach i nie zmieniają sald.
import { useState, type ChangeEvent } from 'react';
import { createPortal } from 'react-dom';
import { Store, deleteStore } from '../../../api';

type DeleteStoreModalProps = {
  store: Store;
  onClose: () => void;
  onDeleted: () => Promise<void>;
};

const DeleteStoreModal = ({ store, onClose, onDeleted }: DeleteStoreModalProps) => {
  const [confirmationText, setConfirmationText] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [deleting, setDeleting] = useState(false);
  const modalTitleId = 'delete-store-modal-title';

  const confirmed = confirmationText.trim() === store.name.trim();

  const handleDelete = async () => {
    if (!confirmed || deleting) return;
    try {
      setDeleting(true);
      await deleteStore(store.id);
      await onDeleted();
      onClose();
    } catch (err) {
      console.error(err);
      setError('Nie udało się usunąć sklepu. Spróbuj ponownie.');
      setDeleting(false);
    }
  };

  // Portal do <body> — patrz ManageStoreModal: unika uwięzienia `position: fixed`
  // w transformowanym `.app__main` po przewinięciu strony.
  return createPortal(
    <>
      <div
        className="modal modal-blur fade show d-block"
        role="dialog"
        aria-modal="true"
        aria-labelledby={modalTitleId}
        style={{ display: 'block' }}
      >
        <div className="modal-dialog modal-dialog-centered" role="document">
          <div className="modal-content">
            <div className="modal-header">
              <h3 className="modal-title text-danger" id={modalTitleId}>
                Usuń sklep
              </h3>
              <button
                type="button"
                className="btn-close is-focusable"
                aria-label="Zamknij"
                onClick={onClose}
                disabled={deleting}
              />
            </div>
            <div className="modal-body">
              {error && (
                <div className="alert alert-danger" role="alert">
                  {error}
                </div>
              )}
              <div className="alert alert-danger" role="alert">
                <div className="fw-semibold mb-1">Ta operacja jest nieodwracalna.</div>
                <p className="mb-0">
                  Usuniesz sklep <strong>{store.name}</strong>. Powiązane transakcje{' '}
                  <strong>nie zostaną skasowane</strong> — stracą jedynie przypisanie do tego sklepu i
                  pozostaną na kontach (salda się nie zmienią).
                </p>
              </div>
              <p className="text-secondary mb-2">
                Jeśli chcesz jedynie ukryć sklep, zamknij to okno i użyj opcji{' '}
                <strong>Zarchiwizuj sklep</strong> — dane zostaną zachowane.
              </p>
              <label htmlFor="delete-store-confirmation" className="form-label">
                Aby potwierdzić, wpisz nazwę sklepu: <strong>{store.name}</strong>
              </label>
              <input
                id="delete-store-confirmation"
                type="text"
                className="form-control"
                autoComplete="off"
                autoFocus
                value={confirmationText}
                placeholder={store.name}
                onChange={(event: ChangeEvent<HTMLInputElement>) => setConfirmationText(event.target.value)}
                disabled={deleting}
              />
            </div>
            <div className="modal-footer">
              <button
                type="button"
                className="btn btn-link link-secondary is-focusable"
                onClick={onClose}
                disabled={deleting}
              >
                Anuluj
              </button>
              <button
                type="button"
                className="btn btn-danger is-focusable"
                onClick={() => void handleDelete()}
                disabled={!confirmed || deleting}
              >
                {deleting ? 'Usuwanie…' : 'Usuń sklep'}
              </button>
            </div>
          </div>
        </div>
      </div>
      <div className="modal-backdrop fade show" />
    </>,
    document.body
  );
};

export default DeleteStoreModal;
