// Modal potwierdzenia trwałego usunięcia konta. Kasowanie konta pociąga za sobą
// usunięcie WSZYSTKICH powiązanych transakcji, więc wymagamy dodatkowego,
// świadomego potwierdzenia: użytkownik musi wpisać nazwę konta, żeby odblokować
// przycisk usuwania.
import { useState, type ChangeEvent } from 'react';
import { Account, deleteAccount } from '../../../api';
import { formatMoney } from '../../../utils/format';

type DeleteAccountModalProps = {
  account: Account;
  onClose: () => void;
  onDeleted: () => Promise<void>;
};

const DeleteAccountModal = ({ account, onClose, onDeleted }: DeleteAccountModalProps) => {
  const [confirmationText, setConfirmationText] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [deleting, setDeleting] = useState(false);
  const modalTitleId = 'delete-account-modal-title';

  const confirmed = confirmationText.trim() === account.name.trim();

  const handleDelete = async () => {
    if (!confirmed || deleting) return;
    try {
      setDeleting(true);
      await deleteAccount(account.id);
      await onDeleted();
      onClose();
    } catch (err) {
      console.error(err);
      setError('Nie udało się usunąć konta. Spróbuj ponownie.');
      setDeleting(false);
    }
  };

  return (
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
                Usuń konto
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
                  Usuniesz konto <strong>{account.name}</strong> (saldo{' '}
                  {formatMoney(account.balance_cents, account.currency)}) oraz{' '}
                  <strong>wszystkie powiązane z nim transakcje</strong>. Nie da się tego cofnąć.
                </p>
              </div>
              <p className="text-secondary mb-2">
                Jeśli chcesz jedynie ukryć konto, zamknij to okno i użyj opcji{' '}
                <strong>Zarchiwizuj konto</strong> — dane zostaną zachowane.
              </p>
              <label htmlFor="delete-account-confirmation" className="form-label">
                Aby potwierdzić, wpisz nazwę konta: <strong>{account.name}</strong>
              </label>
              <input
                id="delete-account-confirmation"
                type="text"
                className="form-control"
                autoComplete="off"
                autoFocus
                value={confirmationText}
                placeholder={account.name}
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
                {deleting ? 'Usuwanie…' : 'Usuń konto i transakcje'}
              </button>
            </div>
          </div>
        </div>
      </div>
      <div className="modal-backdrop fade show" />
    </>
  );
};

export default DeleteAccountModal;
