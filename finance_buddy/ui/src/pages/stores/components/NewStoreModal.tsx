// Modal tworzenia sklepu. Montowany tylko, gdy jest otwarty — dzięki temu stan
// formularza resetuje się naturalnie przy każdym otwarciu (bez efektu z setState).
import { useState, type ChangeEvent, type FormEvent } from 'react';
import { StoreCreate, createStore } from '../../../api';
import { isValidNip } from '../../../utils/nip';
import { GROUP_NAME_DATALIST_ID } from '../model';

type NewStoreModalProps = {
  onClose: () => void;
  onCreated: () => Promise<void>;
};

type NewStoreFormState = {
  name: string;
  groupName: string;
  nip: string;
  address: string;
  comment: string;
};

const getInitialStoreFormState = (): NewStoreFormState => ({
  name: '',
  groupName: '',
  nip: '',
  address: '',
  comment: ''
});

const NewStoreModal = ({ onClose, onCreated }: NewStoreModalProps) => {
  const [formState, setFormState] = useState<NewStoreFormState>(() => getInitialStoreFormState());
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const modalTitleId = 'new-store-modal-title';

  const handleChange = (field: keyof NewStoreFormState) => (event: ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) =>
    setFormState((prev) => ({
      ...prev,
      [field]: event.target.value
    }));

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const name = formState.name.trim();
    const groupName = formState.groupName.trim();
    const nip = formState.nip.trim();
    const address = formState.address.trim();
    const comment = formState.comment.trim();

    if (name.length < 2) {
      setError('Nazwa sklepu musi mieć co najmniej 2 znaki.');
      return;
    }

    if (nip.length && !isValidNip(nip)) {
      setError('Nieprawidłowy NIP (błędna suma kontrolna).');
      return;
    }

    const payload: StoreCreate = {
      name,
      group_name: groupName.length ? groupName : undefined,
      nip: nip.length ? nip : undefined,
      address: address.length ? address : undefined,
      comment: comment.length ? comment : undefined
    };

    try {
      setSubmitting(true);
      await createStore(payload);
      await onCreated();
      onClose();
    } catch (err) {
      console.error(err);
      setError('Nie udało się utworzyć sklepu. Spróbuj ponownie.');
    } finally {
      setSubmitting(false);
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
              <h3 className="modal-title" id={modalTitleId}>
                Nowy sklep
              </h3>
              <button
                type="button"
                className="btn-close is-focusable"
                aria-label="Zamknij"
                onClick={onClose}
                disabled={submitting}
              />
            </div>
            <form onSubmit={handleSubmit}>
              <div className="modal-body">
                {error && (
                  <div className="alert alert-danger" role="alert">
                    {error}
                  </div>
                )}
                <div className="mb-3">
                  <label htmlFor="new-store-name" className="form-label">
                    Nazwa sklepu
                  </label>
                  <input
                    id="new-store-name"
                    type="text"
                    className="form-control"
                    placeholder="np. Lidl, Allegro"
                    value={formState.name}
                    onChange={handleChange('name')}
                    disabled={submitting}
                    required
                  />
                </div>
                <div className="mb-3">
                  <label htmlFor="new-store-group-name" className="form-label">
                    Nazwa grupowa (opcjonalnie)
                  </label>
                  <input
                    id="new-store-group-name"
                    type="text"
                    className="form-control"
                    placeholder="np. Biedronka"
                    value={formState.groupName}
                    onChange={handleChange('groupName')}
                    disabled={submitting}
                    list={GROUP_NAME_DATALIST_ID}
                  />
                  <p className="form-hint mb-0">Pozwala łączyć wiele sklepów pod wspólną nazwą.</p>
                </div>
                <div className="mb-3">
                  <label htmlFor="new-store-nip" className="form-label">
                    NIP (opcjonalnie)
                  </label>
                  <input
                    id="new-store-nip"
                    type="text"
                    className="form-control"
                    placeholder="1234567890"
                    value={formState.nip}
                    onChange={handleChange('nip')}
                    disabled={submitting}
                  />
                </div>
                <div className="mb-3">
                  <label htmlFor="new-store-address" className="form-label">
                    Adres (opcjonalnie)
                  </label>
                  <input
                    id="new-store-address"
                    type="text"
                    className="form-control"
                    placeholder="np. ul. Główna 10"
                    value={formState.address}
                    onChange={handleChange('address')}
                    disabled={submitting}
                  />
                </div>
                <div className="mb-0">
                  <label htmlFor="new-store-comment" className="form-label">
                    Komentarz (opcjonalnie)
                  </label>
                  <textarea
                    id="new-store-comment"
                    className="form-control"
                    rows={3}
                    placeholder="np. uwagi rozliczeniowe"
                    value={formState.comment}
                    onChange={handleChange('comment')}
                    disabled={submitting}
                  />
                </div>
              </div>
              <div className="modal-footer">
                <button
                  type="button"
                  className="btn btn-link link-secondary is-focusable"
                  onClick={onClose}
                  disabled={submitting}
                >
                  Anuluj
                </button>
                <button type="submit" className="btn btn-primary is-focusable" disabled={submitting}>
                  {submitting ? 'Zapisywanie…' : 'Dodaj sklep'}
                </button>
              </div>
            </form>
          </div>
        </div>
      </div>
      <div className="modal-backdrop fade show" />
    </>
  );
};

export default NewStoreModal;
