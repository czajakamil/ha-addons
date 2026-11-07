// Modal tworzenia przedmiotu. Montowany tylko, gdy jest otwarty — dzięki temu
// stan formularza resetuje się naturalnie przy każdym otwarciu (bez efektu z setState).
import { useState, type ChangeEvent, type FormEvent } from 'react';
import { Category, ItemCreate, createItem } from '../../../api';
import TagPathInput from '../../../components/tags/TagPathInput';
import { GROUP_NAME_DATALIST_ID } from '../model';

type NewItemModalProps = {
  onClose: () => void;
  onCreated: () => Promise<void>;
  categories: Category[];
  /** Zamienia ścieżkę tagu na id, tworząc brakujące tagi (wraz z przodkami). */
  ensureCategory: (path: string) => Promise<string>;
};

type NewItemFormState = {
  name: string;
  groupName: string;
  unit: string;
  manufacturer: string;
  comment: string;
  categoryPath: string;
};

const getInitialItemFormState = (): NewItemFormState => ({
  name: '',
  groupName: '',
  unit: '',
  manufacturer: '',
  comment: '',
  categoryPath: ''
});

const NewItemModal = ({ onClose, onCreated, categories, ensureCategory }: NewItemModalProps) => {
  const [formState, setFormState] = useState<NewItemFormState>(() => getInitialItemFormState());
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const modalTitleId = 'new-item-modal-title';

  const handleChange = (field: keyof NewItemFormState) => (event: ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) =>
    setFormState((prev) => ({
      ...prev,
      [field]: event.target.value
    }));

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const name = formState.name.trim();
    const groupName = formState.groupName.trim();
    const unit = formState.unit.trim();
    const manufacturer = formState.manufacturer.trim();
    const comment = formState.comment.trim();
    const categoryPath = formState.categoryPath.trim();

    if (name.length < 2) {
      setError('Nazwa przedmiotu musi mieć co najmniej 2 znaki.');
      return;
    }

    const payload: ItemCreate = { name };
    if (groupName.length) payload.group_name = groupName;
    if (unit.length) payload.unit_of_measurement = unit;
    if (manufacturer.length) payload.manufacturer = manufacturer;
    if (comment.length) payload.comment = comment;
    if (categoryPath.length) {
      try {
        payload.category_id = await ensureCategory(categoryPath);
      } catch (err) {
        console.error(err);
        setError('Nie udało się utworzyć kategorii.');
        return;
      }
    }

    try {
      setSubmitting(true);
      await createItem(payload);
      await onCreated();
      onClose();
    } catch (err) {
      console.error(err);
      setError('Nie udało się utworzyć przedmiotu. Spróbuj ponownie.');
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
                Nowy przedmiot
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
                  <label htmlFor="new-item-name" className="form-label">
                    Nazwa
                  </label>
                  <input
                    id="new-item-name"
                    type="text"
                    className="form-control"
                    placeholder="np. Mleko 3,2%"
                    value={formState.name}
                    onChange={handleChange('name')}
                    disabled={submitting}
                    required
                  />
                </div>
                <div className="mb-3">
                  <label htmlFor="new-item-category" className="form-label">
                    Kategoria (opcjonalnie)
                  </label>
                  <TagPathInput
                    inputId="new-item-category"
                    value={formState.categoryPath}
                    categories={categories}
                    forAccounts={false}
                    disabled={submitting}
                    placeholder="np. dom/remont"
                    onChange={(value) => setFormState((prev) => ({ ...prev, categoryPath: value }))}
                  />
                  <p className="form-hint mb-0">Wpisz ścieżkę kategorii, np. "dom/remont".</p>
                </div>
                <div className="mb-3">
                  <label htmlFor="new-item-group-name" className="form-label">
                    Nazwa grupowa (opcjonalnie)
                  </label>
                  <input
                    id="new-item-group-name"
                    type="text"
                    className="form-control"
                    placeholder="np. Mleko"
                    value={formState.groupName}
                    onChange={handleChange('groupName')}
                    disabled={submitting}
                    list={GROUP_NAME_DATALIST_ID}
                  />
                  <p className="form-hint mb-0">Pozwala połączyć podobne pozycje pod wspólną nazwą.</p>
                </div>
                <div className="mb-3">
                  <label htmlFor="new-item-unit" className="form-label">
                    Jednostka (opcjonalnie)
                  </label>
                  <input
                    id="new-item-unit"
                    type="text"
                    className="form-control"
                    placeholder="np. szt, kg"
                    value={formState.unit}
                    onChange={handleChange('unit')}
                    disabled={submitting}
                  />
                </div>
                <div className="mb-3">
                  <label htmlFor="new-item-manufacturer" className="form-label">
                    Producent (opcjonalnie)
                  </label>
                  <input
                    id="new-item-manufacturer"
                    type="text"
                    className="form-control"
                    placeholder="np. Mlekovita"
                    value={formState.manufacturer}
                    onChange={handleChange('manufacturer')}
                    disabled={submitting}
                  />
                </div>
                <div className="mb-0">
                  <label htmlFor="new-item-comment" className="form-label">
                    Komentarz (opcjonalnie)
                  </label>
                  <textarea
                    id="new-item-comment"
                    className="form-control"
                    rows={3}
                    placeholder="np. ulubiony wariant"
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
                  {submitting ? 'Zapisywanie…' : 'Dodaj przedmiot'}
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

export default NewItemModal;
