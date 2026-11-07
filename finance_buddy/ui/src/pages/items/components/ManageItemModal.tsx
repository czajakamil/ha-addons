// Modal edycji przedmiotu — zbiera wszystkie pola w jednym miejscu (dwuklik na
// wierszu edytuje pojedynczą komórkę, ołówek otwiera pełny formularz).
// Montowany tylko z wybranym przedmiotem — stan formularza inicjalizuje się przy
// montowaniu, więc nie potrzebuje efektu resetującego.
import { useState, type FormEvent } from 'react';
import { createPortal } from 'react-dom';
import { Category, Item, ItemUpdate, deleteItem, updateItem } from '../../../api';
import TagPathInput from '../../../components/tags/TagPathInput';
import { getItemLabel } from '../model';
import ItemAliasesSection from './ItemAliasesSection';
import ItemMergeSection from './ItemMergeSection';

type ManageItemModalProps = {
  item: Item;
  /** Wszystkie przedmioty — kandydaci na cel scalenia. */
  items: Item[];
  usageById: Map<string, number>;
  categories: Category[];
  /** Zamienia ścieżkę tagu na id, tworząc brakujące tagi (wraz z przodkami). */
  ensureCategory: (path: string) => Promise<string>;
  onClose: () => void;
  onSaved: (updated: Item) => void;
  onDeleted: (id: string) => void;
  onMerged: (sourceId: string, targetId: string) => void;
};

// Backend zwraca błędy jako `{"detail": "..."}` (np. 409 przy usuwaniu przedmiotu
// używanego w transakcjach) — wyciągamy z tego czytelny komunikat, jeśli się da.
const extractErrorDetail = (err: unknown): string | null => {
  if (!(err instanceof Error) || !err.message) {
    return null;
  }
  try {
    const parsed = JSON.parse(err.message) as { detail?: unknown };
    if (typeof parsed.detail === 'string') {
      return parsed.detail;
    }
  } catch {
    // Komunikat nie jest JSON-em — pomijamy.
  }
  return null;
};

type ManageItemFormState = {
  name: string;
  groupName: string;
  categoryPath: string;
  unit: string;
  manufacturer: string;
  comment: string;
};

const getInitialManageItemFormState = (item: Item, categories: Category[]): ManageItemFormState => ({
  name: item.name,
  groupName: item.group_name ?? '',
  categoryPath: categories.find((category) => category.id === item.category_id)?.path ?? '',
  unit: item.unit_of_measurement ?? '',
  manufacturer: item.manufacturer ?? '',
  comment: item.comment ?? ''
});

const ManageItemModal = ({
  item,
  items,
  usageById,
  categories,
  ensureCategory,
  onClose,
  onSaved,
  onDeleted,
  onMerged
}: ManageItemModalProps) => {
  const [formState, setFormState] = useState<ManageItemFormState>(() =>
    getInitialManageItemFormState(item, categories)
  );
  const [submitting, setSubmitting] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [confirmingDelete, setConfirmingDelete] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [merging, setMerging] = useState(false);

  const busy = submitting || deleting || merging;

  const modalTitleId = 'manage-item-modal-title';
  const displayLabel = getItemLabel(item);

  const setField = <K extends keyof ManageItemFormState>(field: K, value: ManageItemFormState[K]) => {
    setFormState((prev) => ({ ...prev, [field]: value }));
  };

  const handleDelete = async () => {
    try {
      setDeleting(true);
      setError(null);
      await deleteItem(item.id);
      onDeleted(item.id);
      onClose();
    } catch (err) {
      console.error(err);
      setError(extractErrorDetail(err) ?? 'Nie udało się usunąć przedmiotu.');
      setConfirmingDelete(false);
      setDeleting(false);
    }
  };

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();

    const normalizedName = formState.name.trim();
    const normalizedGroupName = formState.groupName.trim();
    const normalizedUnit = formState.unit.trim();
    const normalizedManufacturer = formState.manufacturer.trim();
    const normalizedComment = formState.comment.trim();
    const normalizedTagPath = formState.categoryPath.trim();

    if (normalizedName.length < 2) {
      setError('Nazwa przedmiotu musi mieć co najmniej 2 znaki.');
      return;
    }

    try {
      setSubmitting(true);
      setError(null);

      // Wpisana ścieżka tagu → id (nieistniejące tagi powstają wraz z przodkami).
      let normalizedCategoryId: string | null = null;
      if (normalizedTagPath) {
        try {
          normalizedCategoryId = await ensureCategory(normalizedTagPath);
        } catch (err) {
          console.error(err);
          setError('Nie udało się utworzyć tagu.');
          return;
        }
      }

      const payload: ItemUpdate = {};
      if (item.name !== normalizedName) {
        payload.name = normalizedName;
      }
      if ((item.group_name ?? '') !== normalizedGroupName) {
        payload.group_name = normalizedGroupName || null;
      }
      if ((item.unit_of_measurement ?? '') !== normalizedUnit) {
        payload.unit_of_measurement = normalizedUnit || null;
      }
      if ((item.manufacturer ?? '') !== normalizedManufacturer) {
        payload.manufacturer = normalizedManufacturer || null;
      }
      if ((item.comment ?? '') !== normalizedComment) {
        payload.comment = normalizedComment || null;
      }
      if ((item.category_id ?? null) !== normalizedCategoryId) {
        payload.category_id = normalizedCategoryId;
      }

      if (Object.keys(payload).length === 0) {
        onClose();
        return;
      }

      const updated = await updateItem(item.id, payload);
      onSaved(updated);
      onClose();
    } catch (err) {
      console.error(err);
      setError('Nie udało się zapisać danych przedmiotu.');
    } finally {
      setSubmitting(false);
    }
  };

  // Renderujemy przez portal do <body> — tak jak modal sklepu — żeby modal nie był
  // uwięziony w `.app__main`, które po przewinięciu dostaje `transform` i staje się
  // blokiem zawierającym dla `position: fixed`.
  return createPortal(
    <>
      <div
        className="modal modal-blur fade show d-block"
        role="dialog"
        aria-modal="true"
        aria-labelledby={modalTitleId}
        style={{ display: 'block' }}
      >
        <div className="modal-dialog modal-dialog-centered modal-lg" role="document">
          <div className="modal-content">
            <div className="modal-header">
              <div>
                <h3 className="modal-title" id={modalTitleId}>
                  Zarządzaj przedmiotem
                </h3>
                <p className="text-secondary mb-0 small">{displayLabel}</p>
              </div>
              <button
                type="button"
                className="btn-close is-focusable"
                aria-label="Zamknij"
                onClick={onClose}
                disabled={busy}
              />
            </div>
            <form onSubmit={handleSubmit}>
              <div className="modal-body">
                {error && (
                  <div className="alert alert-danger" role="alert">
                    {error}
                  </div>
                )}

                <div className="row g-3">
                  <div className="col-lg-6">
                    <label className="form-label" htmlFor="manage-item-name">
                      Nazwa
                    </label>
                    <input
                      id="manage-item-name"
                      type="text"
                      className="form-control"
                      value={formState.name}
                      onChange={(event) => setField('name', event.target.value)}
                      disabled={busy}
                      required
                    />
                  </div>

                  <div className="col-lg-6">
                    <label className="form-label" htmlFor="manage-item-group-name">
                      Nazwa grupowa
                    </label>
                    <input
                      id="manage-item-group-name"
                      type="text"
                      className="form-control"
                      value={formState.groupName}
                      onChange={(event) => setField('groupName', event.target.value)}
                      disabled={busy}
                      placeholder={formState.name || item.name}
                    />
                    <p className="form-hint mb-0">Pozwala połączyć podobne pozycje pod wspólną nazwą.</p>
                  </div>

                  <div className="col-12">
                    <label className="form-label" htmlFor="manage-item-category">
                      Tag
                    </label>
                    <TagPathInput
                      inputId="manage-item-category"
                      value={formState.categoryPath}
                      categories={categories}
                      forAccounts={false}
                      disabled={busy}
                      placeholder="np. dom/remont"
                      onChange={(value) => setField('categoryPath', value)}
                    />
                    <p className="form-hint mb-0">Nieistniejący tag zostanie utworzony przy zapisie.</p>
                  </div>

                  <div className="col-lg-6">
                    <label className="form-label" htmlFor="manage-item-unit">
                      Jednostka
                    </label>
                    <input
                      id="manage-item-unit"
                      type="text"
                      className="form-control"
                      value={formState.unit}
                      onChange={(event) => setField('unit', event.target.value)}
                      disabled={busy}
                      placeholder="np. szt, kg"
                    />
                  </div>

                  <div className="col-lg-6">
                    <label className="form-label" htmlFor="manage-item-manufacturer">
                      Producent
                    </label>
                    <input
                      id="manage-item-manufacturer"
                      type="text"
                      className="form-control"
                      value={formState.manufacturer}
                      onChange={(event) => setField('manufacturer', event.target.value)}
                      disabled={busy}
                      placeholder="np. Mlekovita"
                    />
                  </div>

                  <div className="col-12">
                    <label className="form-label" htmlFor="manage-item-comment">
                      Komentarz
                    </label>
                    <textarea
                      id="manage-item-comment"
                      className="form-control"
                      rows={3}
                      value={formState.comment}
                      onChange={(event) => setField('comment', event.target.value)}
                      disabled={busy}
                      placeholder="np. ulubiony wariant"
                    />
                  </div>
                </div>

                <div className="item-sections">
                  <ItemAliasesSection itemId={item.id} disabled={busy} />
                  <ItemMergeSection
                    item={item}
                    items={items}
                    usageById={usageById}
                    disabled={submitting || deleting}
                    onBusyChange={setMerging}
                    onMerged={onMerged}
                  />
                </div>
              </div>
              <div className="modal-footer justify-content-between">
                {confirmingDelete ? (
                  <div className="d-flex align-items-center gap-2">
                    <span className="text-danger small">Usunąć przedmiot?</span>
                    <button
                      type="button"
                      className="btn btn-danger btn-sm is-focusable"
                      onClick={() => void handleDelete()}
                      disabled={busy}
                    >
                      {deleting ? 'Usuwanie…' : 'Tak, usuń'}
                    </button>
                    <button
                      type="button"
                      className="btn btn-link link-secondary btn-sm is-focusable"
                      onClick={() => setConfirmingDelete(false)}
                      disabled={busy}
                    >
                      Nie
                    </button>
                  </div>
                ) : (
                  <button
                    type="button"
                    className="btn btn-outline-danger is-focusable"
                    onClick={() => {
                      setError(null);
                      setConfirmingDelete(true);
                    }}
                    disabled={busy}
                  >
                    Usuń przedmiot
                  </button>
                )}
                <div className="d-flex gap-2">
                  <button
                    type="button"
                    className="btn btn-link link-secondary is-focusable"
                    onClick={onClose}
                    disabled={busy}
                  >
                    Anuluj
                  </button>
                  <button type="submit" className="btn btn-primary is-focusable" disabled={busy}>
                    {submitting ? 'Zapisywanie…' : 'Zapisz'}
                  </button>
                </div>
              </div>
            </form>
          </div>
        </div>
      </div>
      <div className="modal-backdrop fade show" onClick={busy ? undefined : onClose} />
    </>,
    document.body
  );
};

export default ManageItemModal;
