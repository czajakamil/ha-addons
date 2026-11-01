// Modal ustawień sklepu (payee, automatyczne tagowanie, reguły zmiany nazwy).
// Montowany tylko z wybranym sklepem — stan formularza inicjalizuje się przy
// montowaniu, więc nie potrzebuje efektu resetującego.
import { useState, type FormEvent } from 'react';
import { createPortal } from 'react-dom';
import { Category, Store, StoreUpdate, updateStore } from '../../../api';
import { isValidNip } from '../../../utils/nip';
import TagPathInput from '../../../components/tags/TagPathInput';

type ManageStoreModalProps = {
  store: Store;
  categories: Category[];
  /** Zamienia ścieżkę tagu na id, tworząc brakujące tagi (wraz z przodkami). */
  ensureCategory: (path: string) => Promise<string>;
  onClose: () => void;
  onSaved: (updated: Store) => void;
  /** Otwiera osobne, potwierdzane okno trwałego usunięcia tego sklepu. */
  onRequestDelete: () => void;
};

type ManageStoreFormState = {
  name: string;
  groupName: string;
  nip: string;
  address: string;
  comment: string;
  includeInPayeeList: boolean;
  automaticallyCategorizePayee: boolean;
  automaticCategoryPath: string;
  renamingRulesText: string;
};

const getInitialManageStoreFormState = (store: Store, categories: Category[]): ManageStoreFormState => ({
  name: store.name,
  groupName: store.group_name ?? '',
  nip: store.nip ?? '',
  address: store.address ?? '',
  comment: store.comment ?? '',
  includeInPayeeList: store.include_in_payee_list,
  automaticallyCategorizePayee: store.automatically_categorize_payee,
  automaticCategoryPath: categories.find((category) => category.id === store.automatic_category_id)?.path ?? '',
  renamingRulesText: (store.renaming_rules ?? []).join('\n')
});

const ManageStoreModal = ({
  store,
  categories,
  ensureCategory,
  onClose,
  onSaved,
  onRequestDelete
}: ManageStoreModalProps) => {
  const [formState, setFormState] = useState<ManageStoreFormState>(() =>
    getInitialManageStoreFormState(store, categories)
  );
  const [submitting, setSubmitting] = useState(false);
  const [togglingDeleted, setTogglingDeleted] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const modalTitleId = 'manage-store-modal-title';
  const displayLabel = store.group_name?.trim() || store.name;
  const busy = submitting || togglingDeleted;

  // Archiwizacja/przywrócenie sklepu — odwracalne ukrycie z głównej listy
  // (is_deleted). Trwałe usunięcie idzie przez osobne, potwierdzane okno.
  const handleToggleDeleted = async () => {
    try {
      setTogglingDeleted(true);
      setError(null);
      const updated = await updateStore(store.id, { is_deleted: !store.is_deleted });
      onSaved(updated);
      onClose();
    } catch (err) {
      console.error(err);
      setError('Nie udało się zaktualizować statusu sklepu.');
    } finally {
      setTogglingDeleted(false);
    }
  };

  const setField = <K extends keyof ManageStoreFormState>(field: K, value: ManageStoreFormState[K]) => {
    setFormState((prev) => ({ ...prev, [field]: value }));
  };

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();

    const normalizedGroupName = formState.groupName.trim();
    const normalizedName = formState.name.trim();
    const normalizedNip = formState.nip.trim();
    const normalizedAddress = formState.address.trim();
    const normalizedComment = formState.comment.trim();
    const normalizedRules = formState.renamingRulesText
      .split(/\r?\n/)
      .map((rule) => rule.trim())
      .filter(Boolean);
    const normalizedTagPath = formState.automaticCategoryPath.trim();

    if (!normalizedName || normalizedName.length < 2) {
      setError('Oryginalna nazwa sklepu musi mieć co najmniej 2 znaki.');
      return;
    }

    if (normalizedNip.length && !isValidNip(normalizedNip)) {
      setError('Nieprawidłowy NIP (błędna suma kontrolna).');
      return;
    }

    try {
      setSubmitting(true);
      setError(null);

      // Wpisana ścieżka tagu → id (nieistniejące tagi powstają wraz z przodkami).
      let normalizedAutomaticCategoryId: string | null = null;
      if (formState.automaticallyCategorizePayee && normalizedTagPath) {
        try {
          normalizedAutomaticCategoryId = await ensureCategory(normalizedTagPath);
        } catch (err) {
          console.error(err);
          setError('Nie udało się utworzyć tagu dla sklepu.');
          return;
        }
      }

      const payload: StoreUpdate = {};
      if (store.name !== normalizedName) {
        payload.name = normalizedName;
      }
      if ((store.group_name ?? '') !== normalizedGroupName) {
        payload.group_name = normalizedGroupName || null;
      }
      if ((store.nip ?? '') !== normalizedNip) {
        payload.nip = normalizedNip || null;
      }
      if ((store.address ?? '') !== normalizedAddress) {
        payload.address = normalizedAddress || null;
      }
      if ((store.comment ?? '') !== normalizedComment) {
        payload.comment = normalizedComment || null;
      }
      if (store.include_in_payee_list !== formState.includeInPayeeList) {
        payload.include_in_payee_list = formState.includeInPayeeList;
      }
      if (store.automatically_categorize_payee !== formState.automaticallyCategorizePayee) {
        payload.automatically_categorize_payee = formState.automaticallyCategorizePayee;
      }
      if ((store.automatic_category_id ?? null) !== normalizedAutomaticCategoryId) {
        payload.automatic_category_id = normalizedAutomaticCategoryId;
      }
      if (JSON.stringify(store.renaming_rules ?? []) !== JSON.stringify(normalizedRules)) {
        payload.renaming_rules = normalizedRules;
      }

      if (Object.keys(payload).length === 0) {
        onClose();
        return;
      }

      const updated = await updateStore(store.id, payload);
      onSaved(updated);
      onClose();
    } catch (err) {
      console.error(err);
      setError('Nie udało się zapisać ustawień sklepu.');
    } finally {
      setSubmitting(false);
    }
  };

  // Renderujemy przez portal do <body>, żeby modal nie był uwięziony w
  // `.app__main`, które po przewinięciu strony dostaje `transform` i staje się
  // blokiem zawierającym dla `position: fixed`. Bez tego wysokość `.modal` (100%)
  // nie rozwiązywała się do wysokości okna i całość przestawała się przewijać —
  // stopka i strefa usuwania wypadały poza ekran. Przewija się całe `.modal`
  // (jak w pozostałych modalach), więc NIE używamy `modal-dialog-scrollable`,
  // które i tak nie działa przy `.modal-body` opakowanym w `<form>`.
  return createPortal(
    <>
      <div
        className="modal modal-blur fade show d-block"
        role="dialog"
        aria-modal="true"
        aria-labelledby={modalTitleId}
        style={{ display: 'block' }}
      >
        <div className="modal-dialog modal-xl" role="document">
          <div className="modal-content">
            <div className="modal-header">
              <div>
                <h3 className="modal-title" id={modalTitleId}>
                  Zarządzaj sklepem
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
                    <label className="form-label" htmlFor="manage-store-name">
                      Oryginalna nazwa sklepu
                    </label>
                    <input
                      id="manage-store-name"
                      type="text"
                      className="form-control"
                      value={formState.name}
                      onChange={(event) => setField('name', event.target.value)}
                      disabled={submitting}
                      required
                    />
                  </div>

                  <div className="col-lg-6">
                    <label className="form-label" htmlFor="manage-store-group-name">
                      Nazwa grupowa
                    </label>
                    <input
                      id="manage-store-group-name"
                      type="text"
                      className="form-control"
                      value={formState.groupName}
                      onChange={(event) => setField('groupName', event.target.value)}
                      disabled={submitting}
                      placeholder={formState.name || store.name}
                    />
                    <p className="form-hint mb-0">To nazwa używana do grupowania i podpowiedzi payee.</p>
                  </div>

                  <div className="col-lg-6">
                    <label className="form-label" htmlFor="manage-store-nip">
                      NIP
                    </label>
                    <input
                      id="manage-store-nip"
                      type="text"
                      className="form-control"
                      value={formState.nip}
                      onChange={(event) => setField('nip', event.target.value)}
                      disabled={submitting}
                      placeholder="1234567890"
                    />
                  </div>

                  <div className="col-lg-6">
                    <label className="form-label" htmlFor="manage-store-address">
                      Adres
                    </label>
                    <input
                      id="manage-store-address"
                      type="text"
                      className="form-control"
                      value={formState.address}
                      onChange={(event) => setField('address', event.target.value)}
                      disabled={submitting}
                      placeholder="np. ul. Główna 10"
                    />
                  </div>

                  <div className="col-12">
                    <label className="form-label" htmlFor="manage-store-comment">
                      Komentarz
                    </label>
                    <textarea
                      id="manage-store-comment"
                      className="form-control"
                      rows={3}
                      value={formState.comment}
                      onChange={(event) => setField('comment', event.target.value)}
                      disabled={submitting}
                      placeholder="np. uwagi rozliczeniowe"
                    />
                  </div>

                  <div className="col-12">
                    <div className="d-flex align-items-center gap-2">
                      <input
                        id="manage-store-include-payee-list"
                        type="checkbox"
                        className="form-check-input m-0"
                        style={{ float: 'none' }}
                        checked={formState.includeInPayeeList}
                        onChange={(event) => setField('includeInPayeeList', event.target.checked)}
                        disabled={submitting}
                      />
                      <label
                        className="form-check-label mb-0"
                        htmlFor="manage-store-include-payee-list"
                      >
                        Uwzględniaj na liście payee
                      </label>
                    </div>
                  </div>

                  <div className="col-12">
                    <div className="d-flex align-items-center gap-2">
                      <input
                        id="manage-store-auto-categorize"
                        type="checkbox"
                        className="form-check-input m-0"
                        style={{ float: 'none' }}
                        checked={formState.automaticallyCategorizePayee}
                        onChange={(event) =>
                          setField('automaticallyCategorizePayee', event.target.checked)
                        }
                        disabled={submitting}
                      />
                      <label className="form-check-label mb-0" htmlFor="manage-store-auto-categorize">
                        Automatycznie taguj payee
                      </label>
                    </div>
                  </div>

                  <div className="col-lg-8">
                    <label className="form-label" htmlFor="manage-store-category">
                      Domyślny tag
                    </label>
                    <TagPathInput
                      inputId="manage-store-category"
                      value={formState.automaticCategoryPath}
                      categories={categories}
                      forAccounts={false}
                      disabled={submitting}
                      placeholder="np. codzienne wydatki/spożywcze"
                      onChange={(value) => {
                        setField('automaticCategoryPath', value);
                        if (value.trim() && !formState.automaticallyCategorizePayee) {
                          setField('automaticallyCategorizePayee', true);
                        }
                      }}
                    />
                    <p className="form-hint mb-0">
                      {formState.automaticallyCategorizePayee
                        ? 'Nieistniejący tag zostanie utworzony przy zapisie.'
                        : 'Wpisanie tagu automatycznie włączy auto-tagowanie payee.'}
                    </p>
                  </div>

                  <div className="col-12">
                    <label className="form-label" htmlFor="manage-store-renaming-rules">
                      Reguły zmiany nazwy (po jednej w linii)
                    </label>
                    <textarea
                      id="manage-store-renaming-rules"
                      className="form-control"
                      rows={5}
                      value={formState.renamingRulesText}
                      onChange={(event) => setField('renamingRulesText', event.target.value)}
                      disabled={submitting}
                      placeholder="np. KFN&#10;KlanFINN"
                    />
                    <p className="form-hint mb-0">
                      Importowane nazwy pasujące do tych fraz mogą być mapowane do tego sklepu.
                    </p>
                  </div>
                </div>

                <div className="accounts-edit-danger-zone">
                  <div className="accounts-edit-danger-zone__copy">
                    <div className="fw-semibold">
                      {store.is_deleted ? 'Sklep jest zarchiwizowany' : 'Archiwizacja sklepu'}
                    </div>
                    <p className="text-secondary small mb-0">
                      {store.is_deleted
                        ? 'Przywróć sklep, aby wrócił na główną listę. Transakcje pozostają nienaruszone.'
                        : 'Zarchiwizuj sklep, aby ukryć go na liście. Dane i transakcje zostają zachowane — akcję można cofnąć.'}
                    </p>
                  </div>
                  <button
                    type="button"
                    className={`btn is-focusable ${store.is_deleted ? 'btn-outline-primary' : 'btn-outline-secondary'}`}
                    onClick={() => void handleToggleDeleted()}
                    disabled={busy}
                  >
                    {togglingDeleted
                      ? 'Aktualizowanie…'
                      : store.is_deleted
                        ? 'Przywróć sklep'
                        : 'Zarchiwizuj sklep'}
                  </button>
                </div>

                <div className="accounts-edit-danger-zone accounts-edit-danger-zone--destructive">
                  <div className="accounts-edit-danger-zone__copy">
                    <div className="fw-semibold text-danger">Usunięcie sklepu</div>
                    <p className="text-secondary small mb-0">
                      Trwale usuwa sklep. Powiązane transakcje <strong>nie są kasowane</strong> — tracą
                      jedynie przypisanie do sklepu. Tej operacji nie można cofnąć.
                    </p>
                  </div>
                  <button
                    type="button"
                    className="btn btn-outline-danger is-focusable"
                    onClick={onRequestDelete}
                    disabled={busy}
                  >
                    Usuń sklep
                  </button>
                </div>
              </div>
              <div className="modal-footer">
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
            </form>
          </div>
        </div>
      </div>
      <div className="modal-backdrop fade show" onClick={busy ? undefined : onClose} />
    </>,
    document.body
  );
};

export default ManageStoreModal;
