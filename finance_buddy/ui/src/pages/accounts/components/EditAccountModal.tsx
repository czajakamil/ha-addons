// Modal edycji istniejącego konta. Montowany tylko, gdy jest otwarty — stan
// formularza inicjalizujemy z konta w chwili otwarcia (bez efektu z setState).
// Poza edycją pól udostępnia akcje z "danger zone": archiwizację/przywrócenie
// konta (ukrycie na liście, odwracalne) oraz trwałe usunięcie (przekierowane do
// osobnego, potwierdzanego modalu, bo kasuje też wszystkie transakcje konta).
import { useState, type ChangeEvent, type FormEvent } from 'react';
import { Account, AccountKind, AccountUpdate, Category, updateAccount } from '../../../api';
import TagPathInput from '../../../components/tags/TagPathInput';
import { currencyOptions, type AccountKindOption, type BudgetLabelValue } from '../model';
import { DEFAULT_ACCOUNT_COLOR, DEFAULT_ACCOUNT_ICON } from '../accountPresentation';
import AccountIconPicker from './AccountIconPicker';

type EditAccountModalProps = {
  account: Account;
  onClose: () => void;
  onSaved: () => Promise<void>;
  onRequestDelete: () => void;
  categories: Category[];
  ensureAccountCategory: (label: string) => Promise<string>;
  getCategoryLabel: (categoryId?: string | null) => string;
  accountKindOptions: AccountKindOption[];
};

type EditAccountFormState = {
  name: string;
  currency: string;
  comment: string;
  isOnBudget: BudgetLabelValue;
  accountKind: AccountKind;
  categoryName: string;
  icon: string;
  color: string;
};

const EditAccountModal = ({
  account,
  onClose,
  onSaved,
  onRequestDelete,
  categories,
  ensureAccountCategory,
  getCategoryLabel,
  accountKindOptions
}: EditAccountModalProps) => {
  const [formState, setFormState] = useState<EditAccountFormState>(() => ({
    name: account.name,
    currency: account.currency,
    comment: account.comment ?? '',
    isOnBudget: account.is_on_budget ? 'on' : 'off',
    accountKind: account.account_kind,
    categoryName: getCategoryLabel(account.category_id),
    icon: account.icon ?? DEFAULT_ACCOUNT_ICON,
    color: account.color ?? DEFAULT_ACCOUNT_COLOR
  }));
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [togglingClosed, setTogglingClosed] = useState(false);
  const modalTitleId = 'edit-account-modal-title';

  // Gdy lista typów kont zmieni się w trakcie edycji i wybrany typ zniknie,
  // pokazujemy pierwszą dostępną opcję (wyliczenie zamiast efektu synchronizującego).
  const selectedAccountKind = accountKindOptions.some((option) => option.value === formState.accountKind)
    ? formState.accountKind
    : accountKindOptions[0]?.value ?? formState.accountKind;

  const busy = submitting || togglingClosed;

  const handleChange = (field: keyof EditAccountFormState) =>
    (event: ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => {
      const value = event.target.value;
      setFormState((prev) => ({
        ...prev,
        [field]: field === 'currency' ? value.toUpperCase() : value
      }));
    };

  const handleBudgetStatusChange = (event: ChangeEvent<HTMLSelectElement>) => {
    const value = event.target.value as BudgetLabelValue;
    setFormState((prev) => ({ ...prev, isOnBudget: value }));
  };

  const handleAccountKindChange = (event: ChangeEvent<HTMLSelectElement>) => {
    const value = event.target.value as AccountKind;
    setFormState((prev) => ({ ...prev, accountKind: value }));
  };

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const name = formState.name.trim();
    const currency = formState.currency.trim().toUpperCase();
    const comment = formState.comment.trim();
    const categoryName = formState.categoryName.trim();

    if (name.length < 2) {
      setError('Nazwa konta musi mieć co najmniej 2 znaki.');
      return;
    }

    if (!/^[A-Z]{3}$/.test(currency)) {
      setError('Waluta musi mieć format ISO 4217 (np. PLN, USD).');
      return;
    }

    let categoryId: string | null = null;
    if (categoryName.length) {
      try {
        categoryId = await ensureAccountCategory(categoryName);
      } catch (err) {
        console.error(err);
        setError('Nie udało się utworzyć tagu konta.');
        return;
      }
    }

    // Wysyłamy tylko zmienione pola — dzięki temu PATCH nie nadpisuje wartości,
    // których użytkownik nie dotknął, i unika zbędnych zapisów.
    const payload: AccountUpdate = {};
    if (name !== account.name) payload.name = name;
    if (currency !== account.currency) payload.currency = currency;
    const nextComment = comment.length ? comment : null;
    if (nextComment !== (account.comment ?? null)) payload.comment = nextComment;
    const nextIsOnBudget = formState.isOnBudget === 'on';
    if (nextIsOnBudget !== account.is_on_budget) payload.is_on_budget = nextIsOnBudget;
    if (selectedAccountKind !== account.account_kind) payload.account_kind = selectedAccountKind;
    if (categoryId !== (account.category_id ?? null)) payload.category_id = categoryId;
    if (formState.icon !== (account.icon ?? DEFAULT_ACCOUNT_ICON)) payload.icon = formState.icon;
    if (formState.color !== (account.color ?? DEFAULT_ACCOUNT_COLOR)) payload.color = formState.color;

    if (Object.keys(payload).length === 0) {
      onClose();
      return;
    }

    try {
      setSubmitting(true);
      await updateAccount(account.id, payload);
      await onSaved();
      onClose();
    } catch (err) {
      console.error(err);
      setError('Nie udało się zapisać zmian. Spróbuj ponownie.');
    } finally {
      setSubmitting(false);
    }
  };

  const handleToggleClosed = async () => {
    try {
      setTogglingClosed(true);
      await updateAccount(account.id, { is_closed: !account.is_closed });
      await onSaved();
      onClose();
    } catch (err) {
      console.error(err);
      setError('Nie udało się zaktualizować statusu konta.');
    } finally {
      setTogglingClosed(false);
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
                Edycja konta
              </h3>
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
                <div className="mb-3">
                  <label htmlFor="edit-account-name" className="form-label">
                    Nazwa konta
                  </label>
                  <input
                    id="edit-account-name"
                    type="text"
                    className="form-control"
                    placeholder="np. Konto osobiste"
                    value={formState.name}
                    onChange={handleChange('name')}
                    disabled={busy}
                    required
                  />
                </div>
                <div className="mb-3">
                  <label className="form-label">Ikona i kolor</label>
                  <AccountIconPicker
                    icon={formState.icon}
                    color={formState.color}
                    onIconChange={(icon) => setFormState((prev) => ({ ...prev, icon }))}
                    onColorChange={(color) => setFormState((prev) => ({ ...prev, color }))}
                    disabled={busy}
                  />
                </div>
                <div className="mb-3">
                  <label htmlFor="edit-account-currency" className="form-label">
                    Waluta (ISO 4217)
                  </label>
                  <input
                    id="edit-account-currency"
                    type="text"
                    className="form-control"
                    placeholder="PLN"
                    value={formState.currency}
                    onChange={handleChange('currency')}
                    maxLength={3}
                    list="edit-account-currency-options"
                    disabled={busy}
                    required
                  />
                  <datalist id="edit-account-currency-options">
                    {currencyOptions.map(({ code, name }) => (
                      <option key={code} value={code}>
                        {name}
                      </option>
                    ))}
                  </datalist>
                </div>
                <div className="mb-3">
                  <label htmlFor="edit-account-category" className="form-label">
                    Tag konta (opcjonalnie)
                  </label>
                  <TagPathInput
                    inputId="edit-account-category"
                    value={formState.categoryName}
                    categories={categories}
                    forAccounts
                    disabled={busy}
                    placeholder="np. inwestycje/obligacje"
                    onChange={(value) => setFormState((prev) => ({ ...prev, categoryName: value }))}
                  />
                  <div className="form-text text-secondary mb-0">
                    Wybierz typ konta korzystając z hierarchii tagów (np. inwestycje/obligacje).
                  </div>
                </div>
                <div className="mb-3">
                  <label htmlFor="edit-account-budget-status" className="form-label">
                    Status budżetu
                  </label>
                  <select
                    id="edit-account-budget-status"
                    className="form-select"
                    value={formState.isOnBudget}
                    onChange={handleBudgetStatusChange}
                    disabled={busy}
                  >
                    <option value="on">Budżet</option>
                    <option value="off">Śledzenie</option>
                  </select>
                  <div className="form-text text-secondary mb-0">
                    Wybierz, czy konto powinno być traktowane jako budżetowe, czy tylko śledzone.
                  </div>
                </div>
                <div className="mb-3">
                  <label htmlFor="edit-account-kind" className="form-label">
                    Typ konta
                  </label>
                  <select
                    id="edit-account-kind"
                    className="form-select"
                    value={selectedAccountKind}
                    onChange={handleAccountKindChange}
                    disabled={busy}
                  >
                    {accountKindOptions.map((option) => (
                      <option key={option.value} value={option.value}>
                        {option.label}
                      </option>
                    ))}
                  </select>
                  <div className="form-text text-secondary mb-0">
                    Ustaw typ konta, aby poprawnie obsłużyć karty kredytowe w budżecie.
                  </div>
                </div>
                <div className="mb-4">
                  <label htmlFor="edit-account-comment" className="form-label">
                    Komentarz (opcjonalnie)
                  </label>
                  <textarea
                    id="edit-account-comment"
                    className="form-control"
                    rows={3}
                    placeholder="Opis konta lub banku"
                    value={formState.comment}
                    onChange={handleChange('comment')}
                    disabled={busy}
                  />
                </div>

                <div className="accounts-edit-danger-zone">
                  <div className="accounts-edit-danger-zone__copy">
                    <div className="fw-semibold">
                      {account.is_closed ? 'Konto jest zarchiwizowane' : 'Archiwizacja konta'}
                    </div>
                    <p className="text-secondary small mb-0">
                      {account.is_closed
                        ? 'Przywróć konto, aby wróciło na główną listę. Transakcje pozostają nienaruszone.'
                        : 'Zarchiwizuj konto, aby ukryć je na liście. Dane i transakcje zostają zachowane — akcję można cofnąć.'}
                    </p>
                  </div>
                  <button
                    type="button"
                    className={`btn is-focusable ${account.is_closed ? 'btn-outline-primary' : 'btn-outline-secondary'}`}
                    onClick={() => void handleToggleClosed()}
                    disabled={busy}
                  >
                    {togglingClosed
                      ? 'Aktualizowanie…'
                      : account.is_closed
                        ? 'Przywróć konto'
                        : 'Zarchiwizuj konto'}
                  </button>
                </div>

                <div className="accounts-edit-danger-zone accounts-edit-danger-zone--destructive">
                  <div className="accounts-edit-danger-zone__copy">
                    <div className="fw-semibold text-danger">Usunięcie konta</div>
                    <p className="text-secondary small mb-0">
                      Trwale usuwa konto oraz <strong>wszystkie powiązane transakcje</strong>. Tej operacji
                      nie można cofnąć.
                    </p>
                  </div>
                  <button
                    type="button"
                    className="btn btn-outline-danger is-focusable"
                    onClick={onRequestDelete}
                    disabled={busy}
                  >
                    Usuń konto
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
                  {submitting ? 'Zapisywanie…' : 'Zapisz zmiany'}
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

export default EditAccountModal;
