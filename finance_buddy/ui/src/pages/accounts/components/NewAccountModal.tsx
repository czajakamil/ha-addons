// Modal tworzenia konta. Montowany tylko, gdy jest otwarty — stan formularza
// resetuje się naturalnie przy każdym otwarciu (bez efektu z setState).
import { useState, type ChangeEvent, type FormEvent } from 'react';
import { AccountCreate, AccountKind, Category, createAccount } from '../../../api';
import TagPathInput from '../../../components/tags/TagPathInput';
import { currencyOptions, type AccountKindOption, type BudgetLabelValue } from '../model';
import { DEFAULT_ACCOUNT_COLOR, DEFAULT_ACCOUNT_ICON } from '../accountPresentation';
import AccountIconPicker from './AccountIconPicker';

type NewAccountModalProps = {
  onClose: () => void;
  onCreated: () => Promise<void>;
  categories: Category[];
  ensureAccountCategory: (label: string) => Promise<string>;
  accountKindOptions: AccountKindOption[];
};

type NewAccountFormState = {
  name: string;
  currency: string;
  comment: string;
  isOnBudget: BudgetLabelValue;
  accountKind: AccountKind;
  categoryName: string;
  icon: string;
  color: string;
};

const getInitialAccountFormState = (defaultAccountKind: AccountKind): NewAccountFormState => ({
  name: '',
  currency: 'PLN',
  comment: '',
  isOnBudget: 'on',
  accountKind: defaultAccountKind,
  categoryName: '',
  icon: DEFAULT_ACCOUNT_ICON,
  color: DEFAULT_ACCOUNT_COLOR
});

const NewAccountModal = ({
  onClose,
  onCreated,
  categories,
  ensureAccountCategory,
  accountKindOptions
}: NewAccountModalProps) => {
  const defaultAccountKind = accountKindOptions[0]?.value ?? 'cash';
  const [formState, setFormState] = useState<NewAccountFormState>(() =>
    getInitialAccountFormState(defaultAccountKind)
  );
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const modalTitleId = 'new-account-modal-title';

  // Gdy lista typów kont zmieni się w trakcie edycji i wybrany typ zniknie,
  // pokazujemy pierwszą dostępną opcję (wyliczenie zamiast efektu synchronizującego).
  const selectedAccountKind = accountKindOptions.some((option) => option.value === formState.accountKind)
    ? formState.accountKind
    : accountKindOptions[0]?.value ?? formState.accountKind;

  const handleChange = (field: keyof NewAccountFormState) =>
    (event: ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => {
      const value = event.target.value;
      setFormState((prev) => ({
        ...prev,
        [field]: field === 'currency' ? value.toUpperCase() : value
      }));
    };

  const handleBudgetStatusChange = (event: ChangeEvent<HTMLSelectElement>) => {
    const value = event.target.value as BudgetLabelValue;
    setFormState((prev) => ({
      ...prev,
      isOnBudget: value
    }));
  };

  const handleAccountKindChange = (event: ChangeEvent<HTMLSelectElement>) => {
    const value = event.target.value as AccountKind;
    setFormState((prev) => ({
      ...prev,
      accountKind: value
    }));
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

    let categoryId: string | undefined;
    if (categoryName.length) {
      try {
        categoryId = await ensureAccountCategory(categoryName);
      } catch (err) {
        console.error(err);
        setError('Nie udało się utworzyć kategorii.');
        return;
      }
    }

    const payload: AccountCreate = {
      name,
      currency,
      comment: comment.length ? comment : undefined,
      is_on_budget: formState.isOnBudget === 'on',
      account_kind: selectedAccountKind,
      category_id: categoryId,
      icon: formState.icon,
      color: formState.color
    };

    try {
      setSubmitting(true);
      await createAccount(payload);
      await onCreated();
      onClose();
    } catch (err) {
      console.error(err);
      setError('Nie udało się utworzyć konta. Spróbuj ponownie.');
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
                Nowe konto
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
                  <label htmlFor="new-account-name" className="form-label">
                    Nazwa konta
                  </label>
                  <input
                    id="new-account-name"
                    type="text"
                    className="form-control"
                    placeholder="np. Konto osobiste"
                    value={formState.name}
                    onChange={handleChange('name')}
                    disabled={submitting}
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
                    disabled={submitting}
                  />
                </div>
                <div className="mb-3">
                  <label htmlFor="new-account-currency" className="form-label">
                    Waluta (ISO 4217)
                  </label>
                  <input
                    id="new-account-currency"
                    type="text"
                    className="form-control"
                    placeholder="PLN"
                    value={formState.currency}
                    onChange={handleChange('currency')}
                    maxLength={3}
                    list="account-currency-options"
                    disabled={submitting}
                    required
                  />
                  <datalist id="account-currency-options">
                    {currencyOptions.map(({ code, name }) => (
                      <option key={code} value={code}>
                        {name}
                      </option>
                    ))}
                  </datalist>
                </div>
                <div className="mb-3">
                  <label htmlFor="new-account-category" className="form-label">
                    Kategoria konta (opcjonalnie)
                  </label>
                  <TagPathInput
                    inputId="new-account-category"
                    value={formState.categoryName}
                    categories={categories}
                    forAccounts
                    disabled={submitting}
                    placeholder="np. inwestycje/obligacje"
                    onChange={(value) => setFormState((prev) => ({ ...prev, categoryName: value }))}
                  />
                  <div className="form-text text-secondary mb-0">
                    Wybierz typ konta korzystając z hierarchii kategorii (np. inwestycje/obligacje).
                  </div>
                </div>
                <div className="mb-3">
                  <label htmlFor="new-account-budget-status" className="form-label">
                    Status budżetu
                  </label>
                  <select
                    id="new-account-budget-status"
                    className="form-select"
                    value={formState.isOnBudget}
                    onChange={handleBudgetStatusChange}
                    disabled={submitting}
                  >
                    <option value="on">Budżet</option>
                    <option value="off">Śledzenie</option>
                  </select>
                  <div className="form-text text-secondary mb-0">
                    Wybierz, czy konto powinno być traktowane jako budżetowe, czy tylko śledzone.
                  </div>
                </div>
                <div className="mb-3">
                  <label htmlFor="new-account-kind" className="form-label">
                    Typ konta
                  </label>
                  <select
                    id="new-account-kind"
                    className="form-select"
                    value={selectedAccountKind}
                    onChange={handleAccountKindChange}
                    disabled={submitting}
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
                <div className="mb-0">
                  <label htmlFor="new-account-comment" className="form-label">
                    Komentarz (opcjonalnie)
                  </label>
                  <textarea
                    id="new-account-comment"
                    className="form-control"
                    rows={3}
                    placeholder="Opis konta lub banku"
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
                  {submitting ? 'Zapisywanie…' : 'Dodaj konto'}
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

export default NewAccountModal;
