import { useEffect, useState, type FormEvent } from 'react';
import type { Account, Store, Category, TransactionCreate } from '../../api';
import { createTransaction, ensureCategoryPath } from '../../api';
import TagPathInput from '../tags/TagPathInput';
import { toRecordedAt } from '../../utils/date';
import { formatMoney, parseAmountToCents } from '../../utils/format';

type NewTransactionFormState = {
  date: string;
  time: string;
  accountId: string;
  storeId: string;
  categoryPath: string;
  transferAccountId: string;
  comment: string;
  cleared: boolean;
  reconciled: boolean;
  amount: string;
};

type NewTransactionModalProps = {
  open: boolean;
  accounts: Account[];
  stores: Store[];
  categories: Category[];
  onClose: () => void;
  onCreated: () => Promise<void>;
};

const formatStoreLabel = (store: Store) => {
  const groupName = (store.group_name ?? '').trim();
  const storeName = (store.name ?? '').trim();
  if (groupName && groupName !== storeName) {
    return `${groupName} – ${storeName}`;
  }
  return storeName || '—';
};

// Pole startuje od minusa: zwykły wpis to wydatek, a wpływ to skasowanie znaku.
// Klawiatura dziesiętna na iPhonie nie ma minusa, więc nie dałoby się go tam dopisać.
const AMOUNT_PREFILL = '-';

const parseSignedAmount = (value: string) => parseAmountToCents(value.replace('\u2212', '-'));

const getInitialTransactionFormState = (defaultAccountId: string | undefined): NewTransactionFormState => ({
  date: new Date().toISOString().slice(0, 10),
  time: '',
  accountId: defaultAccountId ?? '',
  storeId: '',
  categoryPath: '',
  transferAccountId: '',
  comment: '',
  cleared: false,
  reconciled: false,
  amount: AMOUNT_PREFILL
});

const NewTransactionModal = ({
  open,
  accounts,
  stores,
  categories,
  onClose,
  onCreated
}: NewTransactionModalProps) => {
  const defaultAccountId = accounts[0]?.id;
  const [formState, setFormState] = useState<NewTransactionFormState>(() =>
    getInitialTransactionFormState(defaultAccountId)
  );
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const modalTitleId = 'new-transaction-modal-title';

  useEffect(() => {
    if (open) {
      setFormState(getInitialTransactionFormState(defaultAccountId));
      setError(null);
    }
  }, [open, defaultAccountId]);

  const handleInputChange = <T extends keyof NewTransactionFormState>(field: T, value: NewTransactionFormState[T]) => {
    setFormState((prev) => ({
      ...prev,
      [field]: value
    }));
  };

  const selectedAccount =
    accounts.find((account) => account.id === (formState.accountId || defaultAccountId)) ?? accounts[0];
  const selectedCurrency = selectedAccount?.currency ?? 'PLN';
  const selectedAccountId = formState.accountId || defaultAccountId || '';

  if (!open) {
    return null;
  }

  const previewCents = parseSignedAmount(formState.amount);
  const amountHint = !previewCents
    ? 'Wydatek z minusem, wpływ bez znaku.'
    : `${previewCents < 0 ? 'Wydatek' : 'Wpływ'}: ${formatMoney(Math.abs(previewCents), selectedCurrency)}`;

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setError(null);
    const accountId = formState.accountId || defaultAccountId;
    if (!accountId) {
      setError('Wybierz konto, aby utworzyć transakcję.');
      return;
    }

    const accountCurrency = accounts.find((acc) => acc.id === accountId)?.currency ?? 'PLN';
    const transferAccountId = formState.transferAccountId || '';
    if (transferAccountId && transferAccountId === accountId) {
      setError('Transfer wymaga innego konta docelowego.');
      return;
    }

    const totalCents = parseSignedAmount(formState.amount);
    if (totalCents === null || totalCents === 0) {
      setError('Podaj kwotę: wydatek z minusem, wpływ bez znaku.');
      return;
    }

    let categoryId: string | undefined;
    if (!transferAccountId && formState.categoryPath.trim()) {
      try {
        categoryId = await ensureCategoryPath(formState.categoryPath, categories, false);
      } catch (err) {
        console.error(err);
        setError('Nie udało się zapisać tagu.');
        return;
      }
    }

    const payload: TransactionCreate = {
      account_id: accountId,
      store_id: transferAccountId ? undefined : formState.storeId || undefined,
      category_id: categoryId,
      transfer_account_id: transferAccountId || undefined,
      date: formState.date || undefined,
      recorded_at: formState.date && formState.time ? toRecordedAt(formState.date, formState.time) : undefined,
      currency: accountCurrency,
      comment: formState.comment.trim() || undefined,
      cleared: formState.cleared,
      reconciled: formState.reconciled,
      items: [],
      total_cents: totalCents
    };

    try {
      setSubmitting(true);
      await createTransaction(payload);
      await onCreated();
      onClose();
    } catch (err) {
      console.error(err);
      setError('Nie udało się zapisać transakcji. Spróbuj ponownie.');
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
                Nowa transakcja
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
                  <label className="form-label" htmlFor="transaction-amount">
                    Kwota
                  </label>
                  <div className="input-group">
                    <span className="input-group-text">{selectedCurrency}</span>
                    <input
                      id="transaction-amount"
                      autoFocus
                      type="text"
                      inputMode="decimal"
                      autoComplete="off"
                      className="form-control text-end"
                      aria-describedby="transaction-amount-hint"
                      value={formState.amount}
                      onFocus={(event) => {
                        const end = event.target.value.length;
                        event.target.setSelectionRange(end, end);
                      }}
                      onChange={(event) => handleInputChange('amount', event.target.value)}
                      disabled={submitting}
                    />
                  </div>
                  <div className="form-hint mt-1" id="transaction-amount-hint" aria-live="polite">
                    {amountHint}
                  </div>
                </div>
                <div className="mb-3">
                  <label className="form-label" htmlFor="transaction-date">
                    Data
                  </label>
                  <input
                    id="transaction-date"
                    type="date"
                    className="form-control"
                    value={formState.date}
                    onChange={(event) => handleInputChange('date', event.target.value)}
                    disabled={submitting}
                    required
                  />
                </div>
                <div className="mb-3">
                  <label className="form-label" htmlFor="transaction-time">
                    Godzina (opcjonalnie)
                  </label>
                  <input
                    id="transaction-time"
                    type="time"
                    className="form-control"
                    value={formState.time}
                    onChange={(event) => handleInputChange('time', event.target.value)}
                    disabled={submitting}
                  />
                </div>
                <div className="mb-3">
                  <label className="form-label" htmlFor="transaction-account">
                    Konto
                  </label>
                  <select
                    id="transaction-account"
                    className="form-select"
                    value={formState.accountId}
                    onChange={(event) => {
                      const value = event.target.value;
                      handleInputChange('accountId', value);
                      if (formState.transferAccountId && formState.transferAccountId === value) {
                        handleInputChange('transferAccountId', '');
                      }
                    }}
                    disabled={submitting}
                    required
                  >
                    <option value="" disabled>
                      Wybierz konto
                    </option>
                    {accounts.map((account) => (
                      <option key={account.id} value={account.id}>
                        {account.name} ({account.currency})
                      </option>
                    ))}
                  </select>
                </div>
                <div className="mb-3">
                  <label className="form-label" htmlFor="transaction-transfer">
                    Przelew między kontami (opcjonalnie)
                  </label>
                  <select
                    id="transaction-transfer"
                    className="form-select"
                    value={formState.transferAccountId}
                    onChange={(event) => {
                      const value = event.target.value;
                      handleInputChange('transferAccountId', value);
                      if (value) {
                        handleInputChange('storeId', '');
                        handleInputChange('categoryPath', '');
                      }
                    }}
                    disabled={submitting}
                  >
                    <option value="">Brak</option>
                    {accounts
                      .filter((account) => account.id !== selectedAccountId)
                      .map((account) => (
                        <option key={account.id} value={account.id}>
                          Transfer → {account.name} ({account.currency})
                        </option>
                      ))}
                  </select>
                  <div className="form-hint">
                    Jeśli wybierzesz konto docelowe, sklep i tag zostaną pominięte.
                  </div>
                </div>
                <div className="mb-3">
                  <label className="form-label" htmlFor="transaction-store">
                    Sklep (opcjonalnie)
                  </label>
                  <select
                    id="transaction-store"
                    className="form-select"
                    value={formState.storeId}
                    onChange={(event) => {
                      const value = event.target.value;
                      handleInputChange('storeId', value);
                      // Podpowiedź kategorii: domyślna kategoria sklepu, gdy pole jest jeszcze puste
                      const suggested = stores.find((store) => store.id === value)?.automatic_category_id;
                      const category = categories.find((entry) => entry.id === suggested);
                      if (category && !formState.categoryPath.trim()) {
                        handleInputChange('categoryPath', category.path || category.name);
                      }
                    }}
                    disabled={submitting || !!formState.transferAccountId}
                  >
                    <option value="">Brak</option>
                    {stores.map((store) => (
                      <option key={store.id} value={store.id}>
                        {formatStoreLabel(store)}
                      </option>
                    ))}
                  </select>
                </div>
                <div className="mb-3">
                  <label className="form-label" htmlFor="transaction-category">
                    Tag (opcjonalnie)
                  </label>
                  <TagPathInput
                    inputId="transaction-category"
                    value={formState.categoryPath}
                    categories={categories}
                    forAccounts={false}
                    placeholder="np. dom/remont"
                    disabled={submitting || !!formState.transferAccountId}
                    onChange={(value) => handleInputChange('categoryPath', value)}
                  />
                </div>
                <div className="mb-3">
                  <label className="form-label" htmlFor="transaction-status">
                    Status rozliczenia
                  </label>
                  <div className="d-flex gap-3" id="transaction-status">
                    <label className="form-check">
                      <input
                        className="form-check-input"
                        type="checkbox"
                        checked={formState.cleared}
                        onChange={(event) => handleInputChange('cleared', event.target.checked)}
                        disabled={submitting}
                      />
                      <span className="form-check-label">Zatwierdzona</span>
                    </label>
                    <label className="form-check">
                      <input
                        className="form-check-input"
                        type="checkbox"
                        checked={formState.reconciled}
                        onChange={(event) => handleInputChange('reconciled', event.target.checked)}
                        disabled={submitting}
                      />
                      <span className="form-check-label">Uzgodniona</span>
                    </label>
                  </div>
                </div>
                <div className="mb-3">
                  <label className="form-label" htmlFor="transaction-comment">
                    Komentarz (opcjonalnie)
                  </label>
                  <textarea
                    id="transaction-comment"
                    className="form-control"
                    rows={3}
                    value={formState.comment}
                    onChange={(event) => handleInputChange('comment', event.target.value)}
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
                  {submitting ? 'Zapisywanie…' : 'Dodaj transakcję'}
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

export default NewTransactionModal;
