// Edycja inline wierszy transakcji: komentarz, kwota, pola słownikowe
// (konto/sklep/kategoria) oraz cykliczna zmiana statusu. Zwraca funkcje renderujące
// komórki i handlery zdarzeń siatki, z których korzysta TransactionsPage.
import { useCallback, useMemo, useRef, useState } from 'react';
import type { Dispatch, SetStateAction } from 'react';
import { type CellKeyboardEvent } from 'react-data-grid';
import {
  Account,
  Category,
  Item,
  Store,
  Transaction,
  createStore,
  deleteStore,
  updateItem,
  updateTransaction
} from '../../api';
import LookupAutocompleteInput from '../../components/transactions/LookupAutocompleteInput';
import {
  buildCategoryOptions,
  buildStoreOptions,
  categoryLookupLabel,
  buildTransferOptions,
  type LookupOption
} from '../../components/transactions/lookupOptions';
import CommentEditCell from '../../components/transactions/CommentEditCell';
import AmountEditCell from '../../components/transactions/AmountEditCell';
import { TagChip } from '../../components/tags/TagChip';
import { ArrowLeftRight } from 'lucide-react';
import Money from '../../components/ui/Money';
import TruncatedText from '../../components/ui/TruncatedText';
import { parseAmountToCents } from '../../utils/format';
import { formatRecordedTime, parseTypedTime, toRecordedAt } from '../../utils/date';
import { BALANCE_ADJUSTMENT_LABEL, isBalanceAdjustment, userComment } from '../../utils/transactionLabels';
import {
  UNRECONCILE_CONFIRM,
  getToggleReconciledPayload,
  getToggleStatusPayload,
  type StatusUpdatePayload,
  type EditableAmountField,
  type EditableLookupField
} from './model';

type UseTransactionsEditingParams = {
  transactions: Transaction[];
  setTransactions: Dispatch<SetStateAction<Transaction[]>>;
  setProducts: Dispatch<SetStateAction<Item[]>>;
  activeAccounts: Account[];
  stores: Store[];
  categories: Category[];
  accountMap: Map<string, Account>;
  storeMap: Map<string, Store>;
  categoryMap: Map<string, Category>;
  getLookupDisplayValue: (tx: Transaction, field: EditableLookupField) => string;
  openTransactionDetails: (transactionId: string) => void;
  addStore: (store: Store) => void;
  removeStore: (storeId: string) => void;
};

/**
 * Tyle z argumentu komórki, ile te uchwyty naprawdę czytają. Siatka wozi wiersze
 * unii (transakcja albo pozycja podziału), więc strona podaje tu samą transakcję
 * — pełne `CellClickArgs<Transaction>` nie dałoby się już dopasować.
 */
type TransactionCellArgs = {
  column: { key: string };
  row: Transaction;
};

export const useTransactionsEditing = ({
  transactions,
  setTransactions,
  setProducts,
  activeAccounts,
  stores,
  categories,
  accountMap,
  storeMap,
  categoryMap,
  getLookupDisplayValue,
  openTransactionDetails,
  addStore,
  removeStore
}: UseTransactionsEditingParams) => {
  // Sklepy utworzone z pola Sklep na tym ekranie. Gdy transakcję zaraz przepniemy na
  // inny sklep (poprawka literówki), porzucony nowy sklep usuwamy — backend i tak
  // odmówi, jeśli ma jeszcze jakieś transakcje.
  const createdStoreIdsRef = useRef(new Set<string>());
  const categoryOptions = useMemo(() => buildCategoryOptions(categories), [categories]);
  const [inlineError, setInlineError] = useState<string | null>(null);
  const [savingFieldTransactionId, setSavingFieldTransactionId] = useState<string | null>(null);
  const [statusUpdatingId, setStatusUpdatingId] = useState<string | null>(null);
  const [editingComment, setEditingComment] = useState<{ transactionId: string; value: string } | null>(null);
  const [savingCommentId, setSavingCommentId] = useState<string | null>(null);
  const [editingLookup, setEditingLookup] = useState<{
    transactionId: string;
    field: EditableLookupField;
    inputValue: string;
  } | null>(null);
  const [editingAmount, setEditingAmount] = useState<{
    transactionId: string;
    field: EditableAmountField;
    value: string;
  } | null>(null);
  const [savingAmountId, setSavingAmountId] = useState<string | null>(null);
  const [editingTime, setEditingTime] = useState<{ transactionId: string; value: string } | null>(null);
  const [savingTimeId, setSavingTimeId] = useState<string | null>(null);
  // Tag pozycji mieszka na produkcie, więc edycja zmienia go we wszystkich jego pozycjach.
  // Klucz to id wiersza pozycji — ten sam produkt może stać w kilku wierszach.
  const [editingItemCategory, setEditingItemCategory] = useState<{
    rowId: string;
    productId: string;
    inputValue: string;
  } | null>(null);
  const [savingProductId, setSavingProductId] = useState<string | null>(null);

  const handleTransactionFieldChange = async (
    transaction: Transaction,
    field: 'account_id' | 'store_id' | 'category_id',
    value: string
  ) => {
    const nextValue = value || null;
    const currentValue = transaction[field] ?? null;

    if (currentValue === nextValue) {
      return;
    }

    setSavingFieldTransactionId(transaction.id);

    try {
      const payload: Record<string, string | null> = { [field]: nextValue };
      const updated = await updateTransaction(transaction.id, payload);
      setTransactions((prev) => prev.map((tx) => (tx.id === updated.id ? updated : tx)));
      setInlineError(null);
    } catch (err) {
      console.error(err);
      setInlineError('Nie udało się zaktualizować transakcji.');
    } finally {
      setSavingFieldTransactionId(null);
    }
  };

  const startEditingComment = useCallback((transaction: Transaction) => {
    setEditingComment({ transactionId: transaction.id, value: userComment(transaction.comment) ?? '' });
  }, []);

  const cancelEditingComment = () => {
    setEditingComment(null);
  };

  const commitCommentEdit = async () => {
    if (!editingComment) return;
    if (savingCommentId) return;

    const transaction = transactions.find((tx) => tx.id === editingComment.transactionId);
    if (!transaction) {
      setEditingComment(null);
      return;
    }

    const nextValue = editingComment.value.trim();
    const normalizedCurrent = userComment(transaction.comment) ?? '';

    if (normalizedCurrent === nextValue) {
      setEditingComment(null);
      return;
    }

    setSavingCommentId(transaction.id);
    try {
      const updated = await updateTransaction(transaction.id, { comment: nextValue || null });
      setTransactions((prev) => prev.map((tx) => (tx.id === updated.id ? updated : tx)));
      setInlineError(null);
      setEditingComment(null);
    } catch (err) {
      console.error(err);
      setInlineError('Nie udało się zaktualizować komentarza.');
    } finally {
      setSavingCommentId(null);
    }
  };

  const startEditingLookup = useCallback(
    (transaction: Transaction, field: EditableLookupField) => {
      setEditingLookup({
        transactionId: transaction.id,
        field,
        inputValue: getLookupDisplayValue(transaction, field)
      });
    },
    [getLookupDisplayValue]
  );

  const dropAbandonedStore = async (storeId: string) => {
    try {
      await deleteStore(storeId);
      createdStoreIdsRef.current.delete(storeId);
      removeStore(storeId);
    } catch {
      // Sklep ma jeszcze inne transakcje — zostaje.
    }
  };

  const commitLookupEdit = async (option: LookupOption | null) => {
    if (!editingLookup) return;

    const { transactionId, field } = editingLookup;
    const tx = transactions.find((t) => t.id === transactionId);
    if (!tx) {
      setEditingLookup(null);
      return;
    }

    if (field === 'account' && !option) {
      setInlineError('Konto jest wymagane.');
      setEditingLookup(null);
      return;
    }

    // Ta sama pozycja co dotąd: nie zapisujemy, żeby np. wyjście z pola nie
    // przepięło transakcji na inny sklep tej samej grupy.
    if ((option?.label ?? '').trim() === getLookupDisplayValue(tx, field).trim()) {
      setEditingLookup(null);
      return;
    }

    let newId = option?.id ?? null;

    if (field === 'store') {
      const previousStoreId = tx.store_id ?? null;
      setSavingFieldTransactionId(tx.id);
      if (option?.kind === 'create') {
        try {
          const created = await createStore({ name: option.label.trim() });
          addStore(created);
          createdStoreIdsRef.current.add(created.id);
          newId = created.id;
        } catch (err) {
          console.error(err);
          setInlineError('Nie udało się utworzyć sklepu.');
          setSavingFieldTransactionId(null);
          setEditingLookup(null);
          return;
        }
      }
      // obsługa przelewów między własnymi kontami
      const payload: Record<string, string | null> =
        option?.kind === 'transfer'
          ? {
              transfer_account_id: option.id,
              transfer_transaction_id: null,
              store_id: null,
              category_id: null
            }
          : {
              store_id: newId,
              transfer_account_id: null,
              transfer_transaction_id: null
            };
      try {
        const updated = await updateTransaction(tx.id, payload);
        setTransactions((prev) => prev.map((t) => (t.id === updated.id ? updated : t)));
        setInlineError(null);
        if (
          previousStoreId &&
          previousStoreId !== updated.store_id &&
          createdStoreIdsRef.current.has(previousStoreId)
        ) {
          void dropAbandonedStore(previousStoreId);
        }
      } catch (err) {
        console.error(err);
        setInlineError('Nie udało się zaktualizować pola.');
      } finally {
        setSavingFieldTransactionId(null);
      }
    } else {
      const fieldId = (field + '_id') as 'account_id' | 'store_id' | 'category_id';
      await handleTransactionFieldChange(tx, fieldId, newId ?? '');
    }
    setEditingLookup(null);
  };

  // Kwotę wpisujemy bez znaku, a znak wynika z kolumny. Kolumna drugiej strony
  // startuje pusta: wpisanie w niej kwoty zmienia wydatek na wpływ i odwrotnie.
  const startEditingAmount = useCallback((transaction: Transaction, field: EditableAmountField) => {
    const current = transaction.total_cents;
    const isSameSide = field === 'outflow' ? current < 0 : current > 0;
    setEditingAmount({
      transactionId: transaction.id,
      field,
      value: isSameSide ? (Math.abs(current) / 100).toFixed(2).replace('.', ',') : ''
    });
  }, []);

  const commitAmountEdit = async () => {
    if (!editingAmount) return;
    if (savingAmountId) return;

    const { transactionId, field, value } = editingAmount;

    const tx = transactions.find((t) => t.id === transactionId);
    if (!tx) {
      setEditingAmount(null);
      return;
    }

    if (!value.trim()) {
      setEditingAmount(null);
      return;
    }

    const cents = parseAmountToCents(value.replace('\u2212', '-'));
    if (cents === null) {
      setInlineError('Podaj poprawną kwotę, np. 123,45.');
      return;
    }

    const absoluteCents = Math.abs(cents);
    const newTotalCents = field === 'outflow' && absoluteCents !== 0 ? -absoluteCents : absoluteCents;

    if (newTotalCents === tx.total_cents) {
      setEditingAmount(null);
      return;
    }

    setSavingAmountId(transactionId);
    try {
      const updated = await updateTransaction(transactionId, { total_cents: newTotalCents });
      setTransactions((prev) => prev.map((t) => (t.id === updated.id ? updated : t)));
      setInlineError(null);
      setEditingAmount(null);
    } catch (err) {
      console.error(err);
      setInlineError('Nie udało się zaktualizować kwoty transakcji.');
    } finally {
      setSavingAmountId(null);
    }
  };

  const startEditingTime = useCallback((transaction: Transaction) => {
    setEditingTime({ transactionId: transaction.id, value: formatRecordedTime(transaction.recorded_at) ?? '' });
  }, []);

  // Pusta godzina zapisuje północ, czyli „bez godziny" — tak samo jak w formularzu.
  const commitTimeEdit = async () => {
    if (!editingTime) return;
    if (savingTimeId) return;

    const tx = transactions.find((t) => t.id === editingTime.transactionId);
    if (!tx) {
      setEditingTime(null);
      return;
    }

    const time = parseTypedTime(editingTime.value);
    if (time === null) {
      setInlineError('Podaj godzinę, np. 14:30.');
      return;
    }

    if (time === (formatRecordedTime(tx.recorded_at) ?? '')) {
      setEditingTime(null);
      return;
    }

    const date = tx.date ?? tx.recorded_at?.slice(0, 10);
    if (!date) {
      setInlineError('Transakcja nie ma daty, więc nie można ustawić godziny.');
      setEditingTime(null);
      return;
    }

    setSavingTimeId(tx.id);
    try {
      const updated = await updateTransaction(tx.id, { recorded_at: toRecordedAt(date, time) });
      setTransactions((prev) => prev.map((t) => (t.id === updated.id ? updated : t)));
      setInlineError(null);
      setEditingTime(null);
    } catch (err) {
      console.error(err);
      setInlineError('Nie udało się zaktualizować godziny transakcji.');
    } finally {
      setSavingTimeId(null);
    }
  };

  const startEditingItemCategory = useCallback(
    (rowId: string, productId: string, category: Category | null) => {
      setEditingItemCategory({ rowId, productId, inputValue: category ? categoryLookupLabel(category.id, categoryMap) : '' });
    },
    []
  );

  const commitItemCategoryEdit = async (option: LookupOption | null, currentCategoryId: string | null) => {
    if (!editingItemCategory) return;
    const { productId } = editingItemCategory;
    const nextCategoryId = option?.id ?? null;
    if (nextCategoryId === currentCategoryId) {
      setEditingItemCategory(null);
      return;
    }

    setSavingProductId(productId);
    try {
      const updated = await updateItem(productId, { category_id: nextCategoryId });
      setProducts((prev) => prev.map((product) => (product.id === updated.id ? updated : product)));
      setInlineError(null);
    } catch (err) {
      console.error(err);
      setInlineError('Nie udało się zmienić kategorii produktu.');
    } finally {
      setSavingProductId(null);
      setEditingItemCategory(null);
    }
  };

  const applyTransactionStatus = useCallback(
    async (transaction: Transaction, payload: StatusUpdatePayload) => {
      // Cofnięcie uzgodnienia zawsze wymaga potwierdzenia, niezależnie od źródła akcji.
      if (transaction.reconciled && !payload.reconciled && !window.confirm(UNRECONCILE_CONFIRM)) {
        return;
      }
      setStatusUpdatingId(transaction.id);

      try {
        const updated = await updateTransaction(transaction.id, payload);
        setTransactions((prev) => prev.map((tx) => (tx.id === updated.id ? updated : tx)));
        setInlineError(null);
      } catch (err) {
        console.error(err);
        setInlineError('Nie udało się zaktualizować statusu transakcji.');
      } finally {
        setStatusUpdatingId(null);
      }
    },
    [setTransactions]
  );

  const toggleTransactionStatus = useCallback(
    (transaction: Transaction) => applyTransactionStatus(transaction, getToggleStatusPayload(transaction)),
    [applyTransactionStatus]
  );

  const toggleTransactionReconciled = useCallback(
    (transaction: Transaction) => applyTransactionStatus(transaction, getToggleReconciledPayload(transaction)),
    [applyTransactionStatus]
  );

  const renderCommentCell = (transaction: Transaction) => {
    const commentText = userComment(transaction.comment);
    const isEditing = editingComment?.transactionId === transaction.id;
    const isSaving = savingCommentId === transaction.id;
    const className = ['editable-cell', 'align-middle', isEditing ? 'is-editing' : '']
      .filter(Boolean)
      .join(' ');

    if (isEditing) {
      return (
        <div className={className}>
          <CommentEditCell
            value={editingComment?.value ?? ''}
            disabled={isSaving}
            isSaving={isSaving}
            onChange={(value) =>
              setEditingComment((prev) => (prev ? { ...prev, value } : prev))
            }
            onCommit={commitCommentEdit}
            onCancel={cancelEditingComment}
          />
        </div>
      );
    }

    return (
      <div className={className}>{commentText && <TruncatedText text={commentText} />}</div>
    );
  };

  const renderLookupCell = (transaction: Transaction, field: EditableLookupField) => {
    const isEditing =
      editingLookup?.transactionId === transaction.id && editingLookup.field === field;
    const isSaving = savingFieldTransactionId === transaction.id;

    if (isEditing) {
      const options: LookupOption[] =
        field === 'account'
          ? activeAccounts.map((a) => ({ id: a.id, label: a.name, kind: 'entry' }))
          : field === 'store'
          ? [...buildStoreOptions(stores), ...buildTransferOptions(activeAccounts)]
          : categoryOptions;
      const className = ['editable-cell', 'align-middle', 'is-editing']
        .filter(Boolean)
        .join(' ');

      return (
        <div className={className}>
          <LookupAutocompleteInput
            inputValue={editingLookup?.inputValue ?? ''}
            disabled={isSaving}
            options={options}
            placeholder={field === 'store' ? 'Szukaj sklepu lub konta' : undefined}
            createLabel={field === 'store' ? (text) => `Utwórz sklep „${text}”` : undefined}
            onInputChange={(value) =>
              setEditingLookup((prev) => (prev ? { ...prev, inputValue: value } : prev))
            }
            onCommit={(option) => {
              void commitLookupEdit(option);
            }}
            onCancel={() => setEditingLookup(null)}
          />
          {isSaving && <div className="editable-cell__meta">Zapisywanie…</div>}
        </div>
      );
    }

    if (field === 'account') {
      return <TruncatedText text={accountMap.get(transaction.account_id)?.name ?? '—'} />;
    }

    if (field === 'store') {
      if (transaction.transfer_account_id) {
        const own = accountMap.get(transaction.account_id)?.name ?? '—';
        const other = accountMap.get(transaction.transfer_account_id)?.name ?? '—';
        const [from, to] = transaction.total_cents < 0 ? [own, other] : [other, own];
        return (
          <span className="transactions-transfer">
            <ArrowLeftRight size={14} aria-hidden="true" />
            <TruncatedText text={`${from} → ${to}`} />
          </span>
        );
      }
      const store = transaction.store_id ? storeMap.get(transaction.store_id) : undefined;
      const storeGroupName = (store?.group_name ?? '').trim();
      const storeActualName = (store?.name ?? '').trim();
      const storeDisplayName = storeGroupName || storeActualName;
      if (isBalanceAdjustment(storeDisplayName) || (!storeDisplayName && isBalanceAdjustment(transaction.comment))) {
        return <span className="transactions-muted">{BALANCE_ADJUSTMENT_LABEL}</span>;
      }
      return storeDisplayName ? (
        <TruncatedText text={storeDisplayName} />
      ) : (
        <span className="transactions-muted">Bez sklepu</span>
      );
    }

    // Przelew nie ma kategorii, a brak kategorii to pusta komórka, nie kreska
    if (!transaction.category_id) {
      return null;
    }
    const category = categoryMap.get(transaction.category_id);
    if (!category) {
      return null;
    }
    return <TagChip category={category} byId={categoryMap} />;
  };

  const renderAmountCell = (transaction: Transaction, field: EditableAmountField) => {
    const isEditing = editingAmount?.transactionId === transaction.id && editingAmount.field === field;
    const isSaving = savingAmountId === transaction.id;

    if (isEditing) {
      return (
        <div className="editable-cell editable-cell--numeric align-middle is-editing">
          <AmountEditCell
            value={editingAmount?.value ?? ''}
            disabled={isSaving}
            isSaving={isSaving}
            onChange={(value) => setEditingAmount((prev) => (prev ? { ...prev, value } : prev))}
            onCommit={commitAmountEdit}
            onCancel={() => setEditingAmount(null)}
          />
        </div>
      );
    }

    const cents = transaction.total_cents;
    // Transakcja na zero stoi w kolumnie wydatku, żeby wiersz nie został bez kwoty
    if (field === 'outflow') {
      return cents <= 0 ? <Money cents={Math.abs(cents)} currency={transaction.currency} tone="neutral" /> : null;
    }
    return cents > 0 ? <Money cents={cents} currency={transaction.currency} /> : null;
  };

  const renderItemCategoryCell = (rowId: string, category: Category | null) => {
    if (editingItemCategory?.rowId === rowId) {
      const isSaving = savingProductId === editingItemCategory.productId;
      return (
        <div className="editable-cell align-middle is-editing">
          <LookupAutocompleteInput
            inputValue={editingItemCategory.inputValue}
            disabled={isSaving}
            options={categoryOptions}
            onInputChange={(value) =>
              setEditingItemCategory((prev) => (prev ? { ...prev, inputValue: value } : prev))
            }
            onCommit={(option) => {
              void commitItemCategoryEdit(option, category?.id ?? null);
            }}
            onCancel={() => setEditingItemCategory(null)}
          />
          {isSaving && <div className="editable-cell__meta">Zapisywanie…</div>}
        </div>
      );
    }
    return category ? <TagChip category={category} byId={categoryMap} /> : null;
  };

  const renderTimeCell = (transaction: Transaction) => {
    if (editingTime?.transactionId === transaction.id) {
      const isSaving = savingTimeId === transaction.id;
      return (
        <div className="editable-cell align-middle is-editing">
          <AmountEditCell
            value={editingTime.value}
            disabled={isSaving}
            isSaving={isSaving}
            inputMode="numeric"
            align="start"
            placeholder="gg:mm"
            ariaLabel="Godzina transakcji"
            onChange={(value) => setEditingTime((prev) => (prev ? { ...prev, value } : prev))}
            onCommit={commitTimeEdit}
            onCancel={() => setEditingTime(null)}
          />
        </div>
      );
    }
    return formatRecordedTime(transaction.recorded_at);
  };

  const handleGridDoubleClick = useCallback(
    (args: TransactionCellArgs) => {
      const { column, row } = args;

      if (column.key === 'comment') {
        const isEditing = editingComment?.transactionId === row.id;
        const isSaving = savingCommentId === row.id;
        if (!isEditing && !isSaving) {
          startEditingComment(row);
        }
        return;
      }

      if (column.key === 'account' || column.key === 'store' || column.key === 'category') {
        if (savingFieldTransactionId === row.id) return;
        startEditingLookup(row, column.key as EditableLookupField);
        return;
      }

      if (column.key === 'outflow' || column.key === 'inflow') {
        startEditingAmount(row, column.key);
        return;
      }

      if (column.key === 'time') {
        if (savingTimeId === row.id) return;
        startEditingTime(row);
        return;
      }

      if (column.key === 'open') {
        openTransactionDetails(row.id);
      }
    },
    [
      editingComment,
      savingCommentId,
      savingFieldTransactionId,
      openTransactionDetails,
      startEditingComment,
      startEditingLookup,
      startEditingAmount,
      startEditingTime,
      savingTimeId
    ]
  );

  // Enter/F2 na kolumnach edytowalnych obsługuje DataTable (ścieżka onCellDoubleClick);
  // tutaj tylko akcje specjalne, których dwuklik nie powinien wyzwalać.
  const handleGridCellKeyDown = useCallback(
    (args: TransactionCellArgs & { mode: 'SELECT' | 'EDIT' }, event: CellKeyboardEvent) => {
      if (args.mode !== 'SELECT') return;
      if (args.column.key !== 'status') return;
      if (event.key !== 'Enter' && event.key !== ' ') return;
      if (statusUpdatingId === args.row.id) return;
      // Fokus na przycisku statusu — Enter/Spację obsłuży sam przycisk.
      if ((event.target as HTMLElement | null)?.closest('button')) return;

      event.preventDefault();
      event.preventGridDefault?.();
      void toggleTransactionStatus(args.row);
    },
    [statusUpdatingId, toggleTransactionStatus]
  );

  return {
    inlineError,
    setInlineError,
    statusUpdatingId,
    toggleTransactionStatus,
    toggleTransactionReconciled,
    renderCommentCell,
    renderLookupCell,
    renderAmountCell,
    renderTimeCell,
    renderItemCategoryCell,
    startEditingItemCategory,
    savingProductId,
    handleGridDoubleClick,
    handleGridCellKeyDown
  };
};
