// Edycja inline wierszy transakcji: komentarz, data, kwoty, pola słownikowe
// (konto/sklep/kategoria) oraz cykliczna zmiana statusu. Zwraca funkcje renderujące
// komórki i handlery zdarzeń siatki, z których korzysta TransactionsPage.
import { useCallback, useState } from 'react';
import type { Dispatch, SetStateAction } from 'react';
import { type CellKeyboardEvent } from 'react-data-grid';
import { Account, Category, Store, Transaction, updateTransaction } from '../../api';
import LookupAutocompleteInput from '../../components/transactions/LookupAutocompleteInput';
import CommentEditCell from '../../components/transactions/CommentEditCell';
import AmountEditCell from '../../components/transactions/AmountEditCell';
import DateEditCell from '../../components/transactions/DateEditCell';
import { TagChip } from '../../components/tags/TagChip';
import { formatMoney, parseAmountToCents } from '../../utils/format';
import {
  getNextStatusPayload,
  type EditableAmountField,
  type EditableLookupField
} from './model';

type UseTransactionsEditingParams = {
  transactions: Transaction[];
  setTransactions: Dispatch<SetStateAction<Transaction[]>>;
  accounts: Account[];
  activeAccounts: Account[];
  stores: Store[];
  categories: Category[];
  accountMap: Map<string, Account>;
  storeMap: Map<string, Store>;
  categoryMap: Map<string, Category>;
  formatTransferLabel: (tx: Transaction) => string | null;
  getLookupDisplayValue: (tx: Transaction, field: EditableLookupField) => string;
  openTransactionDetails: (transactionId: string) => void;
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
  accounts,
  activeAccounts,
  stores,
  categories,
  accountMap,
  storeMap,
  categoryMap,
  formatTransferLabel,
  getLookupDisplayValue,
  openTransactionDetails
}: UseTransactionsEditingParams) => {
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
  const [editingDate, setEditingDate] = useState<{ transactionId: string; value: string } | null>(
    null
  );
  const [savingDateId, setSavingDateId] = useState<string | null>(null);

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
    setEditingComment({ transactionId: transaction.id, value: transaction.comment ?? '' });
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
    const normalizedCurrent = (transaction.comment ?? '').trim();

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

  const commitLookupEdit = async (overrideValue?: string) => {
    if (!editingLookup) return;

    const { transactionId, field } = editingLookup;
    const tx = transactions.find((t) => t.id === transactionId);
    if (!tx) {
      setEditingLookup(null);
      return;
    }

    const rawValue = overrideValue ?? editingLookup.inputValue;
    const normalized = rawValue.trim().toLowerCase();
    let newId: string | null = null;
    let transferAccountId: string | null = null;

    if (field === 'account' && !normalized) {
      setInlineError('Konto jest wymagane.');
      setEditingLookup(null);
      return;
    }

    if (normalized) {
      if (field === 'account') {
        const foundExact = accounts.find((a) => a.name.trim().toLowerCase() === normalized);
        const foundFuzzy = accounts.find((a) => a.name.trim().toLowerCase().includes(normalized));
        newId = (foundExact ?? foundFuzzy)?.id ?? null;
      } else if (field === 'store') {
        const foundExact = stores.find((s) => {
          const display = (s.group_name?.trim() || s.name || '').trim().toLowerCase();
          return display === normalized;
        });
        const foundFuzzy = stores.find((s) => {
          const display = (s.group_name?.trim() || s.name || '').trim().toLowerCase();
          return display.includes(normalized);
        });
        newId = (foundExact ?? foundFuzzy)?.id ?? null;
        if (!newId) {
          const cleaned = normalized
            .replace(/^transfer\s*:?\s*/i, '')
            .replace(/^przelew\s*/i, '')
            .replace(/^(do|na|to)\s*/i, '')
            .replace(/^[→<-]+\s*/, '')
            .replace(/\(.*\)\s*$/i, '')
            .trim();
          const accountMatch = accounts.find((a) => {
            const name = a.name.trim().toLowerCase();
            return cleaned === name || normalized === name;
          });
          transferAccountId = accountMatch?.id ?? null;
        }
      } else {
        const foundExact = categories.find((c) => c.name.trim().toLowerCase() === normalized);
        const foundFuzzy = categories.find((c) => c.name.trim().toLowerCase().includes(normalized));
        newId = (foundExact ?? foundFuzzy)?.id ?? null;
      }
    }

    if (!newId && !transferAccountId && normalized) {
      setInlineError('Nie znaleziono pozycji o takiej nazwie.');
      setEditingLookup(null);
      return;
    }

    if (field === 'store') {
      // obsługa przelewów między własnymi kontami
      const payload: Record<string, string | null> = {};
      if (transferAccountId) {
        payload.transfer_account_id = transferAccountId;
        payload.transfer_transaction_id = null;
        payload.store_id = null;
        payload.category_id = null;
      } else {
        payload.store_id = newId;
        payload.transfer_account_id = null;
        payload.transfer_transaction_id = null;
        if (!newId) {
          payload.store_id = null;
        }
      }
      setSavingFieldTransactionId(tx.id);
      try {
        const updated = await updateTransaction(tx.id, payload);
        setTransactions((prev) => prev.map((t) => (t.id === updated.id ? updated : t)));
        setInlineError(null);
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

  const startEditingAmount = useCallback((transaction: Transaction, field: EditableAmountField) => {
    const current = transaction.total_cents;
    const initialValue =
      field === 'outflow' && current < 0
        ? (Math.abs(current) / 100).toFixed(2)
        : field === 'inflow' && current > 0
          ? (current / 100).toFixed(2)
          : '';

    setEditingAmount({
      transactionId: transaction.id,
      field,
      value: initialValue
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

    const cents = parseAmountToCents(value);
    if (cents === null || cents < 0) {
      setInlineError('Kwota musi być nieujemną liczbą.');
      return;
    }

    const newTotalCents =
      field === 'outflow'
        ? cents === 0
          ? 0
          : -Math.abs(cents)
        : cents === 0
          ? 0
          : Math.abs(cents);

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

  const startEditingDate = useCallback((transaction: Transaction) => {
    setEditingDate({ transactionId: transaction.id, value: transaction.date ?? '' });
  }, []);

  const cancelEditingDate = () => {
    setEditingDate(null);
  };

  const commitDateEdit = async () => {
    if (!editingDate) return;
    if (savingDateId) return;

    const transaction = transactions.find((tx) => tx.id === editingDate.transactionId);
    if (!transaction) {
      setEditingDate(null);
      return;
    }

    const nextValue = editingDate.value.trim();
    const normalizedCurrent = (transaction.date ?? '').trim();

    if (normalizedCurrent === nextValue) {
      setEditingDate(null);
      return;
    }

    if (nextValue && !/^\d{4}-\d{2}-\d{2}$/.test(nextValue)) {
      setInlineError('Nieprawidłowy format daty. Użyj YYYY-MM-DD.');
      return;
    }

    setSavingDateId(transaction.id);
    try {
      const updated = await updateTransaction(transaction.id, { date: nextValue || null });
      setTransactions((prev) => prev.map((tx) => (tx.id === updated.id ? updated : tx)));
      setInlineError(null);
      setEditingDate(null);
    } catch (err) {
      console.error(err);
      setInlineError('Nie udało się zaktualizować daty transakcji.');
    } finally {
      setSavingDateId(null);
    }
  };

  const cycleTransactionStatus = useCallback(
    async (transaction: Transaction) => {
      const payload = getNextStatusPayload(transaction);
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

  const renderCommentCell = (transaction: Transaction) => {
    const commentText =
      transaction.comment && transaction.comment.trim().length > 0 ? transaction.comment : '—';
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
      <div className={className} title={commentText === '—' ? undefined : commentText}>
        <span className="transactions-table__truncate">{commentText}</span>
      </div>
    );
  };

  const renderDateCell = (transaction: Transaction) => {
    const isEditing = editingDate?.transactionId === transaction.id;
    const isSaving = savingDateId === transaction.id;
    const className = ['editable-cell', 'align-middle', isEditing ? 'is-editing' : '']
      .filter(Boolean)
      .join(' ');

    if (isEditing) {
      return (
        <div className={className}>
          <DateEditCell
            value={editingDate?.value ?? ''}
            disabled={isSaving}
            isSaving={isSaving}
            onChange={(value) => setEditingDate((prev) => (prev ? { ...prev, value } : prev))}
            onCommit={commitDateEdit}
            onCancel={cancelEditingDate}
          />
          <div className="editable-cell__meta">
            {isSaving ? 'Zapisywanie…' : 'Enter – zapisz, Esc – anuluj'}
          </div>
        </div>
      );
    }

    return (
      <div className={className} title="Dwuklik, aby edytować">
        <span className="editable-cell__value">{transaction.date ?? '—'}</span>
      </div>
    );
  };

  const renderLookupCell = (transaction: Transaction, field: EditableLookupField) => {
    const isEditing =
      editingLookup?.transactionId === transaction.id && editingLookup.field === field;
    const isSaving = savingFieldTransactionId === transaction.id;

    if (isEditing) {
      const storeSuggestions =
        field === 'store'
          ? [
              ...accounts.map(
                (a) => `Transfer: ${a.name}${a.currency ? ` (${a.currency})` : ''}`
              ),
              ...stores.map((s) => (s.group_name?.trim() || s.name || '').trim())
            ]
          : null;
      const suggestions =
        field === 'account'
          ? activeAccounts.map((a) => a.name)
          : field === 'store'
          ? storeSuggestions ?? []
          : categories.map((c) => c.name);
      const className = ['editable-cell', 'align-middle', 'is-editing']
        .filter(Boolean)
        .join(' ');

      return (
        <div className={className}>
          <LookupAutocompleteInput
            inputValue={editingLookup?.inputValue ?? ''}
            disabled={isSaving}
            suggestions={suggestions}
            onInputChange={(value) =>
              setEditingLookup((prev) => (prev ? { ...prev, inputValue: value } : prev))
            }
            onCommit={(value) => {
              void commitLookupEdit(value);
            }}
            onCancel={() => setEditingLookup(null)}
          />
          <div className="editable-cell__meta">
            {isSaving ? 'Zapisywanie…' : 'Enter – zapisz, Esc – anuluj'}
          </div>
        </div>
      );
    }

    if (field === 'account') {
      return accountMap.get(transaction.account_id)?.name ?? '—';
    }

    if (field === 'store') {
      const transferLabel = formatTransferLabel(transaction);
      if (transferLabel) {
        return <span className="transactions-table__truncate">{transferLabel}</span>;
      }
      const store = transaction.store_id ? storeMap.get(transaction.store_id) : undefined;
      const storeGroupName = (store?.group_name ?? '').trim();
      const storeActualName = (store?.name ?? '').trim();
      const storeDisplayName =
        storeGroupName.length > 0
          ? storeGroupName
          : storeActualName.length > 0
          ? storeActualName
          : '—';
      const showStoreActualName =
        !!store &&
        storeGroupName.length > 0 &&
        storeActualName.length > 0 &&
        storeGroupName !== storeActualName;
      const storeTitle =
        storeDisplayName === '—' ? undefined : storeActualName || storeGroupName || undefined;

      return (
        <div className="transactions-grid__store">
          <span className="transactions-table__truncate" title={storeTitle}>
            {storeDisplayName}
          </span>
          {showStoreActualName && (
            <small className="text-secondary d-block">{storeActualName}</small>
          )}
        </div>
      );
    }

    if (!transaction.category_id) {
      return '—';
    }
    const category = categoryMap.get(transaction.category_id);
    if (!category) {
      return '—';
    }
    return <TagChip category={category} byId={categoryMap} />;
  };

  const renderAmountCell = (transaction: Transaction, field: EditableAmountField) => {
    const isEditing =
      editingAmount?.transactionId === transaction.id && editingAmount.field === field;
    const isSaving = savingAmountId === transaction.id;

    if (isEditing) {
      return (
        <div className="editable-cell editable-cell--numeric align-middle">
          <AmountEditCell
            value={editingAmount?.value ?? ''}
            disabled={isSaving}
            isSaving={isSaving}
            onChange={(value) =>
              setEditingAmount((prev) => (prev ? { ...prev, value } : prev))
            }
            onCommit={commitAmountEdit}
            onCancel={() => setEditingAmount(null)}
          />
        </div>
      );
    }

    if (field === 'outflow') {
      return transaction.total_cents < 0
        ? formatMoney(Math.abs(transaction.total_cents), transaction.currency)
        : '—';
    }

    return transaction.total_cents > 0
      ? formatMoney(transaction.total_cents, transaction.currency)
      : '—';
  };

  const handleGridDoubleClick = useCallback(
    (args: TransactionCellArgs) => {
      const { column, row } = args;

      if (column.key === 'date') {
        if (savingDateId === row.id) return;
        startEditingDate(row);
        return;
      }

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
        startEditingAmount(row, column.key as EditableAmountField);
        return;
      }

      if (column.key === 'open') {
        openTransactionDetails(row.id);
      }
    },
    [
      editingComment,
      savingCommentId,
      savingDateId,
      savingFieldTransactionId,
      openTransactionDetails,
      startEditingComment,
      startEditingDate,
      startEditingLookup,
      startEditingAmount
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
      void cycleTransactionStatus(args.row);
    },
    [statusUpdatingId, cycleTransactionStatus]
  );

  return {
    inlineError,
    setInlineError,
    statusUpdatingId,
    cycleTransactionStatus,
    renderCommentCell,
    renderDateCell,
    renderLookupCell,
    renderAmountCell,
    handleGridDoubleClick,
    handleGridCellKeyDown
  };
};
