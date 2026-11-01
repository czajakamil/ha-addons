// Edycja inline wierszy transakcji: komentarz, kwota, pola słownikowe
// (konto/sklep/kategoria) oraz cykliczna zmiana statusu. Zwraca funkcje renderujące
// komórki i handlery zdarzeń siatki, z których korzysta TransactionsPage.
import { useCallback, useState } from 'react';
import type { Dispatch, SetStateAction } from 'react';
import { type CellKeyboardEvent } from 'react-data-grid';
import { Account, Category, Store, Transaction, updateTransaction } from '../../api';
import LookupAutocompleteInput from '../../components/transactions/LookupAutocompleteInput';
import {
  buildStoreOptions,
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
import { BALANCE_ADJUSTMENT_LABEL, isBalanceAdjustment, userComment } from '../../utils/transactionLabels';
import { getNextStatusPayload, type EditableAmountField, type EditableLookupField } from './model';

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

    const newId = option?.id ?? null;

    if (field === 'store') {
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
          ? [...buildStoreOptions(stores), ...buildTransferOptions(accounts)]
          : categories.map((c) => ({ id: c.id, label: c.name, kind: 'entry' }));
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
        <div className="editable-cell editable-cell--numeric align-middle">
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
    renderLookupCell,
    renderAmountCell,
    handleGridDoubleClick,
    handleGridCellKeyDown
  };
};
