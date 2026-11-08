// Inline-edit sub-module for the spending drill-down table. Owns the
// selected category, the fetched transactions for it, and the editable
// comment / store / amount cells. Kept separate from useReports because it is
// self-contained and only the Spending tab consumes it.
import { useCallback, useEffect, useMemo, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { ListIcon } from 'lucide-react';
import { type CellClickArgs } from 'react-data-grid';
import {
  Account,
  Store,
  Transaction,
  getTransactions,
  updateTransaction,
  ReportsDashboardResponse
} from '../../api';
import { type MoneyColumn } from '../../components/DataTable';
import LookupAutocompleteInput from '../../components/transactions/LookupAutocompleteInput';
import { buildStoreOptions, type LookupOption } from '../../components/transactions/lookupOptions';
import CommentEditCell from '../../components/transactions/CommentEditCell';
import AmountEditCell from '../../components/transactions/AmountEditCell';
import { formatMoney, parseAmountToCents } from '../../utils/format';
import {
  formatDate,
  getBreakdownGridHeight,
  monthEndDate,
  type BreakdownCategorySelection,
  type BreakdownEditableAmountField,
  type BreakdownEditableLookupField,
  type ReportFilters
} from './model';

type UseBreakdownEditingParams = {
  data: ReportsDashboardResponse | null;
  startMonth: string;
  endMonth: string;
  selectedAccountIds: string[];
  selectedCategoryIds: string[];
  stores: Store[];
  storeLookup: Map<string, Store>;
  accountLookup: Map<string, Account>;
  fetchReports: (start: string, end: string, filters?: ReportFilters) => Promise<void>;
  resolveAccountFilterIds: (ids: string[]) => string[] | undefined;
  resolveCategoryFilterIds: (ids: string[]) => string[] | undefined;
};

export const useBreakdownEditing = ({
  data,
  startMonth,
  endMonth,
  selectedAccountIds,
  selectedCategoryIds,
  stores,
  storeLookup,
  accountLookup,
  fetchReports,
  resolveAccountFilterIds,
  resolveCategoryFilterIds
}: UseBreakdownEditingParams) => {
  const navigate = useNavigate();
  const [selectedBreakdownCategory, setSelectedBreakdownCategory] =
    useState<BreakdownCategorySelection | null>(null);
  const [breakdownTransactions, setBreakdownTransactions] = useState<Transaction[]>([]);
  const [breakdownTransactionsLoading, setBreakdownTransactionsLoading] = useState(false);
  const [breakdownTransactionsError, setBreakdownTransactionsError] = useState<string | null>(null);
  const [editingBreakdownComment, setEditingBreakdownComment] = useState<{
    transactionId: string;
    value: string;
  } | null>(null);
  const [editingBreakdownLookup, setEditingBreakdownLookup] = useState<{
    transactionId: string;
    field: BreakdownEditableLookupField;
    inputValue: string;
  } | null>(null);
  const [editingBreakdownAmount, setEditingBreakdownAmount] = useState<{
    transactionId: string;
    field: BreakdownEditableAmountField;
    value: string;
  } | null>(null);
  const [savingBreakdownCommentId, setSavingBreakdownCommentId] = useState<string | null>(null);
  const [savingBreakdownLookupId, setSavingBreakdownLookupId] = useState<string | null>(null);
  const [savingBreakdownAmountId, setSavingBreakdownAmountId] = useState<string | null>(null);
  const [breakdownEditError, setBreakdownEditError] = useState<string | null>(null);

  const resolveTransactionDateLabel = (transaction: Transaction) => {
    const dateValue = transaction.date ?? transaction.recorded_at;
    if (!dateValue) return '—';
    return formatDate(dateValue);
  };
  const resolveTransactionAccountName = (transaction: Transaction) =>
    accountLookup.get(transaction.account_id)?.name ?? '—';
  const formatTransferLabel = useCallback(
    (transaction: Transaction) => {
      if (!transaction.transfer_account_id) return null;
      const counterparty = accountLookup.get(transaction.transfer_account_id);
      if (!counterparty) return 'Przelew';
      const currencyLabel = counterparty.currency ? ` (${counterparty.currency})` : '';
      return `Przelew: ${counterparty.name}${currencyLabel}`;
    },
    [accountLookup]
  );
  const storeOptions = useMemo(() => buildStoreOptions(stores), [stores]);
  const getBreakdownStoreInputValue = useCallback(
    (transaction: Transaction) => {
      if (!transaction.store_id) return '';
      const store = storeLookup.get(transaction.store_id);
      if (!store) return '';
      const groupName = (store.group_name ?? '').trim();
      const name = (store.name ?? '').trim();
      return groupName || name || '';
    },
    [storeLookup]
  );
  const openBreakdownTransactionDetails = useCallback(
    (transactionId: string) => {
      navigate(`/transactions/${transactionId}`);
    },
    [navigate]
  );
  const startEditingBreakdownComment = useCallback((transaction: Transaction) => {
    setBreakdownEditError(null);
    setEditingBreakdownComment({
      transactionId: transaction.id,
      value: transaction.comment ?? ''
    });
  }, []);
  const commitBreakdownCommentEdit = useCallback(async () => {
    if (!editingBreakdownComment) return;

    const transaction = breakdownTransactions.find(
      (tx) => tx.id === editingBreakdownComment.transactionId
    );
    if (!transaction) {
      setEditingBreakdownComment(null);
      return;
    }

    const nextValue = editingBreakdownComment.value.trim();
    const currentValue = (transaction.comment ?? '').trim();

    if (nextValue === currentValue) {
      setEditingBreakdownComment(null);
      return;
    }

    setSavingBreakdownCommentId(transaction.id);
    try {
      const updated = await updateTransaction(transaction.id, { comment: nextValue || null });
      setBreakdownTransactions((prev) =>
        prev.map((item) => (item.id === updated.id ? updated : item))
      );
      setBreakdownEditError(null);
      setEditingBreakdownComment(null);
    } catch (err) {
      console.error(err);
      setBreakdownEditError('Nie udało się zaktualizować komentarza.');
    } finally {
      setSavingBreakdownCommentId(null);
    }
  }, [breakdownTransactions, editingBreakdownComment]);
  const startEditingBreakdownLookup = useCallback(
    (transaction: Transaction) => {
      setBreakdownEditError(null);
      setEditingBreakdownLookup({
        transactionId: transaction.id,
        field: 'store',
        inputValue: getBreakdownStoreInputValue(transaction)
      });
    },
    [getBreakdownStoreInputValue]
  );
  const commitBreakdownLookupEdit = useCallback(
    async (option: LookupOption | null) => {
      if (!editingBreakdownLookup) return;

      const transaction = breakdownTransactions.find(
        (tx) => tx.id === editingBreakdownLookup.transactionId
      );
      if (!transaction) {
        setEditingBreakdownLookup(null);
        return;
      }

      // Ta sama pozycja co dotąd: nie zapisujemy, żeby np. wyjście z pola nie
      // przepięło transakcji na inny sklep tej samej grupy.
      if ((option?.label ?? '') === getBreakdownStoreInputValue(transaction)) {
        setEditingBreakdownLookup(null);
        return;
      }

      const newId = option?.id ?? null;

      setSavingBreakdownLookupId(transaction.id);
      try {
        const updated = await updateTransaction(transaction.id, { store_id: newId });
        setBreakdownTransactions((prev) =>
          prev.map((item) => (item.id === updated.id ? updated : item))
        );
        setBreakdownEditError(null);
        setEditingBreakdownLookup(null);
      } catch (err) {
        console.error(err);
        setBreakdownEditError('Nie udało się zaktualizować odbiorcy.');
      } finally {
        setSavingBreakdownLookupId(null);
      }
    },
    [breakdownTransactions, editingBreakdownLookup, getBreakdownStoreInputValue]
  );
  const startEditingBreakdownAmount = useCallback(
    (transaction: Transaction, field: BreakdownEditableAmountField) => {
      const current = transaction.total_cents;
      let initialValue = '';

      if (field === 'outflow' && current < 0) {
        initialValue = (Math.abs(current) / 100).toFixed(2);
      } else if (field === 'inflow' && current > 0) {
        initialValue = (current / 100).toFixed(2);
      } else {
        initialValue = '';
      }

      setBreakdownEditError(null);
      setEditingBreakdownAmount({
        transactionId: transaction.id,
        field,
        value: initialValue
      });
    },
    []
  );
  const commitBreakdownAmountEdit = useCallback(async () => {
    if (!editingBreakdownAmount) return;
    if (savingBreakdownAmountId) return;

    const { transactionId, field, value } = editingBreakdownAmount;
    const transaction = breakdownTransactions.find((tx) => tx.id === transactionId);
    if (!transaction) {
      setEditingBreakdownAmount(null);
      return;
    }

    if (!value.trim()) {
      setEditingBreakdownAmount(null);
      return;
    }

    const cents = parseAmountToCents(value);
    if (cents === null || cents < 0) {
      setBreakdownEditError('Kwota musi być nieujemną liczbą.');
      return;
    }

    let newTotalCents = transaction.total_cents;

    if (field === 'outflow') {
      newTotalCents = cents === 0 ? 0 : -Math.abs(cents);
    } else {
      newTotalCents = cents === 0 ? 0 : Math.abs(cents);
    }

    if (newTotalCents === transaction.total_cents) {
      setEditingBreakdownAmount(null);
      return;
    }

    setSavingBreakdownAmountId(transactionId);
    try {
      const updated = await updateTransaction(transactionId, { total_cents: newTotalCents });
      setBreakdownTransactions((prev) =>
        prev.map((item) => (item.id === updated.id ? updated : item))
      );
      setBreakdownEditError(null);
      setEditingBreakdownAmount(null);
      void fetchReports(startMonth, endMonth, {
        accountIds: resolveAccountFilterIds(selectedAccountIds),
        categoryIds: resolveCategoryFilterIds(selectedCategoryIds)
      });
    } catch (err) {
      console.error(err);
      setBreakdownEditError('Nie udało się zaktualizować kwoty transakcji.');
    } finally {
      setSavingBreakdownAmountId(null);
    }
  }, [
    breakdownTransactions,
    editingBreakdownAmount,
    endMonth,
    fetchReports,
    resolveAccountFilterIds,
    resolveCategoryFilterIds,
    savingBreakdownAmountId,
    selectedAccountIds,
    selectedCategoryIds,
    startMonth
  ]);
  const renderBreakdownCommentCell = useCallback(
    (transaction: Transaction) => {
      const commentText =
        transaction.comment && transaction.comment.trim().length > 0 ? transaction.comment : '—';
      const isEditing = editingBreakdownComment?.transactionId === transaction.id;
      const isSaving = savingBreakdownCommentId === transaction.id;
      const className = ['editable-cell', 'align-middle', isEditing ? 'is-editing' : '']
        .filter(Boolean)
        .join(' ');

      if (isEditing) {
        return (
          <div className={className}>
            <CommentEditCell
              value={editingBreakdownComment?.value ?? ''}
              disabled={isSaving}
              isSaving={isSaving}
              onChange={(value) => {
                setBreakdownEditError(null);
                setEditingBreakdownComment((prev) => (prev ? { ...prev, value } : prev));
              }}
              onCommit={commitBreakdownCommentEdit}
              onCancel={() => setEditingBreakdownComment(null)}
            />
          </div>
        );
      }

      return (
        <div className={className} title={commentText === '—' ? undefined : commentText}>
          <span className="transactions-table__truncate">{commentText}</span>
        </div>
      );
    },
    [commitBreakdownCommentEdit, editingBreakdownComment, savingBreakdownCommentId]
  );
  const renderBreakdownStoreCell = useCallback(
    (transaction: Transaction) => {
      const isEditing = editingBreakdownLookup?.transactionId === transaction.id;
      const isSaving = savingBreakdownLookupId === transaction.id;

      if (isEditing) {
        const className = ['editable-cell', 'align-middle', 'is-editing']
          .filter(Boolean)
          .join(' ');
        return (
          <div className={className}>
            <LookupAutocompleteInput
              inputValue={editingBreakdownLookup?.inputValue ?? ''}
              disabled={isSaving}
              options={storeOptions}
              onInputChange={(value) => {
                setBreakdownEditError(null);
                setEditingBreakdownLookup((prev) => (prev ? { ...prev, inputValue: value } : prev));
              }}
              onCommit={(option) => {
                void commitBreakdownLookupEdit(option);
              }}
              onCancel={() => setEditingBreakdownLookup(null)}
            />
            {isSaving && <div className="editable-cell__meta">Zapisywanie…</div>}
          </div>
        );
      }

      const transferLabel = formatTransferLabel(transaction);
      if (transferLabel) {
        return <span className="transactions-table__truncate">{transferLabel}</span>;
      }
      const store = transaction.store_id ? storeLookup.get(transaction.store_id) : undefined;
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
    },
    [
      commitBreakdownLookupEdit,
      editingBreakdownLookup,
      formatTransferLabel,
      savingBreakdownLookupId,
      storeLookup,
      storeOptions
    ]
  );
  const renderBreakdownAmountCell = useCallback(
    (transaction: Transaction, field: BreakdownEditableAmountField) => {
      const isEditing =
        editingBreakdownAmount?.transactionId === transaction.id &&
        editingBreakdownAmount.field === field;
      const isSaving = savingBreakdownAmountId === transaction.id;

      if (isEditing) {
        return (
          <div className="editable-cell editable-cell--numeric align-middle is-editing">
            <AmountEditCell
              value={editingBreakdownAmount?.value ?? ''}
              disabled={isSaving}
              isSaving={isSaving}
              onChange={(value) => {
                setBreakdownEditError(null);
                setEditingBreakdownAmount((prev) => (prev ? { ...prev, value } : prev));
              }}
              onCommit={commitBreakdownAmountEdit}
              onCancel={() => setEditingBreakdownAmount(null)}
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
    },
    [commitBreakdownAmountEdit, editingBreakdownAmount, savingBreakdownAmountId]
  );
  const breakdownRowKeyGetter = useCallback((transaction: Transaction) => transaction.id, []);
  const breakdownGridHeight = getBreakdownGridHeight(breakdownTransactions.length);
  const breakdownGridColumns = useMemo<readonly MoneyColumn<Transaction>[]>(
    () => [
      {
        key: 'open',
        name: '',
        width: 48,
        resizable: false,
        cellClass: 'transactions-grid__cell transactions-grid__cell--actions',
        renderCell: ({ row }) => (
          <button
            type="button"
            className="btn btn-light btn-sm d-inline-flex align-items-center gap-1 is-focusable"
            onClick={(event) => {
              event.preventDefault();
              event.stopPropagation();
              openBreakdownTransactionDetails(row.id);
            }}
            aria-label="Pokaż szczegóły transakcji"
            title="Pokaż szczegóły"
          >
            <ListIcon size={14} aria-hidden="true" />
          </button>
        )
      },
      {
        key: 'date',
        name: 'Data',
        width: 110,
        renderCell: ({ row }) => resolveTransactionDateLabel(row)
      },
      {
        key: 'account',
        name: 'Konto',
        width: 140,
        renderCell: ({ row }) => resolveTransactionAccountName(row)
      },
      {
        key: 'store',
        name: 'Sklep / grupa',
        width: 220,
        renderCell: ({ row }) => renderBreakdownStoreCell(row)
      },
      {
        key: 'comment',
        name: 'Komentarz',
        width: 240,
        renderCell: ({ row }) => renderBreakdownCommentCell(row)
      },
      {
        key: 'outflow',
        name: 'Wydatek',
        width: 120,
        cellClass: 'transactions-grid__cell transactions-grid__cell--numeric',
        renderCell: ({ row }) => renderBreakdownAmountCell(row, 'outflow')
      },
      {
        key: 'inflow',
        name: 'Wpływ',
        width: 120,
        cellClass: 'transactions-grid__cell transactions-grid__cell--numeric',
        renderCell: ({ row }) => renderBreakdownAmountCell(row, 'inflow')
      }
    ],
    [
      openBreakdownTransactionDetails,
      renderBreakdownAmountCell,
      renderBreakdownCommentCell,
      renderBreakdownStoreCell,
      resolveTransactionAccountName,
      resolveTransactionDateLabel
    ]
  );
  const handleBreakdownGridDoubleClick = useCallback(
    (args: CellClickArgs<Transaction>) => {
      const { column, row } = args;

      if (column.key === 'comment') {
        const isEditing = editingBreakdownComment?.transactionId === row.id;
        const isSaving = savingBreakdownCommentId === row.id;
        if (!isEditing && !isSaving) {
          startEditingBreakdownComment(row);
        }
        return;
      }

      if (column.key === 'store') {
        const isEditing = editingBreakdownLookup?.transactionId === row.id;
        const isSaving = savingBreakdownLookupId === row.id;
        if (!isEditing && !isSaving) {
          startEditingBreakdownLookup(row);
        }
        return;
      }

      if (column.key === 'outflow' || column.key === 'inflow') {
        const isSaving = savingBreakdownAmountId === row.id;
        if (!isSaving) {
          startEditingBreakdownAmount(row, column.key as BreakdownEditableAmountField);
        }
        return;
      }

      openBreakdownTransactionDetails(row.id);
    },
    [
      editingBreakdownComment,
      editingBreakdownLookup,
      openBreakdownTransactionDetails,
      savingBreakdownAmountId,
      savingBreakdownCommentId,
      savingBreakdownLookupId,
      startEditingBreakdownAmount,
      startEditingBreakdownComment,
      startEditingBreakdownLookup
    ]
  );

  useEffect(() => {
    if (!selectedBreakdownCategory) {
      setEditingBreakdownComment(null);
      setEditingBreakdownLookup(null);
      setEditingBreakdownAmount(null);
      setSavingBreakdownCommentId(null);
      setSavingBreakdownLookupId(null);
      setSavingBreakdownAmountId(null);
      setBreakdownEditError(null);
      setBreakdownTransactions([]);
      setBreakdownTransactionsError(null);
      setBreakdownTransactionsLoading(false);
      return;
    }

    setEditingBreakdownComment(null);
    setEditingBreakdownLookup(null);
    setEditingBreakdownAmount(null);
    setSavingBreakdownCommentId(null);
    setSavingBreakdownLookupId(null);
    setSavingBreakdownAmountId(null);
    setBreakdownEditError(null);
    let active = true;
    const startDate = data?.start_date ?? `${startMonth}-01`;
    const endDate = data?.end_date ?? monthEndDate(endMonth);
    const accountId = selectedAccountIds.length === 1 ? selectedAccountIds[0] : undefined;

    const loadTransactions = async () => {
      try {
        setBreakdownTransactionsLoading(true);
        setBreakdownTransactionsError(null);
        const payload = await getTransactions({
          start_date: startDate,
          end_date: endDate,
          category_id: selectedBreakdownCategory.id,
          account_id: accountId,
          exclude_internal: true,
          limit: 50
        });
        if (!active) return;
        setBreakdownTransactions(payload);
      } catch (err) {
        if (!active) return;
        setBreakdownTransactions([]);
        setBreakdownTransactionsError(
          err instanceof Error ? err.message : 'Nie udało się pobrać transakcji.'
        );
      } finally {
        if (active) {
          setBreakdownTransactionsLoading(false);
        }
      }
    };

    void loadTransactions();

    return () => {
      active = false;
    };
  }, [
    data?.end_date,
    data?.start_date,
    endMonth,
    selectedAccountIds,
    selectedBreakdownCategory,
    startMonth
  ]);

  return {
    selectedBreakdownCategory,
    setSelectedBreakdownCategory,
    breakdownTransactions,
    breakdownTransactionsLoading,
    breakdownTransactionsError,
    breakdownEditError,
    breakdownGridColumns,
    breakdownGridHeight,
    breakdownRowKeyGetter,
    handleBreakdownGridDoubleClick
  };
};
