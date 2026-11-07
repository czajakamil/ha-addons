import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type React from 'react';
import { useNavigate } from 'react-router-dom';
import {
  type CellClickArgs,
  type CellKeyboardEvent,
  type CellKeyDownArgs,
  type CellMouseEvent,
  type ColSpanArgs
} from 'react-data-grid';
import { ChevronDown, ChevronRight, ListFilter, PanelRightOpen, Plus, Receipt, Tags } from 'lucide-react';
import { Store, Transaction, deleteTransaction, updateTransaction } from '../api';
import { useMediaQuery } from '../hooks/useMediaQuery';
import { useUntaggedCount } from '../hooks/useUntaggedCount';
import Button from '../components/Button';
import NewTransactionModal from '../components/transactions/NewTransactionModal';
import DataTable, { type MoneyColumn } from '../components/DataTable';
import EmptyState from '../components/ui/EmptyState';
import Money, { formatSignedMoney } from '../components/ui/Money';
import PageHeader from '../components/ui/PageHeader';
import SkeletonRows from '../components/ui/SkeletonRows';
import TruncatedText from '../components/ui/TruncatedText';
import { copy } from '../copy/pl';
import { formatDayHeading, formatRecordedTime } from '../utils/date';
import { userComment } from '../utils/transactionLabels';
import { buildBalanceAfter } from './accounts/balanceHistory';
import { DEFAULT_COLUMN_WIDTHS, type EditableAmountField, type FilterState } from './transactions/model';
import {
  buildTransactionGridRows,
  itemLabel,
  splitInfoOf,
  type TransactionGridRow
} from './transactions/splitRows';
import { formatOperationsCount } from './transactions/listQuery';
import { useTransactionsData } from './transactions/useTransactionsData';
import { useTransactionsEditing } from './transactions/useTransactionsEditing';
import { runBulkUpdate, type BulkAction, type BulkReport } from './transactions/bulkUpdate';
import TransactionsBulkBar from './transactions/components/TransactionsBulkBar';
import TransactionsBulkReport from './transactions/components/TransactionsBulkReport';
import TransactionsFilters from './transactions/components/TransactionsFilters';
import TransactionsContextMenu from './transactions/components/TransactionsContextMenu';
import TransactionStatusCell from './transactions/components/TransactionStatusCell';
import { formatCategoryPath } from '../utils/categoryPath';

const labels = copy.transactions;
const DAY_ROW_HEIGHT = 32;
const COMPACT_ROW_HEIGHT = 56;
const LOAD_MORE_THRESHOLD_PX = 600;
const COLUMN_WIDTH_TABLE_ID = 'transactions';
const ACCOUNT_COLUMN_WIDTH_TABLE_ID = 'account-transactions';
const BALANCE_AFTER_WIDTH = 128;

// Ta sama lista na stronie konta: bez nagłówka strony, wyboru konta i kolumny konta,
// za to z saldem po transakcji.
export type TransactionsAccountScope = {
  accountId: string;
  balanceCents: number;
  currency: string;
  // Zmiana wartości wczytuje listę od nowa, np. po korekcie salda
  reloadKey: number;
  onChanged: () => void;
};

const getStoreFilterLabel = (store?: Store) => store?.group_name?.trim() || store?.name || labels.storeFilter;

const TransactionsPage = ({ scope }: { scope?: TransactionsAccountScope }) => {
  const navigate = useNavigate();
  const untaggedCount = useUntaggedCount();
  // Na telefonie wiersz to sklep, konto i kwota; szczegóły otwiera dotknięcie
  const isCompact = useMediaQuery('(max-width: 700px)');
  const [selectedIds, setSelectedIds] = useState<ReadonlySet<string>>(() => new Set());
  const [bulkProgress, setBulkProgress] = useState<{ done: number; total: number } | null>(null);
  const [bulkReport, setBulkReport] = useState<BulkReport | null>(null);
  const [showModal, setShowModal] = useState(false);
  // Rozwinięte transakcje trzymamy po id, a nie w danych — zwijanie ma przetrwać
  // przeładowanie listy po edycji.
  const [expandedIds, setExpandedIds] = useState<ReadonlySet<string>>(() => new Set());
  const [contextMenu, setContextMenu] = useState<{
    x: number;
    y: number;
    transaction: Transaction;
  } | null>(null);
  // Komórka, z której otwarto menu kontekstowe — wraca do niej fokus po zamknięciu.
  const contextMenuAnchorRef = useRef<HTMLElement | null>(null);

  const closeContextMenu = useCallback((options?: { refocusCell?: boolean }) => {
    setContextMenu(null);
    if (!options?.refocusCell) return;
    const anchor = contextMenuAnchorRef.current;
    if (!anchor || !anchor.isConnected) return;
    requestAnimationFrame(() => {
      anchor.focus();
    });
  }, []);

  const {
    stores,
    categories,
    setProducts,
    transactions,
    setTransactions,
    totalCount,
    filters,
    applyFilters,
    searchQuery,
    setSearchQuery,
    loading,
    refreshing,
    loadingMore,
    error,
    loadTransactions,
    loadMoreTransactions,
    activeAccounts,
    accountMap,
    storeMap,
    categoryMap,
    productMap,
    getLookupDisplayValue
  } = useTransactionsData(scope?.accountId);

  const openTransactionDetails = useCallback(
    (transactionId: string) => {
      navigate(`/transactions/${transactionId}`);
    },
    [navigate]
  );

  const onScopeChanged = scope?.onChanged;
  const scopeReloadKey = scope?.reloadKey;
  const appliedReloadKeyRef = useRef(scopeReloadKey);
  useEffect(() => {
    if (appliedReloadKeyRef.current === scopeReloadKey) return;
    appliedReloadKeyRef.current = scopeReloadKey;
    void loadTransactions(filters);
  }, [scopeReloadKey, filters, loadTransactions]);

  // Edycja w komórce może zmienić kwotę, a więc saldo i wykres na stronie konta
  const setEditedTransactions = useCallback<typeof setTransactions>(
    (update) => {
      setTransactions(update);
      onScopeChanged?.();
    },
    [setTransactions, onScopeChanged]
  );

  const {
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
    handleGridCellKeyDown: handleEditingCellKeyDown
  } = useTransactionsEditing({
    transactions,
    setTransactions: setEditedTransactions,
    setProducts,
    activeAccounts,
    stores,
    categories,
    accountMap,
    storeMap,
    categoryMap,
    getLookupDisplayValue,
    openTransactionDetails
  });

  useEffect(() => {
    if (!contextMenu) {
      return;
    }

    const handleClose = () => {
      setContextMenu(null);
    };

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        closeContextMenu({ refocusCell: true });
      }
    };

    window.addEventListener('click', handleClose);
    window.addEventListener('contextmenu', handleClose);
    window.addEventListener('scroll', handleClose, true);
    window.addEventListener('keydown', handleKeyDown);

    return () => {
      window.removeEventListener('click', handleClose);
      window.removeEventListener('contextmenu', handleClose);
      window.removeEventListener('scroll', handleClose, true);
      window.removeEventListener('keydown', handleKeyDown);
    };
  }, [contextMenu, closeContextMenu]);

  const handleFiltersChange = (next: FilterState) => {
    setSelectedIds(new Set());
    applyFilters(next);
  };

  const toggleSelected = useCallback((transactionId: string) => {
    setSelectedIds((current) => {
      const next = new Set(current);
      if (!next.delete(transactionId)) next.add(transactionId);
      return next;
    });
  }, []);

  // Zaznaczenie obejmuje tylko wczytane wiersze; po wyszukaniu lub odświeżeniu listy
  // ukryte transakcje wypadają, inaczej licznik obiecywałby więcej, niż akcja zmieni.
  useEffect(() => {
    setSelectedIds((current) => {
      const loaded = new Set(transactions.map((tx) => tx.id));
      const next = new Set([...current].filter((id) => loaded.has(id)));
      return next.size === current.size ? current : next;
    });
  }, [transactions]);

  const runBulkAction = async (targets: readonly Transaction[], action: BulkAction) => {
    setBulkReport(null);
    setInlineError(null);
    setBulkProgress({ done: 0, total: targets.length });
    const report = await runBulkUpdate(targets, action, {
      update: updateTransaction,
      onProgress: (done, updated) => {
        setBulkProgress({ done, total: targets.length });
        const byId = new Map(updated.map((tx) => [tx.id, tx]));
        setTransactions((prev) => prev.map((tx) => byId.get(tx.id) ?? tx));
      }
    });
    setBulkProgress(null);
    setBulkReport(report);
    // Zaznaczone zostają tylko nieudane, żeby można było też użyć na nich innej akcji
    setSelectedIds(new Set(report.failures.map((failure) => failure.transaction.id)));
  };

  const applyToSelected = (action: BulkAction) =>
    runBulkAction(transactions.filter((tx) => selectedIds.has(tx.id)), action);

  const retryFailed = () => {
    if (!bulkReport) return;
    const loaded = new Map(transactions.map((tx) => [tx.id, tx]));
    // Nieudana transakcja mogła zniknąć z listy po zmianie filtrów; id wciąż wystarczy do zapisu
    const targets = bulkReport.failures.map((failure) => loaded.get(failure.transaction.id) ?? failure.transaction);
    void runBulkAction(targets, bulkReport.action);
  };

  const failedIds = useMemo(
    () => new Set(bulkReport?.failures.map((failure) => failure.transaction.id)),
    [bulkReport]
  );

  const handleDelete = async (transaction: Transaction) => {
    setContextMenu(null);
    if (!window.confirm(labels.deleteConfirm)) {
      return;
    }
    try {
      await deleteTransaction(transaction.id);
      await loadTransactions(filters);
      onScopeChanged?.();
    } catch (err) {
      console.error(err);
      setInlineError(labels.deleteFailed);
    }
  };

  const handleContextMenuDeleteClick = () => {
    if (!contextMenu) {
      return;
    }
    const transaction = contextMenu.transaction;
    closeContextMenu({ refocusCell: true });
    void handleDelete(transaction);
  };

  const handleContextMenuOpenClick = () => {
    if (!contextMenu) {
      return;
    }
    const transaction = contextMenu.transaction;
    setContextMenu(null);
    openTransactionDetails(transaction.id);
  };

  const handleContextMenuToggleReconciledClick = () => {
    if (!contextMenu) {
      return;
    }
    const transaction = contextMenu.transaction;
    closeContextMenu({ refocusCell: true });
    void toggleTransactionReconciled(transaction);
  };

  const toggleExpanded = useCallback((transactionId: string) => {
    setExpandedIds((current) => {
      const next = new Set(current);
      if (!next.delete(transactionId)) next.add(transactionId);
      return next;
    });
  }, []);

  const setExpanded = useCallback((transactionId: string, expanded: boolean) => {
    setExpandedIds((current) => {
      if (current.has(transactionId) === expanded) return current;
      const next = new Set(current);
      if (expanded) next.add(transactionId);
      else next.delete(transactionId);
      return next;
    });
  }, []);

  const gridRows = useMemo(
    () => buildTransactionGridRows(transactions, expandedIds, productMap, categoryMap),
    [transactions, expandedIds, productMap, categoryMap]
  );

  // Saldo po transakcji liczymy wstecz od bieżącego, więc lista musi być ciągła od
  // najnowszej: przy wyszukiwaniu lub filtrach brakujące wiersze dałyby złe kwoty.
  const listIsContinuous =
    !searchQuery.trim() && !filters.startDate && !filters.endDate && !filters.categoryIds.length;
  const scopeBalanceCents = scope?.balanceCents;
  const balanceAfter = useMemo(
    () =>
      scopeBalanceCents !== undefined && listIsContinuous
        ? buildBalanceAfter(scopeBalanceCents, transactions)
        : null,
    [scopeBalanceCents, listIsContinuous, transactions]
  );
  const hideCommentColumn = Boolean(scope) && !transactions.some((tx) => userComment(tx.comment));

  const renderSplitToggle = (transaction: Transaction) => {
    const { itemCount, tagCount } = splitInfoOf(transaction, productMap);
    if (!itemCount) return null;
    const expanded = expandedIds.has(transaction.id);
    return (
      <>
        <button
          type="button"
          className="transactions-split__toggle is-focusable"
          aria-expanded={expanded}
          aria-label={`${expanded ? 'Zwiń' : 'Rozwiń'} pozycje (${itemCount})`}
          title={`${itemCount} ${itemCount === 1 ? 'pozycja' : 'pozycje'} — ${
            expanded ? 'zwiń' : 'rozwiń'
          }`}
          onClick={(event) => {
            event.preventDefault();
            event.stopPropagation();
            toggleExpanded(transaction.id);
          }}
        >
          {expanded ? <ChevronDown size={14} aria-hidden="true" /> : <ChevronRight size={14} aria-hidden="true" />}
        </button>
        {tagCount > 1 ? (
          <span className="transactions-split__badge" title="Pozycje mają różne kategorie">
            podział ({tagCount})
          </span>
        ) : null}
      </>
    );
  };

  const renderDayRow = (row: Extract<TransactionGridRow, { kind: 'day' }>) => (
    <div className="transactions-day">
      <span>{row.date ? formatDayHeading(row.date) : '—'}</span>
      {row.totalCents !== null && <Money cents={row.totalCents} currency={row.currency} tone="neutral" />}
    </div>
  );

  // Wiersz dnia sięga do kolumny kwoty, żeby suma stała pod kwotami
  const dayRowSpan = (columnCount: number) => (args: ColSpanArgs<TransactionGridRow, unknown>) =>
    args.type === 'ROW' && args.row.kind === 'day' ? columnCount : undefined;

  const compactMeta = (transaction: Transaction) =>
    [
      formatRecordedTime(transaction.recorded_at),
      scope ? null : accountMap.get(transaction.account_id)?.name
    ]
      .filter(Boolean)
      .join(' · ');

  const compactColumns: readonly MoneyColumn<TransactionGridRow>[] = [
    {
      key: 'store',
      name: labels.columns.store,
      colSpan: dayRowSpan(2),
      renderCell: ({ row }) => {
        if (row.kind === 'day') return renderDayRow(row);
        if (row.kind === 'item') return <TruncatedText text={itemLabel(row.productName, row.item)} />;
        return (
          <div className="transactions-compact">
            {renderLookupCell(row.transaction, 'store')}
            {compactMeta(row.transaction) && (
              <span className="transactions-compact__meta">{compactMeta(row.transaction)}</span>
            )}
          </div>
        );
      }
    },
    {
      key: 'amount',
      name: labels.columns.amount,
      // Na stronie konta mieści też podpis z saldem
      width: scope ? 144 : 112,
      resizable: false,
      cellClass: 'transactions-grid__cell transactions-grid__cell--numeric',
      headerCellClass: 'transactions-grid__cell--numeric',
      renderCell: ({ row }) =>
        row.kind === 'transaction' ? (
          <div className="transactions-compact transactions-compact--numeric">
            <Money cents={row.transaction.total_cents} currency={row.transaction.currency} />
            {balanceAfter?.has(row.transaction.id) && (
              <span className="transactions-compact__meta">
                {labels.balanceAfterShort}{' '}
                {formatSignedMoney(balanceAfter.get(row.transaction.id) ?? 0, scope?.currency)}
              </span>
            )}
          </div>
        ) : null
    }
  ];

  // Wiersz dnia sięga do kolumny wpływu, a jej pozycja zależy od ukrytych kolumn
  const dayRowColumns = 8 - (scope ? 1 : 0) - (hideCommentColumn ? 1 : 0);

  // Pozycja podziału stoi po stronie wynikającej z jej własnego znaku
  const renderItemAmount = (row: Extract<TransactionGridRow, { kind: 'item' }>, field: EditableAmountField) => {
    const cents = (row.transaction.total_cents < 0 ? -1 : 1) * row.totalCents;
    if (field === 'outflow' ? cents > 0 : cents <= 0) return null;
    return <Money cents={Math.abs(cents)} currency={row.transaction.currency} tone="neutral" />;
  };

  const fullColumns: readonly MoneyColumn<TransactionGridRow>[] = [
    {
      key: 'select',
      name: '',
      width: DEFAULT_COLUMN_WIDTHS.select,
      resizable: false,
      colSpan: dayRowSpan(dayRowColumns),
      renderHeaderCell: () => {
        const allSelected = transactions.length > 0 && transactions.every((tx) => selectedIds.has(tx.id));
        return (
          <input
            type="checkbox"
            className="form-check-input"
            aria-label={labels.selectAll}
            checked={allSelected}
            onChange={() =>
              setSelectedIds(allSelected ? new Set() : new Set(transactions.map((tx) => tx.id)))
            }
          />
        );
      },
      renderCell: ({ row }) => {
        if (row.kind === 'day') return renderDayRow(row);
        if (row.kind === 'item') return null;
        return (
          <input
            type="checkbox"
            className="form-check-input"
            aria-label={labels.selectRow}
            checked={selectedIds.has(row.transaction.id)}
            onChange={() => toggleSelected(row.transaction.id)}
            onClick={(event) => event.stopPropagation()}
          />
        );
      }
    },
    {
      key: 'time',
      name: labels.columns.time,
      width: DEFAULT_COLUMN_WIDTHS.time,
      resizable: false,
      cellClass: 'transactions-grid__cell transactions-grid__cell--time',
      renderCell: ({ row }) => (row.kind === 'transaction' ? renderTimeCell(row.transaction) : null)
    },
    {
      key: 'store',
      name: labels.columns.store,
      width: hideCommentColumn ? undefined : DEFAULT_COLUMN_WIDTHS.store,
      renderCell: ({ row }) => {
        if (row.kind === 'transaction') return renderLookupCell(row.transaction, 'store');
        // Bez kolumny komentarza nazwa pozycji podziału trafia tutaj
        if (row.kind === 'item' && hideCommentColumn) {
          return <TruncatedText text={itemLabel(row.productName, row.item)} />;
        }
        return null;
      }
    },
    ...(scope
      ? []
      : [
          {
            key: 'account',
            name: labels.columns.account,
            width: DEFAULT_COLUMN_WIDTHS.account,
            renderCell: ({ row }: { row: TransactionGridRow }) =>
              row.kind === 'transaction' ? renderLookupCell(row.transaction, 'account') : null
          }
        ]),
    {
      key: 'category',
      name: labels.columns.category,
      width: DEFAULT_COLUMN_WIDTHS.category,
      renderCell: ({ row }) => {
        if (row.kind === 'day') return null;
        if (row.kind === 'item') return renderItemCategoryCell(row.id, row.category);
        return (
          <div className="transactions-split__cell">
            {renderSplitToggle(row.transaction)}
            {renderLookupCell(row.transaction, 'category')}
          </div>
        );
      }
    },
    ...(hideCommentColumn
      ? []
      : [
          {
            key: 'comment',
            name: labels.columns.comment,
            width: DEFAULT_COLUMN_WIDTHS.comment,
            renderCell: ({ row }: { row: TransactionGridRow }) => {
              if (row.kind === 'day') return null;
              if (row.kind === 'item') return <TruncatedText text={itemLabel(row.productName, row.item)} />;
              return renderCommentCell(row.transaction);
            }
          }
        ]),
    ...(['outflow', 'inflow'] as const).map(
      (field): MoneyColumn<TransactionGridRow> => ({
        key: field,
        name: labels.columns[field],
        width: DEFAULT_COLUMN_WIDTHS[field],
        cellClass: 'transactions-grid__cell transactions-grid__cell--numeric',
        headerCellClass: 'transactions-grid__cell--numeric',
        renderCell: ({ row }) => {
          if (row.kind === 'day') return null;
          if (row.kind === 'item') return renderItemAmount(row, field);
          return renderAmountCell(row.transaction, field);
        }
      })
    ),
    ...(balanceAfter
      ? [
          {
            key: 'balance_after',
            name: labels.columns.balanceAfter,
            width: BALANCE_AFTER_WIDTH,
            cellClass: 'transactions-grid__cell transactions-grid__cell--numeric',
            headerCellClass: 'transactions-grid__cell--numeric',
            renderCell: ({ row }: { row: TransactionGridRow }) => {
              const cents = row.kind === 'transaction' ? balanceAfter.get(row.transaction.id) : undefined;
              return cents === undefined ? null : (
                <Money cents={cents} currency={scope?.currency} tone={cents < 0 ? 'negative' : 'neutral'} />
              );
            }
          }
        ]
      : []),
    {
      key: 'status',
      name: '',
      width: DEFAULT_COLUMN_WIDTHS.status,
      resizable: false,
      cellClass: 'transactions-grid__cell transactions-grid__cell--status',
      renderCell: ({ row }) =>
        row.kind === 'transaction' ? (
          <TransactionStatusCell
            transaction={row.transaction}
            updating={statusUpdatingId === row.transaction.id}
            onToggle={(transaction) => void toggleTransactionStatus(transaction)}
          />
        ) : null
    },
    {
      key: 'open',
      name: '',
      width: DEFAULT_COLUMN_WIDTHS.open,
      resizable: false,
      cellClass: 'transactions-grid__cell transactions-grid__cell--actions',
      renderCell: ({ row }) =>
        row.kind === 'transaction' ? (
          <button
            type="button"
            className="transactions-row-action"
            onClick={(event) => {
              event.preventDefault();
              event.stopPropagation();
              openTransactionDetails(row.transaction.id);
            }}
            aria-label={labels.details}
            title={labels.details}
          >
            <PanelRightOpen size={14} aria-hidden="true" />
          </button>
        ) : null
    }
  ];

  const transactionsGridColumns = isCompact ? compactColumns : fullColumns;

  const transactionRowKeyGetter = useCallback((row: TransactionGridRow) => row.id, []);

  const transactionRowClass = useCallback(
    (row: TransactionGridRow) => {
      if (row.kind === 'day') return 'transactions-grid__row--day';
      if (row.kind === 'item') return 'transactions-grid__row--item';
      return failedIds.has(row.transaction.id) ? 'transactions-grid__row--failed' : undefined;
    },
    [failedIds]
  );

  const transactionRowHeight = useCallback(
    (row: TransactionGridRow) => {
      if (row.kind === 'day') return DAY_ROW_HEIGHT;
      return isCompact && row.kind === 'transaction' ? COMPACT_ROW_HEIGHT : 40;
    },
    [isCompact]
  );

  const openTransactionContextMenu = useCallback(
    (transaction: Transaction, clientX: number, clientY: number) => {
      setContextMenu({
        x: clientX,
        y: clientY,
        transaction
      });
    },
    []
  );

  const handleGridContextMenu = useCallback(
    (args: CellClickArgs<TransactionGridRow>, event: CellMouseEvent) => {
      // Menu dotyczy transakcji — wiersze dnia i pozycji nie mają czego usuwać ani otwierać.
      if (args.row.kind !== 'transaction') return;
      event.preventDefault();
      event.stopPropagation();
      event.preventGridDefault?.();
      contextMenuAnchorRef.current =
        (event.target as HTMLElement | null)?.closest<HTMLElement>('.rdg-cell') ?? null;
      openTransactionContextMenu(args.row.transaction, event.clientX, event.clientY);
    },
    [openTransactionContextMenu]
  );

  const handleGridCellKeyDown = useCallback(
    (args: CellKeyDownArgs<TransactionGridRow>, event: CellKeyboardEvent) => {
      if (args.row.kind === 'day') return;
      const isContextMenuKey =
        event.key === 'ContextMenu' || (event.shiftKey && event.key === 'F10');

      // →/← rozwija i zwija podział bez sięgania po mysz, ale tylko w kolumnie
      // z chevronem i tylko gdy strzałka ma co zmienić — inaczej odebralibyśmy
      // siatce poziomą nawigację na każdym paragonie.
      if (
        args.mode === 'SELECT' &&
        args.column.key === 'category' &&
        (event.key === 'ArrowRight' || event.key === 'ArrowLeft')
      ) {
        const transaction = args.row.transaction;
        const expand = event.key === 'ArrowRight';
        const changes =
          transaction.items.length > 0 && expandedIds.has(transaction.id) !== expand;
        if (changes) {
          event.preventDefault();
          event.preventGridDefault?.();
          setExpanded(transaction.id, expand);
          return;
        }
      }

      if (args.row.kind === 'item') return;

      if (args.mode === 'SELECT' && isContextMenuKey) {
        event.preventDefault();
        event.preventGridDefault?.();
        const cell = (event.target as HTMLElement | null)?.closest<HTMLElement>('.rdg-cell') ?? null;
        contextMenuAnchorRef.current = cell;
        const rect = cell?.getBoundingClientRect();
        openTransactionContextMenu(
          args.row.transaction,
          rect ? rect.left + Math.min(rect.width / 2, 24) : 0,
          rect ? rect.bottom : 0
        );
        return;
      }

      handleEditingCellKeyDown(
        { column: args.column, row: args.row.transaction, mode: args.mode },
        event
      );
    },
    [handleEditingCellKeyDown, openTransactionContextMenu, expandedIds, setExpanded]
  );

  const handleGridDoubleClickRow = useCallback(
    (args: CellClickArgs<TransactionGridRow>) => {
      const { row } = args;
      if (row.kind === 'item') {
        if (args.column.key === 'category' && savingProductId !== row.item.product_id) {
          startEditingItemCategory(row.id, row.item.product_id, row.category);
        }
        return;
      }
      if (row.kind !== 'transaction') return;
      handleGridDoubleClick({ column: args.column, row: row.transaction });
    },
    [handleGridDoubleClick, startEditingItemCategory, savingProductId]
  );

  const hasMore = totalCount === null || transactions.length < totalCount;
  const noResults = !loading && !error && transactions.length === 0;
  const hasActiveFilters =
    Boolean(searchQuery.trim()) ||
    Boolean(filters.startDate || filters.endDate || filters.categoryIds.length || filters.storeIds?.length || (filters.accountIds.length > 0 && !scope));

  // Kolejne strony doładowują się same, zanim użytkownik dojedzie do końca listy
  const handleGridScroll = (event: React.UIEvent<HTMLDivElement>) => {
    const { scrollTop, clientHeight, scrollHeight } = event.currentTarget;
    if (hasMore && !loadingMore && !refreshing && scrollHeight - scrollTop - clientHeight < LOAD_MORE_THRESHOLD_PX) {
      void loadMoreTransactions();
    }
  };

  const countLabel = totalCount === null ? `${transactions.length}+` : String(totalCount);

  return (
    <div className={scope ? 'transactions-page transactions-page--account' : 'page-shell transactions-page'}>
      {!scope && (
        <PageHeader
          title={labels.title}
          actions={
            <>
              {untaggedCount ? (
                <button type="button" className="text-button" onClick={() => navigate('/transactions/uncategorized')}>
                  <Tags size={14} aria-hidden="true" />
                  {labels.toTag(untaggedCount)}
                </button>
              ) : null}
              <Button icon={<Plus size={16} aria-hidden="true" />} onClick={() => setShowModal(true)}>
                {labels.newTransaction}
              </Button>
            </>
          }
        />
      )}
      <p className="sr-only" aria-live="polite">
        {loading ? '' : formatOperationsCount(transactions.length)}
      </p>

      <TransactionsFilters
        filters={filters}
        onChange={handleFiltersChange}
        searchQuery={searchQuery}
        onSearchChange={setSearchQuery}
        activeAccounts={activeAccounts}
        categories={categories}
        storeLabel={filters.storeIds?.length ? getStoreFilterLabel(storeMap.get(filters.storeIds[0])) : undefined}
        accountLocked={Boolean(scope)}
      />

      {selectedIds.size > 0 && (
        <TransactionsBulkBar
          count={selectedIds.size}
          categories={categories}
          busy={bulkProgress !== null}
          progress={bulkProgress && labels.bulkSaving(bulkProgress.done, bulkProgress.total)}
          onCategoryChange={(categoryId) => {
            const category = categoryMap.get(categoryId);
            void applyToSelected({
              label: labels.bulk.categoryAction(formatCategoryPath(category?.path) || category?.name || ''),
              skippedLabel: labels.bulk.skippedTransfers,
              payload: (tx) => (tx.transfer_account_id ? null : { category_id: categoryId })
            });
          }}
          onApprove={() =>
            void applyToSelected({
              label: labels.bulk.approveAction,
              skippedLabel: labels.bulk.skippedApproved,
              payload: (tx) => (tx.cleared ? null : { cleared: true })
            })
          }
          onClear={() => setSelectedIds(new Set())}
        />
      )}

      {bulkReport && (
        <TransactionsBulkReport
          report={bulkReport}
          busy={bulkProgress !== null}
          transactionName={(tx) => {
            const store = tx.store_id ? storeMap.get(tx.store_id) : undefined;
            return store?.group_name?.trim() || store?.name || tx.comment || '—';
          }}
          onRetry={retryFailed}
          onDismiss={() => setBulkReport(null)}
        />
      )}

      {inlineError && (
        <p className="transactions-error" role="alert">
          {inlineError}
        </p>
      )}
      {error && !loading && (
        <p className="transactions-error" role="alert">
          {error}
        </p>
      )}

      <section className="transactions-card" aria-busy={refreshing || loadingMore}>
        {loading && <SkeletonRows rows={12} />}
        {noResults && (
          <EmptyState
            icon={hasActiveFilters ? ListFilter : Receipt}
            message={hasActiveFilters ? labels.empty : labels.emptyAll}
          />
        )}
        {!loading && !error && !noResults && (
          <DataTable
            aria-label={labels.gridLabel}
            tableId={scope ? ACCOUNT_COLUMN_WIDTH_TABLE_ID : COLUMN_WIDTH_TABLE_ID}
            className="transactions-grid"
            columns={transactionsGridColumns}
            rows={gridRows}
            rowKeyGetter={transactionRowKeyGetter}
            rowClass={transactionRowClass}
            rowHeight={transactionRowHeight}
            onScroll={handleGridScroll}
            onCellClick={
              isCompact
                ? (args) => {
                    if (args.row.kind === 'transaction') openTransactionDetails(args.row.transaction.id);
                  }
                : undefined
            }
            onCellDoubleClick={isCompact ? undefined : handleGridDoubleClickRow}
            onCellKeyDown={handleGridCellKeyDown}
            onCellContextMenu={handleGridContextMenu}
          />
        )}
      </section>

      <p className="transactions-count">{loading ? '' : formatOperationsCount(transactions.length)} {totalCount !== null && !loading && transactions.length < totalCount ? `z ${countLabel}` : ''}</p>

      <NewTransactionModal
        open={showModal}
        accounts={activeAccounts}
        stores={stores}
        categories={categories}
        onClose={() => setShowModal(false)}
        onCreated={async () => {
          await loadTransactions(filters);
          onScopeChanged?.();
        }}
      />

      {contextMenu && (
        <TransactionsContextMenu
          x={contextMenu.x}
          y={contextMenu.y}
          reconciled={contextMenu.transaction.reconciled}
          onOpenDetails={handleContextMenuOpenClick}
          onToggleReconciled={handleContextMenuToggleReconciledClick}
          onDelete={handleContextMenuDeleteClick}
        />
      )}
    </div>
  );
};

export default TransactionsPage;
