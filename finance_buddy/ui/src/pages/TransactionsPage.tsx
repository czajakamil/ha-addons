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
import { Transaction, deleteTransaction, updateTransaction, type TransactionUpdate } from '../api';
import { TagChip } from '../components/tags/TagChip';
import { useMediaQuery } from '../hooks/useMediaQuery';
import { useUntaggedCount } from '../hooks/useUntaggedCount';
import Button from '../components/Button';
import NewTransactionModal from '../components/transactions/NewTransactionModal';
import DataTable, { type MoneyColumn } from '../components/DataTable';
import EmptyState from '../components/ui/EmptyState';
import Money from '../components/ui/Money';
import PageHeader from '../components/ui/PageHeader';
import SkeletonRows from '../components/ui/SkeletonRows';
import TruncatedText from '../components/ui/TruncatedText';
import { copy } from '../copy/pl';
import { formatDayHeading } from '../utils/date';
import { TRANSACTION_COLUMN_IDS, type FilterState, type TransactionColumnId } from './transactions/model';
import {
  buildTransactionGridRows,
  itemLabel,
  splitInfoOf,
  type TransactionGridRow
} from './transactions/splitRows';
import { formatOperationsCount } from './transactions/listQuery';
import { useTransactionsData } from './transactions/useTransactionsData';
import { useTransactionsEditing } from './transactions/useTransactionsEditing';
import { useColumnWidths } from './transactions/useColumnWidths';
import TransactionsBulkBar from './transactions/components/TransactionsBulkBar';
import TransactionsFilters from './transactions/components/TransactionsFilters';
import TransactionsContextMenu from './transactions/components/TransactionsContextMenu';
import TransactionStatusCell from './transactions/components/TransactionStatusCell';

const labels = copy.transactions;
const DAY_ROW_HEIGHT = 32;
const COMPACT_ROW_HEIGHT = 56;
const LOAD_MORE_THRESHOLD_PX = 600;
const BULK_CONCURRENCY = 5;

const TransactionsPage = () => {
  const navigate = useNavigate();
  const untaggedCount = useUntaggedCount();
  // Na telefonie wiersz to sklep, konto i kwota; szczegóły otwiera dotknięcie
  const isCompact = useMediaQuery('(max-width: 700px)');
  const [selectedIds, setSelectedIds] = useState<ReadonlySet<string>>(() => new Set());
  const [bulkBusy, setBulkBusy] = useState(false);
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
    accounts,
    stores,
    categories,
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
  } = useTransactionsData();

  const { getColumnWidth, handleColumnResize } = useColumnWidths();

  const openTransactionDetails = useCallback(
    (transactionId: string) => {
      navigate(`/transactions/${transactionId}`);
    },
    [navigate]
  );

  const {
    inlineError,
    setInlineError,
    statusUpdatingId,
    cycleTransactionStatus,
    renderCommentCell,
    renderLookupCell,
    renderAmountCell,
    handleGridDoubleClick,
    handleGridCellKeyDown: handleEditingCellKeyDown
  } = useTransactionsEditing({
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

  // API nie ma zapisu zbiorczego, więc aktualizujemy porcjami po kilka transakcji
  const applyToSelected = async (payload: (transaction: Transaction) => TransactionUpdate | null) => {
    const targets = transactions.filter((tx) => selectedIds.has(tx.id));
    setBulkBusy(true);
    let failed = false;
    for (let start = 0; start < targets.length; start += BULK_CONCURRENCY) {
      const results = await Promise.allSettled(
        targets.slice(start, start + BULK_CONCURRENCY).map(async (tx) => {
          const update = payload(tx);
          return update ? updateTransaction(tx.id, update) : tx;
        })
      );
      const updated = new Map<string, Transaction>();
      results.forEach((result) => {
        if (result.status === 'fulfilled') updated.set(result.value.id, result.value);
        else failed = true;
      });
      setTransactions((prev) => prev.map((tx) => updated.get(tx.id) ?? tx));
    }
    setBulkBusy(false);
    if (failed) {
      setInlineError(labels.bulkFailed);
    } else {
      setInlineError(null);
      setSelectedIds(new Set());
    }
  };

  const handleDelete = async (transaction: Transaction) => {
    setContextMenu(null);
    if (!window.confirm(labels.deleteConfirm)) {
      return;
    }
    try {
      await deleteTransaction(transaction.id);
      await loadTransactions(filters);
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
          <span className="transactions-split__badge" title="Pozycje mają różne tagi">
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
            <span className="transactions-compact__meta">{accountMap.get(row.transaction.account_id)?.name}</span>
          </div>
        );
      }
    },
    {
      key: 'amount',
      name: labels.columns.amount,
      width: 112,
      resizable: false,
      cellClass: 'transactions-grid__cell transactions-grid__cell--numeric',
      headerCellClass: 'transactions-grid__cell--numeric',
      renderCell: ({ row }) =>
        row.kind === 'transaction' ? (
          <Money cents={row.transaction.total_cents} currency={row.transaction.currency} />
        ) : null
    }
  ];

  const fullColumns: readonly MoneyColumn<TransactionGridRow>[] = [
    {
      key: 'select',
      name: '',
      width: getColumnWidth('select'),
      resizable: false,
      colSpan: dayRowSpan(6),
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
      key: 'store',
      name: labels.columns.store,
      width: getColumnWidth('store'),
      renderCell: ({ row }) =>
        row.kind === 'transaction' ? renderLookupCell(row.transaction, 'store') : null
    },
    {
      key: 'account',
      name: labels.columns.account,
      width: getColumnWidth('account'),
      renderCell: ({ row }) =>
        row.kind === 'transaction' ? renderLookupCell(row.transaction, 'account') : null
    },
    {
      key: 'category',
      name: labels.columns.category,
      width: getColumnWidth('category'),
      renderCell: ({ row }) => {
        if (row.kind === 'day') return null;
        if (row.kind === 'item') {
          return row.category ? <TagChip category={row.category} byId={categoryMap} /> : null;
        }
        return (
          <div className="transactions-split__cell">
            {renderSplitToggle(row.transaction)}
            {renderLookupCell(row.transaction, 'category')}
          </div>
        );
      }
    },
    {
      key: 'comment',
      name: labels.columns.comment,
      width: getColumnWidth('comment'),
      renderCell: ({ row }) => {
        if (row.kind === 'day') return null;
        if (row.kind === 'item') return <TruncatedText text={itemLabel(row.productName, row.item)} />;
        return renderCommentCell(row.transaction);
      }
    },
    {
      key: 'amount',
      name: labels.columns.amount,
      width: getColumnWidth('amount'),
      cellClass: 'transactions-grid__cell transactions-grid__cell--numeric',
      headerCellClass: 'transactions-grid__cell--numeric',
      renderCell: ({ row }) => {
        if (row.kind === 'day') return null;
        if (row.kind === 'item') {
          const sign = row.transaction.total_cents < 0 ? -1 : 1;
          return <Money cents={sign * row.totalCents} currency={row.transaction.currency} tone="neutral" />;
        }
        return renderAmountCell(row.transaction);
      }
    },
    {
      key: 'status',
      name: '',
      width: getColumnWidth('status'),
      resizable: false,
      cellClass: 'transactions-grid__cell transactions-grid__cell--status',
      renderCell: ({ row }) =>
        row.kind === 'transaction' ? (
          <TransactionStatusCell
            transaction={row.transaction}
            updating={statusUpdatingId === row.transaction.id}
            onCycle={(transaction) => void cycleTransactionStatus(transaction)}
          />
        ) : null
    },
    {
      key: 'open',
      name: '',
      width: getColumnWidth('open'),
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
      return undefined;
    },
    []
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
      if (args.row.kind !== 'transaction') return;
      handleGridDoubleClick({ column: args.column, row: args.row.transaction });
    },
    [handleGridDoubleClick]
  );

  const hasMore = totalCount === null || transactions.length < totalCount;
  const noResults = !loading && !error && transactions.length === 0;
  const hasActiveFilters =
    Boolean(searchQuery.trim()) || Object.values(filters).some((value) => Boolean(value));

  // Kolejne strony doładowują się same, zanim użytkownik dojedzie do końca listy
  const handleGridScroll = (event: React.UIEvent<HTMLDivElement>) => {
    const { scrollTop, clientHeight, scrollHeight } = event.currentTarget;
    if (hasMore && !loadingMore && !refreshing && scrollHeight - scrollTop - clientHeight < LOAD_MORE_THRESHOLD_PX) {
      void loadMoreTransactions();
    }
  };

  const countLabel = totalCount === null ? `${transactions.length}+` : String(totalCount);

  return (
    <div className="page-shell transactions-page">
      <PageHeader
        title={labels.title}
        actions={
          <>
            {untaggedCount ? (
              <button type="button" className="text-button" onClick={() => navigate('/transactions/untagged')}>
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
      />

      {selectedIds.size > 0 && (
        <TransactionsBulkBar
          count={selectedIds.size}
          categories={categories}
          busy={bulkBusy}
          onCategoryChange={(categoryId) =>
            void applyToSelected((tx) => (tx.transfer_account_id ? null : { category_id: categoryId }))
          }
          onApprove={() => void applyToSelected((tx) => (tx.cleared ? null : { cleared: true }))}
          onClear={() => setSelectedIds(new Set())}
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
            className="transactions-grid"
            columns={transactionsGridColumns}
            rows={gridRows}
            rowKeyGetter={transactionRowKeyGetter}
            rowClass={transactionRowClass}
            rowHeight={transactionRowHeight}
            onScroll={handleGridScroll}
            onColumnResize={(idx: number, width: number) => {
              const column = transactionsGridColumns[idx];
              if (!column || !('key' in column)) return;
              const columnId = column.key as TransactionColumnId;
              if (!TRANSACTION_COLUMN_IDS.has(columnId)) return;
              handleColumnResize(columnId, width);
            }}
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
        }}
      />

      {contextMenu && (
        <TransactionsContextMenu
          x={contextMenu.x}
          y={contextMenu.y}
          onOpenDetails={handleContextMenuOpenClick}
          onDelete={handleContextMenuDeleteClick}
        />
      )}
    </div>
  );
};

export default TransactionsPage;
