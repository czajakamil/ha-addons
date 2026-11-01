import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  type CellClickArgs,
  type CellKeyboardEvent,
  type CellKeyDownArgs,
  type CellMouseEvent
} from 'react-data-grid';
import {
  IconAlertCircle,
  IconChevronDown,
  IconChevronRight,
  IconCircleDot,
  IconListDetails,
  IconPlus,
  IconTags
} from '@tabler/icons-react';
import { Transaction, deleteTransaction } from '../api';
import { TagChip } from '../components/tags/TagChip';
import { formatMoney } from '../utils/format';
import { useUntaggedCount } from '../hooks/useUntaggedCount';
import Button from '../components/Button';
import TransactionsToolbarRight from '../components/transactions/TransactionsToolbarRight';
import NewTransactionModal from '../components/transactions/NewTransactionModal';
import DataTable, { type MoneyColumn } from '../components/DataTable';
import {
  TRANSACTION_COLUMN_IDS,
  getTransactionsGridHeight,
  type TransactionColumnId
} from './transactions/model';
import {
  buildTransactionGridRows,
  itemLabel,
  splitInfoOf,
  type TransactionGridRow
} from './transactions/splitRows';
import {
  formatLoadMoreLabel,
  formatOperationsCount,
  remainingTransactions
} from './transactions/listQuery';
import { useTransactionsData } from './transactions/useTransactionsData';
import { useTransactionsEditing } from './transactions/useTransactionsEditing';
import { useColumnWidths } from './transactions/useColumnWidths';
import TransactionsFilters from './transactions/components/TransactionsFilters';
import TransactionsContextMenu from './transactions/components/TransactionsContextMenu';
import TransactionStatusCell from './transactions/components/TransactionStatusCell';

const TransactionsPage = () => {
  const navigate = useNavigate();
  const untaggedCount = useUntaggedCount();
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
    setFilters,
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
    formatTransferLabel,
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
    renderDateCell,
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
    formatTransferLabel,
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

  const handleApplyFilters = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    await loadTransactions(filters);
  };

  const handleDelete = async (transaction: Transaction) => {
    setContextMenu(null);
    if (!window.confirm('Usunąć tę transakcję?')) {
      return;
    }
    try {
      await deleteTransaction(transaction.id);
      await loadTransactions(filters);
    } catch (err) {
      console.error(err);
      setInlineError('Nie udało się usunąć transakcji.');
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
          {expanded ? (
            <IconChevronDown size={14} aria-hidden="true" />
          ) : (
            <IconChevronRight size={14} aria-hidden="true" />
          )}
        </button>
        {tagCount > 1 ? (
          <span className="transactions-split__badge" title="Pozycje mają różne tagi">
            podział ({tagCount})
          </span>
        ) : null}
      </>
    );
  };

  const transactionsGridColumns: readonly MoneyColumn<TransactionGridRow>[] = [
    {
      key: 'open',
      name: '',
      width: getColumnWidth('open'),
      resizable: false,
      cellClass: 'transactions-grid__cell transactions-grid__cell--actions',
      renderCell: ({ row }) =>
        row.kind === 'item' ? null : (
          <button
            type="button"
            className="btn btn-light btn-sm d-inline-flex align-items-center gap-1 is-focusable"
            onClick={(event) => {
              event.preventDefault();
              event.stopPropagation();
              openTransactionDetails(row.transaction.id);
            }}
            aria-label="Pokaż szczegóły transakcji"
            title="Pokaż szczegóły"
          >
            <IconListDetails size={14} aria-hidden="true" />
          </button>
        )
    },
    {
      key: 'date',
      name: 'Data',
      width: getColumnWidth('date'),
      renderCell: ({ row }) => (row.kind === 'item' ? null : renderDateCell(row.transaction))
    },
    {
      key: 'account',
      name: 'Konto',
      width: getColumnWidth('account'),
      renderCell: ({ row }) =>
        row.kind === 'item' ? null : renderLookupCell(row.transaction, 'account')
    },
    {
      key: 'store',
      name: 'Sklep / grupa',
      width: getColumnWidth('store'),
      renderCell: ({ row }) =>
        row.kind === 'item' ? null : renderLookupCell(row.transaction, 'store')
    },
    {
      key: 'category',
      name: 'Tag',
      width: getColumnWidth('category'),
      renderCell: ({ row }) =>
        row.kind === 'item' ? (
          <span className="transactions-split__item-tag">
            {row.category ? <TagChip category={row.category} byId={categoryMap} /> : '—'}
          </span>
        ) : (
          <div className="transactions-split__cell">
            {renderSplitToggle(row.transaction)}
            {renderLookupCell(row.transaction, 'category')}
          </div>
        )
    },
    {
      key: 'comment',
      name: 'Komentarz',
      width: getColumnWidth('comment'),
      renderCell: ({ row }) =>
        row.kind === 'item' ? (
          <span className="transactions-table__truncate" title={row.productName}>
            {itemLabel(row.productName, row.item)}
          </span>
        ) : (
          renderCommentCell(row.transaction)
        )
    },
    {
      key: 'outflow',
      name: 'Outflow',
      width: getColumnWidth('outflow'),
      cellClass: 'transactions-grid__cell transactions-grid__cell--numeric',
      isMoney: true,
      renderCell: ({ row }) =>
        row.kind === 'item'
          ? row.transaction.total_cents < 0
            ? formatMoney(row.totalCents, row.transaction.currency)
            : null
          : renderAmountCell(row.transaction, 'outflow')
    },
    {
      key: 'inflow',
      name: 'Inflow',
      width: getColumnWidth('inflow'),
      cellClass: 'transactions-grid__cell transactions-grid__cell--numeric',
      isMoney: true,
      renderCell: ({ row }) =>
        row.kind === 'item'
          ? row.transaction.total_cents > 0
            ? formatMoney(row.totalCents, row.transaction.currency)
            : null
          : renderAmountCell(row.transaction, 'inflow')
    },
    {
      key: 'status',
      name: (
        <span className="transactions-status__header-icon" role="img" aria-label="Status transakcji">
          <IconCircleDot size={14} aria-hidden="true" />
        </span>
      ),
      width: getColumnWidth('status'),
      resizable: false,
      cellClass: 'transactions-grid__cell transactions-grid__cell--status',
      headerCellClass: 'transactions-grid__header--status',
      renderCell: ({ row }) =>
        row.kind === 'item' ? null : (
          <TransactionStatusCell
            transaction={row.transaction}
            updating={statusUpdatingId === row.transaction.id}
            onCycle={(transaction) => void cycleTransactionStatus(transaction)}
          />
        )
    }
  ];

  const transactionRowKeyGetter = useCallback((row: TransactionGridRow) => row.id, []);

  const transactionRowClass = useCallback(
    (row: TransactionGridRow) =>
      row.kind === 'item' ? 'transactions-grid__row--item' : undefined,
    []
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
      // Menu dotyczy transakcji — wiersz pozycji nie ma czego usuwać ani otwierać.
      if (args.row.kind === 'item') return;
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
      if (args.row.kind === 'item') return;
      handleGridDoubleClick({ column: args.column, row: args.row.transaction });
    },
    [handleGridDoubleClick]
  );

  const transactionsGridHeight = getTransactionsGridHeight(gridRows.length);
  const noResults = !loading && !error && transactions.length === 0;
  const hasMore = remainingTransactions(transactions.length, totalCount) > 0;

  return (
    <div className="page accounts-page compact-page">
      <div className="page-shell">
        <div className="d-print-none accounts-page__header">
          <div className="accounts-page__title-wrap">
            <h2 className="page-title accounts-page__title">Transakcje</h2>
            <p className="accounts-page__subtitle" aria-live="polite">
              {loading ? 'Wczytywanie…' : formatOperationsCount(totalCount)}
              {refreshing && !loading && (
                <span className="spinner-border spinner-border-sm ms-2" role="status" aria-label="Wyszukiwanie" />
              )}
            </p>
          </div>
          <div className="accounts-page__actions">
            {untaggedCount ? (
              <Button
                variant="outline-secondary"
                icon={<IconTags size={16} aria-hidden="true" />}
                onClick={() => navigate('/transactions/untagged')}
              >
                Do otagowania ({untaggedCount})
              </Button>
            ) : null}
            <Button icon={<IconPlus size={16} aria-hidden="true" />} onClick={() => setShowModal(true)}>
              Nowa transakcja
            </Button>
          </div>
        </div>

        <div className="page-body page-stack accounts-page__body">
          <section className="card accounts-section-card">
            <div className="accounts-section-card__header">
              <div className="accounts-section-card__header-copy">
                <div className="accounts-section-card__eyebrow">Widok: lista</div>
                <h3 className="accounts-section-card__title">Historia operacji</h3>
                <p className="accounts-section-card__hint">
                  Wyszukiwarka przeszukuje całą historię: sklep, komentarz, tag, produkt i kwotę.
                </p>
              </div>
              <TransactionsToolbarRight searchQuery={searchQuery} setSearchQuery={setSearchQuery} />
            </div>

            <TransactionsFilters
              filters={filters}
              setFilters={setFilters}
              activeAccounts={activeAccounts}
              onSubmit={handleApplyFilters}
            />

            {inlineError && (
              <div className="accounts-section-card__alert">
                <div
                  className="alert alert-danger alert-important d-flex align-items-center gap-2 mb-0"
                  role="alert"
                >
                  <IconAlertCircle size={18} aria-hidden="true" />
                  <div>{inlineError}</div>
                </div>
              </div>
            )}

            {loading && (
              <div className="accounts-section-card__body">
                <div className="placeholder-glow">
                  <div className="placeholder col-12 mb-2" />
                  <div className="placeholder col-10 mb-2" />
                  <div className="placeholder col-12 mb-2" />
                  <div className="placeholder col-8 mb-0" />
                </div>
              </div>
            )}

            {error && !loading && (
              <div className="accounts-section-card__body">
                <div className="alert alert-danger mb-0" role="alert">
                  {error}
                </div>
              </div>
            )}

            {!loading && !error && (
              <div className="accounts-section-card__table">
                {!noResults ? (
                  <DataTable
                    aria-label="Lista transakcji"
                    className="transactions-grid"
                    columns={transactionsGridColumns}
                    rows={gridRows}
                    rowKeyGetter={transactionRowKeyGetter}
                    rowClass={transactionRowClass}
                    onColumnResize={(idx: number, width: number) => {
                      const column = transactionsGridColumns[idx];
                      if (!column || !('key' in column)) {
                        return;
                      }

                      const columnId = column.key as TransactionColumnId;

                      if (!TRANSACTION_COLUMN_IDS.has(columnId)) {
                        return;
                      }

                      handleColumnResize(columnId, width);
                    }}
                    height={transactionsGridHeight}
                    onCellDoubleClick={handleGridDoubleClickRow}
                    onCellKeyDown={handleGridCellKeyDown}
                    onCellContextMenu={handleGridContextMenu}
                    noRowsFallback={
                      <div className="data-grid__empty text-secondary text-center">
                        Brak transakcji w wynikach filtrowania.
                      </div>
                    }
                  />
                ) : (
                  <div className="accounts-empty-state">
                    <div className="accounts-empty-state__title">Brak wyników</div>
                    <p className="accounts-empty-state__subtitle">
                      Spróbuj innego filtra lub innego zapytania wyszukiwania.
                    </p>
                  </div>
                )}
                {hasMore && (
                  <div className="transactions-load-more">
                    <span className="text-secondary">
                      Wczytano {transactions.length} z {formatOperationsCount(totalCount)}
                    </span>
                    <button
                      type="button"
                      className="btn btn-outline-primary btn-sm is-focusable"
                      onClick={() => void loadMoreTransactions()}
                      disabled={loadingMore || refreshing}
                    >
                      {loadingMore && (
                        <span className="spinner-border spinner-border-sm me-2" aria-hidden="true" />
                      )}
                      {formatLoadMoreLabel(transactions.length, totalCount)}
                    </button>
                  </div>
                )}
              </div>
            )}
          </section>
        </div>
      </div>

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
