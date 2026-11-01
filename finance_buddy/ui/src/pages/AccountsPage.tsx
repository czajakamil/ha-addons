import { useCallback, useMemo, useState } from 'react';
import {
  IconAlertCircle,
  IconFileSpreadsheet,
  IconPencil,
  IconPlus,
  IconReceipt2
} from '@tabler/icons-react';
import { useNavigate } from 'react-router-dom';
import { Account, getAccounts } from '../api';
import DataTable, { type MoneyColumn } from '../components/DataTable';
import Button from '../components/Button';
import { useApi } from '../hooks/useApi';
import {
  ACCOUNT_GRID_HEADER_HEIGHT,
  ACCOUNT_GRID_ROW_HEIGHT,
  ACCOUNT_KIND_OPTIONS,
  getAccountKindLabel,
  getAccountsGridHeight,
  isLiabilityAccountKind,
  normalizeCategoryLabel,
  type SortOption
} from './accounts/model';
import { useAccountCategories } from './accounts/useAccountCategories';
import { useAccountsEditing } from './accounts/useAccountsEditing';
import AccountsToolbarRight from './accounts/components/AccountsToolbarRight';
import NewAccountModal from './accounts/components/NewAccountModal';
import EditAccountModal from './accounts/components/EditAccountModal';
import DeleteAccountModal from './accounts/components/DeleteAccountModal';
import BalanceAdjustmentModal from './accounts/components/BalanceAdjustmentModal';
import DebtDetailsModal from './accounts/components/DebtDetailsModal';

const AccountsPage = () => {
  const navigate = useNavigate();
  const {
    data: accounts = [],
    loading,
    error,
    refetch: loadAccounts,
    setData: setAccounts
  } = useApi(async () => (await getAccounts()).accounts, {
    errorMessage: 'Nie udało się wczytać kont.'
  });
  const [searchQuery, setSearchQuery] = useState('');
  const [showNewAccountModal, setShowNewAccountModal] = useState(false);
  const [showClosedAccounts, setShowClosedAccounts] = useState(false);
  const [editingAccountId, setEditingAccountId] = useState<string | null>(null);
  const [deletingAccountId, setDeletingAccountId] = useState<string | null>(null);
  const [debtAccountId, setDebtAccountId] = useState<string | null>(null);
  const [sortOption, setSortOption] = useState<SortOption>('name_asc');

  const { categoryLoadError, categories: accountCategories, ensureAccountCategory, getCategoryLabel } =
    useAccountCategories(accounts);

  const accountKindOptions = ACCOUNT_KIND_OPTIONS;

  const {
    inlineError,
    renderCell,
    renderCategoryCell,
    renderBalanceCell,
    renderKindCell,
    renderBudgetCell,
    handleGridDoubleClick,
    balanceAdjustmentModal,
    adjustingBalance,
    handleCancelBalanceAdjustment,
    handleConfirmBalanceAdjustment
  } = useAccountsEditing({
    accounts,
    setAccounts,
    loadAccounts,
    accountKindOptions,
    categories: accountCategories,
    getCategoryLabel,
    ensureAccountCategory
  });

  const handleAccountCreated = useCallback(async () => {
    await loadAccounts();
  }, [loadAccounts]);

  // Po zapisie w modalu odświeżamy listę i pilnujemy, by zarchiwizowane konto od
  // razu było widoczne w archiwum (użytkownik właśnie je zarchiwizował).
  const handleAccountSaved = useCallback(async () => {
    const updated = await loadAccounts();
    if (updated?.some((account) => account.is_closed)) {
      setShowClosedAccounts(true);
    }
  }, [loadAccounts]);

  // Trwałe usunięcie konta wypada z listy — po skasowaniu wystarczy odświeżyć dane.
  const handleAccountDeleted = useCallback(async () => {
    await loadAccounts();
  }, [loadAccounts]);

  // "Usuń konto" w modalu edycji zamyka edycję i otwiera osobne, potwierdzane
  // okno kasowania dla tego samego konta.
  const handleRequestDelete = useCallback(() => {
    setDeletingAccountId(editingAccountId);
    setEditingAccountId(null);
  }, [editingAccountId]);

  const accountGridColumns: readonly MoneyColumn<Account>[] = [
    {
      key: 'actions',
      name: '',
      width: 124,
      minWidth: 124,
      maxWidth: 124,
      resizable: false,
      headerCellClass: 'text-center',
      cellClass: 'text-center',
      renderCell: ({ row }) => (
        <div className="accounts-edit-cell">
          <button
            type="button"
            className="btn btn-ghost-secondary btn-icon btn-sm accounts-edit-btn is-focusable"
            aria-label={`Edytuj konto ${row.name}`}
            title="Edytuj konto"
            onClick={(event) => {
              event.stopPropagation();
              setEditingAccountId(row.id);
            }}
          >
            <IconPencil size={16} aria-hidden="true" />
          </button>
          <button
            type="button"
            className="btn btn-ghost-secondary btn-icon btn-sm accounts-edit-btn is-focusable"
            aria-label={`Importuj wyciąg CSV na konto ${row.name}`}
            title="Importuj wyciąg CSV"
            onClick={(event) => {
              event.stopPropagation();
              navigate(`/bank-import?account=${encodeURIComponent(row.id)}`);
            }}
          >
            <IconFileSpreadsheet size={16} aria-hidden="true" />
          </button>
          {/* Tylko zobowiązania: karty kredytowe mają własną ścieżkę (kategoria
              spłaty), a dla aktywów nie ma czego prognozować. */}
          {isLiabilityAccountKind(row.account_kind) && (
            <button
              type="button"
              className="btn btn-ghost-secondary btn-icon btn-sm accounts-edit-btn is-focusable"
              aria-label={`Pokaż szczegóły długu: ${row.name}`}
              title="Szczegóły długu"
              onClick={(event) => {
                event.stopPropagation();
                setDebtAccountId(row.id);
              }}
            >
              <IconReceipt2 size={16} aria-hidden="true" />
            </button>
          )}
        </div>
      )
    },
    {
      key: 'name',
      name: 'Nazwa',
      width: 240,
      renderCell: ({ row }) => renderCell(row, 'name')
    },
    {
      key: 'category',
      name: 'Tag',
      width: 260,
      renderCell: ({ row }) => renderCategoryCell(row)
    },
    {
      key: 'currency',
      name: 'Waluta',
      width: 120,
      renderCell: ({ row }) => renderCell(row, 'currency')
    },
    {
      key: 'balance',
      name: 'Saldo',
      width: 200,
      isMoney: true,
      renderCell: ({ row }) => renderBalanceCell(row)
    },
    {
      key: 'kind',
      name: 'Typ',
      width: 210,
      headerCellClass: 'text-center',
      cellClass: 'text-center',
      renderCell: ({ row }) => renderKindCell(row)
    },
    {
      key: 'budget',
      name: 'Budżet',
      width: 140,
      headerCellClass: 'text-center',
      cellClass: 'text-center',
      renderCell: ({ row }) => renderBudgetCell(row)
    },
    {
      key: 'comment',
      name: 'Komentarz',
      renderCell: ({ row }) => renderCell(row, 'comment')
    }
  ];

  const getAccountRowId = useCallback((account: Account) => account.id, []);

  const filteredAccounts = accounts.filter((account) => {
    if (!searchQuery.trim()) {
      return true;
    }
    const needle = searchQuery.toLowerCase();
    const normalizedNeedle = normalizeCategoryLabel(searchQuery);
    const kindLabel = getAccountKindLabel(account.account_kind);
    return (
      account.name.toLowerCase().includes(needle) ||
      account.currency.toLowerCase().includes(needle) ||
      (account.comment ?? '').toLowerCase().includes(needle) ||
      kindLabel.toLowerCase().includes(needle) ||
      normalizeCategoryLabel(getCategoryLabel(account.category_id)).includes(normalizedNeedle)
    );
  });

  const sortedAccounts = useMemo(() => {
    const accountsToSort = filteredAccounts.slice();
    if (sortOption === 'name_asc' || sortOption === 'name_desc') {
      accountsToSort.sort((a, b) => {
        const nameDiff = a.name.localeCompare(b.name, 'pl');
        return sortOption === 'name_asc' ? nameDiff : -nameDiff;
      });
    } else {
      accountsToSort.sort((a, b) => {
        const balanceDiff =
          sortOption === 'balance_desc' ? b.balance_cents - a.balance_cents : a.balance_cents - b.balance_cents;
        if (balanceDiff !== 0) {
          return balanceDiff;
        }
        return a.name.localeCompare(b.name, 'pl');
      });
    }
    return accountsToSort;
  }, [filteredAccounts, sortOption]);

  const openAccounts = sortedAccounts.filter((account) => !account.is_closed);
  const closedAccounts = sortedAccounts.filter((account) => account.is_closed);
  const openAccountsHeight = getAccountsGridHeight(openAccounts.length);
  const closedAccountsHeight = getAccountsGridHeight(closedAccounts.length);

  const noResults = !loading && !error && filteredAccounts.length === 0;
  const activeAccounts = useMemo(
    () => accounts.filter((account) => !account.is_closed),
    [accounts]
  );
  const editingAccount = useMemo(
    () => (editingAccountId ? accounts.find((account) => account.id === editingAccountId) ?? null : null),
    [accounts, editingAccountId]
  );
  const deletingAccount = useMemo(
    () => (deletingAccountId ? accounts.find((account) => account.id === deletingAccountId) ?? null : null),
    [accounts, deletingAccountId]
  );
  const debtAccount = useMemo(
    () => (debtAccountId ? accounts.find((account) => account.id === debtAccountId) ?? null : null),
    [accounts, debtAccountId]
  );

  return (
    <div className="page accounts-page">
      <div className="page-shell">
        <div className="d-print-none accounts-page__header">
          <div className="accounts-page__title-wrap">
            <h2 className="page-title accounts-page__title">Konta</h2>
            <p className="accounts-page__subtitle">
              {activeAccounts.length} aktywne / {closedAccounts.length} zarchiwizowane
            </p>
          </div>
          <div className="accounts-page__actions">
            <Button icon={<IconPlus size={16} aria-hidden="true" />} onClick={() => setShowNewAccountModal(true)}>
              Nowe konto
            </Button>
          </div>
        </div>

        <div className="page-body page-stack accounts-page__body">
          <section className="card accounts-section-card">
            <div className="accounts-section-card__header">
              <AccountsToolbarRight
                searchQuery={searchQuery}
                setSearchQuery={setSearchQuery}
                sortOption={sortOption}
                setSortOption={setSortOption}
              />
            </div>

            {inlineError && (
              <div className="accounts-section-card__alert">
                <div className="alert alert-danger alert-important d-flex align-items-center gap-2 mb-0" role="alert">
                  <IconAlertCircle size={18} aria-hidden="true" />
                  <div>{inlineError}</div>
                </div>
              </div>
            )}

            {categoryLoadError && (
              <div className="accounts-section-card__alert">
                <div className="alert alert-warning alert-important d-flex align-items-center gap-2 mb-0" role="alert">
                  <IconAlertCircle size={18} aria-hidden="true" />
                  <div>{categoryLoadError}</div>
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
                    aria-label="Lista kont"
                    className="accounts-grid"
                    columns={accountGridColumns}
                    rows={openAccounts}
                    rowKeyGetter={getAccountRowId}
                    onCellDoubleClick={handleGridDoubleClick}
                    height={openAccountsHeight}
                    rowHeight={ACCOUNT_GRID_ROW_HEIGHT}
                    headerRowHeight={ACCOUNT_GRID_HEADER_HEIGHT}
                    noRowsFallback={
                      <div className="data-grid__empty text-secondary text-center">
                        Brak aktywnych kont w wynikach filtrowania.
                      </div>
                    }
                  />
                ) : (
                  <div className="accounts-empty-state">
                    <div className="accounts-empty-state__title">Brak wyników</div>
                    <p className="accounts-empty-state__subtitle">
                      Nie znaleziono kont dla podanego filtra. Spróbuj innej frazy wyszukiwania.
                    </p>
                  </div>
                )}
              </div>
            )}
          </section>

          {closedAccounts.length > 0 && (
            <section className="card accounts-section-card accounts-section-card--closed">
              <div className="accounts-section-card__header accounts-section-card__header--closed">
                <div className="accounts-section-card__header-copy">
                  <div className="accounts-section-card__eyebrow">Archiwum</div>
                  <h3 className="accounts-section-card__title">Zarchiwizowane konta</h3>
                  <p className="accounts-section-card__hint">
                    Łącznie: {closedAccounts.length}. Rozwiń sekcję, jeśli chcesz przywrócić konto.
                  </p>
                </div>
                <button
                  type="button"
                  className="btn btn-outline-secondary btn-sm is-focusable"
                  onClick={() => setShowClosedAccounts((prev) => !prev)}
                >
                  {showClosedAccounts ? 'Ukryj' : 'Pokaż'}
                </button>
              </div>
              {showClosedAccounts && (
                <div className="accounts-section-card__table">
                  <DataTable
                    aria-label="Lista zarchiwizowanych kont"
                    className="accounts-grid accounts-grid--archived"
                    columns={accountGridColumns}
                    rows={closedAccounts}
                    rowKeyGetter={getAccountRowId}
                    onCellDoubleClick={handleGridDoubleClick}
                    height={closedAccountsHeight}
                    rowHeight={ACCOUNT_GRID_ROW_HEIGHT}
                    headerRowHeight={ACCOUNT_GRID_HEADER_HEIGHT}
                    noRowsFallback={
                      <div className="data-grid__empty text-secondary text-center">
                        Brak zarchiwizowanych kont w wynikach filtrowania.
                      </div>
                    }
                  />
                </div>
              )}
            </section>
          )}
        </div>
      </div>
      {showNewAccountModal && (
        <NewAccountModal
          onClose={() => setShowNewAccountModal(false)}
          onCreated={handleAccountCreated}
          categories={accountCategories}
          ensureAccountCategory={ensureAccountCategory}
          accountKindOptions={accountKindOptions}
        />
      )}
      {editingAccount && (
        <EditAccountModal
          account={editingAccount}
          onClose={() => setEditingAccountId(null)}
          onSaved={handleAccountSaved}
          onRequestDelete={handleRequestDelete}
          categories={accountCategories}
          ensureAccountCategory={ensureAccountCategory}
          getCategoryLabel={getCategoryLabel}
          accountKindOptions={accountKindOptions}
        />
      )}
      {deletingAccount && (
        <DeleteAccountModal
          account={deletingAccount}
          onClose={() => setDeletingAccountId(null)}
          onDeleted={handleAccountDeleted}
        />
      )}
      {debtAccount && <DebtDetailsModal account={debtAccount} onClose={() => setDebtAccountId(null)} />}
      {balanceAdjustmentModal && (
        <BalanceAdjustmentModal
          state={balanceAdjustmentModal}
          submitting={adjustingBalance}
          onCancel={handleCancelBalanceAdjustment}
          onConfirm={() => void handleConfirmBalanceAdjustment()}
        />
      )}
    </div>
  );
};

export default AccountsPage;
