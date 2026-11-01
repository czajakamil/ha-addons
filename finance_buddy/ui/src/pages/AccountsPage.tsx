import { useCallback, useMemo, useState } from 'react';
import { Plus, Wallet } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import { Account, getAccounts } from '../api';
import Button from '../components/Button';
import EmptyState from '../components/ui/EmptyState';
import Money from '../components/ui/Money';
import PageHeader from '../components/ui/PageHeader';
import SkeletonRows from '../components/ui/SkeletonRows';
import { copy } from '../copy/pl';
import { useApi } from '../hooks/useApi';
import {
  ACCOUNT_KIND_OPTIONS,
  getAccountKindLabel,
  isDormantAccount,
  normalizeCategoryLabel,
  sumInMainCurrency,
  type SortOption
} from './accounts/model';
import AccountsTable from './accounts/components/AccountsTable';
import { useAccountActivity } from './accounts/useAccountActivity';
import { useAccountCategories } from './accounts/useAccountCategories';
import { useAccountsEditing } from './accounts/useAccountsEditing';
import AccountsToolbarRight from './accounts/components/AccountsToolbarRight';
import NewAccountModal from './accounts/components/NewAccountModal';
import EditAccountModal from './accounts/components/EditAccountModal';
import DeleteAccountModal from './accounts/components/DeleteAccountModal';
import BalanceAdjustmentModal from './accounts/components/BalanceAdjustmentModal';
import DebtDetailsModal from './accounts/components/DebtDetailsModal';

const labels = copy.accounts;

const AccountsPage = () => {
  const navigate = useNavigate();
  const {
    data: accounts = [],
    loading,
    error,
    refetch: loadAccounts,
    setData: setAccounts
  } = useApi(async () => (await getAccounts()).accounts, {
    errorMessage: copy.accounts.loadFailed
  });
  const [searchQuery, setSearchQuery] = useState('');
  const [showNewAccountModal, setShowNewAccountModal] = useState(false);
  const [showClosedAccounts, setShowClosedAccounts] = useState(false);
  const [showEmptyBudget, setShowEmptyBudget] = useState(false);
  const [showEmptyTracked, setShowEmptyTracked] = useState(false);
  const activity = useAccountActivity(accounts);
  const [editingAccountId, setEditingAccountId] = useState<string | null>(null);
  const [deletingAccountId, setDeletingAccountId] = useState<string | null>(null);
  const [debtAccountId, setDebtAccountId] = useState<string | null>(null);
  const [sortOption, setSortOption] = useState<SortOption>('name_asc');

  const { categoryLoadError, categories: accountCategories, ensureAccountCategory, getCategoryLabel } =
    useAccountCategories(accounts);

  const accountKindOptions = ACCOUNT_KIND_OPTIONS;

  const {
    inlineError,
    renderBalanceCell,
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
  const budgetAccounts = openAccounts.filter((account) => account.is_on_budget);
  const trackedAccounts = openAccounts.filter((account) => !account.is_on_budget);

  const renderTable = (rows: Account[]) => (
    <AccountsTable
      accounts={rows}
      activity={activity}
      renderBalanceCell={renderBalanceCell}
      onOpen={(account) => navigate(`/accounts/${account.id}`)}
      onEdit={(account) => setEditingAccountId(account.id)}
      onChangeBalance={(account) => handleGridDoubleClick({ column: { key: 'balance' }, row: account } as never)}
      onImport={(account) => navigate(`/bank-import?account=${encodeURIComponent(account.id)}`)}
      onDebt={(account) => setDebtAccountId(account.id)}
    />
  );

  const renderGroup = (title: string, rows: Account[], showEmpty: boolean, onToggleEmpty: () => void) => {
    if (!rows.length) return null;
    const visible = rows.filter((account) => !isDormantAccount(account, activity.get(account.id)));
    const dormantCount = rows.length - visible.length;
    return (
      <section className="accounts-group" aria-label={title}>
        <header className="accounts-group__head">
          <h2 className="accounts-group__title">{title}</h2>
          <Money cents={sumInMainCurrency(rows)} tone="neutral" className="accounts-group__sum" />
        </header>
        {renderTable(showEmpty ? rows : visible)}
        {dormantCount > 0 && (
          <button type="button" className="text-button" onClick={onToggleEmpty}>
            {showEmpty ? labels.hideEmpty : labels.showEmpty(dormantCount)}
          </button>
        )}
      </section>
    );
  };

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
    <div className="page-shell accounts-screen">
      <PageHeader
        title={labels.title}
        count={activeAccounts.length}
        actions={
          <Button icon={<Plus size={16} aria-hidden="true" />} onClick={() => setShowNewAccountModal(true)}>
            {labels.newAccount}
          </Button>
        }
      />
      <AccountsToolbarRight
        searchQuery={searchQuery}
        setSearchQuery={setSearchQuery}
        sortOption={sortOption}
        setSortOption={setSortOption}
      />

      {[inlineError, categoryLoadError, !loading && error].filter(Boolean).map((message) => (
        <p key={String(message)} className="accounts-error" role="alert">
          {message}
        </p>
      ))}

      {loading && <SkeletonRows rows={6} />}
      {noResults && (
        <EmptyState icon={Wallet} message={searchQuery.trim() ? labels.empty : labels.emptyAll} />
      )}

      {!loading && !error && (
        <>
          {renderGroup(labels.onBudget, budgetAccounts, showEmptyBudget, () => setShowEmptyBudget((prev) => !prev))}
          {renderGroup(labels.tracked, trackedAccounts, showEmptyTracked, () => setShowEmptyTracked((prev) => !prev))}
          {closedAccounts.length > 0 && (
            <section className="accounts-group" aria-label={labels.archived}>
              <button type="button" className="text-button" onClick={() => setShowClosedAccounts((prev) => !prev)}>
                {showClosedAccounts ? labels.hideArchived : labels.showArchived(closedAccounts.length)}
              </button>
              {showClosedAccounts && renderTable(closedAccounts)}
            </section>
          )}
        </>
      )}

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
