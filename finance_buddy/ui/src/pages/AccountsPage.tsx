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
import { userComment } from '../utils/transactionLabels';
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
import { useAccountSave } from './accounts/useAccountSave';
import AccountsToolbarRight from './accounts/components/AccountsToolbarRight';
import NewAccountModal from './accounts/components/NewAccountModal';
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
  const { activity, loaded: activityLoaded } = useAccountActivity(accounts);
  const [debtAccountId, setDebtAccountId] = useState<string | null>(null);
  const [sortOption, setSortOption] = useState<SortOption>('name_asc');

  const { categoryLoadError, categories: accountCategories, ensureAccountCategory, getCategoryLabel } =
    useAccountCategories(accounts);

  const accountKindOptions = ACCOUNT_KIND_OPTIONS;

  const applyUpdated = useCallback(
    (updated: Account) =>
      setAccounts((current) => current?.map((account) => (account.id === updated.id ? updated : account))),
    [setAccounts]
  );
  const saveAccount = useAccountSave(applyUpdated);

  const handleAccountCreated = useCallback(async () => {
    await loadAccounts();
  }, [loadAccounts]);

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
      (userComment(account.comment) ?? '').toLowerCase().includes(needle) ||
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
      onOpen={(account) => navigate(`/accounts/${account.id}`)}
      onSettings={(account) => navigate(`/accounts/${account.id}?tab=settings`)}
      onReconcile={(account) => navigate(`/accounts/${account.id}?reconcile=1`)}
      onImport={(account) => navigate(`/bank-import?account=${encodeURIComponent(account.id)}`)}
      onToggleArchived={(account) => {
        if (!account.is_closed) setShowClosedAccounts(true);
        void saveAccount(
          account,
          { is_closed: !account.is_closed },
          { message: account.is_closed ? labels.settings.restored : labels.settings.archived }
        );
      }}
      onRename={(account, name) => void saveAccount(account, { name })}
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

  // Lista czeka na aktywność, żeby puste konta nie mignęły przed ukryciem.
  const listPending = loading || (!error && accounts.length > 0 && !activityLoaded);
  const noResults = !listPending && !error && filteredAccounts.length === 0;
  const activeAccounts = useMemo(
    () => accounts.filter((account) => !account.is_closed),
    [accounts]
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

      {[categoryLoadError, !loading && error].filter(Boolean).map((message) => (
        <p key={String(message)} className="accounts-error" role="alert">
          {message}
        </p>
      ))}

      {listPending && <SkeletonRows rows={6} />}
      {noResults && (
        <EmptyState icon={Wallet} message={searchQuery.trim() ? labels.empty : labels.emptyAll} />
      )}

      {!listPending && !error && (
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
      {debtAccount && <DebtDetailsModal account={debtAccount} onClose={() => setDebtAccountId(null)} />}
    </div>
  );
};

export default AccountsPage;
