// Strona konta: nagłówek z saldem i wykres, a pod nimi zakładki Transakcje i Ustawienia.
import { useCallback, useEffect, useState } from 'react';
import { Link, useParams, useSearchParams } from 'react-router-dom';
import { ChevronLeft, Pencil, Wallet } from 'lucide-react';
import { type Account, getAccounts, getTransactions, type Transaction } from '../../api';
import EmptyState from '../../components/ui/EmptyState';
import Money from '../../components/ui/Money';
import SkeletonRows from '../../components/ui/SkeletonRows';
import { copy } from '../../copy/pl';
import { useApi } from '../../hooks/useApi';
import TransactionsPage from '../TransactionsPage';
import AccountBalanceChart from './components/AccountBalanceChart';
import AccountNameInput from './components/AccountNameInput';
import AccountSettings from './components/AccountSettings';
import ReconcileBalance from './components/ReconcileBalance';
import { getAccountKindLabel } from './model';
import { useAccountCategories } from './useAccountCategories';
import { useAccountSave } from './useAccountSave';

const labels = copy.accounts;
const PAGE_SIZE = 200;
const MAX_PAGES = 25;

const fetchAllTransactions = async (accountId: string) => {
  const all: Transaction[] = [];
  for (let page = 0; page < MAX_PAGES; page += 1) {
    const batch = await getTransactions({ account_id: accountId, offset: page * PAGE_SIZE, limit: PAGE_SIZE });
    all.push(...batch);
    if (batch.length < PAGE_SIZE) break;
  }
  return all;
};

const AccountDetailsPage = () => {
  const { accountId = '' } = useParams();
  const [searchParams, setSearchParams] = useSearchParams();
  const accounts = useApi(async () => (await getAccounts()).accounts, { errorMessage: labels.loadFailed });
  const transactions = useApi(() => fetchAllTransactions(accountId));
  const { categories, ensureAccountCategory, getCategoryLabel } = useAccountCategories(accounts.data ?? []);
  const [renaming, setRenaming] = useState(false);
  const [reloadKey, setReloadKey] = useState(0);
  // Lista kont otwiera tu dymek uzgadniania; parametr znika, żeby odświeżenie go nie otwierało.
  const [openReconcile] = useState(() => searchParams.has('reconcile'));
  const account = accounts.data?.find((entry) => entry.id === accountId);
  const tab = searchParams.get('tab') === 'settings' ? 'settings' : 'transactions';

  useEffect(() => {
    if (!openReconcile) return;
    setSearchParams(
      (current) => {
        current.delete('reconcile');
        return current;
      },
      { replace: true }
    );
  }, [openReconcile, setSearchParams]);

  const setTab = (next: 'transactions' | 'settings') =>
    setSearchParams(next === 'settings' ? { tab: 'settings' } : {}, { replace: true });

  const applyUpdated = useCallback(
    (updated: Account) =>
      accounts.setData((current) => current?.map((entry) => (entry.id === updated.id ? updated : entry))),
    [accounts.setData] // eslint-disable-line react-hooks/exhaustive-deps
  );
  const save = useAccountSave(applyUpdated);

  // Odświeżenie salda i wykresu po zmianie transakcji; lista trzyma własne dane.
  const refreshBalance = useCallback(async () => {
    await Promise.all([accounts.refetch(), transactions.refetch()]);
  }, [accounts.refetch, transactions.refetch]); // eslint-disable-line react-hooks/exhaustive-deps

  const handleReconciled = useCallback(async () => {
    await refreshBalance();
    setReloadKey((key) => key + 1);
  }, [refreshBalance]);

  return (
    <div className="page-shell account-details">
      <Link to="/accounts" className="text-button account-details__back">
        <ChevronLeft size={14} aria-hidden="true" />
        {labels.back}
      </Link>
      {!accounts.data && accounts.loading && <SkeletonRows rows={4} />}
      {!accounts.loading && !account && <EmptyState icon={Wallet} message={labels.notFound} />}
      {account && (
        <>
          <header className="account-details__head">
            <div className="account-details__identity">
              {renaming ? (
                <AccountNameInput
                  name={account.name}
                  className="account-details__name-input"
                  onCancel={() => setRenaming(false)}
                  onSave={(name) => {
                    setRenaming(false);
                    void save(account, { name });
                  }}
                />
              ) : (
                <h1 className="page-heading__title account-details__name">
                  <button
                    type="button"
                    className="account-details__name-button"
                    aria-label={labels.rename(account.name)}
                    onClick={() => setRenaming(true)}
                  >
                    {account.name}
                    <Pencil size={14} aria-hidden="true" />
                  </button>
                </h1>
              )}
              <span className="accounts-name__meta">
                {getAccountKindLabel(account.account_kind)} · {account.currency}
              </span>
            </div>
            <div className="account-details__balance-block">
              <Money
                cents={account.balance_cents}
                currency={account.currency}
                tone={account.balance_cents < 0 ? 'negative' : 'neutral'}
                className="account-details__balance"
              />
              <ReconcileBalance account={account} initiallyOpen={openReconcile} onReconciled={handleReconciled} />
            </div>
          </header>

          <section className="account-details__chart-card">
            <div className="account-details__chart-head">
              <h2 className="home-block__title">{labels.balanceHistory}</h2>
            </div>
            {!transactions.data ? (
              <SkeletonRows rows={4} />
            ) : (
              <AccountBalanceChart
                balanceCents={account.balance_cents}
                currency={account.currency}
                transactions={transactions.data}
              />
            )}
          </section>

          <div className="account-tabs" role="tablist" aria-label={labels.tabs.label}>
            {(['transactions', 'settings'] as const).map((value) => (
              <button
                key={value}
                type="button"
                role="tab"
                id={`account-tab-${value}`}
                aria-selected={tab === value}
                aria-controls={`account-panel-${value}`}
                className={`account-tabs__tab${tab === value ? ' is-active' : ''}`}
                onClick={() => setTab(value)}
              >
                {labels.tabs[value]}
              </button>
            ))}
          </div>

          <div
            role="tabpanel"
            id={`account-panel-${tab}`}
            aria-labelledby={`account-tab-${tab}`}
          >
            {tab === 'transactions' ? (
              <TransactionsPage
                scope={{
                  accountId: account.id,
                  balanceCents: account.balance_cents,
                  currency: account.currency,
                  reloadKey,
                  onChanged: refreshBalance
                }}
              />
            ) : (
              <AccountSettings
                account={account}
                categories={categories}
                tagLabel={getCategoryLabel(account.category_id)}
                ensureCategory={ensureAccountCategory}
                save={save}
              />
            )}
          </div>
        </>
      )}
    </div>
  );
};

export default AccountDetailsPage;
