// Strona sklepu: nagłówek z wydatkami i wykres, a pod nimi zakładki Transakcje i Ustawienia.
// Adres przyjmuje id dowolnego sklepu z grupy, bo sama grupa nie ma własnego id.
import { useCallback, useEffect, useMemo, useState } from 'react';
import { Link, useParams, useSearchParams } from 'react-router-dom';
import { ChevronLeft, Pencil, Store as StoreIcon } from 'lucide-react';
import { getAccounts, getAllStoreTransactions, getAllStores, type Store } from '../../api';
import EmptyState from '../../components/ui/EmptyState';
import Money, { formatSignedMoney } from '../../components/ui/Money';
import SkeletonRows from '../../components/ui/SkeletonRows';
import { copy } from '../../copy/pl';
import { useApi } from '../../hooks/useApi';
import AccountNameInput from '../accounts/components/AccountNameInput';
import { maskAccountNumbers } from './cleanup';
import StoreCategory from './components/StoreCategory';
import StoreSettings from './components/StoreSettings';
import StoreSpendChart from './components/StoreSpendChart';
import StoreTransactions from './components/StoreTransactions';
import { aggregateStoreStats, buildMonthlySpend, buildStoreGroups } from './groups';
import { useStoreCategories } from './useStoreCategories';
import { useStoreGroupSave } from './useStoreGroupSave';

const labels = copy.stores;
const MONTHS_IN_WINDOW = 12;

const StoreDetailsPage = () => {
  const { storeId = '' } = useParams();
  const [searchParams, setSearchParams] = useSearchParams();
  const stores = useApi(getAllStores, { errorMessage: labels.loadFailed });
  const accounts = useApi(async () => (await getAccounts()).accounts);
  const { categories, categoryMap, ensureCategory } = useStoreCategories();
  const [renaming, setRenaming] = useState(false);
  const tab = searchParams.get('tab') === 'settings' ? 'settings' : 'transactions';

  const emptyStats = useMemo(() => new Map(), []);
  const group = useMemo(
    () => buildStoreGroups(stores.data ?? [], emptyStats).find((entry) => entry.members.some((store) => store.id === storeId)),
    [stores.data, emptyStats, storeId]
  );
  const memberKey = group?.members.map((store) => store.id).sort().join(',') ?? '';

  // Wynik niesie klucz grupy, żeby po zmianie składu nie pokazać transakcji poprzedniego
  const transactions = useApi(
    async () => ({ key: memberKey, items: await getAllStoreTransactions(memberKey.split(',')) }),
    { errorMessage: labels.transactionsFailed, immediate: false }
  );
  const { refetch: refetchTransactions } = transactions;
  useEffect(() => {
    if (memberKey) void refetchTransactions();
  }, [memberKey, refetchTransactions]);

  const txList = transactions.data?.key === memberKey ? transactions.data.items : null;
  const stats = useMemo(
    () => (txList ? aggregateStoreStats(txList, group?.members ?? []) : null),
    [txList, group?.members]
  );
  const summary = useMemo(
    () => (group && stats ? buildStoreGroups(group.members, stats)[0] : null),
    [group, stats]
  );
  const memberCounts = useMemo(
    () => (stats ? new Map(Array.from(stats, ([id, stat]) => [id, stat.count])) : null),
    [stats]
  );
  const months = useMemo(() => (txList ? buildMonthlySpend(txList, group?.members ?? []) : null), [txList, group?.members]);

  const applyUpdated = useCallback(
    (updated: Store[]) => {
      const byId = new Map(updated.map((store) => [store.id, store]));
      stores.setData((current) => current?.map((store) => byId.get(store.id) ?? store));
    },
    [stores.setData] // eslint-disable-line react-hooks/exhaustive-deps
  );
  const save = useStoreGroupSave(applyUpdated);

  const setTab = (next: 'transactions' | 'settings') =>
    setSearchParams(next === 'settings' ? { tab: 'settings' } : {}, { replace: true });

  const displayName = group ? maskAccountNumbers(group.name) : '';
  const spent = summary?.spentCents ?? 0;
  const count = summary?.count ?? null;
  const subtitle = group
    ? [count === null ? null : labels.transactionsCount(count), labels.bankNames(group.members.length)]
        .filter(Boolean)
        .join(' · ')
    : '';

  return (
    <div className="page-shell account-details stores-details">
      <Link to="/stores" className="text-button account-details__back">
        <ChevronLeft size={14} aria-hidden="true" />
        {labels.back}
      </Link>
      {!stores.data && stores.loading && <SkeletonRows rows={4} />}
      {stores.error && <p className="accounts-error" role="alert">{stores.error}</p>}
      {stores.data && !group && <EmptyState icon={StoreIcon} message={labels.notFound} />}
      {group && (
        <>
          <header className="account-details__head">
            <div className="stores-details__identity">
              <span className="stores-mark stores-mark--large" aria-hidden="true">
                {displayName.charAt(0).toUpperCase()}
              </span>
              <div className="account-details__identity">
                {renaming ? (
                  <AccountNameInput
                    name={group.name}
                    className="account-details__name-input"
                    ariaLabel={labels.rename(displayName)}
                    tooShortMessage={labels.nameTooShort}
                    onCancel={() => setRenaming(false)}
                    onSave={(name) => {
                      setRenaming(false);
                      // Nazwa grupowa, nie `name`: importer dopasowuje sklepy po `name`
                      void save(group.members.map((store) => ({ store, payload: { group_name: name } })));
                    }}
                  />
                ) : (
                  <h1 className="page-heading__title account-details__name">
                    <button
                      type="button"
                      className="account-details__name-button"
                      aria-label={labels.rename(displayName)}
                      onClick={() => setRenaming(true)}
                    >
                      {displayName}
                      <Pencil size={14} aria-hidden="true" />
                    </button>
                  </h1>
                )}
                <span className="accounts-name__meta stores-details__meta">
                  <StoreCategory categoryId={group.categoryId} categoryMap={categoryMap} emptyLabel={labels.noCategory} />
                  <span>· {subtitle}</span>
                </span>
              </div>
            </div>
            <div className="account-details__balance-block stores-details__spent">
              <span className="stores-muted">{labels.spentTitle}</span>
              {stats ? (
                <>
                  <Money cents={spent} tone="neutral" className="account-details__balance" />
                  <span className="stores-details__average">
                    {labels.averagePerMonth(formatSignedMoney(Math.round(spent / MONTHS_IN_WINDOW)))}
                  </span>
                </>
              ) : (
                <span className="account-details__balance stores-muted">…</span>
              )}
            </div>
          </header>

          {transactions.error && <p className="accounts-error" role="alert">{transactions.error}</p>}
          {months ? <StoreSpendChart months={months} /> : !transactions.error && <SkeletonRows rows={4} />}

          <div className="account-tabs" role="tablist" aria-label={labels.tabs.label}>
            {(['transactions', 'settings'] as const).map((value) => (
              <button
                key={value}
                type="button"
                role="tab"
                id={`store-tab-${value}`}
                aria-selected={tab === value}
                aria-controls={`store-panel-${value}`}
                className={`account-tabs__tab${tab === value ? ' is-active' : ''}`}
                onClick={() => setTab(value)}
              >
                {labels.tabs[value]}
                {value === 'transactions' && count !== null && <span className="stores-muted"> {count}</span>}
              </button>
            ))}
          </div>

          <div role="tabpanel" id={`store-panel-${tab}`} aria-labelledby={`store-tab-${tab}`}>
            {tab === 'transactions' ? (
              txList ? (
                <StoreTransactions
                  transactions={txList}
                  members={group.members}
                  accounts={accounts.data ?? []}
                  categoryMap={categoryMap}
                />
              ) : (
                !transactions.error && <SkeletonRows rows={6} />
              )
            ) : (
              <StoreSettings
                group={summary ?? group}
                transactionCount={count}
                memberCounts={memberCounts}
                categories={categories}
                categoryPath={(group.categoryId && categoryMap.get(group.categoryId)?.path) || ''}
                ensureCategory={ensureCategory}
                save={save}
              />
            )}
          </div>
        </>
      )}
    </div>
  );
};

export default StoreDetailsPage;
