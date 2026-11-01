// Szczegóły konta: saldo i jego historia, odtworzona wstecz z transakcji konta.
import { useMemo } from 'react';
import { Link, useParams } from 'react-router-dom';
import { ResponsiveBar } from '@nivo/bar';
import { ChevronLeft, Wallet } from 'lucide-react';
import { getAccounts, getTransactions, type Transaction } from '../../api';
import EmptyState from '../../components/ui/EmptyState';
import Money, { formatSignedMoney } from '../../components/ui/Money';
import SkeletonRows from '../../components/ui/SkeletonRows';
import { copy } from '../../copy/pl';
import { useApi } from '../../hooks/useApi';
import { getAccountKindLabel } from './model';
import { useChartTheme } from '../reports/useChartTheme';
import { chartColor } from '../../utils/tokens';

const labels = copy.accounts;
const PAGE_SIZE = 200;
const MAX_PAGES = 25;
const MONTHS_SHOWN = 12;

const fetchAllTransactions = async (accountId: string) => {
  const all: Transaction[] = [];
  for (let page = 0; page < MAX_PAGES; page += 1) {
    const batch = await getTransactions({ account_id: accountId, offset: page * PAGE_SIZE, limit: PAGE_SIZE });
    all.push(...batch);
    if (batch.length < PAGE_SIZE) break;
  }
  return all;
};

// Saldo na koniec każdego z ostatnich miesięcy: od bieżącego salda odejmujemy późniejsze transakcje
const buildMonthlyBalances = (balanceCents: number, transactions: Transaction[]) => {
  const now = new Date();
  const points: { month: string; balance: number }[] = [];
  for (let offset = 0; offset < MONTHS_SHOWN; offset += 1) {
    const monthStart = new Date(now.getFullYear(), now.getMonth() - offset, 1);
    const end = new Date(monthStart.getFullYear(), monthStart.getMonth() + 1, 1);
    const later = transactions
      .filter((tx) => tx.date && Date.parse(tx.date) >= end.getTime())
      .reduce((sum, tx) => sum + tx.total_cents, 0);
    points.unshift({
      month: monthStart.toLocaleDateString('pl-PL', { month: 'short' }),
      balance: (balanceCents - later) / 100
    });
  }
  return points;
};

const AccountDetailsPage = () => {
  const { accountId = '' } = useParams();
  const chartTheme = useChartTheme();
  const accounts = useApi(async () => (await getAccounts()).accounts, { errorMessage: labels.loadFailed });
  const transactions = useApi(() => fetchAllTransactions(accountId));
  const account = accounts.data?.find((entry) => entry.id === accountId);

  const points = useMemo(
    () => (account && transactions.data ? buildMonthlyBalances(account.balance_cents, transactions.data) : []),
    [account, transactions.data]
  );

  return (
    <div className="page-shell account-details">
      <Link to="/accounts" className="text-button account-details__back">
        <ChevronLeft size={14} aria-hidden="true" />
        {labels.back}
      </Link>
      {accounts.loading && <SkeletonRows rows={4} />}
      {!accounts.loading && !account && <EmptyState icon={Wallet} message={labels.notFound} />}
      {account && (
        <>
          <header className="account-details__head">
            <div>
              <h1 className="page-heading__title">{account.name}</h1>
              <span className="accounts-name__meta">
                {getAccountKindLabel(account.account_kind)} · {account.currency}
              </span>
            </div>
            <Money
              cents={account.balance_cents}
              currency={account.currency}
              tone={account.balance_cents < 0 ? 'negative' : 'neutral'}
              className="account-details__balance"
            />
          </header>

          <section className="account-details__chart-card">
            <div className="account-details__chart-head">
              <h2 className="home-block__title">{labels.balanceHistory}</h2>
              <Link to={`/transactions?account=${encodeURIComponent(account.id)}`} className="text-button">
                {labels.showTransactions}
              </Link>
            </div>
            {transactions.loading ? (
              <SkeletonRows rows={4} />
            ) : !transactions.data?.length ? (
              <EmptyState icon={Wallet} message={labels.noHistory} />
            ) : (
              <div className="account-details__chart" role="img" aria-label={labels.balanceHistory}>
                <ResponsiveBar
                  data={points}
                  keys={['balance']}
                  indexBy="month"
                  margin={{ top: 8, right: 8, bottom: 28, left: 72 }}
                  padding={0.4}
                  borderRadius={4}
                  colors={[chartColor(0)]}
                  enableLabel={false}
                  axisBottom={{ tickSize: 0, tickPadding: 8 }}
                  axisLeft={{ tickSize: 0, tickPadding: 8, tickValues: 4 }}
                  gridYValues={4}
                  theme={chartTheme.nivoTheme}
                  tooltip={({ value, indexValue }) => (
                    <div className="tooltip-content">
                      {indexValue}: {formatSignedMoney(Number(value) * 100, account.currency)}
                    </div>
                  )}
                />
              </div>
            )}
          </section>
        </>
      )}
    </div>
  );
};

export default AccountDetailsPage;
