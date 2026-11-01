// Saldo na koniec miesiąca od pierwszej transakcji konta: neutralna linia z obszarem.
import { Wallet } from 'lucide-react';
import type { Transaction } from '../../../api';
import EmptyState from '../../../components/ui/EmptyState';
import { formatSignedMoney } from '../../../components/ui/Money';
import { copy } from '../../../copy/pl';
import MonthLineChart from '../../reports/components/MonthLineChart';
import { buildMonthlyBalances } from '../balanceHistory';

const labels = copy.accounts;

type AccountBalanceChartProps = {
  balanceCents: number;
  currency: string;
  transactions: Transaction[];
};

const AccountBalanceChart = ({ balanceCents, currency, transactions }: AccountBalanceChartProps) => {
  const all = buildMonthlyBalances(balanceCents, transactions);
  if (!all.length) return <EmptyState icon={Wallet} message={labels.noHistory} />;

  return (
    <MonthLineChart
      className="account-details__chart"
      ariaLabel={labels.balanceHistory}
      points={all.map((point) => ({ label: point.label, cents: point.balanceCents }))}
      renderTooltip={(point) => `${point.label}: ${formatSignedMoney(point.cents, currency)}`}
    />
  );
};

export default AccountBalanceChart;
