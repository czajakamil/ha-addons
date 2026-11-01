// Saldo na koniec miesiąca od pierwszej transakcji konta: neutralna linia z obszarem.
import { ResponsiveLine } from '@nivo/line';
import { Wallet } from 'lucide-react';
import type { Transaction } from '../../../api';
import EmptyState from '../../../components/ui/EmptyState';
import { formatSignedMoney } from '../../../components/ui/Money';
import { copy } from '../../../copy/pl';
import { useMediaQuery } from '../../../hooks/useMediaQuery';
import { useChartTheme } from '../../reports/useChartTheme';
import { buildMonthlyBalances } from '../balanceHistory';

const labels = copy.accounts;
const COMPACT_MONTHS = 6;
const MAX_AXIS_LABELS = 12;

const axisAmount = new Intl.NumberFormat('pl-PL', { maximumFractionDigits: 0 });

type AccountBalanceChartProps = {
  balanceCents: number;
  currency: string;
  transactions: Transaction[];
};

const AccountBalanceChart = ({ balanceCents, currency, transactions }: AccountBalanceChartProps) => {
  const chartTheme = useChartTheme();
  const isCompact = useMediaQuery('(max-width: 699px)');

  const all = buildMonthlyBalances(balanceCents, transactions);
  if (!all.length) return <EmptyState icon={Wallet} message={labels.noHistory} />;

  const points = isCompact ? all.slice(-COMPACT_MONTHS) : all;
  const step = Math.ceil(points.length / MAX_AXIS_LABELS);
  // Etykiety liczone od końca, żeby bieżący miesiąc zawsze był podpisany
  const tickValues = points
    .filter((_, index) => (points.length - 1 - index) % step === 0)
    .map((point) => point.label);

  return (
    <div className="account-details__chart" role="img" aria-label={labels.balanceHistory}>
      <ResponsiveLine
        data={[{ id: 'balance', data: points.map((point) => ({ x: point.label, y: point.balanceCents / 100 })) }]}
        margin={{ top: 8, right: 16, bottom: 28, left: 64 }}
        yScale={{ type: 'linear', min: 'auto', max: 'auto' }}
        curve="monotoneX"
        colors={[chartTheme.muted]}
        lineWidth={2}
        enableArea
        areaOpacity={0.12}
        enablePoints={false}
        enableGridX={false}
        gridYValues={4}
        axisBottom={{ tickSize: 0, tickPadding: 8, tickValues }}
        axisLeft={{ tickSize: 0, tickPadding: 8, tickValues: 4, format: (value) => axisAmount.format(Number(value)) }}
        theme={chartTheme.nivoTheme}
        useMesh
        tooltip={({ point }) => (
          <div className="tooltip-content">
            {String(point.data.x)}: {formatSignedMoney(Number(point.data.y) * 100, currency)}
          </div>
        )}
      />
    </div>
  );
};

export default AccountBalanceChart;
