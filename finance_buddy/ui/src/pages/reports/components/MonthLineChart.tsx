// Stan na koniec miesiąca: neutralna linia z obszarem.
import type { ReactNode } from 'react';
import { ResponsiveLine } from '@nivo/line';
import { useVisibleMonths } from '../../../hooks/useVisibleMonths';
import { useChartTheme } from '../useChartTheme';

const MAX_AXIS_LABELS = 12;

const axisAmount = new Intl.NumberFormat('pl-PL', { maximumFractionDigits: 0, useGrouping: 'always' as unknown as boolean });

export type MonthLinePoint = { label: string; cents: number };

type MonthLineChartProps<T extends MonthLinePoint> = {
  points: readonly T[];
  ariaLabel: string;
  className: string;
  renderTooltip: (point: T) => ReactNode;
};

const MonthLineChart = <T extends MonthLinePoint>({
  points,
  ariaLabel,
  className,
  renderTooltip
}: MonthLineChartProps<T>) => {
  const chartTheme = useChartTheme();
  const visible = useVisibleMonths(points);
  const step = Math.ceil(visible.length / MAX_AXIS_LABELS);
  // Etykiety liczone od końca, żeby bieżący miesiąc zawsze był podpisany
  const tickValues = visible
    .filter((_, index) => (visible.length - 1 - index) % step === 0)
    .map((point) => point.label);

  return (
    <div className={className} role="img" aria-label={ariaLabel}>
      <ResponsiveLine
        data={[{ id: 'balance', data: visible.map((point) => ({ x: point.label, y: point.cents / 100 })) }]}
        margin={{ top: 8, right: 16, bottom: 28, left: 64 }}
        yScale={{ type: 'linear', min: 'auto', max: 'auto' }}
        curve="monotoneX"
        colors={[chartTheme.muted]}
        lineWidth={2}
        enableArea
        areaOpacity={0.12}
        // Pojedynczy miesiąc nie narysuje linii
        enablePoints={visible.length === 1}
        enableGridX={false}
        gridYValues={4}
        axisBottom={{ tickSize: 0, tickPadding: 8, tickValues }}
        axisLeft={{ tickSize: 0, tickPadding: 8, tickValues: 4, format: (value) => axisAmount.format(Number(value)) }}
        theme={chartTheme.nivoTheme}
        useMesh
        tooltip={({ point }) => {
          const source = visible.find((entry) => entry.label === String(point.data.x));
          return source ? <div className="tooltip-content">{renderTooltip(source)}</div> : null;
        }}
      />
    </div>
  );
};

export default MonthLineChart;
