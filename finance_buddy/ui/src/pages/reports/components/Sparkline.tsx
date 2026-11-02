// Mini-wykres miesięcy w wierszu tabeli wydatków, z kwotą w dymku.
import { useCallback, useState } from 'react';
import { moneyLabel, reportMonthLabel } from '../model';
import type { SpendingMonth } from '../spending';
import ChartTooltip from './ChartTooltip';

const HEIGHT_PX = 20;

type SparklineProps = {
  months: readonly SpendingMonth[];
  values: readonly number[];
  color: string;
};

const Sparkline = ({ months, values, color }: SparklineProps) => {
  const [hover, setHover] = useState<{ index: number; anchor: DOMRect } | null>(null);
  const dismiss = useCallback(() => setHover(null), []);
  const peak = Math.max(...values, 1);
  const hovered = hover ? months[hover.index] : null;

  return (
    <span className="spending-row__spark" aria-hidden="true" onMouseLeave={dismiss}>
      {values.map((value, index) => (
        <span
          key={months[index].month}
          className="spending-row__spark-column"
          onMouseEnter={(event) =>
            setHover({ index, anchor: event.currentTarget.getBoundingClientRect() })
          }
        >
          <span
            style={{
              height: `${Math.max(1, Math.round((value / peak) * HEIGHT_PX))}px`,
              background: color,
              opacity: months[index].partial ? 0.35 : 1
            }}
          />
        </span>
      ))}
      {hover && hovered && (
        <ChartTooltip anchor={hover.anchor} onDismiss={dismiss}>
          <div className="report-tooltip__title--sm">
            {reportMonthLabel(hovered.month)}
            {hovered.partial && <span className="report-tooltip__muted"> · w toku</span>}
          </div>
          <div>{moneyLabel(values[hover.index])}</div>
        </ChartTooltip>
      )}
    </span>
  );
};

export default Sparkline;
