// Podział wydatków za cały okres jako pierścień: te same serie i kolory co słupki
// (największe plus „Pozostałe”), obok lista z kwotą i udziałem.
import { useState } from 'react';
import { pluralPL } from '../../../utils/format';
import { moneyLabel, percentLabel } from '../model';
import type { SpendingModel, SpendingSeries } from '../spending';

const SIZE = 200;
const STROKE = 30;
const RADIUS = (SIZE - STROKE) / 2;
const CIRCUMFERENCE = 2 * Math.PI * RADIUS;
// Przerwa między kawałkami, żeby sąsiednie kolory się nie zlewały
const GAP = 2;

type SpendingDonutProps = {
  model: SpendingModel;
  onSelect?: (series: SpendingSeries) => void;
};

const SpendingDonut = ({ model, onSelect }: SpendingDonutProps) => {
  const [activeKey, setActiveKey] = useState<string | null>(null);
  const series = model.chartSeries.filter((entry) => entry.total > 0);
  const active = series.find((entry) => entry.key === activeKey) ?? null;
  const monthCount = model.months.length;
  const canOpen = (entry: SpendingSeries) => Boolean(onSelect) && !entry.isOther;

  // Kawałek zaczyna się tam, gdzie kończą się wszystkie poprzednie
  const arcs = series.map((entry, index) => ({
    entry,
    start: series.slice(0, index).reduce((acc, prev) => acc + prev.share, 0) * CIRCUMFERENCE,
    length: entry.share * CIRCUMFERENCE
  }));

  const summary = series.map((entry) => `${entry.name}: ${percentLabel(entry.share)}`).join(', ');

  return (
    <div className="spending-donut">
      <div className="spending-donut__chart">
        <svg viewBox={`0 0 ${SIZE} ${SIZE}`} role="img" aria-label={summary}>
          <g transform={`rotate(-90 ${SIZE / 2} ${SIZE / 2})`}>
            {arcs.map(({ entry, start: offset, length }) => (
              <circle
                key={entry.key}
                className={`spending-donut__arc${activeKey && activeKey !== entry.key ? ' is-dimmed' : ''}${canOpen(entry) ? ' is-clickable' : ''}`}
                cx={SIZE / 2}
                cy={SIZE / 2}
                r={RADIUS}
                fill="none"
                stroke={entry.color}
                strokeWidth={STROKE}
                strokeDasharray={`${Math.max(length - (series.length > 1 ? GAP : 0), 0.5)} ${CIRCUMFERENCE}`}
                strokeDashoffset={-offset}
                onMouseEnter={() => setActiveKey(entry.key)}
                onMouseLeave={() => setActiveKey(null)}
                onClick={canOpen(entry) ? () => onSelect?.(entry) : undefined}
              />
            ))}
          </g>
        </svg>
        <div className="spending-donut__center" aria-hidden="true">
          {active ? (
            <>
              <span className="spending-donut__center-label">{active.name}</span>
              <strong className="spending-donut__center-value">{moneyLabel(active.total)}</strong>
              <span className="spending-donut__center-hint">{percentLabel(active.share)} wydatków</span>
            </>
          ) : (
            <>
              <span className="spending-donut__center-label">Razem</span>
              <strong className="spending-donut__center-value">{moneyLabel(model.total)}</strong>
              <span className="spending-donut__center-hint">
                {monthCount} {pluralPL(monthCount, 'miesiąc', 'miesiące', 'miesięcy')}
              </span>
            </>
          )}
        </div>
      </div>
      <ul className="spending-donut__list" aria-label="Udział w wydatkach">
        {series.map((entry) => {
          const content = (
            <>
              <span className="spending-chart__swatch" style={{ background: entry.color }} aria-hidden="true" />
              <span className="spending-donut__name">{entry.name}</span>
              <span className="spending-donut__amount">{moneyLabel(entry.total)}</span>
              <span className="spending-donut__share">{percentLabel(entry.share)}</span>
            </>
          );
          const rowProps = {
            className: `spending-donut__row${activeKey === entry.key ? ' is-active' : ''}`,
            onMouseEnter: () => setActiveKey(entry.key),
            onMouseLeave: () => setActiveKey(null)
          };
          return (
            <li key={entry.key}>
              {canOpen(entry) ? (
                <button
                  type="button"
                  {...rowProps}
                  className={`${rowProps.className} is-clickable`}
                  onFocus={() => setActiveKey(entry.key)}
                  onBlur={() => setActiveKey(null)}
                  onClick={() => onSelect?.(entry)}
                >
                  {content}
                </button>
              ) : (
                <div {...rowProps}>{content}</div>
              )}
            </li>
          );
        })}
      </ul>
    </div>
  );
};

export default SpendingDonut;
