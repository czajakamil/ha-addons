// Struktura kwot: posortowana lista z neutralnymi paskami, jak w Strukturze wydatków.
import type { CSSProperties } from 'react';
import { moneyLabel, percentLabel } from '../model';

export type ReportBarRow = {
  key: string;
  name: string;
  total: number;
  percentage: number;
  onSelect?: () => void;
};

type ReportBarListProps = {
  title: string;
  rows: readonly ReportBarRow[];
};

const ReportBarList = ({ title, rows }: ReportBarListProps) => (
  <div className="breakdown-list">
    <div className="breakdown-list__header">
      <span>{title}</span>
      <span>Kwota</span>
    </div>
    <ul className="breakdown-list__items">
      {rows.map((row) => {
        const percent = Math.min(Math.max(row.percentage * 100, 0), 100);
        return (
          <li
            key={row.key}
            className={`breakdown-list__item${row.onSelect ? ' is-clickable' : ''}`}
            role={row.onSelect ? 'button' : undefined}
            tabIndex={row.onSelect ? 0 : undefined}
            onClick={row.onSelect}
            onKeyDown={(event) => {
              if (row.onSelect && (event.key === 'Enter' || event.key === ' ')) {
                event.preventDefault();
                row.onSelect();
              }
            }}
            style={{ ['--bar-percent' as const]: `${percent > 0 ? Math.max(percent, 2) : 0}%` } as CSSProperties}
          >
            <div className="breakdown-list__row">
              <div className="breakdown-list__name-row">
                <span className="breakdown-list__name">{row.name}</span>
              </div>
              <div className={`breakdown-list__amount${row.total < 0 ? ' is-negative' : ''}`}>
                {moneyLabel(row.total)}
              </div>
            </div>
            <div className="breakdown-list__bar-row">
              <div className="breakdown-list__bar-wrap">
                <div className="breakdown-list__bar" aria-hidden>
                  <span className="breakdown-list__bar-fill" />
                </div>
              </div>
              <div className="breakdown-list__percent">{percentLabel(row.percentage)}</div>
            </div>
          </li>
        );
      })}
    </ul>
  </div>
);

export default ReportBarList;
