import { CSSProperties, useMemo, useState } from 'react';
import type { Category, NetWorthAccountBalance } from '../../../api';
import {
  OTHER_ACCOUNT_KINDS_LABEL,
  OTHER_CATEGORIES_LABEL,
  groupBreakdownEntries
} from '../model';
import {
  buildKindEntries,
  buildTagEntries,
  findDrillableTagIds,
  tagPath
} from '../netWorthComposition';
import ReportBarList from './ReportBarList';

type NetWorthCompositionProps = {
  balances: readonly NetWorthAccountBalance[];
  categories: readonly Category[];
};

const NetWorthComposition = ({ balances, categories }: NetWorthCompositionProps) => {
  const [view, setView] = useState<'types' | 'tags'>('types');
  const [activeTagId, setActiveTagId] = useState<string | null>(null);

  const drillableIds = useMemo(() => findDrillableTagIds(balances, categories), [balances, categories]);
  const rows = useMemo(() => {
    const entries =
      view === 'types' ? buildKindEntries(balances) : buildTagEntries(balances, categories, activeTagId);
    const percentBase = entries.reduce((sum, entry) => sum + entry.value, 0);
    return groupBreakdownEntries(
      entries,
      percentBase,
      view === 'types' ? OTHER_ACCOUNT_KINDS_LABEL : OTHER_CATEGORIES_LABEL
    );
  }, [activeTagId, balances, categories, view]);

  const handleBack = () => {
    const active = categories.find((category) => category.id === activeTagId);
    setActiveTagId(active?.parent_id ?? null);
  };

  if (!rows.length) return null;

  return (
    <div className="net-worth-composition">
      <div className="spending-trends-controls">
        <div>
          <h4 className="breakdown-layout__title">Skład majątku</h4>
          <p className="report-card__subtitle">Stan na koniec okresu, konta z saldem: {balances.length}.</p>
        </div>
        <div
          className="breakdown-toggle"
          role="group"
          aria-label="Grupuj skład majątku"
          style={{ ['--toggle-index' as const]: view === 'types' ? 0 : 1 } as CSSProperties}
        >
          <span className="breakdown-toggle__thumb" aria-hidden />
          <button
            type="button"
            className={`breakdown-toggle__option${view === 'types' ? ' is-active' : ''}`}
            onClick={() => {
              setView('types');
              setActiveTagId(null);
            }}
          >
            Typy
          </button>
          <button
            type="button"
            className={`breakdown-toggle__option${view === 'tags' ? ' is-active' : ''}`}
            onClick={() => setView('tags')}
          >
            Tagi
          </button>
        </div>
      </div>
      {view === 'tags' && activeTagId && (
        <div className="breakdown-filter">
          <span className="breakdown-filter__label">Ścieżka tagu:</span>
          <span className="breakdown-filter__value">{tagPath(activeTagId, categories)}</span>
          <button type="button" className="breakdown-filter__clear" onClick={handleBack}>
            Wróć
          </button>
        </div>
      )}
      <ReportBarList
        title={view === 'types' ? 'Typy kont' : 'Tagi kont'}
        rows={rows.map((entry) => {
          const { categoryId } = entry;
          return {
            key: entry.key,
            name: entry.name,
            total: entry.total,
            percentage: entry.percentage,
            onSelect:
              view === 'tags' && categoryId && drillableIds.has(categoryId) && !entry.isOther
                ? () => setActiveTagId(categoryId)
                : undefined
          };
        })}
      />
    </div>
  );
};

export default NetWorthComposition;
