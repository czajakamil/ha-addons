import type { CSSProperties } from 'react';
import type { Category } from '../../api';
import { resolveTagColor } from './tagColors';
import { formatCategoryPath } from '../../utils/categoryPath';

type TagColorDotProps = {
  /** Wartość CSS koloru (np. z resolveTagColor().value). */
  value: string;
  /** Kolor odziedziczony po przodku — rysujemy jako obwódkę zamiast wypełnienia. */
  inherited?: boolean;
  size?: number;
};

/** Sama kropka koloru — do drzewa tagów i list. */
export const TagColorDot = ({ value, inherited = false, size = 10 }: TagColorDotProps) => (
  <span
    className={`tag-dot${inherited ? ' tag-dot--inherited' : ''}`}
    style={{ '--tag-color': value, '--tag-dot-size': `${size}px` } as CSSProperties}
    aria-hidden="true"
  />
);

type TagChipProps = {
  category: Category | undefined | null;
  /** Mapa wszystkich kategorii (do dziedziczenia koloru po przodkach). */
  byId: Map<string, Category>;
  /** Etykieta — domyślnie sama nazwa liścia; pełną ścieżkę pokazujemy w tooltipie. */
  label?: string;
  /** Pod nazwą, mniejszym tekstem, ścieżka przodków („Jedzenie” nad „Zakupy Spożywcze”). */
  showParent?: boolean;
};

const ancestorPath = (category: Category, byId: Map<string, Category>) => {
  const names: string[] = [];
  const seen = new Set<string>([category.id]);
  let parentId = category.parent_id;
  while (parentId && !seen.has(parentId)) {
    seen.add(parentId);
    const parent = byId.get(parentId);
    if (!parent) break;
    names.unshift(parent.name);
    parentId = parent.parent_id;
  }
  return names.join(' › ');
};

/** Chip przypisanego tagu: kropka koloru + nazwa na delikatnie zabarwionym tle. */
export const TagChip = ({ category, byId, label, showParent = false }: TagChipProps) => {
  if (!category) {
    return <span className="text-secondary">—</span>;
  }
  const { value, inherited } = resolveTagColor(category, byId);
  const parentPath = showParent ? ancestorPath(category, byId) : '';
  if (parentPath) {
    return (
      <span
        className="tag-chip tag-chip--with-parent"
        style={{ '--tag-color': value } as CSSProperties}
        title={formatCategoryPath(category.path)}
      >
        <TagColorDot value={value} inherited={inherited} size={8} />
        <span className="tag-chip__text">
          <span className="tag-chip__label">{label ?? category.name}</span>
          <span className="tag-chip__parent">{parentPath}</span>
        </span>
      </span>
    );
  }
  return (
    <span
      className="tag-chip"
      style={{ '--tag-color': value } as CSSProperties}
      title={formatCategoryPath(category.path)}
    >
      <TagColorDot value={value} inherited={inherited} size={8} />
      <span className="tag-chip__label">{label ?? category.name}</span>
    </span>
  );
};
