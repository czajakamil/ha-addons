// Drzewo kategorii ze statystykami: API zwraca własne transakcje każdej kategorii,
// a kwoty gałęzi (rodzic + wszystkie podkategorie) składamy tutaj.
import type { Category, CategoryStatsRow } from '../../api';

export const MONTHS_IN_WINDOW = 12;

export type CategoryTotals = {
  count: number;
  spentCents: number;
  lastDate: string | null;
  /** 12 miesięcy od najstarszego, bieżący na końcu. */
  monthly: number[];
};

export type CategoryNode = {
  category: Category;
  depth: number;
  children: CategoryNode[];
  /** Własne transakcje i gałąź razem z podkategoriami. */
  own: CategoryTotals;
  total: CategoryTotals;
};

const emptyTotals = (): CategoryTotals => ({
  count: 0,
  spentCents: 0,
  lastDate: null,
  monthly: Array.from({ length: MONTHS_IN_WINDOW }, () => 0)
});

const laterDate = (a: string | null, b: string | null) => (!a ? b : !b ? a : a > b ? a : b);

const addTotals = (into: CategoryTotals, other: CategoryTotals) => {
  into.count += other.count;
  into.spentCents += other.spentCents;
  into.lastDate = laterDate(into.lastDate, other.lastDate);
  other.monthly.forEach((cents, index) => {
    into.monthly[index] += cents;
  });
};

export const toStatsMap = (rows: readonly CategoryStatsRow[]): Map<string, CategoryTotals> =>
  new Map(
    rows.map((row) => [
      row.category_id,
      {
        count: row.count,
        spentCents: row.spent_cents_12m,
        lastDate: row.last_date ?? null,
        monthly: [...row.monthly_cents]
      }
    ])
  );

/** Kolejność rodzeństwa: sort_order, potem nazwa (API zwraca już tak posortowane). */
const compareSiblings = (a: Category, b: Category) =>
  (a.sort_order ?? 0) - (b.sort_order ?? 0) || a.name.localeCompare(b.name, 'pl');

export const buildCategoryTree = (
  categories: readonly Category[],
  stats: ReadonlyMap<string, CategoryTotals> | null
): CategoryNode[] => {
  const ids = new Set(categories.map((category) => category.id));
  const byParent = new Map<string | null, Category[]>();
  categories.forEach((category) => {
    // Rodzic spoza zakresu (np. inna przestrzeń) — pokazujemy jako korzeń, żeby nie zgubić węzła
    const key = category.parent_id && ids.has(category.parent_id) ? category.parent_id : null;
    byParent.set(key, [...(byParent.get(key) ?? []), category]);
  });

  const build = (parentId: string | null, depth: number, seen: Set<string>): CategoryNode[] =>
    [...(byParent.get(parentId) ?? [])].sort(compareSiblings).flatMap((category) => {
      if (seen.has(category.id)) return []; // obrona przed cyklem
      const nextSeen = new Set(seen).add(category.id);
      const children = build(category.id, depth + 1, nextSeen);
      const ownStats = stats?.get(category.id);
      const own = ownStats ? { ...ownStats, monthly: [...ownStats.monthly] } : emptyTotals();
      const total = { ...own, monthly: [...own.monthly] };
      children.forEach((child) => addTotals(total, child.total));
      return [{ category, depth, children, own, total }];
    });

  return build(null, 0, new Set());
};

export const walkTree = (nodes: readonly CategoryNode[], visit: (node: CategoryNode) => void) => {
  nodes.forEach((node) => {
    visit(node);
    walkTree(node.children, visit);
  });
};

export const findNode = (nodes: readonly CategoryNode[], id: string): CategoryNode | null => {
  for (const node of nodes) {
    if (node.category.id === id) return node;
    const found = findNode(node.children, id);
    if (found) return found;
  }
  return null;
};

export const subtreeIds = (node: CategoryNode): string[] => {
  const ids: string[] = [];
  walkTree([node], (entry) => ids.push(entry.category.id));
  return ids;
};

/** Pierwszy dzień miesiąca sprzed 11 miesięcy, czyli początek okna statystyk. */
export const windowStart = (today = new Date()) => {
  const start = new Date(today.getFullYear(), today.getMonth() - (MONTHS_IN_WINDOW - 1), 1);
  const month = String(start.getMonth() + 1).padStart(2, '0');
  return `${start.getFullYear()}-${month}-01`;
};

/** Klucze „RRRR-MM” okna statystyk, od najstarszego. */
export const windowMonths = (today = new Date()) =>
  Array.from({ length: MONTHS_IN_WINDOW }, (_, index) => {
    const date = new Date(today.getFullYear(), today.getMonth() - (MONTHS_IN_WINDOW - 1 - index), 1);
    return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
  });

/** Gałąź bez żadnej transakcji w oknie 12 miesięcy. */
export const isUnused = (node: CategoryNode, start = windowStart()) =>
  !node.total.lastDate || node.total.lastDate < start;

export type VisibleRow = CategoryNode & { expanded: boolean; hasChildren: boolean };

type FlattenOptions = {
  expanded: ReadonlySet<string>;
  /** Szukana fraza: zostają pasujące węzły i ich przodkowie, wszystko rozwinięte. */
  query: string;
  /** Ukrywa nieużywane gałęzie (poza tymi w `keep`, np. świeżo dodanymi). */
  hideUnused: boolean;
  keep: ReadonlySet<string>;
};

export const flattenVisible = (nodes: readonly CategoryNode[], options: FlattenOptions): VisibleRow[] => {
  const needle = options.query.trim().toLocaleLowerCase('pl');
  const start = windowStart();
  const rows: VisibleRow[] = [];

  const keepsSomething = (node: CategoryNode): boolean =>
    options.keep.has(node.category.id) || node.children.some(keepsSomething);
  const shown = (node: CategoryNode) =>
    needle !== '' || !options.hideUnused || !isUnused(node, start) || keepsSomething(node);
  const matches = (node: CategoryNode): boolean =>
    node.category.name.toLocaleLowerCase('pl').includes(needle) || node.children.some(matches);

  const visit = (list: readonly CategoryNode[]) => {
    list.forEach((node) => {
      if (!shown(node) || (needle && !matches(node))) return;
      const children = node.children.filter(shown);
      const expanded = needle !== '' || options.expanded.has(node.category.id);
      rows.push({ ...node, expanded, hasChildren: children.length > 0 });
      if (expanded) visit(children);
    });
  };
  visit(nodes);
  return rows;
};

export const countUnused = (nodes: readonly CategoryNode[]) => {
  const start = windowStart();
  let count = 0;
  // Liczymy najwyższe nieużywane gałęzie — tyle wierszy znika z listy
  const visit = (list: readonly CategoryNode[]) =>
    list.forEach((node) => {
      if (isUnused(node, start)) count += 1;
      else visit(node.children);
    });
  visit(nodes);
  return count;
};

export type DropPosition = 'before' | 'after' | 'inside';

export type MovePlan = { parentId: string | null; orderedIds: string[] };

/**
 * Nowa kolejność rodzeństwa po upuszczeniu `draggedId` przy `targetId`.
 * null, gdy ruch nic nie zmienia albo wpiąłby gałąź w nią samą.
 */
export const planMove = (
  roots: readonly CategoryNode[],
  draggedId: string,
  targetId: string,
  position: DropPosition
): MovePlan | null => {
  if (draggedId === targetId) return null;
  const dragged = findNode(roots, draggedId);
  const target = findNode(roots, targetId);
  if (!dragged || !target) return null;
  if (subtreeIds(dragged).includes(targetId)) return null;

  if (position === 'inside') {
    const ids = target.children.map((child) => child.category.id).filter((id) => id !== draggedId);
    if (dragged.category.parent_id === targetId) return null;
    return { parentId: targetId, orderedIds: [...ids, draggedId] };
  }

  const parentId = target.category.parent_id ?? null;
  const parent = parentId ? findNode(roots, parentId) : null;
  const siblings = (parent ? parent.children : roots).map((node) => node.category.id);
  const before = siblings.filter((id) => id !== draggedId);
  const index = before.indexOf(targetId) + (position === 'after' ? 1 : 0);
  const orderedIds = [...before.slice(0, index), draggedId, ...before.slice(index)];
  const unchanged =
    (dragged.category.parent_id ?? null) === parentId && orderedIds.every((id, i) => id === siblings[i]);
  return unchanged ? null : { parentId, orderedIds };
};

/** Przesunięcie o jedno miejsce wśród rodzeństwa (alternatywa dla przeciągania z klawiatury). */
export const planShift = (roots: readonly CategoryNode[], id: string, delta: -1 | 1): MovePlan | null => {
  const node = findNode(roots, id);
  if (!node) return null;
  const parentId = node.category.parent_id ?? null;
  const parent = parentId ? findNode(roots, parentId) : null;
  const siblings = (parent ? parent.children : roots).map((entry) => entry.category.id);
  const index = siblings.indexOf(id);
  const next = index + delta;
  if (next < 0 || next >= siblings.length) return null;
  const orderedIds = [...siblings];
  [orderedIds[index], orderedIds[next]] = [orderedIds[next], orderedIds[index]];
  return { parentId, orderedIds };
};

/** Kategorie, pod które można przenieść gałąź: wszystkie poza nią samą i jej potomkami. */
export const moveTargets = (roots: readonly CategoryNode[], id: string) => {
  const node = findNode(roots, id);
  const excluded = new Set(node ? subtreeIds(node) : [id]);
  const targets: Category[] = [];
  walkTree(roots, (entry) => {
    if (!excluded.has(entry.category.id)) targets.push(entry.category);
  });
  return targets;
};
