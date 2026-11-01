// Typy, stałe i czyste helpery strony budżetu — bez stanu i bez JSX.
import type { BudgetAssignmentUpdate, BudgetCategoryRow } from '../../api';
import { copy } from '../../copy/pl';
import { formatMoney } from '../../utils/format';

export type CategoryNode = {
  parent: BudgetCategoryRow;
  children: BudgetCategoryRow[];
};

export type CategoryTone = 'neutral' | 'positive' | 'warning' | 'negative';

export type BudgetStatusFilter = 'all' | 'overspent' | 'underfunded' | 'available';

export const FILTER_LABELS: Record<BudgetStatusFilter, string> = copy.budget.filters;

export const BUDGET_STATUS_FILTERS = Object.keys(FILTER_LABELS) as BudgetStatusFilter[];

// --- Miesiące ---------------------------------------------------------------

export const toMonthKey = (date: Date) => {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  return `${year}-${month}`;
};

export const shiftMonth = (date: Date, offset: number) =>
  new Date(date.getFullYear(), date.getMonth() + offset, 1);

export const formatMonthKeyLabel = (monthKey: string) => {
  const [yearStr, monthStr] = monthKey.split('-');
  const year = Number(yearStr);
  const month = Number(monthStr);
  if (!year || !month || month < 1 || month > 12) {
    return monthKey;
  }
  return new Date(year, month - 1, 1).toLocaleDateString('pl-PL', {
    month: 'long',
    year: 'numeric'
  });
};

// Niepoprawny klucz miesiąca cofa się do bieżącego miesiąca
export const parseMonthKey = (monthKey: string) => {
  const [yearStr, monthStr] = monthKey.split('-');
  const year = Number(yearStr);
  const month = Number(monthStr);
  if (!year || !month || month < 1 || month > 12) {
    const now = new Date();
    return new Date(now.getFullYear(), now.getMonth(), 1);
  }
  return new Date(year, month - 1, 1);
};

export const buildMonthKey = (year: number, monthIndex: number) =>
  `${year}-${String(monthIndex + 1).padStart(2, '0')}`;

export const MONTH_SHORT_LABELS = ['Sty', 'Lut', 'Mar', 'Kwi', 'Maj', 'Cze', 'Lip', 'Sie', 'Wrz', 'Paź', 'Lis', 'Gru'];

export const capitalizeLabel = (value: string) => {
  if (!value) return value;
  return value.charAt(0).toUpperCase() + value.slice(1);
};

// --- Kwoty --------------------------------------------------------------------

// Budżet agreguje kwoty z wielu kont; backend nie zwraca waluty, więc przyjmujemy PLN
export const BUDGET_CURRENCY = 'PLN';
export const moneyLabel = (cents: number) => formatMoney(cents, BUDGET_CURRENCY);

export const averageCents = (values: number[]) => {
  if (!values.length) return 0;
  return Math.round(values.reduce((sum, value) => sum + value, 0) / values.length);
};

export const spendingTargetFromActivity = (activityCents: number) => Math.max(0, -activityCents);

// --- Statusy i filtry ---------------------------------------------------------

const normalizeBudgetStatus = (status?: string | null) => (status ?? '').trim().toLowerCase();

export const categoryMatchesFilter = (row: BudgetCategoryRow, filter: BudgetStatusFilter) => {
  if (filter === 'all') return true;
  const normalizedStatus = normalizeBudgetStatus(row.status);

  if (filter === 'overspent') {
    return row.available_cents < 0 || normalizedStatus.includes('overspent');
  }

  if (filter === 'underfunded') {
    return normalizedStatus.includes('underfund');
  }

  return row.available_cents > 0 || normalizedStatus.includes('available');
};

export const resolveAvailableTone = (row: BudgetCategoryRow): CategoryTone => {
  const normalizedStatus = normalizeBudgetStatus(row.status);
  if (row.available_cents < 0 || normalizedStatus.includes('overspent')) {
    return 'negative';
  }
  if (normalizedStatus.includes('underfund')) {
    return 'warning';
  }
  if (row.available_cents > 0) {
    return 'positive';
  }
  return 'neutral';
};

// Ile środków koperty już wydano, w procentach; powyżej 100 oznacza przekroczenie.
// Null, gdy kategoria nie ma ani przypisania, ani aktywności.
export const buildEnvelopeUsage = (row: BudgetCategoryRow): number | null => {
  if (row.assigned_cents === 0 && row.activity_cents === 0 && row.available_cents === 0) {
    return null;
  }
  if (row.available_cents < 0) return 101;
  const spent = Math.max(-row.activity_cents, 0);
  const funds = spent + row.available_cents;
  return funds > 0 ? (spent / funds) * 100 : 0;
};

// Nazwa podkategorii może nieść pełną ścieżkę z „/" — pokazujemy tylko ostatni człon
export const formatCategoryName = (name: string, depth: number) => {
  if (depth === 0) return name;
  const trimmed = name.trim();
  const lastSlash = trimmed.lastIndexOf('/');
  if (lastSlash === -1) return trimmed;
  const suffix = trimmed.slice(lastSlash + 1).trim();
  return suffix || trimmed;
};

// --- Drzewo kategorii -----------------------------------------------------------

export const buildCategoryTree = (categories: BudgetCategoryRow[]): CategoryNode[] => {
  if (!categories.length) return [];
  const byId = new Map(categories.map((category) => [category.category_id, category]));
  const childrenByParent = new Map<string | null, BudgetCategoryRow[]>();

  categories.forEach((category) => {
    const parentKey = category.parent_id ?? null;
    const bucket = childrenByParent.get(parentKey) ?? [];
    bucket.push(category);
    childrenByParent.set(parentKey, bucket);
  });

  const sortByName = (a: BudgetCategoryRow, b: BudgetCategoryRow) =>
    a.category_name.localeCompare(b.category_name, 'pl');

  const topLevel = (childrenByParent.get(null) ?? []).slice().sort(sortByName);
  const nodes = topLevel.map((parent) => ({
    parent,
    children: (childrenByParent.get(parent.category_id) ?? []).slice().sort(sortByName)
  }));

  const orphanChildren = categories.filter(
    (category) => category.parent_id && !byId.has(category.parent_id)
  );

  orphanChildren.forEach((orphan) => {
    const existing = nodes.some((node) => node.parent.category_id === orphan.category_id);
    if (!existing) {
      nodes.push({
        parent: orphan,
        children: (childrenByParent.get(orphan.category_id) ?? []).slice().sort(sortByName)
      });
    }
  });

  return nodes.sort((a, b) => sortByName(a.parent, b.parent));
};

// Liście drzewa: dzieci grup albo sama kategoria, gdy nie ma podkategorii
export const collectLeafCategories = (tree: CategoryNode[]) =>
  tree.flatMap((node) => (node.children.length ? node.children : [node.parent]));

// Grupy zostają, jeśli pasuje choć jedno dziecko; same dzieci są przefiltrowane
export const filterCategoryTree = (tree: CategoryNode[], filter: BudgetStatusFilter): CategoryNode[] => {
  if (filter === 'all') {
    return tree;
  }

  return tree
    .map((node) => {
      if (!node.children.length) {
        return categoryMatchesFilter(node.parent, filter) ? node : null;
      }
      const filteredChildren = node.children.filter((child) => categoryMatchesFilter(child, filter));
      if (!filteredChildren.length) {
        return null;
      }
      return { ...node, children: filteredChildren };
    })
    .filter((node): node is CategoryNode => node !== null);
};

export const countByFilter = (leaves: BudgetCategoryRow[]): Record<BudgetStatusFilter, number> => ({
  all: leaves.length,
  overspent: leaves.filter((row) => categoryMatchesFilter(row, 'overspent')).length,
  underfunded: leaves.filter((row) => categoryMatchesFilter(row, 'underfunded')).length,
  available: leaves.filter((row) => categoryMatchesFilter(row, 'available')).length
});

// Wiersz grupy pokazuje własne kwoty rodzica powiększone o sumę dzieci
export const sumGroupTotals = (node: CategoryNode): BudgetCategoryRow =>
  node.children.reduce(
    (acc, child) => ({
      ...acc,
      assigned_cents: acc.assigned_cents + child.assigned_cents,
      activity_cents: acc.activity_cents + child.activity_cents,
      available_cents: acc.available_cents + child.available_cents
    }),
    {
      ...node.parent,
      assigned_cents: node.parent.assigned_cents,
      activity_cents: node.parent.activity_cents,
      available_cents: node.parent.available_cents
    }
  );

// --- Auto-Assign ----------------------------------------------------------------

export type AutoAssignMode =
  | 'underfunded'
  | 'assignedLastMonth'
  | 'spentLastMonth'
  | 'averageAssigned'
  | 'averageSpent'
  | 'reduceOverfunding'
  | 'resetAvailable'
  | 'resetAssigned';

export type AutoAssignTotals = Record<AutoAssignMode, number>;

export const AUTO_ASSIGN_OPTION_GROUPS: { mode: AutoAssignMode; label: string }[][] = [
  [
    { mode: 'underfunded', label: copy.budget.autoAssign.underfunded },
    { mode: 'assignedLastMonth', label: copy.budget.autoAssign.assignedLastMonth },
    { mode: 'spentLastMonth', label: copy.budget.autoAssign.spentLastMonth },
    { mode: 'averageAssigned', label: copy.budget.autoAssign.averageAssigned },
    { mode: 'averageSpent', label: copy.budget.autoAssign.averageSpent }
  ],
  [
    { mode: 'reduceOverfunding', label: copy.budget.autoAssign.reduceOverfunding },
    { mode: 'resetAvailable', label: copy.budget.autoAssign.resetAvailable },
    { mode: 'resetAssigned', label: copy.budget.autoAssign.resetAssigned }
  ]
];

// Docelowe „przypisane" kategorii dla danego trybu. `lastMonthRow` to ta sama
// kategoria z poprzedniego miesiąca — brak oznacza, że go nie wczytano.
export const resolveAutoAssignTarget = (
  mode: AutoAssignMode,
  row: BudgetCategoryRow,
  lastMonthRow?: BudgetCategoryRow
): number => {
  switch (mode) {
    case 'underfunded':
      return row.available_cents < 0 ? row.assigned_cents - row.available_cents : row.assigned_cents;
    case 'assignedLastMonth':
      return lastMonthRow?.assigned_cents ?? row.assigned_cents;
    case 'spentLastMonth':
      return spendingTargetFromActivity((lastMonthRow ?? row).activity_cents);
    case 'averageAssigned':
      return averageCents(lastMonthRow ? [row.assigned_cents, lastMonthRow.assigned_cents] : [row.assigned_cents]);
    case 'averageSpent': {
      const samples = [spendingTargetFromActivity(row.activity_cents)];
      if (lastMonthRow) {
        samples.push(spendingTargetFromActivity(lastMonthRow.activity_cents));
      }
      return averageCents(samples);
    }
    case 'reduceOverfunding': {
      const assignedPositive = Math.max(0, row.assigned_cents);
      const reducible = Math.max(0, Math.min(row.available_cents, assignedPositive));
      return row.assigned_cents - reducible;
    }
    case 'resetAvailable':
      return row.assigned_cents - row.available_cents;
    case 'resetAssigned':
      return 0;
  }
};

export const buildLastMonthCategoryMap = (categories: BudgetCategoryRow[]) =>
  new Map<string, BudgetCategoryRow>(categories.map((row) => [row.category_id, row]));

// Kwoty pokazywane obok opcji Auto-Assign. „Underfunded" i „Reduce Overfunding"
// to wielkość zmiany, nie suma docelowych przypisań; „Reset Assigned" zawsze 0.
export const buildAutoAssignTotals = (
  leaves: BudgetCategoryRow[],
  lastMonthCategoryMap: Map<string, BudgetCategoryRow>
): AutoAssignTotals => {
  const totals: AutoAssignTotals = {
    underfunded: 0,
    assignedLastMonth: 0,
    spentLastMonth: 0,
    averageAssigned: 0,
    averageSpent: 0,
    reduceOverfunding: 0,
    resetAvailable: 0,
    resetAssigned: 0
  };

  leaves.forEach((row) => {
    const lastMonthRow = lastMonthCategoryMap.get(row.category_id);
    totals.underfunded += Math.max(0, -row.available_cents);
    totals.assignedLastMonth += resolveAutoAssignTarget('assignedLastMonth', row, lastMonthRow);
    totals.spentLastMonth += resolveAutoAssignTarget('spentLastMonth', row, lastMonthRow);
    totals.averageAssigned += resolveAutoAssignTarget('averageAssigned', row, lastMonthRow);
    totals.averageSpent += resolveAutoAssignTarget('averageSpent', row, lastMonthRow);
    totals.reduceOverfunding += Math.max(0, row.assigned_cents - resolveAutoAssignTarget('reduceOverfunding', row));
    totals.resetAvailable += resolveAutoAssignTarget('resetAvailable', row);
  });

  return totals;
};

// Zapisy do wysłania: tylko kategorie, których przypisanie faktycznie się zmienia
export const buildAutoAssignUpdates = (
  mode: AutoAssignMode,
  leaves: BudgetCategoryRow[],
  lastMonthCategoryMap: Map<string, BudgetCategoryRow>,
  month: string
): BudgetAssignmentUpdate[] =>
  leaves
    .map((row) => {
      const nextAssigned = Math.round(
        resolveAutoAssignTarget(mode, row, lastMonthCategoryMap.get(row.category_id))
      );
      if (!Number.isFinite(nextAssigned) || nextAssigned === row.assigned_cents) {
        return null;
      }

      return {
        category_id: row.category_id,
        month,
        assigned_cents: nextAssigned
      };
    })
    .filter((update): update is BudgetAssignmentUpdate => update !== null);
