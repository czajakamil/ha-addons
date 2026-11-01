// Pure model layer for ReportsPage: types, constants and side-effect-free
// helpers. Extracted from ReportsPage.tsx to keep the page component focused on
// state and rendering. Nothing here touches React state or the DOM.
import { fullMonthName, monthDate } from '../../utils/months';
import { ArrowLeftRightIcon, ChartAreaIcon, ChartLineIcon, ChartPieIcon, WorkflowIcon } from 'lucide-react';
import type { Category, IncomeExpenseCategoryRow, IncomeExpenseMonthRow } from '../../api';
import { DATA_TABLE_ROW_HEIGHT } from '../../components/DataTable';
import { formatMoney } from '../../utils/format';

import { readToken } from '../../utils/tokens';
export type MonthRange = { start: string; end: string };
export type QuickRangeId = 'this-month' | 'last-3-months' | 'last-6-months' | 'last-year' | 'ytd';
export type ReportFilters = { accountIds?: string[]; categoryIds?: string[] };

export const formatMonthInput = (date: Date) =>
  `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;

export const startOfMonth = (date: Date) => new Date(date.getFullYear(), date.getMonth(), 1);

export const QUICK_RANGE_OPTIONS: { id: QuickRangeId; label: string; getRange: () => MonthRange }[] = [
  {
    id: 'this-month',
    label: 'Ten miesiąc',
    getRange: () => {
      const now = startOfMonth(new Date());
      const month = formatMonthInput(now);
      return { start: month, end: month };
    }
  },
  {
    id: 'last-3-months',
    label: 'Ostatnie 3 miesiące',
    getRange: () => {
      const now = startOfMonth(new Date());
      const startDate = new Date(now.getFullYear(), now.getMonth() - 2, 1);
      return { start: formatMonthInput(startDate), end: formatMonthInput(now) };
    }
  },
  {
    id: 'last-6-months',
    label: 'Ostatnie 6 miesięcy',
    getRange: () => {
      const now = startOfMonth(new Date());
      const startDate = new Date(now.getFullYear(), now.getMonth() - 5, 1);
      return { start: formatMonthInput(startDate), end: formatMonthInput(now) };
    }
  },
  {
    id: 'last-year',
    label: 'Ostatni rok',
    getRange: () => {
      const now = startOfMonth(new Date());
      const startDate = new Date(now.getFullYear() - 1, now.getMonth(), 1);
      return { start: formatMonthInput(startDate), end: formatMonthInput(now) };
    }
  },
  {
    id: 'ytd',
    label: 'YTD',
    getRange: () => {
      const now = startOfMonth(new Date());
      const startDate = new Date(now.getFullYear(), 0, 1);
      return { start: formatMonthInput(startDate), end: formatMonthInput(now) };
    }
  }
];

export const DEFAULT_QUICK_RANGE: QuickRangeId = 'last-3-months';

export const createInitialRange = () =>
  QUICK_RANGE_OPTIONS.find((option) => option.id === DEFAULT_QUICK_RANGE)?.getRange() ??
  QUICK_RANGE_OPTIONS[0].getRange();

export const monthEndDate = (month: string) => {
  const [yearStr, monthStr] = month.split('-');
  const year = Number(yearStr);
  const monthNumber = Number(monthStr);
  if (!year || !monthNumber) {
    return `${month}-01`;
  }
  const lastDay = new Date(year, monthNumber, 0).getDate();
  return `${month}-${String(lastDay).padStart(2, '0')}`;
};

// Raporty agregują kwoty z wielu kont; backend nie zwraca waluty, więc przyjmujemy PLN
export const REPORT_CURRENCY = 'PLN';
export const moneyLabel = (cents: number) => formatMoney(cents, REPORT_CURRENCY);
export const percentLabel = (value: number) => `${(value * 100).toFixed(1).replace('.', ',')}%`;
// Backend podpisuje miesiące po angielsku, więc nazwę bierzemy z klucza „RRRR-MM”
export const reportMonthLabel = (month: string) => fullMonthName.format(monthDate(month));
export const reportMonthShort = (month: string) => {
  const date = monthDate(month);
  return `${date.toLocaleDateString('pl-PL', { month: 'short' })} ${String(date.getFullYear()).slice(2)}`;
};
export const formatDate = (isoDate: string) =>
  new Date(isoDate).toLocaleDateString('pl-PL', {
    year: 'numeric',
    month: 'short',
    day: 'numeric'
  });
export const formatMonthLabel = (month: string) =>
  new Date(`${month}-01`).toLocaleDateString('pl-PL', {
    year: 'numeric',
    month: 'short'
  });

// Rok podajemy raz, gdy oba końce zakresu leżą w tym samym
export const formatMonthRangeShort = (start: string, end: string) => {
  const endLabel = fullMonthName.format(monthDate(end));
  if (start.slice(0, 4) !== end.slice(0, 4)) return `${fullMonthName.format(monthDate(start))} – ${endLabel}`;
  if (start === end) return endLabel;
  return `${monthDate(start).toLocaleDateString('pl-PL', { month: 'short' })} – ${endLabel}`;
};

export type CategoryAggregate = { id?: string | null; name: string; total: number };
export type BreakdownChartEntry = {
  key: string;
  name: string;
  categoryId?: string;
  percentage: number;
  total: number;
  value: number;
  color: string;
  isOther?: boolean;
};
export type CategoryTreeNode = { parent: Category; children: Category[] };
export type CategoryMenuEntry =
  | { type: 'uncategorized' }
  | { type: 'parent'; category: Category; hasChildren: boolean }
  | { type: 'child'; category: Category; parentId: string };
export type SnakeyNode = { id: string; name: string; type: 'account' | 'category' };
export type SnakeyLink = { source: string; target: string; value: number };
export type SnakeyData = { nodes: SnakeyNode[]; links: SnakeyLink[] };
export type SnakeySummary = {
  data: SnakeyData;
  total: number;
  displayedTotal: number;
  transactionCount: number;
  hiddenCount: number;
};
export type BreakdownCategorySelection = { id: string; name: string; color?: string };
export type BreakdownEditableLookupField = 'store';
export type BreakdownEditableAmountField = 'outflow' | 'inflow';

/** Rozszerza zaznaczone id tagów o całe pod-drzewa (rollup po tagu-rodzicu), analogicznie do path_prefix z backendu. */
export const expandCategoryIdsWithDescendants = (ids: string[], categories: Category[]) => {
  if (!ids.length) return ids;
  const childrenByParent = new Map<string, string[]>();
  categories.forEach((category) => {
    if (!category.parent_id) return;
    const bucket = childrenByParent.get(category.parent_id) ?? [];
    bucket.push(category.id);
    childrenByParent.set(category.parent_id, bucket);
  });

  const expanded = new Set<string>();
  const visit = (id: string) => {
    if (expanded.has(id)) return;
    expanded.add(id);
    (childrenByParent.get(id) ?? []).forEach(visit);
  };
  ids.forEach(visit);

  return Array.from(expanded);
};

export const stripParentPrefix = (name: string, parentName?: string) => {
  if (!parentName) return name;
  const prefix = `${parentName}/`.toLowerCase();
  if (name.toLowerCase().startsWith(prefix)) {
    const stripped = name.slice(prefix.length).trim();
    return stripped || name;
  }
  return name;
};

export const aggregateCategories = (months: IncomeExpenseMonthRow[], type: 'income' | 'expense') => {
  const totals = new Map<string, CategoryAggregate>();
  months.forEach((month) => {
    const list = type === 'income' ? month.income_categories : month.expense_categories;
    list.forEach((category) => {
      const key = category.category_id ?? category.category_name ?? 'uncategorized';
      const existing = totals.get(key) ?? {
        id: category.category_id,
        name: category.category_name ?? 'Uncategorized',
        total: 0
      };
      existing.total += category.total_cents;
      totals.set(key, existing);
    });
  });
  return Array.from(totals.values())
    .sort((a, b) => b.total - a.total)
    .slice(0, 5);
};

export const REPORT_TABS = [
  { id: 'spending-breakdown', label: 'Wydatki', icon: ChartPieIcon },
  { id: 'account-categories', label: 'Tagi', icon: ChartPieIcon },
  { id: 'spending-trends', label: 'Trendy', icon: ChartLineIcon },
  { id: 'net-worth', label: 'Majątek', icon: ChartAreaIcon },
  { id: 'snakey', label: 'Przepływy', icon: WorkflowIcon },
  { id: 'income-expense', label: 'Przychody i wydatki', icon: ArrowLeftRightIcon }
] as const;

export type ReportTabId = (typeof REPORT_TABS)[number]['id'];

export const MIN_BREAKDOWN_PERCENTAGE = 0.01;
export const OTHER_CATEGORIES_LABEL = 'Pozostałe';
export const BREAKDOWN_TOP_LIMIT = 10;
export const OTHER_ACCOUNT_KINDS_LABEL = 'Pozostałe typy';
export const otherCategoryColor = () => readToken('--text-muted');
export const UNCATEGORIZED_CATEGORY_ID = '__uncategorized__';
export const UNCATEGORIZED_CATEGORY_NAME = 'Bez tagu';
export const ACCOUNT_UNCATEGORIZED_ID = '__account_uncategorized__';
export const ACCOUNT_UNCATEGORIZED_NAME = 'Bez kategorii';
export const SPENDING_TRENDS_CATEGORY_LIMIT = 6;
export const SPENDING_TRENDS_OTHER_LABEL = 'Pozostałe';
export const SNAKEY_MAX_TRANSACTIONS = 2000;
export const SNAKEY_PAGE_SIZE = 200;
export const SNAKEY_SPENDING_CATEGORY_LIMIT = 8;
export const SNAKEY_TRANSFER_LINK_LIMIT = 12;
export type SnakeyTransferDirection = 'outflow-to-inflow' | 'inflow-to-outflow';
export const SNAKEY_TRANSFER_DIRECTION = 'outflow-to-inflow' as SnakeyTransferDirection;
export const SNAKEY_OTHER_CATEGORY_ID = '__snakey_other__';
export const SNAKEY_OTHER_CATEGORY_LABEL = 'Pozostałe wydatki';
export const SNAKEY_UNKNOWN_ACCOUNT_LABEL = 'Nieznane konto';
export const INTERNAL_CATEGORY_NAME = 'internal master category';
export const INFLOW_READY_TO_ASSIGN = 'inflow: ready to assign';
export const BREAKDOWN_GRID_MIN_HEIGHT = 240;
export const BREAKDOWN_GRID_MAX_HEIGHT = 520;

export const getBreakdownGridHeight = (rowCount: number) => {
  if (rowCount <= 0) return BREAKDOWN_GRID_MIN_HEIGHT;
  const estimated = 60 + DATA_TABLE_ROW_HEIGHT * rowCount;
  return Math.min(BREAKDOWN_GRID_MAX_HEIGHT, Math.max(BREAKDOWN_GRID_MIN_HEIGHT, estimated));
};

export const normalizeCategoryName = (rawName?: string | null) => {
  const fallback = rawName?.trim() || 'Uncategorized';
  const slashIndex = fallback.indexOf('/');
  if (slashIndex === -1) {
    return { groupName: fallback, categoryName: fallback };
  }
  const groupName = fallback.slice(0, slashIndex).trim() || 'Uncategorized';
  const categoryName = fallback.slice(slashIndex + 1).trim() || groupName;
  return { groupName, categoryName };
};

export const normalizeAccountKind = (value?: string | null) =>
  (value ?? '').trim().toLowerCase().replace(/\s+/g, '').replace(/_/g, '');

export const shouldReverseTransferDirection = (direction: SnakeyTransferDirection) =>
  direction === 'inflow-to-outflow';

export const ACCOUNT_KIND_LABELS = new Map<string, string>([
  ['cash', 'Aktywo'],
  ['creditcard', 'Karta kredytowa'],
  ['liability', 'Zobowiązanie']
]);

export const getAccountKindLabel = (value?: string | null) => {
  const normalized = normalizeAccountKind(value);
  if (!normalized) return 'Nieznane';
  return ACCOUNT_KIND_LABELS.get(normalized) ?? value ?? 'Nieznane';
};

export const isExcludedCategory = (rawName?: string | null) => {
  if (!rawName) return false;
  const normalized = rawName.trim().toLowerCase();
  return (
    normalized === INTERNAL_CATEGORY_NAME ||
    normalized === INFLOW_READY_TO_ASSIGN ||
    normalized.includes(INTERNAL_CATEGORY_NAME)
  );
};

export const resolveTrendBucket = (
  category: IncomeExpenseCategoryRow,
  mode: 'categories' | 'groups'
): { key: string; label: string } => {
  const { groupName, categoryName } = normalizeCategoryName(category.category_name);

  if (mode === 'groups') {
    const label =
      !groupName || groupName.toLowerCase() === 'uncategorized'
        ? UNCATEGORIZED_CATEGORY_NAME
        : groupName;
    const keyBase = groupName?.toLowerCase() || 'uncategorized';
    return { key: `group:${keyBase}`, label };
  }

  const label =
    !categoryName || categoryName.toLowerCase() === 'uncategorized'
      ? UNCATEGORIZED_CATEGORY_NAME
      : categoryName;
  const key = category.category_id ?? category.category_name ?? UNCATEGORIZED_CATEGORY_ID;
  return { key, label };
};

export const groupBreakdownEntries = (
  entries: BreakdownChartEntry[],
  percentBase: number,
  otherLabel: string = OTHER_CATEGORIES_LABEL
) => {
  if (!entries.length) return [];
  if (percentBase <= 0) return entries;

  let otherValue = 0;
  let otherTotal = 0;
  const visibleEntries = entries.filter((entry) => {
    const isSmall = entry.percentage < MIN_BREAKDOWN_PERCENTAGE;
    if (isSmall) {
      otherValue += entry.value;
      otherTotal += entry.total;
    }
    return !isSmall;
  });

  if (otherValue > 0) {
    const otherPercentage = percentBase > 0 ? otherValue / percentBase : 0;
    visibleEntries.push({
      key: 'other-categories',
      name: otherLabel,
      percentage: otherPercentage,
      total: otherTotal,
      value: otherValue,
      color: otherCategoryColor(),
      isOther: true
    });
  }

  return visibleEntries.length ? visibleEntries : entries;
};
