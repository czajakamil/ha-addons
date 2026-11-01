// Dane diagramu Przepływy: źródła pieniędzy → grupy kategorii → kategorie.
import type { Category, Transaction } from '../../api';
import {
  SNAKEY_OTHER_CATEGORY_ID,
  SNAKEY_OTHER_CATEGORY_LABEL,
  SNAKEY_SPENDING_CATEGORY_LIMIT,
  UNCATEGORIZED_CATEGORY_ID,
  UNCATEGORIZED_CATEGORY_NAME,
  isExcludedCategory,
  normalizeCategoryName,
  type SnakeyData,
  type SnakeyLink,
  type SnakeyNode
} from './model';

const INCOME_NODE = 'income';
const SAVINGS_NODE = 'savings';
const SPENDING_NODE = 'spending';
const SURPLUS_NODE = 'surplus';

type BuildFlowsInput = {
  transactions: readonly Transaction[];
  categories: readonly Category[];
  categoryNameLookup: ReadonlyMap<string, string>;
  accountIds: ReadonlySet<string> | null;
  categoryIds: ReadonlySet<string> | null;
};

export const buildFlows = ({
  transactions,
  categories,
  categoryNameLookup,
  accountIds,
  categoryIds
}: BuildFlowsInput) => {
  const categoryPaths = new Map(categories.map((category) => [category.id, category.path ?? category.name]));
  const totals = new Map<string, { group: string; name: string; value: number }>();
  let income = 0;
  let spending = 0;
  let transactionCount = 0;

  transactions.forEach((transaction) => {
    if (transaction.transfer_account_id) return;
    if (accountIds && !accountIds.has(transaction.account_id)) return;
    if (transaction.total_cents > 0) {
      income += transaction.total_cents;
      return;
    }
    if (transaction.total_cents === 0) return;

    const categoryId = transaction.category_id ?? UNCATEGORIZED_CATEGORY_ID;
    if (categoryIds && !categoryIds.has(categoryId)) return;
    const rawName =
      categoryId === UNCATEGORIZED_CATEGORY_ID
        ? UNCATEGORIZED_CATEGORY_NAME
        : categoryNameLookup.get(categoryId) ?? UNCATEGORIZED_CATEGORY_NAME;
    if (isExcludedCategory(rawName)) return;

    const path = categoryPaths.get(categoryId) ?? rawName;
    const { groupName, categoryName } = normalizeCategoryName(path);
    const amount = -transaction.total_cents;
    const entry = totals.get(categoryId) ?? { group: groupName, name: categoryName, value: 0 };
    entry.value += amount;
    totals.set(categoryId, entry);
    spending += amount;
    transactionCount += 1;
  });

  const fromSavings = Math.max(0, spending - income);
  const sorted = Array.from(totals.entries()).sort((a, b) => b[1].value - a[1].value);
  const hiddenCount = Math.max(0, sorted.length - SNAKEY_SPENDING_CATEGORY_LIMIT);
  const nodes = new Map<string, SnakeyNode>();
  const links = new Map<string, SnakeyLink>();
  const addNode = (id: string, name: string, type: SnakeyNode['type']) => {
    if (!nodes.has(id)) nodes.set(id, { id, name, type });
  };
  const addLink = (source: string, target: string, value: number) => {
    const key = `${source}>${target}`;
    const link = links.get(key);
    if (link) link.value += value;
    else links.set(key, { source, target, value });
  };

  // Przy deficycie oba źródła spływają do węzła „Wydatki”: dzięki temu Przychody mają
  // swoją prawdziwą wartość, a diagram nie zmyśla, z którego źródła opłacono którą grupę.
  let groupSource = INCOME_NODE;
  if (fromSavings > 0) {
    groupSource = SPENDING_NODE;
    addNode(SPENDING_NODE, 'Wydatki', 'category');
    if (income > 0) {
      addNode(INCOME_NODE, 'Przychody', 'account');
      addLink(INCOME_NODE, SPENDING_NODE, income);
    }
    addNode(SAVINGS_NODE, 'Z oszczędności', 'account');
    addLink(SAVINGS_NODE, SPENDING_NODE, fromSavings);
  } else if (spending > 0) {
    addNode(INCOME_NODE, 'Przychody', 'account');
  }

  sorted.forEach(([id, entry], index) => {
    const isTop = index < SNAKEY_SPENDING_CATEGORY_LIMIT;
    const groupId = isTop ? `group:${entry.group}` : `group:${SNAKEY_OTHER_CATEGORY_ID}`;
    addNode(groupId, isTop ? entry.group : SNAKEY_OTHER_CATEGORY_LABEL, 'category');
    addLink(groupSource, groupId, entry.value);
    // Kategoria bez grupy ma jedną nazwę na obu poziomach; wtedy nie powtarzamy jej dwa razy
    if (!isTop || entry.group === entry.name) return;
    const leafId = `cat:${id}`;
    addNode(leafId, entry.name, 'category');
    addLink(groupId, leafId, entry.value);
  });

  if (spending > 0 && income > spending) {
    addNode(SURPLUS_NODE, 'Nadwyżka', 'category');
    addLink(INCOME_NODE, SURPLUS_NODE, income - spending);
  }

  return {
    data: { nodes: Array.from(nodes.values()), links: Array.from(links.values()) } as SnakeyData,
    income,
    spending,
    fromSavings,
    transactionCount,
    hiddenCount
  };
};
