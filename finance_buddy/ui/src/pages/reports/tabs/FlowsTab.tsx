import { useMemo } from 'react';
import { ResponsiveSankey } from '@nivo/sankey';
import {
  SNAKEY_MAX_TRANSACTIONS,
  SNAKEY_OTHER_CATEGORY_ID,
  SNAKEY_OTHER_CATEGORY_LABEL,
  SNAKEY_SPENDING_CATEGORY_LIMIT,
  UNCATEGORIZED_CATEGORY_ID,
  UNCATEGORIZED_CATEGORY_NAME,
  isExcludedCategory,
  moneyLabel,
  normalizeCategoryName,
  type SnakeyData,
  type SnakeyLink,
  type SnakeyNode
} from '../model';
import { chartColor } from '../../../utils/tokens';
import { useChartTheme } from '../useChartTheme';
import type { ReportsController } from '../useReports';

const INCOME_NODE = 'income';
const SURPLUS_NODE = 'surplus';

type FlowsTabProps = {
  controller: ReportsController;
};

const FlowsTab = ({ controller }: FlowsTabProps) => {
  const chartTheme = useChartTheme();
  const {
    snakeyTransactions,
    snakeyLoading,
    snakeyError,
    snakeyLimitReached,
    selectedAccountIds,
    selectedCategoryIds,
    allAccountOptionIds,
    allCategoryOptionIds,
    categories,
    categoryNameLookup
  } = controller;

  const categoryPaths = useMemo(
    () => new Map(categories.map((category) => [category.id, category.path ?? category.name])),
    [categories]
  );

  // Przychody → grupy kategorii → kategorie. Ewentualna nadwyżka przychodów to osobna gałąź.
  const flows = useMemo(() => {
    const hasAccountFilter =
      selectedAccountIds.length > 0 && selectedAccountIds.length !== allAccountOptionIds.length;
    const accountFilter = hasAccountFilter ? new Set(selectedAccountIds) : null;
    const hasCategoryFilter =
      selectedCategoryIds.length > 0 && selectedCategoryIds.length !== allCategoryOptionIds.length;
    const categoryFilter = hasCategoryFilter ? new Set(selectedCategoryIds) : null;

    const totals = new Map<string, { group: string; name: string; value: number }>();
    let income = 0;
    let spending = 0;
    let transactionCount = 0;

    snakeyTransactions.forEach((transaction) => {
      if (transaction.transfer_account_id) return;
      if (accountFilter && !accountFilter.has(transaction.account_id)) return;
      if (transaction.total_cents > 0) {
        income += transaction.total_cents;
        return;
      }
      if (transaction.total_cents === 0) return;

      const categoryId = transaction.category_id ?? UNCATEGORIZED_CATEGORY_ID;
      if (categoryFilter && !categoryFilter.has(categoryId)) return;
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

    if (spending > 0) addNode(INCOME_NODE, 'Przychody', 'account');
    sorted.forEach(([id, entry], index) => {
      const isTop = index < SNAKEY_SPENDING_CATEGORY_LIMIT;
      const groupId = isTop ? `group:${entry.group}` : `group:${SNAKEY_OTHER_CATEGORY_ID}`;
      const leafId = isTop ? `cat:${id}` : `cat:${SNAKEY_OTHER_CATEGORY_ID}`;
      addNode(groupId, isTop ? entry.group : SNAKEY_OTHER_CATEGORY_LABEL, 'category');
      // Kategoria bez grupy ma jedną nazwę na obu poziomach; wtedy nie powtarzamy jej dwa razy
      addLink(INCOME_NODE, groupId, entry.value);
      if (isTop && entry.group === entry.name) return;
      if (!isTop) return;
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
      transactionCount,
      hiddenCount
    };
  }, [
    allAccountOptionIds,
    allCategoryOptionIds,
    categoryNameLookup,
    categoryPaths,
    selectedAccountIds,
    selectedCategoryIds,
    snakeyTransactions
  ]);

  return (
    <section className="card report-card report-card--full">
      <div className="report-card__header">
        <div>
          <h3>Przepływy</h3>
          <p className="report-card__subtitle">Przychody, grupy kategorii i kategorie w wybranym okresie.</p>
        </div>
        <div className="report-summary-grid">
          <div>
            <p>Przychody</p>
            <strong>{moneyLabel(flows.income)}</strong>
          </div>
          <div>
            <p>Wydatki</p>
            <strong>{moneyLabel(flows.spending)}</strong>
          </div>
        </div>
      </div>
      {snakeyLoading ? (
        <p className="report-empty">Ładowanie diagramu...</p>
      ) : snakeyError ? (
        <p className="report-empty">{snakeyError}</p>
      ) : flows.data.links.length === 0 ? (
        <p className="report-empty">Brak przepływów w wybranym okresie.</p>
      ) : (
        <>
          <div
            className="snakey-chart snakey-chart--full"
            role="img"
            aria-label="Przychody rozdzielone na grupy kategorii i kategorie"
          >
            <ResponsiveSankey
              data={flows.data}
              margin={{ top: 16, right: 160, bottom: 16, left: 96 }}
              align="justify"
              sort="descending"
              colors={[1, 2, 3, 4, 5, 6, 7, 8].map((slot) => chartColor(slot - 1))}
              nodeOpacity={1}
              nodeThickness={12}
              nodeSpacing={16}
              label={(node) => String((node as SnakeyNode).name ?? node.id)}
              labelTextColor={chartTheme.text}
              theme={chartTheme.nivoTheme}
              valueFormat={(value) => moneyLabel(Number(value))}
              linkOpacity={0.3}
              linkHoverOthersOpacity={0.1}
            />
          </div>
          {flows.hiddenCount > 0 && (
            <p className="snakey-note">
              Mniejsze kategorie ({flows.hiddenCount}) są połączone w „{SNAKEY_OTHER_CATEGORY_LABEL}”.
            </p>
          )}
          {snakeyLimitReached && (
            <p className="snakey-note">
              Diagram oparty na pierwszych {SNAKEY_MAX_TRANSACTIONS} transakcjach z wybranego
              zakresu. Zawęź daty lub filtry, aby zobaczyć pełny obraz.
            </p>
          )}
        </>
      )}
    </section>
  );
};

export default FlowsTab;
