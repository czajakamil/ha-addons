import { useMemo } from 'react';
import { ResponsiveSankey } from '@nivo/sankey';
import type { Transaction } from '../../../api';
import {
  SNAKEY_MAX_TRANSACTIONS,
  SNAKEY_OTHER_CATEGORY_ID,
  SNAKEY_OTHER_CATEGORY_LABEL,
  SNAKEY_SPENDING_CATEGORY_LIMIT,
  SNAKEY_TRANSFER_DIRECTION,
  SNAKEY_TRANSFER_LINK_LIMIT,
  SNAKEY_UNKNOWN_ACCOUNT_LABEL,
  UNCATEGORIZED_CATEGORY_ID,
  UNCATEGORIZED_CATEGORY_NAME,
  isExcludedCategory,
  moneyLabel,
  shouldReverseTransferDirection,
  type SnakeyData,
  type SnakeyLink,
  type SnakeyNode,
  type SnakeySummary
} from '../model';
import { useChartTheme } from '../useChartTheme';
import type { ReportsController } from '../useReports';

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
    accountLookup,
    categoryNameLookup
  } = controller;

  const snakeySpendingSummary = useMemo<SnakeySummary>(() => {
    const empty: SnakeySummary = {
      data: { nodes: [], links: [] },
      total: 0,
      displayedTotal: 0,
      transactionCount: 0,
      hiddenCount: 0
    };

    if (!snakeyTransactions.length) {
      return empty;
    }

    const hasAccountFilter =
      selectedAccountIds.length > 0 && selectedAccountIds.length !== allAccountOptionIds.length;
    const accountFilterSet = hasAccountFilter ? new Set(selectedAccountIds) : null;
    const hasCategoryFilter =
      selectedCategoryIds.length > 0 && selectedCategoryIds.length !== allCategoryOptionIds.length;
    const categoryFilterSet = hasCategoryFilter ? new Set(selectedCategoryIds) : null;
    const allowUncategorized =
      !categoryFilterSet || categoryFilterSet.has(UNCATEGORIZED_CATEGORY_ID);
    const categoryIdFilterSet = categoryFilterSet
      ? new Set(Array.from(categoryFilterSet).filter((id) => id !== UNCATEGORIZED_CATEGORY_ID))
      : null;

    const categoryTotals = new Map<string, number>();
    const flows: { accountId: string; categoryId: string; value: number }[] = [];
    let total = 0;
    let transactionCount = 0;

    snakeyTransactions.forEach((transaction) => {
      if (transaction.total_cents >= 0) return;
      if (transaction.transfer_account_id) return;
      if (accountFilterSet && !accountFilterSet.has(transaction.account_id)) return;

      const categoryId = transaction.category_id ?? UNCATEGORIZED_CATEGORY_ID;
      if (categoryId === UNCATEGORIZED_CATEGORY_ID) {
        if (!allowUncategorized) return;
      } else if (categoryIdFilterSet && !categoryIdFilterSet.has(categoryId)) {
        return;
      }

      const categoryName =
        categoryId === UNCATEGORIZED_CATEGORY_ID
          ? UNCATEGORIZED_CATEGORY_NAME
          : categoryNameLookup.get(categoryId) ?? 'Tag';
      if (isExcludedCategory(categoryName)) {
        return;
      }

      const amount = Math.abs(transaction.total_cents);
      total += amount;
      transactionCount += 1;
      categoryTotals.set(categoryId, (categoryTotals.get(categoryId) ?? 0) + amount);
      flows.push({ accountId: transaction.account_id, categoryId, value: amount });
    });

    if (!flows.length) {
      return empty;
    }

    const sortedCategoryIds = Array.from(categoryTotals.entries())
      .sort((a, b) => b[1] - a[1])
      .map(([id]) => id);
    const topCategoryIds = new Set(sortedCategoryIds.slice(0, SNAKEY_SPENDING_CATEGORY_LIMIT));
    if (categoryTotals.has(UNCATEGORIZED_CATEGORY_ID)) {
      topCategoryIds.add(UNCATEGORIZED_CATEGORY_ID);
    }
    const hiddenCount = Math.max(0, categoryTotals.size - topCategoryIds.size);

    const linkTotals = new Map<string, Map<string, number>>();
    flows.forEach(({ accountId, categoryId, value }) => {
      const resolvedCategoryId =
        categoryId === UNCATEGORIZED_CATEGORY_ID || topCategoryIds.has(categoryId)
          ? categoryId
          : SNAKEY_OTHER_CATEGORY_ID;
      const source = `acc:${accountId}`;
      const target = `cat:${resolvedCategoryId}`;
      const targets = linkTotals.get(source) ?? new Map();
      targets.set(target, (targets.get(target) ?? 0) + value);
      linkTotals.set(source, targets);
    });

    const nodeMap = new Map<string, SnakeyNode>();
    const ensureNode = (id: string, name: string, type: SnakeyNode['type']) => {
      if (!nodeMap.has(id)) {
        nodeMap.set(id, { id, name, type });
      }
    };

    linkTotals.forEach((targets, sourceId) => {
      const accountId = sourceId.replace('acc:', '');
      const accountName = accountLookup.get(accountId)?.name ?? SNAKEY_UNKNOWN_ACCOUNT_LABEL;
      ensureNode(sourceId, accountName, 'account');

      targets.forEach((_, targetId) => {
        const categoryId = targetId.replace('cat:', '');
        let categoryName = SNAKEY_OTHER_CATEGORY_LABEL;
        if (categoryId === UNCATEGORIZED_CATEGORY_ID) {
          categoryName = UNCATEGORIZED_CATEGORY_NAME;
        } else if (categoryId !== SNAKEY_OTHER_CATEGORY_ID) {
          categoryName = categoryNameLookup.get(categoryId) ?? 'Tag';
        }
        ensureNode(targetId, categoryName, 'category');
      });
    });

    const links: SnakeyLink[] = [];
    linkTotals.forEach((targets, source) => {
      targets.forEach((value, target) => {
        links.push({ source, target, value });
      });
    });

    return {
      data: { nodes: Array.from(nodeMap.values()), links },
      total,
      displayedTotal: total,
      transactionCount,
      hiddenCount
    };
  }, [
    accountLookup,
    allAccountOptionIds,
    allCategoryOptionIds,
    categoryNameLookup,
    selectedAccountIds,
    selectedCategoryIds,
    snakeyTransactions
  ]);

  const snakeyTransferSummary = useMemo<SnakeySummary>(() => {
    const empty: SnakeySummary = {
      data: { nodes: [], links: [] },
      total: 0,
      displayedTotal: 0,
      transactionCount: 0,
      hiddenCount: 0
    };

    if (!snakeyTransactions.length) {
      return empty;
    }

    const hasAccountFilter =
      selectedAccountIds.length > 0 && selectedAccountIds.length !== allAccountOptionIds.length;
    const accountFilterSet = hasAccountFilter ? new Set(selectedAccountIds) : null;

    const linkTotals = new Map<string, Map<string, number>>();
    let total = 0;
    let transactionCount = 0;

    const transferGroups = new Map<string, Transaction[]>();
    snakeyTransactions.forEach((transaction) => {
      if (!transaction.transfer_account_id) return;
      const key = transaction.transfer_transaction_id ?? transaction.id;
      const list = transferGroups.get(key) ?? [];
      list.push(transaction);
      transferGroups.set(key, list);
    });

    transferGroups.forEach((transactions) => {
      const outflow = transactions.find((tx) => tx.total_cents < 0);
      const inflow = transactions.find((tx) => tx.total_cents > 0);
      const chosen = outflow ?? inflow ?? transactions[0];
      if (!chosen?.transfer_account_id) return;

      let sourceAccountId = outflow
        ? outflow.account_id
        : inflow
        ? inflow.transfer_account_id
        : chosen.account_id;
      let targetAccountId = outflow
        ? outflow.transfer_account_id
        : inflow
        ? inflow.account_id
        : chosen.transfer_account_id;

      if (!sourceAccountId || !targetAccountId) return;

      if (shouldReverseTransferDirection(SNAKEY_TRANSFER_DIRECTION)) {
        [sourceAccountId, targetAccountId] = [targetAccountId, sourceAccountId];
      }

      if (accountFilterSet && !accountFilterSet.has(sourceAccountId) && !accountFilterSet.has(targetAccountId)) {
        return;
      }

      const amount = Math.abs(outflow?.total_cents ?? inflow?.total_cents ?? chosen.total_cents);
      if (!amount) return;

      total += amount;
      transactionCount += 1;

      const source = `acc:${sourceAccountId}`;
      const target = `acc:${targetAccountId}`;
      const targets = linkTotals.get(source) ?? new Map();
      targets.set(target, (targets.get(target) ?? 0) + amount);
      linkTotals.set(source, targets);
    });

    const links: SnakeyLink[] = [];
    linkTotals.forEach((targets, source) => {
      targets.forEach((value, target) => {
        links.push({ source, target, value });
      });
    });

    if (!links.length) {
      return empty;
    }

    const sortedLinks = links.slice().sort((a, b) => b.value - a.value);
    const limitedLinks = sortedLinks.slice(0, SNAKEY_TRANSFER_LINK_LIMIT);
    const hiddenCount = Math.max(0, sortedLinks.length - limitedLinks.length);
    const displayedTotal = limitedLinks.reduce((sum, link) => sum + link.value, 0);

    const nodeMap = new Map<string, SnakeyNode>();
    const ensureNode = (id: string, name: string, type: SnakeyNode['type']) => {
      if (!nodeMap.has(id)) {
        nodeMap.set(id, { id, name, type });
      }
    };

    limitedLinks.forEach((link) => {
      const sourceAccountId = link.source.replace('acc:', '');
      const targetAccountId = link.target.replace('acc:', '');
      const sourceName = accountLookup.get(sourceAccountId)?.name ?? SNAKEY_UNKNOWN_ACCOUNT_LABEL;
      const targetName = accountLookup.get(targetAccountId)?.name ?? SNAKEY_UNKNOWN_ACCOUNT_LABEL;
      ensureNode(link.source, sourceName, 'account');
      ensureNode(link.target, targetName, 'account');
    });

    return {
      data: { nodes: Array.from(nodeMap.values()), links: limitedLinks },
      total,
      displayedTotal,
      transactionCount,
      hiddenCount
    };
  }, [accountLookup, allAccountOptionIds, selectedAccountIds, snakeyTransactions]);

  const snakeyCombinedData = useMemo<SnakeyData>(() => {
    if (
      snakeySpendingSummary.data.links.length === 0 &&
      snakeyTransferSummary.data.links.length === 0
    ) {
      return { nodes: [], links: [] };
    }

    const nodeMap = new Map<string, SnakeyNode>();
    snakeySpendingSummary.data.nodes.forEach((node) => nodeMap.set(node.id, node));
    snakeyTransferSummary.data.nodes.forEach((node) => nodeMap.set(node.id, node));

    return {
      nodes: Array.from(nodeMap.values()),
      links: [...snakeySpendingSummary.data.links, ...snakeyTransferSummary.data.links]
    };
  }, [snakeySpendingSummary, snakeyTransferSummary]);

  return (
    <section className="card report-card report-card--full">
      <div className="report-card__header">
        <div>
          <h3>Snakey</h3>
          <p className="report-card__subtitle">
            Przepływ pieniędzy z kont do kategorii oraz transfery między kontami.
          </p>
        </div>
        <div className="report-summary-grid">
          <div>
            <p>Wydatki</p>
            <strong>{moneyLabel(snakeySpendingSummary.total)}</strong>
          </div>
          <div>
            <p>Transfery między kontami</p>
            <strong>{moneyLabel(snakeyTransferSummary.total)}</strong>
          </div>
        </div>
      </div>
      {snakeyLoading ? (
        <p className="report-empty">Ładowanie diagramu...</p>
      ) : snakeyError ? (
        <p className="report-empty">{snakeyError}</p>
      ) : (
        <>
          <div className="snakey-grid">
            <div className="snakey-panel">
              <div className="snakey-panel__header">
                <div>
                  <h4 className="snakey-panel__title">Wydatki + transfery</h4>
                  <p className="snakey-panel__meta">
                    {snakeySpendingSummary.transactionCount} wydatków •{' '}
                    {snakeyTransferSummary.transactionCount} transferów
                  </p>
                </div>
                <div className="snakey-panel__badges">
                  {snakeySpendingSummary.hiddenCount > 0 && (
                    <span className="snakey-badge">
                      +{snakeySpendingSummary.hiddenCount} mniejszych kategorii
                    </span>
                  )}
                  {snakeyTransferSummary.hiddenCount > 0 && (
                    <span className="snakey-badge">
                      +{snakeyTransferSummary.hiddenCount} mniejszych przepływów
                    </span>
                  )}
                </div>
              </div>
              {snakeyCombinedData.links.length === 0 ? (
                <p className="report-empty">Brak wydatków i transferów w wybranym okresie.</p>
              ) : (
                <div
                  className="snakey-chart snakey-chart--full"
                  role="img"
                  aria-label="Wydatki według kont i kategorii oraz transfery między kontami"
                >
                  <ResponsiveSankey
                    data={snakeyCombinedData}
                    margin={{ top: 20, right: 20, bottom: 20, left: 20 }}
                    align="justify"
                    sort="descending"
                    colors={{ scheme: 'set2' }}
                    nodeOpacity={1}
                    nodeThickness={14}
                    nodeSpacing={18}
                    label={(node) => String((node as SnakeyNode).name ?? node.id)}
                    labelTextColor={chartTheme.text}
                    theme={chartTheme.nivoTheme}
                    valueFormat={(value) => moneyLabel(Number(value))}
                    linkOpacity={0.35}
                    linkHoverOthersOpacity={0.1}
                  />
                </div>
              )}
            </div>
          </div>
          {snakeyLimitReached && (
            <p className="snakey-note">
              Diagram oparty na pierwszych {SNAKEY_MAX_TRANSACTIONS} transakcjach z wybranego
              zakresu. Zawęź daty lub filtry, aby zobaczyć pełny obraz.
            </p>
          )}
          {snakeyTransferSummary.hiddenCount > 0 &&
            snakeyTransferSummary.displayedTotal < snakeyTransferSummary.total && (
              <p className="snakey-note">
                Pokazano {moneyLabel(snakeyTransferSummary.displayedTotal)} z{' '}
                {moneyLabel(snakeyTransferSummary.total)} transferów (reszta została pominięta).
              </p>
            )}
        </>
      )}
    </section>
  );
};

export default FlowsTab;
