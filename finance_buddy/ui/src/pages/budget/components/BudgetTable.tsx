// Tabela budżetu: grupy kategorii (zwijane) z wierszami dzieci.
import { Fragment, useCallback, useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import { ListFilter, PiggyBank } from 'lucide-react';
import type { BudgetCategoryRow } from '../../../api';
import EmptyState from '../../../components/ui/EmptyState';
import SkeletonRows from '../../../components/ui/SkeletonRows';
import { copy } from '../../../copy/pl';
import { sumGroupTotals, type BudgetStatusFilter, type CategoryNode } from '../model';
import type { AdjacentRows, BudgetEditing } from '../useBudgetEditing';
import BudgetRow from './BudgetRow';
import ResizableTable from '../../../components/ui/ResizableTable';

type BudgetTableProps = {
  categoryTree: CategoryNode[];
  statusFilter: BudgetStatusFilter;
  onClearFilter: () => void;
  error: string | null;
  loading: boolean;
  hasBudget: boolean;
  editing: BudgetEditing;
};

const BudgetTable = ({
  categoryTree,
  statusFilter,
  onClearFilter,
  error,
  loading,
  hasBudget,
  editing
}: BudgetTableProps) => {
  const [collapsedGroups, setCollapsedGroups] = useState<Set<string>>(new Set());

  const toggleGroup = useCallback((categoryId: string) => {
    setCollapsedGroups((prev) => {
      const next = new Set(prev);
      if (next.has(categoryId)) {
        next.delete(categoryId);
      } else {
        next.add(categoryId);
      }
      return next;
    });
  }, []);

  // Kolejność widocznych pól „Przypisane" dla Tab i Shift+Tab
  const adjacentRows = useMemo(() => {
    const visible: BudgetCategoryRow[] = categoryTree.flatMap((node) => {
      if (!node.children.length) return [node.parent];
      return collapsedGroups.has(node.parent.category_id) ? [] : node.children;
    });
    const adjacent = new Map<string, AdjacentRows>();
    visible.forEach((row, index) => {
      adjacent.set(row.category_id, { previous: visible[index - 1], next: visible[index + 1] });
    });
    return adjacent;
  }, [categoryTree, collapsedGroups]);

  const renderBody = () => {
    if (loading && !hasBudget) {
      return <SkeletonRows />;
    }
    if (!hasBudget) {
      return null;
    }
    if (categoryTree.length === 0) {
      return statusFilter === 'all' ? (
        <EmptyState
          icon={PiggyBank}
          message={copy.budget.emptyAll}
          action={
            <Link to="/categories" className="text-button">
              {copy.budget.addGroup}
            </Link>
          }
        />
      ) : (
        <EmptyState
          icon={ListFilter}
          message={copy.budget.emptyFiltered}
          action={
            <button type="button" className="text-button" onClick={onClearFilter}>
              {copy.budget.showAll}
            </button>
          }
        />
      );
    }
    return (
      <ResizableTable tableId="budget" className="budget-table">
        <thead>
          <tr>
            <th scope="col">{copy.budget.columns.category}</th>
            <th scope="col" className="budget-table__amount">
              {copy.budget.columns.assigned}
            </th>
            <th scope="col" className="budget-table__amount budget-table__activity">
              {copy.budget.columns.activity}
            </th>
            <th scope="col" className="budget-table__amount">
              {copy.budget.columns.available}
            </th>
          </tr>
        </thead>
        <tbody>
          {categoryTree.map((node) => {
            const hasChildren = node.children.length > 0;
            const isCollapsed = hasChildren && collapsedGroups.has(node.parent.category_id);
            return (
              <Fragment key={node.parent.category_id}>
                <BudgetRow
                  row={node.parent}
                  depth={0}
                  totalsOverride={hasChildren ? sumGroupTotals(node) : node.parent}
                  hasChildren={hasChildren}
                  isCollapsed={isCollapsed}
                  onToggle={hasChildren ? () => toggleGroup(node.parent.category_id) : undefined}
                  adjacent={adjacentRows.get(node.parent.category_id) ?? {}}
                  editing={editing}
                  loading={loading}
                />
                {!isCollapsed &&
                  node.children.map((child) => (
                    <BudgetRow
                      key={child.category_id}
                      row={child}
                      depth={1}
                      adjacent={adjacentRows.get(child.category_id) ?? {}}
                      editing={editing}
                      loading={loading}
                    />
                  ))}
              </Fragment>
            );
          })}
        </tbody>
      </ResizableTable>
    );
  };

  return (
    <section className="budget-table-card">
      {error && (
        <p className="budget-error" role="alert">
          {error}
        </p>
      )}
      {renderBody()}
    </section>
  );
};

export default BudgetTable;
