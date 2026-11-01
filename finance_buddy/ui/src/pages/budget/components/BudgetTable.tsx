// Karta z tabelą budżetu: grupy kategorii (zwijane) z wierszami dzieci.
import { Fragment, useCallback, useState } from 'react';
import { FILTER_LABELS, sumGroupTotals, type BudgetStatusFilter, type CategoryNode } from '../model';
import type { BudgetEditing } from '../useBudgetEditing';
import BudgetRow from './BudgetRow';

type BudgetTableProps = {
  categoryTree: CategoryNode[];
  statusFilter: BudgetStatusFilter;
  error: string | null;
  loading: boolean;
  hasBudget: boolean;
  editing: BudgetEditing;
};

const BudgetTable = ({ categoryTree, statusFilter, error, loading, hasBudget, editing }: BudgetTableProps) => {
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

  return (
    <section className="card budget-table-card">
      <div className="budget-table-toolbar">
        <button type="button" className="budget-table-toolbar__action">
          + Category Group
        </button>
        <span className="budget-table-state">Filtr: {FILTER_LABELS[statusFilter]}</span>
      </div>

      {error && (
        <div className="budget-table-alert">
          <div className="alert alert-danger mb-0" role="alert">
            {error}
          </div>
        </div>
      )}

      <div className="budget-table-shell">
        {loading && !hasBudget ? (
          <div className="budget-table-loading text-secondary">Ładuję budżet…</div>
        ) : (
          <div className="table-responsive budget-table-wrap">
            <table className="table table-vcenter budget-table mb-0">
              <thead>
                <tr>
                  <th>
                    <span className="budget-header-category">
                      <span className="budget-header-disclosure" aria-hidden="true">
                        ˅
                      </span>
                      <span className="budget-row-select" aria-hidden="true" />
                      <span>Tag</span>
                    </span>
                  </th>
                  <th className="text-end">Przypisane</th>
                  <th className="text-end">Aktywność</th>
                  <th className="text-end">Dostępne</th>
                </tr>
              </thead>
              <tbody>
                {categoryTree.length === 0 && (
                  <tr>
                    <td colSpan={4} className="text-center text-secondary py-4">
                      {statusFilter === 'all'
                        ? 'Brak tagów do budżetowania.'
                        : 'Brak tagów pasujących do wybranego filtra.'}
                    </td>
                  </tr>
                )}
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
                        editing={editing}
                        loading={loading}
                      />
                      {!isCollapsed &&
                        node.children.map((child) => (
                          <BudgetRow key={child.category_id} row={child} depth={1} editing={editing} loading={loading} />
                        ))}
                    </Fragment>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </section>
  );
};

export default BudgetTable;
