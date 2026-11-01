import { useMemo, useState } from 'react';
import AutoAssignCard from './budget/components/AutoAssignCard';
import BudgetSummaryCard from './budget/components/BudgetSummaryCard';
import BudgetTable from './budget/components/BudgetTable';
import BudgetToolbar from './budget/components/BudgetToolbar';
import {
  buildCategoryTree,
  collectLeafCategories,
  countByFilter,
  filterCategoryTree,
  toMonthKey,
  type BudgetStatusFilter
} from './budget/model';
import { useBudgetData } from './budget/useBudgetData';
import { useBudgetEditing } from './budget/useBudgetEditing';

const BudgetPage = () => {
  const [activeMonth, setActiveMonth] = useState<string>(() => toMonthKey(new Date()));
  const [statusFilter, setStatusFilter] = useState<BudgetStatusFilter>('all');
  const { budget, loading, error, setError, loadBudget, lastMonthBudget, activeMonthDate } =
    useBudgetData(activeMonth);

  const categoryTree = useMemo(() => buildCategoryTree(budget?.categories ?? []), [budget?.categories]);
  const leafCategories = useMemo(() => collectLeafCategories(categoryTree), [categoryTree]);
  const filteredCategoryTree = useMemo(
    () => filterCategoryTree(categoryTree, statusFilter),
    [categoryTree, statusFilter]
  );
  const filterCounts = useMemo(() => countByFilter(leafCategories), [leafCategories]);

  const editing = useBudgetEditing({
    activeMonth,
    leafCategories,
    lastMonthBudget,
    loading,
    loadBudget,
    setError
  });

  return (
    <div className="page-shell budget-page">
      <BudgetToolbar
        activeMonth={activeMonth}
        activeMonthDate={activeMonthDate}
        onMonthChange={setActiveMonth}
        rtaValue={budget?.summary.rta_cents ?? 0}
        statusFilter={statusFilter}
        onStatusFilterChange={setStatusFilter}
        filterCounts={filterCounts}
      />

      <div className="budget-layout">
        <BudgetTable
          categoryTree={filteredCategoryTree}
          statusFilter={statusFilter}
          onClearFilter={() => setStatusFilter('all')}
          error={error}
          loading={loading}
          hasBudget={budget !== null}
          editing={editing}
        />

        <aside className="budget-aside">
          <BudgetSummaryCard summary={budget?.summary} />
          <AutoAssignCard editing={editing} />
        </aside>
      </div>
    </div>
  );
};

export default BudgetPage;
