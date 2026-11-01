// Wiersz tabeli budżetu: grupa (sumy dzieci, przełącznik zwijania) albo kategoria
// (pasek koperty, edytowalne „Przypisane").
import { useEffect, useRef } from 'react';
import { ChevronRight } from 'lucide-react';
import type { BudgetCategoryRow } from '../../../api';
import EnvelopeProgress from '../../../components/ui/EnvelopeProgress';
import Money from '../../../components/ui/Money';
import TruncatedText from '../../../components/ui/TruncatedText';
import { copy } from '../../../copy/pl';
import { buildEnvelopeUsage, formatCategoryName, resolveAvailableTone } from '../model';
import type { AdjacentRows, BudgetEditing } from '../useBudgetEditing';

type AssignedCellProps = {
  row: BudgetCategoryRow;
  adjacent: AdjacentRows;
  editing: BudgetEditing;
  loading: boolean;
};

const AssignedCell = ({ row, adjacent, editing, loading }: AssignedCellProps) => {
  const isEditing = editing.editingCategoryId === row.category_id;
  const isSaving = editing.savingCategoryId === row.category_id;
  const buttonRef = useRef<HTMLButtonElement>(null);
  const wasEditing = useRef(false);

  // Po Enter lub Esc fokus wraca na komórkę; Tab przenosi go sam na następne pole
  useEffect(() => {
    if (wasEditing.current && !isEditing && document.activeElement === document.body) {
      buttonRef.current?.focus();
    }
    wasEditing.current = isEditing;
  }, [isEditing]);

  if (isEditing) {
    return (
      <input
        type="text"
        className="budget-assign-input"
        aria-label={copy.budget.editAssigned(row.category_name)}
        value={editing.editValue}
        autoFocus
        onChange={(event) => editing.setEditValue(event.target.value)}
        onFocus={editing.handleEditFocus}
        onBlur={editing.handleEditBlur}
        onKeyDown={(event) => editing.handleEditKeyDown(event, adjacent)}
        disabled={isSaving}
        inputMode="decimal"
      />
    );
  }

  return (
    <button
      ref={buttonRef}
      type="button"
      className="budget-assign-button"
      aria-label={copy.budget.editAssigned(row.category_name)}
      onClick={() => editing.startEditing(row)}
      disabled={loading}
    >
      <Money cents={row.assigned_cents} tone="neutral" />
    </button>
  );
};

type BudgetRowProps = {
  row: BudgetCategoryRow;
  depth: number;
  // Kwoty do wyświetlenia zamiast własnych kwot wiersza (sumy grupy)
  totalsOverride?: BudgetCategoryRow;
  hasChildren?: boolean;
  isCollapsed?: boolean;
  onToggle?: () => void;
  adjacent: AdjacentRows;
  editing: BudgetEditing;
  loading: boolean;
};

const BudgetRow = ({
  row,
  depth,
  totalsOverride,
  hasChildren = false,
  isCollapsed = false,
  onToggle,
  adjacent,
  editing,
  loading
}: BudgetRowProps) => {
  const dataRow = totalsOverride ?? row;
  const usage = hasChildren ? null : buildEnvelopeUsage(dataRow);
  const toggleLabel = isCollapsed ? copy.budget.expandGroup : copy.budget.collapseGroup;

  return (
    <tr className={hasChildren ? 'budget-row budget-row--group' : 'budget-row'}>
      <td>
        <div className={depth > 0 ? 'budget-category budget-category--child' : 'budget-category'}>
          {hasChildren && (
            <button
              type="button"
              className={`budget-toggle${isCollapsed ? '' : ' is-open'}`}
              onClick={onToggle}
              aria-label={toggleLabel}
              aria-expanded={!isCollapsed}
            >
              <ChevronRight size={16} aria-hidden="true" />
            </button>
          )}
          <div className="budget-category__copy">
            <TruncatedText text={formatCategoryName(row.category_name, depth)} />
            {usage !== null && <EnvelopeProgress percent={usage} />}
          </div>
        </div>
      </td>
      <td className="budget-table__amount">
        {hasChildren ? (
          <Money cents={dataRow.assigned_cents} tone="neutral" />
        ) : (
          <AssignedCell row={row} adjacent={adjacent} editing={editing} loading={loading} />
        )}
      </td>
      <td className="budget-table__amount budget-table__activity">
        <Money cents={dataRow.activity_cents} />
      </td>
      <td className="budget-table__amount">
        {hasChildren ? (
          <Money cents={dataRow.available_cents} tone={dataRow.available_cents < 0 ? 'negative' : 'neutral'} />
        ) : (
          <Money cents={dataRow.available_cents} tone={resolveAvailableTone(dataRow)} chip />
        )}
      </td>
    </tr>
  );
};

export default BudgetRow;
