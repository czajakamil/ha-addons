// Wiersz tabeli budżetu: grupa (depth 0, sumy dzieci, przełącznik zwijania) albo
// kategoria (depth 1, pasek postępu, edytowalne „Przypisane").
import type { CSSProperties } from 'react';
import type { BudgetCategoryRow } from '../../../api';
import {
  buildCategoryHint,
  buildCategoryProgress,
  formatCategoryName,
  moneyLabel,
  resolveAmountClass,
  resolveAvailableTone
} from '../model';
import type { BudgetEditing } from '../useBudgetEditing';

type AssignedCellProps = {
  row: BudgetCategoryRow;
  editing: BudgetEditing;
  loading: boolean;
};

const AssignedCell = ({ row, editing, loading }: AssignedCellProps) => {
  const isEditing = editing.editingCategoryId === row.category_id;
  const isSaving = editing.savingCategoryId === row.category_id;

  if (isEditing) {
    return (
      <div className="budget-cell budget-cell--editing">
        <input
          type="text"
          className="form-control form-control-sm text-end"
          value={editing.editValue}
          autoFocus
          onChange={(event) => editing.setEditValue(event.target.value)}
          onBlur={() => void editing.commitEdit()}
          onKeyDown={editing.handleEditKeyDown}
          disabled={isSaving}
          inputMode="decimal"
        />
        <div className="budget-cell__meta text-end">{isSaving ? 'Zapisywanie…' : 'Enter aby zapisać'}</div>
      </div>
    );
  }

  return (
    <button
      type="button"
      className="budget-assign-button"
      onClick={() => editing.startEditing(row)}
      title="Kliknij, aby ustawić przypisanie"
      disabled={loading}
    >
      {moneyLabel(row.assigned_cents)}
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
  editing,
  loading
}: BudgetRowProps) => {
  const dataRow = totalsOverride ?? row;
  const hint = depth > 0 ? buildCategoryHint(dataRow) : null;
  const progress = depth > 0 ? buildCategoryProgress(dataRow) : null;
  const rowClass = ['budget-row', depth > 0 ? 'budget-row--child' : 'budget-row--group'].join(' ');
  const toggleLabel = isCollapsed ? 'Rozwiń grupę' : 'Zwiń grupę';
  const progressStyle =
    progress && depth > 0
      ? ({
          ['--budget-progress-available' as const]: `${progress.availablePercent}%`,
          ['--budget-progress-overspent' as const]: `${progress.overspentPercent}%`
        } as CSSProperties)
      : undefined;

  return (
    <tr className={rowClass}>
      <td className="budget-cell budget-cell--category">
        <span className={depth > 0 ? 'budget-category-name budget-category-name--child' : 'budget-category-name'}>
          {!hasChildren && <span className="budget-row-select" aria-hidden="true" />}
          {hasChildren && (
            <button
              type="button"
              className="budget-group-toggle"
              onClick={onToggle}
              aria-label={toggleLabel}
              aria-expanded={!isCollapsed}
            >
              <span className="budget-group-toggle__icon" aria-hidden="true">
                {isCollapsed ? '>' : 'v'}
              </span>
            </button>
          )}
          {hasChildren && <span className="budget-row-select" aria-hidden="true" />}
          <span className="budget-category-copy">
            <span className="budget-category-label">{formatCategoryName(row.category_name, depth)}</span>
            {depth > 0 && (hint || progress) && (
              <span className="budget-category-detail">
                {hint && <span className={`budget-category-hint is-${hint.tone}`}>{hint.text}</span>}
                {progress && <span className="budget-progress budget-progress--inline" style={progressStyle} aria-hidden="true" />}
              </span>
            )}
          </span>
        </span>
      </td>
      <td className="budget-cell budget-cell--assigned text-end">
        {hasChildren ? (
          moneyLabel(dataRow.assigned_cents)
        ) : (
          <AssignedCell row={row} editing={editing} loading={loading} />
        )}
      </td>
      <td className="budget-cell text-end">
        <span className={resolveAmountClass(dataRow, dataRow.activity_cents)}>
          {moneyLabel(dataRow.activity_cents)}
        </span>
      </td>
      <td className="budget-cell text-end">
        {hasChildren ? (
          <span className={['budget-group-total', resolveAmountClass(dataRow, dataRow.available_cents)].join(' ')}>
            {moneyLabel(dataRow.available_cents)}
          </span>
        ) : (
          <span className={['budget-amount-pill', `is-${resolveAvailableTone(dataRow)}`].join(' ')}>
            {moneyLabel(dataRow.available_cents)}
          </span>
        )}
      </td>
    </tr>
  );
};

export default BudgetRow;
