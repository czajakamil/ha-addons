// Edycja przypisań budżetu: inline edycja kwoty jednej kategorii oraz Auto-Assign,
// który ustawia przypisania wszystkich liści naraz. Po każdym zapisie przeładowuje
// miesiąc przez loadBudget z useBudgetData.
import { useCallback, useMemo, useState, type KeyboardEvent } from 'react';
import { BudgetCategoryRow, BudgetMonthResponse, updateBudgetAssignment } from '../../api';
import { parseAmountToCents } from '../../utils/format';
import {
  buildAutoAssignTotals,
  buildAutoAssignUpdates,
  buildLastMonthCategoryMap,
  type AutoAssignMode
} from './model';

type UseBudgetEditingParams = {
  activeMonth: string;
  leafCategories: BudgetCategoryRow[];
  lastMonthBudget: BudgetMonthResponse | null;
  loading: boolean;
  loadBudget: (monthKey: string) => Promise<void>;
  setError: (error: string | null) => void;
};

export const useBudgetEditing = ({
  activeMonth,
  leafCategories,
  lastMonthBudget,
  loading,
  loadBudget,
  setError
}: UseBudgetEditingParams) => {
  const [editingCategoryId, setEditingCategoryId] = useState<string | null>(null);
  const [editValue, setEditValue] = useState('');
  const [savingCategoryId, setSavingCategoryId] = useState<string | null>(null);
  const [autoAssignMenuOpen, setAutoAssignMenuOpen] = useState(false);
  const [autoAssignApplying, setAutoAssignApplying] = useState(false);

  const lastMonthCategoryMap = useMemo(
    () => buildLastMonthCategoryMap(lastMonthBudget?.categories ?? []),
    [lastMonthBudget?.categories]
  );

  const autoAssignTotals = useMemo(
    () => buildAutoAssignTotals(leafCategories, lastMonthCategoryMap),
    [leafCategories, lastMonthCategoryMap]
  );

  const applyAutoAssign = useCallback(
    async (mode: AutoAssignMode) => {
      if (autoAssignApplying) return;

      setAutoAssignMenuOpen(false);
      if (!leafCategories.length) return;

      const updates = buildAutoAssignUpdates(mode, leafCategories, lastMonthCategoryMap, activeMonth);
      if (!updates.length) return;

      try {
        setAutoAssignApplying(true);
        setError(null);
        await Promise.all(updates.map((payload) => updateBudgetAssignment(payload)));
        await loadBudget(activeMonth);
      } catch (err) {
        console.error(err);
        setError('Nie udało się zastosować Auto-Assign.');
      } finally {
        setAutoAssignApplying(false);
      }
    },
    [activeMonth, autoAssignApplying, lastMonthCategoryMap, leafCategories, loadBudget, setError]
  );

  const autoAssignDisabled = loading || autoAssignApplying || savingCategoryId !== null || !leafCategories.length;

  const startEditing = (row: BudgetCategoryRow) => {
    if (savingCategoryId) return;
    setEditingCategoryId(row.category_id);
    setEditValue((row.assigned_cents / 100).toFixed(2));
    setError(null);
  };

  const cancelEditing = () => {
    if (savingCategoryId) return;
    setEditingCategoryId(null);
    setEditValue('');
  };

  const commitEdit = async () => {
    if (!editingCategoryId || savingCategoryId) return;
    const nextValue = parseAmountToCents(editValue);
    if (nextValue === null) {
      setError('Podaj poprawną kwotę (np. 123.45).');
      return;
    }

    try {
      setSavingCategoryId(editingCategoryId);
      await updateBudgetAssignment({
        category_id: editingCategoryId,
        month: activeMonth,
        assigned_cents: nextValue
      });
      await loadBudget(activeMonth);
      setError(null);
      setEditingCategoryId(null);
      setEditValue('');
    } catch (err) {
      console.error(err);
      setError('Nie udało się zapisać budżetu.');
    } finally {
      setSavingCategoryId(null);
    }
  };

  const handleEditKeyDown = (event: KeyboardEvent<HTMLInputElement>) => {
    if (event.key === 'Escape') {
      event.preventDefault();
      cancelEditing();
      return;
    }
    if (event.key === 'Enter') {
      event.preventDefault();
      void commitEdit();
    }
  };

  return {
    editingCategoryId,
    editValue,
    setEditValue,
    savingCategoryId,
    startEditing,
    commitEdit,
    handleEditKeyDown,
    autoAssignMenuOpen,
    setAutoAssignMenuOpen,
    autoAssignTotals,
    autoAssignDisabled,
    applyAutoAssign
  };
};

export type BudgetEditing = ReturnType<typeof useBudgetEditing>;
