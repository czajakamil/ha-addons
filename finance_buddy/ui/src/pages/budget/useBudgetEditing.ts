// Edycja przypisań budżetu: inline edycja kwoty jednej kategorii oraz Auto-Assign,
// który ustawia przypisania wszystkich liści naraz. Po każdym zapisie przeładowuje
// miesiąc przez loadBudget z useBudgetData.
import { useCallback, useMemo, useRef, useState, type FocusEvent, type KeyboardEvent } from 'react';
import { BudgetCategoryRow, BudgetMonthResponse, updateBudgetAssignment } from '../../api';
import { copy } from '../../copy/pl';
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

// Sąsiednie edytowalne wiersze, między którymi przechodzi Tab i Shift+Tab
export type AdjacentRows = {
  previous?: BudgetCategoryRow;
  next?: BudgetCategoryRow;
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
  // Zapis klawiszem lub anulowanie usuwa pole; blur, który to wywoła, nie może zapisać drugi raz
  const skipBlurCommit = useRef(false);
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
        setError(copy.budget.autoAssign.failed);
      } finally {
        setAutoAssignApplying(false);
      }
    },
    [activeMonth, autoAssignApplying, lastMonthCategoryMap, leafCategories, loadBudget, setError]
  );

  const autoAssignDisabled = loading || autoAssignApplying || savingCategoryId !== null || !leafCategories.length;

  const beginEditing = (row: BudgetCategoryRow) => {
    setEditingCategoryId(row.category_id);
    setEditValue((row.assigned_cents / 100).toFixed(2).replace('.', ','));
    setError(null);
  };

  const stopEditing = () => {
    setEditingCategoryId(null);
    setEditValue('');
  };

  const startEditing = (row: BudgetCategoryRow) => {
    if (savingCategoryId) return;
    skipBlurCommit.current = false;
    beginEditing(row);
  };

  const cancelEditing = () => {
    if (savingCategoryId) return;
    skipBlurCommit.current = true;
    stopEditing();
    setError(null);
  };

  // Zapisuje bieżącą kwotę i przechodzi do `nextRow`. Bez zmiany kwoty nie woła API.
  const commitEdit = async (nextRow?: BudgetCategoryRow) => {
    if (!editingCategoryId || savingCategoryId) return false;
    const nextValue = parseAmountToCents(editValue);
    if (nextValue === null) {
      setError(copy.budget.invalidAmount);
      return false;
    }

    skipBlurCommit.current = true;
    const moveOn = () => {
      if (nextRow) {
        beginEditing(nextRow);
      } else {
        stopEditing();
      }
    };

    const current = leafCategories.find((row) => row.category_id === editingCategoryId);
    if (current?.assigned_cents === nextValue) {
      setError(null);
      moveOn();
      return true;
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
      moveOn();
      return true;
    } catch (err) {
      console.error(err);
      skipBlurCommit.current = false;
      setError(copy.budget.saveFailed);
      return false;
    } finally {
      setSavingCategoryId(null);
    }
  };

  const handleEditFocus = (event: FocusEvent<HTMLInputElement>) => {
    skipBlurCommit.current = false;
    event.target.select();
  };

  const handleEditBlur = () => {
    if (!skipBlurCommit.current) {
      void commitEdit();
    }
  };

  const handleEditKeyDown = (event: KeyboardEvent<HTMLInputElement>, adjacent: AdjacentRows) => {
    if (event.key === 'Escape') {
      event.preventDefault();
      cancelEditing();
      return;
    }
    if (event.key === 'Enter') {
      event.preventDefault();
      void commitEdit();
      return;
    }
    if (event.key === 'Tab') {
      const nextRow = event.shiftKey ? adjacent.previous : adjacent.next;
      // Na końcu listy fokus wychodzi z tabeli naturalnie; blur zapisuje kwotę
      if (nextRow) {
        event.preventDefault();
        void commitEdit(nextRow);
      }
    }
  };

  return {
    editingCategoryId,
    editValue,
    setEditValue,
    savingCategoryId,
    startEditing,
    handleEditFocus,
    handleEditBlur,
    handleEditKeyDown,
    autoAssignMenuOpen,
    setAutoAssignMenuOpen,
    autoAssignTotals,
    autoAssignDisabled,
    applyAutoAssign
  };
};

export type BudgetEditing = ReturnType<typeof useBudgetEditing>;
