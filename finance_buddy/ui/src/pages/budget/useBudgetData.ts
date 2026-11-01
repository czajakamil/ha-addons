// Warstwa danych strony budżetu: wybrany miesiąc oraz — tylko dla miesięcy, w których
// działa Auto-Assign — poprzedni miesiąc jako punkt odniesienia dla jego opcji.
// Celowo nie używa useApi — fetcher przyjmuje klucz miesiąca, a zapisy przeładowują
// dane przez loadBudget.
import { useCallback, useEffect, useMemo, useState } from 'react';
import { BudgetMonthResponse, getBudgetMonth } from '../../api';
import { copy } from '../../copy/pl';
import { parseMonthKey, shiftMonth, toMonthKey } from './model';

export const useBudgetData = (activeMonth: string) => {
  const [budget, setBudget] = useState<BudgetMonthResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [lastMonthBudget, setLastMonthBudget] = useState<BudgetMonthResponse | null>(null);
  const activeMonthDate = useMemo(() => parseMonthKey(activeMonth), [activeMonth]);
  const previousMonthKey = useMemo(() => toMonthKey(shiftMonth(activeMonthDate, -1)), [activeMonthDate]);
  const autoAssignAllowedMonths = useMemo(() => {
    const now = new Date();
    return new Set([toMonthKey(now), toMonthKey(shiftMonth(now, 1))]);
  }, []);

  const loadBudget = useCallback(async (monthKey: string) => {
    try {
      setLoading(true);
      setError(null);
      const data = await getBudgetMonth(monthKey);
      setBudget(data);
    } catch (err) {
      console.error(err);
      setError(copy.budget.loadFailed);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadBudget(activeMonth);
  }, [activeMonth, loadBudget]);

  useEffect(() => {
    let cancelled = false;

    if (!autoAssignAllowedMonths.has(previousMonthKey) || previousMonthKey === activeMonth) {
      setLastMonthBudget(null);
      return () => {
        cancelled = true;
      };
    }

    const loadPreviousMonth = async () => {
      try {
        const data = await getBudgetMonth(previousMonthKey);
        if (!cancelled) {
          setLastMonthBudget(data);
        }
      } catch {
        if (!cancelled) {
          setLastMonthBudget(null);
        }
      }
    };

    void loadPreviousMonth();

    return () => {
      cancelled = true;
    };
  }, [activeMonth, autoAssignAllowedMonths, previousMonthKey]);

  return {
    budget,
    loading,
    error,
    setError,
    loadBudget,
    lastMonthBudget,
    activeMonthDate
  };
};
