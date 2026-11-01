// Wspólny stan i szkielet commita dla inline edycji tagu w komórce tabeli
// (dwuklik → TagPathInput, Enter/blur → ścieżka → id → zapis encji).
// Strona wstrzykuje odczyt bieżącego tagu, zamianę ścieżki na id (ensureCategory)
// oraz zapis encji z podmianą w liście — hook nie zna typu encji ani API.
import { useCallback, useEffect, useState } from 'react';
import { GRID_NAVIGATION_KEYS } from '../../utils/gridFocus';

type UseTagCellEditingParams<T extends { id: string }> = {
  entities: T[];
  /** Id tagu przypisanego do encji (null gdy brak) — do pominięcia zapisu bez zmian. */
  getCurrentCategoryId: (entity: T) => string | null;
  /** Wartość pola na starcie edycji (ścieżka/etykieta bieżącego tagu albo ''). */
  getInitialValue: (entity: T) => string;
  /** Zamienia wpisaną ścieżkę tagu na id, tworząc brakujące tagi. */
  resolveCategoryId: (path: string) => Promise<string>;
  /** Zapisuje nowy tag encji i podmienia ją w liście strony. */
  save: (entity: T, categoryId: string | null) => Promise<void>;
  /** Komunikat błędu strony (inlineError). */
  setError: (message: string | null) => void;
  errorMessages: {
    /** Gdy nie uda się utworzyć/odnaleźć tagu ze ścieżki. */
    resolve: string;
    /** Gdy nie uda się zapisać encji. */
    save: string;
  };
};

export const useTagCellEditing = <T extends { id: string }>({
  entities,
  getCurrentCategoryId,
  getInitialValue,
  resolveCategoryId,
  save,
  setError,
  errorMessages
}: UseTagCellEditingParams<T>) => {
  const [editingId, setEditingId] = useState<string | null>(null);
  const [editValue, setEditValue] = useState('');
  const [savingId, setSavingId] = useState<string | null>(null);

  const startEditing = useCallback(
    (entity: T) => {
      if (savingId) return;
      setEditingId(entity.id);
      setEditValue(getInitialValue(entity));
      setError(null);
    },
    [savingId, getInitialValue, setError]
  );

  const cancelEditing = useCallback(() => {
    if (savingId) return;
    setEditingId(null);
    setEditValue('');
  }, [savingId]);

  const commitEdit = async (valueOverride?: string) => {
    if (!editingId) return;
    const entity = entities.find((item) => item.id === editingId);
    if (!entity) {
      cancelEditing();
      return;
    }

    const normalized = (valueOverride ?? editValue).trim();

    let nextCategoryId: string | null;
    try {
      nextCategoryId = normalized.length ? await resolveCategoryId(normalized) : null;
    } catch (err) {
      console.error(err);
      setError(errorMessages.resolve);
      cancelEditing();
      return;
    }

    if ((getCurrentCategoryId(entity) ?? null) === (nextCategoryId ?? null)) {
      cancelEditing();
      return;
    }

    try {
      setSavingId(entity.id);
      await save(entity, nextCategoryId);
      setError(null);
    } catch (err) {
      console.error(err);
      setError(errorMessages.save);
    } finally {
      setSavingId(null);
      cancelEditing();
    }
  };

  // W trakcie edycji tagu klawisze nawigacji nie mogą trafiać do react-data-grid.
  useEffect(() => {
    if (editingId === null) return;

    const handleKeyDownCapture = (event: KeyboardEvent) => {
      if (!GRID_NAVIGATION_KEYS.has(event.key)) return;
      const activeElement = document.activeElement as HTMLElement | null;
      if (!activeElement?.closest('.editable-cell')) return;
      // Wewnątrz autocomplete strzałki nawigują po podpowiedziach — obsługuje
      // (i zatrzymuje) je handler inputa; blokada tutaj by je zabiła, zanim
      // dotrą do pola.
      if (activeElement.closest('.lookup-autocomplete')) return;
      event.stopPropagation();
      event.stopImmediatePropagation();
    };

    window.addEventListener('keydown', handleKeyDownCapture, true);
    return () => window.removeEventListener('keydown', handleKeyDownCapture, true);
  }, [editingId]);

  return {
    editingId,
    savingId,
    editValue,
    setEditValue,
    startEditing,
    cancelEditing,
    commitEdit
  };
};
