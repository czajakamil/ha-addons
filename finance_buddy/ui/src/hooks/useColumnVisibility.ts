// Widoczność kolumn listy, zapamiętana per ekran. Kolumny wypełnione w mniej niż 5% wierszy
// są domyślnie ukryte, dopóki użytkownik nie zdecyduje inaczej w menu „Kolumny".
import { useCallback, useMemo, useState } from 'react';

const SPARSE_THRESHOLD = 0.05;

export type OptionalColumn<R> = {
  key: string;
  label: string;
  hasValue: (row: R) => boolean;
};

const storageKey = (screen: string) => `fb-columns-${screen}`;

const readStored = (screen: string): Record<string, boolean> => {
  try {
    return JSON.parse(window.localStorage.getItem(storageKey(screen)) ?? '{}') as Record<string, boolean>;
  } catch {
    return {};
  }
};

export const useColumnVisibility = <R,>(screen: string, columns: OptionalColumn<R>[], rows: readonly R[]) => {
  const [choices, setChoices] = useState<Record<string, boolean>>(() => readStored(screen));

  const isVisible = useCallback(
    (column: OptionalColumn<R>) => {
      const choice = choices[column.key];
      if (choice !== undefined) return choice;
      if (!rows.length) return false;
      return rows.filter(column.hasValue).length / rows.length >= SPARSE_THRESHOLD;
    },
    [choices, rows]
  );

  const hiddenKeys = useMemo(
    () => new Set(columns.filter((column) => !isVisible(column)).map((column) => column.key)),
    [columns, isVisible]
  );

  const setVisible = useCallback(
    (key: string, visible: boolean) => {
      setChoices((prev) => {
        const next = { ...prev, [key]: visible };
        try {
          window.localStorage.setItem(storageKey(screen), JSON.stringify(next));
        } catch {
          /* wybór zostaje tylko na czas sesji */
        }
        return next;
      });
    },
    [screen]
  );

  return { hiddenKeys, setVisible };
};
