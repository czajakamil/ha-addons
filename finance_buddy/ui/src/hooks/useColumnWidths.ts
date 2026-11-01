// Szerokości kolumn zapamiętywane na koncie użytkownika, wspólne dla wszystkich tabel.
// Stan trzymamy per tabela poza Reactem: jedna tabela bywa na ekranie kilka razy (Konta
// w grupach, Sklepy aktywne i archiwum), a API zastępuje cały zapis, więc osobne kopie
// nadpisywałyby sobie nawzajem zmiany.
import { useCallback, useSyncExternalStore } from 'react';
import { getTableColumnWidths, saveTableColumnWidths } from '../api';

export type ColumnWidths = Readonly<Record<string, number>>;

export const MIN_COLUMN_WIDTH = 48;
export const PHONE_QUERY = '(max-width: 700px)';
const PERSIST_DELAY_MS = 400;
const EMPTY: ColumnWidths = {};

type TableEntry = {
  widths: ColumnWidths;
  // Rośnie, gdy z serwera przyjdą inne szerokości niż te na ekranie
  loadedVersion: number;
  listeners: Set<() => void>;
  saveTimer: number | null;
  loading: boolean;
};

const tables = new Map<string, TableEntry>();

const entryFor = (tableId: string) => {
  let entry = tables.get(tableId);
  if (!entry) {
    entry = { widths: EMPTY, loadedVersion: 0, listeners: new Set(), saveTimer: null, loading: false };
    tables.set(tableId, entry);
  }
  return entry;
};

const publish = (entry: TableEntry, widths: ColumnWidths) => {
  entry.widths = widths;
  entry.listeners.forEach((listener) => listener());
};

const subscribe = (tableId: string, listener: () => void) => {
  const entry = entryFor(tableId);
  entry.listeners.add(listener);
  // Pierwszy subskrybent odświeża dane: na koncie mogły się zmienić, np. po przelogowaniu
  if (entry.listeners.size === 1 && !entry.loading) {
    entry.loading = true;
    getTableColumnWidths(tableId)
      .then((response) => {
        if (entry.saveTimer !== null) return;
        const widths = response.widths ?? EMPTY;
        if (JSON.stringify(widths) === JSON.stringify(entry.widths)) return;
        entry.loadedVersion += 1;
        publish(entry, widths);
      })
      .catch((err) => console.error('Failed to load column widths', err))
      .finally(() => {
        entry.loading = false;
      });
  }
  return () => {
    entry.listeners.delete(listener);
  };
};

const setColumnWidth = (tableId: string, columnId: string, width: number) => {
  const entry = entryFor(tableId);
  const nextWidth = Math.max(MIN_COLUMN_WIDTH, Math.round(width));
  if (entry.widths[columnId] === nextWidth) return;
  publish(entry, { ...entry.widths, [columnId]: nextWidth });

  if (entry.saveTimer !== null) window.clearTimeout(entry.saveTimer);
  entry.saveTimer = window.setTimeout(() => {
    entry.saveTimer = null;
    saveTableColumnWidths(tableId, { ...entry.widths }).catch((err) =>
      console.error('Failed to save column widths', err)
    );
  }, PERSIST_DELAY_MS);
};

/** Bez `tableId` hook nic nie czyta i nie zapisuje; tak działa m.in. układ telefonu. */
export const useColumnWidths = (tableId: string | undefined) => {
  const subscribeToTable = useCallback(
    (listener: () => void) => (tableId ? subscribe(tableId, listener) : () => {}),
    [tableId]
  );
  const widths = useSyncExternalStore(subscribeToTable, () => (tableId ? entryFor(tableId).widths : EMPTY));
  const loadedVersion = useSyncExternalStore(subscribeToTable, () =>
    tableId ? entryFor(tableId).loadedVersion : 0
  );
  const setWidth = useCallback(
    (columnId: string, width: number) => {
      if (tableId) setColumnWidth(tableId, columnId, width);
    },
    [tableId]
  );
  return { widths, loadedVersion, setWidth };
};
