// Szerokości kolumn tabeli transakcji: wczytanie zapisanych wartości z backendu
// i zapisywanie zmian z opóźnieniem (debounce), by nie zalewać API przy przeciąganiu.
import { useCallback, useEffect, useState } from 'react';
import { getTableColumnWidths, saveTableColumnWidths } from '../../api';
import {
  COLUMN_WIDTH_PERSIST_DELAY,
  COLUMN_WIDTH_TABLE_ID,
  DEFAULT_COLUMN_WIDTHS,
  MIN_COLUMN_WIDTH,
  sanitizeStoredColumnWidths,
  toPersistableColumnWidths,
  type TransactionColumnId
} from './model';

export const useColumnWidths = () => {
  const [columnWidths, setColumnWidths] = useState<Partial<Record<TransactionColumnId, number>>>({});
  const [columnWidthsFetched, setColumnWidthsFetched] = useState(false);

  useEffect(() => {
    const fetchColumnWidths = async () => {
      try {
        const response = await getTableColumnWidths(COLUMN_WIDTH_TABLE_ID);
        const sanitized = sanitizeStoredColumnWidths(response.widths ?? {});
        setColumnWidths(sanitized);
      } catch (err) {
        console.error('Failed to load column widths', err);
        setColumnWidths({});
      } finally {
        setColumnWidthsFetched(true);
      }
    };

    void fetchColumnWidths();
  }, []);

  useEffect(() => {
    if (!columnWidthsFetched) {
      return;
    }
    const payload = toPersistableColumnWidths(columnWidths);
    if (!Object.keys(payload).length) {
      return;
    }
    const timeoutId = window.setTimeout(() => {
      void saveTableColumnWidths(COLUMN_WIDTH_TABLE_ID, payload).catch((err) => {
        console.error('Failed to save column widths', err);
      });
    }, COLUMN_WIDTH_PERSIST_DELAY);
    return () => {
      window.clearTimeout(timeoutId);
    };
  }, [columnWidths, columnWidthsFetched]);

  const getColumnWidth = (columnId: TransactionColumnId) =>
    columnWidths[columnId] ?? DEFAULT_COLUMN_WIDTHS[columnId];

  const handleColumnResize = useCallback((columnId: TransactionColumnId, width: number) => {
    const nextWidth = Math.max(MIN_COLUMN_WIDTH, Math.round(width));

    setColumnWidths((prev) => {
      if (prev?.[columnId] === nextWidth) {
        return prev;
      }
      return { ...prev, [columnId]: nextWidth };
    });
  }, []);

  return { getColumnWidth, handleColumnResize };
};
