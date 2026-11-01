import { request } from './client';

export interface TableColumnWidthsResponse {
  table: string;
  widths: Record<string, number>;
}

export const getTableColumnWidths = (tableId: string) =>
  request<TableColumnWidthsResponse>(`/preferences/table-columns/${tableId}`);

export const saveTableColumnWidths = (tableId: string, widths: Record<string, number>) =>
  request<TableColumnWidthsResponse>(`/preferences/table-columns/${tableId}`, {
    method: 'PUT',
    body: JSON.stringify({ widths })
  });
