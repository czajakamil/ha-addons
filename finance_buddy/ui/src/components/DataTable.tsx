import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  type CSSProperties,
  type FocusEvent,
  type ReactNode
} from 'react';
import DataGrid, {
  type CellClickArgs,
  type CellKeyboardEvent,
  type CellKeyDownArgs,
  type CellMouseEvent,
  type ColumnOrColumnGroup,
  type DataGridHandle,
  type DataGridProps
} from 'react-data-grid';
import { useTheme } from '../hooks/useTheme';

export const DATA_TABLE_ROW_HEIGHT = 30;
export const DATA_TABLE_HEADER_ROW_HEIGHT = 25;

const currencyFormatter = new Intl.NumberFormat('pl-PL', {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
  useGrouping: true
});

const formatCurrency = (value: number) => currencyFormatter.format(value).replace(/\u00A0/g, ' ');

const formatCellValue = (value: unknown, isMoney?: boolean): ReactNode => {
  if (typeof value !== 'number') return value as ReactNode;
  if (!isMoney) return value;
  return formatCurrency(value);
};

export type MoneyColumn<R> = ColumnOrColumnGroup<R> & {
  isMoney?: boolean;
};

type DataTableProps<R> = {
  height?: number;
  noRowsFallback?: ReactNode;
} & Omit<DataGridProps<R>, 'className' | 'style' | 'defaultColumnOptions' | 'renderers' | 'columns'> & {
  className?: string;
  style?: CSSProperties;
  defaultColumnOptions?: DataGridProps<R>['defaultColumnOptions'];
  renderers?: DataGridProps<R>['renderers'];
  columns: readonly MoneyColumn<R>[];
};

const DataTable = <R,>({
  height,
  className,
  noRowsFallback,
  renderers,
  defaultColumnOptions,
  style,
  rowHeight = DATA_TABLE_ROW_HEIGHT,
  headerRowHeight = DATA_TABLE_HEADER_ROW_HEIGHT,
  columns,
  rows,
  onCellDoubleClick,
  onCellKeyDown,
  onSelectedCellChange,
  ...props
}: DataTableProps<R>) => {
  const gridRef = useRef<DataGridHandle>(null);
  const hasSelectedCellRef = useRef(false);
  const { theme } = useTheme();
  const mergedRenderers = useMemo(
    () => (noRowsFallback ? { ...renderers, noRowsFallback } : renderers),
    [noRowsFallback, renderers]
  );

  const mergedDefaultColumnOptions = useMemo(() => {
    const mergedCellClass = ['data-table__cell', (defaultColumnOptions as any)?.cellClass]
      .filter(Boolean)
      .join(' ');

    return {
      resizable: true,
      ...defaultColumnOptions,
      cellClass: mergedCellClass
    } as DataGridProps<R>['defaultColumnOptions'];
  }, [defaultColumnOptions]);

  const mergedStyle = useMemo(
    () => (height ? { ...style, blockSize: `${height}px` } : style),
    [height, style]
  );

  useEffect(() => {
    if (rows.length === 0) {
      hasSelectedCellRef.current = false;
    }
  }, [rows.length]);

  const columnsWithCurrencyFormatting = useMemo(() => {
    const addFormatting = (column: MoneyColumn<R>): MoneyColumn<R> => {
      if ('children' in column) {
        return { ...column, children: column.children.map(addFormatting) };
      }

      const { isMoney } = column;
      const renderCell = column.renderCell;

      return {
        ...column,
        renderCell: renderCell
          ? (args) => formatCellValue(renderCell(args), isMoney)
          : ({ row }) => formatCellValue((row as any)[column.key], isMoney)
      };
    };

    return columns.map(addFormatting);
  }, [columns]);

  const mergedOnSelectedCellChange = useCallback(
    (args: Parameters<NonNullable<DataGridProps<R>['onSelectedCellChange']>>[0]) => {
      hasSelectedCellRef.current = true;
      onSelectedCellChange?.(args);
    },
    [onSelectedCellChange]
  );

  const mergedOnCellKeyDown = useMemo(() => {
    if (!onCellDoubleClick && !onCellKeyDown) return undefined;

    return (args: CellKeyDownArgs<R>, event: CellKeyboardEvent) => {
      onCellKeyDown?.(args, event);

      if (!onCellDoubleClick) return;
      // Enter i F2 (konwencja arkuszy kalkulacyjnych) uruchamiają akcję komórki.
      if (event.key !== 'Enter' && event.key !== 'F2') return;

      const target = event.target as HTMLElement | null;
      // Gdy fokus jest w edytorze albo na interaktywnym elemencie komórki
      // (np. przycisku), Enter obsługuje ten element — nie akcja komórki.
      const isInteractive = target?.closest(
        'input, textarea, select, button, a, [contenteditable="true"]'
      );
      if (isInteractive || event.defaultPrevented) return;

      event.preventDefault();
      event.stopPropagation();
      event.preventGridDefault?.();
      onCellDoubleClick(args as unknown as CellClickArgs<R>, event as unknown as CellMouseEvent);
    };
  }, [onCellDoubleClick, onCellKeyDown]);

  const handleGridFocusCapture = useCallback(
    (event: FocusEvent<HTMLDivElement>) => {
      if (hasSelectedCellRef.current) return;
      if (rows.length === 0 || columns.length === 0) return;

      const target = event.target as HTMLElement | null;
      if (!target?.closest('.rdg')) return;

      hasSelectedCellRef.current = true;
      gridRef.current?.selectCell({ rowIdx: 0, idx: 0 });
    },
    [columns.length, rows.length]
  );

  return (
    <div className="data-grid-wrapper" onFocusCapture={handleGridFocusCapture}>
      <DataGrid
        {...props}
        ref={gridRef}
        columns={columnsWithCurrencyFormatting}
        rows={rows}
        className={[theme === 'dark' ? 'rdg-dark' : 'rdg-light', className]
          .filter(Boolean)
          .join(' ')}
        renderers={mergedRenderers}
        defaultColumnOptions={mergedDefaultColumnOptions}
        style={mergedStyle}
        rowHeight={rowHeight}
        headerRowHeight={headerRowHeight}
        onCellDoubleClick={onCellDoubleClick}
        onCellKeyDown={mergedOnCellKeyDown}
        onSelectedCellChange={mergedOnSelectedCellChange}
      />
    </div>
  );
};

export default DataTable;
