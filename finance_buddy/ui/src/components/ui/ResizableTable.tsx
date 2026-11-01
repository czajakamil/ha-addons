// Zwykła tabela HTML z przeciąganiem krawędzi nagłówka i zapamiętaną szerokością kolumn.
// Kolumnę rozpoznajemy po tekście nagłówka albo `data-col`; kolumny bez nazwy (akcje) mają stałą szerokość.
import { useLayoutEffect, useRef, type PointerEvent as ReactPointerEvent, type TableHTMLAttributes } from 'react';
import { MIN_COLUMN_WIDTH, PHONE_QUERY, useColumnWidths, type ColumnWidths } from '../../hooks/useColumnWidths';
import { useMediaQuery } from '../../hooks/useMediaQuery';

const HANDLE_WIDTH_PX = 8;

const columnKey = (header: HTMLElement) => header.dataset.col ?? header.textContent?.trim() ?? '';

const headersOf = (table: HTMLTableElement) =>
  Array.from(table.querySelectorAll<HTMLTableCellElement>('thead tr:last-child th')).filter(
    (header) => header.offsetParent !== null
  );

const resetLayout = (table: HTMLTableElement) => {
  table.style.tableLayout = '';
  table.style.width = '';
  table.querySelectorAll<HTMLElement>('thead th').forEach((header) => {
    header.style.width = '';
  });
};

const applyWidths = (table: HTMLTableElement, widths: ColumnWidths) => {
  resetLayout(table);
  const headers = headersOf(table);
  if (!headers.some((header) => widths[columnKey(header)])) return;

  // Pomiar przed ustawieniem stałego układu: niezmienione kolumny zostają przy swojej szerokości
  const measured = headers.map((header) => widths[columnKey(header)] ?? header.getBoundingClientRect().width);
  const available = table.parentElement?.clientWidth ?? 0;
  const total = measured.reduce((sum, width) => sum + width, 0);
  // Gdy kolumny są węższe od miejsca, resztę dostaje pierwsza niezmieniona kolumna
  const filler = total < available ? headers.findIndex((header) => !widths[columnKey(header)]) : -1;

  headers.forEach((header, index) => {
    // 'auto', a nie pusty styl: arkusze ekranów też ustawiają szerokości kolumn
    header.style.width = index === filler ? 'auto' : `${measured[index]}px`;
  });
  table.style.tableLayout = 'fixed';
  table.style.width = filler >= 0 ? '100%' : `${total}px`;
};

type ResizableTableProps = TableHTMLAttributes<HTMLTableElement> & {
  tableId: string;
};

const ResizableTable = ({ tableId, className, children, ...rest }: ResizableTableProps) => {
  const tableRef = useRef<HTMLTableElement>(null);
  const isPhone = useMediaQuery(PHONE_QUERY);
  const { widths, setWidth } = useColumnWidths(isPhone ? undefined : tableId);
  const appliedRef = useRef('');

  // Bez listy zależności, bo nagłówki mogą się zmienić razem z danymi. Podpis chroni
  // przed ponownym pomiarem przy każdym renderze, np. przy edycji kwot w Budżecie.
  useLayoutEffect(() => {
    const table = tableRef.current;
    if (!table) return;
    const signature = JSON.stringify([widths, headersOf(table).map(columnKey)]);
    if (signature === appliedRef.current) return;
    appliedRef.current = signature;
    applyWidths(table, widths);
  });

  const handlePointerDown = (event: ReactPointerEvent<HTMLTableElement>) => {
    const header = (event.target as HTMLElement).closest<HTMLTableCellElement>('thead th');
    const table = tableRef.current;
    if (isPhone || !header || !table || !columnKey(header)) return;
    const rect = header.getBoundingClientRect();
    if (event.clientX < rect.right - HANDLE_WIDTH_PX) return;

    event.preventDefault();
    const startX = event.clientX;
    const startWidth = rect.width;
    // Zamrożenie obecnego układu, żeby kolumna szła dokładnie za kursorem
    const headers = headersOf(table);
    const frozen = headers.map((column) => column.getBoundingClientRect().width);
    headers.forEach((column, index) => {
      column.style.width = `${frozen[index]}px`;
    });
    const othersWidth = frozen.reduce((sum, width) => sum + width, 0) - startWidth;
    table.style.tableLayout = 'fixed';

    let nextWidth = startWidth;
    const handleMove = (move: PointerEvent) => {
      nextWidth = Math.max(MIN_COLUMN_WIDTH, startWidth + move.clientX - startX);
      header.style.width = `${nextWidth}px`;
      table.style.width = `${othersWidth + nextWidth}px`;
    };
    const handleUp = () => {
      window.removeEventListener('pointermove', handleMove);
      window.removeEventListener('pointerup', handleUp);
      // Puszczenie przycisku nie może kliknąć w nagłówek, który bywa przyciskiem sortowania
      const swallowClick = (click: MouseEvent) => click.stopPropagation();
      window.addEventListener('click', swallowClick, { capture: true, once: true });
      window.setTimeout(() => window.removeEventListener('click', swallowClick, { capture: true }), 0);
      setWidth(columnKey(header), nextWidth);
    };
    window.addEventListener('pointermove', handleMove);
    window.addEventListener('pointerup', handleUp);
  };

  return (
    <table
      {...rest}
      ref={tableRef}
      className={['resizable-table', className].filter(Boolean).join(' ')}
      onPointerDown={handlePointerDown}
    >
      {children}
    </table>
  );
};

export default ResizableTable;
