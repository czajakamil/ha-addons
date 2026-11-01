// Przywracanie fokusu do komórki react-data-grid po zamknięciu edytora inline.
//
// Kotwicę (komórkę + siatkę) trzeba złapać PRZED commitem — po nim edytor jest
// odmontowany i `closest('.rdg-cell')` na jego elemencie zwraca null, przez co
// fokus spada na <body> i klawiatura przestaje działać w tabeli.
export type GridCellAnchor = {
  cell: HTMLElement | null;
  grid: HTMLElement | null;
};

// Klawisze nawigacji, których edytory inline nie mogą przepuścić do siatki —
// inaczej react-data-grid przejmie strzałki i wyrzuci fokus z pola w trakcie edycji.
export const GRID_NAVIGATION_KEYS = new Set<string>([
  'ArrowLeft',
  'ArrowRight',
  'ArrowUp',
  'ArrowDown',
  'Home',
  'End',
  'PageUp',
  'PageDown'
]);

export const captureGridCellAnchor = (from: EventTarget | null): GridCellAnchor => {
  const el = from instanceof Element ? from : null;
  const cell = el?.closest<HTMLElement>('.rdg-cell') ?? null;
  return { cell, grid: cell?.closest<HTMLElement>('.rdg') ?? null };
};

// Gdy React wymienił element komórki, awaryjnie celujemy w komórkę, którą
// react-data-grid oznacza tabindexem 0 (aktualnie zaznaczoną).
const applyGridFocus = ({ cell, grid }: GridCellAnchor) => {
  if (cell?.isConnected) {
    cell.focus();
    return;
  }
  grid?.querySelector<HTMLElement>('[role="row"] > [tabindex="0"]')?.focus();
};

// Fokus ustawiamy od razu — przeniesienie go z edytora na komórkę sprawia, że
// późniejszy unmount edytora nie zrzuci fokusu na <body>. Druga próba w rAF
// łata przypadek, gdyby rerender po commicie wymienił element komórki.
export const refocusGridCell = (anchor: GridCellAnchor) => {
  if (!anchor.cell && !anchor.grid) return;
  applyGridFocus(anchor);
  requestAnimationFrame(() => applyGridFocus(anchor));
};
