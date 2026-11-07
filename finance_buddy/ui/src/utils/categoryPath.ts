// Ścieżka kategorii do pokazania ludziom: „Rachunki › Czynsz” zamiast „Rachunki/Czynsz”.
// Ukośnik zostaje tylko tam, gdzie ścieżkę się wpisuje (pole tagu tworzy z niej gałąź).
export const formatCategoryPath = (path: string | null | undefined) =>
  (path ?? '')
    .split('/')
    .map((segment) => segment.trim())
    .filter(Boolean)
    .join(' › ');
