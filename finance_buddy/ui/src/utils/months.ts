// Etykiety miesięcy z klucza „RRRR-MM”.
export const fullMonthName = new Intl.DateTimeFormat('pl-PL', { month: 'short', year: 'numeric' });

export const monthDate = (month: string) => {
  const [year, index] = month.split('-').map(Number);
  return new Date(year, index - 1, 1);
};

export const monthRangeLabel = (first: string, last: string) =>
  `${fullMonthName.format(monthDate(first))} – ${fullMonthName.format(monthDate(last))}`;
