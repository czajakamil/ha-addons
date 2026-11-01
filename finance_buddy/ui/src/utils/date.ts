// Daty po polsku. Wejście to ISO (YYYY-MM-DD) z backendu, bez wpływu strefy czasowej.
const parseIso = (iso: string) => {
  const [year, month, day] = iso.split('-').map(Number);
  return new Date(year, month - 1, day);
};

const shortFormatter = new Intl.DateTimeFormat('pl-PL', { day: 'numeric', month: 'short' });
const longFormatter = new Intl.DateTimeFormat('pl-PL', { weekday: 'long', day: 'numeric', month: 'long' });
const longWithYearFormatter = new Intl.DateTimeFormat('pl-PL', {
  weekday: 'long',
  day: 'numeric',
  month: 'long',
  year: 'numeric'
});

const startOfToday = () => {
  const now = new Date();
  return new Date(now.getFullYear(), now.getMonth(), now.getDate());
};

export const toIsoDate = (date: Date) => {
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  return `${date.getFullYear()}-${month}-${day}`;
};

/** „1 lis" */
export const formatShortDate = (iso: string) => shortFormatter.format(parseIso(iso));

/** „Dziś", „Wczoraj", „Sobota, 31 października"; rok tylko spoza bieżącego. */
export const formatDayHeading = (iso: string) => {
  const date = parseIso(iso);
  const today = startOfToday();
  const diffDays = Math.round((today.getTime() - date.getTime()) / 86_400_000);
  if (diffDays === 0) return 'Dziś';
  if (diffDays === 1) return 'Wczoraj';
  const formatter = date.getFullYear() === today.getFullYear() ? longFormatter : longWithYearFormatter;
  const label = formatter.format(date);
  return label.charAt(0).toUpperCase() + label.slice(1);
};
