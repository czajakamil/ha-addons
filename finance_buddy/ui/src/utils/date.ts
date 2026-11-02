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

/**
 * „14:30" z `recorded_at`; null, gdy źródło nie podało godziny (zapisana jest wtedy północ).
 * Backend trzyma czas lokalny jako UTC, więc odczyt też idzie w UTC, a nie w strefie przeglądarki.
 */
export const formatRecordedTime = (recordedAt?: string | null) => {
  if (!recordedAt) return null;
  const moment = new Date(recordedAt);
  if (Number.isNaN(moment.getTime())) return null;
  const hours = moment.getUTCHours();
  const minutes = moment.getUTCMinutes();
  if (hours === 0 && minutes === 0) return null;
  return `${String(hours).padStart(2, '0')}:${String(minutes).padStart(2, '0')}`;
};

/** `recorded_at` z pól formularza. Godzina idzie jako UTC, a pusta to północ, czyli brak godziny. */
export const toRecordedAt = (date: string, time: string) => `${date}T${time || '00:00'}:00Z`;
