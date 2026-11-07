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

/** „1.11.2026" albo „2026-11-01" → ISO; null, gdy to nie jest prawdziwa data, pusty tekst → ''. */
export const parseTypedDate = (text: string) => {
  const value = text.trim();
  if (!value) return '';
  const dayFirst = /^(\d{1,2})[.\-/](\d{1,2})[.\-/](\d{4})$/.exec(value);
  const yearFirst = /^(\d{4})-(\d{1,2})-(\d{1,2})$/.exec(value);
  const parts = dayFirst
    ? [dayFirst[3], dayFirst[2], dayFirst[1]]
    : yearFirst && [yearFirst[1], yearFirst[2], yearFirst[3]];
  if (!parts) return null;
  const [year, month, day] = parts.map(Number);
  const date = new Date(year, month - 1, day);
  if (date.getMonth() !== month - 1 || date.getDate() !== day) return null;
  return toIsoDate(date);
};

/** ISO → „01.11.2026". */
export const formatTypedDate = (iso: string) => {
  const [year, month, day] = iso.split('-');
  return year && month && day ? `${day}.${month}.${year}` : iso;
};

/**
 * „9:15", „09.15", „915" albo „9" → „09:15" / „09:00"; null, gdy to nie jest godzina,
 * pusty tekst → ''. Skróty bez dwukropka przyspieszają wpisywanie z klawiatury.
 */
export const parseTypedTime = (text: string) => {
  const value = text.trim();
  if (!value) return '';
  const match = /^(\d{1,2})(?:[:.]?(\d{2}))?$/.exec(value);
  const minutes = match?.[2] ?? '00';
  if (!match || Number(match[1]) > 23 || Number(minutes) > 59) return null;
  return `${match[1].padStart(2, '0')}:${minutes}`;
};
