const DATE = new Intl.DateTimeFormat('pl-PL', { day: 'numeric', month: 'short', year: 'numeric' });
const DATE_TIME = new Intl.DateTimeFormat('pl-PL', {
  day: 'numeric',
  month: 'short',
  year: 'numeric',
  hour: '2-digit',
  minute: '2-digit'
});

export const formatDate = (value: string) => DATE.format(new Date(value));

export const formatDateTime = (value: string | null | undefined) =>
  value ? DATE_TIME.format(new Date(value)) : '—';

export const errorDetail = (err: unknown, fallback: string) => {
  if (!(err instanceof Error)) return fallback;
  try {
    const parsed = JSON.parse(err.message) as { detail?: string };
    return parsed.detail ?? err.message;
  } catch {
    return err.message || fallback;
  }
};
