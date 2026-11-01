// Odczyt tokenów dla bibliotek, które nie rozumieją var(--x), np. Nivo.
export const readToken = (name: string) =>
  typeof document === 'undefined'
    ? ''
    : getComputedStyle(document.documentElement).getPropertyValue(name).trim();

const CHART_SLOTS = 8;

/** Kolor serii w stałej kolejności; kolejne serie zawijają się po palecie. */
export const chartColor = (index: number) => readToken(`--chart-${(index % CHART_SLOTS) + 1}`);
