// Twarde spacje z Intl zamieniamy na zwykłe, żeby kwoty renderowały się spójnie
const normalizeSpaces = (value: string) => value.replace(/[\u00A0\u202F]/g, ' ');

// Fallback, gdy kod waluty jest nieznany dla Intl (np. literówka w danych konta)
const plainFormatter = new Intl.NumberFormat('pl-PL', {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
  useGrouping: 'always' as unknown as boolean
});

const moneyFormatters = new Map<string, Intl.NumberFormat | null>();

const getMoneyFormatter = (currency: string): Intl.NumberFormat | null => {
  if (!moneyFormatters.has(currency)) {
    let formatter: Intl.NumberFormat | null = null;
    try {
      formatter = new Intl.NumberFormat('pl-PL', {
        style: 'currency',
        currency,
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
        // Domyślnie pl-PL grupuje dopiero od 10 000; separator ma być zawsze
        useGrouping: 'always' as unknown as boolean
      });
    } catch {
      formatter = null;
    }
    moneyFormatters.set(currency, formatter);
  }
  return moneyFormatters.get(currency) ?? null;
};

export const formatMoney = (cents: number, currency: string) => {
  const amount = (cents ?? 0) / 100;
  const formatter = getMoneyFormatter(currency);
  if (!formatter) {
    const base = normalizeSpaces(plainFormatter.format(amount));
    return currency ? `${base} ${currency}` : base;
  }
  return normalizeSpaces(formatter.format(amount));
};

// Parsuje kwotę wpisaną przez użytkownika ("1 234,56", "1.234,56", "1234.56")
// na grosze. Zwraca null, gdy tekst jest pusty lub nie jest poprawną liczbą.
export const parseAmountToCents = (value: string): number | null => {
  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }

  // Spacje traktujemy jako separatory tysięcy
  let normalized = trimmed.replace(/\s+/g, '');

  if (normalized.includes(',')) {
    // Przecinek to separator dziesiętny, kropki to separatory tysięcy
    normalized = normalized.replace(/\./g, '').replace(',', '.');
  } else {
    const dotCount = (normalized.match(/\./g) ?? []).length;
    if (dotCount > 1) {
      // Kilka kropek: ostatnia jest separatorem dziesiętnym, pozostałe tysięcy
      const lastDot = normalized.lastIndexOf('.');
      normalized = `${normalized.slice(0, lastDot).replace(/\./g, '')}.${normalized.slice(lastDot + 1)}`;
    }
  }

  const numeric = Number(normalized);
  if (!Number.isFinite(numeric)) {
    return null;
  }
  return Math.round(numeric * 100);
};

/** Polska forma liczebnika: 1 operacja, 2 operacje, 5 operacji, 22 operacje. */
export const pluralPL = (n: number, one: string, few: string, many: string) => {
  const abs = Math.abs(n);
  const mod10 = abs % 10;
  const mod100 = abs % 100;

  if (abs === 1) return one;
  if (mod10 >= 2 && mod10 <= 4 && !(mod100 >= 12 && mod100 <= 14)) return few;
  return many;
};
