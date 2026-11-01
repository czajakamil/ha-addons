// Czyste helpery widoku długu: formatowanie warunków i przygotowanie danych
// pod wykres. Bez JSX i bez stanu — dzięki temu da się to przetestować bez
// renderowania Nivo.
import type { AccountDebt, DebtForecastStatus, DebtPaymentSplit } from '../../api';
import { formatMoney } from '../../utils/format';

// Kapitał zieleni, odsetki czerwienią — ta sama konwencja co w Raportach
// (aktywa/długi). Nivo nie czyta zmiennych CSS, więc kolory serii są wartościami;
// tekst, siatka i tooltip idą z `useChartTheme`.
export const DEBT_PRINCIPAL_COLOR = '#10b981';
export const DEBT_INTEREST_COLOR = '#ef4444';

/** Powyżej tylu rat wykres miesięczny robi się nieczytelny i agregujemy do lat. */
export const MONTHLY_CHART_LIMIT = 36;

/**
 * Oprocentowanie z punktów bazowych na tekst.
 *
 * 655 bps to 6,55%. Dzielimy przez 100, nie przez 10000 — punkt bazowy to
 * setna część punktu procentowego.
 */
export const formatInterestRate = (bps: number | null | undefined) => {
  if (bps === null || bps === undefined) return null;
  return `${(bps / 100).toLocaleString('pl-PL', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  })}%`;
};

/** `2026-08` na `sierpień 2026`. Zwraca wejście, gdy nie da się go rozpoznać. */
export const formatMonth = (month: string | null | undefined) => {
  if (!month) return null;
  const match = /^(\d{4})-(\d{2})$/.exec(month);
  if (!match) return month;
  const date = new Date(Number(match[1]), Number(match[2]) - 1, 1);
  if (Number.isNaN(date.getTime())) return month;
  return date.toLocaleDateString('pl-PL', { month: 'long', year: 'numeric' });
};

/** „24 raty" — polska odmiana, bo „24 rat" i „2 raty" to nie to samo. */
export const formatInstalments = (months: number) => {
  const abs = Math.abs(months);
  const lastTwo = abs % 100;
  const last = abs % 10;
  if (abs === 1) return '1 rata';
  const usesPluralFew = last >= 2 && last <= 4 && !(lastTwo >= 12 && lastTwo <= 14);
  return `${months} ${usesPluralFew ? 'raty' : 'rat'}`;
};

/**
 * Dlaczego prognozy nie ma. Każdy status znaczy co innego i użytkownik ma
 * dostać powód, a nie puste miejsce — zwłaszcza `never_amortizes`, które jest
 * realną informacją o kredycie, a nie brakiem danych.
 */
export const FORECAST_STATUS_MESSAGES: Record<Exclude<DebtForecastStatus, 'ok'>, string> = {
  missing_terms:
    'YNAB nie podał dla tego konta oprocentowania albo raty minimalnej. Uzupełnij je po stronie YNAB-a i zaimportuj ponownie.',
  never_amortizes:
    'Rata minimalna nie pokrywa nawet miesięcznych odsetek — przy tych warunkach saldo nie maleje.',
  exceeds_horizon: 'Przy tych warunkach spłata wykracza poza 50 lat.'
};

export type DebtChartDatum = {
  label: string;
  principal: number;
  interest: number;
};

/**
 * Harmonogram na dane wykresu, w złotówkach (nie groszach) — oś Y ma pokazywać
 * kwoty, a nie setne części grosza.
 *
 * Kredyt hipoteczny to nawet 360 rat; tylu słupków nie da się przeczytać, więc
 * dłuższe harmonogramy sumujemy do lat. Krótkie zostają miesięczne.
 */
export const buildDebtChartData = (
  schedule: readonly DebtPaymentSplit[],
  monthlyLimit: number = MONTHLY_CHART_LIMIT
): { data: DebtChartDatum[]; grouped: boolean } => {
  if (!schedule.length) return { data: [], grouped: false };

  if (schedule.length <= monthlyLimit) {
    return {
      data: schedule.map((step) => ({
        label: step.month,
        principal: step.principal_cents / 100,
        interest: step.interest_cents / 100
      })),
      grouped: false
    };
  }

  const byYear = new Map<string, DebtChartDatum>();
  schedule.forEach((step) => {
    const year = step.month.slice(0, 4);
    const current = byYear.get(year) ?? { label: year, principal: 0, interest: 0 };
    current.principal += step.principal_cents / 100;
    current.interest += step.interest_cents / 100;
    byYear.set(year, current);
  });

  return { data: Array.from(byYear.values()), grouped: true };
};

/**
 * Kwota na oś Y — bez groszy i bez symbolu waluty.
 *
 * Pełne `formatMoney` („45 000,00 zł") nie mieści się w marginesie osi i
 * ucina się do „15 000,00 zł", czyli pokazuje LICZBĘ, której tam nie ma.
 * Waluta jest już w podsumowaniu nad wykresem.
 */
export const formatAxisAmount = (amount: number) =>
  Math.round(amount).toLocaleString('pl-PL', { maximumFractionDigits: 0 });

/**
 * Co która etykieta osi X ma być podpisana.
 *
 * Trzydzieści lat kredytu to trzydzieści podpisów, które zlewają się w
 * nieczytelną smugę. Przerzedzamy je do `maxTicks`, zamiast zmniejszać czcionkę.
 */
export const pickAxisTicks = (labels: readonly string[], maxTicks = 12): string[] => {
  if (labels.length <= maxTicks) return [...labels];
  const step = Math.ceil(labels.length / maxTicks);
  return labels.filter((_, index) => index % step === 0);
};

/** Wiersze podsumowania nad wykresem. Pomija to, czego YNAB nie podał. */
export const buildDebtSummary = (debt: AccountDebt, currency: string) => {
  const rows: { key: string; label: string; value: string }[] = [
    {
      key: 'outstanding',
      label: 'Pozostało do spłaty',
      value: formatMoney(debt.outstanding_cents, currency)
    }
  ];

  const rate = formatInterestRate(debt.current_terms?.interest_rate_bps);
  if (rate) rows.push({ key: 'rate', label: 'Oprocentowanie', value: rate });

  const payment = debt.current_terms?.minimum_payment_cents;
  if (payment !== null && payment !== undefined) {
    rows.push({ key: 'payment', label: 'Rata minimalna', value: formatMoney(payment, currency) });
  }

  // Escrow pokazujemy tylko, gdy naprawdę jest — na żywym koncie ten szereg
  // bywa pusty i pusty wiersz „—" niczego nie wnosi.
  const escrow = debt.current_terms?.escrow_cents;
  if (escrow !== null && escrow !== undefined) {
    rows.push({ key: 'escrow', label: 'Escrow', value: formatMoney(escrow, currency) });
  }

  return rows;
};
