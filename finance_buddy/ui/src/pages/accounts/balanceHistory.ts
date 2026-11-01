// Czyste wyliczenia salda konta z jego transakcji. API nie ma historii sald,
// więc odtwarzamy ją wstecz od bieżącego salda.
import type { Transaction } from '../../api';

export type BalancePoint = { month: string; label: string; balanceCents: number };

const monthKey = (date: Date) =>
  `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;

const shiftMonth = (key: string, delta: number) => {
  const [year, month] = key.split('-').map(Number);
  return monthKey(new Date(year, month - 1 + delta, 1));
};

const monthLabel = (key: string, withYear: boolean) => {
  const [year, month] = key.split('-').map(Number);
  const short = new Date(year, month - 1, 1).toLocaleDateString('pl-PL', { month: 'short' });
  return withYear ? `${short} ${String(year).slice(2)}` : short;
};

/**
 * Saldo na koniec każdego miesiąca, od miesiąca przed pierwszą transakcją (saldo
 * otwarcia, bez niego konto z jednego miesiąca dałoby pojedynczy punkt) do
 * bieżącego albo ostatniego z transakcją, jeśli jest późniejszy.
 */
export const buildMonthlyBalances = (
  balanceCents: number,
  transactions: readonly Pick<Transaction, 'date' | 'total_cents'>[],
  now = new Date()
): BalancePoint[] => {
  const totals = new Map<string, number>();
  for (const tx of transactions) {
    if (!tx.date) continue;
    const key = tx.date.slice(0, 7);
    totals.set(key, (totals.get(key) ?? 0) + tx.total_cents);
  }
  if (!totals.size) return [];

  const keys = [...totals.keys()].sort();
  const current = monthKey(now);
  const last = keys[keys.length - 1] > current ? keys[keys.length - 1] : current;
  const months: string[] = [];
  for (let key = shiftMonth(keys[0], -1); key <= last; key = shiftMonth(key, 1)) months.push(key);

  const withYear = months[0].slice(0, 4) !== last.slice(0, 4);
  let balance = balanceCents;
  const points: BalancePoint[] = [];
  for (let index = months.length - 1; index >= 0; index -= 1) {
    const month = months[index];
    points.unshift({ month, label: monthLabel(month, withYear), balanceCents: balance });
    balance -= totals.get(month) ?? 0;
  }
  return points;
};

/**
 * Saldo po każdej transakcji listy ułożonej od najnowszej. Wymaga ciągłego
 * początku listy: pominięty wiersz (np. przez filtr) przesunąłby wszystkie niższe.
 */
export const buildBalanceAfter = (
  balanceCents: number,
  transactions: readonly Pick<Transaction, 'id' | 'total_cents'>[]
) => {
  const result = new Map<string, number>();
  let balance = balanceCents;
  for (const tx of transactions) {
    result.set(tx.id, balance);
    balance -= tx.total_cents;
  }
  return result;
};
