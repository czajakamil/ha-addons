import { describe, expect, it } from 'vitest';
import { buildBalanceAfter, buildMonthlyBalances } from './balanceHistory';

describe('buildBalanceAfter', () => {
  it('liczy saldo wstecz od bieżącego, od najnowszej transakcji', () => {
    const result = buildBalanceAfter(1000, [
      { id: 'c', total_cents: -200 },
      { id: 'b', total_cents: 500 },
      { id: 'a', total_cents: 700 }
    ]);
    expect([...result.entries()]).toEqual([
      ['c', 1000],
      ['b', 1200],
      ['a', 700]
    ]);
  });
});

describe('buildMonthlyBalances', () => {
  const now = new Date(2026, 10, 15);

  it('zaczyna od salda otwarcia przed pierwszą transakcją i wypełnia puste miesiące', () => {
    const points = buildMonthlyBalances(
      800,
      [
        { date: '2026-08-10', total_cents: 1000 },
        { date: '2026-10-03', total_cents: -200 }
      ],
      now
    );
    expect(points.map((point) => [point.month, point.balanceCents])).toEqual([
      ['2026-07', 0],
      ['2026-08', 1000],
      ['2026-09', 1000],
      ['2026-10', 800],
      ['2026-11', 800]
    ]);
  });

  it('dochodzi do późniejszej transakcji, jeśli ma datę z przyszłości', () => {
    const points = buildMonthlyBalances(100, [{ date: '2026-12-01', total_cents: 100 }], now);
    expect(points.map((point) => point.month)).toEqual(['2026-11', '2026-12']);
    expect(points[0].balanceCents).toBe(0);
  });

  it('dopisuje rok, gdy zakres przechodzi przez nowy rok', () => {
    const points = buildMonthlyBalances(0, [{ date: '2025-12-01', total_cents: 0 }], new Date(2026, 0, 5));
    expect(points[points.length - 1].label).toMatch(/26$/);
  });

  it('bez transakcji nie ma punktów', () => {
    expect(buildMonthlyBalances(500, [], now)).toEqual([]);
  });
});
