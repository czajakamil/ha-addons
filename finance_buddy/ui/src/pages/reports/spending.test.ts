import { describe, expect, it } from 'vitest';
import type { IncomeExpenseMonthRow } from '../../api';
import { buildSpending, niceScale } from './spending';

const month = (key: string, categories: [string | null, string, number][]): IncomeExpenseMonthRow => ({
  month: key,
  label: key,
  income_cents: 0,
  expense_cents: categories.reduce((acc, [, , cents]) => acc + cents, 0),
  net_cents: 0,
  income_categories: [],
  expense_categories: categories.map(([id, name, cents]) => ({
    category_id: id,
    category_name: name,
    total_cents: cents,
    total_amount: cents / 100
  }))
});

const months = [
  month('2026-08', [['rent', 'Dom/Czynsz', 100_00], ['food', 'Jedzenie/Zakupy', 40_00]]),
  month('2026-09', [['rent', 'Dom/Czynsz', 100_00], ['food', 'Jedzenie/Zakupy', 60_00]]),
  month('2026-10', [['rent', 'Dom/Czynsz', 100_00], ['food', 'Jedzenie/Zakupy', 80_00], [null, 'Uncategorized', 20_00]]),
  month('2026-11', [['food', 'Jedzenie/Zakupy', 5_00]])
];

const build = (overrides: Partial<Parameters<typeof buildSpending>[0]> = {}) =>
  buildSpending({
    months,
    view: 'categories',
    groupFilter: null,
    currentMonth: '2026-11',
    colorOf: (index) => `c${index}`,
    otherColor: 'grey',
    ...overrides
  });

describe('buildSpending', () => {
  it('liczy średnią tylko z pełnych miesięcy i oznacza bieżący', () => {
    const model = build();
    expect(model.completeMonths).toBe(3);
    expect(model.partialMonth?.month).toBe('2026-11');
    expect(model.average).toBeCloseTo((140_00 + 160_00 + 200_00) / 3);
    expect(model.total).toBe(140_00 + 160_00 + 200_00 + 5_00);
    expect(model.compareMonth).toBe('2026-10');
  });

  it('sortuje malejąco, koloruje największych i porównuje ostatni pełny miesiąc ze średnią', () => {
    const [rent, food, none] = build().rows;
    expect([rent.name, food.name, none.name]).toEqual(['Czynsz', 'Zakupy', 'Bez tagu']);
    expect(rent.color).toBe('c0');
    expect(rent.change).toBeCloseTo(0);
    expect(food.change).toBeCloseTo((80_00 - 60_00) / 60_00);
    expect(none.categoryId).toBe('__uncategorized__');
  });

  it('zbiera resztę do „Pozostałe” na wykresie i w tabeli', () => {
    const many = Array.from({ length: 12 }, (_, i): [string, string, number] => [`id${i}`, `G/T${i}`, (20 - i) * 100]);
    const model = build({ months: [month('2026-10', many)], currentMonth: '2026-11' });
    expect(model.chartSeries).toHaveLength(6);
    expect(model.chartSeries[5]).toMatchObject({ name: 'Pozostałe', isOther: true, color: 'grey' });
    expect(model.rows).toHaveLength(11);
    expect(model.rows[10].name).toBe('Pozostałe · 2 kategorie');
    expect(model.rows[5].color).toBe('grey');
  });

  it('grupuje i zawęża do grupy', () => {
    const groups = build({ view: 'groups' }).rows;
    expect(groups.map((row) => row.name)).toEqual(['Dom', 'Jedzenie', 'Bez tagu']);
    const food = build({ groupFilter: 'jedzenie' });
    expect(food.rows.map((row) => row.name)).toEqual(['Zakupy']);
  });

  it('bez pełnego miesiąca nie ma średniej ani porównania', () => {
    const model = build({ months: [months[3]] });
    expect(model.average).toBeNull();
    expect(model.compareMonth).toBeNull();
    expect(model.rows[0].change).toBeNull();
  });
});

describe('niceScale', () => {
  it('daje okrągły krok i szczyt nad maksimum', () => {
    const { top, ticks } = niceScale(520_000);
    expect(top).toBeGreaterThanOrEqual(520_000);
    expect(ticks[0]).toBe(0);
    expect(ticks.length).toBeLessThanOrEqual(6);
  });
});
