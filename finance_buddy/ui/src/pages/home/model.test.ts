import { describe, expect, it } from 'vitest';
import type { HomeEnvelope, HomePace } from '../../api';
import { daysLeftIncludingToday, describeEnvelopes, paceGeometry, paceVerdict, perDay } from './model';

const envelope = (available: number): HomeEnvelope => ({
  category_id: String(available),
  category_name: 'X',
  available_cents: available,
  spent_cents: 100,
  funds_cents: 100 + available
});

describe('daysLeftIncludingToday', () => {
  it('liczy dzisiejszy dzień', () => {
    expect(daysLeftIncludingToday('2026-11-08')).toBe(23);
    expect(daysLeftIncludingToday('2026-11-30')).toBe(1);
    expect(daysLeftIncludingToday('2026-02-01')).toBe(28);
  });
});

describe('perDay', () => {
  it('dzieli to, co zostało, przez dni z dzisiejszym', () => {
    expect(perDay(23_000, '2026-11-08')).toEqual({ cents: 1_000, days: 23, label: '23 dni' });
  });

  it('nie ma czego dzielić przy zerze albo minusie', () => {
    expect(perDay(0, '2026-11-08')).toBeNull();
    expect(perDay(-500, '2026-11-08')).toBeNull();
  });

  it('ostatni dzień miesiąca to cała kwota na dziś', () => {
    expect(perDay(4_200, '2026-11-30')).toEqual({ cents: 4_200, days: 1, label: '1 dzień' });
  });
});

describe('describeEnvelopes', () => {
  it('łączy przekroczone i bliskie limitu', () => {
    expect(describeEnvelopes(1, [envelope(-760), envelope(3_200), envelope(0)])).toBe(
      '1 kategoria ponad budżet, 1 blisko limitu'
    );
  });

  it('odmienia liczbę kategorii', () => {
    expect(describeEnvelopes(3, [])).toBe('3 kategorie ponad budżet');
    expect(describeEnvelopes(5, [])).toBe('5 kategorii ponad budżet');
  });

  it('pusto, gdy nic nie wymaga uwagi', () => {
    expect(describeEnvelopes(0, [])).toBe('');
  });
});

const pace = (current: number[], average: number[]): HomePace => ({
  days_in_month: 30,
  current_cents: current,
  average_cents: average,
  average_months: average.length ? 6 : 0
});

describe('paceVerdict', () => {
  const avg = Array.from({ length: 30 }, (_, i) => (i + 1) * 10_000);

  it('porównuje z średnią na dziś, nie na koniec miesiąca', () => {
    expect(paceVerdict(pace([10_000, 30_000], avg))).toBe('100,00 zł więcej niż zwykle do tego dnia.');
    expect(paceVerdict(pace([5_000, 15_000], avg))).toBe('50,00 zł mniej niż zwykle do tego dnia.');
  });

  it('w granicach 5% to zwykłe tempo', () => {
    expect(paceVerdict(pace([10_000, 20_900], avg))).toBe('Wydajesz w zwykłym tempie.');
  });

  it('bez średniej nie ma z czym porównać', () => {
    expect(paceVerdict(pace([10_000], []))).toBeNull();
  });
});

describe('paceGeometry', () => {
  it('pierwszy dzień na lewej krawędzi, ostatni na prawej, wspólna skala', () => {
    const g = paceGeometry(pace([0, 50_000], [0, ...Array(29).fill(100_000)]), 290, 100);
    expect(g.x(1)).toBe(0);
    expect(g.x(30)).toBe(290);
    expect(g.y(0)).toBe(100);
    expect(g.current).toBe('0,100 10,53.7');
  });
});
