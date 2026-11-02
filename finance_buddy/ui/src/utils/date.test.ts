import { describe, expect, it } from 'vitest';
import { formatRecordedTime, toRecordedAt } from './date';

describe('formatRecordedTime', () => {
  it('pokazuje godzinę tak, jak ją zapisano, bez przeliczania na strefę przeglądarki', () => {
    expect(formatRecordedTime('2026-11-01T14:30:00Z')).toBe('14:30');
    expect(formatRecordedTime('2026-11-01T07:05:59+00:00')).toBe('07:05');
  });

  it('nie pokazuje północy, bo oznacza brak godziny', () => {
    expect(formatRecordedTime('2026-11-01T00:00:00Z')).toBeNull();
  });

  it('znosi brak i błędną wartość', () => {
    expect(formatRecordedTime(null)).toBeNull();
    expect(formatRecordedTime(undefined)).toBeNull();
    expect(formatRecordedTime('nie-data')).toBeNull();
  });
});

describe('toRecordedAt', () => {
  it('zapisuje godzinę, którą odczyta formatRecordedTime', () => {
    expect(formatRecordedTime(toRecordedAt('2026-11-01', '14:30'))).toBe('14:30');
  });

  it('pusta godzina czyści ją', () => {
    expect(formatRecordedTime(toRecordedAt('2026-11-01', ''))).toBeNull();
  });
});
