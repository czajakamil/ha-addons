import { describe, expect, it } from 'vitest';
import { formatRecordedTime, formatTypedDate, parseTypedDate, parseTypedTime, toRecordedAt } from './date';

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

describe('wpisywanie daty i godziny', () => {
  it('rozumie datę po polsku i ISO', () => {
    expect(parseTypedDate('1.11.2026')).toBe('2026-11-01');
    expect(parseTypedDate('2026-11-01')).toBe('2026-11-01');
    expect(formatTypedDate('2026-11-01')).toBe('01.11.2026');
  });

  it('odrzuca datę, której nie ma, a pusta jest dozwolona', () => {
    expect(parseTypedDate('31.02.2026')).toBeNull();
    expect(parseTypedDate('1.11')).toBeNull();
    expect(parseTypedDate(' ')).toBe('');
  });

  it('rozumie godzinę i odrzuca niemożliwą', () => {
    expect(parseTypedTime('9:15')).toBe('09:15');
    expect(parseTypedTime('14.30')).toBe('14:30');
    expect(parseTypedTime('24:00')).toBeNull();
    expect(parseTypedTime('')).toBe('');
  });
});
