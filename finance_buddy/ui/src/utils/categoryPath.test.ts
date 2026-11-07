import { describe, expect, it } from 'vitest';
import { formatCategoryPath } from './categoryPath';

describe('formatCategoryPath', () => {
  it('joins segments with a chevron and skips empty ones', () => {
    expect(formatCategoryPath('Rachunki/Czynsz')).toBe('Rachunki › Czynsz');
    expect(formatCategoryPath('🍔 Jedzenie')).toBe('🍔 Jedzenie');
    expect(formatCategoryPath('a//b/')).toBe('a › b');
    expect(formatCategoryPath(undefined)).toBe('');
  });
});
