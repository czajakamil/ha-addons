import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';
import type { Category } from '../../api';
import { TagChip } from './TagChip';

const categories: Category[] = [
  { id: 'food', name: 'Jedzenie', path: 'jedzenie' },
  { id: 'groceries', name: 'Zakupy Spożywcze', parent_id: 'food', path: 'jedzenie/zakupy-spozywcze' }
];
const byId = new Map(categories.map((category) => [category.id, category]));

afterEach(cleanup);

describe('TagChip', () => {
  it('z showParent pokazuje kategorię nadrzędną pod nazwą', () => {
    render(<TagChip category={byId.get('groceries')} byId={byId} showParent />);
    expect(screen.getByText('Zakupy Spożywcze')).toBeTruthy();
    expect(screen.getByText('Jedzenie')).toBeTruthy();
  });

  it('kategoria główna nie dostaje drugiej linii', () => {
    const { container } = render(<TagChip category={byId.get('food')} byId={byId} showParent />);
    expect(container.querySelector('.tag-chip__parent')).toBeNull();
  });

  it('domyślnie pokazuje samą nazwę', () => {
    render(<TagChip category={byId.get('groceries')} byId={byId} />);
    expect(screen.queryByText('Jedzenie')).toBeNull();
  });
});
