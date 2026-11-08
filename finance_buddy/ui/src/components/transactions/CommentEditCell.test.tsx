import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import CommentEditCell from './CommentEditCell';

const setup = () => {
  const onCommit = vi.fn();
  const onCancel = vi.fn();
  render(
    <CommentEditCell value="Zakupy" onChange={vi.fn()} onCommit={onCommit} onCancel={onCancel} />
  );
  return { textarea: screen.getByRole('textbox'), onCommit, onCancel };
};

afterEach(cleanup);

describe('CommentEditCell', () => {
  it('Esc anuluje i blur po nim niczego nie zapisuje', () => {
    const { textarea, onCommit, onCancel } = setup();
    fireEvent.keyDown(textarea, { key: 'Escape' });
    fireEvent.blur(textarea);
    expect(onCancel).toHaveBeenCalledTimes(1);
    expect(onCommit).not.toHaveBeenCalled();
  });

  it('Cmd+Enter zapisuje raz, mimo blura po oddaniu fokusu', () => {
    const { textarea, onCommit } = setup();
    fireEvent.keyDown(textarea, { key: 'Enter', metaKey: true });
    fireEvent.blur(textarea);
    expect(onCommit).toHaveBeenCalledTimes(1);
  });

  it('kliknięcie obok (blur) zapisuje', () => {
    const { textarea, onCommit } = setup();
    fireEvent.blur(textarea);
    expect(onCommit).toHaveBeenCalledTimes(1);
  });
});
