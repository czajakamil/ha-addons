import { useState } from 'react';
import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, beforeAll, describe, expect, it, vi } from 'vitest';
import LookupAutocompleteInput from './LookupAutocompleteInput';
import type { LookupOption } from './lookupOptions';

const options: LookupOption[] = [
  { id: 's1', label: 'Revolut Bank', kind: 'entry' },
  { id: 's2', label: 'Orlen', kind: 'entry' },
  { id: 'a1', label: 'Przelew: Revolut (EUR) (PLN)', rowLabel: 'Revolut (EUR) (PLN)', kind: 'transfer' }
];

const setup = (initial = '', createLabel?: (text: string) => string) => {
  const onCommit = vi.fn();
  const onCancel = vi.fn();
  const Harness = () => {
    const [value, setValue] = useState(initial);
    return (
      <LookupAutocompleteInput
        inputValue={value}
        options={options}
        onInputChange={setValue}
        onCommit={onCommit}
        onCancel={onCancel}
        createLabel={createLabel}
      />
    );
  };
  render(<Harness />);
  return { input: screen.getByRole('textbox'), onCommit, onCancel };
};

beforeAll(() => {
  Element.prototype.scrollIntoView = vi.fn();
});

afterEach(cleanup);

describe('LookupAutocompleteInput', () => {
  it('po wejściu pokazuje całą listę, sekcje i wybraną pozycję jako tło wiersza', () => {
    setup('Orlen');

    expect(screen.getByText('Sklepy')).toBeTruthy();
    expect(screen.getByText('Przelew na konto')).toBeTruthy();
    expect(screen.getAllByRole('button')).toHaveLength(3);
    const selected = screen.getByRole('button', { name: 'Orlen' });
    expect(selected.className).toContain('lookup-autocomplete__option--selected');
    expect(selected.getAttribute('aria-current')).toBe('true');
  });

  it('podświetla fragment bez rozbijania słowa i zapisuje opcję z id', () => {
    const { input, onCommit } = setup();

    fireEvent.change(input, { target: { value: 'rev' } });
    const transfer = screen.getByRole('button', { name: 'Revolut (EUR) (PLN)' });
    expect(transfer.querySelector('mark')?.textContent).toBe('Rev');
    expect(transfer.textContent).toBe('Revolut (EUR) (PLN)');

    fireEvent.mouseDown(transfer);
    expect(onCommit).toHaveBeenCalledWith(options[2]);
  });

  it('Enter przy pustym polu czyści wartość zamiast wybierać pierwszą pozycję', () => {
    const { input, onCommit } = setup();

    fireEvent.keyDown(input, { key: 'Enter' });
    expect(onCommit).toHaveBeenCalledWith(null);
  });

  it('wyjście z niepasującym tekstem anuluje edycję', () => {
    const { input, onCommit, onCancel } = setup();

    fireEvent.change(input, { target: { value: 'rev' } });
    fireEvent.blur(input);
    expect(onCommit).not.toHaveBeenCalled();
    expect(onCancel).toHaveBeenCalledOnce();
  });

  describe('tworzenie nowej pozycji', () => {
    const createLabel = (text: string) => `Utwórz sklep „${text}”`;

    it('Enter tworzy, gdy nic nie pasuje', () => {
      const { input, onCommit } = setup('', createLabel);

      fireEvent.change(input, { target: { value: '  Biedronak ' } });
      expect(screen.getByRole('button', { name: 'Utwórz sklep „Biedronak”' })).toBeTruthy();
      fireEvent.keyDown(input, { key: 'Enter' });
      expect(onCommit).toHaveBeenCalledWith(
        expect.objectContaining({ kind: 'create', label: 'Biedronak' })
      );
    });

    it('przy częściowym dopasowaniu Enter wybiera istniejącą pozycję, a „Utwórz” jest na końcu', () => {
      const { input, onCommit } = setup('', createLabel);

      fireEvent.change(input, { target: { value: 'orl' } });
      const buttons = screen.getAllByRole('button');
      expect(buttons[buttons.length - 1].textContent).toBe('Utwórz sklep „orl”');
      fireEvent.keyDown(input, { key: 'Enter' });
      expect(onCommit).toHaveBeenCalledWith(options[1]);
    });

    it('nie proponuje tworzenia, gdy tekst to dokładnie istniejąca pozycja', () => {
      const { input } = setup('', createLabel);

      fireEvent.change(input, { target: { value: 'orlen' } });
      expect(screen.queryByText(/Utwórz sklep/)).toBeNull();
    });

    it('wyjście z pola nie tworzy', () => {
      const { input, onCommit, onCancel } = setup('', createLabel);

      fireEvent.change(input, { target: { value: 'Biedronak' } });
      fireEvent.blur(input);
      expect(onCommit).not.toHaveBeenCalled();
      expect(onCancel).toHaveBeenCalledOnce();
    });
  });
});
