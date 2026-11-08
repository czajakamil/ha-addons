import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, beforeAll, describe, expect, it, vi } from 'vitest';
import DataTable from './DataTable';

beforeAll(() => {
  // jsdom nie ma matchMedia (useMediaQuery), CSS.escape ani ResizeObserver (react-data-grid)
  window.matchMedia ??= ((query: string) => ({
    matches: false,
    media: query,
    onchange: null,
    addEventListener: () => {},
    removeEventListener: () => {},
    addListener: () => {},
    removeListener: () => {},
    dispatchEvent: () => false
  })) as typeof window.matchMedia;
  Element.prototype.scrollIntoView ??= () => {};
  globalThis.CSS ??= { escape: (value: string) => value } as unknown as typeof CSS;
  globalThis.ResizeObserver ??= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  } as unknown as typeof ResizeObserver;
});

type Row = { id: string; name: string };

const setup = () => {
  const onCellDoubleClick = vi.fn();
  render(
    <DataTable<Row>
      rows={[{ id: '1', name: 'Biedronka' }]}
      rowKeyGetter={(row) => row.id}
      enableVirtualization={false}
      columns={[
        { key: 'name', name: 'Nazwa', renderCell: ({ row }) => <span>{row.name}</span> },
        { key: 'edit', name: 'Edycja', renderCell: () => <input aria-label="pole" defaultValue="Lidl" /> }
      ]}
      onCellDoubleClick={onCellDoubleClick}
    />
  );
  return { onCellDoubleClick };
};

afterEach(cleanup);

describe('DataTable', () => {
  it('dwuklik w komórce uruchamia akcję komórki', () => {
    const { onCellDoubleClick } = setup();
    fireEvent.doubleClick(screen.getByText('Biedronka'));
    expect(onCellDoubleClick).toHaveBeenCalledTimes(1);
  });

  it('dwuklik w otwartym polu edycji nie restartuje edycji (zaznaczanie słowa)', () => {
    const { onCellDoubleClick } = setup();
    fireEvent.doubleClick(screen.getByLabelText('pole'));
    expect(onCellDoubleClick).not.toHaveBeenCalled();
  });
});
