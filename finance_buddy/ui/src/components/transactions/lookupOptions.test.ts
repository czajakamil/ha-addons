import { describe, expect, it } from 'vitest';
import type { Account, Store } from '../../api';
import {
  buildCategoryOptions,
  buildStoreOptions,
  buildTransferOptions,
  defaultLookupHighlight,
  filterLookupOptions,
  findSelectedLookupIndex,
  resolveTypedLookup,
  selectableAccounts,
  splitLookupMatch,
  type LookupOption
} from './lookupOptions';

const account = (id: string, name: string, currency = 'PLN') => ({ id, name, currency }) as Account;
const store = (id: string, name: string, group_name?: string) => ({ id, name, group_name }) as Store;
const entry = (id: string, label: string): LookupOption => ({ id, label, kind: 'entry' });

describe('buildTransferOptions', () => {
  it('niesie id konta niezależnie od nawiasów i początku nazwy', () => {
    const options = buildTransferOptions([
      account('a1', 'Revolut (EUR)'),
      account('a2', 'Main Account (PLN)'),
      account('a3', 'Nagrody')
    ]);

    expect(options.map((option) => [option.id, option.label, option.rowLabel, option.kind])).toEqual([
      ['a1', 'Przelew: Revolut (EUR) (PLN)', 'Revolut (EUR) (PLN)', 'transfer'],
      ['a2', 'Przelew: Main Account (PLN) (PLN)', 'Main Account (PLN) (PLN)', 'transfer'],
      ['a3', 'Przelew: Nagrody (PLN)', 'Nagrody (PLN)', 'transfer']
    ]);
  });
});

describe('buildStoreOptions', () => {
  it('łączy sklepy jednej grupy w pozycję z id pierwszego i pomija puste nazwy', () => {
    const options = buildStoreOptions([
      store('s1', 'ZABKA 123', 'Żabka'),
      store('s2', 'ZABKA 456', 'Żabka'),
      store('s3', 'Orlen'),
      store('s4', '  ')
    ]);

    expect(options).toEqual([entry('s1', 'Żabka'), entry('s3', 'Orlen')]);
  });
});

describe('resolveTypedLookup', () => {
  const options = [entry('s1', 'Revolut'), entry('s2', 'Revolut Bank'), entry('c1', 'Dom'), entry('c2', 'Dom')];

  it('rozróżnia puste pole, dokładne dopasowanie i brak dopasowania', () => {
    expect(resolveTypedLookup(options, '  ')).toEqual({ kind: 'empty' });
    expect(resolveTypedLookup(options, ' revolut ')).toEqual({ kind: 'match', option: options[0] });
    expect(resolveTypedLookup(options, 'rev')).toEqual({ kind: 'none' });
  });

  it('nie zgaduje, gdy dokładne dopasowanie jest niejednoznaczne', () => {
    expect(resolveTypedLookup(options, 'dom')).toEqual({ kind: 'none' });
  });
});

describe('defaultLookupHighlight', () => {
  const options = [entry('s2', 'Revolut Bank'), entry('s1', 'Revolut')];

  it('bez tekstu nie podświetla niczego', () => {
    expect(defaultLookupHighlight(options, '')).toBe(-1);
  });

  it('woli dokładne dopasowanie od pierwszego na liście', () => {
    expect(defaultLookupHighlight(filterLookupOptions(options, 'revolut'), 'revolut')).toBe(1);
    expect(defaultLookupHighlight(filterLookupOptions(options, 'rev'), 'rev')).toBe(0);
  });

  it('bez wyników nie podświetla niczego', () => {
    expect(defaultLookupHighlight(filterLookupOptions(options, 'xyz'), 'xyz')).toBe(-1);
  });
});

describe('findSelectedLookupIndex', () => {
  it('znajduje tylko dokładną nazwę', () => {
    const options = [entry('s2', 'Revolut Bank'), entry('s1', 'Revolut')];
    expect(findSelectedLookupIndex(options, 'revolut')).toBe(1);
    expect(findSelectedLookupIndex(options, 'rev')).toBe(-1);
    expect(findSelectedLookupIndex(options, '')).toBe(-1);
  });
});

describe('splitLookupMatch', () => {
  it('zachowuje wielkość liter i całe słowo', () => {
    expect(splitLookupMatch('Karta Revolut', 'rev')).toEqual({
      before: 'Karta ',
      match: 'Rev',
      after: 'olut'
    });
  });

  it('zwraca null, gdy wiersz nie zawiera fragmentu', () => {
    expect(splitLookupMatch('Revolut', 'przelew')).toBeNull();
    expect(splitLookupMatch('Revolut', '')).toBeNull();
  });
});

describe('selectableAccounts', () => {
  const open = { id: 'a1', is_closed: false } as Account;
  const closed = { id: 'a2', is_closed: true } as Account;

  it('ukrywa zarchiwizowane konta', () => {
    expect(selectableAccounts([open, closed])).toEqual([open]);
    expect(selectableAccounts([open, closed], 'a1')).toEqual([open]);
  });

  it('zostawia zarchiwizowane konto, które pole już ma ustawione', () => {
    expect(selectableAccounts([open, closed], 'a2')).toEqual([open, closed]);
  });
});

describe('buildCategoryOptions', () => {
  it('pokazuje pełną ścieżkę w kolejności drzewa', () => {
    const options = buildCategoryOptions([
      { id: 'c', name: 'Domena Internetowa', parent_id: 'd' },
      { id: 'r', name: 'Rachunki', parent_id: null },
      { id: 'd', name: 'Dom', parent_id: null },
      { id: 'x', name: 'Czynsz', parent_id: 'r' }
    ]);
    expect(options.map((option) => [option.id, option.label])).toEqual([
      ['r', 'Rachunki'],
      ['x', 'Rachunki › Czynsz'],
      ['d', 'Dom'],
      ['c', 'Dom › Domena Internetowa']
    ]);
  });
});
