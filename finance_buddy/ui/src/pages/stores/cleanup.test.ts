import { describe, expect, it } from 'vitest';
import type { Store } from '../../api';
import { buildCleanupItems, maskAccountNumbers, normalizeStoreName } from './cleanup';

let nextId = 0;
const store = (name: string, extra: Partial<Store> = {}): Store => ({
  id: `s${(nextId += 1)}`,
  name,
  include_in_payee_list: true,
  automatically_categorize_payee: false,
  automatic_category_id: 'cat',
  renaming_rules: [],
  is_deleted: false,
  ...extra
});

const reasonOf = (name: string, others: Store[] = [], owner = 'Jan Kowalski') =>
  buildCleanupItems([store(name), ...others], owner).find((item) => item.store.name === name);

describe('normalizeStoreName', () => {
  it('ignoruje wielkość liter, spacje i formy prawne', () => {
    expect(normalizeStoreName('  ABSOLUT   Sp. z o.o.')).toBe('absolut');
    expect(normalizeStoreName('Absolut Sp Z O.o.')).toBe('absolut');
    expect(normalizeStoreName('Orlen S.A.')).toBe('orlen');
  });
});

describe('maskAccountNumbers', () => {
  it('maskuje IBAN ze spacjami i bez, a sąsiednie słowa zostawia', () => {
    expect(maskAccountNumbers('Account: LT13 3250 0000 0000 1248 SENT')).toBe('Account: LT13 •••• 1248 SENT');
    expect(maskAccountNumbers('LT133250000000001248')).toBe('LT13 •••• 1248');
    expect(maskAccountNumbers('PL61109010140000071219812874')).toBe('PL61 •••• 2874');
  });

  it('maskuje krajowy numer konta, ale nie krótkie liczby', () => {
    expect(maskAccountNumbers('61 1090 1014 0000 0712 1981 2874')).toBe('6110 •••• 2874');
    expect(maskAccountNumbers('BIEDRONKA 4521 KRAKOW')).toBe('BIEDRONKA 4521 KRAKOW');
  });
});

describe('buildCleanupItems', () => {
  it('wykrywa duplikaty po normalizacji', () => {
    const items = buildCleanupItems([store('Absolut Sp. z o.o.'), store('ABSOLUT'), store('Lidl')]);
    expect(items.map((item) => item.reason)).toEqual(['duplicate', 'duplicate']);
    expect(items[0].duplicateOf?.name).toBe('ABSOLUT');
  });

  it('wykrywa surowe nazwy: cyfry, adres, kod pocztowy, IBAN', () => {
    expect(reasonOf('1024902301')).toMatchObject({ reason: 'raw', rawKind: 'digits' });
    expect(reasonOf('ADYEN N.V. SIMON CARMIGGELSTRAAT 6 1011 DJ A NETHERLANDS')).toMatchObject({
      reason: 'raw',
      rawKind: 'address',
      proposedName: 'Adyen'
    });
    expect(reasonOf('Sklep 30-001 Kraków')?.reason).toBe('raw');
    expect(reasonOf('Sklep ul. Długa 5')?.reason).toBe('raw');
    expect(reasonOf('Czynsz LT133250000000001248')).toMatchObject({ reason: 'raw', rawKind: 'account', proposedName: 'Czynsz' });
  });

  it('nie uznaje zwykłych nazw za surowe', () => {
    expect(reasonOf('IKEA.pl')).toBeUndefined();
    expect(reasonOf('BIEDRONKA 4521 KRAKOW')).toBeUndefined();
    expect(reasonOf('Stacja 24')).toBeUndefined();
  });

  it('przelew własny wymaga nazwiska właściciela i znacznika', () => {
    expect(reasonOf('1/JAN KOWALSKI - SENT FROM REVOLUT')?.reason).toBe('self');
    expect(reasonOf('Jan Kowalski Account: LT13 3250 0000 0000 1248')?.reason).toBe('self');
    expect(reasonOf('ANNA NOWAK - SENT FROM REVOLUT')).toBeUndefined();
    expect(reasonOf('Jan Kowalski')).toBeUndefined();
    expect(reasonOf('JAN KOWALSKI SENT FROM', [], '')).toBeUndefined();
  });

  it('bez kategorii: tylko gdy nie ma mocniejszego powodu, a każdy sklep ma jeden powód', () => {
    const items = buildCleanupItems([
      store('Adwokat', { automatic_category_id: null }),
      store('123456', { automatic_category_id: null })
    ]);
    expect(items.map((item) => item.reason)).toEqual(['uncategorized', 'raw']);
  });

  it('pomija zarchiwizowane', () => {
    expect(buildCleanupItems([store('Adwokat', { automatic_category_id: null, is_deleted: true })])).toEqual([]);
  });
});
