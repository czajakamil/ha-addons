import { describe, expect, it } from 'vitest';
import type { Store } from '../../api';
import {
  buildCleanupItems as buildFromGroups,
  buildMergePlans,
  groupCategorySuggestions,
  maskAccountNumbers,
  normalizeStoreName
} from './cleanup';
import { buildStoreGroups, toStatsMap } from './groups';

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

const buildCleanupItems = (stores: Store[], ownerName = '', ambiguous: ReadonlySet<string> = new Set()) =>
  buildFromGroups(buildStoreGroups(stores, new Map()), ownerName, ambiguous);

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

  it('przelew własny nie zgaduje po samym imieniu ani po fragmencie słowa', () => {
    expect(reasonOf('JAN NOWAK - PRZELEW - Account: 4211 5446', [], 'Jan')).toBeUndefined();
    expect(reasonOf('JAN NOWAK - PRZELEW - Account: 4211 5446', [], 'jan')).toBeUndefined();
    expect(reasonOf('JANINA KOWALSKA - Account: 4211 5446', [], 'Jan Kowalsk')).toBeUndefined();
  });

  it('bez kategorii: tylko gdy nie ma mocniejszego powodu, a każdy sklep ma jeden powód', () => {
    const items = buildCleanupItems([
      store('Adwokat', { automatic_category_id: null }),
      store('123456', { automatic_category_id: null })
    ]);
    expect(items.map((item) => item.reason)).toEqual(['uncategorized', 'raw']);
  });

  it('alias niejednoznaczny z serwera daje duplikat także bez drugiej aktywnej nazwy', () => {
    const lone = store('Lidl');
    const [item] = buildCleanupItems([lone], '', new Set([lone.id]));
    expect(item).toMatchObject({ reason: 'duplicate', isAmbiguous: true });
  });

  it('grupa z uporządkowaną nazwą nie jest surowa, choć nazwa z banku zostaje', () => {
    const items = buildCleanupItems([
      store('BIEDRONKA ul. Długa 5 30-001 Kraków', { group_name: 'Biedronka' }),
      store('Sklep ul. Krótka 2')
    ]);
    expect(items.map((item) => [item.group.name, item.reason])).toEqual([['Sklep ul. Krótka 2', 'raw']]);
  });

  it('kategoria jednego sklepu wystarcza całej grupie, a grupa bez kategorii to jedna pozycja', () => {
    const items = buildCleanupItems([
      store('LIDL 1', { group_name: 'Lidl', automatic_category_id: null }),
      store('LIDL 2', { group_name: 'Lidl' }),
      store('Adwokat A', { group_name: 'Adwokat', automatic_category_id: null }),
      store('Adwokat B', { group_name: 'Adwokat', automatic_category_id: null })
    ]);
    expect(items).toHaveLength(1);
    expect(items[0]).toMatchObject({ reason: 'uncategorized', group: { name: 'Adwokat' } });
  });

  it('nazwy z banku w jednej grupie nie są duplikatami, a dwie grupy o tej samej nazwie są', () => {
    expect(buildCleanupItems([store('Absolut Sp. z o.o.', { group_name: 'Absolut' }), store('ABSOLUT')])).toEqual([]);
    const items = buildCleanupItems([store('Absolut Sp. z o.o.'), store('ABSOLUT', { group_name: 'Absolut S.A.' })]);
    expect(items.map((item) => item.reason)).toEqual(['duplicate', 'duplicate']);
    expect(items[0].duplicateOf?.key).toBe(items[1].group.key);
  });

  it('pomija zarchiwizowane', () => {
    expect(buildCleanupItems([store('Adwokat', { automatic_category_id: null, is_deleted: true })])).toEqual([]);
  });
});

describe('buildMergePlans', () => {
  it('każdy duplikat scala się osobno, do grupy z największą liczbą transakcji', () => {
    const payu = store('PayU');
    const payuSa = store('PayU S.A.', { automatic_category_id: 'fees' });
    const targdisA = store('Nowy Targdis Sikorskie');
    const targdisB = store('Nowy Targdis Sikorskie');
    const stats = toStatsMap([
      { store_id: payu.id, count: 0, spent_cents_12m: 0, last_date: null },
      { store_id: payuSa.id, count: 3, spent_cents_12m: 0, last_date: null },
      { store_id: targdisA.id, count: 60, spent_cents_12m: 0, last_date: null },
      { store_id: targdisB.id, count: 18, spent_cents_12m: 0, last_date: null }
    ]);
    const groups = buildStoreGroups([payu, payuSa, targdisA, targdisB, store('Lidl')], stats);
    // Dwie identyczne nazwy z banku: oba aliasy są niejednoznaczne
    const items = buildFromGroups(groups, '', new Set([targdisA.id, targdisB.id]));

    const plans = buildMergePlans(items);

    expect(plans).toHaveLength(2);
    expect(plans.find((plan) => plan.targetId === payuSa.id)).toMatchObject({
      sourceIds: [payu.id],
      name: 'PayU S.A.',
      categoryId: 'fees'
    });
    expect(plans.find((plan) => plan.targetId === targdisA.id)).toMatchObject({ sourceIds: [targdisB.id] });
  });

  it('pomija samotny niejednoznaczny alias, bo nie ma czego scalić', () => {
    const lone = store('Lidl');
    expect(buildMergePlans(buildCleanupItems([lone], '', new Set([lone.id])))).toEqual([]);
  });
});

describe('groupCategorySuggestions', () => {
  it('w grupie wygrywa podpowiedź sklepu z największą liczbą transakcji', () => {
    const a = store('LIDL 1', { group_name: 'Lidl' });
    const b = store('LIDL 2', { group_name: 'Lidl' });
    const groups = buildStoreGroups([a, b], new Map());
    const result = groupCategorySuggestions(groups, [
      { store_id: a.id, category_id: 'home', count: 2, total: 3 },
      { store_id: b.id, category_id: 'food', count: 40, total: 50 }
    ]);
    expect(result.get(groups[0].key)).toEqual({ categoryId: 'food', count: 40, total: 50 });
  });
});
