/**
 * Stan formularza paragonu i przejścia między nim a etykietą z API.
 *
 * Pola liczbowe trzymamy w formularzu jako **tekst**, a nie liczby: przepisywanie
 * z paragonu to ciąg stanów pośrednich („12,", „12,4"), a wymuszanie liczby na
 * każdym naciśnięciu klawisza kasuje to, co użytkownik właśnie pisze. Konwersja
 * idzie w jednym miejscu — przy zapisie.
 */

import type {
  ReceiptDraftCommitItem,
  ReceiptDraftItem,
  ReceiptDraftPayload
} from '../../api';
import type { ItemMatch } from '../../api';
import { parseAmountToCents } from '../../utils/format';

/** Sentynel w kolumnie produktu: załóż nowy produkt z prefillem nazwy. */
export const NEW_PRODUCT = '__new__';
/** Sentynel: pozycja nie jest towarem (np. linia rabatu) — nie trafia do transakcji. */
export const NO_PRODUCT = '';

export interface ItemRow {
  /** Klucz React-a, stabilny mimo przestawiania wierszy. */
  key: string;
  rawName: string;
  productCode: string;
  quantity: string;
  unit: string;
  unitPrice: string;
  lineTotal: string;
  /** Wartość liczy się sama z ilość × cena, dopóki nie zostanie wpisana ręcznie. */
  lineTotalManual: boolean;
  discount: string;
  discountLabel: string;
  taxCode: string;
  productId: string;
  newProductName: string;
  categoryPath: string;
  matchType: ItemMatch['match_type'];
  originalMatchProductId: string | null;
  candidateIds: string[];
}

export interface ReceiptForm {
  storeName: string;
  nip: string;
  /** Tekst `RRRR-MM-DD GG:MM`, tak jak drukuje paragon. */
  dateText: string;
  total: string;
  currency: string;
  discountsDeclared: string;
  receiptDiscount: string;
  notes: string;
  accountId: string;
  storeId: string;
  /** Tag transakcji jako ścieżka — brakujące segmenty zakłada się przy zapisie. */
  categoryPath: string;
}

let rowCounter = 0;
const nextRowKey = () => `row-${(rowCounter += 1)}`;

export const centsToInput = (cents: number | null | undefined) =>
  cents == null ? '' : (cents / 100).toFixed(2);

export const inputToCents = (text: string): number | null => {
  const trimmed = (text ?? '').trim();
  if (!trimmed) return null;
  return parseAmountToCents(trimmed);
};

export const inputToNumber = (text: string): number | null => {
  const value = Number((text ?? '').replace(',', '.').trim());
  return Number.isFinite(value) ? value : null;
};

const isDiscountLine = (item: ReceiptDraftItem) =>
  (item.line_total_cents ?? 0) < 0 || /rabat|opust/i.test(item.raw_name);

/**
 * Nazwa produktu to inny byt niż surowy tekst z paragonu (ten zostaje bez
 * zmian — jedzie do aliasów i dopasowania kolejnych paragonów). Paragon
 * fiskalny drukuje WIELKIMI LITERAMI, więc dla podpowiedzi nazwy produktu
 * cofamy do zdania — ale tylko gdy cały tekst krzyczy wielkimi; ręczną
 * poprawkę z mieszaną wielkością liter zostawiamy bez zmian. Ta sama reguła
 * co `_prefill_name` w backend/finance_app/services/product_matching.py —
 * zanim odpowiedź dopasowania dojedzie z API, wiersz i tak musi się czymś
 * wypełnić.
 */
const sentenceCaseIfShouting = (text: string): string =>
  /\p{Lu}/u.test(text) && !/\p{Ll}/u.test(text) ? text[0] + text.slice(1).toLowerCase() : text;

export const emptyRow = (): ItemRow => ({
  key: nextRowKey(),
  rawName: '',
  productCode: '',
  quantity: '1',
  unit: '',
  unitPrice: '',
  lineTotal: '',
  lineTotalManual: false,
  discount: '',
  discountLabel: '',
  taxCode: '',
  productId: NEW_PRODUCT,
  newProductName: '',
  categoryPath: '',
  matchType: 'none',
  originalMatchProductId: null,
  candidateIds: []
});

export const rowFromItem = (item: ReceiptDraftItem, match?: ItemMatch): ItemRow => {
  const candidateIds = match?.candidates.map((candidate) => candidate.product_id) ?? [];
  let productId: string = NO_PRODUCT;
  if (!isDiscountLine(item)) {
    if (match?.matched_product_id) productId = match.matched_product_id;
    else if (candidateIds.length) productId = candidateIds[0];
    else productId = NEW_PRODUCT;
  }

  return {
    key: nextRowKey(),
    rawName: item.raw_name ?? '',
    productCode: item.product_code ?? '',
    quantity: item.quantity == null ? '1' : String(item.quantity),
    unit: item.unit ?? '',
    unitPrice: centsToInput(item.unit_price_cents),
    lineTotal: centsToInput(item.line_total_cents),
    // Wartość z OCR traktujemy jak wpisaną ręcznie: przeliczenie z ilość × cena
    // nadpisałoby odczyt z paragonu tam, gdzie cena jednostkowa jest zaokrąglona.
    lineTotalManual: item.line_total_cents != null,
    discount: centsToInput(item.discount_cents),
    discountLabel: item.discount_label ?? '',
    taxCode: item.tax_code ?? '',
    productId,
    newProductName: sentenceCaseIfShouting(match?.prefill_name || item.raw_name || ''),
    categoryPath: match?.suggested_category_path ?? '',
    matchType: match?.match_type ?? 'none',
    originalMatchProductId: match?.matched_product_id ?? null,
    candidateIds
  };
};

export const rowsFromPayload = (payload: ReceiptDraftPayload, matches: ItemMatch[] = []) =>
  (payload.items ?? []).map((item, index) => rowFromItem(item, matches[index]));

export const formFromPayload = (
  payload: ReceiptDraftPayload,
  defaults: { storeId?: string | null } = {}
): ReceiptForm => ({
  storeName: payload.store_name ?? '',
  nip: payload.nip ?? '',
  dateText: payload.happened_at ? payload.happened_at.slice(0, 16).replace('T', ' ') : '',
  total: centsToInput(payload.total_cents),
  currency: payload.currency || 'PLN',
  discountsDeclared: centsToInput(payload.discounts_declared_cents),
  receiptDiscount: centsToInput(payload.receipt_discount_cents),
  notes: payload.notes ?? '',
  accountId: '',
  storeId: defaults.storeId ?? '',
  categoryPath: ''
});

const itemFromRow = (row: ItemRow): ReceiptDraftItem => {
  const discount = inputToCents(row.discount);
  return {
    raw_name: row.rawName.trim(),
    product_code: row.productCode.trim() || null,
    quantity: inputToNumber(row.quantity),
    unit: row.unit.trim() || null,
    unit_price_cents: inputToCents(row.unitPrice),
    line_total_cents: inputToCents(row.lineTotal),
    // Rabat normalizujemy na ujemny, żeby nie trzeba było wbijać minusa przy
    // każdej pozycji; opis rabatu ma sens tylko razem z kwotą.
    discount_cents: discount ? -Math.abs(discount) : null,
    discount_label: discount && row.discountLabel.trim() ? row.discountLabel.trim() : null,
    tax_code: row.taxCode.trim().toUpperCase() || null
  };
};

export const payloadFromForm = (
  form: ReceiptForm,
  rows: ItemRow[],
  happenedAt: string | null
): ReceiptDraftPayload => {
  const declared = inputToCents(form.discountsDeclared);
  const receiptDiscount = inputToCents(form.receiptDiscount);
  return {
    store_name: form.storeName.trim() || null,
    nip: form.nip.trim() || null,
    happened_at: happenedAt,
    total_cents: inputToCents(form.total),
    currency: form.currency.trim() || 'PLN',
    items: rows.filter((row) => row.rawName.trim()).map(itemFromRow),
    discounts_declared_cents: declared ? Math.abs(declared) : null,
    receipt_discount_cents: receiptDiscount ? -Math.abs(receiptDiscount) : null,
    notes: form.notes.trim() || null
  };
};

/** Pozycje gotowe dla bramki zapisu — bez wierszy, które nie są towarem. */
export const commitItemsFromRows = (rows: ItemRow[]): ReceiptDraftCommitItem[] =>
  rows
    .filter((row) => row.rawName.trim() && row.productId !== NO_PRODUCT)
    .map((row) => {
      const isNew = row.productId === NEW_PRODUCT;
      const changed = !isNew && row.productId !== row.originalMatchProductId;
      return {
        raw_name: row.rawName.trim(),
        quantity: inputToNumber(row.quantity) || 1,
        unit: row.unit.trim() || null,
        unit_price_cents: inputToCents(row.unitPrice),
        line_total_cents: inputToCents(row.lineTotal),
        product_id: isNew ? null : row.productId,
        new_product_name: isNew ? row.newProductName.trim() || row.rawName.trim() : null,
        category_path: isNew ? row.categoryPath.trim() || null : null,
        // Pętla uczenia: alias zapisujemy, gdy dopasowanie było niepewne albo
        // gdy użytkownik je poprawił — potwierdzenie pewnego trafienia niczego
        // nie uczy.
        learn_alias: isNew || changed || row.matchType === 'fuzzy' || row.matchType === 'none'
      };
    });

/**
 * Do czego sumują się pozycje razem z rabatami — na potrzeby stopki tabeli.
 *
 * Ta sama arytmetyka co `itemsBalanceCents` w regułach kontroli, ale liczona z
 * wierszy formularza i bez wymagania kompletu wartości: stopka ma pokazywać
 * bieżący stan także w połowie przepisywania.
 */
export const rowsBalance = (rows: ItemRow[], receiptDiscount: string) => {
  const gross = rows.reduce((sum, row) => sum + (inputToCents(row.lineTotal) ?? 0), 0);
  const itemDiscounts = rows.reduce(
    (sum, row) => sum + Math.abs(inputToCents(row.discount) ?? 0),
    0
  );
  const onReceipt = Math.abs(inputToCents(receiptDiscount) ?? 0);
  return { gross, discounts: itemDiscounts + onReceipt, balance: gross - itemDiscounts - onReceipt };
};
