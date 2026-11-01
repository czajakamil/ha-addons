/**
 * Rozwijany podział transakcji na pozycje — odpowiednik „splitu" z YNAB-a.
 *
 * Różnica jest w tym, czego NIE trzeba zapisywać: tag pozycji mieszka na
 * produkcie (`Product.category_id`), więc podział wynika z danych, które lista
 * transakcji i tak już pobiera. „Split" to po prostu transakcja, której pozycje
 * wskazują na więcej niż jeden tag.
 *
 * Wiersze siatki są płaskie — react-data-grid nie zna hierarchii — więc dziecko
 * niesie ze sobą referencję do transakcji rodzica. Dzięki temu każda kolumna
 * może się rozgałęzić na `row.kind` bez sięgania po indeks w tablicy.
 */

import type { Category, Item, Transaction, TransactionItem } from '../../api';

export type TransactionGridRow =
  | { kind: 'day'; id: string; date: string; totalCents: number | null; currency: string }
  | { kind: 'transaction'; id: string; transaction: Transaction }
  | {
      kind: 'item';
      id: string;
      transaction: Transaction;
      item: TransactionItem;
      productName: string;
      category: Category | null;
      totalCents: number;
    };

/** Wartość pozycji: cena jednostkowa × ilość, zaokrąglona do grosza. */
export const itemTotalCents = (item: TransactionItem) =>
  Math.round(item.unit_price_cents * item.quantity);

export interface SplitInfo {
  itemCount: number;
  /** Ile różnych tagów mają produkty pozycji. Więcej niż jeden = podział. */
  tagCount: number;
}

export const splitInfoOf = (
  transaction: Transaction,
  productMap: ReadonlyMap<string, Item>
): SplitInfo => {
  const tagIds = new Set<string>();
  for (const item of transaction.items) {
    const categoryId = productMap.get(item.product_id)?.category_id;
    if (categoryId) tagIds.add(categoryId);
  }
  return { itemCount: transaction.items.length, tagCount: tagIds.size };
};

/** Etykieta pozycji: ilość pokazujemy tylko wtedy, gdy wnosi informację. */
export const itemLabel = (productName: string, item: TransactionItem) => {
  if (item.quantity === 1) return productName;
  const quantity = Number.isInteger(item.quantity)
    ? String(item.quantity)
    : item.quantity.toFixed(3).replace(/0+$/, '').replace(/[.,]$/, '');
  return `${quantity}${item.unit ? ` ${item.unit}` : ' ×'} ${productName}`;
};

export const buildTransactionGridRows = (
  transactions: readonly Transaction[],
  expandedIds: ReadonlySet<string>,
  productMap: ReadonlyMap<string, Item>,
  categoryMap: ReadonlyMap<string, Category>
): TransactionGridRow[] => {
  const rows: TransactionGridRow[] = [];
  let index = 0;

  while (index < transactions.length) {
    const date = transactions[index].date ?? '';
    let end = index;
    while (end < transactions.length && (transactions[end].date ?? '') === date) end += 1;
    const day = transactions.slice(index, end);
    const currency = day[0].currency;
    // Suma dnia ma sens tylko w jednej walucie
    const totalCents = day.every((tx) => tx.currency === currency)
      ? day.reduce((sum, tx) => sum + tx.total_cents, 0)
      : null;
    rows.push({ kind: 'day', id: `day:${date}`, date, totalCents, currency });

    for (const transaction of day) {
      rows.push({ kind: 'transaction', id: transaction.id, transaction });
      if (!expandedIds.has(transaction.id)) continue;

      for (const item of transaction.items) {
        const product = productMap.get(item.product_id);
        const categoryId = product?.category_id ?? null;
        rows.push({
          kind: 'item',
          id: `${transaction.id}:${item.id}`,
          transaction,
          item,
          productName: product?.name ?? 'nieznany produkt',
          category: categoryId ? categoryMap.get(categoryId) ?? null : null,
          totalCents: itemTotalCents(item)
        });
      }
    }
    index = end;
  }

  return rows;
};
