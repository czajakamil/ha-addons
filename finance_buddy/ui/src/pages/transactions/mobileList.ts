// Lista transakcji na telefonie: dni z sumą i wiersze z gotowymi etykietami. Bez stanu i bez JSX.
import type { Account, Category, Item, Store, Transaction } from '../../api';
import { resolveTagColor } from '../../components/tags/tagColors';
import { formatRecordedTime } from '../../utils/date';
import { BALANCE_ADJUSTMENT_LABEL, isBalanceAdjustment, userComment } from '../../utils/transactionLabels';
import { itemCategory } from '../transactionDetails/model';

// Kropki mieszczą się w wierszu do trzech; resztę mówi etykieta „N kategorii"
const MAX_DOTS = 3;

export type MobileRowKind = 'regular' | 'transfer' | 'adjustment';

export type MobileRow = {
  transaction: Transaction;
  kind: MobileRowKind;
  name: string;
  /** Bez sklepu: nazwa wyszarzona. */
  nameMuted: boolean;
  /** null — wiersz nie ma kategorii do pokazania (korekta salda). */
  categoryLabel: string | null;
  /** Wydatek bez kategorii: etykieta w kolorze ostrzeżenia. */
  categoryMissing: boolean;
  dots: string[];
  account: string | null;
  comment: string | null;
  time: string | null;
  unapproved: boolean;
};

export type MobileDay = {
  date: string;
  rows: MobileRow[];
  /** Suma wydatków i wpływów dnia bez przelewów i korekt; null, gdy nie ma czego sumować. */
  sumCents: number | null;
  currency: string;
};

export type MobileListContext = {
  accountMap: ReadonlyMap<string, Account>;
  storeMap: ReadonlyMap<string, Store>;
  categoryMap: Map<string, Category>;
  productMap: ReadonlyMap<string, Item>;
  /** Lista na stronie konta: konto w wierszu nic nie mówi. */
  hideAccount?: boolean;
};

export const categoriesLabel = (count: number) => {
  const mod10 = count % 10;
  const mod100 = count % 100;
  const word = mod10 >= 2 && mod10 <= 4 && (mod100 < 12 || mod100 > 14) ? 'kategorie' : 'kategorii';
  return `${count} ${word}`;
};

const categoryInfo = (transaction: Transaction, ctx: MobileListContext) => {
  const ids: string[] = [];
  if (transaction.items.length) {
    for (const item of transaction.items) {
      const { categoryId } = itemCategory(item, ctx.productMap, ctx.categoryMap, transaction.category_id);
      if (categoryId && !ids.includes(categoryId)) ids.push(categoryId);
    }
  } else if (transaction.category_id && ctx.categoryMap.has(transaction.category_id)) {
    ids.push(transaction.category_id);
  }
  const dots = ids.slice(0, MAX_DOTS).map((id) => resolveTagColor(ctx.categoryMap.get(id), ctx.categoryMap).value);
  if (ids.length > 1) return { label: categoriesLabel(ids.length), missing: false, dots };
  if (ids.length === 1) return { label: ctx.categoryMap.get(ids[0])?.name ?? '', missing: false, dots };
  return { label: 'Bez kategorii', missing: true, dots };
};

export const buildMobileRow = (transaction: Transaction, ctx: MobileListContext): MobileRow => {
  const accountName = ctx.accountMap.get(transaction.account_id)?.name ?? '—';
  const base = {
    transaction,
    account: ctx.hideAccount ? null : accountName,
    comment: userComment(transaction.comment),
    time: formatRecordedTime(transaction.recorded_at),
    unapproved: !transaction.cleared
  };

  if (transaction.transfer_account_id) {
    const other = ctx.accountMap.get(transaction.transfer_account_id)?.name ?? '—';
    const [from, to] = transaction.total_cents < 0 ? [accountName, other] : [other, accountName];
    return {
      ...base,
      kind: 'transfer',
      name: `${from} → ${to}`,
      nameMuted: false,
      categoryLabel: 'Przelew',
      categoryMissing: false,
      dots: [],
      account: null
    };
  }

  const store = transaction.store_id ? ctx.storeMap.get(transaction.store_id) : undefined;
  const storeName = (store?.group_name ?? '').trim() || (store?.name ?? '').trim();
  if (isBalanceAdjustment(storeName) || (!storeName && isBalanceAdjustment(transaction.comment))) {
    return {
      ...base,
      kind: 'adjustment',
      name: BALANCE_ADJUSTMENT_LABEL,
      nameMuted: true,
      categoryLabel: null,
      categoryMissing: false,
      dots: [],
      account: accountName,
      comment: null,
      unapproved: false
    };
  }

  const category = categoryInfo(transaction, ctx);
  return {
    ...base,
    kind: 'regular',
    name: storeName || 'Bez sklepu',
    nameMuted: !storeName,
    categoryLabel: category.label,
    categoryMissing: category.missing,
    dots: category.dots
  };
};

/** Transakcje są już posortowane od najnowszej; kolejne o tej samej dacie tworzą dzień. */
export const buildMobileDays = (transactions: readonly Transaction[], ctx: MobileListContext): MobileDay[] => {
  const days: MobileDay[] = [];
  for (const transaction of transactions) {
    const date = transaction.date ?? '';
    let day = days[days.length - 1];
    if (!day || day.date !== date) {
      day = { date, rows: [], sumCents: null, currency: transaction.currency };
      days.push(day);
    }
    day.rows.push(buildMobileRow(transaction, ctx));
  }
  for (const day of days) {
    const counted = day.rows.filter((row) => row.kind === 'regular');
    // Suma jednej transakcji powtarza jej kwotę, a różne waluty się nie sumują
    if (counted.length >= 2 && counted.every((row) => row.transaction.currency === day.currency)) {
      day.sumCents = counted.reduce((sum, row) => sum + row.transaction.total_cents, 0);
    }
  }
  return days;
};
