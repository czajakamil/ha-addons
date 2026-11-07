import { request } from './client';

export interface TransactionItem {
  id: string;
  product_id: string;
  unit_price_cents: number;
  quantity: number;
  unit?: string | null;
  comment?: string | null;
  /** Nazwa tak, jak wydrukował ją paragon; pozycje dodane ręcznie jej nie mają. */
  raw_name?: string | null;
}

export interface TransactionItemCreate {
  product_id: string;
  unit_price_cents: number;
  quantity: number;
  unit?: string | null;
  comment?: string | null;
  raw_name?: string | null;
}

export interface Transaction {
  id: string;
  account_id: string;
  store_id?: string | null;
  category_id?: string | null;
  transfer_account_id?: string | null;
  transfer_transaction_id?: string | null;
  receipt_number?: string | null;
  recorded_at?: string | null;
  date?: string | null;
  currency: string;
  payment_method?: string | null;
  comment?: string | null;
  cleared: boolean;
  reconciled: boolean;
  total_cents: number;
  /** Surowy opis z importu CSV; transakcje z YNAB i ręczne go nie mają. */
  bank_description?: string | null;
  items: TransactionItem[];
}

export interface TransactionAttachment {
  id: string;
  transaction_id: string;
  original_name: string;
  content_type?: string | null;
  size_bytes: number;
  created_at: string;
  download_url: string;
}

export interface TransactionItemDetail extends TransactionItem {
  product_name?: string | null;
}

export interface TransactionDetail extends Transaction {
  account_name?: string | null;
  store_name?: string | null;
  category_name?: string | null;
  items: TransactionItemDetail[];
  attachments: TransactionAttachment[];
}

export interface TransactionCreate {
  account_id: string;
  store_id?: string | null;
  category_id?: string | null;
  transfer_account_id?: string | null;
  transfer_transaction_id?: string | null;
  receipt_number?: string | null;
  recorded_at?: string | null;
  date?: string | null;
  currency?: string;
  payment_method?: string | null;
  comment?: string | null;
  cleared?: boolean;
  reconciled?: boolean;
  items: TransactionItemCreate[];
  total_cents?: number;
}

export type TransactionUpdate = Partial<TransactionCreate>;

export interface TransactionQueryParams {
  start_date?: string;
  end_date?: string;
  category_id?: string;
  account_id?: string;
  account_ids?: readonly string[];
  store_id?: string;
  store_ids?: readonly string[];
  category_ids?: readonly string[];
  search?: string;
  exclude_internal?: boolean;
  offset?: number;
  limit?: number;
}

const toSearchParams = (params: TransactionQueryParams) => {
  const searchParams = new URLSearchParams();
  if (params.start_date) searchParams.set('start_date', params.start_date);
  if (params.end_date) searchParams.set('end_date', params.end_date);
  if (params.category_id) searchParams.set('category_id', params.category_id);
  if (params.account_id) searchParams.set('account_id', params.account_id);
  params.account_ids?.forEach((id) => searchParams.append('account_ids', id));
  if (params.store_id) searchParams.set('store_id', params.store_id);
  params.store_ids?.forEach((id) => searchParams.append('store_ids', id));
  params.category_ids?.forEach((id) => searchParams.append('category_ids', id));
  if (params.search) searchParams.set('search', params.search);
  if (typeof params.exclude_internal === 'boolean') {
    searchParams.set('exclude_internal', params.exclude_internal ? 'true' : 'false');
  }
  if (typeof params.offset === 'number') searchParams.set('offset', params.offset.toString());
  if (typeof params.limit === 'number') searchParams.set('limit', params.limit.toString());
  const query = searchParams.toString();
  return query ? `?${query}` : '';
};

export const getTransactions = (params: TransactionQueryParams = {}) =>
  request<Transaction[]>(`/transactions${toSearchParams(params)}`);

/** Strona wyników widoku Transakcje; `total` liczy wszystkie pasujące, nie tylko stronę. */
export interface TransactionPage {
  items: Transaction[];
  total: number;
  offset: number;
  limit: number;
}

export type TransactionPageParams = Pick<
  TransactionQueryParams,
  'start_date' | 'end_date' | 'account_id' | 'account_ids' | 'store_ids' | 'category_ids' | 'search' | 'offset' | 'limit'
>;

export const getTransactionsPage = (params: TransactionPageParams = {}) =>
  request<TransactionPage>(`/transactions/page${toSearchParams(params)}`);

export const getTransaction = (id: string) => request<TransactionDetail>(`/transactions/${id}`);

export const createTransaction = (payload: TransactionCreate) =>
  request<Transaction>('/transactions', { method: 'POST', body: JSON.stringify(payload) });

export const updateTransaction = (id: string, payload: TransactionUpdate) =>
  request<Transaction>(`/transactions/${id}`, { method: 'PATCH', body: JSON.stringify(payload) });

export const deleteTransaction = (id: string) =>
  request<void>(`/transactions/${id}`, { method: 'DELETE' });

export const uploadTransactionAttachment = (transactionId: string, file: File) => {
  const formData = new FormData();
  formData.append('file', file);
  return request<TransactionAttachment>(`/transactions/${transactionId}/attachments`, {
    method: 'POST',
    body: formData
  });
};

export const deleteTransactionAttachment = (transactionId: string, attachmentId: string) =>
  request<void>(`/transactions/${transactionId}/attachments/${attachmentId}`, { method: 'DELETE' });

// --- Kolejka „do otagowania" ---------------------------------------------

/** Skąd backend wziął podpowiedź tagu (services/tagging.py). */
export type UntaggedSuggestionSource = 'store_rule' | 'store' | 'store_group' | 'similar';

export interface UntaggedSuggestion {
  category_id: string;
  source: UntaggedSuggestionSource;
  /** Ile otagowanych transakcji stoi za podpowiedzią (0 dla reguły sklepu). */
  matches: number;
  /** Dla „similar": opis najbliższej otagowanej transakcji. */
  similar_to?: string | null;
}

export interface UntaggedTransaction {
  id: string;
  date: string;
  total_cents: number;
  currency: string;
  comment?: string | null;
  account_id: string;
  account_name: string;
  store_id?: string | null;
  store_name?: string | null;
  /** Ile transakcji tego sklepu czeka w kolejce — podgląd „otaguj cały sklep". */
  store_untagged_count: number;
  suggestion?: UntaggedSuggestion | null;
}

export interface UntaggedTransactionsPage {
  total: number;
  items: UntaggedTransaction[];
}

export const getUntaggedTransactions = (params: { offset?: number; limit?: number } = {}) => {
  const searchParams = new URLSearchParams();
  if (typeof params.offset === 'number') searchParams.set('offset', params.offset.toString());
  if (typeof params.limit === 'number') searchParams.set('limit', params.limit.toString());
  const query = searchParams.toString();
  return request<UntaggedTransactionsPage>(`/transactions/untagged${query ? `?${query}` : ''}`);
};

export const getUntaggedCount = (storeId?: string) =>
  request<{ count: number }>(
    `/transactions/untagged/count${storeId ? `?store_id=${encodeURIComponent(storeId)}` : ''}`
  );

/** Taguje wszystkie transakcje sklepu z kolejki; zwraca, ile się zmieniło. */
export const tagStoreUntagged = (storeId: string, categoryId: string) =>
  request<{ updated: number }>('/transactions/untagged/tag-store', {
    method: 'POST',
    body: JSON.stringify({ store_id: storeId, category_id: categoryId })
  });
