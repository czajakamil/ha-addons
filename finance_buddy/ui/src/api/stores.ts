import { request } from './client';

export interface Store {
  id: string;
  name: string;
  group_name?: string | null;
  include_in_payee_list: boolean;
  automatically_categorize_payee: boolean;
  automatic_category_id?: string | null;
  renaming_rules: string[];
  nip?: string | null;
  address?: string | null;
  comment?: string | null;
  is_deleted: boolean;
}

export interface StoreCreate {
  name: string;
  group_name?: string | null;
  include_in_payee_list?: boolean;
  automatically_categorize_payee?: boolean;
  automatic_category_id?: string | null;
  renaming_rules?: string[];
  nip?: string | null;
  address?: string | null;
  comment?: string | null;
  is_deleted?: boolean;
}

export interface StoreUpdate {
  name?: string;
  group_name?: string | null;
  include_in_payee_list?: boolean;
  automatically_categorize_payee?: boolean;
  automatic_category_id?: string | null;
  renaming_rules?: string[];
  nip?: string | null;
  address?: string | null;
  comment?: string | null;
  is_deleted?: boolean;
}

export interface StoreQueryParams {
  search?: string;
  offset?: number;
  limit?: number;
}

export const getStores = (params: StoreQueryParams = {}) => {
  const search = new URLSearchParams();
  if (params.search) search.set('search', params.search);
  if (typeof params.offset === 'number') search.set('offset', params.offset.toString());
  if (typeof params.limit === 'number') search.set('limit', params.limit.toString());
  const query = search.toString();
  return request<Store[]>(`/stores${query ? `?${query}` : ''}`);
};

export const getAllStores = async (pageSize = 200) => {
  const all: Store[] = [];
  let offset = 0;
  while (true) {
    const chunk = await getStores({ offset, limit: pageSize });
    all.push(...chunk);
    if (chunk.length < pageSize) {
      break;
    }
    offset += pageSize;
  }
  return all;
};

export const createStore = (payload: StoreCreate) =>
  request<Store>('/stores', { method: 'POST', body: JSON.stringify(payload) });

export const updateStore = (id: string, payload: StoreUpdate) =>
  request<Store>(`/stores/${id}`, { method: 'PATCH', body: JSON.stringify(payload) });

export const deleteStore = (id: string) => request<void>(`/stores/${id}`, { method: 'DELETE' });

/** Statystyki z serwera. Sklep bez transakcji nie ma wiersza. */
export interface StoreStatsRow {
  store_id: string;
  count: number;
  spent_cents_12m: number;
  last_date?: string | null;
}

export interface StoreMonthlySpend {
  month: string;
  cents: number;
}

const storeIdsQuery = (storeIds: readonly string[]) =>
  storeIds.map((id) => `store_ids=${encodeURIComponent(id)}`).join('&');

export const getStoreStats = (storeIds: readonly string[] = []) =>
  request<StoreStatsRow[]>(`/stores/stats${storeIds.length ? `?${storeIdsQuery(storeIds)}` : ''}`);

export const getStoreMonthlySpend = (storeIds: readonly string[]) =>
  request<StoreMonthlySpend[]>(`/stores/stats/monthly?${storeIdsQuery(storeIds)}`);

/** Nazwa z banku, po której importer rozpoznaje sklep. */
export interface StoreAlias {
  id: string;
  store_id: string;
  pattern: string;
  match_type: 'exact' | 'prefix';
  is_ambiguous: boolean;
}

export interface StoreAliasPreview {
  match_type: 'exact' | 'prefix';
  matches: number;
  matches_other_stores: number;
  conflict?: string | null;
}

export const getStoreAliases = (storeIds: readonly string[]) =>
  request<StoreAlias[]>(`/stores/aliases?${storeIdsQuery(storeIds)}`);

export const getAmbiguousStoreAliases = () => request<StoreAlias[]>('/stores/aliases?ambiguous=true');

export const previewStoreAlias = (pattern: string, storeIds: readonly string[]) =>
  request<StoreAliasPreview>('/stores/aliases/preview', {
    method: 'POST',
    body: JSON.stringify({ pattern, store_ids: storeIds })
  });

export const addStoreAlias = (storeId: string, pattern: string) =>
  request<StoreAlias>(`/stores/${storeId}/aliases`, { method: 'POST', body: JSON.stringify({ pattern }) });

export const deleteStoreAlias = (aliasId: string) =>
  request<void>(`/stores/aliases/${aliasId}`, { method: 'DELETE' });

export interface StoreMergeConflict {
  kind: string;
  message: string;
  blocking: boolean;
}

export interface StoreMergePreview {
  transactions: number;
  receipts: number;
  aliases: number;
  conflicts: StoreMergeConflict[];
  can_merge: boolean;
}

export interface StoreMergeResult {
  merge_id: string;
  target_id: string;
  transactions: number;
  expires_at: string;
}

export interface StoreMerge {
  id: string;
  target_store_id: string;
  source_names: string[];
  transactions: number;
  created_at: string;
  expires_at: string;
}

type StoreMergeRequest = { source_ids: string[]; target_id: string };

export const previewStoreMerge = (payload: StoreMergeRequest) =>
  request<StoreMergePreview>('/stores/merge', {
    method: 'POST',
    body: JSON.stringify({ ...payload, dry_run: true })
  });

export const mergeStores = (payload: StoreMergeRequest & { name?: string; category_id?: string | null }) =>
  request<StoreMergeResult>('/stores/merge', { method: 'POST', body: JSON.stringify(payload) });

export const getStoreMerges = (storeIds: readonly string[]) =>
  request<StoreMerge[]>(`/stores/merges?${storeIdsQuery(storeIds)}`);

export const undoStoreMerge = (mergeId: string) =>
  request<{ restored_transactions: number; kept_transactions: number }>(`/stores/merges/${mergeId}/undo`, {
    method: 'POST'
  });

export interface SelfTransferCandidate {
  id: string;
  date: string;
  amount_cents: number;
  currency: string;
  account_id: string;
  account_name: string;
  description?: string | null;
}

export type SelfTransferStatus = 'paired' | 'ambiguous' | 'other_currency' | 'no_partner' | 'covered' | 'has_items';

export interface SelfTransferRow extends SelfTransferCandidate {
  status: SelfTransferStatus;
  suggested?: string | null;
  candidates: SelfTransferCandidate[];
}

export interface SelfTransferPreview {
  rows: SelfTransferRow[];
  already_transfers: number;
}

export interface SelfTransferDecision {
  transaction_id: string;
  partner_id?: string | null;
  account_id?: string | null;
}

export interface SelfTransferConversion {
  id: string;
  converted: number;
  created_at: string;
  expires_at: string;
}

export const previewSelfTransfers = (storeIds: string[]) =>
  request<SelfTransferPreview>('/stores/self-transfers/preview', {
    method: 'POST',
    body: JSON.stringify({ store_ids: storeIds })
  });

export const convertSelfTransfers = (decisions: SelfTransferDecision[]) =>
  request<{ conversion_id: string; converted: number; expires_at: string }>('/stores/self-transfers/convert', {
    method: 'POST',
    body: JSON.stringify({ decisions })
  });

export const getSelfTransferConversions = (storeIds: readonly string[]) =>
  request<SelfTransferConversion[]>(`/stores/self-transfers/conversions?${storeIdsQuery(storeIds)}`);

export const undoSelfTransferConversion = (id: string) =>
  request<{ restored: number; kept: number }>(`/stores/self-transfers/conversions/${id}/undo`, { method: 'POST' });

export interface StoreCategorySuggestion {
  store_id: string;
  category_id: string;
  count: number;
  total: number;
}

/** Najczęstsza kategoria transakcji sklepu, gdy wyraźnie przeważa (serwer: co najmniej 60%). */
export const getStoreCategorySuggestions = () => request<StoreCategorySuggestion[]>('/stores/category-suggestions');
