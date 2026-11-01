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
