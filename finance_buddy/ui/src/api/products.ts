import { request } from './client';

export interface Item {
  id: string;
  name: string;
  group_name?: string | null;
  name_simplified?: string | null;
  unit_of_measurement?: string | null;
  category_id?: string | null;
  tax_rate?: number | null;
  manufacturer?: string | null;
  comment?: string | null;
}

export interface ItemCreate {
  name: string;
  group_name?: string | null;
  name_simplified?: string | null;
  unit_of_measurement?: string | null;
  category_id?: string | null;
  tax_rate?: number | null;
  manufacturer?: string | null;
  comment?: string | null;
}

export interface ItemUpdate {
  name?: string | null;
  group_name?: string | null;
  name_simplified?: string | null;
  unit_of_measurement?: string | null;
  category_id?: string | null;
  tax_rate?: number | null;
  manufacturer?: string | null;
  comment?: string | null;
}

export interface ItemQueryParams {
  search?: string;
  offset?: number;
  limit?: number;
}

export const getItems = (params: ItemQueryParams = {}) => {
  const search = new URLSearchParams();
  if (params.search) search.set('search', params.search);
  if (typeof params.offset === 'number') search.set('offset', params.offset.toString());
  if (typeof params.limit === 'number') search.set('limit', params.limit.toString());
  const query = search.toString();
  return request<Item[]>(`/items${query ? `?${query}` : ''}`);
};

export const getAllItems = async (pageSize = 200) => {
  const all: Item[] = [];
  let offset = 0;
  while (true) {
    const chunk = await getItems({ offset, limit: pageSize });
    all.push(...chunk);
    if (chunk.length < pageSize) {
      break;
    }
    offset += pageSize;
  }
  return all;
};

export const createItem = (payload: ItemCreate) =>
  request<Item>('/items', { method: 'POST', body: JSON.stringify(payload) });

export const updateItem = (id: string, payload: ItemUpdate) =>
  request<Item>(`/items/${id}`, { method: 'PATCH', body: JSON.stringify(payload) });

export const deleteItem = (id: string) =>
  request<void>(`/items/${id}`, { method: 'DELETE' });

export interface ItemUsage {
  item_id: string;
  transaction_items: number;
  aliases: number;
}

export const getItemUsage = () => request<ItemUsage[]>('/items/usage');

/** Zakupy przedmiotu. Przedmiot nigdy niekupiony nie ma wiersza. */
export interface ItemStatsRow {
  item_id: string;
  count: number;
  spent_cents_12m: number;
  last_date?: string | null;
}

export interface ItemMonthlySpend {
  month: string;
  cents: number;
}

export const getItemStats = () => request<ItemStatsRow[]>('/items/stats');

export const getItemMonthlySpend = (itemId: string) =>
  request<ItemMonthlySpend[]>(`/items/stats/monthly?item_ids=${encodeURIComponent(itemId)}`);

export interface ItemMergeResult {
  target_id: string;
  merged_count: number;
  items_moved: number;
  aliases_moved: number;
  aliases_added: number;
  dry_run: boolean;
}

/** Scala `sourceIds` w `targetId`; z `dryRun` tylko liczy skutki. */
export const mergeItems = (sourceIds: string[], targetId: string, dryRun = false) =>
  request<ItemMergeResult>('/items/merge', {
    method: 'POST',
    body: JSON.stringify({ source_ids: sourceIds, target_id: targetId, dry_run: dryRun })
  });

/** Nazwa z paragonu rozpoznawana jako ten produkt; bez sklepu = we wszystkich sklepach. */
export interface ItemAlias {
  id: string;
  alias: string;
  store_id?: string | null;
  store_name?: string | null;
}

export const getItemAliases = (itemId: string) => request<ItemAlias[]>(`/items/${itemId}/aliases`);

export const deleteItemAlias = (itemId: string, aliasId: string) =>
  request<void>(`/items/${itemId}/aliases/${aliasId}`, { method: 'DELETE' });

// --- Dopasowanie pozycji paragonu do produktów (Etap 3 pipeline'u OCR) ---

export type ProductMatchType = 'alias_store' | 'alias_global' | 'name' | 'fuzzy' | 'none';

export interface ProductCandidate {
  product_id: string;
  name: string;
  score: number;
  category_id?: string | null;
  category_path?: string | null;
}

export interface ItemMatch {
  raw_name: string;
  match_type: ProductMatchType;
  matched_product_id?: string | null;
  matched_product_name?: string | null;
  candidates: ProductCandidate[];
  suggested_category_id?: string | null;
  suggested_category_path?: string | null;
  prefill_name: string;
}

export interface ProductMatchRequest {
  store_id?: string | null;
  raw_names: string[];
}

export const matchProducts = (payload: ProductMatchRequest) =>
  request<{ matches: ItemMatch[] }>('/product-matches', {
    method: 'POST',
    body: JSON.stringify(payload)
  });

export interface ProductAliasCreate {
  product_id: string;
  raw_name: string;
  store_id?: string | null;
}

export interface ProductAlias {
  id: string;
  product_id: string;
  store_id?: string | null;
  alias: string;
}

export const saveProductAlias = (payload: ProductAliasCreate) =>
  request<ProductAlias>('/product-aliases', {
    method: 'POST',
    body: JSON.stringify(payload)
  });

export interface TransactionItemSummary {
  id: string;
  transaction_id: string;
  product_id: string;
  product_name?: string | null;
  transaction_date?: string | null;
  store_name?: string | null;
  account_name?: string | null;
  quantity: number;
  unit?: string | null;
  unit_price_cents: number;
  total_cents: number;
  comment?: string | null;
  currency?: string | null;
}

export interface TransactionItemQueryParams {
  search?: string;
  offset?: number;
  limit?: number;
}

export const getTransactionItems = (params: TransactionItemQueryParams = {}) => {
  const search = new URLSearchParams();
  if (params.search) search.set('search', params.search);
  if (typeof params.offset === 'number') search.set('offset', params.offset.toString());
  if (typeof params.limit === 'number') search.set('limit', params.limit.toString());
  const query = search.toString();
  return request<TransactionItemSummary[]>(`/transaction-items${query ? `?${query}` : ''}`);
};
