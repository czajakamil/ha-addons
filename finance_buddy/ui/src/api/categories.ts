import { request } from './client';

export interface Category {
  id: string;
  name: string;
  parent_id?: string | null;
  is_for_accounts?: boolean;
  /** Klucz koloru z palety (np. "teal"); null/undefined = dziedziczony po przodku. */
  color?: string | null;
  /** Znormalizowana ścieżka tagu, np. "dom/remont/lazienka" (wyliczana przez backend). */
  path: string;
  /** Pozycja wśród rodzeństwa; lista z API jest nią już posortowana. */
  sort_order?: number;
}

export interface CategoryCreate {
  /** Nazwa nowego tagu — użyj razem z parent_id, ALBO podaj samą path. */
  name?: string;
  parent_id?: string | null;
  /** Ścieżka "dom/remont/lazienka" — tworzy brakujących przodków (jak w Obsidianie). */
  path?: string;
  is_for_accounts?: boolean;
  color?: string | null;
}

export interface CategoryUpdate {
  name?: string;
  parent_id?: string | null;
  is_for_accounts?: boolean;
  /** null = wyczyść własny kolor (dziedzicz po rodzicu). Pomiń pole, by nie ruszać koloru. */
  color?: string | null;
}

export interface CategoryQueryParams {
  parent_id?: string;
  for_accounts?: boolean;
  /** Tag + całe pod-drzewo (np. "dom" zwróci "dom", "dom/remont", ...). */
  path_prefix?: string;
  offset?: number;
  limit?: number;
}

export const getCategories = (params: CategoryQueryParams = {}) => {
  const search = new URLSearchParams();
  if (params.parent_id) search.set('parent_id', params.parent_id);
   if (typeof params.for_accounts === 'boolean') search.set('for_accounts', params.for_accounts ? 'true' : 'false');
  if (params.path_prefix) search.set('path_prefix', params.path_prefix);
  if (typeof params.offset === 'number') search.set('offset', params.offset.toString());
  if (typeof params.limit === 'number') search.set('limit', params.limit.toString());
  const query = search.toString();
  return request<Category[]>(`/categories${query ? `?${query}` : ''}`);
};

export const getAllCategories = async (pageSize = 200, params: CategoryQueryParams = {}) => {
  const all: Category[] = [];
  let offset = 0;
  while (true) {
    const chunk = await getCategories({ ...params, offset, limit: pageSize });
    all.push(...chunk);
    if (chunk.length < pageSize) {
      break;
    }
    offset += pageSize;
  }
  return all;
};

export const createCategory = (payload: CategoryCreate) =>
  request<Category>('/categories', { method: 'POST', body: JSON.stringify(payload) });

/** Zwraca id istniejącego tagu o danej ścieżce albo tworzy go (wraz z przodkami). */
export const ensureCategoryPath = async (
  path: string,
  categories: Category[],
  isForAccounts = false
): Promise<string> => {
  const trimmed = path.trim();
  if (!trimmed) {
    throw new Error('Ścieżka kategorii jest wymagana.');
  }
  const existing = categories.find(
    (category) =>
      category.path.toLowerCase() === trimmed.toLowerCase() &&
      (category.is_for_accounts ?? false) === isForAccounts
  );
  if (existing) {
    return existing.id;
  }
  const created = await createCategory({ path: trimmed, is_for_accounts: isForAccounts });
  return created.id;
};

export const updateCategory = (id: string, payload: CategoryUpdate) =>
  request<Category>(`/categories/${id}`, { method: 'PATCH', body: JSON.stringify(payload) });

export interface CategoryDeletePreview {
  category_count: number;
  product_count: number;
  transaction_count: number;
}

/** Skala skutków usunięcia pod-drzewa tagów — do potwierdzenia przed deleteCategory. */
export const getCategoryDeletePreview = (id: string) =>
  request<CategoryDeletePreview>(`/categories/${id}/delete-preview`);

/** Usuwa pustą kategorię z pod-drzewem (409, gdy ma transakcje); przedmioty tracą przypisanie. */
export const deleteCategory = (id: string) =>
  request<void>(`/categories/${id}`, { method: 'DELETE' });

/** Własne transakcje kategorii (bez podkategorii); sumy gałęzi składa UI. */
export interface CategoryStatsRow {
  category_id: string;
  count: number;
  spent_cents_12m: number;
  last_date?: string | null;
  /** 12 miesięcy od najstarszego, bieżący na końcu. */
  monthly_cents: number[];
}

export const getCategoryStats = () => request<CategoryStatsRow[]>('/categories/stats');

export interface CategoryTopStore {
  name: string;
  store_id: string;
  count: number;
  spent_cents_12m: number;
}

/** Sklepy z największymi wydatkami w kategorii razem z podkategoriami (12 mies.). */
export const getCategoryTopStores = (id: string) =>
  request<CategoryTopStore[]>(`/categories/${id}/top-stores`);

/** Ustawia kolejność dzieci rodzica; kategorie spod innego rodzica przenosi pod niego. */
export const reorderCategories = (parentId: string | null, categoryIds: readonly string[]) =>
  request<Category[]>('/categories/order', {
    method: 'PUT',
    body: JSON.stringify({ parent_id: parentId, category_ids: categoryIds })
  });
