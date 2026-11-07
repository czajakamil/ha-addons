// Tagi kont: pobranie pełnego drzewa, zawężenie do kategorii faktycznie
// używanych przez konta (wraz z przodkami) i tworzenie brakujących ścieżek
// ("Inwestycje / Obligacje") przy edycji.
import { useCallback, useMemo } from 'react';
import { Account, Category, ensureCategoryPath, getAllCategories } from '../../api';
import { useApi } from '../../hooks/useApi';

// Zostawia tylko tagi przypisane do kont oraz ich przodków (żeby dało się
// zbudować pełne ścieżki), zachowując kolejność źródłową.
const filterAccountCategoriesInUse = (source: Category[], accountList: Account[]) => {
  if (!source.length) return [];
  const byId = new Map<string, Category>(source.map((cat) => [cat.id, cat]));
  const used = new Set(
    accountList
      .map((account) => account.category_id)
      .filter((id): id is string => Boolean(id))
  );
  if (!used.size) return [];

  const keep = new Set<string>();

  const addWithAncestors = (categoryId: string) => {
    let current: string | null = categoryId;
    while (current) {
      if (keep.has(current)) break;
      keep.add(current);
      const parentId: string | null = byId.get(current)?.parent_id ?? null;
      current = parentId;
    }
  };

  used.forEach(addWithAncestors);

  return source.filter((cat) => keep.has(cat.id));
};

export const useAccountCategories = (accounts: Account[]) => {
  const {
    data: allAccountCategories = [],
    error: categoryLoadError,
    setData: setAllAccountCategories
  } = useApi(
    async () => {
      const data = await getAllCategories(200, { for_accounts: true });
      return data.slice().sort((a, b) => a.name.localeCompare(b.name, 'pl'));
    },
    { errorMessage: 'Nie udało się wczytać kategorii kont.' }
  );

  // Wyliczenie zamiast trzymania drugiej listy w stanie i synchronizowania jej efektem.
  const categories = useMemo(
    () => filterAccountCategoriesInUse(allAccountCategories, accounts),
    [allAccountCategories, accounts]
  );

  const categoryMap = useMemo(() => new Map(categories.map((category) => [category.id, category])), [categories]);

  const ensureAccountCategory = useCallback(
    async (path: string): Promise<string> => {
      const id = await ensureCategoryPath(path, allAccountCategories, true);
      if (!allAccountCategories.some((cat) => cat.id === id)) {
        const data = await getAllCategories(200, { for_accounts: true });
        setAllAccountCategories(data.slice().sort((a, b) => a.name.localeCompare(b.name, 'pl')));
      }
      return id;
    },
    [allAccountCategories, setAllAccountCategories]
  );

  const getCategoryLabel = useCallback(
    (categoryId?: string | null) => {
      if (!categoryId) return '';
      return categoryMap.get(categoryId)?.path ?? '';
    },
    [categoryMap]
  );

  return {
    categoryLoadError,
    categories,
    ensureAccountCategory,
    getCategoryLabel
  };
};
