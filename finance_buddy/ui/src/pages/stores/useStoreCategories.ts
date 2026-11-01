// Kategorie wydatków dla Sklepów: wczytanie, mapa do kropek i tworzenie brakujących ścieżek.
import { useCallback, useMemo } from 'react';
import { ensureCategoryPath, getAllCategories } from '../../api';
import { buildCategoryMap } from '../../components/tags/tagColors';
import { useApi } from '../../hooks/useApi';

export const useStoreCategories = () => {
  const { data: categories = [], setData } = useApi(() => getAllCategories(200, { for_accounts: false }));
  const categoryMap = useMemo(() => buildCategoryMap(categories), [categories]);

  const ensureCategory = useCallback(
    async (path: string) => {
      const id = await ensureCategoryPath(path, categories, false);
      if (!categories.some((category) => category.id === id)) {
        setData(await getAllCategories(200, { for_accounts: false }));
      }
      return id;
    },
    [categories, setData]
  );

  return { categories, categoryMap, ensureCategory };
};
