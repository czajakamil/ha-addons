// Filtr kategorii na liście transakcji: drzewo w kolejności z ekranu Kategorie,
// wyszukiwanie po nazwach i zaznaczanie całych gałęzi. Bez stanu i bez JSX.
import type { Category } from '../../api';
import type { CheckboxFilterOption } from '../../components/CheckboxFilterMenu';
import { buildCategoryTree, subtreeIds, walkTree, type CategoryNode } from '../categories/model';

// Ten sam znacznik rozumie backend w `category_ids`
export const UNCATEGORIZED_ID = '__uncategorized__';

export const buildFilterTree = (categories: readonly Category[]) => buildCategoryTree(categories, null);

/** Wszystkie id do zaznaczenia: kategorie z drzewa i „Bez kategorii”. */
export const allFilterIds = (tree: readonly CategoryNode[]) => {
  const ids = [UNCATEGORIZED_ID];
  walkTree(tree, (node) => ids.push(node.category.id));
  return ids;
};

/**
 * Pozycje menu. Pasujący rodzic pokazuje całą gałąź; pasująca podkategoria
 * pokazuje się z przodkami, żeby było wiadomo, skąd pochodzi.
 */
export const categoryFilterOptions = (
  tree: readonly CategoryNode[],
  search: string,
  pendingIds: readonly string[],
  uncategorizedLabel: string
): CheckboxFilterOption[] => {
  const query = search.trim().toLocaleLowerCase('pl');
  const matches = (name: string) => !query || name.toLocaleLowerCase('pl').includes(query);
  const pending = new Set(pendingIds);
  const options: CheckboxFilterOption[] = [];

  if (matches(uncategorizedLabel)) {
    options.push({ id: UNCATEGORIZED_ID, label: uncategorizedLabel });
  }

  const visit = (node: CategoryNode, ancestorMatched: boolean): CheckboxFilterOption[] => {
    const selfMatched = ancestorMatched || matches(node.category.name);
    const children = node.children.flatMap((child) => visit(child, selfMatched));
    if (!selfMatched && children.length === 0) return [];
    const branch = subtreeIds(node);
    const selected = branch.filter((id) => pending.has(id)).length;
    return [
      {
        id: node.category.id,
        label: node.category.name,
        depth: node.depth,
        indeterminate: node.children.length > 0 && selected > 0 && selected < branch.length
      },
      ...children
    ];
  };
  tree.forEach((node) => options.push(...visit(node, false)));
  return options;
};

/**
 * Kliknięcie kategorii zaznacza lub odznacza całą jej gałąź. Odznaczenie
 * podkategorii zdejmuje też przodków — rodzic zaznaczony znaczy „cała grupa”.
 */
export const toggleCategory = (tree: readonly CategoryNode[], pendingIds: readonly string[], id: string) => {
  const pending = new Set(pendingIds);
  if (id === UNCATEGORIZED_ID) {
    if (pending.has(id)) pending.delete(id);
    else pending.add(id);
    return [...pending];
  }

  const path: CategoryNode[] = [];
  const find = (nodes: readonly CategoryNode[]): boolean =>
    nodes.some((node) => {
      path.push(node);
      if (node.category.id === id || find(node.children)) return true;
      path.pop();
      return false;
    });
  if (!find(tree)) return [...pending];

  const target = path[path.length - 1];
  const branch = subtreeIds(target);
  if (pending.has(id)) {
    branch.forEach((item) => pending.delete(item));
    path.forEach((node) => pending.delete(node.category.id));
  } else {
    branch.forEach((item) => pending.add(item));
    // Zaznaczona ostatnia podkategoria domyka rodzica
    for (let index = path.length - 2; index >= 0; index -= 1) {
      const ancestor = path[index];
      if (!subtreeIds(ancestor).every((item) => item === ancestor.category.id || pending.has(item))) break;
      pending.add(ancestor.category.id);
    }
  }
  return [...pending];
};

/**
 * Nazwy wyboru do przycisku i chipa: cała zaznaczona gałąź to jedna nazwa
 * (rodzica), pozostałe kategorie — każda osobno.
 */
export const selectedCategoryNames = (
  tree: readonly CategoryNode[],
  selectedIds: readonly string[],
  uncategorizedLabel: string
) => {
  const selected = new Set(selectedIds);
  const names: string[] = selected.has(UNCATEGORIZED_ID) ? [uncategorizedLabel] : [];
  const visit = (node: CategoryNode) => {
    if (subtreeIds(node).every((id) => selected.has(id))) {
      names.push(node.category.name);
      return;
    }
    if (selected.has(node.category.id)) names.push(node.category.name);
    node.children.forEach(visit);
  };
  tree.forEach(visit);
  return names;
};

/** Link ?category=… z ekranu kategorii: rodzic obejmuje swoje podkategorie. */
export const expandCategoryParam = (categories: readonly Category[], id: string) => {
  if (!id) return [];
  if (id === UNCATEGORIZED_ID) return [id];
  let found: CategoryNode | null = null;
  walkTree(buildFilterTree(categories), (node) => {
    if (node.category.id === id) found = node;
  });
  return found ? subtreeIds(found) : [id];
};
