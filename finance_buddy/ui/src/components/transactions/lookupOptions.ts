import type { Account, Category, Store } from '../../api';

/**
 * Pozycja listy wyboru. Zapis idzie po `id`, a `label` to pełna nazwa do filtrowania
 * i pola tekstowego. `rowLabel` zastępuje ją w wierszu, gdy sekcja mówi resztę.
 */
export type LookupOption = {
  id: string;
  label: string;
  rowLabel?: string;
  // 'create' to pozycja „Utwórz…” z wpisanego tekstu (label = ten tekst), nie istniejąca wartość.
  kind: 'entry' | 'transfer' | 'create';
};

export type TypedLookupResult =
  | { kind: 'empty' }
  | { kind: 'match'; option: LookupOption }
  | { kind: 'none' };

const normalize = (value: string) => value.trim().toLowerCase();

const findExactIndexes = (options: LookupOption[], text: string) => {
  const needle = normalize(text);
  return options.flatMap((option, index) => (normalize(option.label) === needle ? [index] : []));
};

export const filterLookupOptions = (options: LookupOption[], text: string) => {
  const needle = normalize(text);
  if (!needle) return options;
  return options.filter((option) => normalize(option.label).includes(needle));
};

/**
 * Domyślnie podświetlona pozycja: bez tekstu żadna (samo Enter nie może wybrać
 * przypadkowej pierwszej opcji), a z tekstem dokładne dopasowanie albo pierwsza.
 */
export const defaultLookupHighlight = (filtered: LookupOption[], text: string) => {
  if (!normalize(text) || !filtered.length) return -1;
  return findExactIndexes(filtered, text)[0] ?? 0;
};

/** Pozycja odpowiadająca wartości pola sprzed edycji; -1, gdy jej nie ma na liście. */
export const findSelectedLookupIndex = (options: LookupOption[], value: string) =>
  normalize(value) ? findExactIndexes(options, value)[0] ?? -1 : -1;

/** Dzieli tekst wiersza wokół wpisanego fragmentu; null, gdy fragmentu w nim nie ma. */
export const splitLookupMatch = (text: string, query: string) => {
  const needle = normalize(query);
  const start = needle ? text.toLowerCase().indexOf(needle) : -1;
  if (start < 0) return null;
  const end = start + needle.length;
  return { before: text.slice(0, start), match: text.slice(start, end), after: text.slice(end) };
};

/** Tekst bez wyboru z listy liczy się tylko jako dokładne, jednoznaczne dopasowanie. */
export const resolveTypedLookup = (options: LookupOption[], text: string): TypedLookupResult => {
  if (!normalize(text)) return { kind: 'empty' };
  const exact = findExactIndexes(options, text);
  return exact.length === 1 ? { kind: 'match', option: options[exact[0]] } : { kind: 'none' };
};

/** Sklepy jednej grupy to na liście jedna pozycja, z id pierwszego z nich. */
export const buildStoreOptions = (stores: Store[]): LookupOption[] => {
  const seen = new Set<string>();
  const options: LookupOption[] = [];
  stores.forEach((store) => {
    const label = (store.group_name?.trim() || store.name || '').trim();
    if (!label || seen.has(label)) return;
    seen.add(label);
    options.push({ id: store.id, label, kind: 'entry' });
  });
  return options;
};

/** Konta do wyboru: bez zarchiwizowanych, poza tym, które pole już ma ustawione. */
export const selectableAccounts = (accounts: Account[], currentId?: string | null) =>
  accounts.filter((account) => !account.is_closed || account.id === currentId);

export const buildTransferOptions = (accounts: Account[]): LookupOption[] =>
  accounts.map((account) => {
    const rowLabel = `${account.name}${account.currency ? ` (${account.currency})` : ''}`;
    return { id: account.id, label: `Przelew: ${rowLabel}`, rowLabel, kind: 'transfer' };
  });

type CategoryLike = Pick<Category, 'id' | 'name' | 'parent_id'>;

/** Pełna ścieżka kategorii z nazw przodków: „Dom › Domena Internetowa”. */
export const categoryLookupLabel = (
  categoryId: string,
  byId: ReadonlyMap<string, CategoryLike>
): string => {
  const names: string[] = [];
  const seen = new Set<string>();
  let current = byId.get(categoryId);
  while (current && !seen.has(current.id)) {
    seen.add(current.id);
    names.unshift(current.name.trim());
    current = current.parent_id ? byId.get(current.parent_id) : undefined;
  }
  return names.join(' › ');
};

/**
 * Kategorie do wyboru w kolejności drzewa (rodzic, potem jego dzieci), każda z pełną
 * ścieżką — po samej nazwie podkategorii nie widać, pod czym wisi.
 */
export const buildCategoryOptions = (categories: readonly CategoryLike[]): LookupOption[] => {
  const byId = new Map(categories.map((category) => [category.id, category]));
  const children = new Map<string | null, CategoryLike[]>();
  categories.forEach((category) => {
    const parent = category.parent_id && byId.has(category.parent_id) ? category.parent_id : null;
    children.set(parent, [...(children.get(parent) ?? []), category]);
  });
  const options: LookupOption[] = [];
  const visit = (parentId: string | null) => {
    (children.get(parentId) ?? []).forEach((category) => {
      options.push({ id: category.id, label: categoryLookupLabel(category.id, byId), kind: 'entry' });
      visit(category.id);
    });
  };
  visit(null);
  return options;
};
