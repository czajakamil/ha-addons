// Typy, stałe i czyste helpery strony przedmiotów — bez stanu i bez JSX.
import type { Item, ItemMergeResult, ItemStatsRow } from '../../api';
import { pluralPL } from '../../utils/format';
import { DATA_TABLE_ROW_HEIGHT } from '../../components/DataTable';

export const GROUP_NAME_DATALIST_ID = 'item-group-name-suggestions';

// Tag ma osobny cykl edycji (useTagCellEditing) — nie jest polem tekstowym.
export type EditableField = 'name' | 'group_name' | 'unit_of_measurement' | 'manufacturer' | 'comment';

export type EditingCell = {
  itemId: string;
  field: EditableField;
};

const EDITABLE_FIELDS: readonly EditableField[] = [
  'name',
  'group_name',
  'unit_of_measurement',
  'manufacturer',
  'comment'
] as const;

export const isEditableField = (value: string): value is EditableField =>
  EDITABLE_FIELDS.some((field) => field === value);

const ITEMS_GRID_MIN_HEIGHT = 240;
const ITEMS_GRID_MAX_HEIGHT = 560;

export const getGridHeight = (rowCount: number) => {
  if (rowCount <= 0) {
    return ITEMS_GRID_MIN_HEIGHT;
  }
  const estimated = 48 + DATA_TABLE_ROW_HEIGHT * rowCount;
  return Math.max(ITEMS_GRID_MIN_HEIGHT, Math.min(estimated, ITEMS_GRID_MAX_HEIGHT));
};

export const getFieldInitialValue = (item: Item, field: EditableField): string => {
  switch (field) {
    case 'comment':
      return item.comment ?? '';
    case 'group_name':
      return item.group_name ?? '';
    case 'manufacturer':
      return item.manufacturer ?? '';
    case 'unit_of_measurement':
      return item.unit_of_measurement ?? '';
    case 'name':
    default:
      return item.name;
  }
};

export const getFieldDisplayValue = (item: Item, field: EditableField): string => {
  if (field === 'name') {
    return item.name;
  }
  const value = getFieldInitialValue(item, field);
  return value.length ? value : '—';
};

export const getItemLabel = (item: Item) => item.group_name?.trim() || item.name;

export type ItemStat = { count: number; spentCents: number; lastDate: string | null };

export const toItemStatsMap = (rows: readonly ItemStatsRow[]) =>
  new Map<string, ItemStat>(
    rows.map((row) => [row.item_id, { count: row.count, spentCents: row.spent_cents_12m, lastDate: row.last_date ?? null }])
  );

export type SortOption = 'name_asc' | 'name_desc' | 'group_asc' | 'spent_desc' | 'count_desc' | 'last_desc';

export const SORT_OPTIONS: { value: SortOption; label: string }[] = [
  { value: 'name_asc', label: 'Alfabetycznie (A-Z)' },
  { value: 'name_desc', label: 'Alfabetycznie (Z-A)' },
  { value: 'group_asc', label: 'Nazwa grupowa (A-Z)' },
  { value: 'spent_desc', label: 'Najwięcej wydane (12 mies.)' },
  { value: 'count_desc', label: 'Najczęściej kupowane' },
  { value: 'last_desc', label: 'Ostatnio kupione' }
];

const statValue = (stat: ItemStat | undefined, sortOption: SortOption): number | string => {
  if (sortOption === 'spent_desc') return stat?.spentCents ?? 0;
  if (sortOption === 'count_desc') return stat?.count ?? 0;
  return stat?.lastDate ?? '';
};

// Sortowanie listy przedmiotów. Przy sortowaniu po nazwie grupowej przedmioty
// bez grupy lądują na końcu, a w obrębie grupy porządkujemy je alfabetycznie po nazwie.
// Po statystykach malejąco; remisy (np. nigdy niekupione) idą alfabetycznie.
export const sortItems = (
  items: Item[],
  sortOption: SortOption,
  stats: ReadonlyMap<string, ItemStat> = new Map()
): Item[] => {
  const sorted = items.slice();
  sorted.sort((a, b) => {
    if (sortOption === 'group_asc') {
      const aGroup = (a.group_name ?? '').trim();
      const bGroup = (b.group_name ?? '').trim();
      if (!aGroup && bGroup) return 1;
      if (aGroup && !bGroup) return -1;
      const groupDiff = aGroup.localeCompare(bGroup, 'pl', { sensitivity: 'base' });
      if (groupDiff !== 0) return groupDiff;
      return a.name.localeCompare(b.name, 'pl');
    }
    if (sortOption === 'spent_desc' || sortOption === 'count_desc' || sortOption === 'last_desc') {
      const aValue = statValue(stats.get(a.id), sortOption);
      const bValue = statValue(stats.get(b.id), sortOption);
      if (aValue !== bValue) return aValue < bValue ? 1 : -1;
      return a.name.localeCompare(b.name, 'pl');
    }
    const nameDiff = a.name.localeCompare(b.name, 'pl');
    return sortOption === 'name_asc' ? nameDiff : -nameDiff;
  });
  return sorted;
};

// --- Scalanie przedmiotów ---

export const pozycje = (count: number) => `${count} ${pluralPL(count, 'pozycja', 'pozycje', 'pozycji')}`;
const nazwy = (count: number) => `${count} ${pluralPL(count, 'nazwa', 'nazwy', 'nazw')}`;

export const describeMerge = (result: ItemMergeResult, sourceLabel: string, targetLabel: string) => {
  const names = result.aliases_moved + result.aliases_added;
  const parts = [pozycje(result.items_moved)];
  if (names) parts.push(`${nazwy(names)} z paragonów`);
  return `Do „${targetLabel}” przejdzie ${parts.join(' i ')}. „${sourceLabel}” zniknie z listy.`;
};
