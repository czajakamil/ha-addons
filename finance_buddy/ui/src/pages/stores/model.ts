// Typy, stałe i czyste helpery strony sklepów — bez stanu i bez JSX.
import { Store } from '../../api';
import { DATA_TABLE_ROW_HEIGHT } from '../../components/DataTable';

export const GROUP_NAME_DATALIST_ID = 'store-group-name-suggestions';

export type EditableField = 'name' | 'group_name' | 'nip' | 'address' | 'comment';

export type EditingCell = {
  storeId: string;
  field: EditableField;
};

const EDITABLE_FIELDS: readonly EditableField[] = ['name', 'group_name', 'nip', 'address', 'comment'] as const;

export const isEditableField = (value: string): value is EditableField =>
  EDITABLE_FIELDS.some((field) => field === value);

const STORES_GRID_MIN_HEIGHT = 240;
const STORES_GRID_MAX_HEIGHT = 560;

export const getGridHeight = (rowCount: number) => {
  if (rowCount <= 0) {
    return STORES_GRID_MIN_HEIGHT;
  }
  const estimated = 48 + DATA_TABLE_ROW_HEIGHT * rowCount;
  return Math.max(STORES_GRID_MIN_HEIGHT, Math.min(estimated, STORES_GRID_MAX_HEIGHT));
};

export const getFieldInitialValue = (store: Store, field: EditableField): string => {
  switch (field) {
    case 'comment':
      return store.comment ?? '';
    case 'name':
      return store.name;
    case 'group_name':
      return store.group_name ?? '';
    case 'address':
      return store.address ?? '';
    case 'nip':
    default:
      return store.nip ?? '';
  }
};

export const getFieldDisplayValue = (store: Store, field: EditableField): string => {
  if (field === 'name') {
    return store.name;
  }
  const value = getFieldInitialValue(store, field);
  return value.length ? value : '—';
};

export const getStoreLabel = (store: Store) => store.group_name?.trim() || store.name;

export type SortOption = 'name_asc' | 'name_desc' | 'group_asc';

export const SORT_OPTIONS: { value: SortOption; label: string }[] = [
  { value: 'name_asc', label: 'Alfabetycznie (A-Z)' },
  { value: 'name_desc', label: 'Alfabetycznie (Z-A)' },
  { value: 'group_asc', label: 'Nazwa grupowa (A-Z)' }
];

// Sortowanie listy sklepów. Przy sortowaniu po nazwie grupowej sklepy bez grupy
// lądują na końcu, a w obrębie grupy porządkujemy je alfabetycznie po nazwie.
export const sortStores = (stores: Store[], sortOption: SortOption): Store[] => {
  const sorted = stores.slice();
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
    const nameDiff = a.name.localeCompare(b.name, 'pl');
    return sortOption === 'name_asc' ? nameDiff : -nameDiff;
  });
  return sorted;
};
