import { useCallback, useEffect, useMemo, useState } from 'react';
import { type CellClickArgs, type Column } from 'react-data-grid';
import { IconAlertCircle, IconPencil, IconPlus } from '@tabler/icons-react';
import {
  type Category,
  Item,
  ensureCategoryPath,
  getAllCategories,
  getAllItems
} from '../api';
import DataTable from '../components/DataTable';
import Button from '../components/Button';
import { useApi } from '../hooks/useApi';
import {
  GROUP_NAME_DATALIST_ID,
  getGridHeight,
  getItemLabel,
  isEditableField,
  sortItems,
  type SortOption
} from './items/model';
import { useItemsEditing } from './items/useItemsEditing';
import ItemsToolbarRight from './items/components/ItemsToolbarRight';
import NewItemModal from './items/components/NewItemModal';
import ManageItemModal from './items/components/ManageItemModal';

const ItemsPage = () => {
  const {
    data: items = [],
    loading,
    error,
    refetch: fetchItems,
    setData: setItems
  } = useApi(getAllItems, { errorMessage: 'Nie udało się wczytać przedmiotów.' });
  const [categories, setCategories] = useState<Category[]>([]);
  const [searchQuery, setSearchQuery] = useState('');
  const [sortOption, setSortOption] = useState<SortOption>('name_asc');
  const [showNewItemModal, setShowNewItemModal] = useState(false);
  const [detailsItemId, setDetailsItemId] = useState<string | null>(null);

  const categoryPathById = useMemo(
    () => new Map(categories.map((category) => [category.id, category.path])),
    [categories]
  );

  const getCategoryPath = useCallback(
    (categoryId?: string | null) => (categoryId ? categoryPathById.get(categoryId) ?? '' : ''),
    [categoryPathById]
  );

  const groupNameSuggestions = useMemo(() => {
    const unique = new Set<string>();
    items.forEach((item) => {
      const name = (item.group_name ?? '').trim();
      if (name.length) {
        unique.add(name);
      }
    });
    return Array.from(unique).sort((a, b) => a.localeCompare(b, 'pl-PL', { sensitivity: 'base' }));
  }, [items]);

  // Zamienia wpisaną ścieżkę tagu na id (tworząc brakujące tagi) i odświeża
  // lokalną listę, żeby nowy tag od razu podpowiadał się w autocomplete.
  const ensureItemCategory = useCallback(
    async (path: string): Promise<string> => {
      const id = await ensureCategoryPath(path, categories, false);
      if (!categories.some((category) => category.id === id)) {
        const data = await getAllCategories(200, { for_accounts: false });
        setCategories(data);
      }
      return id;
    },
    [categories]
  );

  const {
    startEditing,
    startTagEditing,
    tagEditingItemId,
    tagSavingItemId,
    renderCell,
    renderTagCell,
    inlineError,
    setInlineError
  } = useItemsEditing({
    items,
    setItems,
    categories,
    getCategoryPath,
    ensureCategory: ensureItemCategory
  });

  // Tagi ładujemy od razu — kolumna „Tag" pokazuje ścieżkę tagu przypisanego do przedmiotu.
  useEffect(() => {
    void (async () => {
      try {
        const data = await getAllCategories(200, { for_accounts: false });
        setCategories(data);
      } catch (err) {
        console.error(err);
        setInlineError('Nie udało się wczytać tagów.');
      }
    })();
  }, [setInlineError]);

  const handleItemCreated = useCallback(async () => {
    await fetchItems();
  }, [fetchItems]);

  const handleItemSaved = useCallback(
    (updated: Item) => {
      setItems((prev) => (prev ?? []).map((entry) => (entry.id === updated.id ? updated : entry)));
    },
    [setItems]
  );

  const handleItemDeleted = useCallback(
    (id: string) => {
      setItems((prev) => (prev ?? []).filter((entry) => entry.id !== id));
      setDetailsItemId(null);
    },
    [setItems]
  );

  const detailsItem = detailsItemId ? items.find((entry) => entry.id === detailsItemId) ?? null : null;

  useEffect(() => {
    if (detailsItemId && !detailsItem) {
      setDetailsItemId(null);
    }
  }, [detailsItemId, detailsItem]);

  const itemColumns: readonly Column<Item>[] = [
    {
      key: 'actions',
      name: '',
      width: 56,
      minWidth: 56,
      maxWidth: 56,
      resizable: false,
      headerCellClass: 'text-center',
      cellClass: 'text-center',
      renderCell: ({ row }) => (
        <div className="accounts-edit-cell">
          <button
            type="button"
            className="btn btn-ghost-secondary btn-icon btn-sm accounts-edit-btn is-focusable"
            aria-label={`Edytuj przedmiot ${getItemLabel(row)}`}
            title="Edytuj przedmiot"
            onClick={(event) => {
              event.stopPropagation();
              setInlineError(null);
              setDetailsItemId(row.id);
            }}
          >
            <IconPencil size={16} aria-hidden="true" />
          </button>
        </div>
      )
    },
    {
      key: 'name',
      name: 'Nazwa',
      width: 220,
      renderCell: ({ row }) => renderCell(row, 'name')
    },
    {
      key: 'group_name',
      name: 'Nazwa grupowa',
      width: 200,
      renderCell: ({ row }) => renderCell(row, 'group_name')
    },
    {
      key: 'tag',
      name: 'Tag',
      width: 220,
      renderCell: ({ row }) => renderTagCell(row)
    },
    {
      key: 'unit_of_measurement',
      name: 'Jednostka',
      width: 140,
      renderCell: ({ row }) => renderCell(row, 'unit_of_measurement')
    },
    {
      key: 'manufacturer',
      name: 'Producent',
      width: 200,
      renderCell: ({ row }) => renderCell(row, 'manufacturer')
    },
    {
      key: 'comment',
      name: 'Komentarz',
      minWidth: 200,
      renderCell: ({ row }) => renderCell(row, 'comment')
    }
  ];

  const getItemRowId = useCallback((item: Item) => item.id, []);

  const handleGridDoubleClick = useCallback(
    (args: CellClickArgs<Item>) => {
      const field = args.column.key;
      if (field === 'tag') {
        if (tagEditingItemId !== args.row.id && !tagSavingItemId) {
          startTagEditing(args.row);
        }
        return;
      }
      if (typeof field === 'string' && isEditableField(field)) {
        startEditing(args.row, field);
      }
    },
    [startEditing, startTagEditing, tagEditingItemId, tagSavingItemId]
  );

  const filteredItems = items.filter((item) => {
    if (!searchQuery.trim()) {
      return true;
    }
    const needle = searchQuery.toLowerCase();
    return (
      item.name.toLowerCase().includes(needle) ||
      (item.group_name ?? '').toLowerCase().includes(needle) ||
      (item.category_id ? categoryPathById.get(item.category_id) ?? '' : '').toLowerCase().includes(needle) ||
      (item.unit_of_measurement ?? '').toLowerCase().includes(needle) ||
      (item.manufacturer ?? '').toLowerCase().includes(needle) ||
      (item.comment ?? '').toLowerCase().includes(needle)
    );
  });

  const sortedItems = useMemo(() => sortItems(filteredItems, sortOption), [filteredItems, sortOption]);

  const noResults = !loading && !error && filteredItems.length === 0;

  return (
    <div className="page accounts-page items-page">
      <div className="page-shell">
        <div className="d-print-none accounts-page__header">
          <div className="accounts-page__title-wrap">
            <h2 className="page-title accounts-page__title">Przedmioty</h2>
            <p className="accounts-page__subtitle">{items.length} pozycji w słowniku</p>
          </div>
          <div className="accounts-page__actions">
            <Button icon={<IconPlus size={16} aria-hidden="true" />} onClick={() => setShowNewItemModal(true)}>
              Nowy przedmiot
            </Button>
          </div>
        </div>

        <div className="page-body page-stack accounts-page__body">
          <section className="card accounts-section-card">
            <div className="accounts-section-card__header">
              <ItemsToolbarRight
                searchQuery={searchQuery}
                setSearchQuery={setSearchQuery}
                sortOption={sortOption}
                setSortOption={setSortOption}
              />
            </div>

            {inlineError && (
              <div className="accounts-section-card__alert">
                <div className="alert alert-danger alert-important d-flex align-items-center gap-2 mb-0" role="alert">
                  <IconAlertCircle size={18} aria-hidden="true" />
                  <div>{inlineError}</div>
                </div>
              </div>
            )}

            {loading && (
              <div className="accounts-section-card__body">
                <div className="placeholder-glow">
                  <div className="placeholder col-12 mb-2" />
                  <div className="placeholder col-10 mb-2" />
                  <div className="placeholder col-12 mb-2" />
                  <div className="placeholder col-8 mb-0" />
                </div>
              </div>
            )}

            {error && !loading && (
              <div className="accounts-section-card__body">
                <div className="alert alert-danger mb-0" role="alert">
                  {error}
                </div>
              </div>
            )}

            {!loading && !error && (
              <div className="accounts-section-card__table">
                {!noResults ? (
                  <DataTable
                    aria-label="Lista przedmiotów"
                    className="accounts-grid items-grid"
                    columns={itemColumns}
                    rows={sortedItems}
                    rowKeyGetter={getItemRowId}
                    height={getGridHeight(sortedItems.length)}
                    onCellDoubleClick={handleGridDoubleClick}
                    noRowsFallback={
                      <div className="data-grid__empty text-secondary text-center">
                        Brak przedmiotów w wynikach filtrowania.
                      </div>
                    }
                  />
                ) : (
                  <div className="accounts-empty-state">
                    <div className="accounts-empty-state__title">Brak wyników</div>
                    <p className="accounts-empty-state__subtitle">
                      Nie znaleziono przedmiotów dla podanego filtra. Spróbuj innej frazy wyszukiwania.
                    </p>
                  </div>
                )}
              </div>
            )}
          </section>
        </div>
      </div>
      {showNewItemModal && (
        <NewItemModal
          onClose={() => setShowNewItemModal(false)}
          onCreated={handleItemCreated}
          categories={categories}
          ensureCategory={ensureItemCategory}
        />
      )}
      {detailsItem && (
        <ManageItemModal
          key={detailsItem.id}
          item={detailsItem}
          categories={categories}
          ensureCategory={ensureItemCategory}
          onClose={() => setDetailsItemId(null)}
          onSaved={handleItemSaved}
          onDeleted={handleItemDeleted}
        />
      )}
      <datalist id={GROUP_NAME_DATALIST_ID}>
        {groupNameSuggestions.map((name) => (
          <option key={name} value={name} />
        ))}
      </datalist>
    </div>
  );
};

export default ItemsPage;
