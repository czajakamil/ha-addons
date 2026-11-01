import { useCallback, useEffect, useMemo, useState } from 'react';
import { type CellClickArgs, type Column } from 'react-data-grid';
import { CircleAlertIcon, PencilIcon, PlusIcon } from 'lucide-react';
import { Category, Store, ensureCategoryPath, getAllCategories, getAllStores } from '../api';
import DataTable from '../components/DataTable';
import Button from '../components/Button';
import ColumnsMenu from '../components/ui/ColumnsMenu';
import { useApi } from '../hooks/useApi';
import { useColumnVisibility } from '../hooks/useColumnVisibility';
import { isAutoComment, userComment } from '../utils/transactionLabels';
import {
  GROUP_NAME_DATALIST_ID,
  getGridHeight,
  getStoreLabel,
  isEditableField,
  sortStores,
  type SortOption
} from './stores/model';
import { useStoresEditing } from './stores/useStoresEditing';
import StoresToolbarRight from './stores/components/StoresToolbarRight';
import NewStoreModal from './stores/components/NewStoreModal';
import ManageStoreModal from './stores/components/ManageStoreModal';
import DeleteStoreModal from './stores/components/DeleteStoreModal';

const OPTIONAL_COLUMNS = [
  { key: 'group_name', label: 'Nazwa grupowa', hasValue: (row: Store) => Boolean(row.group_name?.trim()) },
  { key: 'tag', label: 'Tag', hasValue: (row: Store) => Boolean(row.automatic_category_id) },
  { key: 'nip', label: 'NIP', hasValue: (row: Store) => Boolean(row.nip?.trim()) },
  { key: 'address', label: 'Adres', hasValue: (row: Store) => Boolean(row.address?.trim()) },
  { key: 'comment', label: 'Komentarz', hasValue: (row: Store) => Boolean(userComment(row.comment)) }
];

const StoresPage = () => {
  const {
    data: stores = [],
    loading,
    error,
    refetch: fetchStores,
    setData: setStores
  } = useApi(getAllStores, { errorMessage: 'Nie udało się wczytać sklepów.' });
  const [categories, setCategories] = useState<Category[]>([]);
  const [categoriesLoading, setCategoriesLoading] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [sortOption, setSortOption] = useState<SortOption>('name_asc');
  const [showNewStoreModal, setShowNewStoreModal] = useState(false);
  const [showArchivedStores, setShowArchivedStores] = useState(false);
  const [detailsStoreId, setDetailsStoreId] = useState<string | null>(null);
  const [deletingStoreId, setDeletingStoreId] = useState<string | null>(null);
  const [openingDetailsStoreId, setOpeningDetailsStoreId] = useState<string | null>(null);

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
    stores.forEach((store) => {
      const name = (store.group_name ?? '').trim();
      if (name.length) {
        unique.add(name);
      }
    });
    return Array.from(unique).sort((a, b) => a.localeCompare(b, 'pl-PL', { sensitivity: 'base' }));
  }, [stores]);

  const handleStoreCreated = useCallback(async () => {
    await fetchStores();
  }, [fetchStores]);

  // Zamienia wpisaną ścieżkę tagu na id (tworząc brakujące tagi) i odświeża
  // lokalną listę, żeby nowy tag od razu podpowiadał się w autocomplete.
  const ensureStoreCategory = useCallback(
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
    tagEditingStoreId,
    tagSavingStoreId,
    renderCell,
    renderTagCell,
    inlineError,
    setInlineError
  } = useStoresEditing({
    stores,
    setStores,
    categories,
    getCategoryPath,
    ensureCategory: ensureStoreCategory
  });

  const ensureCategoriesLoaded = useCallback(async () => {
    if (categories.length > 0 || categoriesLoading) {
      return;
    }

    try {
      setCategoriesLoading(true);
      const data = await getAllCategories(200, { for_accounts: false });
      setCategories(data);
    } catch (err) {
      console.error(err);
      setInlineError('Nie udało się wczytać tagów.');
    } finally {
      setCategoriesLoading(false);
    }
  }, [categories.length, categoriesLoading, setInlineError]);

  // Tagi ładujemy od razu — kolumna „Tag” pokazuje ścieżkę tagu przypisanego do sklepu.
  useEffect(() => {
    void ensureCategoriesLoaded();
  }, [ensureCategoriesLoaded]);

  // Po zapisie sklepu w modalu aktualizujemy listę; przy archiwizacji dodatkowo
  // rozwijamy sekcję archiwum, żeby sklep był od razu widoczny na nowym miejscu.
  const handleStoreSaved = useCallback(
    (updated: Store) => {
      setStores((prev) => (prev ?? []).map((item) => (item.id === updated.id ? updated : item)));
      if (updated.is_deleted) {
        setShowArchivedStores(true);
      }
    },
    [setStores]
  );

  // Trwałe usunięcie sklepu wypada z listy — po skasowaniu odświeżamy dane.
  const handleStoreDeleted = useCallback(async () => {
    await fetchStores();
  }, [fetchStores]);

  const deletingStore = deletingStoreId
    ? stores.find((item) => item.id === deletingStoreId) ?? null
    : null;

  useEffect(() => {
    if (deletingStoreId && !deletingStore) {
      setDeletingStoreId(null);
    }
  }, [deletingStoreId, deletingStore]);

  const detailsStore = detailsStoreId ? stores.find((item) => item.id === detailsStoreId) ?? null : null;

  useEffect(() => {
    if (detailsStoreId && !detailsStore) {
      setDetailsStoreId(null);
    }
  }, [detailsStoreId, detailsStore]);

  // Tagi ładujemy PRZED otwarciem modala — formularz inicjalizuje się przy
  // montowaniu i potrzebuje ścieżki tagu wyliczonej z automatic_category_id.
  const handleOpenStoreDetailsModal = async (store: Store) => {
    if (openingDetailsStoreId) return;

    setInlineError(null);
    setOpeningDetailsStoreId(store.id);

    try {
      await ensureCategoriesLoaded();
      setDetailsStoreId(store.id);
    } catch (err) {
      console.error(err);
    } finally {
      setOpeningDetailsStoreId(null);
    }
  };

  const { hiddenKeys, setVisible } = useColumnVisibility('stores', OPTIONAL_COLUMNS, stores);

  const allColumns: readonly Column<Store>[] = [
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
            aria-label={`Edytuj sklep ${getStoreLabel(row)}`}
            title="Edytuj sklep"
            onClick={(event) => {
              event.stopPropagation();
              void handleOpenStoreDetailsModal(row);
            }}
            disabled={openingDetailsStoreId === row.id}
          >
            <PencilIcon size={16} aria-hidden="true" />
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
      width: 200,
      renderCell: ({ row }) => renderTagCell(row)
    },
    {
      key: 'nip',
      name: 'NIP',
      width: 140,
      renderCell: ({ row }) => renderCell(row, 'nip')
    },
    {
      key: 'address',
      name: 'Adres',
      width: 240,
      renderCell: ({ row }) => renderCell(row, 'address')
    },
    {
      key: 'comment',
      name: 'Komentarz',
      minWidth: 200,
      renderCell: ({ row }) => (isAutoComment(row.comment) ? null : renderCell(row, 'comment'))
    }
  ];
  // Ostatnia widoczna kolumna wypełnia szerokość, żeby po ukryciu pól tabela nie kończyła się w połowie
  const storeColumns = allColumns
    .filter((column) => !hiddenKeys.has(String(column.key)))
    .map((column, index, visible) => (index === visible.length - 1 ? { ...column, width: undefined } : column));

  const getStoreRowId = useCallback((store: Store) => store.id, []);

  const handleGridDoubleClick = useCallback(
    (args: CellClickArgs<Store>) => {
      const field = args.column.key;
      if (field === 'tag') {
        if (tagEditingStoreId !== args.row.id && !tagSavingStoreId) {
          startTagEditing(args.row);
        }
        return;
      }
      if (typeof field === 'string' && isEditableField(field)) {
        startEditing(args.row, field);
      }
    },
    [startEditing, startTagEditing, tagEditingStoreId, tagSavingStoreId]
  );

  const filteredStores = stores.filter((store) => {
    if (!searchQuery.trim()) {
      return true;
    }
    const needle = searchQuery.toLowerCase();
    return (
      store.name.toLowerCase().includes(needle) ||
      (store.group_name ?? '').toLowerCase().includes(needle) ||
      (store.automatic_category_id ? categoryPathById.get(store.automatic_category_id) ?? '' : '')
        .toLowerCase()
        .includes(needle) ||
      (store.nip ?? '').toLowerCase().includes(needle) ||
      (store.address ?? '').toLowerCase().includes(needle) ||
      (store.comment ?? '').toLowerCase().includes(needle)
    );
  });

  const sortedStores = useMemo(() => sortStores(filteredStores, sortOption), [filteredStores, sortOption]);

  const activeStores = sortedStores.filter((store) => !store.is_deleted);
  const archivedStores = sortedStores.filter((store) => store.is_deleted);
  const totalActiveStores = stores.filter((store) => !store.is_deleted).length;
  const totalArchivedStores = stores.length - totalActiveStores;

  const noResults = !loading && !error && filteredStores.length === 0;

  return (
    <div className="page accounts-page">
      <div className="page-shell">
        <div className="d-print-none accounts-page__header">
          <div className="accounts-page__title-wrap">
            <h2 className="page-title accounts-page__title">Sklepy</h2>
            <p className="accounts-page__subtitle">
              {totalActiveStores} aktywne / {totalArchivedStores} zarchiwizowane
            </p>
          </div>
          <div className="accounts-page__actions">
            <ColumnsMenu columns={OPTIONAL_COLUMNS} hiddenKeys={hiddenKeys} onChange={setVisible} />
            <Button icon={<PlusIcon size={16} aria-hidden="true" />} onClick={() => setShowNewStoreModal(true)}>
              Nowy sklep
            </Button>
          </div>
        </div>

        <div className="page-body page-stack accounts-page__body">
          <section className="card accounts-section-card">
            <div className="accounts-section-card__header">
              <StoresToolbarRight
                searchQuery={searchQuery}
                setSearchQuery={setSearchQuery}
                sortOption={sortOption}
                setSortOption={setSortOption}
              />
            </div>

            {inlineError && (
              <div className="accounts-section-card__alert">
                <div className="alert alert-danger alert-important d-flex align-items-center gap-2 mb-0" role="alert">
                  <CircleAlertIcon size={18} aria-hidden="true" />
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
                    aria-label="Lista sklepów"
                    className="accounts-grid stores-grid"
                    columns={storeColumns}
                    rows={activeStores}
                    rowKeyGetter={getStoreRowId}
                    height={getGridHeight(activeStores.length)}
                    onCellDoubleClick={handleGridDoubleClick}
                    noRowsFallback={
                      <div className="data-grid__empty text-secondary text-center">
                        Brak aktywnych sklepów w wynikach filtrowania.
                      </div>
                    }
                  />
                ) : (
                  <div className="accounts-empty-state">
                    <div className="accounts-empty-state__title">Brak wyników</div>
                    <p className="accounts-empty-state__subtitle">
                      Nie znaleziono sklepów dla podanego filtra. Spróbuj innej frazy wyszukiwania.
                    </p>
                  </div>
                )}
              </div>
            )}
          </section>

          {archivedStores.length > 0 && (
            <section className="card accounts-section-card accounts-section-card--closed">
              <div className="accounts-section-card__header accounts-section-card__header--closed">
                <div className="accounts-section-card__header-copy">
                  <div className="accounts-section-card__eyebrow">Archiwum</div>
                  <h3 className="accounts-section-card__title">Zarchiwizowane sklepy</h3>
                  <p className="accounts-section-card__hint">Łącznie: {archivedStores.length}.</p>
                </div>
                <button
                  type="button"
                  className="btn btn-outline-secondary btn-sm is-focusable"
                  onClick={() => setShowArchivedStores((prev) => !prev)}
                >
                  {showArchivedStores ? 'Ukryj' : 'Pokaż'}
                </button>
              </div>
              {showArchivedStores && (
                <div className="accounts-section-card__table">
                  <DataTable
                    aria-label="Lista zarchiwizowanych sklepów"
                    className="accounts-grid accounts-grid--archived stores-grid stores-grid--archived"
                    columns={storeColumns}
                    rows={archivedStores}
                    rowKeyGetter={getStoreRowId}
                    height={getGridHeight(archivedStores.length)}
                    onCellDoubleClick={handleGridDoubleClick}
                    noRowsFallback={
                      <div className="data-grid__empty text-secondary text-center">
                        Brak zarchiwizowanych sklepów w wynikach filtrowania.
                      </div>
                    }
                  />
                </div>
              )}
            </section>
          )}
        </div>
      </div>
      {showNewStoreModal && (
        <NewStoreModal onClose={() => setShowNewStoreModal(false)} onCreated={handleStoreCreated} />
      )}
      {detailsStore && (
        <ManageStoreModal
          key={detailsStore.id}
          store={detailsStore}
          categories={categories}
          ensureCategory={ensureStoreCategory}
          onClose={() => setDetailsStoreId(null)}
          onSaved={handleStoreSaved}
          onRequestDelete={() => {
            setDeletingStoreId(detailsStore.id);
            setDetailsStoreId(null);
          }}
        />
      )}
      {deletingStore && (
        <DeleteStoreModal
          store={deletingStore}
          onClose={() => setDeletingStoreId(null)}
          onDeleted={handleStoreDeleted}
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

export default StoresPage;
