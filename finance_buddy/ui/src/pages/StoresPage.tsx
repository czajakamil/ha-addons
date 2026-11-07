// Lista sklepów wzorowana na Kontach: widoki „Wszystkie” i „Do uporządkowania”.
import { useCallback, useMemo, useState } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { Plus, Search, Store as StoreIcon } from 'lucide-react';
import { toast } from 'sonner';
import {
  deleteStore,
  getAllStores,
  getAmbiguousStoreAliases,
  getSession,
  getStoreCategorySuggestions,
  getStoreStats,
  type Store
} from '../api';
import Button from '../components/Button';
import ColumnsMenu from '../components/ui/ColumnsMenu';
import EmptyState from '../components/ui/EmptyState';
import PageHeader from '../components/ui/PageHeader';
import SegmentedControl from '../components/ui/SegmentedControl';
import SkeletonRows from '../components/ui/SkeletonRows';
import { copy } from '../copy/pl';
import { useApi } from '../hooks/useApi';
import { useColumnVisibility } from '../hooks/useColumnVisibility';
import { buildCleanupItems, groupCategorySuggestions, type CleanupItem } from './stores/cleanup';
import { GROUP_NAME_DATALIST_ID } from './stores/model';
import NewStoreModal from './stores/components/NewStoreModal';
import StoreGroupsTable, { type TableSort } from './stores/components/StoreGroupsTable';
import StoresCleanup from './stores/components/StoresCleanup';
import {
  buildStoreGroups,
  DEFAULT_DIRECTION,
  isInactiveGroup,
  sortStoreGroups,
  toStatsMap,
  type GroupSort,
  type StoreGroup
} from './stores/groups';
import { useStoreCategories } from './stores/useStoreCategories';
import { categoryPayload, useStoreGroupSave } from './stores/useStoreGroupSave';

const labels = copy.stores;
const SORTS = Object.keys(labels.sort) as GroupSort[];

const DELETE_CONCURRENCY = 5;

const alwaysShown = () => true;
// Kolumny statystyk są zawsze domyślnie widoczne; rzadkie NIP i adres ukrywa reguła 5%.
const OPTIONAL_COLUMNS = [
  { key: 'category', label: labels.columns.category, hasValue: alwaysShown },
  { key: 'count', label: labels.columns.count, hasValue: alwaysShown },
  { key: 'spent', label: labels.columns.spent, hasValue: alwaysShown },
  { key: 'last', label: labels.columns.last, hasValue: alwaysShown },
  { key: 'nip', label: labels.columns.nip, hasValue: (group: StoreGroup) => group.members.some((store) => store.nip) },
  { key: 'address', label: labels.columns.address, hasValue: (group: StoreGroup) => group.members.some((store) => store.address) }
];

const StoresPage = () => {
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();
  const { data: stores = [], loading, error, refetch, setData: setStores } = useApi(getAllStores, {
    errorMessage: labels.loadFailed
  });
  const session = useApi(getSession);
  const ambiguous = useApi(getAmbiguousStoreAliases);
  const statsRequest = useApi(() => getStoreStats(), { errorMessage: labels.statsFailed });
  const suggestionsRequest = useApi(getStoreCategorySuggestions);
  const { categories, categoryMap, ensureCategory } = useStoreCategories();
  const [searchQuery, setSearchQuery] = useState('');
  const [sort, setSort] = useState<TableSort>({ key: 'name', direction: 'asc' });
  const [showInactive, setShowInactive] = useState(false);
  const [showArchived, setShowArchived] = useState(false);
  const [showNewStore, setShowNewStore] = useState(false);
  const view = searchParams.get('view') === 'cleanup' ? 'cleanup' : 'all';

  const setView = (next: 'all' | 'cleanup') =>
    setSearchParams(next === 'cleanup' ? { view: 'cleanup' } : {}, { replace: true });

  const stats = useMemo(() => (statsRequest.data ? toStatsMap(statsRequest.data) : null), [statsRequest.data]);
  const groups = useMemo(() => buildStoreGroups(stores, stats ?? new Map()), [stores, stats]);
  const ownerName = session.data?.display_name?.trim() || session.data?.username?.trim() || '';
  const ambiguousIds = useMemo(() => new Set(ambiguous.data?.map((alias) => alias.store_id)), [ambiguous.data]);
  const cleanupItems = useMemo(
    () => buildCleanupItems(groups, ownerName, ambiguousIds),
    [groups, ownerName, ambiguousIds]
  );
  const activeGroups = groups.filter((group) => !group.isArchived);
  // Także uporządkowane: pusty sklep z kategorią i ładną nazwą też jest do usunięcia.
  // Bez statystyk nie wiadomo, które są puste.
  const emptyGroups = stats ? activeGroups.filter((group) => group.count === 0) : [];
  const suggestions = useMemo(
    () => groupCategorySuggestions(groups, suggestionsRequest.data ?? []),
    [groups, suggestionsRequest.data]
  );

  const needle = searchQuery.trim().toLocaleLowerCase('pl-PL');
  const matches = (group: StoreGroup) =>
    !needle ||
    group.name.toLocaleLowerCase('pl-PL').includes(needle) ||
    group.members.some((store) =>
      [store.name, store.nip, store.address].some((value) => value?.toLocaleLowerCase('pl-PL').includes(needle))
    ) ||
    Boolean(group.categoryId && categoryMap.get(group.categoryId)?.path.toLocaleLowerCase('pl-PL').includes(needle));

  // Pierwsze kliknięcie kolumny ustawia jej naturalny kierunek, kolejne go odwraca
  const toggleSort = (key: GroupSort) =>
    setSort((current) =>
      current.key === key
        ? { key, direction: current.direction === 'asc' ? 'desc' : 'asc' }
        : { key, direction: DEFAULT_DIRECTION[key] }
    );
  // Bez statystyk liczby są puste, więc do ich wczytania zostaje kolejność alfabetyczna
  const effectiveSort: TableSort = stats || sort.key === 'name' ? sort : { key: 'name', direction: 'asc' };
  const sorted = sortStoreGroups(groups.filter(matches), effectiveSort.key, effectiveSort.direction);
  const archivedGroups = sorted.filter((group) => group.isArchived);
  const visibleActive = sorted.filter((group) => !group.isArchived);
  // Bez statystyk nie wiadomo, co jest nieaktywne, więc do ich wczytania pokazujemy wszystko
  const recentGroups = stats ? visibleActive.filter((group) => !isInactiveGroup(group)) : visibleActive;
  const inactiveGroups = stats ? visibleActive.filter((group) => isInactiveGroup(group)) : [];

  const filteredCleanup = useMemo(
    () =>
      needle
        ? cleanupItems.filter(({ group }) =>
            [group.name, ...group.members.flatMap((store) => [store.name, store.nip])].some((value) =>
              value?.toLocaleLowerCase('pl-PL').includes(needle)
            )
          )
        : cleanupItems,
    [cleanupItems, needle]
  );

  const applyUpdated = useCallback(
    (updated: Store[]) => {
      const byId = new Map(updated.map((store) => [store.id, store]));
      setStores((current) => current?.map((store) => byId.get(store.id) ?? store));
    },
    [setStores]
  );
  const save = useStoreGroupSave(applyUpdated);

  const toggleArchived = (group: StoreGroup) => {
    if (!group.isArchived) setShowArchived(true);
    void save(
      group.members.map((store) => ({ store, payload: { is_deleted: !group.isArchived } })),
      group.isArchived ? labels.restored : labels.archived
    );
  };

  const setCategoryForItems = async (items: CleanupItem[], path: string) => {
    try {
      const categoryId = await ensureCategory(path);
      // Cała grupa dostaje tę samą kategorię, tak jak przy edycji w widoku „Wszystkie”
      return await save(
        items.flatMap(({ group }) => group.members.map((store) => ({ store, payload: categoryPayload(store, categoryId) })))
      );
    } catch (err) {
      console.error(err);
      return false;
    }
  };

  const applySuggestions = (items: CleanupItem[]) => {
    const changes = items.flatMap(({ group }) => {
      const categoryId = suggestions.get(group.key)?.categoryId;
      return categoryId ? group.members.map((store) => ({ store, payload: categoryPayload(store, categoryId) })) : [];
    });
    if (changes.length) void save(changes, labels.suggestionsApplied(items.length));
  };

  const verifyNames = (items: CleanupItem[]) =>
    void save(
      items.flatMap(({ group }) => group.members.map((store) => ({ store, payload: { verified_name: group.name } }))),
      labels.namesVerified(items.length)
    );

  const deleteGroups = async (toDelete: StoreGroup[]) => {
    const targets = toDelete.flatMap((group) => group.members);
    if (!targets.length || !window.confirm(labels.deleteConfirm(toDelete.length))) return;
    // Po kilka naraz, jak przy zapisie grup: po imporcie z YNAB pustych sklepów bywają setki
    const results: PromiseSettledResult<void>[] = [];
    for (let start = 0; start < targets.length; start += DELETE_CONCURRENCY) {
      const batch = targets.slice(start, start + DELETE_CONCURRENCY);
      results.push(...(await Promise.allSettled(batch.map((store) => deleteStore(store.id)))));
    }
    const removed = new Set(targets.filter((_, index) => results[index].status === 'fulfilled').map((store) => store.id));
    const failed = targets.length - removed.size;
    results.forEach((result) => result.status === 'rejected' && console.error(result.reason));
    if (removed.size) setStores((current) => current?.filter((store) => !removed.has(store.id)));
    if (failed) toast.error(labels.deletePartial(failed, targets.length));
    else toast(labels.deleted(toDelete.length));
    // Usunięta nazwa może przestać być niejednoznaczna dla innego sklepu
    void ambiguous.refetch();
  };

  const { hiddenKeys, setVisible } = useColumnVisibility('stores-groups', OPTIONAL_COLUMNS, activeGroups);

  const renderTable = (rows: StoreGroup[]) => (
    <StoreGroupsTable
      groups={rows}
      stats={stats}
      hiddenKeys={hiddenKeys}
      sort={effectiveSort}
      onSort={toggleSort}
      categoryMap={categoryMap}
      onOpen={(group) => navigate(`/payees/${group.id}`)}
      onSettings={(group) => navigate(`/payees/${group.id}?tab=settings`)}
      // Nazwa grupowa, nie `name`: importer dopasowuje sklepy po `name`
      onRename={(group, name) => void save(group.members.map((store) => ({ store, payload: { group_name: name } })))}
      onToggleArchived={toggleArchived}
    />
  );

  return (
    <div className="page-shell accounts-screen stores-screen">
      <PageHeader
        title={labels.title}
        count={activeGroups.length}
        actions={
          <>
            <ColumnsMenu columns={OPTIONAL_COLUMNS} hiddenKeys={hiddenKeys} onChange={setVisible} />
            <Button icon={<Plus size={16} aria-hidden="true" />} onClick={() => setShowNewStore(true)}>
              {labels.newStore}
            </Button>
          </>
        }
      />

      <div className="stores-toolbar">
        <SegmentedControl
          label={labels.viewLabel}
          value={view}
          options={[
            { value: 'all', label: labels.viewAll, count: activeGroups.length },
            { value: 'cleanup', label: labels.viewCleanup, count: cleanupItems.length }
          ]}
          onChange={setView}
        />
        <div className="accounts-toolbar__field accounts-toolbar__field--search stores-toolbar__search">
          <div className="input-icon">
            <span className="input-icon-addon">
              <Search size={16} aria-hidden="true" />
            </span>
            <input
              type="search"
              className="form-control form-control-sm accounts-search"
              placeholder={labels.search}
              aria-label={labels.searchLabel}
              value={searchQuery}
              onChange={(event) => setSearchQuery(event.target.value)}
            />
          </div>
        </div>
        {view === 'all' && (
          <select
            className="form-select form-select-sm stores-toolbar__sort"
            aria-label={labels.sortLabel}
            value={sort.key}
            onChange={(event) => {
              const key = event.target.value as GroupSort;
              setSort({ key, direction: DEFAULT_DIRECTION[key] });
            }}
          >
            {SORTS.map((value) => (
              <option key={value} value={value}>
                {labels.sort[value]}
              </option>
            ))}
          </select>
        )}
      </div>

      {error && !loading && <p className="accounts-error" role="alert">{error}</p>}
      {statsRequest.error && (
        <p className="accounts-error" role="alert">
          {labels.statsFailed}{' '}
          <button type="button" className="text-button" onClick={() => void statsRequest.refetch()}>
            {labels.retry}
          </button>
        </p>
      )}
      {loading && !stores.length && <SkeletonRows rows={8} />}
      {!loading && !error && !stores.length && <EmptyState icon={StoreIcon} message={labels.emptyAll} />}

      {stores.length > 0 && view === 'all' && (
        <>
          {cleanupItems.length > 0 && (
            <div className="stores-banner">
              <span className="stores-banner__dot" aria-hidden="true" />
              <span className="stores-banner__text">{labels.banner(cleanupItems.length)}</span>
              <button type="button" className="account-button" onClick={() => setView('cleanup')}>
                {labels.review}
              </button>
            </div>
          )}
          {!stats && !statsRequest.error && <p className="stores-muted stores-status">{labels.statsLoading}</p>}
          {needle && !sorted.length && <EmptyState icon={StoreIcon} message={labels.empty} />}
          {recentGroups.length > 0 && renderTable(recentGroups)}
          {inactiveGroups.length > 0 && (
            <section className="accounts-group">
              <button type="button" className="text-button" onClick={() => setShowInactive((prev) => !prev)}>
                {showInactive ? labels.hideInactive : labels.showInactive(inactiveGroups.length)}
              </button>
              {showInactive && renderTable(inactiveGroups)}
            </section>
          )}
          {archivedGroups.length > 0 && (
            <section className="accounts-group">
              <button type="button" className="text-button" onClick={() => setShowArchived((prev) => !prev)}>
                {showArchived ? labels.hideArchived : labels.showArchived(archivedGroups.length)}
              </button>
              {showArchived && renderTable(archivedGroups)}
            </section>
          )}
        </>
      )}

      {stores.length > 0 && view === 'cleanup' && (
        <StoresCleanup
          items={filteredCleanup}
          stats={stats}
          categories={categories}
          categoryMap={categoryMap}
          suggestions={suggestions}
          onApplySuggestions={applySuggestions}
          onVerifyNames={verifyNames}
          onOpen={(storeId) => navigate(`/payees/${storeId}`)}
          onArchive={({ group }) =>
            void save(group.members.map((store) => ({ store, payload: { is_deleted: true } })), labels.archived)
          }
          emptyGroups={emptyGroups}
          onDelete={(toDelete) => void deleteGroups(toDelete)}
          onSetCategory={setCategoryForItems}
          ensureCategory={ensureCategory}
          onMerged={async () => {
            await Promise.all([refetch(), statsRequest.refetch(), ambiguous.refetch(), suggestionsRequest.refetch()]);
          }}
        />
      )}

      {showNewStore && <NewStoreModal onClose={() => setShowNewStore(false)} onCreated={async () => void (await refetch())} />}
      <datalist id={GROUP_NAME_DATALIST_ID}>
        {activeGroups.map((group) => (
          <option key={group.key} value={group.name} />
        ))}
      </datalist>
    </div>
  );
};

export default StoresPage;
