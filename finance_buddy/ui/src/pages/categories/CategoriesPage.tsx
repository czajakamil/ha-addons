// Ekran Kategorie: drzewo z kwotami gałęzi, wyszukiwarką, przeciąganiem i akcjami w „⋯”.
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { toast } from 'sonner';
import { Plus, Search, TagsIcon } from 'lucide-react';
import {
  type Category,
  deleteCategory,
  ensureCategoryPath,
  getAllCategories,
  getCategoryStats,
  reorderCategories,
  updateCategory
} from '../../api';
import Button from '../../components/Button';
import TagPathInput from '../../components/tags/TagPathInput';
import { buildCategoryMap } from '../../components/tags/tagColors';
import EmptyState from '../../components/ui/EmptyState';
import PageHeader from '../../components/ui/PageHeader';
import SegmentedControl from '../../components/ui/SegmentedControl';
import SkeletonRows from '../../components/ui/SkeletonRows';
import { copy } from '../../copy/pl';
import { useApi } from '../../hooks/useApi';
import { errorDetail } from '../stores/apiError';
import CategoryTreeTable from './components/CategoryTreeTable';
import MoveCategoryDialog from './components/MoveCategoryDialog';
import {
  buildCategoryTree,
  countUnused,
  flattenVisible,
  moveTargets,
  planMove,
  planShift,
  toStatsMap,
  walkTree,
  type DropPosition,
  type MovePlan,
  type VisibleRow
} from './model';

const labels = copy.categories;

type Scope = 'spending' | 'accounts';

const CategoriesPage = () => {
  const [scope, setScope] = useState<Scope>('spending');
  const forAccounts = scope === 'accounts';
  const categoriesRequest = useApi(() => getAllCategories(200, { for_accounts: forAccounts }), {
    errorMessage: labels.loadFailed
  });
  const statsRequest = useApi(getCategoryStats, { errorMessage: labels.statsFailed });
  const { refetch, setData } = categoriesRequest;
  const categories = useMemo(() => categoriesRequest.data ?? [], [categoriesRequest.data]);
  const hasLoaded = categoriesRequest.data !== undefined;

  const [expanded, setExpanded] = useState<ReadonlySet<string>>(new Set());
  const [query, setQuery] = useState('');
  const [showUnused, setShowUnused] = useState(false);
  // Świeżo dodane kategorie nie mają transakcji, a nie mogą zniknąć zaraz po dodaniu
  const [keep, setKeep] = useState<ReadonlySet<string>>(new Set());
  const [renamingId, setRenamingId] = useState<string | null>(null);
  const [moving, setMoving] = useState<Category | null>(null);
  const [newPath, setNewPath] = useState<string | null>(null);
  const [creating, setCreating] = useState(false);

  const firstScopeRender = useRef(true);
  useEffect(() => {
    if (firstScopeRender.current) {
      firstScopeRender.current = false;
      return;
    }
    setExpanded(new Set());
    setNewPath(null);
    setData(undefined); // bez drzewa z poprzedniego zakresu
    void refetch();
  }, [scope, refetch, setData]);

  const stats = useMemo(() => (statsRequest.data ? toStatsMap(statsRequest.data) : null), [statsRequest.data]);
  const tree = useMemo(() => buildCategoryTree(categories, stats), [categories, stats]);
  const categoryMap = useMemo(() => buildCategoryMap(categories), [categories]);
  const hideUnused = !showUnused && stats !== null;
  const rows = useMemo(
    () => flattenVisible(tree, { expanded, query, hideUnused, keep }),
    [tree, expanded, query, hideUnused, keep]
  );
  const unusedCount = useMemo(() => (stats ? countUnused(tree) : 0), [tree, stats]);

  const toggle = useCallback(
    (id: string) =>
      setExpanded((current) => {
        const next = new Set(current);
        if (!next.delete(id)) next.add(id);
        return next;
      }),
    []
  );
  const expandAll = () => {
    const ids = new Set<string>();
    walkTree(tree, (node) => node.children.length && ids.add(node.category.id));
    setExpanded(ids);
  };

  const reload = useCallback(async () => {
    await Promise.all([refetch(), statsRequest.refetch()]);
  }, [refetch, statsRequest.refetch]); // eslint-disable-line react-hooks/exhaustive-deps

  const applyPlan = async (plan: MovePlan, name: string, silent = false) => {
    try {
      await reorderCategories(plan.parentId, plan.orderedIds);
      if (plan.parentId) setExpanded((current) => new Set(current).add(plan.parentId as string));
      await refetch();
      if (!silent) toast(labels.moved(name));
    } catch (err) {
      console.error(err);
      toast.error(errorDetail(err) ?? labels.moveFailed);
    }
  };

  const handleDrop = (draggedId: string, targetId: string, position: DropPosition) => {
    const plan = planMove(tree, draggedId, targetId, position);
    const dragged = categoryMap.get(draggedId);
    if (plan && dragged) void applyPlan(plan, dragged.name, (dragged.parent_id ?? null) === plan.parentId);
  };

  const handleShift = (category: Category, delta: -1 | 1) => {
    const plan = planShift(tree, category.id, delta);
    if (plan) void applyPlan(plan, category.name, true);
  };

  const handleRename = async (category: Category, name: string) => {
    try {
      const updated = await updateCategory(category.id, { name });
      setData((current) => current?.map((entry) => (entry.id === updated.id ? updated : entry)));
      await refetch(); // ścieżki potomków też się zmieniły
    } catch (err) {
      console.error(err);
      toast.error(errorDetail(err) ?? labels.saveFailed);
    }
  };

  const handleMove = async (category: Category, parentId: string | null) => {
    try {
      await updateCategory(category.id, { parent_id: parentId });
      if (parentId) setExpanded((current) => new Set(current).add(parentId));
      setMoving(null);
      await refetch();
      toast(labels.moved(category.name));
    } catch (err) {
      console.error(err);
      toast.error(errorDetail(err) ?? labels.moveFailed);
    }
  };

  const handleDelete = async (row: VisibleRow) => {
    let descendants = 0;
    walkTree(row.children, () => (descendants += 1));
    if (!window.confirm(labels.deleteConfirm(row.category.name, descendants))) return;
    try {
      await deleteCategory(row.category.id);
      await reload();
      toast(labels.deleted(row.category.name));
    } catch (err) {
      console.error(err);
      toast.error(errorDetail(err) ?? labels.deleteFailed);
    }
  };

  const handleCreate = async () => {
    const path = (newPath ?? '').trim().replace(/\/+$/, '');
    if (!path || creating) return;
    setCreating(true);
    try {
      const id = await ensureCategoryPath(path, categories, forAccounts);
      const fresh = (await refetch()) ?? [];
      const created = fresh.find((entry) => entry.id === id);
      // Rozwijamy przodków, żeby nowa kategoria była od razu widoczna
      setExpanded((current) => {
        const next = new Set(current);
        let parent = created?.parent_id ? fresh.find((entry) => entry.id === created.parent_id) : undefined;
        while (parent) {
          next.add(parent.id);
          const parentId: string | null | undefined = parent.parent_id;
          parent = parentId ? fresh.find((entry) => entry.id === parentId) : undefined;
        }
        return next;
      });
      setKeep((current) => new Set(current).add(id));
      setNewPath(null);
      toast(labels.created(created?.name ?? path));
    } catch (err) {
      console.error(err);
      toast.error(errorDetail(err) ?? labels.createFailed);
    } finally {
      setCreating(false);
    }
  };

  const error = categoriesRequest.error;

  return (
    <div className="page-shell categories-screen">
      <PageHeader
        title={labels.title}
        count={hasLoaded ? categories.length : undefined}
        actions={
          <Button icon={<Plus size={16} aria-hidden="true" />} onClick={() => setNewPath((current) => current ?? '')}>
            {labels.newCategory}
          </Button>
        }
      />

      <div className="categories-toolbar">
        <SegmentedControl
          label={labels.scopeLabel}
          value={scope}
          options={[
            { value: 'spending', label: labels.scopeSpending },
            { value: 'accounts', label: labels.scopeAccounts }
          ]}
          onChange={setScope}
        />
        <div className="accounts-toolbar__field accounts-toolbar__field--search categories-toolbar__search">
          <div className="input-icon">
            <span className="input-icon-addon">
              <Search size={16} aria-hidden="true" />
            </span>
            <input
              type="search"
              className="form-control form-control-sm accounts-search"
              placeholder={labels.search}
              aria-label={labels.searchLabel}
              value={query}
              onChange={(event) => setQuery(event.target.value)}
            />
          </div>
        </div>
        {!query && (
          <span className="categories-toolbar__tree-actions">
            <button type="button" className="text-button" onClick={expandAll}>
              {labels.expandAll}
            </button>
            <button type="button" className="text-button" onClick={() => setExpanded(new Set())}>
              {labels.collapseAll}
            </button>
          </span>
        )}
      </div>

      {newPath !== null && (
        <form
          className="categories-new"
          onSubmit={(event) => {
            event.preventDefault();
            void handleCreate();
          }}
        >
          <label className="sr-only" htmlFor="categories-new-path">
            {labels.newLabel}
          </label>
          <div className="categories-new__input">
            <TagPathInput
              inputId="categories-new-path"
              value={newPath}
              categories={categories}
              forAccounts={forAccounts}
              placeholder={labels.newPlaceholder}
              disabled={creating}
              autoFocus
              commitOnBlur={false}
              onChange={setNewPath}
              onCommit={() => void handleCreate()}
              onCancel={() => setNewPath(null)}
            />
          </div>
          <button type="submit" className="account-button account-button--primary" disabled={creating || !newPath.trim()}>
            {labels.add}
          </button>
          <button type="button" className="account-button" onClick={() => setNewPath(null)}>
            {labels.cancel}
          </button>
        </form>
      )}

      {error && <p className="accounts-error" role="alert">{error}</p>}
      {statsRequest.error && <p className="accounts-error" role="alert">{statsRequest.error}</p>}

      {!hasLoaded && categoriesRequest.loading && <SkeletonRows rows={8} />}
      {hasLoaded && !error && !categories.length && <EmptyState icon={TagsIcon} message={labels.empty} />}
      {hasLoaded && !error && categories.length > 0 && (
        <div className="categories-list">
          {rows.length ? (
            <CategoryTreeTable
              rows={rows}
              categoryMap={categoryMap}
              statsLoaded={stats !== null}
              renamingId={renamingId}
              onRenameStart={setRenamingId}
              onRename={(category, name) => void handleRename(category, name)}
              onToggle={toggle}
              onAddChild={(category) => setNewPath(`${category.path}/`)}
              onMove={setMoving}
              onShift={handleShift}
              onDelete={(row) => void handleDelete(row)}
              onDrop={handleDrop}
            />
          ) : (
            <EmptyState icon={Search} message={labels.noMatches} />
          )}
          {!query && unusedCount > 0 && (
            <button type="button" className="text-button categories-list__unused" onClick={() => setShowUnused((value) => !value)}>
              {showUnused ? labels.hideUnused : labels.showUnused(unusedCount)}
            </button>
          )}
          <p className="categories-list__footnote">{labels.footnote}</p>
        </div>
      )}

      {moving && (
        <MoveCategoryDialog
          category={moving}
          targets={moveTargets(tree, moving.id)}
          onClose={() => setMoving(null)}
          onConfirm={(parentId) => handleMove(moving, parentId)}
        />
      )}
    </div>
  );
};

export default CategoriesPage;
