import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { ChevronDownIcon, ChevronRightIcon, CircleAlertIcon, PlusIcon, Trash2Icon } from 'lucide-react';
import {
  type Category,
  type CategoryDeletePreview,
  deleteCategory,
  ensureCategoryPath,
  getAllCategories,
  getCategoryDeletePreview,
  updateCategory
} from '../api';
import Button from '../components/Button';
import TagPathInput from '../components/tags/TagPathInput';
import TagColorPicker from '../components/tags/TagColorPicker';
import { buildCategoryMap, resolveTagColor } from '../components/tags/tagColors';
import { useApi } from '../hooks/useApi';

type TagTreeNode = {
  category: Category;
  children: TagTreeNode[];
};

const buildTree = (categories: Category[]): TagTreeNode[] => {
  const byParent = new Map<string | null, Category[]>();
  categories.forEach((category) => {
    const key = category.parent_id ?? null;
    const bucket = byParent.get(key) ?? [];
    bucket.push(category);
    byParent.set(key, bucket);
  });

  const sortByName = (a: Category, b: Category) => a.name.localeCompare(b.name, 'pl');
  Array.from(byParent.values()).forEach((bucket) => bucket.sort(sortByName));

  const build = (parentId: string | null): TagTreeNode[] =>
    (byParent.get(parentId) ?? []).map((category) => ({
      category,
      children: build(category.id)
    }));

  return build(null);
};

type TagRowProps = {
  node: TagTreeNode;
  depth: number;
  categories: Category[];
  categoryMap: Map<string, Category>;
  forAccounts: boolean;
  expanded: Set<string>;
  toggleExpanded: (id: string) => void;
  onSaved: (updated: Category) => void;
  onDeleted: (id: string) => void;
  setError: (message: string | null) => void;
};

const TagRow = ({
  node,
  depth,
  categories,
  categoryMap,
  forAccounts,
  expanded,
  toggleExpanded,
  onSaved,
  onDeleted,
  setError
}: TagRowProps) => {
  const { category, children } = node;
  const [editing, setEditing] = useState(false);
  const [editValue, setEditValue] = useState(category.path);
  const [saving, setSaving] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [savingColor, setSavingColor] = useState(false);
  const isExpanded = expanded.has(category.id);
  const resolvedColor = resolveTagColor(category, categoryMap);

  const startEditing = () => {
    setEditValue(category.path);
    setEditing(true);
    setError(null);
  };

  const cancelEditing = () => {
    setEditing(false);
    setEditValue(category.path);
  };

  const commitEditing = async (rawPath: string) => {
    const trimmed = rawPath.trim();
    if (!trimmed || trimmed === category.path) {
      cancelEditing();
      return;
    }

    const segments = trimmed.split('/').map((segment) => segment.trim()).filter(Boolean);
    if (!segments.length) {
      cancelEditing();
      return;
    }
    const normalizedPath = segments.join('/');

    // Dopisanie segmentu do własnej ścieżki (np. "…/Wakacje" → "…/Wakacje/Egipt")
    // to tworzenie NOWEGO tagu potomnego, a nie przenoszenie węzła w samego siebie.
    if (normalizedPath.toLowerCase().startsWith(category.path.toLowerCase() + '/')) {
      setSaving(true);
      try {
        await ensureCategoryPath(normalizedPath, categories, forAccounts);
        setEditing(false);
        if (!expanded.has(category.id)) toggleExpanded(category.id);
        onSaved(category);
      } catch (err) {
        console.error(err);
        setError('Nie udało się utworzyć tagu potomnego — sprawdź, czy nie tworzy duplikatu.');
      } finally {
        setSaving(false);
      }
      return;
    }

    const newName = segments[segments.length - 1];
    const parentPath = segments.slice(0, -1).join('/');

    setSaving(true);
    try {
      let newParentId: string | null = category.parent_id ?? null;
      if (parentPath) {
        newParentId = await ensureCategoryPath(parentPath, categories, forAccounts);
      } else {
        newParentId = null;
      }

      const payload: { name?: string; parent_id?: string | null } = {};
      if (newName !== category.name) payload.name = newName;
      if (newParentId !== (category.parent_id ?? null)) payload.parent_id = newParentId;

      if (Object.keys(payload).length === 0) {
        cancelEditing();
        return;
      }

      const updated = await updateCategory(category.id, payload);
      onSaved(updated);
      setEditing(false);
    } catch (err) {
      console.error(err);
      setError('Nie udało się zapisać zmiany — sprawdź, czy ścieżka nie tworzy duplikatu lub cyklu.');
    } finally {
      setSaving(false);
    }
  };

  const handleColorSelect = async (colorId: string | null) => {
    if (colorId === (category.color ?? null)) return;
    setSavingColor(true);
    setError(null);
    try {
      const updated = await updateCategory(category.id, { color: colorId });
      onSaved(updated);
    } catch (err) {
      console.error(err);
      setError('Nie udało się zmienić koloru tagu.');
    } finally {
      setSavingColor(false);
    }
  };

  const handleDelete = async () => {
    setError(null);
    let preview: CategoryDeletePreview;
    try {
      preview = await getCategoryDeletePreview(category.id);
    } catch (err) {
      console.error(err);
      setError('Nie udało się sprawdzić skutków usunięcia.');
      return;
    }

    const message = `Usunąć tag "${category.path}"? Zostanie usuniętych: ${preview.category_count} tag(ów), odpięte: ${preview.product_count} produkt(ów) i ${preview.transaction_count} transakcji.`;
    if (!window.confirm(message)) {
      return;
    }

    setDeleting(true);
    try {
      await deleteCategory(category.id);
      onDeleted(category.id);
    } catch (err) {
      console.error(err);
      setError('Nie udało się usunąć tagu.');
    } finally {
      setDeleting(false);
    }
  };

  return (
    <>
      <div className="tags-tree__row" style={{ paddingLeft: depth * 20 }}>
        <button
          type="button"
          className="tags-tree__toggle"
          onClick={() => toggleExpanded(category.id)}
          disabled={!children.length}
          aria-label={isExpanded ? 'Zwiń' : 'Rozwiń'}
        >
          {children.length ? (
            isExpanded ? (
              <ChevronDownIcon size={14} aria-hidden="true" />
            ) : (
              <ChevronRightIcon size={14} aria-hidden="true" />
            )
          ) : (
            <span className="tags-tree__toggle-spacer" />
          )}
        </button>
        <TagColorPicker
          resolved={resolvedColor}
          ownColorId={category.color ?? null}
          canInherit={!!category.parent_id}
          disabled={savingColor}
          onSelect={(colorId) => void handleColorSelect(colorId)}
        />

        {editing ? (
          <div className="tags-tree__edit">
            <TagPathInput
              value={editValue}
              categories={categories}
              forAccounts={forAccounts}
              disabled={saving}
              autoFocus
              onChange={setEditValue}
              onCommit={(value) => void commitEditing(value)}
              onCancel={cancelEditing}
            />
          </div>
        ) : (
          <button type="button" className="tags-tree__name" onClick={startEditing} title="Kliknij, aby zmienić nazwę lub przenieść">
            {category.name}
          </button>
        )}

        <div className="tags-tree__actions">
          <button
            type="button"
            className="btn btn-outline-danger btn-sm is-focusable"
            onClick={() => void handleDelete()}
            disabled={deleting}
          >
            <Trash2Icon size={14} aria-hidden="true" />
          </button>
        </div>
      </div>

      {isExpanded &&
        children.map((child) => (
          <TagRow
            key={child.category.id}
            node={child}
            depth={depth + 1}
            categories={categories}
            categoryMap={categoryMap}
            forAccounts={forAccounts}
            expanded={expanded}
            toggleExpanded={toggleExpanded}
            onSaved={onSaved}
            onDeleted={onDeleted}
            setError={setError}
          />
        ))}
    </>
  );
};

type TagScope = 'spending' | 'accounts';

const TagsPage = () => {
  const [scope, setScope] = useState<TagScope>('spending');
  const forAccounts = scope === 'accounts';

  const {
    data,
    loading,
    error,
    refetch,
    setData
  } = useApi(
    useCallback(() => getAllCategories(200, { for_accounts: forAccounts }), [forAccounts]),
    { errorMessage: 'Nie udało się wczytać tagów.' }
  );
  const categories = data ?? [];
  // Drzewo znika tylko przy pierwszym ładowaniu / zmianie zakresu (data === undefined).
  // Przy refetchu po zapisie zostaje na ekranie — inaczej każdy zapis (np. koloru)
  // odmontowywał wiersze i zamykał otwarty popover pickera.
  const hasLoaded = data !== undefined;

  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [rowsError, setRowsError] = useState<string | null>(null);
  const [newTagPath, setNewTagPath] = useState('');
  const [creating, setCreating] = useState(false);

  const isFirstScopeRenderRef = useRef(true);
  useEffect(() => {
    setExpanded(new Set());
    setRowsError(null);
    setNewTagPath('');
    if (isFirstScopeRenderRef.current) {
      isFirstScopeRenderRef.current = false;
      return;
    }
    setData(undefined); // pokaż placeholder zamiast drzewa z poprzedniego zakresu
    void refetch();
    // odświeżenie po zmianie zakresu — refetch celowo pominięty w deps (patrz useApi)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [scope]);

  const tree = useMemo(() => buildTree(categories), [categories]);
  const categoryMap = useMemo(() => buildCategoryMap(categories), [categories]);

  const toggleExpanded = useCallback((id: string) => {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(id)) {
        next.delete(id);
      } else {
        next.add(id);
      }
      return next;
    });
  }, []);

  const handleSaved = useCallback(() => {
    void refetch();
  }, [refetch]);

  const handleDeleted = useCallback(() => {
    void refetch();
  }, [refetch]);

  const handleCreateTag = async () => {
    const trimmed = newTagPath.trim();
    if (!trimmed) return;
    setCreating(true);
    setRowsError(null);
    try {
      await ensureCategoryPath(trimmed, categories, forAccounts);
      setNewTagPath('');
      await refetch();
    } catch (err) {
      console.error(err);
      setRowsError('Nie udało się utworzyć tagu.');
    } finally {
      setCreating(false);
    }
  };

  return (
    <div className="page compact-page">
      <div className="page-shell">
        <div className="page-header d-print-none">
          <div className="row align-items-center">
            <div className="col">
              <h2 className="page-title">Tagi</h2>
              <p className="text-secondary mb-0">
                Zarządzaj hierarchią tagów — zmiana nazwy, przenoszenie i usuwanie gałęzi drzewa.
              </p>
            </div>
          </div>
        </div>

        <div className="page-body page-stack">
          <div className="btn-group" role="group" aria-label="Zakres tagów">
            <button
              type="button"
              className={`btn btn-sm ${scope === 'spending' ? 'btn-primary' : 'btn-outline-primary'} is-focusable`}
              onClick={() => setScope('spending')}
            >
              Tagi wydatków
            </button>
            <button
              type="button"
              className={`btn btn-sm ${scope === 'accounts' ? 'btn-primary' : 'btn-outline-primary'} is-focusable`}
              onClick={() => setScope('accounts')}
            >
              Tagi kont
            </button>
          </div>

          <section className="card accounts-section-card">
            <div className="accounts-section-card__header">
              <div className="accounts-section-card__header-copy">
                <h3 className="accounts-section-card__title">Drzewo tagów</h3>
                <p className="accounts-section-card__hint">
                  Kliknij nazwę, aby zmienić ją lub przenieść (edytuj pełną ścieżkę, np. "dom/remont").
                </p>
              </div>
              <div className="d-flex gap-2 align-items-center">
                <div style={{ minWidth: 220 }}>
                  <TagPathInput
                    value={newTagPath}
                    categories={categories}
                    forAccounts={forAccounts}
                    placeholder="np. dom/remont/lazienka"
                    disabled={creating}
                    onChange={setNewTagPath}
                    onCommit={() => void handleCreateTag()}
                  />
                </div>
                <Button
                  icon={<PlusIcon size={16} aria-hidden="true" />}
                  onClick={() => void handleCreateTag()}
                  disabled={creating || !newTagPath.trim()}
                >
                  Dodaj tag
                </Button>
              </div>
            </div>

            {(error || rowsError) && (
              <div className="accounts-section-card__alert">
                <div className="alert alert-danger alert-important d-flex align-items-center gap-2 mb-0" role="alert">
                  <CircleAlertIcon size={18} aria-hidden="true" />
                  <div>{rowsError ?? error}</div>
                </div>
              </div>
            )}

            {loading && !hasLoaded && (
              <div className="accounts-section-card__body">
                <div className="placeholder-glow">
                  <div className="placeholder col-12 mb-2" />
                  <div className="placeholder col-10 mb-2" />
                  <div className="placeholder col-8 mb-0" />
                </div>
              </div>
            )}

            {hasLoaded && !error && (
              <div className="accounts-section-card__body tags-tree">
                {tree.length ? (
                  tree.map((node) => (
                    <TagRow
                      key={node.category.id}
                      node={node}
                      depth={0}
                      categories={categories}
                      categoryMap={categoryMap}
                      forAccounts={forAccounts}
                      expanded={expanded}
                      toggleExpanded={toggleExpanded}
                      onSaved={handleSaved}
                      onDeleted={handleDeleted}
                      setError={setRowsError}
                    />
                  ))
                ) : (
                  <div className="accounts-empty-state">
                    <div className="accounts-empty-state__title">Brak tagów</div>
                    <p className="accounts-empty-state__subtitle">Dodaj pierwszy tag korzystając z pola powyżej.</p>
                  </div>
                )}
              </div>
            )}
          </section>
        </div>
      </div>
    </div>
  );
};

export default TagsPage;
