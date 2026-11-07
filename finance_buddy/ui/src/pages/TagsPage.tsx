import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { ChevronDownIcon, ChevronRightIcon, PlusIcon, TagsIcon, Trash2Icon } from 'lucide-react';
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
import EmptyState from '../components/ui/EmptyState';
import PageHeader from '../components/ui/PageHeader';
import SegmentedControl from '../components/ui/SegmentedControl';
import SkeletonRows from '../components/ui/SkeletonRows';
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
        setError('Nie udało się utworzyć podkategorii — sprawdź, czy nie tworzy duplikatu.');
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
      setError('Nie udało się zmienić koloru kategorii.');
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

    const message = `Usunąć kategorię "${category.path}"? Zostanie usuniętych: ${preview.category_count} kategorii, odpięte: ${preview.product_count} produkt(ów) i ${preview.transaction_count} transakcji.`;
    if (!window.confirm(message)) {
      return;
    }

    setDeleting(true);
    try {
      await deleteCategory(category.id);
      onDeleted(category.id);
    } catch (err) {
      console.error(err);
      setError('Nie udało się usunąć kategorii.');
    } finally {
      setDeleting(false);
    }
  };

  return (
    <>
      <div className="tags-row" role="treeitem" aria-expanded={children.length ? isExpanded : undefined} style={{ paddingInlineStart: depth * 24 }}>
        <button
          type="button"
          className="tags-row__toggle"
          onClick={() => toggleExpanded(category.id)}
          disabled={!children.length}
          aria-label={isExpanded ? `Zwiń ${category.name}` : `Rozwiń ${category.name}`}
        >
          {children.length ? (
            isExpanded ? (
              <ChevronDownIcon size={14} aria-hidden="true" />
            ) : (
              <ChevronRightIcon size={14} aria-hidden="true" />
            )
          ) : null}
        </button>
        <TagColorPicker
          resolved={resolvedColor}
          ownColorId={category.color ?? null}
          canInherit={!!category.parent_id}
          disabled={savingColor}
          onSelect={(colorId) => void handleColorSelect(colorId)}
        />

        {editing ? (
          <div className="tags-row__edit">
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
          <button
            type="button"
            className={`tags-row__name${children.length ? ' tags-row__name--group' : ''}`}
            onClick={startEditing}
            title="Zmień nazwę lub przenieś"
          >
            {category.name}
          </button>
        )}

        {children.length > 0 && !editing && <span className="tags-row__count">{children.length}</span>}

        <button
          type="button"
          className="tags-row__delete"
          onClick={() => void handleDelete()}
          disabled={deleting}
          aria-label={`Usuń ${category.name}`}
          title="Usuń"
        >
          <Trash2Icon size={14} aria-hidden="true" />
        </button>
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
    { errorMessage: 'Nie udało się wczytać kategorii.' }
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
      setRowsError('Nie udało się utworzyć kategorii.');
    } finally {
      setCreating(false);
    }
  };

  return (
    <div className="page-shell page-stack-layout tags-page">
      <PageHeader title="Kategorie" count={hasLoaded ? categories.length : undefined} />

      <div className="tags-toolbar">
        <SegmentedControl
          label="Zakres kategorii"
          value={scope}
          options={[
            { value: 'spending', label: 'Wydatki' },
            { value: 'accounts', label: 'Konta' }
          ]}
          onChange={setScope}
        />
        <form
          className="tags-toolbar__add"
          onSubmit={(event) => {
            event.preventDefault();
            void handleCreateTag();
          }}
        >
          <TagPathInput
            value={newTagPath}
            categories={categories}
            forAccounts={forAccounts}
            placeholder="Nowa kategoria, np. dom/remont"
            disabled={creating}
            onChange={setNewTagPath}
            onCommit={() => void handleCreateTag()}
          />
          <Button
            type="submit"
            icon={<PlusIcon size={16} aria-hidden="true" />}
            disabled={creating || !newTagPath.trim()}
          >
            Dodaj
          </Button>
        </form>
      </div>

      {(error || rowsError) && (
        <p className="transactions-error" role="alert">
          {rowsError ?? error}
        </p>
      )}

      <section className="tags-card">
        {loading && !hasLoaded && <SkeletonRows rows={8} />}
        {hasLoaded && !error && !tree.length && (
          <EmptyState icon={TagsIcon} message="Nie masz jeszcze kategorii. Dodaj pierwszą powyżej." />
        )}
        {hasLoaded && !error && tree.length > 0 && (
          <>
            <div className="tags-card__hint">
              Kliknij nazwę, aby ją zmienić. Pełna ścieżka, np. „dom/remont”, przenosi kategorię do innej gałęzi.
            </div>
            <div role="tree" aria-label="Drzewo kategorii">
              {tree.map((node) => (
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
              ))}
            </div>
          </>
        )}
      </section>
    </div>
  );
};

export default TagsPage;
