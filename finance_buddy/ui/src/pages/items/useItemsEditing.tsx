// Edycja inline komórek tabeli przedmiotów (dwuklik → input, Enter/blur → zapis).
// Trzyma stan edytowanej komórki i pojedynczy komunikat błędu strony.
import { useCallback, useRef, useState } from 'react';
import type { ChangeEvent, Dispatch, KeyboardEvent as ReactKeyboardEvent, SetStateAction } from 'react';
import { Category, Item, ItemUpdate, updateItem } from '../../api';
import TagEditCell from '../../components/tags/TagEditCell';
import { useTagCellEditing } from '../../components/tags/useTagCellEditing';
import {
  GRID_NAVIGATION_KEYS,
  captureGridCellAnchor,
  refocusGridCell
} from '../../utils/gridFocus';
import {
  GROUP_NAME_DATALIST_ID,
  getFieldDisplayValue,
  getFieldInitialValue,
  type EditableField,
  type EditingCell
} from './model';

type UseItemsEditingParams = {
  items: Item[];
  setItems: Dispatch<SetStateAction<Item[] | undefined>>;
  categories: Category[];
  /** Ścieżka tagu przypisanego do przedmiotu (albo '' gdy brak). */
  getCategoryPath: (categoryId?: string | null) => string;
  /** Zamienia wpisaną ścieżkę tagu na id, tworząc brakujące tagi. */
  ensureCategory: (path: string) => Promise<string>;
};

export const useItemsEditing = ({
  items,
  setItems,
  categories,
  getCategoryPath,
  ensureCategory
}: UseItemsEditingParams) => {
  const [editingCell, setEditingCell] = useState<EditingCell | null>(null);
  const [savingCell, setSavingCell] = useState<EditingCell | null>(null);
  const [editValue, setEditValue] = useState('');
  const [inlineError, setInlineError] = useState<string | null>(null);
  const skipNextCommitRef = useRef(false);

  const tagEditing = useTagCellEditing<Item>({
    entities: items,
    getCurrentCategoryId: (item) => item.category_id ?? null,
    getInitialValue: (item) => getCategoryPath(item.category_id),
    resolveCategoryId: ensureCategory,
    save: async (item, nextCategoryId) => {
      const updated = await updateItem(item.id, { category_id: nextCategoryId });
      setItems((prev) => (prev ?? []).map((entry) => (entry.id === updated.id ? updated : entry)));
    },
    setError: setInlineError,
    errorMessages: {
      resolve: 'Nie udało się utworzyć tagu.',
      save: 'Nie udało się zaktualizować przedmiotu.'
    }
  });

  const startEditing = useCallback(
    (item: Item, field: EditableField) => {
      if (savingCell) return;
      skipNextCommitRef.current = false;
      const currentValue = getFieldInitialValue(item, field);
      setEditingCell({ itemId: item.id, field });
      setEditValue(currentValue);
      setInlineError(null);
    },
    [savingCell]
  );

  const cancelEditing = () => {
    skipNextCommitRef.current = true;
    setEditingCell(null);
    setEditValue('');
  };

  const commitEdit = async (overrideRawValue?: string) => {
    if (skipNextCommitRef.current) {
      skipNextCommitRef.current = false;
      return;
    }
    if (!editingCell || savingCell) return;

    const item = items.find((entry) => entry.id === editingCell.itemId);
    if (!item) return;

    const field = editingCell.field;
    const rawValue = overrideRawValue ?? editValue;
    const nextValue = field === 'comment' ? rawValue : rawValue.trim();

    if (field === 'name' && nextValue.length < 2) {
      setInlineError('Nazwa przedmiotu musi mieć co najmniej 2 znaki.');
      return;
    }

    const currentValue = getFieldInitialValue(item, field);
    if (currentValue === nextValue) {
      cancelEditing();
      return;
    }

    const payload: ItemUpdate = {};
    if (field === 'name') {
      payload.name = nextValue;
    } else if (field === 'group_name') {
      payload.group_name = nextValue.length ? nextValue : null;
    } else if (field === 'unit_of_measurement') {
      payload.unit_of_measurement = nextValue.length ? nextValue : null;
    } else if (field === 'manufacturer') {
      payload.manufacturer = nextValue.length ? nextValue : null;
    } else {
      payload.comment = nextValue.length ? nextValue : null;
    }

    setSavingCell(editingCell);
    try {
      const updated = await updateItem(item.id, payload);
      setItems((prev) => (prev ?? []).map((entry) => (entry.id === updated.id ? updated : entry)));
      setInlineError(null);
      cancelEditing();
    } catch (err) {
      console.error(err);
      setInlineError('Nie udało się zaktualizować przedmiotu.');
    } finally {
      setSavingCell(null);
    }
  };

  const handleKeyDown = (event: ReactKeyboardEvent<HTMLInputElement | HTMLTextAreaElement>, field: EditableField) => {
    if (event.key === 'Escape') {
      event.preventDefault();
      event.stopPropagation();
      const anchor = captureGridCellAnchor(event.target);
      cancelEditing();
      refocusGridCell(anchor);
      return;
    }
    if (event.key === 'Enter') {
      // Kotwica przed commitem — po nim edytor jest odmontowany i fokus
      // spadłby na <body>, wyłączając nawigację klawiaturą w tabeli.
      const anchor = captureGridCellAnchor(event.target);
      if (field === 'group_name') {
        const input = event.currentTarget as HTMLInputElement;
        window.setTimeout(() => {
          void commitEdit(input.value).then(() => refocusGridCell(anchor));
        }, 0);
        return;
      }
      if (field === 'comment' && !(event.metaKey || event.ctrlKey)) {
        return;
      }
      event.preventDefault();
      event.stopPropagation();
      void commitEdit().then(() => refocusGridCell(anchor));
      return;
    }
    if (GRID_NAVIGATION_KEYS.has(event.key)) {
      // W trybie edycji klawisze nawigacji obsługuje pole, nie siatka.
      event.stopPropagation();
    }
  };

  const renderTagCell = (item: Item) => (
    <TagEditCell
      isEditing={tagEditing.editingId === item.id}
      isSaving={tagEditing.savingId === item.id}
      displayValue={getCategoryPath(item.category_id)}
      categoryId={item.category_id}
      editValue={tagEditing.editValue}
      categories={categories}
      forAccounts={false}
      placeholder="np. dom/remont"
      onChange={tagEditing.setEditValue}
      onCommit={(value) => void tagEditing.commitEdit(value)}
      onCancel={tagEditing.cancelEditing}
    />
  );

  const renderCell = (item: Item, field: EditableField) => {
    const isEditing = editingCell?.itemId === item.id && editingCell.field === field;
    const isSaving = savingCell?.itemId === item.id && savingCell.field === field;
    const cellClass = ['editable-cell', 'align-middle', isEditing ? 'is-editing' : ''].filter(Boolean).join(' ');

    if (isEditing) {
      const commonProps = {
        autoFocus: true,
        value: editValue,
        disabled: isSaving,
        className: 'form-control form-control-sm',
        onChange: (event: ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => setEditValue(event.target.value),
        onBlur: () => void commitEdit(),
        onKeyDown: (event: ReactKeyboardEvent<HTMLInputElement | HTMLTextAreaElement>) => handleKeyDown(event, field)
      };

      return (
        <div className={cellClass}>
          {field === 'comment' ? (
            <textarea rows={3} {...commonProps} />
          ) : (
            <input
              type="text"
              {...commonProps}
              list={field === 'group_name' ? GROUP_NAME_DATALIST_ID : undefined}
            />
          )}
          <div className="editable-cell__meta">
            {isSaving ? 'Zapisywanie…' : field === 'comment' ? 'Cmd/Ctrl + Enter aby zapisać' : 'Enter aby zapisać'}
          </div>
        </div>
      );
    }

    const displayValue = getFieldDisplayValue(item, field);

    return (
      <div className={cellClass} title="Dwuklik, aby edytować">
        <span className="editable-cell__value">{displayValue}</span>
      </div>
    );
  };

  return {
    startEditing,
    startTagEditing: tagEditing.startEditing,
    tagEditingItemId: tagEditing.editingId,
    tagSavingItemId: tagEditing.savingId,
    renderCell,
    renderTagCell,
    inlineError,
    setInlineError
  };
};
