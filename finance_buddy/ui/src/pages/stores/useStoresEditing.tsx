// Edycja inline komórek tabeli sklepów (dwuklik → input, Enter/blur → zapis).
// Trzyma stan edytowanej komórki i pojedynczy komunikat błędu strony.
import { useCallback, useRef, useState } from 'react';
import type { ChangeEvent, Dispatch, KeyboardEvent as ReactKeyboardEvent, SetStateAction } from 'react';
import { Category, Store, StoreUpdate, updateStore } from '../../api';
import TagEditCell from '../../components/tags/TagEditCell';
import { useTagCellEditing } from '../../components/tags/useTagCellEditing';
import {
  GRID_NAVIGATION_KEYS,
  captureGridCellAnchor,
  refocusGridCell
} from '../../utils/gridFocus';
import { isValidNip } from '../../utils/nip';
import {
  GROUP_NAME_DATALIST_ID,
  getFieldDisplayValue,
  getFieldInitialValue,
  type EditableField,
  type EditingCell
} from './model';

type UseStoresEditingParams = {
  stores: Store[];
  setStores: Dispatch<SetStateAction<Store[] | undefined>>;
  categories: Category[];
  /** Ścieżka tagu przypisanego do sklepu (albo '' gdy brak). */
  getCategoryPath: (categoryId?: string | null) => string;
  /** Zamienia wpisaną ścieżkę tagu na id, tworząc brakujące tagi. */
  ensureCategory: (path: string) => Promise<string>;
};

export const useStoresEditing = ({
  stores,
  setStores,
  categories,
  getCategoryPath,
  ensureCategory
}: UseStoresEditingParams) => {
  const [editingCell, setEditingCell] = useState<EditingCell | null>(null);
  const [savingCell, setSavingCell] = useState<EditingCell | null>(null);
  const [editValue, setEditValue] = useState('');
  const [inlineError, setInlineError] = useState<string | null>(null);
  const skipNextCommitRef = useRef(false);

  const tagEditing = useTagCellEditing<Store>({
    entities: stores,
    getCurrentCategoryId: (store) => store.automatic_category_id ?? null,
    getInitialValue: (store) => getCategoryPath(store.automatic_category_id),
    resolveCategoryId: ensureCategory,
    save: async (store, nextCategoryId) => {
      // Ustawienie tagu włącza auto-tagowanie payee — tak samo jak modal edycji sklepu.
      const payload: StoreUpdate = { automatic_category_id: nextCategoryId };
      if (nextCategoryId && !store.automatically_categorize_payee) {
        payload.automatically_categorize_payee = true;
      }
      const updated = await updateStore(store.id, payload);
      setStores((prev) => (prev ?? []).map((item) => (item.id === updated.id ? updated : item)));
    },
    setError: setInlineError,
    errorMessages: {
      resolve: 'Nie udało się utworzyć tagu sklepu.',
      save: 'Nie udało się zaktualizować tagu sklepu.'
    }
  });

  const startEditing = useCallback(
    (store: Store, field: EditableField) => {
      if (savingCell) return;
      skipNextCommitRef.current = false;
      const currentValue = getFieldInitialValue(store, field);
      setEditingCell({ storeId: store.id, field });
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

    const store = stores.find((item) => item.id === editingCell.storeId);
    if (!store) return;

    const field = editingCell.field;
    const rawValue = overrideRawValue ?? editValue;
    const nextValue = field === 'comment' ? rawValue : rawValue.trim();

    if (field === 'name' && nextValue.length < 2) {
      setInlineError('Nazwa sklepu musi mieć co najmniej 2 znaki.');
      return;
    }

    if (field === 'nip' && nextValue.length && !isValidNip(nextValue)) {
      setInlineError('Nieprawidłowy NIP (błędna suma kontrolna).');
      return;
    }

    const currentValue = getFieldInitialValue(store, field);

    if (currentValue === nextValue) {
      cancelEditing();
      return;
    }

    const payload: StoreUpdate = {};
    if (field === 'name') {
      payload.name = nextValue;
    } else if (field === 'comment') {
      payload.comment = nextValue.length ? nextValue : null;
    } else if (field === 'address') {
      payload.address = nextValue.length ? nextValue : null;
    } else if (field === 'group_name') {
      payload.group_name = nextValue.length ? nextValue : null;
    } else {
      payload.nip = nextValue.length ? nextValue : null;
    }

    setSavingCell(editingCell);
    try {
      const updated = await updateStore(store.id, payload);
      setStores((prev) => (prev ?? []).map((item) => (item.id === updated.id ? updated : item)));
      setInlineError(null);
      cancelEditing();
    } catch (err) {
      console.error(err);
      setInlineError('Nie udało się zaktualizować danych sklepu.');
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

  const renderTagCell = (store: Store) => (
    <TagEditCell
      isEditing={tagEditing.editingId === store.id}
      isSaving={tagEditing.savingId === store.id}
      displayValue={getCategoryPath(store.automatic_category_id)}
      categoryId={store.automatic_category_id}
      editValue={tagEditing.editValue}
      categories={categories}
      forAccounts={false}
      placeholder="np. codzienne wydatki/spożywcze"
      onChange={tagEditing.setEditValue}
      onCommit={(value) => void tagEditing.commitEdit(value)}
      onCancel={tagEditing.cancelEditing}
    />
  );

  const renderCell = (store: Store, field: EditableField) => {
    const isEditing = editingCell?.storeId === store.id && editingCell.field === field;
    const isSaving = savingCell?.storeId === store.id && savingCell.field === field;
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

    const displayValue = getFieldDisplayValue(store, field);

    return (
      <div className={cellClass} title="Dwuklik, aby edytować">
        <span className="editable-cell__value">{displayValue}</span>
      </div>
    );
  };

  return {
    startEditing,
    startTagEditing: tagEditing.startEditing,
    tagEditingStoreId: tagEditing.editingId,
    tagSavingStoreId: tagEditing.savingId,
    renderCell,
    renderTagCell,
    inlineError,
    setInlineError
  };
};
