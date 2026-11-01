// Komórka tabeli z tagiem: podgląd ścieżki albo edytor TagPathInput z metą
// „Zapisywanie… / Enter – zapisz, Esc – anuluj”. Czysto prezentacyjna — stan
// i zapis dostarcza useTagCellEditing (albo strona).
import { useMemo } from 'react';
import type { Category } from '../../api';
import TagPathInput from './TagPathInput';
import { TagChip } from './TagChip';
import { buildCategoryMap } from './tagColors';

type TagEditCellProps = {
  isEditing: boolean;
  isSaving: boolean;
  /** Ścieżka/etykieta tagu w trybie podglądu ('' gdy brak tagu). */
  displayValue: string;
  /** Id przypisanego tagu — do pokolorowania chipu w podglądzie (opcjonalne). */
  categoryId?: string | null;
  editValue: string;
  categories: Category[];
  /** Podpowiedzi tylko tagów kont (true) albo tagów wydatków (false). */
  forAccounts: boolean;
  placeholder: string;
  onChange: (value: string) => void;
  onCommit: (value: string) => void;
  onCancel: () => void;
};

const TagEditCell = ({
  isEditing,
  isSaving,
  displayValue,
  categoryId,
  editValue,
  categories,
  forAccounts,
  placeholder,
  onChange,
  onCommit,
  onCancel
}: TagEditCellProps) => {
  const cellClass = ['editable-cell', 'align-middle', isEditing ? 'is-editing' : '']
    .filter(Boolean)
    .join(' ');

  const byId = useMemo(() => buildCategoryMap(categories), [categories]);

  if (isEditing) {
    return (
      <div className={cellClass}>
        <TagPathInput
          value={editValue}
          categories={categories}
          forAccounts={forAccounts}
          disabled={isSaving}
          placeholder={placeholder}
          autoFocus
          onChange={onChange}
          onCommit={onCommit}
          onCancel={onCancel}
        />
        <div className="editable-cell__meta">
          {isSaving ? 'Zapisywanie…' : 'Enter – zapisz, Esc – anuluj'}
        </div>
      </div>
    );
  }

  const category = categoryId ? byId.get(categoryId) : undefined;

  // Mamy dopasowany tag → kolorowy chip (jak w transakcjach), z pełną ścieżką jako etykietą.
  if (category) {
    return (
      <div className={cellClass} title={displayValue || category.path}>
        <TagChip category={category} byId={byId} label={displayValue || category.name} />
      </div>
    );
  }

  return (
    <div className={cellClass} title={displayValue || 'Dwuklik, aby edytować'}>
      <span className="editable-cell__value editable-cell__value--placeholder">
        {displayValue || '—'}
      </span>
    </div>
  );
};

export default TagEditCell;
