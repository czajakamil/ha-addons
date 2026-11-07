// Lista wyboru kategorii nadrzędnej: pełne ścieżki w kolejności drzewa, na początku „brak”.
import type { Category } from '../../../api';
import { copy } from '../../../copy/pl';

const ROOT_VALUE = '';

type ParentSelectProps = {
  id: string;
  value: string | null;
  targets: Category[];
  disabled?: boolean;
  autoFocus?: boolean;
  onChange: (parentId: string | null) => void;
};

const ParentSelect = ({ id, value, targets, disabled, autoFocus, onChange }: ParentSelectProps) => (
  <select
    id={id}
    className="form-select"
    value={value ?? ROOT_VALUE}
    disabled={disabled}
    autoFocus={autoFocus}
    onChange={(event) => onChange(event.target.value || null)}
  >
    <option value={ROOT_VALUE}>{copy.categories.moveDialog.root}</option>
    {targets.map((target) => (
      <option key={target.id} value={target.id}>
        {target.path.split('/').join(' › ')}
      </option>
    ))}
  </select>
);

export default ParentSelect;
