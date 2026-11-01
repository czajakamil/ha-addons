// Pasek akcji zbiorczych nad listą: pojawia się po zaznaczeniu wierszy.
import type { Category } from '../../../api';
import { copy } from '../../../copy/pl';

const labels = copy.transactions;

type TransactionsBulkBarProps = {
  count: number;
  categories: Category[];
  busy: boolean;
  onCategoryChange: (categoryId: string) => void;
  onApprove: () => void;
  onClear: () => void;
};

const TransactionsBulkBar = ({
  count,
  categories,
  busy,
  onCategoryChange,
  onApprove,
  onClear
}: TransactionsBulkBarProps) => (
  <div className="transactions-bulk" role="toolbar" aria-label={labels.selected(count)}>
    <span className="transactions-bulk__count">{labels.selected(count)}</span>
    <select
      className="form-select"
      aria-label={labels.bulkCategory}
      value=""
      disabled={busy}
      onChange={(event) => {
        if (event.target.value) onCategoryChange(event.target.value);
      }}
    >
      <option value="">{labels.bulkCategory}</option>
      {categories.map((category) => (
        <option key={category.id} value={category.id}>
          {category.path || category.name}
        </option>
      ))}
    </select>
    <button type="button" className="btn btn-primary" disabled={busy} onClick={onApprove}>
      {labels.bulkApprove}
    </button>
    <button type="button" className="text-button" disabled={busy} onClick={onClear}>
      {labels.bulkClear}
    </button>
  </div>
);

export default TransactionsBulkBar;
