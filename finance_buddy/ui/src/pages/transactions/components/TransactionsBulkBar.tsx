// Pasek akcji zbiorczych nad listą: pojawia się po zaznaczeniu wierszy.
import { useMemo } from 'react';
import type { Category } from '../../../api';
import CategoryPickerMenu from '../../../components/CategoryPickerMenu';
import { copy } from '../../../copy/pl';
import { buildFilterTree, categoryFilterOptions } from '../categoryFilter';

const labels = copy.transactions;

type TransactionsBulkBarProps = {
  count: number;
  categories: Category[];
  busy: boolean;
  progress: string | null;
  onCategoryChange: (categoryId: string) => void;
  onApprove: () => void;
  onClear: () => void;
};

const TransactionsBulkBar = ({
  count,
  categories,
  busy,
  progress,
  onCategoryChange,
  onApprove,
  onClear
}: TransactionsBulkBarProps) => {
  const tree = useMemo(() => buildFilterTree(categories), [categories]);
  return (
    <div className="transactions-bulk" role="toolbar" aria-label={labels.selected(count)}>
      <span className="transactions-bulk__count" aria-live="polite">
        {progress ?? labels.selected(count)}
      </span>
      {/* Drzewo z wyszukiwarką zamiast długiej listy ścieżek; „Bez kategorii” tu nie ma sensu */}
      <CategoryPickerMenu
        id="transactions-bulk-category"
        label={labels.bulkCategory}
        triggerClassName="transactions-filters__menu-trigger"
        triggerLabel={labels.bulkCategory}
        searchPlaceholder={labels.searchCategories}
        emptyText={labels.noCategoriesFound}
        disabled={busy}
        options={(search) => categoryFilterOptions(tree, search, [], null)}
        onSelect={onCategoryChange}
      />
      <button type="button" className="btn btn-primary" disabled={busy} onClick={onApprove}>
        {labels.bulkApprove}
      </button>
      <button type="button" className="text-button" disabled={busy} onClick={onClear}>
        {labels.bulkClear}
      </button>
    </div>
  );
};

export default TransactionsBulkBar;
