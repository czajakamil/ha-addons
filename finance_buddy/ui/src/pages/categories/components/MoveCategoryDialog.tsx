// „Przenieś do…”: wybór nowej kategorii nadrzędnej spośród tych, które nie są w tej gałęzi.
import { useState } from 'react';
import type { Category } from '../../../api';
import { copy } from '../../../copy/pl';
import StoreDialog from '../../stores/components/StoreDialog';
import ParentSelect from './ParentSelect';

const labels = copy.categories;
const text = labels.moveDialog;

type MoveCategoryDialogProps = {
  category: Category;
  targets: Category[];
  onClose: () => void;
  onConfirm: (parentId: string | null) => Promise<void>;
};

const MoveCategoryDialog = ({ category, targets, onClose, onConfirm }: MoveCategoryDialogProps) => {
  const [parentId, setParentId] = useState<string | null>(category.parent_id ?? null);
  const [busy, setBusy] = useState(false);
  const unchanged = parentId === (category.parent_id ?? null);

  const submit = async () => {
    if (busy || unchanged) return;
    setBusy(true);
    await onConfirm(parentId);
    setBusy(false);
  };

  return (
    <StoreDialog
      title={text.title(category.name)}
      onClose={onClose}
      footer={
        <>
          <button type="button" className="account-button" onClick={onClose}>
            {labels.cancel}
          </button>
          <button
            type="button"
            className="account-button account-button--primary"
            disabled={busy || unchanged}
            onClick={() => void submit()}
          >
            {text.confirm}
          </button>
        </>
      }
    >
      <div className="stores-dialog__field">
        <label className="account-field-label" htmlFor="categories-move-parent">
          {text.label}
        </label>
        <ParentSelect id="categories-move-parent" value={parentId} targets={targets} onChange={setParentId} autoFocus />
      </div>
    </StoreDialog>
  );
};

export default MoveCategoryDialog;
