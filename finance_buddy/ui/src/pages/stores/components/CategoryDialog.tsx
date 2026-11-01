// Ustawienie domyślnej kategorii dla zaznaczonych sklepów.
import { useState } from 'react';
import type { Category } from '../../../api';
import TagPathInput from '../../../components/tags/TagPathInput';
import { copy } from '../../../copy/pl';
import StoreDialog from './StoreDialog';

const labels = copy.stores;
const text = labels.categoryDialog;

type CategoryDialogProps = {
  count: number;
  categories: Category[];
  onClose: () => void;
  onConfirm: (path: string) => Promise<void>;
};

const CategoryDialog = ({ count, categories, onClose, onConfirm }: CategoryDialogProps) => {
  const [path, setPath] = useState('');
  const [busy, setBusy] = useState(false);

  const submit = async (value: string) => {
    if (!value.trim() || busy) return;
    setBusy(true);
    await onConfirm(value.trim());
    setBusy(false);
  };

  return (
    <StoreDialog
      title={text.title(count)}
      onClose={onClose}
      footer={
        <>
          <button type="button" className="account-button" onClick={onClose}>
            {labels.cancel}
          </button>
          <button
            type="button"
            className="account-button account-button--primary"
            disabled={!path.trim() || busy}
            onClick={() => void submit(path)}
          >
            {text.confirm}
          </button>
        </>
      }
    >
      <div className="stores-dialog__field">
        <label className="account-field-label" htmlFor="stores-bulk-category">
          {text.label}
        </label>
        <TagPathInput
          inputId="stores-bulk-category"
          value={path}
          categories={categories}
          forAccounts={false}
          autoFocus
          commitOnBlur={false}
          placeholder={text.placeholder}
          onChange={setPath}
          onCommit={(value) => void submit(value)}
          onCancel={onClose}
        />
      </div>
    </StoreDialog>
  );
};

export default CategoryDialog;
