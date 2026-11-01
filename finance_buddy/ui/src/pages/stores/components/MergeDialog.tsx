// Okno scalania: podgląd z serwera (dry_run) przed zatwierdzeniem. Konflikt blokujący wyłącza „Scal”.
import { useState } from 'react';
import { toast } from 'sonner';
import { type Category, mergeStores, previewStoreMerge, undoStoreMerge } from '../../../api';
import TagPathInput from '../../../components/tags/TagPathInput';
import { copy } from '../../../copy/pl';
import { useApi } from '../../../hooks/useApi';
import { errorDetail } from '../apiError';
import { maskAccountNumbers } from '../cleanup';
import StoreDialog from './StoreDialog';

const labels = copy.stores;
const text = labels.mergeDialog;
const UNDO_TOAST_MS = 8000;

type MergeDialogProps = {
  sourceIds: string[];
  targetId: string;
  defaultName: string;
  names: string[];
  categories: Category[];
  categoryPath: string;
  ensureCategory: (path: string) => Promise<string>;
  onClose: () => void;
  onMerged: (targetId: string) => Promise<void>;
  onUndone: () => Promise<void>;
};

const MergeDialog = ({
  sourceIds,
  targetId,
  defaultName,
  names,
  categories,
  categoryPath,
  ensureCategory,
  onClose,
  onMerged,
  onUndone
}: MergeDialogProps) => {
  const preview = useApi(() => previewStoreMerge({ source_ids: sourceIds, target_id: targetId }), {
    errorMessage: text.summaryFailed
  });
  const [name, setName] = useState(defaultName);
  const [path, setPath] = useState(categoryPath);
  const [busy, setBusy] = useState(false);
  const nameValid = name.trim().length >= 2;
  const canSubmit = Boolean(preview.data?.can_merge) && nameValid && !busy;

  const undo = async (mergeId: string) => {
    try {
      await undoStoreMerge(mergeId);
      toast(text.undone);
      await onUndone();
    } catch (err) {
      console.error(err);
      toast.error(errorDetail(err) ?? text.undoFailed);
    }
  };

  const submit = async () => {
    setBusy(true);
    try {
      const categoryId = path.trim() ? await ensureCategory(path.trim()) : null;
      const result = await mergeStores({
        source_ids: sourceIds,
        target_id: targetId,
        name: name.trim(),
        category_id: categoryId
      });
      toast(text.merged, {
        duration: UNDO_TOAST_MS,
        action: { label: labels.undo, onClick: () => void undo(result.merge_id) }
      });
      await onMerged(result.target_id);
    } catch (err) {
      console.error(err);
      toast.error(errorDetail(err) ?? text.failed);
      // Stan mógł się zmienić, więc podgląd ma pokazać aktualny konflikt.
      await preview.refetch();
      setBusy(false);
    }
  };

  return (
    <StoreDialog
      title={text.title(names.length)}
      onClose={onClose}
      footer={
        <>
          <button type="button" className="account-button" disabled={busy} onClick={onClose}>
            {labels.cancel}
          </button>
          <button
            type="button"
            className="account-button account-button--primary"
            disabled={!canSubmit}
            onClick={() => void submit()}
          >
            {busy ? text.merging : text.confirm}
          </button>
        </>
      }
    >
      <label className="stores-dialog__field">
        <span className="account-field-label">{text.name}</span>
        <input className="form-control" value={name} onChange={(event) => setName(event.target.value)} />
        {!nameValid && <span className="stores-aliases__preview is-error">{text.nameTooShort}</span>}
      </label>
      <div className="stores-dialog__field">
        <label className="account-field-label" htmlFor="stores-merge-category">
          {text.category}
        </label>
        <TagPathInput
          inputId="stores-merge-category"
          value={path}
          categories={categories}
          forAccounts={false}
          commitOnBlur={false}
          placeholder={labels.categoryDialog.placeholder}
          onChange={setPath}
        />
      </div>
      <div className="stores-dialog__field">
        <span className="account-field-label">{text.names}</span>
        <ul className="stores-list">
          {names.map((entry, index) => (
            <li key={`${entry}-${index}`} className="stores-list__row">
              <span className="stores-list__name">{maskAccountNumbers(entry)}</span>
            </li>
          ))}
        </ul>
      </div>
      <div aria-live="polite" className="stores-dialog__field">
        {preview.loading && !preview.data && <p className="stores-dialog__note">{text.summaryLoading}</p>}
        {preview.error && <p className="stores-merge-conflict is-blocking">{preview.error}</p>}
        {preview.data && (
          <>
            <p className="stores-dialog__note">
              {text.summary(preview.data.transactions, preview.data.receipts, preview.data.aliases)} {text.undoHint}
            </p>
            {preview.data.conflicts.map((conflict) => (
              <p
                key={conflict.message}
                className={`stores-merge-conflict${conflict.blocking ? ' is-blocking' : ''}`}
              >
                {conflict.message}
              </p>
            ))}
          </>
        )}
      </div>
    </StoreDialog>
  );
};

export default MergeDialog;
