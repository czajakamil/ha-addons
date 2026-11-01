// Aliasy sklepu w Ustawieniach: lista z „Odepnij”, dodawanie i zapowiedź, ile transakcji alias dopasuje.
import { useEffect, useState } from 'react';
import { toast } from 'sonner';
import {
  addStoreAlias,
  deleteStoreAlias,
  getStoreAliases,
  previewStoreAlias,
  type StoreAlias,
  type StoreAliasPreview
} from '../../../api';
import SkeletonRows from '../../../components/ui/SkeletonRows';
import { copy } from '../../../copy/pl';
import { useApi } from '../../../hooks/useApi';
import { errorDetail } from '../apiError';
import { maskAccountNumbers } from '../cleanup';

const labels = copy.stores;
const text = labels.settings;
const PREVIEW_DEBOUNCE_MS = 300;

type StoreAliasesProps = {
  storeIds: string[];
  // Nowy alias trafia do głównego sklepu grupy
  targetStoreId: string;
};

const StoreAliases = ({ storeIds, targetStoreId }: StoreAliasesProps) => {
  const aliases = useApi(() => getStoreAliases(storeIds), { errorMessage: text.aliasLoadFailed });
  const [draft, setDraft] = useState('');
  const [preview, setPreview] = useState<StoreAliasPreview | null>(null);
  const [previewError, setPreviewError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const storeKey = storeIds.join(',');

  useEffect(() => {
    const pattern = draft.trim();
    if (!pattern) {
      setPreview(null);
      setPreviewError(null);
      return;
    }
    let stale = false;
    const timer = window.setTimeout(() => {
      previewStoreAlias(pattern, storeKey.split(','))
        .then((result) => {
          if (stale) return;
          setPreview(result);
          setPreviewError(null);
        })
        .catch((err: unknown) => {
          if (stale) return;
          setPreview(null);
          setPreviewError(errorDetail(err) ?? text.aliasFailed);
        });
    }, PREVIEW_DEBOUNCE_MS);
    return () => {
      stale = true;
      window.clearTimeout(timer);
    };
  }, [draft, storeKey]);

  const add = async (pattern: string) => {
    setBusy(true);
    try {
      await addStoreAlias(targetStoreId, pattern);
      setDraft('');
      await aliases.refetch();
      return true;
    } catch (err) {
      console.error(err);
      toast.error(errorDetail(err) ?? text.aliasFailed);
      return false;
    } finally {
      setBusy(false);
    }
  };

  const remove = async (alias: StoreAlias) => {
    try {
      await deleteStoreAlias(alias.id);
      await aliases.refetch();
    } catch (err) {
      console.error(err);
      toast.error(text.aliasFailed);
      return;
    }
    // Niejednoznacznego nie da się przywrócić: jego nazwa ma już innego właściciela
    toast(
      text.aliasRemoved,
      alias.is_ambiguous
        ? undefined
        : { action: { label: labels.undo, onClick: () => void add(alias.pattern) } }
    );
  };

  const canAdd = Boolean(draft.trim()) && !busy && !previewError && !preview?.conflict;

  return (
    <div className="stores-aliases">
      {aliases.error && <p className="stores-aliases__preview is-error">{aliases.error}</p>}
      {!aliases.data && aliases.loading && <SkeletonRows rows={2} />}
      {aliases.data &&
        (aliases.data.length ? (
          <ul className="stores-list">
            {aliases.data.map((alias) => {
              const pattern = maskAccountNumbers(alias.pattern);
              return (
                <li key={alias.id} className="stores-list__row">
                  <span className="stores-list__name" title={pattern}>
                    {pattern}
                  </span>
                  {alias.is_ambiguous && (
                    <span className="stores-badge stores-badge--duplicate" title={text.aliasAmbiguousHint}>
                      {text.aliasAmbiguous}
                    </span>
                  )}
                  <button
                    type="button"
                    className="stores-aliases__remove"
                    aria-label={text.aliasRemoveLabel(pattern)}
                    onClick={() => void remove(alias)}
                  >
                    {text.aliasRemove}
                  </button>
                </li>
              );
            })}
          </ul>
        ) : (
          <p className="stores-aliases__preview">{text.aliasEmpty}</p>
        ))}
      {aliases.data?.some((alias) => alias.is_ambiguous) && (
        <p className="stores-aliases__preview">{text.aliasAmbiguousHint}</p>
      )}
      <form
        className="stores-aliases__add"
        onSubmit={(event) => {
          event.preventDefault();
          if (canAdd) {
            void add(draft.trim()).then((added) => {
              if (added) toast(text.aliasAdded);
            });
          }
        }}
      >
        <input
          className="form-control"
          aria-label={text.aliasLabel}
          autoComplete="off"
          placeholder={text.aliasPlaceholder}
          value={draft}
          onChange={(event) => setDraft(event.target.value)}
        />
        <button type="submit" className="account-button" disabled={!canAdd}>
          {text.aliasAdd}
        </button>
      </form>
      <div aria-live="polite">
        {previewError && <p className="stores-aliases__preview is-error">{previewError}</p>}
        {preview?.conflict && <p className="stores-aliases__preview is-error">{preview.conflict}</p>}
        {preview && !preview.conflict && (
          <p className="stores-aliases__preview">{text.aliasPreview(preview.matches, preview.matches_other_stores)}</p>
        )}
      </div>
    </div>
  );
};

export default StoreAliases;
