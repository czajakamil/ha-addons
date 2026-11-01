// Zapis pól sklepów z „Cofnij” w toaście. Grupa to kilka sklepów, a API zapisuje po jednym, więc idziemy po 5 naraz.
import { useCallback } from 'react';
import { toast } from 'sonner';
import { type Store, type StoreUpdate, updateStore } from '../../api';
import { copy } from '../../copy/pl';

const labels = copy.stores;
const UNDO_TOAST_MS = 8000;
const SAVE_CONCURRENCY = 5;

type Change = { store: Store; payload: StoreUpdate };

const previousValues = ({ store, payload }: Change): Change => ({
  store,
  payload: Object.fromEntries(
    Object.keys(payload).map((field) => [field, store[field as keyof Store] ?? null])
  ) as StoreUpdate
});

const applyChanges = async (changes: readonly Change[], onUpdated: (stores: Store[]) => void) => {
  const updated: Store[] = [];
  let failed = 0;
  for (let start = 0; start < changes.length; start += SAVE_CONCURRENCY) {
    const results = await Promise.allSettled(
      changes.slice(start, start + SAVE_CONCURRENCY).map(({ store, payload }) => updateStore(store.id, payload))
    );
    results.forEach((result) => {
      if (result.status === 'fulfilled') updated.push(result.value);
      else {
        console.error(result.reason);
        failed += 1;
      }
    });
  }
  if (updated.length) onUpdated(updated);
  return failed;
};

/** Domyślna kategoria działa przy imporcie tylko razem z flagą auto-kategoryzacji. */
export const categoryPayload = (store: Store, categoryId: string | null): StoreUpdate => ({
  automatic_category_id: categoryId,
  ...(categoryId && !store.automatically_categorize_payee ? { automatically_categorize_payee: true } : {})
});

export const useStoreGroupSave = (onUpdated: (stores: Store[]) => void) =>
  useCallback(
    async (changes: Change[], message: string = labels.saved) => {
      const failed = await applyChanges(changes, onUpdated);
      if (failed) {
        toast.error(changes.length === 1 ? labels.saveFailed : labels.savePartial(failed, changes.length));
        return false;
      }
      const undo = async () => {
        const undoFailed = await applyChanges(changes.map(previousValues), onUpdated);
        if (undoFailed) toast.error(labels.undoFailed);
        else toast(labels.undone);
      };
      toast(message, { duration: UNDO_TOAST_MS, action: { label: labels.undo, onClick: () => void undo() } });
      return true;
    },
    [onUpdated]
  );
