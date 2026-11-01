// Zapis pól konta z „Cofnij” w toaście. Cofnięcie jest po stronie frontendu:
// pamiętamy poprzednie wartości zmienianych pól i zapisujemy je ponownie.
import { useCallback } from 'react';
import { toast } from 'sonner';
import { type Account, type AccountUpdate, updateAccount } from '../../api';
import { copy } from '../../copy/pl';

const labels = copy.accounts;
const UNDO_TOAST_MS = 8000;

type SaveOptions = { message?: string; undoable?: boolean };

const previousValues = (account: Account, payload: AccountUpdate): AccountUpdate =>
  Object.fromEntries(
    Object.keys(payload).map((field) => [field, account[field as keyof Account] ?? null])
  ) as AccountUpdate;

export const useAccountSave = (onUpdated: (account: Account) => void) =>
  useCallback(
    async (account: Account, payload: AccountUpdate, { message = labels.saved, undoable = true }: SaveOptions = {}) => {
      try {
        onUpdated(await updateAccount(account.id, payload));
      } catch (err) {
        console.error(err);
        toast.error(labels.saveFailed);
        return false;
      }
      const undo = async () => {
        try {
          onUpdated(await updateAccount(account.id, previousValues(account, payload)));
          toast(labels.undone);
        } catch (err) {
          console.error(err);
          toast.error(labels.undoFailed);
        }
      };
      toast(message, undoable
          ? { duration: UNDO_TOAST_MS, action: { label: labels.undo, onClick: () => void undo() } }
          : undefined);
      return true;
    },
    [onUpdated]
  );
