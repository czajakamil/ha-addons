// Zamiana na przelewy: podgląd z serwera, a użytkownik decyduje o każdej transakcji.
// Pewne pary są wstępnie wybrane; inne waluty i brak drugiej strony zawsze wybiera sam.
import { useEffect, useMemo, useState } from 'react';
import { toast } from 'sonner';
import {
  convertSelfTransfers,
  getAccounts,
  previewSelfTransfers,
  type SelfTransferDecision,
  undoSelfTransferConversion
} from '../../../api';
import { formatSignedMoney } from '../../../components/ui/Money';
import { copy } from '../../../copy/pl';
import { useApi } from '../../../hooks/useApi';
import { formatShortDate } from '../../../utils/date';
import { errorDetail } from '../apiError';
import { maskAccountNumbers } from '../cleanup';
import StoreDialog from './StoreDialog';

const labels = copy.stores;
const text = labels.transfers;
const UNDO_TOAST_MS = 8000;

// Wartość pola: '' nie zamieniaj, 'tx:<id>' połącz, 'acc:<id>' bez drugiej strony.
const toDecision = (transactionId: string, choice: string): SelfTransferDecision => {
  const [kind, id] = [choice.slice(0, choice.indexOf(':')), choice.slice(choice.indexOf(':') + 1)];
  return kind === 'tx' ? { transaction_id: transactionId, partner_id: id } : { transaction_id: transactionId, account_id: id };
};

type SelfTransferDialogProps = {
  storeIds: string[];
  onClose: () => void;
  onChanged: () => Promise<void>;
};

const SelfTransferDialog = ({ storeIds, onClose, onChanged }: SelfTransferDialogProps) => {
  const preview = useApi(() => previewSelfTransfers(storeIds), { errorMessage: text.failed });
  const accounts = useApi(async () => (await getAccounts()).accounts);
  const [choices, setChoices] = useState<Record<string, string>>({});
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    if (!preview.data) return;
    setChoices(
      Object.fromEntries(preview.data.rows.flatMap((row) => (row.suggested ? [[row.id, `tx:${row.suggested}`]] : [])))
    );
  }, [preview.data]);

  const selected = useMemo(() => Object.entries(choices).filter(([, choice]) => choice), [choices]);

  const undo = async (id: string) => {
    try {
      const result = await undoSelfTransferConversion(id);
      toast(text.undone(result.restored, result.kept));
      await onChanged();
    } catch (err) {
      console.error(err);
      toast.error(errorDetail(err) ?? text.undoFailed);
    }
  };

  const submit = async () => {
    setBusy(true);
    try {
      const result = await convertSelfTransfers(selected.map(([id, choice]) => toDecision(id, choice)));
      toast(text.done(result.converted), {
        duration: UNDO_TOAST_MS,
        action: { label: labels.undo, onClick: () => void undo(result.conversion_id) }
      });
      onClose();
      await onChanged();
    } catch (err) {
      console.error(err);
      toast.error(errorDetail(err) ?? text.saveFailed);
      await preview.refetch();
      setBusy(false);
    }
  };

  const rows = preview.data?.rows ?? [];

  return (
    <StoreDialog
      title={text.title}
      wide
      onClose={onClose}
      footer={
        <>
          <button type="button" className="account-button" disabled={busy} onClick={onClose}>
            {labels.cancel}
          </button>
          <button
            type="button"
            className="account-button account-button--primary"
            disabled={!selected.length || busy}
            onClick={() => void submit()}
          >
            {busy ? text.saving : text.confirm(selected.length)}
          </button>
        </>
      }
    >
      <p className="stores-dialog__note">{text.effect}</p>
      {preview.loading && !preview.data && <p className="stores-dialog__note">{text.loading}</p>}
      {preview.error && <p className="stores-merge-conflict is-blocking">{preview.error}</p>}
      {preview.data && !rows.length && <p className="stores-dialog__note">{text.empty}</p>}
      {Boolean(preview.data?.already_transfers) && (
        <p className="stores-dialog__note">{text.alreadyTransfers(preview.data?.already_transfers ?? 0)}</p>
      )}
      {rows.length > 0 && (
        <div className="stores-transfer-list">
          {rows.map((row) => {
            const date = formatShortDate(row.date);
            const disabled = row.status === 'has_items' || row.status === 'covered';
            return (
              <div key={row.id} className="stores-transfer-row">
                <div className="stores-transfer-row__head">
                  <span>
                    {date} · {row.account_name}
                    {row.description ? ` · ${maskAccountNumbers(row.description)}` : ''}
                  </span>
                  <strong>{formatSignedMoney(row.amount_cents, row.currency)}</strong>
                </div>
                <span className="account-hint">{text.status[row.status]}</span>
                {!disabled && (
                  <select
                    className="form-select form-select-sm"
                    aria-label={text.choiceLabel(date)}
                    value={choices[row.id] ?? ''}
                    onChange={(event) => setChoices((prev) => ({ ...prev, [row.id]: event.target.value }))}
                  >
                    <option value="">{text.skip}</option>
                    {row.candidates.length > 0 && (
                      <optgroup label={text.withPartner}>
                        {row.candidates.map((candidate) => (
                          <option key={candidate.id} value={`tx:${candidate.id}`}>
                            {candidate.account_name} · {formatShortDate(candidate.date)} ·{' '}
                            {formatSignedMoney(candidate.amount_cents, candidate.currency)}
                          </option>
                        ))}
                      </optgroup>
                    )}
                    <optgroup label={text.oneSided}>
                      {accounts.data
                        ?.filter((account) => account.id !== row.account_id)
                        .map((account) => (
                          <option key={account.id} value={`acc:${account.id}`}>
                            {account.name} ({account.currency})
                          </option>
                        ))}
                    </optgroup>
                  </select>
                )}
              </div>
            );
          })}
        </div>
      )}
    </StoreDialog>
  );
};

export default SelfTransferDialog;
