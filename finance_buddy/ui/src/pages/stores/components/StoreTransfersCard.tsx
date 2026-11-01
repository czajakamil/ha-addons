// Karta na stronie sklepu: zamiana na przelewy na żądanie i lista zamian do cofnięcia.
import { useState } from 'react';
import { toast } from 'sonner';
import { getSelfTransferConversions, type SelfTransferConversion, undoSelfTransferConversion } from '../../../api';
import { copy } from '../../../copy/pl';
import { useApi } from '../../../hooks/useApi';
import { formatShortDate } from '../../../utils/date';
import { errorDetail } from '../apiError';
import SelfTransferDialog from './SelfTransferDialog';

const labels = copy.stores;
const text = labels.transfers;

const StoreTransfersCard = ({ storeIds, onChanged }: { storeIds: string[]; onChanged: () => Promise<void> }) => {
  const conversions = useApi(() => getSelfTransferConversions(storeIds));
  const [open, setOpen] = useState(false);

  const refresh = async () => {
    await Promise.all([conversions.refetch(), onChanged()]);
  };

  const undo = async (conversion: SelfTransferConversion) => {
    try {
      const result = await undoSelfTransferConversion(conversion.id);
      toast(text.undone(result.restored, result.kept));
      await refresh();
    } catch (err) {
      console.error(err);
      toast.error(errorDetail(err) ?? text.undoFailed);
    }
  };

  return (
    <section className="stores-merge-card" aria-label={text.cardTitle}>
      <div className="stores-merge-card__row">
        <div className="stores-merge-card__copy">
          <strong>{text.cardTitle}</strong>
          <p className="account-hint">{text.cardHint}</p>
        </div>
        <button type="button" className="account-button" onClick={() => setOpen(true)}>
          {text.open}
        </button>
      </div>
      {Boolean(conversions.data?.length) && (
        <div className="stores-dialog__field">
          <span className="account-field-label">{text.history}</span>
          <ul className="stores-list">
            {conversions.data?.map((conversion) => (
              <li key={conversion.id} className="stores-list__row">
                <span className="stores-list__name">
                  {text.entry(conversion.converted, formatShortDate(conversion.expires_at.slice(0, 10)))}
                </span>
                <button type="button" className="stores-aliases__remove" onClick={() => void undo(conversion)}>
                  {labels.undo}
                </button>
              </li>
            ))}
          </ul>
        </div>
      )}
      {open && <SelfTransferDialog storeIds={storeIds} onClose={() => setOpen(false)} onChanged={refresh} />}
    </section>
  );
};

export default StoreTransfersCard;
