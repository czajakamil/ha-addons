// Okno scalania z makiety. Scalanie wymaga zmian w API (etap 2), więc zatwierdzenie jest nieaktywne.
import { copy } from '../../../copy/pl';
import { maskAccountNumbers } from '../cleanup';
import StoreDialog from './StoreDialog';

const labels = copy.stores;
const text = labels.mergeDialog;

type MergeDialogProps = {
  items: { id: string; name: string; count: number | null }[];
  categoryLabel: string;
  onClose: () => void;
};

const MergeDialog = ({ items, categoryLabel, onClose }: MergeDialogProps) => {
  const total = items.reduce((sum, item) => sum + (item.count ?? 0), 0);
  return (
    <StoreDialog
      title={text.title(items.length)}
      onClose={onClose}
      footer={
        <>
          <span className="account-hint">{text.requiresStage2}</span>
          <button type="button" className="account-button" onClick={onClose}>
            {labels.cancel}
          </button>
          <button type="button" className="account-button account-button--primary" disabled>
            {text.confirm}
          </button>
        </>
      }
    >
      <label className="stores-dialog__field">
        <span className="account-field-label">{text.name}</span>
        <input className="form-control" defaultValue={maskAccountNumbers(items[0]?.name ?? '')} />
      </label>
      <div className="stores-dialog__field">
        <span className="account-field-label">{text.category}</span>
        <span className="stores-dialog__value">{categoryLabel || labels.noCategory}</span>
      </div>
      <div className="stores-dialog__field">
        <span className="account-field-label">{text.names}</span>
        <ul className="stores-list">
          {items.map((item) => (
            <li key={item.id} className="stores-list__row">
              <span className="stores-list__name">{maskAccountNumbers(item.name)}</span>
              {item.count !== null && <span className="stores-list__count">{labels.transactionsCount(item.count)}</span>}
            </li>
          ))}
        </ul>
      </div>
      <p className="stores-dialog__note">{text.summary(total)}</p>
    </StoreDialog>
  );
};

export default MergeDialog;
