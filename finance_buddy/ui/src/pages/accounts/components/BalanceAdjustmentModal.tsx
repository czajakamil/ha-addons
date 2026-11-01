// Modal potwierdzenia transakcji wyrównującej saldo konta.
import { formatMoney } from '../../../utils/format';
import { type BalanceAdjustmentModalState } from '../model';

type BalanceAdjustmentModalProps = {
  state: BalanceAdjustmentModalState;
  submitting: boolean;
  onCancel: () => void;
  onConfirm: () => void;
};

const BalanceAdjustmentModal = ({ state, submitting, onCancel, onConfirm }: BalanceAdjustmentModalProps) => {
  const { account, targetBalanceCents } = state;
  const adjustmentCents = targetBalanceCents - account.balance_cents;
  const targetLabel = formatMoney(targetBalanceCents, account.currency);
  const adjustmentLabel = `${adjustmentCents >= 0 ? '+' : '-'}${formatMoney(Math.abs(adjustmentCents), account.currency)}`;
  const modalTitleId = 'balance-adjustment-modal-title';

  return (
    <>
      <div
        className="modal modal-blur fade show d-block"
        role="dialog"
        aria-modal="true"
        aria-labelledby={modalTitleId}
        style={{ display: 'block' }}
      >
        <div className="modal-dialog modal-dialog-centered" role="document">
          <div className="modal-content">
            <div className="modal-header">
              <h3 className="modal-title" id={modalTitleId}>
                Wyrównanie salda konta
              </h3>
              <button
                type="button"
                className="btn-close is-focusable"
                aria-label="Zamknij"
                onClick={onCancel}
                disabled={submitting}
              />
            </div>
            <div className="modal-body">
              <p className="mb-0">
                Wpisałeś inną kwotę niż wychodzi z obliczeń. Zostanie stworzona transakcja wyrównująca do kwoty:{' '}
                <strong>{targetLabel}</strong>, o kwocie <strong>{adjustmentLabel}</strong>. Czy chcesz kontynuować?
              </p>
            </div>
            <div className="modal-footer">
              <button type="button" className="btn btn-outline-secondary is-focusable" onClick={onCancel} disabled={submitting}>
                NIE
              </button>
              <button
                type="button"
                className="btn btn-primary is-focusable"
                onClick={onConfirm}
                disabled={submitting}
              >
                {submitting ? 'Tworzenie…' : 'TAK'}
              </button>
            </div>
          </div>
        </div>
      </div>
      <div className="modal-backdrop fade show" />
    </>
  );
};

export default BalanceAdjustmentModal;
