// „Uzgodnij saldo”: użytkownik wpisuje rzeczywiste saldo, widzi różnicę i dopiero
// po potwierdzeniu powstaje korekta. Dymek zamiast modalu: modal zostaje dla nowego konta.
import { useEffect, useRef, useState } from 'react';
import { Scale } from 'lucide-react';
import { toast } from 'sonner';
import { type Account, adjustAccountBalance } from '../../../api';
import Money, { formatSignedMoney } from '../../../components/ui/Money';
import { copy } from '../../../copy/pl';
import { parseAmountToCents } from '../../../utils/format';

const labels = copy.accounts;
const dialog = labels.reconcileDialog;

type ReconcileBalanceProps = {
  account: Account;
  initiallyOpen?: boolean;
  onReconciled: () => Promise<void>;
};

const ReconcileBalance = ({ account, initiallyOpen = false, onReconciled }: ReconcileBalanceProps) => {
  const [open, setOpen] = useState(initiallyOpen);
  const [value, setValue] = useState(() => (account.balance_cents / 100).toFixed(2).replace('.', ','));
  const [submitting, setSubmitting] = useState(false);
  const rootRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (!open) return;
    const closeOnOutside = (event: MouseEvent) => {
      if (!rootRef.current?.contains(event.target as Node)) setOpen(false);
    };
    const closeOnEscape = (event: KeyboardEvent) => {
      if (event.key === 'Escape') setOpen(false);
    };
    document.addEventListener('mousedown', closeOnOutside);
    document.addEventListener('keydown', closeOnEscape);
    return () => {
      document.removeEventListener('mousedown', closeOnOutside);
      document.removeEventListener('keydown', closeOnEscape);
    };
  }, [open]);

  const toggle = () => {
    setValue((account.balance_cents / 100).toFixed(2).replace('.', ','));
    setOpen((prev) => !prev);
  };

  const targetCents = parseAmountToCents(value);
  const differenceCents = targetCents === null ? null : targetCents - account.balance_cents;

  const submit = async () => {
    if (targetCents === null || !differenceCents || submitting) return;
    try {
      setSubmitting(true);
      await adjustAccountBalance(account.id, { target_balance_cents: targetCents });
      await onReconciled();
      toast(dialog.done);
      setOpen(false);
    } catch (err) {
      console.error(err);
      toast.error(dialog.failed);
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="account-reconcile" ref={rootRef}>
      <button
        type="button"
        className="account-button"
        aria-expanded={open}
        onClick={toggle}
      >
        <Scale size={14} aria-hidden="true" />
        {labels.reconcile}
      </button>
      {open && (
        <form
          className="account-reconcile__popover"
          aria-label={labels.reconcile}
          onSubmit={(event) => {
            event.preventDefault();
            void submit();
          }}
        >
          <label className="account-field-label" htmlFor="account-reconcile-input">
            {dialog.actual}
          </label>
          <input
            id="account-reconcile-input"
            className="form-control"
            inputMode="decimal"
            autoComplete="off"
            autoFocus
            value={value}
            aria-invalid={targetCents === null}
            onFocus={(event) => event.currentTarget.select()}
            onChange={(event) => setValue(event.target.value)}
          />
          <dl className="account-reconcile__summary">
            <dt>{dialog.inApp}</dt>
            <dd>
              <Money cents={account.balance_cents} currency={account.currency} tone="neutral" />
            </dd>
            <dt>{dialog.difference}</dt>
            <dd>
              {differenceCents === null ? (
                '—'
              ) : (
                <Money
                  cents={differenceCents}
                  currency={account.currency}
                  tone={differenceCents < 0 ? 'negative' : 'auto'}
                />
              )}
            </dd>
          </dl>
          <p className="account-hint" role="status">
            {targetCents === null
              ? dialog.invalid
              : differenceCents
                ? dialog.effect(formatSignedMoney(differenceCents, account.currency))
                : dialog.same}
          </p>
          <div className="account-actions">
            <button type="button" className="account-button" onClick={() => setOpen(false)}>
              {dialog.cancel}
            </button>
            <button
              type="submit"
              className="account-button account-button--primary"
              disabled={!differenceCents || submitting}
            >
              {dialog.confirm}
            </button>
          </div>
        </form>
      )}
    </div>
  );
};

export default ReconcileBalance;
