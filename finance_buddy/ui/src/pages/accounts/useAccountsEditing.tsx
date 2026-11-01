// Edycja salda konta w wierszu listy: po wpisaniu nowej wartości modal pyta o wyrównanie.
import { useCallback, useState, type ChangeEvent, type KeyboardEvent as ReactKeyboardEvent } from 'react';
import { Account, AccountBalanceAdjustmentPayload, adjustAccountBalance } from '../../api';
import Money from '../../components/ui/Money';
import { parseAmountToCents } from '../../utils/format';
import { type BalanceAdjustmentModalState } from './model';

type UseAccountsEditingParams = {
  loadAccounts: () => Promise<Account[] | undefined>;
};

export const useAccountsEditing = ({ loadAccounts }: UseAccountsEditingParams) => {
  const [inlineError, setInlineError] = useState<string | null>(null);
  const [balanceEditingAccountId, setBalanceEditingAccountId] = useState<string | null>(null);
  const [balanceEditValue, setBalanceEditValue] = useState('');
  const [balanceAdjustmentModal, setBalanceAdjustmentModal] = useState<BalanceAdjustmentModalState | null>(null);
  const [adjustingBalance, setAdjustingBalance] = useState(false);

  const startBalanceEditing = useCallback((account: Account) => {
    setBalanceEditingAccountId(account.id);
    setBalanceEditValue((account.balance_cents / 100).toFixed(2));
    setInlineError(null);
  }, []);

  const cancelBalanceEditing = () => {
    setBalanceEditingAccountId(null);
    setBalanceEditValue('');
  };

  const commitBalanceEdit = (account: Account) => {
    const nextValueCents = parseAmountToCents(balanceEditValue);
    if (nextValueCents === null) {
      setInlineError('Podaj poprawną kwotę salda (np. 123.45).');
      return;
    }

    if (nextValueCents === account.balance_cents) {
      cancelBalanceEditing();
      return;
    }

    setBalanceAdjustmentModal({
      account,
      targetBalanceCents: nextValueCents
    });
    cancelBalanceEditing();
  };

  const handleBalanceKeyDown = (event: ReactKeyboardEvent<HTMLInputElement>, account: Account) => {
    if (event.key === 'Escape') {
      event.preventDefault();
      event.stopPropagation();
      cancelBalanceEditing();
      return;
    }
    if (event.key === 'Enter') {
      event.preventDefault();
      event.stopPropagation();
      commitBalanceEdit(account);
    }
  };

  const handleCancelBalanceAdjustment = () => {
    if (adjustingBalance) return;
    setBalanceAdjustmentModal(null);
  };

  const handleConfirmBalanceAdjustment = async () => {
    if (!balanceAdjustmentModal) return;
    const payload: AccountBalanceAdjustmentPayload = {
      target_balance_cents: balanceAdjustmentModal.targetBalanceCents
    };
    try {
      setAdjustingBalance(true);
      await adjustAccountBalance(balanceAdjustmentModal.account.id, payload);
      await loadAccounts();
      setBalanceAdjustmentModal(null);
      setInlineError(null);
    } catch (err) {
      console.error(err);
      setInlineError('Nie udało się wyrównać salda. Spróbuj ponownie.');
    } finally {
      setAdjustingBalance(false);
    }
  };

  const renderBalanceCell = (account: Account) => {
    if (balanceEditingAccountId === account.id) {
      return (
        <div className="editable-cell align-middle fw-semibold text-end is-editing">
          <input
            type="text"
            className="form-control form-control-sm text-end"
            autoFocus
            value={balanceEditValue}
            onChange={(event: ChangeEvent<HTMLInputElement>) => setBalanceEditValue(event.target.value)}
            onBlur={() => commitBalanceEdit(account)}
            onKeyDown={(event) => handleBalanceKeyDown(event, account)}
            inputMode="decimal"
          />
          <div className="editable-cell__meta text-end">Enter aby kontynuować</div>
        </div>
      );
    }

    return (
      <Money
        cents={account.balance_cents}
        currency={account.currency}
        tone={account.balance_cents < 0 ? 'negative' : 'neutral'}
      />
    );
  };

  return {
    inlineError,
    renderBalanceCell,
    startBalanceEditing,
    balanceAdjustmentModal,
    adjustingBalance,
    handleCancelBalanceAdjustment,
    handleConfirmBalanceAdjustment
  };
};
