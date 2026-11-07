// Status transakcji w komórce tabeli, zawsze widoczny. Każdy stan ma inny kształt (nie tylko
// kolor): pusty okrąg = do zatwierdzenia, okrąg z ptaszkiem = zatwierdzona, kłódka = uzgodniona.
import { Circle, CircleCheck, Lock } from 'lucide-react';
import { Transaction } from '../../../api';
import { STATUS_PRESENTATION, getTransactionStatusState, type TransactionStatusState } from '../model';

const renderTransactionStatusIcon = (status: TransactionStatusState) => {
  switch (status) {
    case 'pending':
      return <Circle size={14} aria-hidden="true" />;
    case 'cleared':
      return <CircleCheck size={14} aria-hidden="true" />;
    case 'reconciled':
      return <Lock size={14} aria-hidden="true" />;
  }
};

type TransactionStatusCellProps = {
  transaction: Transaction;
  updating: boolean;
  onToggle: (transaction: Transaction) => void;
};

const TransactionStatusCell = ({ transaction, updating, onToggle }: TransactionStatusCellProps) => {
  const statusState = getTransactionStatusState(transaction);
  const statusPresentation = STATUS_PRESENTATION[statusState];
  const statusButtonTitle = `${statusPresentation.label}. ${statusPresentation.clickHint}`;

  return (
    <div className="transactions-status text-center">
      <button
        type="button"
        className={['transactions-status__button', `transactions-status__button--${statusState}`]
          .filter(Boolean)
          .join(' ')}
        onClick={() => onToggle(transaction)}
        disabled={updating}
        title={statusButtonTitle}
        aria-label={statusButtonTitle}
        aria-busy={updating}
      >
        {renderTransactionStatusIcon(statusState)}
      </button>
    </div>
  );
};

export default TransactionStatusCell;
