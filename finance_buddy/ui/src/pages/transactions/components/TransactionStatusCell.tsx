// Status transakcji w komórce tabeli. Widać go tylko przy niezatwierdzonych, reszta odsłania
// przycisk dopiero po najechaniu na wiersz: wyróżniamy wyjątek, nie normę.
import { Check, CheckCheck, Clock } from 'lucide-react';
import { Transaction } from '../../../api';
import { STATUS_PRESENTATION, getTransactionStatusState, type TransactionStatusState } from '../model';

const renderTransactionStatusIcon = (status: TransactionStatusState) => {
  switch (status) {
    case 'pending':
      return <Clock size={14} aria-hidden="true" />;
    case 'cleared':
      return <Check size={14} aria-hidden="true" />;
    case 'reconciled':
      return <CheckCheck size={14} aria-hidden="true" />;
  }
};

type TransactionStatusCellProps = {
  transaction: Transaction;
  updating: boolean;
  onCycle: (transaction: Transaction) => void;
};

const TransactionStatusCell = ({ transaction, updating, onCycle }: TransactionStatusCellProps) => {
  const statusState = getTransactionStatusState(transaction);
  const statusPresentation = STATUS_PRESENTATION[statusState];
  const statusButtonTitle = `${statusPresentation.label}. Kliknij, aby ${statusPresentation.nextAction}.`;

  return (
    <div className="transactions-status text-center">
      <button
        type="button"
        className={['transactions-status__button', `transactions-status__button--${statusState}`]
          .filter(Boolean)
          .join(' ')}
        onClick={() => onCycle(transaction)}
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
