// Wynik akcji zbiorczej: ile się udało, które transakcje nie przeszły i ponowienie tylko tych.
import { CircleAlert, CircleCheck, RotateCcw, X } from 'lucide-react';
import Money from '../../../components/ui/Money';
import TruncatedText from '../../../components/ui/TruncatedText';
import type { Transaction } from '../../../api';
import { copy } from '../../../copy/pl';
import { formatShortDate } from '../../../utils/date';
import type { BulkReport } from '../bulkUpdate';

const labels = copy.transactions.bulk;

type TransactionsBulkReportProps = {
  report: BulkReport;
  busy: boolean;
  transactionName: (transaction: Transaction) => string;
  onRetry: () => void;
  onDismiss: () => void;
};

const TransactionsBulkReport = ({
  report,
  busy,
  transactionName,
  onRetry,
  onDismiss
}: TransactionsBulkReportProps) => {
  const { action, updatedCount, skippedCount, failures } = report;
  const hasFailures = failures.length > 0;
  const Icon = hasFailures ? CircleAlert : CircleCheck;

  return (
    <section
      className={`transactions-report${hasFailures ? ' transactions-report--failed' : ''}`}
      role={hasFailures ? 'alert' : 'status'}
    >
      <header className="transactions-report__header">
        <Icon size={16} aria-hidden="true" className="transactions-report__icon" />
        <p className="transactions-report__summary">
          <strong>{action.label}:</strong> {labels.summary(updatedCount, updatedCount + failures.length)}
          {skippedCount > 0 && ` ${labels.skipped(skippedCount, action.skippedLabel)}`}
        </p>
        {hasFailures && (
          <button type="button" className="btn btn-primary btn-sm" disabled={busy} onClick={onRetry}>
            <RotateCcw size={14} aria-hidden="true" />
            {labels.retry(failures.length)}
          </button>
        )}
        <button
          type="button"
          className="icon-button"
          aria-label={labels.dismiss}
          title={labels.dismiss}
          disabled={busy}
          onClick={onDismiss}
        >
          <X size={14} aria-hidden="true" />
        </button>
      </header>
      {hasFailures && (
        <ul className="transactions-report__list" aria-label={labels.failedList}>
          {failures.map(({ transaction, reason }) => (
            <li key={transaction.id} className="transactions-report__item">
              <span className="transactions-report__date">
                {transaction.date ? formatShortDate(transaction.date) : '—'}
              </span>
              <TruncatedText text={transactionName(transaction)} />
              <span className="transactions-report__reason">{reason}</span>
              <Money cents={transaction.total_cents} currency={transaction.currency} tone="neutral" />
            </li>
          ))}
        </ul>
      )}
    </section>
  );
};

export default TransactionsBulkReport;
