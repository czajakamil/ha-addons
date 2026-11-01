import Button from '../../../components/Button';
import type { ReceiptCheckIssue } from '../../../utils/receiptChecks';

type IssueNoteProps = {
  issue: ReceiptCheckIssue | null;
  /** Etykieta odznaki, np. „sprawdź datę"; komunikat reguły idzie obok niej. */
  badLabel: string;
  warnLabel?: string;
  onApplySuggestion?: (suggestion: string) => void;
  suggestionLabel?: (suggestion: string) => string;
};

/**
 * Zastrzeżenie pod polem formularza.
 *
 * Przycisk poprawki pojawia się **tylko** wtedy, gdy reguła podała podpowiedź —
 * a podaje ją tam, gdzie poprawka wynika z reguły (zamiana dnia z miesiącem),
 * a nie z wyboru jednego z kilku pasujących kandydatów. Dlatego pole NIP-u
 * przycisku nie ma: podstawiony numer wygląda potem na zweryfikowany i nikt do
 * niego nie wraca.
 */
const IssueNote = ({
  issue,
  badLabel,
  warnLabel,
  onApplySuggestion,
  suggestionLabel
}: IssueNoteProps) => {
  if (!issue) return null;
  const isBad = issue.level === 'bad';

  return (
    <div className="receipt-field__note">
      <span className={`receipt-badge receipt-badge--${isBad ? 'bad' : 'warn'}`}>
        {isBad ? badLabel : (warnLabel ?? badLabel)}
      </span>
      <span className="text-secondary">{issue.message}</span>
      {issue.suggestion && onApplySuggestion ? (
        <Button
          variant="outline-secondary"
          size="sm"
          onClick={() => onApplySuggestion(issue.suggestion as string)}
        >
          {suggestionLabel
            ? suggestionLabel(issue.suggestion)
            : `popraw na ${issue.suggestion.slice(0, 10)}`}
        </Button>
      ) : null}
    </div>
  );
};

export default IssueNote;
