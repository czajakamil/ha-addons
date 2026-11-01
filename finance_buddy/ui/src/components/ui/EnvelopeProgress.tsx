// Pasek postępu koperty: do 85% neutralny, 85–100% ostrzeżenie, powyżej 100% przekroczenie.
type EnvelopeProgressProps = {
  percent: number;
};

const resolveLevel = (percent: number) => {
  if (percent > 100) return 'over';
  if (percent >= 85) return 'near';
  return 'ok';
};

const EnvelopeProgress = ({ percent }: EnvelopeProgressProps) => (
  <span className={`envelope-progress envelope-progress--${resolveLevel(percent)}`} aria-hidden="true">
    <span className="envelope-progress__fill" style={{ width: `${Math.min(percent, 100)}%` }} />
  </span>
);

export default EnvelopeProgress;
