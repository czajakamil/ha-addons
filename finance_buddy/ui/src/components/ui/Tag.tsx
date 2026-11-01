// Tag lub kategoria: kropka w kolorze i nazwa. Emoji użytkownika zastępuje kropkę.
import TruncatedText from './TruncatedText';

type TagProps = {
  name: string;
  color?: string | null;
  emoji?: string | null;
};

const Tag = ({ name, color, emoji }: TagProps) => (
  <span className="tag-label">
    {emoji ? (
      <span className="tag-label__emoji" aria-hidden="true">
        {emoji}
      </span>
    ) : (
      <span
        className="tag-label__dot"
        style={{ background: color ?? 'var(--tag-neutral)' }}
        aria-hidden="true"
      />
    )}
    <TruncatedText text={name} />
  </span>
);

export default Tag;
