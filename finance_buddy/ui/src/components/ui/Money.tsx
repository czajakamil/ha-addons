// Jedyny komponent kwot: tabular-nums, minus U+2212, kolor zależny od znaczenia, nie od znaku.
import { formatMoney } from '../../utils/format';

// auto: wpływ na zielono, wydatek neutralnie. neutral: bez koloru dla dodatnich.
// negative/warning/positive: stan wynikający z budżetu, np. przekroczony limit.
export type MoneyTone = 'auto' | 'neutral' | 'positive' | 'negative' | 'warning';

type MoneyProps = {
  cents: number;
  currency?: string;
  tone?: MoneyTone;
  // Tło w kolorze stanu, tylko dla kwot różnych od zera
  chip?: boolean;
  className?: string;
};

const resolveTone = (cents: number, tone: MoneyTone) => {
  if (cents === 0) return 'zero';
  if (tone === 'auto') return cents > 0 ? 'positive' : 'neutral';
  return tone;
};

export const formatSignedMoney = (cents: number, currency = 'PLN') =>
  formatMoney(cents, currency).replace('-', '−');

const Money = ({ cents, currency = 'PLN', tone = 'auto', chip = false, className }: MoneyProps) => {
  const classes = ['money', `money--${resolveTone(cents, tone)}`];
  if (chip) classes.push('money--chip');
  if (className) classes.push(className);
  return <span className={classes.join(' ')}>{formatSignedMoney(cents, currency)}</span>;
};

export default Money;
