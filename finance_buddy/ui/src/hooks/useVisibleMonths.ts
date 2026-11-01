import { useMediaQuery } from './useMediaQuery';

const COMPACT_MONTHS = 6;

/** Na telefonie wykresy miesięczne pokazują tylko ostatnie pół roku. */
export const useVisibleMonths = <T,>(months: readonly T[]): readonly T[] => {
  const isCompact = useMediaQuery('(max-width: 699px)');
  return isCompact ? months.slice(-COMPACT_MONTHS) : months;
};
