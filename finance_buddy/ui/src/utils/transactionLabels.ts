// Etykiety transakcji pochodzących z importu: automatyczne teksty YNAB nie trafiają do interfejsu.
const AUTO_COMMENT = /^(imported from ynab\b|entered automatically by ynab\b)/i;
const RECONCILIATION = /reconciliation balance adjustment/i;

export const BALANCE_ADJUSTMENT_LABEL = 'Korekta salda';

/** Komentarz wpisany przez użytkownika; automatyczne komentarze z importu dają null. */
export const userComment = (comment?: string | null) => {
  const trimmed = comment?.trim();
  return trimmed && !AUTO_COMMENT.test(trimmed) ? trimmed : null;
};

export const isBalanceAdjustment = (label?: string | null) => Boolean(label && RECONCILIATION.test(label));
