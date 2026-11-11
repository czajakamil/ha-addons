import { request } from './client';

export type AccountKind = string;

export interface Account {
  id: string;
  name: string;
  currency: string;
  comment?: string | null;
  category_id?: string | null;
  account_kind: AccountKind;
  icon?: string | null;
  color?: string | null;
  balance_cents: number;
  is_closed: boolean;
  is_deleted?: boolean;
  is_on_budget: boolean;
  /** Przypięte do paska bocznego. */
  is_favorite?: boolean;
}

export interface YnabBudgetSnapshot {
  data: Record<string, unknown>;
}

export interface AccountsResponse {
  accounts: Account[];
  ynab_budget?: YnabBudgetSnapshot | null;
}

export interface AccountUpdate {
  name?: string;
  currency?: string;
  comment?: string | null;
  category_id?: string | null;
  account_kind?: AccountKind;
  icon?: string | null;
  color?: string | null;
  is_closed?: boolean;
  is_on_budget?: boolean;
  is_favorite?: boolean;
}

export interface AccountCreate {
  name: string;
  currency: string;
  comment?: string | null;
  category_id?: string | null;
  account_kind?: AccountKind;
  icon?: string | null;
  color?: string | null;
  is_closed?: boolean;
  is_on_budget?: boolean;
}

export interface AccountBalanceAdjustmentPayload {
  target_balance_cents: number;
}

/** Warunki kredytu w jednym miesiącu. YNAB trzyma je jako szereg czasowy. */
export interface AccountDebtTerms {
  month: string;
  /** Punkty bazowe: 6,55% to 655. Dzielimy przez 100, żeby dostać procenty. */
  interest_rate_bps: number | null;
  minimum_payment_cents: number | null;
  escrow_cents: number | null;
}

export interface DebtPaymentSplit {
  month: string;
  opening_balance_cents: number;
  payment_cents: number;
  interest_cents: number;
  principal_cents: number;
  closing_balance_cents: number;
}

/**
 * `ok` — prognoza policzona. Pozostałe statusy to powody, dla których jej nie
 * ma, i każdy z nich znaczy co innego dla użytkownika:
 * `missing_terms` (YNAB nie podał stopy albo raty), `never_amortizes` (rata nie
 * pokrywa odsetek), `exceeds_horizon` (spłata poza 50-letnim horyzontem).
 */
export type DebtForecastStatus = 'ok' | 'missing_terms' | 'never_amortizes' | 'exceeds_horizon';

export interface DebtForecast {
  status: DebtForecastStatus;
  months: number | null;
  payoff_month: string | null;
  total_interest_cents: number | null;
}

export interface AccountDebt {
  account_id: string;
  /** Saldo jak wszędzie indziej: dla długu ujemne. */
  balance_cents: number;
  /** To samo saldo jako dodatnia wielkość długu. */
  outstanding_cents: number;
  current_terms: AccountDebtTerms | null;
  terms: AccountDebtTerms[];
  forecast: DebtForecast;
  schedule: DebtPaymentSplit[];
}

export const getAccounts = () => request<AccountsResponse>('/accounts');
export const createAccount = (payload: AccountCreate) =>
  request<Account>('/accounts', { method: 'POST', body: JSON.stringify(payload) });
export const updateAccount = (id: string, payload: AccountUpdate) =>
  request<Account>(`/accounts/${id}`, { method: 'PATCH', body: JSON.stringify(payload) });
export const adjustAccountBalance = (id: string, payload: AccountBalanceAdjustmentPayload) =>
  request<Account>(`/accounts/${id}/adjust-balance`, { method: 'POST', body: JSON.stringify(payload) });
// Warunki kredytu i prognoza spłaty. Działa dla każdego konta — konto bez
// warunków zwraca pustą historię i `forecast.status === 'missing_terms'`.
export const getAccountDebt = (id: string) => request<AccountDebt>(`/accounts/${id}/debt`);
// Trwałe usunięcie konta wraz ze wszystkimi powiązanymi transakcjami (nieodwracalne).
export const deleteAccount = (id: string) =>
  request<void>(`/accounts/${id}`, { method: 'DELETE' });
