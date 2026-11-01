import { request } from './client';

export interface HomeEnvelope {
  category_id: string;
  category_name: string;
  available_cents: number;
  spent_cents: number;
  /** Ile było w kopercie na ten miesiąc: przeniesione + przypisane. */
  funds_cents: number;
}

export interface HomeStaleAccount {
  account_id: string;
  account_name: string;
  last_data_date: string;
  days_since: number;
}

export interface HomeRecentTransaction {
  id: string;
  date: string;
  total_cents: number;
  currency: string;
  account_name: string;
  store_name?: string | null;
  comment?: string | null;
  category_id?: string | null;
  is_transfer: boolean;
  needs_tag: boolean;
}

export interface HomeSummary {
  today: string;
  month: string;
  untagged_count: number;
  receipts_todo: number;
  ready_to_assign_cents: number;
  overspent_count: number;
  envelopes: HomeEnvelope[];
  spent_month_cents: number;
  spent_previous_month_cents: number;
  on_budget_balance_cents: number;
  last_transaction_date?: string | null;
  stale_threshold_days: number;
  stale_accounts: HomeStaleAccount[];
  recent: HomeRecentTransaction[];
}

export const getHomeSummary = () => request<HomeSummary>('/home/summary');
