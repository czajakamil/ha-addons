import { request } from './client';

export interface MonthlyCategorySummary {
  category_id?: string | null;
  category_name?: string | null;
  total_cents: number;
  total_amount: number;
}

export interface SpendingBreakdownEntry {
  category_id?: string | null;
  category_name?: string | null;
  total_cents: number;
  total_amount: number;
  percentage: number;
  transaction_count?: number;
}

export interface SpendingOutflowInfo {
  store_name?: string | null;
  comment?: string | null;
  category_name?: string | null;
  date: string;
  total_cents: number;
  total_amount: number;
}

export interface SpendingBreakdownReport {
  total_spending_cents: number;
  total_spending_amount: number;
  entries: SpendingBreakdownEntry[];
  largest_outflow?: SpendingOutflowInfo | null;
}

export interface MonthlySpendingPoint {
  month: string;
  label: string;
  total_spending_cents: number;
  total_spending_amount: number;
}

export interface SpendingTrendsReport {
  total_spending_cents: number;
  average_monthly_spending_cents: number;
  months: MonthlySpendingPoint[];
}

export interface IncomeExpenseCategoryRow {
  category_id?: string | null;
  category_name?: string | null;
  total_cents: number;
  total_amount: number;
}

export interface IncomeExpenseMonthRow {
  month: string;
  label: string;
  income_cents: number;
  expense_cents: number;
  net_cents: number;
  income_categories: IncomeExpenseCategoryRow[];
  expense_categories: IncomeExpenseCategoryRow[];
}

export interface IncomeExpenseReport {
  total_income_cents: number;
  total_expense_cents: number;
  months: IncomeExpenseMonthRow[];
}

export interface NetWorthPoint {
  month: string;
  label: string;
  assets_cents: number;
  debts_cents: number;
  net_worth_cents: number;
  change_cents: number;
}

export interface NetWorthAccountBalance {
  account_id: string;
  account_kind: string;
  category_id?: string | null;
  balance_cents: number;
}

export interface NetWorthReport {
  months: NetWorthPoint[];
  latest_assets_cents: number;
  latest_debts_cents: number;
  latest_net_worth_cents: number;
  accounts: NetWorthAccountBalance[];
}

export interface AgeOfMoneyPoint {
  month: string;
  label: string;
  age_of_money_days?: number | null;
  on_budget_balance_cents: number;
  average_daily_outflow_cents: number;
  sample_start_date: string;
  sample_end_date: string;
  sampled_days: number;
}

export interface AgeOfMoneyReport {
  on_budget_balance_cents: number;
  average_daily_outflow_cents: number;
  age_of_money_days?: number | null;
  sample_start_date: string;
  sample_end_date: string;
  sampled_days: number;
  history: AgeOfMoneyPoint[];
}

export interface ReportsDashboardResponse {
  start_date: string;
  end_date: string;
  spending_breakdown: SpendingBreakdownReport;
  spending_trends: SpendingTrendsReport;
  net_worth: NetWorthReport;
  income_vs_expense: IncomeExpenseReport;
  age_of_money: AgeOfMoneyReport;
}

export const getMonthlyCategoryReport = (year: number, month: number) =>
  request<MonthlyCategorySummary[]>(`/reports/monthly-category?year=${year}&month=${month}`);

export interface ReportsDashboardParams {
  start_date?: string;
  end_date?: string;
  age_of_money_window_days?: number;
  account_ids?: string[];
  category_ids?: string[];
}

export const getReportsDashboard = (params: ReportsDashboardParams = {}) => {
  const search = new URLSearchParams();
  if (params.start_date) search.set('start_date', params.start_date);
  if (params.end_date) search.set('end_date', params.end_date);
   if (params.account_ids) params.account_ids.forEach((id) => search.append('account_ids', id));
   if (params.category_ids) params.category_ids.forEach((id) => search.append('category_ids', id));
  if (typeof params.age_of_money_window_days === 'number') {
    search.set('age_of_money_window_days', params.age_of_money_window_days.toString());
  }
  const query = search.toString();
  return request<ReportsDashboardResponse>(`/reports/dashboard${query ? `?${query}` : ''}`);
};
