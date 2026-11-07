import { request } from './client';

export interface BudgetCategoryRow {
  category_id: string;
  category_name: string;
  parent_id?: string | null;
  assigned_cents: number;
  activity_cents: number;
  available_cents: number;
  is_credit_payment: boolean;
  status?: string | null;
}

export interface BudgetMonthSummary {
  month: string;
  label: string;
  rta_cents: number;
  assigned_cents: number;
  activity_cents: number;
  available_cents: number;
  on_budget_balance_cents: number;
}

export interface BudgetMonthResponse {
  month: string;
  label: string;
  summary: BudgetMonthSummary;
  categories: BudgetCategoryRow[];
}

export interface BudgetAssignmentUpdate {
  category_id: string;
  month: string;
  assigned_cents: number;
}

export const getBudgetMonth = (month: string) =>
  request<BudgetMonthResponse>(`/budget/month?month=${encodeURIComponent(month)}`);

export const updateBudgetAssignment = (payload: BudgetAssignmentUpdate) =>
  request<BudgetAssignmentUpdate>('/budget/assignments', {
    method: 'POST',
    body: JSON.stringify(payload)
  });

export interface YnabBudgetSummary {
  id: string;
  name: string;
  last_modified_on?: string | null;
}

export interface YnabSettings {
  api_key_set: boolean;
  api_key_from_config: boolean;
  budget_id?: string | null;
  last_imported_at?: string | null;
  last_server_knowledge?: number | null;
}

export interface YnabSettingsUpdatePayload {
  api_key?: string | null;
  budget_id?: string | null;
}

export interface YnabImportResult {
  budget_id: string;
  budget_name?: string | null;
  accounts: number;
  stores: number;
  categories: number;
  transactions_inserted: number;
  transactions_skipped: number;
  stores_categorized?: number;
}

export const getYnabSettings = () => request<YnabSettings>('/ynab/settings');

export const updateYnabSettings = (payload: YnabSettingsUpdatePayload) =>
  request<YnabSettings>('/ynab/settings', {
    method: 'PUT',
    body: JSON.stringify(payload)
  });

export const listYnabBudgets = (apiKey?: string) =>
  request<YnabBudgetSummary[]>('/ynab/budgets', {
    method: 'POST',
    body: JSON.stringify(apiKey ? { api_key: apiKey } : {})
  });

export const importYnabBudget = (budgetId: string, apiKey?: string) =>
  request<YnabImportResult>('/ynab/import', {
    method: 'POST',
    body: JSON.stringify(apiKey ? { api_key: apiKey, budget_id: budgetId } : { budget_id: budgetId })
  });
