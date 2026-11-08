import { request } from './client';

/** Synchronizacja z bankiem przez Enable Banking (docs/design/enable-banking.md). */

export interface BankSyncSettings {
  configured: boolean;
  app_id?: string | null;
  app_name?: string | null;
  redirect_url?: string | null;
}

export interface BankAspsp {
  name: string;
  country: string;
  logo?: string | null;
  max_consent_days: number;
  psu_types: string[];
}

export interface BankConnectionAccount {
  id: string;
  uid: string;
  name?: string | null;
  iban_masked?: string | null;
  currency?: string | null;
  account_id?: string | null;
  account_name?: string | null;
  account_balance_cents?: number | null;
  sync_from?: string | null;
  last_synced_at?: string | null;
  last_booked_date?: string | null;
  bank_balance_cents?: number | null;
  bank_balance_at?: string | null;
}

export interface BankConnection {
  id: string;
  aspsp_name: string;
  aspsp_country: string;
  status: 'pending' | 'active' | 'expired';
  valid_until?: string | null;
  last_synced_at?: string | null;
  last_error?: string | null;
  created_at: string;
  accounts: BankConnectionAccount[];
}

export interface BankSyncAccountResult {
  connection_account_id: string;
  account_name?: string | null;
  created: number;
  merged: number;
  batch_id?: string | null;
  error?: string | null;
}

export interface BankSyncResult {
  connection: BankConnection;
  accounts: BankSyncAccountResult[];
}

export const getBankSyncSettings = () => request<BankSyncSettings>('/bank-sync/settings');

export const updateBankSyncSettings = (app_id: string, private_key_pem: string) =>
  request<BankSyncSettings>('/bank-sync/settings', {
    method: 'PUT',
    body: JSON.stringify({ app_id, private_key_pem })
  });

export const deleteBankSyncSettings = () =>
  request<void>('/bank-sync/settings', { method: 'DELETE' });

export const listBanks = (country = 'PL') =>
  request<BankAspsp[]>(`/bank-sync/banks?country=${encodeURIComponent(country)}`);

export const listBankConnections = () => request<BankConnection[]>('/bank-sync/connections');

export const startBankConnection = (aspsp_name: string, country = 'PL') =>
  request<{ connection_id: string; url: string; valid_until: string }>('/bank-sync/connections', {
    method: 'POST',
    body: JSON.stringify({ aspsp_name, country })
  });

export const completeBankConnection = (connectionId: string, redirect: string) =>
  request<BankConnection>(`/bank-sync/connections/${connectionId}/complete`, {
    method: 'POST',
    body: JSON.stringify({ redirect })
  });

export const syncBankConnection = (connectionId: string) =>
  request<BankSyncResult>(`/bank-sync/connections/${connectionId}/sync`, {
    method: 'POST'
  });

export const deleteBankConnection = (connectionId: string) =>
  request<void>(`/bank-sync/connections/${connectionId}`, { method: 'DELETE' });

export const updateBankAccountLink = (
  linkId: string,
  payload: { account_id?: string | null; sync_from?: string | null }
) =>
  request<BankConnection>(`/bank-sync/accounts/${linkId}`, {
    method: 'PATCH',
    body: JSON.stringify(payload)
  });
