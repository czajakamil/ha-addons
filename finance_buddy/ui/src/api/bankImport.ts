import { request } from './client';
import type {
  Column,
  Decision,
  Mapping,
  RowAction,
  RowStatus
} from '../pages/bankImport/mapping';

/**
 * Import wyciągu CSV (docs/design/import-csv.md §7).
 *
 * Bezstanowo: plik idzie przy każdym kroku jako multipart, a parametry jako
 * JSON w polu formularza. Backend parsuje plik od nowa i przy zapisie
 * ponownie przepuszcza go przez deduplikację.
 */

export interface BankParseOptions {
  encoding?: string | null;
  delimiter?: string | null;
  header_row?: number | null;
  date_format?: string | null;
  decimal_separator?: ',' | '.' | null;
}

export interface BankMapping extends Mapping {
  header_names?: Record<string, string>;
}

export interface BankImportProfile {
  id: string;
  name: string;
  account_id?: string | null;
  parse_options: BankParseOptions;
  mapping: BankMapping;
  last_used_at?: string | null;
}

export interface BankPreviousImport {
  batch_id: string;
  created_at: string;
  account_id: string;
  account_name: string;
}

export interface BankInspectResponse {
  options: BankParseOptions;
  columns: Column[];
  row_count: number;
  file_sha256: string;
  profile?: BankImportProfile | null;
  profile_match?: 'full' | 'partial' | null;
  profile_mismatched_columns: number[];
  previous_import?: BankPreviousImport | null;
}

export interface BankMatchInfo {
  transaction_id: string;
  account_id: string;
  account_name: string;
  date: string;
  amount_cents: number;
  label: string;
  source: 'receipt' | 'bank' | 'other';
}

export interface BankPreviewRow {
  index: number;
  line: number;
  status: RowStatus;
  reason?: string | null;
  date?: string | null;
  amount_cents?: number | null;
  currency?: string | null;
  payee?: string | null;
  memo?: string | null;
  suggested_action: RowAction;
  match?: BankMatchInfo | null;
}

export interface BankPreviewResponse {
  options: BankParseOptions;
  file_sha256: string;
  rows: BankPreviewRow[];
  counts: Partial<Record<RowStatus, number>>;
  previous_import?: BankPreviousImport | null;
}

export interface BankImportSpec {
  account_id: string;
  options: BankParseOptions;
  mapping: BankMapping;
}

export interface BankCommitRequest extends BankImportSpec {
  file_sha256: string;
  decisions: Decision[];
  profile_id?: string | null;
  save_profile?: { name: string; bind_account: boolean } | null;
  file_name?: string | null;
}

export interface BankImportBatch {
  id: string;
  account_id: string;
  account_name: string;
  profile_id?: string | null;
  file_name: string;
  date_from?: string | null;
  date_to?: string | null;
  created_count: number;
  merged_count: number;
  skipped_count: number;
  created_at: string;
}

const form = (file: File, field: string, payload: unknown) => {
  const body = new FormData();
  body.append('file', file);
  body.append(field, JSON.stringify(payload));
  return body;
};

export const inspectBankFile = (file: File, options: BankParseOptions = {}) =>
  request<BankInspectResponse>('/bank-import/inspect', {
    method: 'POST',
    body: form(file, 'options', options)
  });

export const previewBankFile = (file: File, spec: BankImportSpec) =>
  request<BankPreviewResponse>('/bank-import/preview', {
    method: 'POST',
    body: form(file, 'spec', spec)
  });

export const commitBankFile = (file: File, payload: BankCommitRequest) =>
  request<BankImportBatch>('/bank-import/commit', {
    method: 'POST',
    body: form(file, 'request', payload)
  });

export const getBankImportProfiles = () =>
  request<BankImportProfile[]>('/bank-import/profiles');

export const deleteBankImportProfile = (id: string) =>
  request<void>(`/bank-import/profiles/${id}`, { method: 'DELETE' });

export const getBankImportBatches = (accountId?: string) =>
  request<BankImportBatch[]>(
    `/bank-import/batches${accountId ? `?account_id=${encodeURIComponent(accountId)}` : ''}`
  );

export const undoBankImportBatch = (id: string) =>
  request<{ deleted: number; unlinked: number }>(`/bank-import/batches/${id}`, {
    method: 'DELETE'
  });
