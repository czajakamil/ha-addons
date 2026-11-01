import { API_BASE_URL, ApiError, getAuthToken, request } from './client';
import type { ReceiptCheckIssue, ReceiptCheckRules } from '../utils/receiptChecks';
import type { BankMatchInfo } from './bankImport';

/** Pozycja etykiety — tak, jak jest wydrukowana na paragonie (kwoty dodatnie, rabaty ujemne). */
export interface ReceiptDraftItem {
  raw_name: string;
  product_code?: string | null;
  quantity?: number | null;
  unit?: string | null;
  unit_price_cents?: number | null;
  line_total_cents?: number | null;
  discount_cents?: number | null;
  discount_label?: string | null;
  tax_code?: string | null;
}

export interface ReceiptDraftPayload {
  store_name?: string | null;
  nip?: string | null;
  /** Tekst `RRRR-MM-DDTGG:MM` — celowo nie `Date`, patrz utils/receiptChecks. */
  happened_at?: string | null;
  total_cents?: number | null;
  currency: string;
  items: ReceiptDraftItem[];
  discounts_declared_cents?: number | null;
  receipt_discount_cents?: number | null;
  notes?: string | null;
}

export type ReceiptDraftStatus = 'todo' | 'skipped' | 'committed';

export interface ReceiptDraftSummary {
  id: string;
  status: ReceiptDraftStatus;
  source_name: string;
  page_number: number;
  scan_date?: string | null;
  store_name?: string | null;
  happened_at?: string | null;
  total_cents?: number | null;
  currency: string;
  item_count: number;
  issues: ReceiptCheckIssue[];
  transaction_id?: string | null;
  error?: string | null;
  created_at: string;
}

export interface ReceiptDraftDetail extends ReceiptDraftSummary {
  payload: ReceiptDraftPayload;
  ocr_payload?: ReceiptDraftPayload | null;
  raw_text?: string | null;
  matched_store_id?: string | null;
  matched_store_name?: string | null;
  confidence: number;
  image_url?: string | null;
}

export interface ReceiptDraftCounts {
  total: number;
  todo: number;
  review: number;
  skipped: number;
  committed: number;
}

export interface ReceiptDraftListResponse {
  drafts: ReceiptDraftSummary[];
  counts: ReceiptDraftCounts;
  rules: ReceiptCheckRules;
}

/** Kubełki po statusie, rozłączne. Zastrzeżenia to cecha wiersza, nie filtr. */
export type ReceiptQueueFilter = 'all' | 'todo' | 'done' | 'skipped';

export interface ReceiptDraftCommitItem {
  raw_name: string;
  quantity: number;
  unit?: string | null;
  unit_price_cents?: number | null;
  line_total_cents?: number | null;
  product_id?: string | null;
  new_product_name?: string | null;
  category_path?: string | null;
  learn_alias?: boolean;
}

export interface ReceiptDraftCommitRequest {
  account_id: string;
  store_id?: string | null;
  category_id?: string | null;
  comment?: string | null;
  payload: ReceiptDraftPayload;
  items: ReceiptDraftCommitItem[];
  /** Kody blokad przyjętych świadomie — imiennie, nie flagą „zapisz mimo wszystko". */
  acknowledged_issues?: string[];
  /** Transakcja z wyciągu, do której podpinamy paragon zamiast tworzyć nową. */
  merge_into_transaction_id?: string | null;
}

export interface ReceiptDraftCommitResult {
  transaction_id: string;
  draft: ReceiptDraftDetail;
}

export const getReceiptDrafts = (filter: ReceiptQueueFilter = 'all') =>
  request<ReceiptDraftListResponse>(`/receipt-drafts?filter=${filter}`);

export const getReceiptDraft = (draftId: string) =>
  request<ReceiptDraftDetail>(`/receipt-drafts/${draftId}`);

export const saveReceiptDraft = (
  draftId: string,
  body: { payload?: ReceiptDraftPayload; status?: ReceiptDraftStatus }
) =>
  request<ReceiptDraftDetail>(`/receipt-drafts/${draftId}`, {
    method: 'PATCH',
    body: JSON.stringify(body)
  });

export const commitReceiptDraft = (draftId: string, body: ReceiptDraftCommitRequest) =>
  request<ReceiptDraftCommitResult>(`/receipt-drafts/${draftId}/commit`, {
    method: 'POST',
    body: JSON.stringify(body)
  });

/** Transakcje z wyciągu pasujące do paragonu (konto, kwota, data ±4 dni). */
export const getReceiptBankMatches = (
  draftId: string,
  params: { account_id: string; date: string; total_cents: number }
) => {
  const query = new URLSearchParams({
    account_id: params.account_id,
    date: params.date,
    total_cents: String(params.total_cents)
  });
  return request<BankMatchInfo[]>(`/receipt-drafts/${draftId}/bank-matches?${query}`);
};

export const deleteReceiptDraft = (draftId: string) =>
  request<void>(`/receipt-drafts/${draftId}`, { method: 'DELETE' });

/**
 * Zastrzeżenia z odpowiedzi 422 bramki zapisu; pusta lista dla każdego innego błędu.
 *
 * Bramka odzywa się tylko wtedy, gdy formularz czegoś nie pokazał — czyli gdy
 * wspólne fikstury nie objęły przypadku. Dlatego nie wystarczy pokazać „błąd
 * serwera": trzeba pokazać, co dokładnie zatrzymało zapis.
 */
export const commitIssuesFromError = (error: unknown): ReceiptCheckIssue[] => {
  if (!(error instanceof ApiError) || error.status !== 422) return [];
  const detail = (error.data as { detail?: { issues?: ReceiptCheckIssue[] } } | null)?.detail;
  return detail?.issues ?? [];
};

/** Adres obrazu strony — z tokenem w nagłówku nie da się go wstawić prosto w `<img src>`. */
export const fetchReceiptDraftImage = async (draftId: string): Promise<string> => {
  const token = getAuthToken();
  const response = await fetch(`${API_BASE_URL}/receipt-drafts/${draftId}/image`, {
    headers: token ? { Authorization: `Bearer ${token}` } : undefined
  });
  if (!response.ok) throw new Error('Nie udało się pobrać obrazu paragonu.');
  return URL.createObjectURL(await response.blob());
};

export const uploadReceiptsWithProgress = (
  files: File[],
  opts: {
    onUploadProgress?: (loaded: number, total: number) => void;
    onStateChange?: (state: 'uploading' | 'processing' | 'done') => void;
    signal?: AbortSignal;
  } = {}
) =>
  new Promise<ReceiptDraftListResponse>((resolve, reject) => {
    const formData = new FormData();
    files.forEach((file) => formData.append('files', file));

    const xhr = new XMLHttpRequest();
    xhr.open('POST', `${API_BASE_URL}/receipt-drafts`);
    xhr.responseType = 'json';
    const token = getAuthToken();
    if (token) {
      xhr.setRequestHeader('Authorization', `Bearer ${token}`);
    }

    opts.onStateChange?.('uploading');

    if (xhr.upload && opts.onUploadProgress) {
      xhr.upload.onprogress = (event) => {
        if (event.lengthComputable) {
          opts.onUploadProgress?.(event.loaded, event.total);
        }
      };
    }

    xhr.onreadystatechange = () => {
      if (xhr.readyState === XMLHttpRequest.HEADERS_RECEIVED) {
        opts.onStateChange?.('processing');
      }
    };

    xhr.onerror = () => reject(new Error('Nie udało się przesłać plików.'));
    xhr.onabort = () => reject(new DOMException('Aborted', 'AbortError'));

    xhr.onload = () => {
      opts.onStateChange?.('done');
      if (xhr.status >= 200 && xhr.status < 300) {
        const payload = xhr.response ?? JSON.parse(xhr.responseText || '{}');
        resolve(payload as ReceiptDraftListResponse);
      } else {
        reject(new Error(xhr.responseText || xhr.statusText || 'Błąd serwera'));
      }
    };

    if (opts.signal) {
      if (opts.signal.aborted) {
        xhr.abort();
        return;
      }
      opts.signal.addEventListener('abort', () => xhr.abort());
    }

    xhr.send(formData);
  });
