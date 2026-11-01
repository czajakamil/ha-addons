import { resolveApiBaseUrl } from './basePath';

export const API_BASE_URL = resolveApiBaseUrl(
  (import.meta.env.VITE_API_BASE_URL as string | undefined) ?? 'http://localhost:8000'
);

const AUTH_TOKEN_KEY = 'finance.auth.token';

export const getAuthToken = () => localStorage.getItem(AUTH_TOKEN_KEY);
export const setAuthToken = (token: string) => localStorage.setItem(AUTH_TOKEN_KEY, token);
export const clearAuthToken = () => localStorage.removeItem(AUTH_TOKEN_KEY);

/**
 * Błąd odpowiedzi z kodem statusu i sparsowanym ciałem.
 *
 * `message` zostaje surowym tekstem odpowiedzi, żeby dotychczasowe `catch`-e
 * czytające `err.message` działały bez zmian; `data` jest dla tych wywołań,
 * które potrzebują struktury — jak bramka zapisu paragonu, odsyłająca 422 z
 * listą zastrzeżeń do pokazania przy konkretnych polach.
 */
export class ApiError extends Error {
  readonly status: number;
  readonly data: unknown;

  constructor(status: number, body: string, data: unknown) {
    super(body || `Błąd serwera (${status})`);
    this.name = 'ApiError';
    this.status = status;
    this.data = data;
  }
}

export async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const isFormData = init?.body instanceof FormData;
  const headers = new Headers(init?.headers ?? {});
  if (!isFormData && !headers.has('Content-Type')) {
    headers.set('Content-Type', 'application/json');
  }
  const token = getAuthToken();
  if (token && !headers.has('Authorization')) {
    headers.set('Authorization', `Bearer ${token}`);
  }
  const response = await fetch(`${API_BASE_URL}${path}`, {
    ...init,
    headers
  });

  if (!response.ok) {
    const text = await response.text();
    let data: unknown = null;
    try {
      data = text ? JSON.parse(text) : null;
    } catch {
      data = null;
    }
    throw new ApiError(response.status, text || response.statusText, data);
  }

  if (response.status === 204) {
    return undefined as T;
  }

  return response.json() as Promise<T>;
}

/**
 * Pobiera zasób binarny (załącznik, podgląd paragonu) z nagłówkiem autoryzacji.
 *
 * `<img src>` i `<a href>` nie wysyłają nagłówka `Authorization`, więc chronionych
 * plików nie da się podać przeglądarce bezpośrednio adresem API — trzeba je
 * pobrać przez `fetch` i udostępnić jako `blob:` URL (patrz `useAuthorizedObjectUrl`).
 */
export async function requestBlob(path: string, init?: RequestInit): Promise<Blob> {
  const headers = new Headers(init?.headers ?? {});
  const token = getAuthToken();
  if (token && !headers.has('Authorization')) {
    headers.set('Authorization', `Bearer ${token}`);
  }
  const url = /^https?:\/\//i.test(path) ? path : `${API_BASE_URL}${path}`;
  const response = await fetch(url, { ...init, headers });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || response.statusText);
  }

  return response.blob();
}

export const getHealth = () => request<{ status: string }>('/health');
