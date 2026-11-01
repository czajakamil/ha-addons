import { useEffect, useState } from 'react';
import { requestBlob } from '../api/client';

export type UseAuthorizedObjectUrlOptions = {
  /** Komunikat pokazywany użytkownikowi, gdy pliku nie udało się pobrać. */
  errorMessage?: string;
};

export type UseAuthorizedObjectUrlResult = {
  /** `blob:` URL gotowy do wstawienia w `<img src>` / `<a href>`; null zanim się pobierze. */
  url: string | null;
  loading: boolean;
  error: string | null;
};

const DEFAULT_ERROR_MESSAGE = 'Nie udało się wczytać pliku.';

/**
 * Udostępnia chroniony plik z API jako `blob:` URL.
 *
 * Endpointy plikowe wymagają nagłówka `Authorization`, którego przeglądarka nie
 * wyśle przy `<img src>` ani `<a href>` — dlatego plik pobieramy `fetch`em
 * (patrz `requestBlob`) i zamieniamy na obiektowy URL. Hook sam zwalnia URL
 * i przerywa żądanie przy odmontowaniu lub zmianie ścieżki.
 *
 * `path` jest względny wobec `API_BASE_URL` (np. `/transactions/…/attachments/…`).
 * Przekazanie `null`/`undefined` wyłącza pobieranie.
 */
export function useAuthorizedObjectUrl(
  path: string | null | undefined,
  options?: UseAuthorizedObjectUrlOptions
): UseAuthorizedObjectUrlResult {
  const errorMessage = options?.errorMessage ?? DEFAULT_ERROR_MESSAGE;

  const [url, setUrl] = useState<string | null>(null);
  const [loading, setLoading] = useState<boolean>(Boolean(path));
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!path) {
      setUrl(null);
      setLoading(false);
      setError(null);
      return;
    }

    let cancelled = false;
    let objectUrl: string | null = null;
    const controller = new AbortController();

    // Poprzedni URL zostaje zwolniony w cleanupie, więc nie może zostać w stanie —
    // inaczej po zmianie `path` pokazalibyśmy przez chwilę martwy `blob:`.
    setUrl(null);
    setLoading(true);
    setError(null);

    requestBlob(path, { signal: controller.signal })
      .then((blob) => {
        if (cancelled) return;
        objectUrl = URL.createObjectURL(blob);
        setUrl(objectUrl);
      })
      .catch((err) => {
        if (cancelled) return;
        console.error(err);
        setUrl(null);
        setError(errorMessage);
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => {
      cancelled = true;
      controller.abort();
      if (objectUrl) {
        URL.revokeObjectURL(objectUrl);
      }
    };
  }, [path, errorMessage]);

  return { url, loading, error };
}
