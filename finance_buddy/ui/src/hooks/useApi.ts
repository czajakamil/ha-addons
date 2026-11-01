import { useCallback, useEffect, useRef, useState } from 'react';
import type { Dispatch, SetStateAction } from 'react';

export type UseApiOptions = {
  /** Czy pobrać dane od razu po zamontowaniu (domyślnie true). */
  immediate?: boolean;
  /** Komunikat pokazywany użytkownikowi, gdy żądanie się nie powiedzie. */
  errorMessage?: string;
};

export type UseApiResult<T> = {
  data: T | undefined;
  loading: boolean;
  error: string | null;
  /** Ponawia żądanie; zwraca świeże dane lub undefined przy błędzie. */
  refetch: () => Promise<T | undefined>;
  /** Pozwala lokalnie zmodyfikować dane bez ponownego pobierania (np. po edycji inline). */
  setData: Dispatch<SetStateAction<T | undefined>>;
};

const DEFAULT_ERROR_MESSAGE = 'Nie udało się wczytać danych.';

/**
 * Enkapsuluje powtarzalny wzorzec pobierania danych: loading / error / data + refetch.
 * Zastępuje ręczne setLoading/try/catch/finally rozsiane po stronach.
 *
 * `fetcher` jest trzymany w refie, więc nie musi być memoizowany przez wywołującego —
 * hook nie zapętli się, nawet jeśli przekażesz świeżą funkcję przy każdym renderze.
 */
export function useApi<T>(fetcher: () => Promise<T>, options?: UseApiOptions): UseApiResult<T> {
  const { immediate = true, errorMessage = DEFAULT_ERROR_MESSAGE } = options ?? {};

  const [data, setData] = useState<T | undefined>(undefined);
  const [loading, setLoading] = useState<boolean>(immediate);
  const [error, setError] = useState<string | null>(null);

  const fetcherRef = useRef(fetcher);
  // Aktualizacja refa w efekcie (nie w trakcie renderu) — zgodnie z regułami
  // Reacta; refetch i tak jest wywoływany dopiero po zamontowaniu.
  useEffect(() => {
    fetcherRef.current = fetcher;
  });

  // Identyfikuje najnowsze żądanie, by odrzucić odpowiedzi, które „spóźniły się"
  // (uniknięcie wyścigu, gdy refetch zostanie wywołany kilka razy szybko po sobie).
  const requestIdRef = useRef(0);
  const isMountedRef = useRef(true);

  useEffect(() => {
    isMountedRef.current = true;
    return () => {
      isMountedRef.current = false;
    };
  }, []);

  const refetch = useCallback(async (): Promise<T | undefined> => {
    const requestId = ++requestIdRef.current;
    setLoading(true);
    try {
      const result = await fetcherRef.current();
      if (!isMountedRef.current || requestId !== requestIdRef.current) {
        return result;
      }
      setData(result);
      setError(null);
      return result;
    } catch (err) {
      console.error(err);
      if (isMountedRef.current && requestId === requestIdRef.current) {
        setError(errorMessage);
      }
      return undefined;
    } finally {
      if (isMountedRef.current && requestId === requestIdRef.current) {
        setLoading(false);
      }
    }
  }, [errorMessage]);

  useEffect(() => {
    if (immediate) {
      void refetch();
    }
    // celowo tylko na montowaniu — kolejne pobrania robi się przez refetch()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return { data, loading, error, refetch, setData };
}
