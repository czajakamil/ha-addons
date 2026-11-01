// Komunikat z backendu, np. kto już ma ten alias. FastAPI przy walidacji zwraca listę, nie tekst.
import { ApiError } from '../../api';

export const errorDetail = (err: unknown) => {
  const detail = err instanceof ApiError ? (err.data as { detail?: unknown } | null)?.detail : null;
  return typeof detail === 'string' ? detail : null;
};
