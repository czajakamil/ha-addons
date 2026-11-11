// Ulubione konta przypięte do paska bocznego, współdzielone przez całą aplikację.
//
// Jeden stan na moduł, tak jak licznik „do otagowania”: pasek boczny czyta
// listę, a zapis konta (useAccountSave) i zmiana trasy wołają
// `refreshFavoriteAccounts()`, żeby saldo i przypięcia były świeże.
import { useSyncExternalStore } from 'react';
import { type Account, getAccounts } from '../api';

let favorites: Account[] = [];
let inFlight: Promise<void> | null = null;
const listeners = new Set<() => void>();

const emit = () => listeners.forEach((listener) => listener());

const subscribe = (listener: () => void) => {
  listeners.add(listener);
  return () => listeners.delete(listener);
};

const getSnapshot = () => favorites;

const byName = (a: Account, b: Account) => a.name.localeCompare(b.name, 'pl');

/** Pobiera świeże salda z backendu; równoległe wywołania dzielą jedno żądanie. */
export const refreshFavoriteAccounts = (): Promise<void> => {
  if (inFlight) return inFlight;
  inFlight = getAccounts()
    .then(({ accounts }) => {
      favorites = accounts.filter((account) => account.is_favorite && !account.is_deleted).sort(byName);
      emit();
    })
    .catch((err) => {
      // Przypięte konta to dodatek — błąd nie może psuć nawigacji.
      console.warn('Nie udało się pobrać ulubionych kont', err);
    })
    .finally(() => {
      inFlight = null;
    });
  return inFlight;
};

/** Przypięte konta posortowane po nazwie. Pobranie zleca wołający. */
export const useFavoriteAccounts = (): Account[] => useSyncExternalStore(subscribe, getSnapshot, getSnapshot);
