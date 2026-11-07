// Licznik transakcji „do otagowania" współdzielony przez nawigację i strony.
//
// Jeden stan na moduł zamiast kontekstu: licznik czyta pasek boczny i nagłówek
// Transakcji, a zmienia go kolejka — każda z tych stron woła
// `refreshUntaggedCount()` po zapisie i wszyscy subskrybenci dostają nową liczbę.
import { useSyncExternalStore } from 'react';
import { getUntaggedCount } from '../api';

let count: number | null = null;
let inFlight: Promise<void> | null = null;
const listeners = new Set<() => void>();

const emit = () => listeners.forEach((listener) => listener());

const subscribe = (listener: () => void) => {
  listeners.add(listener);
  return () => listeners.delete(listener);
};

const getSnapshot = () => count;

/** Pobiera świeżą liczbę z backendu; równoległe wywołania dzielą jedno żądanie. */
export const refreshUntaggedCount = (): Promise<void> => {
  if (inFlight) return inFlight;
  inFlight = getUntaggedCount()
    .then(({ count: next }) => {
      count = next;
      emit();
    })
    .catch((err) => {
      // Licznik to dodatek — błąd nie może psuć nawigacji.
      console.warn('Nie udało się pobrać licznika transakcji bez kategorii', err);
    })
    .finally(() => {
      inFlight = null;
    });
  return inFlight;
};

/** Lokalna korekta po zapisie, zanim przyjdzie odpowiedź z backendu. */
export const setUntaggedCount = (next: number) => {
  count = Math.max(0, next);
  emit();
};

/** Bieżąca liczba (null, dopóki nikt jej nie pobrał). Pobranie zleca wołający. */
export const useUntaggedCount = (): number | null =>
  useSyncExternalStore(subscribe, getSnapshot, getSnapshot);
