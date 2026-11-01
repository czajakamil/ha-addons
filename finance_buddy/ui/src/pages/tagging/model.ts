/**
 * Logika kolejki „do otagowania" bez Reacta — żeby dało się ją testować wprost.
 *
 * Podpowiedź liczy backend (services/tagging.py): opiera się na historii całej
 * bazy, której przeglądarka celowo nie pobiera. Tutaj zostaje to, co dotyczy
 * samego przechodzenia kolejki: kursor, doładowywanie stron i klawisze.
 */

import type { UntaggedSuggestion, UntaggedTransaction } from '../../api';

export type QueueItem = UntaggedTransaction & {
  /** Pominięta w tej sesji — zostaje na liście, ale kursor ją przeskakuje. */
  skipped?: boolean;
};

export const PAGE_SIZE = 50;
/** Ile pozycji przed końcem załadowanej części dociągamy następną stronę. */
export const LOAD_AHEAD = 5;

/** Krótkie uzasadnienie podpowiedzi — pod chipem tagu. */
export const describeSuggestion = (suggestion: UntaggedSuggestion): string => {
  switch (suggestion.source) {
    case 'store_rule':
      return 'reguła sklepu';
    case 'store':
      return `najczęściej w tym sklepie (${suggestion.matches}×)`;
    case 'store_group':
      return `najczęściej w tej sieci (${suggestion.matches}×)`;
    case 'similar':
      return suggestion.similar_to
        ? `podobne do „${suggestion.similar_to}" (${suggestion.matches}×)`
        : `podobne opisy (${suggestion.matches}×)`;
  }
};

/** Ścieżka podpowiedzianego tagu albo '' — gdy brak podpowiedzi lub tag zniknął. */
export const suggestedPath = (
  item: UntaggedTransaction | undefined,
  pathById: ReadonlyMap<string, string>
): string => {
  const id = item?.suggestion?.category_id;
  return id ? pathById.get(id) ?? '' : '';
};

/**
 * Pierwsza niepominięta pozycja od `from` w przód; `items.length`, gdy takiej
 * nie ma (ekran „koniec kolejki").
 */
export const nextPendingIndex = (items: QueueItem[], from: number): number => {
  for (let index = Math.max(0, from); index < items.length; index += 1) {
    if (!items[index].skipped) return index;
  }
  return items.length;
};

/**
 * Usuwa otagowane pozycje i wylicza nowy kursor: zostaje na tym samym miejscu,
 * więc na ekran wjeżdża następna transakcja — tak jak przy zdejmowaniu kartki
 * ze stosu.
 */
export const removeTagged = (
  items: QueueItem[],
  cursor: number,
  isTagged: (item: QueueItem) => boolean
): { items: QueueItem[]; cursor: number } => {
  const removedBefore = items.slice(0, cursor).filter(isTagged).length;
  const next = items.filter((item) => !isTagged(item));
  return { items: next, cursor: nextPendingIndex(next, cursor - removedBefore) };
};

/**
 * Offset następnej strony. Otagowane pozycje znikają z kolejki również na
 * serwerze, więc offset to liczba pozycji, które wciąż są na liście (łącznie
 * z pominiętymi) — a nie liczba pobranych.
 */
export const nextOffset = (items: QueueItem[]) => items.length;

export const shouldLoadMore = (items: QueueItem[], cursor: number, total: number) =>
  items.length < total && cursor >= items.length - LOAD_AHEAD;

/** Scala doładowaną stronę, pomijając pozycje, które już są (np. po zmianie danych). */
export const appendPage = (items: QueueItem[], page: UntaggedTransaction[]): QueueItem[] => {
  const known = new Set(items.map((item) => item.id));
  return [...items, ...page.filter((item) => !known.has(item.id))];
};

export type QueueAction = 'accept' | 'edit' | 'skip' | 'next' | 'previous' | 'tagStore';

type KeyLike = Pick<KeyboardEvent, 'key' | 'ctrlKey' | 'metaKey' | 'altKey'>;

/**
 * Skróty kolejki, gdy fokus nie stoi w polu tekstowym. Litery na wzór
 * Gmaila (j/k) plus strzałki dla tych, którzy ich nie znają.
 */
export const queueKeyAction = (event: KeyLike): QueueAction | null => {
  if (event.ctrlKey || event.metaKey || event.altKey) return null;
  switch (event.key) {
    case 'Enter':
      return 'accept';
    case 't':
    case 'T':
    case '/':
      return 'edit';
    case 's':
    case 'S':
      return 'skip';
    case 'ArrowDown':
    case 'j':
    case 'J':
      return 'next';
    case 'ArrowUp':
    case 'k':
    case 'K':
      return 'previous';
    case 'w':
    case 'W':
      return 'tagStore';
    default:
      return null;
  }
};
