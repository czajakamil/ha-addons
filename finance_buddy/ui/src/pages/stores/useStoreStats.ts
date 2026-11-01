// Statystyki listy sklepów. Jedno przejście po wszystkich transakcjach zamiast żądania na sklep:
// koszt zależy od liczby transakcji (1 + T/200 żądań), a nie od liczby sklepów.
import { useCallback, useEffect, useRef, useState } from 'react';
import { getTransactionsPage, type Transaction } from '../../api';

const PAGE_SIZE = 200;
const CONCURRENCY = 4;

const fetchAllTransactions = async () => {
  const first = await getTransactionsPage({ offset: 0, limit: PAGE_SIZE });
  const byId = new Map(first.items.map((tx) => [tx.id, tx]));
  const offsets: number[] = [];
  for (let offset = PAGE_SIZE; offset < first.total; offset += PAGE_SIZE) offsets.push(offset);
  for (let start = 0; start < offsets.length; start += CONCURRENCY) {
    const pages = await Promise.all(
      offsets.slice(start, start + CONCURRENCY).map((offset) => getTransactionsPage({ offset, limit: PAGE_SIZE }))
    );
    // Transakcja dodana w trakcie przesuwa strony, więc ten sam wiersz może przyjść dwa razy
    pages.forEach((page) => page.items.forEach((tx) => byId.set(tx.id, tx)));
  }
  return Array.from(byId.values());
};

export const useAllTransactions = () => {
  const [transactions, setTransactions] = useState<Transaction[] | null>(null);
  const [failed, setFailed] = useState(false);
  const seqRef = useRef(0);

  const load = useCallback(async () => {
    const seq = ++seqRef.current;
    setFailed(false);
    try {
      const all = await fetchAllTransactions();
      if (seq === seqRef.current) setTransactions(all);
    } catch (err) {
      console.error(err);
      if (seq === seqRef.current) setFailed(true);
    }
  }, []);

  useEffect(() => {
    void load();
    return () => {
      seqRef.current += 1;
    };
  }, [load]);

  return { transactions, failed, reload: load };
};
