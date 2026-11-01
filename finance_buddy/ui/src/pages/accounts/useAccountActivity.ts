// Data ostatniej transakcji każdego konta. API nie zwraca jej na liście kont, więc pytamy o najnowszą
// transakcję osobno: null to konto bez transakcji, brak klucza to dane jeszcze w drodze.
import { useEffect, useState } from 'react';
import { getTransactions, type Account } from '../../api';

export const useAccountActivity = (accounts: Account[]) => {
  const [activity, setActivity] = useState<ReadonlyMap<string, string | null>>(() => new Map());
  const ids = accounts.map((account) => account.id).join(',');

  useEffect(() => {
    if (!ids) return;
    let active = true;
    void Promise.all(
      ids.split(',').map(async (id) => {
        const [latest] = await getTransactions({ account_id: id, limit: 1 });
        return [id, latest?.date ?? null] as const;
      })
    )
      .then((entries) => {
        if (active) setActivity(new Map(entries));
      })
      .catch((err) => console.error(err));
    return () => {
      active = false;
    };
  }, [ids]);

  return activity;
};
