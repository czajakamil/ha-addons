import { useEffect, useState } from 'react';
import { useLocation } from 'react-router-dom';
import { getReceiptTodoCount } from '../api';

/**
 * Liczba paragonów „do przejrzenia" przy linku w pasku bocznym.
 *
 * Szara, nie ostrzegawcza: to informacja o kolejce, nie problem do naprawy.
 * Odświeżana przy każdej zmianie strony — przejrzenie paragonu kończy się
 * nawigacją albo zmianą widoku, a COUNT na serwerze jest tani.
 */
const ReceiptsNavBadge = () => {
  const [count, setCount] = useState<number | null>(null);
  const { pathname } = useLocation();

  useEffect(() => {
    let cancelled = false;
    getReceiptTodoCount()
      .then(({ count: next }) => {
        if (!cancelled) setCount(next);
      })
      .catch((err) => {
        // Licznik to dodatek — błąd nie może psuć nawigacji.
        console.warn('Nie udało się pobrać licznika paragonów', err);
      });
    return () => {
      cancelled = true;
    };
  }, [pathname]);

  if (!count) return null;
  const label = `${count} do przejrzenia`;
  return (
    <span className="sidebar__badge sidebar__badge--muted" title={label} aria-label={label}>
      {count}
    </span>
  );
};

export default ReceiptsNavBadge;
