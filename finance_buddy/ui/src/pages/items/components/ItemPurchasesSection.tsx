// Zakupy przedmiotu: liczby z listy i wydatki miesięcznie, tak jak w karcie sklepu.
// Wykres pobieramy tylko dla przedmiotu, który był kiedyś kupiony.
import { useEffect, useState } from 'react';
import { type ItemMonthlySpend, getItemMonthlySpend } from '../../../api';
import MonthBars from '../../../components/ui/MonthBars';
import { formatSignedMoney } from '../../../components/ui/Money';
import { useVisibleMonths } from '../../../hooks/useVisibleMonths';
import { formatShortDate } from '../../../utils/date';
import { pluralPL } from '../../../utils/format';
import { monthRangeLabel } from '../../../utils/months';
import type { ItemStat } from '../model';

type ItemPurchasesSectionProps = {
  itemId: string;
  /** null, gdy przedmiotu nikt nie kupił albo statystyki się nie wczytały. */
  stat: ItemStat | null;
};

const MonthsChart = ({ months }: { months: ItemMonthlySpend[] }) => {
  const visible = useVisibleMonths(months);
  const lastMonth = visible[visible.length - 1].month;
  return (
    <>
      <p className="text-secondary small mb-2">Wydatki miesięcznie, {monthRangeLabel(visible[0].month, lastMonth)}</p>
      <MonthBars months={visible} highlighted={lastMonth} ariaLabel="Wydatki na przedmiot miesięcznie" />
    </>
  );
};

const ItemPurchasesSection = ({ itemId, stat }: ItemPurchasesSectionProps) => {
  const [months, setMonths] = useState<ItemMonthlySpend[] | null>(null);
  const [error, setError] = useState(false);
  const bought = Boolean(stat?.count);

  useEffect(() => {
    if (!bought) return;
    let cancelled = false;
    getItemMonthlySpend(itemId)
      .then((data) => {
        if (!cancelled) setMonths(data);
      })
      .catch((err: unknown) => {
        console.error(err);
        if (!cancelled) setError(true);
      });
    return () => {
      cancelled = true;
    };
  }, [itemId, bought]);

  return (
    <section className="item-purchases" aria-labelledby="item-purchases-title">
      <h4 className="item-section__title" id="item-purchases-title">
        Zakupy
      </h4>
      {!stat || !bought ? (
        <p className="text-secondary small mb-0">Tego przedmiotu nie ma jeszcze na żadnej transakcji.</p>
      ) : (
        <>
          <p className="form-hint mb-2 item-purchases__summary">
            {stat.count} {pluralPL(stat.count, 'zakup', 'zakupy', 'zakupów')}
            {stat.lastDate ? `, ostatni ${formatShortDate(stat.lastDate)}` : ''}.{' '}
            {formatSignedMoney(stat.spentCents)} w ostatnich 12 miesiącach.
          </p>
          {error && <p className="text-secondary small mb-0">Nie udało się wczytać wykresu.</p>}
          {!months && !error && <p className="text-secondary small mb-0">Wczytywanie…</p>}
          {months && <MonthsChart months={months} />}
        </>
      )}
    </section>
  );
};

export default ItemPurchasesSection;
