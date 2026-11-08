// Lista transakcji na telefonie: dni z przyklejonym nagłówkiem, wiersz w dwóch–trzech liniach.
// Dotknięcie otwiera szczegóły, przesunięcie w lewo odsłania szybkie akcje.
import { useEffect, useRef, useState, type TouchEvent } from 'react';
import { ArrowLeftRight, Check, Tag, Undo2 } from 'lucide-react';
import type { Transaction } from '../../../api';
import Money from '../../../components/ui/Money';
import { formatDayHeading } from '../../../utils/date';
import type { MobileDay, MobileRow } from '../mobileList';

const ACTION_WIDTH = 80;
// Ruch krótszy niż próg to jeszcze dotknięcie, nie przesunięcie
const DRAG_SLOP_PX = 8;
const LOAD_MORE_MARGIN = '600px';

type RowActions = {
  onOpen: (transaction: Transaction) => void;
  onToggleApproved: (transaction: Transaction) => void;
  onPickCategory: (transaction: Transaction) => void;
};

type SwipeRowProps = RowActions & {
  row: MobileRow;
  open: boolean;
  busy: boolean;
  onOpenChange: (open: boolean) => void;
};

const SwipeRow = ({ row, open, busy, onOpenChange, onOpen, onToggleApproved, onPickCategory }: SwipeRowProps) => {
  const { transaction } = row;
  const canPickCategory = row.kind === 'regular';
  const canApprove = row.kind !== 'adjustment';
  const actionsWidth = ACTION_WIDTH * (Number(canPickCategory) + Number(canApprove));
  const [drag, setDrag] = useState<number | null>(null);
  // Ostatnie przesunięcie także w refie: „touchend" może przyjść przed renderem z ostatnim „touchmove"
  const dragRef = useRef<number | null>(null);
  const gesture = useRef<{ x: number; y: number; axis: 'x' | 'y' | null } | null>(null);
  // Po przesunięciu przeglądarka i tak wyśle „click" — nie może on otworzyć szczegółów
  const swallowClick = useRef(false);

  const handleTouchStart = (event: TouchEvent) => {
    // Nie każda przeglądarka wysyła „click" po przesunięciu — flaga z poprzedniego gestu nie może zjeść tego
    swallowClick.current = false;
    if (!actionsWidth) return;
    const touch = event.touches[0];
    gesture.current = { x: touch.clientX, y: touch.clientY, axis: null };
  };

  const handleTouchMove = (event: TouchEvent) => {
    const start = gesture.current;
    if (!start) return;
    const touch = event.touches[0];
    const dx = touch.clientX - start.x;
    const dy = touch.clientY - start.y;
    if (start.axis === null) {
      if (Math.abs(dx) < DRAG_SLOP_PX && Math.abs(dy) < DRAG_SLOP_PX) return;
      start.axis = Math.abs(dx) > Math.abs(dy) ? 'x' : 'y';
    }
    if (start.axis !== 'x') return;
    const base = open ? -actionsWidth : 0;
    dragRef.current = Math.min(0, Math.max(-actionsWidth, base + dx));
    setDrag(dragRef.current);
  };

  const handleTouchEnd = () => {
    const wasDragging = gesture.current?.axis === 'x';
    const last = dragRef.current;
    gesture.current = null;
    dragRef.current = null;
    if (!wasDragging || last === null) return;
    swallowClick.current = true;
    onOpenChange(last < -actionsWidth / 2);
    setDrag(null);
  };

  const shift = drag ?? (open ? -actionsWidth : 0);
  const showActions = actionsWidth > 0 && shift < 0;
  const isTransfer = row.kind === 'transfer';
  const isMuted = row.kind !== 'regular';

  return (
    <li className="tx-mobile-row">
      {showActions && (
        <div className="tx-mobile-row__actions" style={{ width: actionsWidth }}>
          {canPickCategory && (
            <button
              type="button"
              className="tx-mobile-row__action"
              onClick={() => {
                onOpenChange(false);
                onPickCategory(transaction);
              }}
            >
              <Tag size={18} aria-hidden="true" />
              Kategoria
            </button>
          )}
          {canApprove && (
            <button
              type="button"
              className="tx-mobile-row__action tx-mobile-row__action--primary"
              disabled={busy}
              onClick={() => {
                onOpenChange(false);
                onToggleApproved(transaction);
              }}
            >
              {transaction.cleared ? <Undo2 size={18} aria-hidden="true" /> : <Check size={18} aria-hidden="true" />}
              {transaction.cleared ? 'Cofnij' : 'Zatwierdź'}
            </button>
          )}
        </div>
      )}
      <button
        type="button"
        className={`tx-mobile-row__main${drag !== null ? ' is-dragging' : ''}`}
        style={{ transform: shift ? `translateX(${shift}px)` : undefined }}
        onTouchStart={handleTouchStart}
        onTouchMove={handleTouchMove}
        onTouchEnd={handleTouchEnd}
        onTouchCancel={handleTouchEnd}
        onClick={() => {
          if (swallowClick.current) {
            swallowClick.current = false;
            return;
          }
          if (open) onOpenChange(false);
          else onOpen(transaction);
        }}
      >
        <span className="tx-mobile-row__body">
          <span className="tx-mobile-row__title">
            {isTransfer && <ArrowLeftRight size={14} className="tx-mobile-row__transfer-icon" aria-hidden="true" />}
            <span className={`tx-mobile-row__name${row.nameMuted ? ' is-muted' : ''}`}>{row.name}</span>
            {row.unapproved && <span className="tx-mobile-row__pending" role="img" aria-label="Niezatwierdzona" />}
          </span>
          <span className="tx-mobile-row__meta">
            {row.dots.map((color, index) => (
              <span key={index} className="tx-mobile-row__dot" style={{ background: color }} aria-hidden="true" />
            ))}
            {row.categoryLabel && (
              <span className={`tx-mobile-row__category${row.categoryMissing ? ' is-missing' : ''}`}>
                {row.categoryLabel}
              </span>
            )}
            {row.account && (
              <span className="tx-mobile-row__account">
                {row.categoryLabel ? '· ' : ''}
                {row.account}
              </span>
            )}
          </span>
          {row.comment && <span className="tx-mobile-row__comment">{row.comment}</span>}
        </span>
        <span className="tx-mobile-row__side">
          <Money
            cents={transaction.total_cents}
            currency={transaction.currency}
            tone={isMuted ? 'neutral' : 'auto'}
            className={isMuted ? 'tx-mobile-row__amount is-muted' : 'tx-mobile-row__amount'}
          />
          {row.time && <span className="tx-mobile-row__time">{row.time}</span>}
        </span>
      </button>
    </li>
  );
};

type TransactionsMobileListProps = RowActions & {
  days: MobileDay[];
  hasMore: boolean;
  loadingMore: boolean;
  onLoadMore: () => void;
  statusUpdatingId: string | null;
};

const TransactionsMobileList = ({
  days,
  hasMore,
  loadingMore,
  onLoadMore,
  statusUpdatingId,
  ...actions
}: TransactionsMobileListProps) => {
  // Odsłonięty jest najwyżej jeden wiersz naraz
  const [openId, setOpenId] = useState<string | null>(null);
  const sentinelRef = useRef<HTMLDivElement>(null);
  const loadMoreRef = useRef(onLoadMore);
  useEffect(() => {
    loadMoreRef.current = onLoadMore;
  }, [onLoadMore]);

  useEffect(() => {
    const sentinel = sentinelRef.current;
    if (!sentinel || !hasMore || loadingMore) return;
    const observer = new IntersectionObserver(
      (entries) => {
        if (entries.some((entry) => entry.isIntersecting)) loadMoreRef.current();
      },
      { rootMargin: LOAD_MORE_MARGIN }
    );
    observer.observe(sentinel);
    return () => observer.disconnect();
  }, [hasMore, loadingMore]);

  return (
    <div className="tx-mobile-list">
      {days.map((day) => (
        <section key={day.date} className="tx-mobile-day" aria-label={day.date ? formatDayHeading(day.date) : 'Bez daty'}>
          <div className="tx-mobile-day__head">
            <span>{day.date ? formatDayHeading(day.date) : '—'}</span>
            {day.sumCents !== null && <Money cents={day.sumCents} currency={day.currency} tone="neutral" />}
          </div>
          <ul className="tx-mobile-day__rows">
            {day.rows.map((row) => (
              <SwipeRow
                key={row.transaction.id}
                row={row}
                open={openId === row.transaction.id}
                busy={statusUpdatingId === row.transaction.id}
                onOpenChange={(open) => setOpenId(open ? row.transaction.id : null)}
                {...actions}
              />
            ))}
          </ul>
        </section>
      ))}
      <div ref={sentinelRef} className="tx-mobile-list__more" aria-live="polite">
        {hasMore ? (loadingMore ? 'Ładowanie starszych…' : '') : ''}
      </div>
    </div>
  );
};

export default TransactionsMobileList;
