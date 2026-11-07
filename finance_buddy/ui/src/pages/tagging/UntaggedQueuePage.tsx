import { useEffect, useMemo, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import { ArrowLeftIcon, CheckIcon, SkipForwardIcon, TagsIcon } from 'lucide-react';
import Button from '../../components/Button';
import PageHeader from '../../components/ui/PageHeader';
import TruncatedText from '../../components/ui/TruncatedText';
import { formatShortDate } from '../../utils/date';
import TagPathInput from '../../components/tags/TagPathInput';
import { TagChip } from '../../components/tags/TagChip';
import {
  ensureCategoryPath,
  getAllCategories,
  getUntaggedCount,
  getUntaggedTransactions,
  tagStoreUntagged,
  updateTransaction,
  type Category
} from '../../api';
import { setUntaggedCount } from '../../hooks/useUntaggedCount';
import { formatMoney } from '../../utils/format';
import {
  PAGE_SIZE,
  appendPage,
  describeSuggestion,
  nextOffset,
  nextPendingIndex,
  queueKeyAction,
  removeTagged,
  shouldLoadMore,
  suggestedPath,
  type QueueAction,
  type QueueItem
} from './model';
import ResizableTable from '../../components/ui/ResizableTable';
import { formatCategoryPath } from '../../utils/categoryPath';

const TAG_INPUT_ID = 'untagged-queue-tag';

type BulkConfirm = { storeId: string; storeName: string; path: string; count: number };

const isTypingTarget = (target: EventTarget | null) =>
  target instanceof HTMLElement &&
  (target.isContentEditable || ['INPUT', 'TEXTAREA', 'SELECT'].includes(target.tagName));

/** Enter i spacja na przycisku/linku mają zadziałać natywnie, a nie jako skrót. */
const isActivatableTarget = (target: EventTarget | null) =>
  target instanceof HTMLElement && Boolean(target.closest('button, a'));

const releaseFocus = () => {
  const active = document.activeElement;
  if (active instanceof HTMLElement) active.blur();
};

const Kbd = ({ children }: { children: string }) => <kbd className="untagged-queue__kbd">{children}</kbd>;

/**
 * Kolejka „do otagowania": transakcje bez tagu, jedna po drugiej, z
 * podpowiedzią gotową do przyjęcia Enterem.
 *
 * Osobna strona, a nie filtr Transakcji: siatka jest do przeglądania i edycji
 * wielu pól, a tu chodzi o jedną decyzję na transakcję w tempie klawiatury.
 * Lista pod kartą bieżącej transakcji pokazuje, co jeszcze czeka.
 */
const UntaggedQueuePage = () => {
  const [items, setItems] = useState<QueueItem[]>([]);
  const [total, setTotal] = useState(0);
  const [cursor, setCursor] = useState(0);
  const [categories, setCategories] = useState<Category[]>([]);
  const [loading, setLoading] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  // Własny wybór tagu dla bieżącej pozycji; bez niego pole pokazuje podpowiedź.
  const [override, setOverride] = useState<{ id: string; path: string } | null>(null);
  const [confirm, setConfirm] = useState<BulkConfirm | null>(null);

  const pathById = useMemo(
    () => new Map(categories.map((category) => [category.id, category.path])),
    [categories]
  );
  const categoryById = useMemo(
    () => new Map(categories.map((category) => [category.id, category])),
    [categories]
  );

  const current: QueueItem | undefined = items[cursor];
  const draftPath =
    current && override?.id === current.id ? override.path : suggestedPath(current, pathById);
  const skippedCount = items.filter((item) => item.skipped).length;

  useEffect(() => {
    let cancelled = false;
    Promise.all([getUntaggedTransactions({ offset: 0, limit: PAGE_SIZE }), getAllCategories()])
      .then(([page, loadedCategories]) => {
        if (cancelled) return;
        setItems(page.items);
        setTotal(page.total);
        setUntaggedCount(page.total);
        setCategories(loadedCategories);
      })
      .catch((err) => {
        console.error(err);
        if (!cancelled) setError('Nie udało się pobrać kolejki. Sprawdź, czy backend działa.');
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  // Doładowanie następnej strony, gdy kursor zbliża się do końca pobranej części.
  // Z akcji, a nie z efektu: to ruch kursora jest powodem, a ref chroni przed
  // dwoma równoległymi żądaniami tej samej strony.
  const loadingMoreRef = useRef(false);
  const maybeLoadMore = (list: QueueItem[], at: number, knownTotal: number) => {
    if (loadingMoreRef.current || !shouldLoadMore(list, at, knownTotal)) return;
    loadingMoreRef.current = true;
    setLoadingMore(true);
    getUntaggedTransactions({ offset: nextOffset(list), limit: PAGE_SIZE })
      .then((page) => {
        setItems((prev) => appendPage(prev, page.items));
        setTotal(page.total);
      })
      .catch((err) => console.error(err))
      .finally(() => {
        loadingMoreRef.current = false;
        setLoadingMore(false);
      });
  };

  const moveTo = (list: QueueItem[], index: number, knownTotal = total) => {
    setCursor(index);
    setOverride(null);
    setConfirm(null);
    maybeLoadMore(list, index, knownTotal);
  };

  const focusTagInput = () => {
    document.getElementById(TAG_INPUT_ID)?.focus();
  };

  /** Ścieżka → id tagu; nowy tag (wpisana nowa ścieżka) dociąga świeże drzewo. */
  const resolveCategoryId = async (path: string) => {
    const id = await ensureCategoryPath(path, categories);
    if (!pathById.has(id)) setCategories(await getAllCategories());
    return id;
  };

  const tagCurrent = async (path: string) => {
    if (!current || busy) return;
    const trimmed = path.trim();
    if (!trimmed) {
      focusTagInput();
      return;
    }
    const target = current;
    setBusy(true);
    setError(null);
    try {
      const categoryId = await resolveCategoryId(trimmed);
      await updateTransaction(target.id, { category_id: categoryId });
      const next = removeTagged(items, cursor, (item) => item.id === target.id);
      setItems(next.items);
      setTotal(total - 1);
      setUntaggedCount(total - 1);
      moveTo(next.items, next.cursor, total - 1);
      setNotice(`${target.store_name || target.comment || 'Transakcja'} → ${trimmed}`);
      releaseFocus();
    } catch (err) {
      console.error(err);
      setError('Nie udało się zapisać kategorii.');
    } finally {
      setBusy(false);
    }
  };

  const skipCurrent = () => {
    if (!current) return;
    const next = items.map((item, index) => (index === cursor ? { ...item, skipped: true } : item));
    setItems(next);
    moveTo(next, nextPendingIndex(next, cursor + 1));
    releaseFocus();
  };

  const askTagStore = async () => {
    if (!current?.store_id || busy) return;
    const path = draftPath.trim();
    if (!path) {
      setError('Najpierw wybierz kategorię (T), potem nadaj ją całemu sklepowi.');
      focusTagInput();
      return;
    }
    setBusy(true);
    setError(null);
    try {
      // Świeża liczba zamiast tej z listy — strona mogła się zestarzeć.
      const { count } = await getUntaggedCount(current.store_id);
      setConfirm({
        storeId: current.store_id,
        storeName: current.store_name || 'ten sklep',
        path,
        count
      });
      releaseFocus();
    } catch (err) {
      console.error(err);
      setError('Nie udało się policzyć transakcji sklepu.');
    } finally {
      setBusy(false);
    }
  };

  const confirmTagStore = async () => {
    if (!confirm || busy) return;
    setBusy(true);
    setError(null);
    try {
      const categoryId = await resolveCategoryId(confirm.path);
      const { updated } = await tagStoreUntagged(confirm.storeId, categoryId);
      const next = removeTagged(items, cursor, (item) => item.store_id === confirm.storeId);
      setItems(next.items);
      setTotal(total - updated);
      setUntaggedCount(total - updated);
      moveTo(next.items, next.cursor, total - updated);
      setNotice(`${confirm.storeName}: ${updated} × → ${formatCategoryPath(confirm.path)}`);
    } catch (err) {
      console.error(err);
      setError('Nie udało się nadać kategorii transakcjom sklepu.');
    } finally {
      setBusy(false);
    }
  };

  const runAction = (action: QueueAction) => {
    switch (action) {
      case 'accept':
        void tagCurrent(draftPath);
        return;
      case 'edit':
        focusTagInput();
        return;
      case 'skip':
        skipCurrent();
        return;
      case 'next':
        moveTo(items, Math.min(cursor + 1, items.length));
        return;
      case 'previous':
        moveTo(items, Math.max(cursor - 1, 0));
        return;
      case 'tagStore':
        void askTagStore();
        return;
    }
  };

  // Nasłuch na oknie, a nie na kontenerze: po zapisie fokus wraca na <body>
  // i skróty muszą działać dalej bez klikania w stronę. Handler czytamy z refa,
  // żeby listener nie widział stanu sprzed renderu.
  const keyHandlerRef = useRef<(event: KeyboardEvent) => void>(() => {});
  useEffect(() => {
    keyHandlerRef.current = (event: KeyboardEvent) => {
      if (isTypingTarget(event.target) || busy) return;
      if (confirm) {
        if (event.key === 'Escape') {
          event.preventDefault();
          setConfirm(null);
        } else if (event.key === 'Enter' && !isActivatableTarget(event.target)) {
          event.preventDefault();
          void confirmTagStore();
        }
        return;
      }
      const action = queueKeyAction(event);
      if (!action) return;
      if (action === 'accept' && isActivatableTarget(event.target)) return;
      event.preventDefault();
      runAction(action);
    };
  });
  useEffect(() => {
    const listener = (event: KeyboardEvent) => keyHandlerRef.current(event);
    window.addEventListener('keydown', listener);
    return () => window.removeEventListener('keydown', listener);
  }, []);

  const suggestion = current?.suggestion;
  const suggestedCategory = suggestion ? categoryById.get(suggestion.category_id) : undefined;
  const position = Math.min(cursor + 1, items.length);

  return (
    <div className="page-shell page-stack-layout untagged-queue">
      <PageHeader
        title="Bez kategorii"
        count={loading ? undefined : total}
        actions={
          <Link to="/transactions" className="text-button">
            <ArrowLeftIcon size={14} aria-hidden="true" />
            Transakcje
          </Link>
        }
      />

      {error && (
        <p className="transactions-error" role="alert">
          {error}
        </p>
      )}

      <div className="untagged-queue__layout">
        <section className="untagged-queue__focus" aria-live="polite">
          {loading ? (
            <p className="untagged-queue__muted">Ładuję…</p>
          ) : !items.length ? (
            <div className="untagged-queue__empty">
              <CheckIcon size={24} strokeWidth={1.5} aria-hidden="true" />
              <strong>Wszystko ma kategorię</strong>
              <span className="untagged-queue__muted">Nowe transakcje bez kategorii pojawią się tutaj.</span>
            </div>
          ) : !current ? (
            <div className="untagged-queue__empty">
              <SkipForwardIcon size={24} strokeWidth={1.5} aria-hidden="true" />
              <strong>Koniec kolejki</strong>
              <span className="untagged-queue__muted">
                Pominięte: {skippedCount}. Wybierz je z listy, aby wrócić.
              </span>
            </div>
          ) : (
            <>
              <div className="untagged-queue__progress" role="img" aria-label={`Pozycja ${position} z ${total}`}>
                <span style={{ width: `${(position / Math.max(total, 1)) * 100}%` }} />
              </div>
              <div className="untagged-queue__meta">
                <span>
                  {position} z {total}
                </span>
                <span>{current.date ? formatShortDate(current.date) : '—'}</span>
                <span>{current.account_name}</span>
                {current.skipped ? <span className="receipt-badge">pominięta</span> : null}
              </div>

              <div className="untagged-queue__headline">
                <div className="untagged-queue__payee">
                  <strong>{current.store_name || current.comment || 'Bez opisu'}</strong>
                  {current.store_name && current.comment && current.comment !== current.store_name ? (
                    <span className="untagged-queue__muted">{current.comment}</span>
                  ) : null}
                </div>
                <span className={`untagged-queue__amount${current.total_cents < 0 ? '' : ' is-income'}`}>
                  {formatMoney(current.total_cents, current.currency)}
                </span>
              </div>

              <div className="untagged-queue__field">
                <label className="form-label" htmlFor={TAG_INPUT_ID}>
                  Kategoria
                </label>
                <TagPathInput
                  inputId={TAG_INPUT_ID}
                  value={draftPath}
                  categories={categories}
                  forAccounts={false}
                  commitOnBlur={false}
                  placeholder="Wpisz kategorię, np. jedzenie/zakupy"
                  disabled={busy}
                  onChange={(path) => setOverride({ id: current.id, path })}
                  onCommit={(path) => void tagCurrent(path)}
                  onCancel={() => {
                    setOverride(null);
                    releaseFocus();
                  }}
                />
                <div className="untagged-queue__suggestion">
                  {suggestion && suggestedCategory ? (
                    <>
                      <TagChip category={suggestedCategory} byId={categoryById} label={formatCategoryPath(suggestedCategory.path)} />
                      <span className="untagged-queue__muted">{describeSuggestion(suggestion)}</span>
                    </>
                  ) : (
                    <span className="untagged-queue__muted">Brak podpowiedzi: nic podobnego nie ma jeszcze kategorii.</span>
                  )}
                </div>
              </div>

              {confirm ? (
                <div className="untagged-queue__confirm" role="alertdialog" aria-label="Nadać kategorię całemu sklepowi?">
                  <p>
                    Nadać kategorię <strong>{formatCategoryPath(confirm.path)}</strong> <strong>{confirm.count}</strong>{' '}
                    {confirm.count === 1 ? 'transakcji' : 'transakcjom'} z „{confirm.storeName}”?
                  </p>
                  <div className="untagged-queue__actions">
                    <Button onClick={() => void confirmTagStore()} disabled={busy}>
                      Tak, nadaj <Kbd>⏎</Kbd>
                    </Button>
                    <Button variant="outline-secondary" onClick={() => setConfirm(null)}>
                      Anuluj <Kbd>Esc</Kbd>
                    </Button>
                  </div>
                </div>
              ) : (
                <div className="untagged-queue__actions">
                  <Button
                    className="untagged-queue__accept"
                    icon={<CheckIcon size={16} aria-hidden="true" />}
                    onClick={() => void tagCurrent(draftPath)}
                    disabled={busy || !draftPath.trim()}
                  >
                    Przyjmij <Kbd>⏎</Kbd>
                  </Button>
                  <Button variant="outline-secondary" onClick={skipCurrent} disabled={busy}>
                    Pomiń <Kbd>S</Kbd>
                  </Button>
                  {current.store_id ? (
                    <Button
                      variant="outline-secondary"
                      icon={<TagsIcon size={16} aria-hidden="true" />}
                      onClick={() => void askTagStore()}
                      disabled={busy || !draftPath.trim()}
                    >
                      Cały sklep ({current.store_untagged_count}) <Kbd>W</Kbd>
                    </Button>
                  ) : null}
                </div>
              )}
            </>
          )}

          {notice ? <div className="untagged-queue__notice">Ostatnio: {notice}</div> : null}
          <div className="untagged-queue__help">
            <Kbd>T</Kbd> inna kategoria · <Kbd>J</Kbd>/<Kbd>K</Kbd> następna/poprzednia · <Kbd>Esc</Kbd> wyjście z pola
          </div>
        </section>

        {items.length > 0 && (
          <section className="untagged-queue__rest" aria-label="Kolejka">
            <ResizableTable tableId="untagged-queue" className="untagged-queue__list">
              <thead>
                <tr>
                  <th>Sklep</th>
                  <th className="untagged-queue__list-date">Data</th>
                  <th className="untagged-queue__list-amount">Kwota</th>
                </tr>
              </thead>
              <tbody>
                {items.map((item, index) => {
                  const path = suggestedPath(item, pathById);
                  return (
                    <tr
                      key={item.id}
                      className={(index === cursor ? 'is-current' : '') + (item.skipped ? ' is-skipped' : '')}
                      aria-current={index === cursor ? 'true' : undefined}
                      onClick={() => moveTo(items, index)}
                    >
                      <td>
                        <TruncatedText text={item.store_name || item.comment || '—'} />
                        {path ? (
                          <span className="untagged-queue__list-hint">
                            <TruncatedText text={formatCategoryPath(path)} />
                          </span>
                        ) : null}
                      </td>
                      <td className="untagged-queue__list-date">{item.date ? formatShortDate(item.date) : '—'}</td>
                      <td className="untagged-queue__list-amount">{formatMoney(item.total_cents, item.currency)}</td>
                    </tr>
                  );
                })}
                {loadingMore ? (
                  <tr>
                    <td colSpan={3} className="untagged-queue__muted">
                      Ładuję kolejne…
                    </td>
                  </tr>
                ) : null}
              </tbody>
            </ResizableTable>
          </section>
        )}
      </div>
    </div>
  );
};

export default UntaggedQueuePage;
