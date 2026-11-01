import { useEffect, useMemo, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import { ArrowLeftIcon, CheckIcon, SkipForwardIcon, TagsIcon } from 'lucide-react';
import Button from '../../components/Button';
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
      setError('Nie udało się zapisać tagu.');
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
      setError('Najpierw wybierz tag (T), potem otaguj cały sklep.');
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
      setNotice(`${confirm.storeName}: ${updated} × → ${confirm.path}`);
    } catch (err) {
      console.error(err);
      setError('Nie udało się otagować transakcji sklepu.');
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
    <div className="page-wrapper compact-page">
      <div className="page-shell">
        <div className="page-header d-print-none">
          <div className="row align-items-center">
            <div className="col">
              <h2 className="page-title">Do otagowania</h2>
              <div className="text-secondary mt-1">
                {loading
                  ? 'Ładuję kolejkę…'
                  : `${total} transakcji bez tagu. Przelewy między kontami, korekty salda i konta śledzone są pominięte.`}
              </div>
            </div>
            <div className="col-auto">
              <Link to="/transactions" className="btn btn-outline-secondary btn-sm">
                <ArrowLeftIcon size={16} className="me-1" aria-hidden="true" />
                Transakcje
              </Link>
            </div>
          </div>
        </div>

        <div className="page-body">
          {error ? (
            <div className="alert alert-danger" role="alert">
              {error}
            </div>
          ) : null}

          <section className="card untagged-queue__current" aria-live="polite">
            <div className="card-body">
              {loading ? (
                <p className="text-secondary mb-0">Ładuję…</p>
              ) : !items.length ? (
                <div className="untagged-queue__empty">
                  <CheckIcon size={28} aria-hidden="true" />
                  <div>
                    <strong>Wszystko otagowane.</strong>
                    <div className="text-secondary">Nowe transakcje bez tagu pojawią się tutaj.</div>
                  </div>
                </div>
              ) : !current ? (
                <div className="untagged-queue__empty">
                  <SkipForwardIcon size={28} aria-hidden="true" />
                  <div>
                    <strong>Koniec kolejki.</strong>
                    <div className="text-secondary">
                      Pominięte: {skippedCount}. <Kbd>↑</Kbd> wraca do poprzednich.
                    </div>
                  </div>
                </div>
              ) : (
                <>
                  <div className="untagged-queue__meta">
                    <span>
                      {position} / {total}
                    </span>
                    <span>{current.date}</span>
                    <span>{current.account_name}</span>
                    {current.skipped ? <span className="receipt-badge">pominięta</span> : null}
                  </div>

                  <div className="untagged-queue__headline">
                    <div className="untagged-queue__payee">
                      <strong>{current.store_name || current.comment || 'Bez opisu'}</strong>
                      {current.store_name && current.comment && current.comment !== current.store_name ? (
                        <span className="text-secondary">{current.comment}</span>
                      ) : null}
                    </div>
                    <span
                      className={`untagged-queue__amount${current.total_cents < 0 ? '' : ' is-income'}`}
                    >
                      {formatMoney(current.total_cents, current.currency)}
                    </span>
                  </div>

                  <div className="untagged-queue__tag-row">
                    <label className="untagged-queue__label" htmlFor={TAG_INPUT_ID}>
                      Tag
                    </label>
                    <div className="untagged-queue__tag-input">
                      <TagPathInput
                        inputId={TAG_INPUT_ID}
                        value={draftPath}
                        categories={categories}
                        forAccounts={false}
                        commitOnBlur={false}
                        placeholder="Wpisz tag, np. jedzenie/zakupy"
                        disabled={busy}
                        onChange={(path) => setOverride({ id: current.id, path })}
                        onCommit={(path) => void tagCurrent(path)}
                        onCancel={() => {
                          setOverride(null);
                          releaseFocus();
                        }}
                      />
                    </div>
                    <div className="untagged-queue__suggestion">
                      {suggestion && suggestedCategory ? (
                        <>
                          <span className="text-secondary">Podpowiedź:</span>
                          <TagChip
                            category={suggestedCategory}
                            byId={categoryById}
                            label={suggestedCategory.path}
                          />
                          <span className="text-secondary">{describeSuggestion(suggestion)}</span>
                        </>
                      ) : (
                        <span className="text-secondary">
                          Brak podpowiedzi — nic podobnego nie było jeszcze otagowane.
                        </span>
                      )}
                    </div>
                  </div>

                  {confirm ? (
                    <div className="alert alert-warning untagged-queue__confirm mb-0" role="alertdialog">
                      <div>
                        Otagować <strong>{confirm.count}</strong>{' '}
                        {confirm.count === 1 ? 'transakcję' : 'transakcji'} z „{confirm.storeName}"
                        tagiem <strong>{confirm.path}</strong>?
                      </div>
                      <div className="d-flex gap-2">
                        <Button size="sm" onClick={() => void confirmTagStore()} disabled={busy}>
                          Tak, otaguj <Kbd>⏎</Kbd>
                        </Button>
                        <Button size="sm" variant="outline-secondary" onClick={() => setConfirm(null)}>
                          Anuluj <Kbd>Esc</Kbd>
                        </Button>
                      </div>
                    </div>
                  ) : (
                    <div className="untagged-queue__actions">
                      <Button
                        size="sm"
                        icon={<CheckIcon size={16} />}
                        onClick={() => void tagCurrent(draftPath)}
                        disabled={busy || !draftPath.trim()}
                      >
                        Przyjmij <Kbd>⏎</Kbd>
                      </Button>
                      <Button size="sm" variant="outline-secondary" onClick={focusTagInput} disabled={busy}>
                        Inny tag <Kbd>T</Kbd>
                      </Button>
                      <Button size="sm" variant="outline-secondary" onClick={skipCurrent} disabled={busy}>
                        Pomiń <Kbd>S</Kbd>
                      </Button>
                      {current.store_id ? (
                        <Button
                          size="sm"
                          variant="outline-secondary"
                          icon={<TagsIcon size={16} />}
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
                <Kbd>⏎</Kbd> przyjmij · <Kbd>T</Kbd> inny tag · <Kbd>S</Kbd> pomiń · <Kbd>J</Kbd>/
                <Kbd>K</Kbd> następna/poprzednia · <Kbd>W</Kbd> cały sklep · w polu tagu <Kbd>Esc</Kbd>{' '}
                wraca
              </div>
            </div>
          </section>

          {items.length ? (
            <div className="card mt-3">
              <div className="table-responsive">
                <table className="table table-vcenter card-table untagged-queue__list">
                  <thead>
                    <tr>
                      <th>Data</th>
                      <th>Sklep / opis</th>
                      <th>Konto</th>
                      <th className="text-end">Kwota</th>
                      <th>Podpowiedź</th>
                    </tr>
                  </thead>
                  <tbody>
                    {items.map((item, index) => {
                      const path = suggestedPath(item, pathById);
                      return (
                        <tr
                          key={item.id}
                          className={
                            (index === cursor ? 'is-current' : '') + (item.skipped ? ' is-skipped' : '')
                          }
                          aria-current={index === cursor ? 'true' : undefined}
                          onClick={() => moveTo(items, index)}
                        >
                          <td className="text-nowrap">{item.date}</td>
                          <td>{item.store_name || item.comment || '—'}</td>
                          <td className="text-secondary">{item.account_name}</td>
                          <td className="text-end text-nowrap">
                            {formatMoney(item.total_cents, item.currency)}
                          </td>
                          <td className="text-secondary">{path || '—'}</td>
                        </tr>
                      );
                    })}
                    {loadingMore ? (
                      <tr>
                        <td colSpan={5} className="text-secondary text-center">
                          Ładuję kolejne…
                        </td>
                      </tr>
                    ) : null}
                  </tbody>
                </table>
              </div>
            </div>
          ) : null}
        </div>
      </div>
    </div>
  );
};

export default UntaggedQueuePage;
