import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useNavigate, useParams, useSearchParams } from 'react-router-dom';
import { ArrowLeftIcon, CheckIcon, MoveLeftIcon, MoveRightIcon, SaveIcon, SkipForwardIcon } from 'lucide-react';
import Button from '../../components/Button';
import { formatMoney } from '../../utils/format';
import TagPathInput from '../../components/tags/TagPathInput';
import EntityAutocomplete, { type EntityOption } from '../../components/transactions/EntityAutocomplete';
import { selectableAccounts } from '../../components/transactions/lookupOptions';
import {
  commitIssuesFromError,
  commitReceiptDraft,
  ensureCategoryPath,
  getAccounts,
  getAllCategories,
  getAllItems,
  getAllStores,
  getReceiptBankMatches,
  getReceiptDraft,
  getReceiptDrafts,
  matchProducts,
  saveReceiptDraft,
  updateStore,
  type Account,
  type BankMatchInfo,
  type Category,
  type Item,
  type ReceiptDraftDetail,
  type ReceiptDraftSummary,
  type ReceiptQueueFilter,
  type Store
} from '../../api';
import {
  DEFAULT_RULES,
  formatDateField,
  isDateFieldGarbled,
  labelIssues,
  parseDateField,
  type ReceiptCheckIssue,
  type ReceiptCheckRules
} from '../../utils/receiptChecks';
import { normalizeNip } from '../../utils/nip';
import IssueNote from './components/IssueNote';
import ReceiptImagePane from './components/ReceiptImagePane';
import ReceiptItemsTable from './components/ReceiptItemsTable';
import { suggestTransactionTag } from './tagSuggestion';
import {
  commitItemsFromRows,
  emptyRow,
  formFromPayload,
  payloadFromForm,
  rowsFromPayload,
  type ItemRow,
  type ReceiptForm
} from './model';

const FILTER_LABELS: Record<ReceiptQueueFilter, string> = {
  all: 'wszystkie',
  todo: 'do zrobienia',
  done: 'zatwierdzone',
  skipped: 'pominięte'
};

const queueMark = (draft: ReceiptDraftSummary) => {
  if (draft.issues.some((issue) => issue.level === 'bad')) return '⚠';
  if (draft.issues.length) return '?';
  if (draft.status === 'committed') return '✓';
  if (draft.status === 'skipped') return '⏭';
  return '•';
};

/**
 * Pełnoekranowy edytor jednego paragonu: obraz po lewej, etykieta po prawej.
 *
 * Kolejka (filtr, ⟨Poprzedni / Następny⟩, skok) jest w nagłówku, żeby przejście
 * do kolejnego paragonu nie wymagało wracania do listy — przy wsadzie na
 * kilkadziesiąt stron to jest różnica między pracą a klikaniem.
 */
const ReceiptDraftEditorPage = () => {
  const { draftId = '' } = useParams();
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();
  // Filtr z URL-a bywa cudzy: stara zakładka może nieść nieistniejący już
  // kubełek („review"), a wtedy `select` nie miałby czego zaznaczyć.
  const filterParam = searchParams.get('filter') ?? '';
  const filter: ReceiptQueueFilter =
    filterParam in FILTER_LABELS ? (filterParam as ReceiptQueueFilter) : 'todo';

  const [draft, setDraft] = useState<ReceiptDraftDetail | null>(null);
  const [queue, setQueue] = useState<ReceiptDraftSummary[]>([]);
  const [reviewCount, setReviewCount] = useState(0);
  const [rules, setRules] = useState<ReceiptCheckRules>(DEFAULT_RULES);

  const [form, setForm] = useState<ReceiptForm | null>(null);
  const [rows, setRows] = useState<ItemRow[]>([]);
  const [matching, setMatching] = useState(false);

  const [accounts, setAccounts] = useState<Account[]>([]);
  const [stores, setStores] = useState<Store[]>([]);
  const [categories, setCategories] = useState<Category[]>([]);
  const [products, setProducts] = useState<Item[]>([]);

  const [status, setStatus] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  // Transakcje z wyciągu, pod które można podpiąć ten paragon (import-csv §5.5).
  const [bankMatches, setBankMatches] = useState<BankMatchInfo[]>([]);
  const [mergeInto, setMergeInto] = useState<string | null>(null);
  // Blokady pokazane użytkownikowi do świadomej decyzji — wszystkie naraz, żeby
  // poprawienie jednej nie odsłaniało drugiej dopiero przy kolejnej próbie.
  const [pendingBlockers, setPendingBlockers] = useState<ReceiptCheckIssue[] | null>(null);
  // Zaznaczone przy zatwierdzeniu przepisze tag na sklep, żeby kolejny paragon
  // z tej sieci dostał go sam (pętla uczenia jak przy aliasach produktów).
  const [rememberStoreTag, setRememberStoreTag] = useState(false);

  const dateFieldRef = useRef<HTMLInputElement>(null);
  // Dopóki użytkownik sam nie wybierze sklepu z listy, poprawka NIP-u w
  // etykiecie ma prawo podmienić dopasowanie — inaczej ręczna korekta błędnego
  // odczytu OCR nigdy by nie doczepiła istniejącego sklepu.
  const storeManuallySetRef = useRef(false);
  // Tag raz ruszony ręcznie przestaje być podpowiedzią — także wtedy, gdy
  // użytkownik go wyczyścił, bo skasowanie to też decyzja.
  const tagManuallySetRef = useRef(false);

  // --- Dane słownikowe ---

  useEffect(() => {
    void (async () => {
      try {
        const [{ accounts: accountsData }, storesData, categoriesData, productsData] =
          await Promise.all([
            getAccounts(),
            getAllStores(),
            getAllCategories(300, { for_accounts: false }),
            getAllItems()
          ]);
        setAccounts(accountsData);
        setStores(storesData);
        setCategories(categoriesData);
        setProducts(
          productsData.slice().sort((a, b) => (a.name || '').localeCompare(b.name || '', 'pl'))
        );
      } catch (err) {
        console.error('Nie udało się pobrać słowników', err);
      }
    })();
  }, []);

  const refreshQueue = useCallback(async () => {
    const response = await getReceiptDrafts('all');
    setQueue(response.drafts);
    setReviewCount(response.counts.review);
    setRules(response.rules);
  }, []);

  useEffect(() => {
    void refreshQueue();
  }, [refreshQueue]);

  // --- Wczytanie paragonu ---

  useEffect(() => {
    if (!draftId) return;
    let cancelled = false;
    setStatus('Wczytywanie…');
    setError(null);
    setPendingBlockers(null);

    void (async () => {
      try {
        const detail = await getReceiptDraft(draftId);
        if (cancelled) return;
        setDraft(detail);
        storeManuallySetRef.current = false;
        tagManuallySetRef.current = false;
        setRememberStoreTag(false);
        setForm(formFromPayload(detail.payload, { storeId: detail.matched_store_id }));
        setRows(rowsFromPayload(detail.payload));
        setStatus(null);

        const rawNames = (detail.payload.items ?? []).map((item) => item.raw_name);
        if (!rawNames.length) return;
        setMatching(true);
        try {
          const { matches } = await matchProducts({
            store_id: detail.matched_store_id ?? undefined,
            raw_names: rawNames
          });
          // Odpowiedź mogła przyjść, gdy formularz stoi już na innym paragonie —
          // bez tej bramki dopasowania wsypałyby się pod cudzy klucz.
          if (!cancelled) setRows(rowsFromPayload(detail.payload, matches));
        } catch (err) {
          console.error('Nie udało się dopasować pozycji', err);
        } finally {
          if (!cancelled) setMatching(false);
        }
      } catch (err) {
        if (cancelled) return;
        console.error(err);
        setError('Nie udało się wczytać paragonu.');
        setStatus(null);
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [draftId]);

  // --- Wyliczenia ---

  const happenedAt = form ? parseDateField(form.dateText) : null;
  const dateGarbled = form ? isDateFieldGarbled(form.dateText) : false;

  const payload = useMemo(
    () => (form ? payloadFromForm(form, rows, happenedAt) : null),
    [form, rows, happenedAt]
  );

  // Ten sam zakup mógł już przyjść z wyciągu bankowego — wtedy paragon
  // podpinamy do tamtej transakcji, zamiast tworzyć drugą.
  const matchAccountId = form?.accountId ?? '';
  const matchDate = payload?.happened_at?.slice(0, 10) ?? '';
  const matchTotal = payload?.total_cents ?? null;
  const matchDraftId = draft?.status === 'committed' ? null : draft?.id ?? null;
  useEffect(() => {
    if (!matchDraftId || !matchAccountId || !matchDate || matchTotal == null) {
      return undefined;
    }
    let cancelled = false;
    const timer = window.setTimeout(async () => {
      try {
        const found = await getReceiptBankMatches(matchDraftId, {
          account_id: matchAccountId,
          date: matchDate,
          total_cents: matchTotal
        });
        if (cancelled) return;
        setBankMatches(found);
        // Jeden kandydat = domyślnie podpinamy; kilku = użytkownik wybiera.
        setMergeInto(found.length === 1 ? found[0].transaction_id : null);
      } catch (err) {
        if (!cancelled) console.error('Nie udało się sprawdzić transakcji z banku', err);
      }
    }, 300);
    return () => {
      cancelled = true;
      window.clearTimeout(timer);
    };
  }, [matchDraftId, matchAccountId, matchDate, matchTotal]);
  const hasMatchInputs = Boolean(matchDraftId && matchAccountId && matchDate && matchTotal != null);
  const visibleBankMatches = hasMatchInputs ? bankMatches : [];
  const effectiveMergeInto = visibleBankMatches.some((m) => m.transaction_id === mergeInto)
    ? mergeInto
    : null;

  const issues = useMemo(
    () => (payload ? labelIssues(payload, draft?.scan_date, rules) : []),
    [payload, draft?.scan_date, rules]
  );

  const issueFor = (field: string) => issues.find((issue) => issue.field === field) ?? null;
  const blockers = issues.filter((issue) => issue.level === 'bad');

  const visibleQueue = useMemo(
    () =>
      queue.filter((item) => {
        if (item.id === draftId) return true;
        if (filter === 'todo') return item.status === 'todo';
        if (filter === 'done') return item.status === 'committed';
        if (filter === 'skipped') return item.status === 'skipped';
        return true;
      }),
    [queue, filter, draftId]
  );

  const neighbour = useCallback(
    (direction: 1 | -1) => {
      const index = visibleQueue.findIndex((item) => item.id === draftId);
      if (index === -1) return null;
      return visibleQueue[index + direction]?.id ?? null;
    },
    [visibleQueue, draftId]
  );

  // --- Zapis ---

  const updateForm = (patch: Partial<ReceiptForm>) =>
    setForm((current) => (current ? { ...current, ...patch } : current));

  // Sklepy z bazy zaindeksowane po NIP-ie — do doszukania dopasowania przy
  // ręcznej poprawce tego pola w etykiecie (patrz storeManuallySetRef).
  const storesByNip = useMemo(() => {
    const index = new Map<string, Store>();
    for (const store of stores) {
      const nip = normalizeNip(store.nip);
      if (nip) index.set(nip, store);
    }
    return index;
  }, [stores]);

  useEffect(() => {
    if (!form || storeManuallySetRef.current) return;
    const nip = normalizeNip(form.nip);
    if (!nip) return;
    const match = storesByNip.get(nip);
    if (match && match.id !== form.storeId) {
      updateForm({ storeId: match.id });
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [form?.nip, storesByNip]);

  const storeOptions = useMemo<EntityOption[]>(
    () => stores.map((store) => ({ id: store.id, label: store.group_name?.trim() || store.name })),
    [stores]
  );

  const selectedStore = useMemo(
    () => stores.find((store) => store.id === form?.storeId) ?? null,
    [stores, form?.storeId]
  );

  const categoryPathById = useMemo(
    () => new Map(categories.map((category) => [category.id, category.path])),
    [categories]
  );

  const tagSuggestion = useMemo(
    () => suggestTransactionTag({ store: selectedStore, rows, products, categories }),
    [selectedStore, rows, products, categories]
  );

  // Podpowiedź wchodzi tylko w puste pole i tylko dopóki nikt go nie ruszył.
  useEffect(() => {
    if (!form || tagManuallySetRef.current) return;
    if (form.categoryPath.trim() || !tagSuggestion) return;
    updateForm({ categoryPath: tagSuggestion.path });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tagSuggestion, form?.categoryPath]);

  const tagPath = form?.categoryPath.trim() ?? '';
  // Znacznik pochodzenia gaśnie sam, gdy wartość przestaje być tą podpowiedzianą.
  const tagFromSuggestion = Boolean(tagSuggestion) && tagPath === tagSuggestion?.path;
  const storeTagPath = selectedStore?.automatic_category_id
    ? categoryPathById.get(selectedStore.automatic_category_id) ?? ''
    : '';
  const storeLabel = selectedStore?.group_name?.trim() || selectedStore?.name || '';
  const canRememberStoreTag = Boolean(
    selectedStore &&
      tagPath &&
      !(selectedStore.automatically_categorize_payee && storeTagPath === tagPath)
  );

  const currentAccountId = form?.accountId;
  const accountOptions = useMemo<EntityOption[]>(
    () =>
      selectableAccounts(accounts, currentAccountId).map((account) => ({
        id: account.id,
        label: account.name,
        hint: account.currency ?? undefined
      })),
    [accounts, currentAccountId]
  );

  const updateRow = (index: number, patch: Partial<ItemRow>) =>
    setRows((current) => current.map((row, idx) => (idx === index ? { ...row, ...patch } : row)));

  const moveRow = (from: number, to: number) =>
    setRows((current) => {
      const next = [...current];
      const [moved] = next.splice(from, 1);
      next.splice(to, 0, moved);
      return next;
    });

  const persistDraft = useCallback(async () => {
    if (!draft || !payload || draft.status === 'committed') return;
    await saveReceiptDraft(draft.id, { payload });
    await refreshQueue();
  }, [draft, payload, refreshQueue]);

  /** Wyjście z paragonu zawsze przez zapis: wracanie do wsadu ma nic nie gubić. */
  const leaveTo = async (path: string) => {
    try {
      await persistDraft();
    } catch (err) {
      console.error('Nie udało się zapisać szkicu przed przejściem dalej', err);
    }
    navigate(path);
  };

  const goTo = (targetId: string | null) => {
    if (!targetId) return;
    void leaveTo(`/receipts/${targetId}?filter=${filter}`);
  };

  const handleSaveDraft = async () => {
    setBusy(true);
    setError(null);
    try {
      await persistDraft();
      setStatus('Zapisano szkic');
    } catch (err) {
      console.error(err);
      setError('Nie udało się zapisać szkicu.');
    } finally {
      setBusy(false);
    }
  };

  const handleSkip = async () => {
    if (!draft) return;
    setBusy(true);
    try {
      await saveReceiptDraft(draft.id, { payload: payload ?? undefined, status: 'skipped' });
      await refreshQueue();
      const next = neighbour(1);
      if (next) navigate(`/receipts/${next}?filter=${filter}`);
      else navigate('/receipts');
    } catch (err) {
      console.error(err);
      setError('Nie udało się pominąć paragonu.');
    } finally {
      setBusy(false);
    }
  };

  /** Ścieżka tagu → id, zakładając po drodze brakujące segmenty (jak w Obsidianie). */
  const ensureTag = useCallback(
    async (path: string) => {
      const id = await ensureCategoryPath(path, categories, false);
      setCategories(await getAllCategories(300, { for_accounts: false }));
      return id;
    },
    [categories]
  );

  const commit = useCallback(
    async (acknowledged: string[]) => {
      if (!draft || !form || !payload) return;
      setBusy(true);
      setError(null);
      setStatus('Zapisywanie transakcji…');
      try {
        // Tag transakcji podajemy ścieżką, więc brakujące segmenty muszą powstać
        // zanim payload pójdzie dalej; produkty, sklep i aliasy zakłada już
        // serwer w jednym przebiegu z transakcją.
        const categoryId = form.categoryPath.trim()
          ? await ensureTag(form.categoryPath.trim())
          : null;
        const result = await commitReceiptDraft(draft.id, {
          account_id: form.accountId,
          store_id: form.storeId || null,
          category_id: categoryId,
          comment: form.notes.trim() || null,
          payload,
          items: commitItemsFromRows(rows),
          acknowledged_issues: acknowledged,
          merge_into_transaction_id: effectiveMergeInto
        });
        setPendingBlockers(null);
        // Uczymy sklep dopiero po udanym zapisie — z transakcji, która stanęła.
        if (rememberStoreTag && form.storeId && categoryId) {
          try {
            await updateStore(form.storeId, {
              automatic_category_id: categoryId,
              automatically_categorize_payee: true
            });
            setStores(await getAllStores());
          } catch (err) {
            console.error('Nie udało się zapamiętać kategorii dla sklepu', err);
          }
        }
        await refreshQueue();
        setStatus(`Zapisano transakcję`);
        const next = neighbour(1);
        if (next) navigate(`/receipts/${next}?filter=${filter}`);
        else navigate(`/transactions/${result.transaction_id}`);
      } catch (err) {
        console.error(err);
        const serverIssues = commitIssuesFromError(err);
        if (serverIssues.length) {
          // Bramka zatrzymała coś, czego formularz nie pokazał — to znaczy, że
          // wspólne fikstury nie objęły przypadku.
          setPendingBlockers(serverIssues);
          setError('Serwer zatrzymał zapis — sprawdź zastrzeżenia poniżej.');
        } else {
          setError('Nie udało się zapisać transakcji.');
        }
        setStatus(null);
      } finally {
        setBusy(false);
      }
    },
    [
      draft,
      form,
      payload,
      rows,
      ensureTag,
      rememberStoreTag,
      effectiveMergeInto,
      refreshQueue,
      navigate,
      filter,
      neighbour
    ]
  );

  const handleCommit = useCallback(async () => {
    if (!form || !draft || draft.status === 'committed') return;
    if (dateGarbled) {
      setError('Nie umiem odczytać daty. Format: RRRR-MM-DD GG:MM (np. 2024-06-10 18:04).');
      dateFieldRef.current?.focus();
      return;
    }
    if (!form.accountId) {
      setError('Wybierz konto, na które ma trafić wydatek.');
      return;
    }
    if (blockers.length) {
      setPendingBlockers(blockers);
      return;
    }
    await commit([]);
  }, [form, draft, dateGarbled, blockers, commit]);

  // Cmd/Ctrl+Enter — zapis i skok dalej bez sięgania po mysz.
  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key === 'Enter') {
        event.preventDefault();
        void handleCommit();
      }
    };
    document.addEventListener('keydown', onKeyDown);
    return () => document.removeEventListener('keydown', onKeyDown);
  }, [handleCommit]);

  if (!draft || !form) {
    return (
      <div className="page-wrapper compact-page">
        <div className="page-shell">
          <div className="page-body text-secondary">{error ?? status ?? 'Wczytywanie…'}</div>
        </div>
      </div>
    );
  }

  const isCommitted = draft.status === 'committed';

  return (
    <div className="receipt-editor">
      <header className="receipt-editor__header">
        <Button
          variant="link"
          size="sm"
          icon={<ArrowLeftIcon size={16} />}
          onClick={() => void leaveTo('/receipts')}
        >
          Skrzynka
        </Button>
        <span className="text-secondary">
          {visibleQueue.findIndex((item) => item.id === draftId) + 1} / {visibleQueue.length}
        </span>
        {reviewCount ? (
          <span className="receipt-badge receipt-badge--warn">
            Do przejrzenia: {reviewCount}
          </span>
        ) : null}
        {isCommitted ? (
          <span className="receipt-badge receipt-badge--ok">zatwierdzony</span>
        ) : null}

        <span className="flex-grow-1" />

        <label className="receipt-editor__filter">
          <span className="text-secondary">pokaż</span>
          <select
            className="form-select form-select-sm"
            value={filter}
            onChange={(event) => setSearchParams({ filter: event.target.value })}
          >
            {Object.entries(FILTER_LABELS).map(([value, label]) => (
              <option key={value} value={value}>
                {label}
              </option>
            ))}
          </select>
        </label>
        <Button
          variant="outline-secondary"
          size="sm"
          icon={<MoveLeftIcon size={16} />}
          disabled={!neighbour(-1)}
          onClick={() => goTo(neighbour(-1))}
        >
          Poprzedni
        </Button>
        <select
          className="form-select form-select-sm receipt-editor__jump"
          value={draftId}
          onChange={(event) => goTo(event.target.value)}
        >
          {visibleQueue.map((item) => (
            <option key={item.id} value={item.id}>
              {queueMark(item)} {item.source_name} · s.{item.page_number}
              {item.issues.length ? ` — ${item.issues[0].message}` : ''}
            </option>
          ))}
        </select>
        <Button
          variant="outline-secondary"
          size="sm"
          icon={<MoveRightIcon size={16} />}
          iconPosition="right"
          disabled={!neighbour(1)}
          onClick={() => goTo(neighbour(1))}
        >
          Następny
        </Button>
      </header>

      <div className="receipt-editor__body">
        <ReceiptImagePane
          draftId={draft.id}
          hasImage={Boolean(draft.image_url)}
          label={`${draft.source_name} · strona ${draft.page_number}`}
        />

        <div className="receipt-editor__form">
          {draft.error ? (
            <div className="alert alert-danger py-2 px-3">{draft.error}</div>
          ) : null}

          <div className="receipt-field-grid">
            <div className="receipt-field">
              <label className="form-label" htmlFor="receipt-store">
                Sklep
              </label>
              <input
                id="receipt-store"
                className="form-control"
                value={form.storeName}
                onChange={(event) => updateForm({ storeName: event.target.value })}
              />
              <IssueNote issue={issueFor('store_name')} badLabel="brak nazwy" warnLabel="brak nazwy" />
            </div>

            <div className="receipt-field">
              <label className="form-label" htmlFor="receipt-nip">
                NIP
              </label>
              <input
                id="receipt-nip"
                className="form-control"
                inputMode="numeric"
                autoComplete="off"
                value={form.nip}
                onChange={(event) => updateForm({ nip: event.target.value })}
              />
              {/* Świadomie bez przycisku poprawki — patrz IssueNote. */}
              <IssueNote issue={issueFor('nip')} badLabel="sprawdź NIP" />
            </div>

            <div className="receipt-field">
              <label className="form-label" htmlFor="receipt-date">
                Data i godzina
              </label>
              <input
                id="receipt-date"
                ref={dateFieldRef}
                className="form-control"
                type="text"
                inputMode="numeric"
                placeholder="2024-06-10 18:04"
                value={form.dateText}
                onChange={(event) => updateForm({ dateText: event.target.value })}
              />
              {dateGarbled ? (
                <div className="receipt-field__note">
                  <span className="receipt-badge receipt-badge--bad">sprawdź datę</span>
                  <span className="text-secondary">
                    nie umiem odczytać tej daty — format RRRR-MM-DD GG:MM
                  </span>
                </div>
              ) : (
                <IssueNote
                  issue={issueFor('happened_at')}
                  badLabel="sprawdź datę"
                  warnLabel="podejrzana data"
                  onApplySuggestion={(suggestion) =>
                    updateForm({ dateText: formatDateField(suggestion) })
                  }
                />
              )}
            </div>

            <div className="receipt-field receipt-field--split">
              <div>
                <label className="form-label" htmlFor="receipt-total">
                  Suma
                </label>
                <input
                  id="receipt-total"
                  className="form-control text-end"
                  inputMode="decimal"
                  placeholder="0.00"
                  value={form.total}
                  onChange={(event) => updateForm({ total: event.target.value })}
                />
              </div>
              <div className="receipt-field__currency">
                <label className="form-label" htmlFor="receipt-currency">
                  Waluta
                </label>
                <input
                  id="receipt-currency"
                  className="form-control"
                  value={form.currency}
                  onChange={(event) => updateForm({ currency: event.target.value })}
                />
              </div>
            </div>

            <div className="receipt-field">
              <label className="form-label" htmlFor="receipt-account">
                Konto
              </label>
              <EntityAutocomplete
                inputId="receipt-account"
                value={form.accountId}
                options={accountOptions}
                onSelect={(id) => updateForm({ accountId: id })}
                emptyOptionLabel="— wybierz konto —"
                placeholder="Wpisz, aby wyszukać konto"
              />
            </div>

            <div className="receipt-field">
              <label className="form-label" htmlFor="receipt-store-link">
                Sklep w bazie
              </label>
              <EntityAutocomplete
                inputId="receipt-store-link"
                value={form.storeId}
                options={storeOptions}
                onSelect={(id) => {
                  storeManuallySetRef.current = true;
                  updateForm({ storeId: id });
                }}
                emptyOptionLabel="— załóż z etykiety —"
                placeholder="Wpisz, aby wyszukać sklep"
              />
            </div>
          </div>

          <ReceiptItemsTable
            rows={rows}
            products={products}
            categories={categories}
            total={form.total}
            receiptDiscount={form.receiptDiscount}
            matching={matching}
            onChange={updateRow}
            onMove={moveRow}
            onRemove={(index) => setRows((current) => current.filter((_, idx) => idx !== index))}
            onAdd={() => setRows((current) => [...current, emptyRow()])}
          />

          <div className="receipt-field-grid mt-2">
            <div className="receipt-field">
              <label className="form-label" htmlFor="receipt-discounts">
                Rabaty razem (kontrolnie)
              </label>
              <input
                id="receipt-discounts"
                className="form-control text-end"
                inputMode="decimal"
                placeholder="z linii „udzielono rabatów”"
                value={form.discountsDeclared}
                onChange={(event) => updateForm({ discountsDeclared: event.target.value })}
              />
            </div>
            <div className="receipt-field">
              <label className="form-label" htmlFor="receipt-discount">
                Rabat na paragon
              </label>
              <input
                id="receipt-discount"
                className="form-control text-end"
                inputMode="decimal"
                placeholder="tylko gdy nie dotyczy pozycji"
                value={form.receiptDiscount}
                onChange={(event) => updateForm({ receiptDiscount: event.target.value })}
              />
            </div>
            <div className="receipt-field">
              <label className="form-label">Kategoria transakcji</label>
              <TagPathInput
                value={form.categoryPath}
                categories={categories}
                forAccounts={false}
                placeholder="np. codzienne/zakupy"
                onChange={(value) => {
                  tagManuallySetRef.current = true;
                  updateForm({ categoryPath: value });
                }}
              />
              {tagFromSuggestion && tagSuggestion ? (
                <div className="receipt-field__note">
                  <span className="receipt-badge receipt-badge--ok">
                    {tagSuggestion.source === 'store' ? 'ze sklepu' : 'z pozycji'}
                  </span>
                  <span className="text-secondary">
                    {tagSuggestion.source === 'store'
                      ? `zapamiętany dla: ${storeLabel}`
                      : `wspólna kategoria ${tagSuggestion.tagged} z ${tagSuggestion.total} pozycji`}
                  </span>
                </div>
              ) : null}
              {canRememberStoreTag ? (
                <label className="receipt-field__remember">
                  <input
                    type="checkbox"
                    className="form-check-input m-0"
                    checked={rememberStoreTag}
                    onChange={(event) => setRememberStoreTag(event.target.checked)}
                  />
                  <span className="text-secondary">
                    Zapamiętaj tę kategorię dla sklepu {storeLabel}
                  </span>
                </label>
              ) : null}
            </div>
            <div className="receipt-field">
              <label className="form-label" htmlFor="receipt-notes">
                Notatki
              </label>
              <input
                id="receipt-notes"
                className="form-control"
                value={form.notes}
                onChange={(event) => updateForm({ notes: event.target.value })}
              />
            </div>
          </div>

          <details className="receipt-advanced">
            <summary className="text-secondary">Surowy tekst OCR</summary>
            {draft.raw_text ? (
              <pre className="receipt-raw-text mt-2">{draft.raw_text}</pre>
            ) : (
              <p className="text-secondary mt-2 mb-0">(brak surowego tekstu)</p>
            )}
          </details>
        </div>
      </div>

      <footer className="receipt-editor__actions">
        {pendingBlockers?.length ? (
          <div className="receipt-blockers">
            <strong>Zapis zatrzymany:</strong>
            <ul>
              {pendingBlockers.map((issue) => (
                <li key={issue.code}>{issue.message}</li>
              ))}
            </ul>
            <Button
              variant="outline-secondary"
              size="sm"
              onClick={() => void commit(pendingBlockers.map((issue) => issue.code))}
              disabled={busy}
            >
              Zatwierdź mimo to
            </Button>
            <Button variant="link" size="sm" onClick={() => setPendingBlockers(null)}>
              Poprawię
            </Button>
          </div>
        ) : null}

        {visibleBankMatches.length && !isCommitted ? (
          <div className="receipt-bank-match">
            <strong>
              {visibleBankMatches.length === 1
                ? 'Na koncie jest już transakcja z banku:'
                : 'Na koncie są pasujące transakcje z banku:'}
            </strong>
            {visibleBankMatches.map((match) => (
              <label key={match.transaction_id} className="form-check mb-0">
                <input
                  type="checkbox"
                  className="form-check-input"
                  checked={effectiveMergeInto === match.transaction_id}
                  onChange={(event) =>
                    setMergeInto(event.target.checked ? match.transaction_id : null)
                  }
                />
                <span className="form-check-label">
                  podepnij paragon do: {match.date} · {formatMoney(
                    match.amount_cents,
                    accounts.find((a) => a.id === match.account_id)?.currency ?? 'PLN'
                  )} ·{' '}
                  {match.label}
                </span>
              </label>
            ))}
          </div>
        ) : null}

        <span className={error ? 'text-danger' : 'text-secondary'}>{error ?? status ?? ''}</span>
        <span className="flex-grow-1" />
        <Button
          variant="outline-secondary"
          icon={<SkipForwardIcon size={16} />}
          onClick={() => void handleSkip()}
          disabled={busy || isCommitted}
        >
          Pomiń
        </Button>
        <Button
          variant="outline-primary"
          icon={<SaveIcon size={16} />}
          onClick={() => void handleSaveDraft()}
          disabled={busy || isCommitted}
        >
          Zapisz szkic
        </Button>
        <Button
          variant="primary"
          icon={<CheckIcon size={16} />}
          onClick={() => void handleCommit()}
          disabled={busy || isCommitted}
          title="Cmd/Ctrl + Enter"
        >
          Zatwierdź
        </Button>
      </footer>
    </div>
  );
};

export default ReceiptDraftEditorPage;
