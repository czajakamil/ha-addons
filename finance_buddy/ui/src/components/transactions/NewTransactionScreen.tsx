// „Nowa transakcja" na telefonie: pełny ekran z dużą kwotą, wierszami pól i wyborem sklepu
// lub kategorii na osobnych podekranach. Natywny <dialog> daje warstwę nad stroną, pułapkę
// fokusu i Esc (Esc na podekranie wraca do formularza).
import { useEffect, useMemo, useRef, useState, type CSSProperties, type FormEvent, type ReactNode } from 'react';
import { toast } from 'sonner';
import { ArrowLeftRight, Camera, Check, ChevronLeft, ChevronRight, FileText, Plus, Search, X } from 'lucide-react';
import type { Account, Category, Store, Transaction, TransactionCreate } from '../../api';
import { createStore, createTransaction, uploadTransactionAttachment } from '../../api';
import { resolveTagColor } from '../tags/tagColors';
import { toIsoDate, toRecordedAt } from '../../utils/date';
import { parseAmountToCents } from '../../utils/format';
import { formatSignedMoney } from '../ui/Money';
import {
  categoryParentPrefix,
  categoryPickerRows,
  recentCategoryIds,
  recentStoreIds,
  storeDisplayName,
  suggestedCategoryId
} from './newTransactionModel';

type Screen = 'form' | 'store' | 'category' | 'account';

// Sklep: istniejący, nowy (powstanie przy zapisie) albo przelew na inne konto
type Payee =
  | { kind: 'none' }
  | { kind: 'store'; storeId: string }
  | { kind: 'newStore'; name: string }
  | { kind: 'transfer'; accountId: string };

// Plik czeka w formularzu do zapisu: załącznik wymaga istniejącej transakcji
type PendingFile = { id: number; file: File; previewUrl: string | null };

const ACCEPTED_FILES = 'image/*,application/pdf';

// Podgląd zdjęcia to adres blob: — zwalniamy go przy usunięciu pliku, nowym otwarciu i zamknięciu
const revokePreviews = (list: readonly PendingFile[]) =>
  list.forEach((entry) => entry.previewUrl && URL.revokeObjectURL(entry.previewUrl));

type NewTransactionScreenProps = {
  open: boolean;
  accounts: Account[];
  stores: Store[];
  categories: Category[];
  /** Ostatnie transakcje (od najnowszej) — źródło podpowiedzi sklepów i kategorii. */
  recentTransactions: readonly Transaction[];
  defaultAccountId?: string;
  onClose: () => void;
  onCreated: () => Promise<void>;
  /** Sklep wpisany z ręki i utworzony przy zapisie. */
  onStoreCreated?: (store: Store) => void;
};

const nowTime = () => {
  const now = new Date();
  return `${String(now.getHours()).padStart(2, '0')}:${String(now.getMinutes()).padStart(2, '0')}`;
};

const Dot = ({ color }: { color: string }) => (
  <span className="new-tx__dot" style={{ background: color } as CSSProperties} aria-hidden="true" />
);

const SubHeader = ({ title, onBack }: { title: string; onBack: () => void }) => (
  <div className="new-tx__header">
    <button type="button" className="new-tx__back is-focusable" onClick={onBack}>
      <ChevronLeft size={18} aria-hidden="true" />
      Wstecz
    </button>
    <h2 className="new-tx__title">{title}</h2>
    <span />
  </div>
);

const SearchField = ({ value, onChange, placeholder }: { value: string; onChange: (v: string) => void; placeholder: string }) => (
  <label className="new-tx__search">
    <Search size={16} aria-hidden="true" />
    <span className="sr-only">{placeholder}</span>
    <input type="search" value={value} placeholder={placeholder} autoFocus onChange={(event) => onChange(event.target.value)} />
  </label>
);

const Section = ({ title, children }: { title: string; children: ReactNode }) => (
  <div>
    <div className="new-tx__section">{title}</div>
    {children}
  </div>
);

const NewTransactionScreen = ({
  open,
  accounts,
  stores,
  categories,
  recentTransactions,
  defaultAccountId,
  onClose,
  onCreated,
  onStoreCreated
}: NewTransactionScreenProps) => {
  const dialogRef = useRef<HTMLDialogElement>(null);
  const [screen, setScreen] = useState<Screen>('form');
  const [search, setSearch] = useState('');
  const [negative, setNegative] = useState(true);
  const [amount, setAmount] = useState('');
  const [accountId, setAccountId] = useState('');
  const [payee, setPayee] = useState<Payee>({ kind: 'none' });
  const [categoryId, setCategoryId] = useState<string | null>(null);
  // Kategoria wybrana ręcznie nie ustępuje podpowiedzi po zmianie sklepu
  const [categoryTouched, setCategoryTouched] = useState(false);
  const [date, setDate] = useState('');
  const [time, setTime] = useState('');
  const [comment, setComment] = useState('');
  const [cleared, setCleared] = useState(true);
  const [files, setFiles] = useState<PendingFile[]>([]);
  // Ostatnia lista plików dla sprzątania przy nowym otwarciu i odmontowaniu
  const filesRef = useRef<PendingFile[]>([]);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const nextFileId = useRef(0);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const categoryMap = useMemo(() => new Map(categories.map((category) => [category.id, category])), [categories]);
  const categoryIds = useMemo(() => new Set(categories.map((category) => category.id)), [categories]);
  const storeMap = useMemo(() => new Map(stores.map((store) => [store.id, store])), [stores]);

  useEffect(() => {
    const dialog = dialogRef.current;
    if (!dialog) return;
    if (open && !dialog.open) {
      setScreen('form');
      setSearch('');
      setNegative(true);
      setAmount('');
      setAccountId(defaultAccountId ?? accounts[0]?.id ?? '');
      setPayee({ kind: 'none' });
      setCategoryId(null);
      setCategoryTouched(false);
      setDate(toIsoDate(new Date()));
      setTime(nowTime());
      setComment('');
      setCleared(true);
      revokePreviews(filesRef.current);
      setFiles([]);
      setError(null);
      dialog.showModal();
    }
    if (!open && dialog.open) dialog.close();
  }, [open, defaultAccountId, accounts]);

  useEffect(() => {
    filesRef.current = files;
  }, [files]);
  useEffect(() => () => revokePreviews(filesRef.current), []);

  const addFiles = (list: FileList | null) => {
    if (!list?.length) return;
    const added = Array.from(list).map((file) => ({
      id: nextFileId.current++,
      file,
      previewUrl: file.type.startsWith('image/') ? URL.createObjectURL(file) : null
    }));
    setFiles((prev) => [...prev, ...added]);
  };

  const removeFile = (id: number) => {
    revokePreviews(files.filter((entry) => entry.id === id));
    setFiles((prev) => prev.filter((entry) => entry.id !== id));
  };

  const account = accounts.find((entry) => entry.id === accountId);
  const currency = account?.currency ?? 'PLN';
  const isTransfer = payee.kind === 'transfer';
  const transferAccount = isTransfer ? accounts.find((entry) => entry.id === payee.accountId) : undefined;
  const amountCents = Math.abs(parseAmountToCents(amount) ?? 0);
  const signedCents = negative ? -amountCents : amountCents;
  const category = categoryId ? categoryMap.get(categoryId) : undefined;
  const store = payee.kind === 'store' ? storeMap.get(payee.storeId) : undefined;
  const payeeLabel =
    payee.kind === 'store'
      ? store
        ? storeDisplayName(store)
        : '—'
      : payee.kind === 'newStore'
        ? payee.name
        : isTransfer
          ? transferAccount?.name ?? '—'
          : null;

  const openScreen = (next: Screen) => {
    setSearch('');
    setScreen(next);
  };

  const pickStore = (storeId: string) => {
    setPayee({ kind: 'store', storeId });
    if (!categoryTouched) {
      const suggested = suggestedCategoryId(storeMap.get(storeId), recentTransactions, categoryIds);
      setCategoryId(suggested);
      // Kategoria przychodu (do przypisania) oznacza wpływ
      if (suggested && categoryMap.get(suggested)?.is_for_accounts) setNegative(false);
    }
    setScreen('form');
  };

  const pickCategory = (id: string) => {
    setCategoryId(id);
    setCategoryTouched(true);
    setScreen('form');
  };

  const amountHint = isTransfer
    ? negative
      ? `Przelew z tego konta na ${transferAccount?.name ?? '—'}`
      : `Przelew na to konto z ${transferAccount?.name ?? '—'}`
    : negative
      ? 'Wydatek · stuknij −, żeby zmienić na wpływ'
      : 'Wpływ · stuknij +, żeby zmienić na wydatek';

  const handleSubmit = async (event: FormEvent) => {
    event.preventDefault();
    setError(null);
    if (!account) {
      setError('Wybierz konto.');
      return;
    }
    if (!amountCents) {
      setError('Podaj kwotę.');
      return;
    }
    if (isTransfer && payee.accountId === account.id) {
      setError('Przelew wymaga innego konta.');
      return;
    }
    setSubmitting(true);
    try {
      let storeId: string | undefined;
      if (payee.kind === 'store') storeId = payee.storeId;
      if (payee.kind === 'newStore') {
        const created = await createStore({ name: payee.name.trim() });
        onStoreCreated?.(created);
        storeId = created.id;
      }
      const payload: TransactionCreate = {
        account_id: account.id,
        store_id: isTransfer ? undefined : storeId,
        category_id: isTransfer ? undefined : categoryId ?? undefined,
        transfer_account_id: isTransfer ? payee.accountId : undefined,
        date: date || undefined,
        recorded_at: date && time ? toRecordedAt(date, time) : undefined,
        currency,
        comment: comment.trim() || undefined,
        cleared,
        reconciled: false,
        items: [],
        total_cents: signedCents
      };
      const created = await createTransaction(payload);
      let failedUploads = 0;
      for (const entry of files) {
        try {
          await uploadTransactionAttachment(created.id, entry.file);
        } catch (err) {
          console.error(err);
          failedUploads += 1;
        }
      }
      await onCreated();
      onClose();
      if (failedUploads) {
        toast.error(
          `Transakcja zapisana, ale nie udało się dołączyć plików: ${failedUploads} z ${files.length}. Dodasz je w szczegółach transakcji.`
        );
      }
    } catch (err) {
      console.error(err);
      setError('Nie udało się zapisać transakcji. Spróbuj ponownie.');
    } finally {
      setSubmitting(false);
    }
  };

  const query = search.trim().toLocaleLowerCase('pl');

  const renderStorePicker = () => {
    const transferTargets = accounts.filter(
      (entry) => entry.id !== accountId && (!query || entry.name.toLocaleLowerCase('pl').includes(query))
    );
    const storeList = query
      ? stores
          .filter((entry) => !entry.is_deleted)
          .filter((entry) => `${entry.name} ${entry.group_name ?? ''}`.toLocaleLowerCase('pl').includes(query))
          .slice(0, 50)
      : recentStoreIds(recentTransactions)
          .map((id) => storeMap.get(id))
          .filter((entry): entry is Store => Boolean(entry));
    const exact = stores.some((entry) => storeDisplayName(entry).toLocaleLowerCase('pl') === query);
    return (
      <>
        <SubHeader title="Sklep" onBack={() => setScreen('form')} />
        <div className="new-tx__picker-search">
          <SearchField value={search} onChange={setSearch} placeholder="Szukaj lub wpisz nowy…" />
        </div>
        <div className="new-tx__picker-list">
          {query && !exact && (
            <button
              type="button"
              className="new-tx__option"
              onClick={() => {
                setPayee({ kind: 'newStore', name: search.trim() });
                setScreen('form');
              }}
            >
              <span className="new-tx__avatar" aria-hidden="true">
                <Plus size={15} />
              </span>
              <span className="new-tx__option-text">
                <span>Dodaj sklep „{search.trim()}”</span>
                <span className="new-tx__muted">Nowy sklep powstanie przy zapisie</span>
              </span>
            </button>
          )}
          {transferTargets.length > 0 && (
            <Section title="Przelew na konto">
              {transferTargets.map((entry) => (
                <button
                  key={entry.id}
                  type="button"
                  className="new-tx__option"
                  onClick={() => {
                    setPayee({ kind: 'transfer', accountId: entry.id });
                    setScreen('form');
                  }}
                >
                  <span className="new-tx__avatar" aria-hidden="true">
                    <ArrowLeftRight size={15} />
                  </span>
                  <span className="new-tx__option-text">
                    <span>{entry.name}</span>
                    <span className="new-tx__muted">saldo {formatSignedMoney(entry.balance_cents, entry.currency)}</span>
                  </span>
                </button>
              ))}
            </Section>
          )}
          {storeList.length > 0 && (
            <Section title={query ? 'Sklepy' : 'Ostatnie sklepy'}>
              {storeList.map((entry) => {
                const name = storeDisplayName(entry);
                const suggested = suggestedCategoryId(entry, recentTransactions, categoryIds);
                const suggestedCategory = suggested ? categoryMap.get(suggested) : undefined;
                return (
                  <button key={entry.id} type="button" className="new-tx__option" onClick={() => pickStore(entry.id)}>
                    <span className="new-tx__avatar" aria-hidden="true">
                      {name.charAt(0).toLocaleUpperCase('pl')}
                    </span>
                    <span className="new-tx__option-text">
                      <span>{name}</span>
                      {suggestedCategory && (
                        <span className="new-tx__muted new-tx__inline">
                          <Dot color={resolveTagColor(suggestedCategory, categoryMap).value} />
                          {suggestedCategory.name}
                        </span>
                      )}
                    </span>
                  </button>
                );
              })}
            </Section>
          )}
        </div>
      </>
    );
  };

  const categoryRow = (id: string, key: string, prefix: string, depth = 0, selectable = true) => {
    const entry = categoryMap.get(id);
    if (!entry) return null;
    const color = resolveTagColor(entry, categoryMap).value;
    const content = (
      <>
        <span style={{ width: depth * 20, flex: 'none' }} />
        <Dot color={color} />
        <span className="new-tx__option-label">
          {prefix && <span className="new-tx__muted">{prefix}</span>}
          {entry.name}
        </span>
        {selectable && id === categoryId && <Check size={18} aria-hidden="true" />}
      </>
    );
    return selectable ? (
      <button
        key={key}
        type="button"
        className="new-tx__option new-tx__option--compact"
        aria-pressed={id === categoryId}
        onClick={() => pickCategory(id)}
      >
        {content}
      </button>
    ) : (
      <div key={key} className="new-tx__option new-tx__option--compact new-tx__option--group">
        {content}
      </div>
    );
  };

  const renderCategoryPicker = () => {
    const suggested = isTransfer ? null : suggestedCategoryId(store, recentTransactions, categoryIds);
    const recent = recentCategoryIds(recentTransactions).filter((id) => categoryMap.has(id));
    const rows = categoryPickerRows(categories, search);
    return (
      <>
        <SubHeader title="Kategoria" onBack={() => setScreen('form')} />
        <div className="new-tx__picker-search">
          <SearchField value={search} onChange={setSearch} placeholder="Szukaj kategorii…" />
        </div>
        <div className="new-tx__picker-list">
          {!query && suggested && store && (
            <Section title={`Podpowiedź dla sklepu ${storeDisplayName(store)}`}>
              {categoryRow(suggested, `s-${suggested}`, categoryParentPrefix(categoryMap.get(suggested)!, categoryMap))}
            </Section>
          )}
          {!query && recent.length > 0 && (
            <Section title="Ostatnio używane">
              {recent.map((id) => categoryRow(id, `r-${id}`, categoryParentPrefix(categoryMap.get(id)!, categoryMap)))}
            </Section>
          )}
          <Section title={query ? 'Wyniki' : 'Wszystkie'}>
            {rows.map((row) => categoryRow(row.id, `a-${row.id}`, row.prefix, row.depth, row.selectable))}
            {rows.length === 0 && <p className="new-tx__empty">Brak kategorii pasujących do wyszukiwania.</p>}
          </Section>
        </div>
      </>
    );
  };

  const renderAccountPicker = () => (
    <>
      <SubHeader title="Konto" onBack={() => setScreen('form')} />
      <div className="new-tx__picker-list">
        {accounts.map((entry) => (
          <button
            key={entry.id}
            type="button"
            className="new-tx__option"
            aria-pressed={entry.id === accountId}
            onClick={() => {
              setAccountId(entry.id);
              if (payee.kind === 'transfer' && payee.accountId === entry.id) setPayee({ kind: 'none' });
              setScreen('form');
            }}
          >
            <span className="new-tx__option-text">
              <span>{entry.name}</span>
              <span className="new-tx__muted">saldo {formatSignedMoney(entry.balance_cents, entry.currency)}</span>
            </span>
            {entry.id === accountId && <Check size={18} aria-hidden="true" />}
          </button>
        ))}
      </div>
    </>
  );

  const renderForm = () => (
    <form className="new-tx__form" onSubmit={handleSubmit}>
      <div className="new-tx__header">
        <button type="button" className="new-tx__icon-button is-focusable" aria-label="Anuluj" onClick={onClose}>
          <X size={20} aria-hidden="true" />
        </button>
        <h2 className="new-tx__title">Nowa transakcja</h2>
        <span />
      </div>

      <div className="new-tx__body">
        <div className="new-tx__amount">
          <div className="new-tx__amount-row">
            <button
              type="button"
              className={`new-tx__sign${negative ? '' : ' is-positive'}`}
              aria-label={negative ? 'Wydatek. Zmień na wpływ' : 'Wpływ. Zmień na wydatek'}
              onClick={() => setNegative((value) => !value)}
            >
              {negative ? '−' : '+'}
            </button>
            <label className="new-tx__amount-field">
              <span className="sr-only">Kwota</span>
              <input
                type="text"
                inputMode="decimal"
                autoComplete="off"
                placeholder="0,00"
                value={amount}
                style={{ width: `${Math.max(4, amount.length + 0.5)}ch` }}
                onChange={(event) => setAmount(event.target.value.replace(/[^\d.,\s]/g, ''))}
              />
              <span className="new-tx__currency">{currency === 'PLN' ? 'zł' : currency}</span>
            </label>
          </div>
          <span className="new-tx__muted" aria-live="polite">
            {amountHint}
          </span>
        </div>

        {error && (
          <p className="new-tx__error" role="alert">
            {error}
          </p>
        )}

        <div className="new-tx__card">
          <button type="button" className="new-tx__row" onClick={() => openScreen('store')}>
            <span className="new-tx__row-label">Sklep</span>
            <span className="new-tx__row-value">
              {isTransfer && <ArrowLeftRight size={15} className="new-tx__muted-icon" aria-hidden="true" />}
              <span className={payeeLabel ? '' : 'new-tx__muted'}>{payeeLabel ?? 'Wybierz'}</span>
            </span>
            <ChevronRight size={16} className="new-tx__muted-icon" aria-hidden="true" />
          </button>

          {isTransfer ? (
            <div className="new-tx__row">
              <span className="new-tx__row-label">Kategoria</span>
              <span className="new-tx__row-value new-tx__row-value--stack">
                <span className="new-tx__muted">Przelew — bez kategorii</span>
                <span className="new-tx__hint">Nie liczy się do wydatków w Raportach</span>
              </span>
            </div>
          ) : (
            <button type="button" className="new-tx__row" onClick={() => openScreen('category')}>
              <span className="new-tx__row-label">Kategoria</span>
              <span className="new-tx__row-value new-tx__row-value--stack">
                {category ? (
                  <span className="new-tx__inline">
                    <Dot color={resolveTagColor(category, categoryMap).value} />
                    <span className="new-tx__option-label">{category.name}</span>
                  </span>
                ) : (
                  <span className="new-tx__muted">Bez kategorii</span>
                )}
                {category && !categoryTouched && payeeLabel && (
                  <span className="new-tx__hint">Jak zwykle w {payeeLabel}</span>
                )}
              </span>
              <ChevronRight size={16} className="new-tx__muted-icon" aria-hidden="true" />
            </button>
          )}

          <button type="button" className="new-tx__row" onClick={() => openScreen('account')}>
            <span className="new-tx__row-label">Konto</span>
            <span className="new-tx__row-value">{account?.name ?? 'Wybierz'}</span>
            <ChevronRight size={16} className="new-tx__muted-icon" aria-hidden="true" />
          </button>

          <div className="new-tx__row">
            <span className="new-tx__row-label">Kiedy</span>
            <span className="new-tx__row-value new-tx__when">
              <input type="date" aria-label="Data" value={date} required onChange={(event) => setDate(event.target.value)} />
              <input type="time" aria-label="Godzina" value={time} onChange={(event) => setTime(event.target.value)} />
            </span>
          </div>

          <label className="new-tx__row">
            <span className="new-tx__row-label">Komentarz</span>
            <input
              type="text"
              className="new-tx__row-input"
              placeholder="Opcjonalnie"
              value={comment}
              onChange={(event) => setComment(event.target.value)}
            />
          </label>
        </div>

        <div className="new-tx__card">
          <button
            type="button"
            role="switch"
            aria-checked={cleared}
            className="new-tx__row new-tx__switch"
            onClick={() => setCleared((value) => !value)}
          >
            <Check size={18} className={cleared ? '' : 'new-tx__muted-icon'} aria-hidden="true" />
            <span className="new-tx__row-value">Zatwierdzona</span>
            <span className="new-tx__track" aria-hidden="true" />
          </button>
        </div>

        <input
          ref={fileInputRef}
          type="file"
          accept={ACCEPTED_FILES}
          multiple
          hidden
          onChange={(event) => {
            addFiles(event.target.files);
            // Ten sam plik wybrany drugi raz też ma wywołać onChange
            event.target.value = '';
          }}
        />
        {files.length === 0 ? (
          <button type="button" className="new-tx__receipt" onClick={() => fileInputRef.current?.click()}>
            <Camera size={20} className="new-tx__muted-icon" aria-hidden="true" />
            <span className="new-tx__row-value new-tx__row-value--stack">
              <span>Zdjęcie paragonu lub plik</span>
              <span className="new-tx__hint">Zrób zdjęcie albo wybierz z galerii lub PDF — dołączymy je do transakcji</span>
            </span>
          </button>
        ) : (
          <div className="new-tx__files" role="group" aria-label="Załączniki">
            {files.map((entry) => (
              <div key={entry.id} className="new-tx__file">
                {entry.previewUrl ? (
                  <img src={entry.previewUrl} alt={entry.file.name} />
                ) : (
                  <span className="new-tx__file-doc">
                    <FileText size={22} aria-hidden="true" />
                    <span>{entry.file.name}</span>
                  </span>
                )}
                <button
                  type="button"
                  className="new-tx__file-remove"
                  aria-label={`Usuń ${entry.file.name}`}
                  onClick={() => removeFile(entry.id)}
                >
                  <X size={14} aria-hidden="true" />
                </button>
              </div>
            ))}
            <button type="button" className="new-tx__file new-tx__file-add" onClick={() => fileInputRef.current?.click()}>
              <Plus size={20} aria-hidden="true" />
              Dodaj
            </button>
          </div>
        )}
      </div>

      <div className="new-tx__footer">
        <button type="submit" className="new-tx__submit" disabled={submitting}>
          {submitting
            ? files.length
              ? 'Zapisywanie i dołączanie plików…'
              : 'Zapisywanie…'
            : `Dodaj transakcję${amountCents ? ` · ${formatSignedMoney(signedCents, currency)}` : ''}`}
        </button>
      </div>
    </form>
  );

  return (
    <dialog
      ref={dialogRef}
      className="new-tx"
      aria-label="Nowa transakcja"
      onCancel={(event) => {
        // Esc na podekranie wraca do formularza zamiast zamykać całość
        if (screen !== 'form') {
          event.preventDefault();
          setScreen('form');
        }
      }}
      onClose={onClose}
    >
      {open && (
        <div className="new-tx__screen">
          {screen === 'form' && renderForm()}
          {screen === 'store' && renderStorePicker()}
          {screen === 'category' && renderCategoryPicker()}
          {screen === 'account' && renderAccountPicker()}
        </div>
      )}
    </dialog>
  );
};

export default NewTransactionScreen;
