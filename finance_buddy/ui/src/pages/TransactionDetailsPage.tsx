import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { ArrowLeftIcon, CheckIcon, CircleAlertIcon, FlagIcon, ImageIcon, ImagePlusIcon, LinkIcon, PlusIcon, Trash2Icon } from 'lucide-react';
import Button from '../components/Button';
import TagPathInput from '../components/tags/TagPathInput';
import { TagChip } from '../components/tags/TagChip';
import { buildCategoryMap } from '../components/tags/tagColors';
import EntityAutocomplete, {
  type EntityOption
} from '../components/transactions/EntityAutocomplete';
import { selectableAccounts } from '../components/transactions/lookupOptions';
import {
  Account,
  Category,
  Item,
  Store,
  TransactionAttachment,
  TransactionDetail,
  type TransactionItemCreate,
  createItem,
  deleteTransactionAttachment,
  ensureCategoryPath,
  getAccounts,
  getAllCategories,
  getAllItems,
  getAllStores,
  getTransaction,
  updateItem,
  updateTransaction,
  uploadTransactionAttachment
} from '../api';
import Money from '../components/ui/Money';
import SkeletonRows from '../components/ui/SkeletonRows';
import { formatDayHeading, formatRecordedTime, toRecordedAt } from '../utils/date';
import { useAuthorizedObjectUrl } from '../hooks/useAuthorizedObjectUrl';
import { formatMoney, parseAmountToCents } from '../utils/format';
import { DateInput, TimeInput } from '../components/ui/DateTimeInput';
import ResizableTable from '../components/ui/ResizableTable';

type RouteParams = {
  transactionId?: string;
};

type TransactionStatusState = 'pending' | 'cleared' | 'reconciled';

type EditableTransactionFields = {
  account_id: string;
  store_id: string;
  comment: string;
  date: string;
  time: string;
};

type NewItemForm = {
  product_id: string;
  product_name: string;
  tag_path: string;
  quantity: string;
  unit_price: string;
  comment: string;
};

const EMPTY_ITEM_FORM: NewItemForm = {
  product_id: '',
  product_name: '',
  tag_path: '',
  quantity: '',
  unit_price: '',
  comment: ''
};

const STATUS_PRESENTATION: Record<
  TransactionStatusState,
  { label: string; description: string; tone: 'neutral' | 'success' | 'info' }
> = {
  pending: { label: 'Przetwarzana', description: 'Oczekuje na potwierdzenie', tone: 'info' },
  cleared: { label: 'Zatwierdzona', description: 'Zaksięgowana w banku', tone: 'neutral' },
  reconciled: { label: 'Uzgodniona', description: 'Sprawdzona i uzgodniona', tone: 'success' }
};

const getStatusState = (transaction: TransactionDetail | null): TransactionStatusState => {
  if (!transaction) return 'pending';
  if (transaction.reconciled) return 'reconciled';
  if (transaction.cleared) return 'cleared';
  return 'pending';
};

const formatDateTime = (value?: string | null) =>
  value ? new Date(value).toLocaleString('pl-PL', { hour12: false }) : '—';

const formatFileSize = (bytes: number) => {
  if (!bytes || bytes < 0) return '—';
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
};

type AttachmentCardProps = {
  attachment: TransactionAttachment;
  onDelete: (attachmentId: string) => void;
  deleting: boolean;
};

/**
 * Kafelek załącznika. Plik pobiera przez `useAuthorizedObjectUrl`, bo endpoint
 * `/transactions/{id}/attachments/{id}` wymaga tokenu — surowy adres API w `src`/`href`
 * zwróciłby 401 (zepsuta miniatura, martwy przycisk „Otwórz").
 */
const AttachmentCard = ({ attachment, onDelete, deleting }: AttachmentCardProps) => {
  const isImage = (attachment.content_type ?? '').startsWith('image/');
  const { url, loading, error } = useAuthorizedObjectUrl(attachment.download_url, {
    errorMessage: 'Nie udało się wczytać załącznika.'
  });

  return (
    <div className="attachment-card">
      <div className="attachment-card__preview">
        {isImage && url ? (
          <a href={url} target="_blank" rel="noreferrer">
            <img src={url} alt={attachment.original_name} />
          </a>
        ) : (
          <div className="attachment-card__placeholder" title={error ?? undefined}>
            {loading ? (
              <span
                className="spinner-border spinner-border-sm"
                role="status"
                aria-label="Wczytywanie załącznika"
              />
            ) : error ? (
              <CircleAlertIcon size={20} aria-hidden="true" />
            ) : (
              <ImageIcon size={20} aria-hidden="true" />
            )}
          </div>
        )}
      </div>
      <div className="attachment-card__body">
        <div className="attachment-card__title" title={attachment.original_name}>
          {attachment.original_name}
        </div>
        <div className="tx-page__muted">
          {formatFileSize(attachment.size_bytes)} · {formatDateTime(attachment.created_at)}
        </div>
      </div>
      <div className="attachment-card__actions">
        {url ? (
          <a className="icon-button" href={url} target="_blank" rel="noreferrer" title="Otwórz" aria-label={`Otwórz ${attachment.original_name}`}>
            <LinkIcon size={14} aria-hidden="true" />
          </a>
        ) : null}
        <button
          type="button"
          className="icon-button"
          title="Usuń"
          aria-label={`Usuń ${attachment.original_name}`}
          onClick={() => onDelete(attachment.id)}
          disabled={deleting}
        >
          <Trash2Icon size={14} aria-hidden="true" />
        </button>
      </div>
    </div>
  );
};

const TransactionDetailsPage = () => {
  const { transactionId } = useParams<RouteParams>();
  const navigate = useNavigate();
  const fileInputRef = useRef<HTMLInputElement | null>(null);

  const [accounts, setAccounts] = useState<Account[]>([]);
  const [stores, setStores] = useState<Store[]>([]);
  const [categories, setCategories] = useState<Category[]>([]);
  const [products, setProducts] = useState<Item[]>([]);
  const [transaction, setTransaction] = useState<TransactionDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [attachmentError, setAttachmentError] = useState<string | null>(null);
  const [uploading, setUploading] = useState(false);
  const [removingAttachmentId, setRemovingAttachmentId] = useState<string | null>(null);
  const [savingForm, setSavingForm] = useState(false);
  const [formError, setFormError] = useState<string | null>(null);
  const [editFields, setEditFields] = useState<EditableTransactionFields>({
    account_id: '',
    store_id: '',
    comment: '',
    date: '',
    time: ''
  });
  const [newItemForm, setNewItemForm] = useState<NewItemForm>(EMPTY_ITEM_FORM);
  const [showItemForm, setShowItemForm] = useState(false);
  const [savingItem, setSavingItem] = useState(false);
  const [itemError, setItemError] = useState<string | null>(null);
  const [categoryPath, setCategoryPath] = useState('');

  const sortedAttachments = useMemo(() => {
    if (!transaction) return [];
    return [...transaction.attachments].sort(
      (a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime()
    );
  }, [transaction]);

  const lastPriceByProduct = useMemo(() => {
    const map = new Map<string, string>();
    transaction?.items.forEach((item) => {
      map.set(item.product_id, (item.unit_price_cents / 100).toFixed(2));
    });
    return map;
  }, [transaction]);

  const accountOptions = useMemo<EntityOption[]>(
    () =>
      selectableAccounts(accounts, transaction?.account_id).map((account) => ({
        id: account.id,
        label: account.name,
        hint: account.currency ?? undefined
      })),
    [accounts, transaction?.account_id]
  );
  const storeOptions = useMemo<EntityOption[]>(
    () => stores.map((store) => ({ id: store.id, label: store.group_name?.trim() || store.name })),
    [stores]
  );
  const productOptions = useMemo<EntityOption[]>(
    () => products.map((product) => ({ id: product.id, label: product.name })),
    [products]
  );
  const categoryPathById = useMemo(() => {
    const map = new Map<string, string>();
    categories.forEach((category) => map.set(category.id, category.path));
    return map;
  }, [categories]);

  const loadTransaction = useCallback(async () => {
    if (!transactionId) {
      setError('Brakuje identyfikatora transakcji.');
      setLoading(false);
      return;
    }
    try {
      setLoading(true);
      const data = await getTransaction(transactionId);
      setTransaction(data);
      setError(null);
    } catch (err) {
      console.error(err);
      setError('Nie udało się wczytać transakcji.');
    } finally {
      setLoading(false);
    }
  }, [transactionId]);

  const loadReferences = useCallback(async () => {
    try {
      const [{ accounts: accountsData }, storesData, categoriesData, productsData] = await Promise.all([
        getAccounts(),
        getAllStores(),
        getAllCategories(200, { for_accounts: false }),
        getAllItems(200)
      ]);
      setAccounts(accountsData);
      setStores(storesData);
      setCategories(categoriesData);
      setProducts(productsData);
    } catch (err) {
      console.error('Nie udało się załadować słowników', err);
    }
  }, []);

  useEffect(() => {
    void loadTransaction();
    void loadReferences();
  }, [loadTransaction, loadReferences]);

  useEffect(() => {
    if (!transaction) return;
    setEditFields({
      account_id: transaction.account_id ?? '',
      store_id: transaction.store_id ?? '',
      comment: transaction.comment ?? '',
      date: transaction.date ?? '',
      time: formatRecordedTime(transaction.recorded_at) ?? ''
    });
    const currentCategory = categories.find((cat) => cat.id === (transaction.category_id ?? ''));
    setCategoryPath(currentCategory?.path ?? '');
  }, [transaction, categories]);

  const categoryMap = useMemo(() => buildCategoryMap(categories), [categories]);
  // Podgląd koloru dla aktualnie wpisanej ścieżki tagu (dopasowanie bez wielkości liter).
  const matchedTagCategory = useMemo(() => {
    const trimmed = categoryPath.trim().toLowerCase();
    if (!trimmed) return undefined;
    return categories.find(
      (cat) => !(cat.is_for_accounts ?? false) && cat.path.toLowerCase() === trimmed
    );
  }, [categoryPath, categories]);

  const openFilePicker = () => {
    fileInputRef.current?.click();
  };

  const handleFileChange = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    event.target.value = '';
    if (!file || !transactionId) return;

    setAttachmentError(null);
    setUploading(true);

    try {
      const attachment = await uploadTransactionAttachment(transactionId, file);
      setTransaction((prev) =>
        prev ? { ...prev, attachments: [attachment, ...prev.attachments] } : prev
      );
    } catch (err) {
      console.error(err);
      setAttachmentError('Nie udało się dodać załącznika.');
    } finally {
      setUploading(false);
    }
  };

  const handleDeleteAttachment = async (attachmentId: string) => {
    if (!transactionId) return;
    if (!window.confirm('Usunąć ten załącznik?')) return;

    setAttachmentError(null);
    setRemovingAttachmentId(attachmentId);
    try {
      await deleteTransactionAttachment(transactionId, attachmentId);
      setTransaction((prev) =>
        prev ? { ...prev, attachments: prev.attachments.filter((a) => a.id !== attachmentId) } : prev
      );
    } catch (err) {
      console.error(err);
      setAttachmentError('Nie udało się usunąć pliku.');
    } finally {
      setRemovingAttachmentId(null);
    }
  };

  const handleFieldChange = (field: keyof EditableTransactionFields, value: string) => {
    setEditFields((prev) => ({ ...prev, [field]: value }));
    setFormError(null);
  };

  const handleSaveFields = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!transaction || !transactionId) return;

    let resolvedCategoryId: string | null = null;
    if (categoryPath.trim()) {
      try {
        resolvedCategoryId = await ensureCategoryPath(categoryPath, categories, false);
      } catch (err) {
        console.error(err);
        setFormError('Nie udało się zapisać tagu.');
        return;
      }
    }

    const payload: Record<string, string | null> = {};
    if (editFields.account_id !== (transaction.account_id ?? '')) payload.account_id = editFields.account_id || null;
    if (editFields.store_id !== (transaction.store_id ?? '')) payload.store_id = editFields.store_id || null;
    if (resolvedCategoryId !== (transaction.category_id ?? null)) payload.category_id = resolvedCategoryId;
    if (editFields.comment !== (transaction.comment ?? '')) payload.comment = editFields.comment || null;
    if (editFields.date !== (transaction.date ?? '')) payload.date = editFields.date || null;
    // Sama zmiana daty zachowuje godzinę po stronie backendu
    if (editFields.time !== (formatRecordedTime(transaction.recorded_at) ?? '')) {
      payload.recorded_at = toRecordedAt(editFields.date, editFields.time);
    }

    if (!Object.keys(payload).length) {
      setFormError('Brak zmian do zapisania.');
      return;
    }

    setSavingForm(true);
    try {
      await updateTransaction(transactionId, payload);
      await loadTransaction();
      setFormError(null);
    } catch (err) {
      console.error(err);
      setFormError('Nie udało się zapisać zmian.');
    } finally {
      setSavingForm(false);
    }
  };

  const handleNewItemChange = (
    field: 'quantity' | 'unit_price' | 'comment' | 'tag_path',
    value: string
  ) => {
    setNewItemForm((prev) => ({ ...prev, [field]: value }));
    setItemError(null);
  };

  // Wybór istniejącego produktu: uzupełnia nazwę, ostatnią cenę oraz tag
  // przypisany do produktu (jeśli ma kategorię).
  const handleSelectProduct = (productId: string) => {
    const product = products.find((prod) => prod.id === productId);
    const tagPath = product?.category_id ? categoryPathById.get(product.category_id) ?? '' : '';
    setNewItemForm((prev) => ({
      ...prev,
      product_id: productId,
      product_name: product?.name ?? '',
      tag_path: tagPath,
      unit_price: lastPriceByProduct.get(productId) ?? prev.unit_price
    }));
    setItemError(null);
  };

  // Wpisanie nowej nazwy: produkt zostanie utworzony przy dodawaniu pozycji.
  const handleCreateProduct = (name: string) => {
    setNewItemForm((prev) => ({ ...prev, product_id: '', product_name: name }));
    setItemError(null);
  };

  const handleAddItem = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!transaction || !transactionId) return;

    const productName = newItemForm.product_name.trim();
    if (!productName) {
      setItemError('Wybierz lub wpisz produkt.');
      return;
    }
    const quantity = parseFloat(newItemForm.quantity.replace(',', '.'));
    if (!Number.isFinite(quantity) || quantity <= 0) {
      setItemError('Podaj dodatnią ilość.');
      return;
    }
    const unitPriceCents = parseAmountToCents(newItemForm.unit_price);
    if (unitPriceCents === null || unitPriceCents < 0) {
      setItemError('Podaj poprawną cenę.');
      return;
    }

    setSavingItem(true);
    try {
      // Tag produktu (opcjonalny) — tworzy brakującą ścieżkę i zwraca id.
      const tagPath = newItemForm.tag_path.trim();
      const productCategoryId = tagPath ? await ensureCategoryPath(tagPath, categories, false) : null;

      // Nowy produkt tworzymy w locie; przy istniejącym aktualizujemy tag, gdy się zmienił.
      let productId = newItemForm.product_id;
      if (!productId) {
        const created = await createItem({ name: productName, category_id: productCategoryId });
        productId = created.id;
      } else if (tagPath) {
        const existing = products.find((prod) => prod.id === productId);
        if (existing && (existing.category_id ?? null) !== productCategoryId) {
          await updateItem(productId, { category_id: productCategoryId });
        }
      }

      const currentItemsPayload: TransactionItemCreate[] = transaction.items.map((item) => ({
        product_id: item.product_id,
        unit_price_cents: item.unit_price_cents,
        quantity: item.quantity,
        unit: item.unit ?? undefined,
        comment: item.comment ?? undefined,
        raw_name: item.raw_name ?? undefined
      }));

      currentItemsPayload.push({
        product_id: productId,
        unit_price_cents: unitPriceCents,
        quantity,
        unit: undefined,
        comment: newItemForm.comment.trim() || undefined
      });

      await updateTransaction(transactionId, { items: currentItemsPayload });
      await Promise.all([loadTransaction(), loadReferences()]);
      setNewItemForm(EMPTY_ITEM_FORM);
      setShowItemForm(false);
      setItemError(null);
    } catch (err) {
      console.error(err);
      setItemError('Nie udało się dodać pozycji.');
    } finally {
      setSavingItem(false);
    }
  };

  const statusState = getStatusState(transaction);
  const statusPresentation = STATUS_PRESENTATION[statusState];

  const store = stores.find((entry) => entry.id === transaction?.store_id);
  const accountName = accounts.find((entry) => entry.id === transaction?.account_id)?.name;
  const heading =
    store?.group_name?.trim() || store?.name || transaction?.comment || (transaction ? 'Transakcja' : '');

  return (
    <div className="page-shell page-stack-layout tx-page">
      <button type="button" className="text-button tx-page__back" onClick={() => navigate('/transactions')}>
        <ArrowLeftIcon size={14} aria-hidden="true" />
        Transakcje
      </button>

      {error && (
        <p className="transactions-error" role="alert">
          {error}
        </p>
      )}

      {loading && <SkeletonRows rows={6} />}

      {!loading && transaction && (
        <>
          <header className="tx-page__header">
            <div className="tx-page__title">
              <h1>{heading}</h1>
              <p>
                {transaction.date ? formatDayHeading(transaction.date) : '—'}
                {formatRecordedTime(transaction.recorded_at) ? `, ${formatRecordedTime(transaction.recorded_at)}` : ''}
                {accountName ? ` · ${accountName}` : ''}
              </p>
            </div>
            <div className="tx-page__total">
              <Money cents={transaction.total_cents} currency={transaction.currency} />
              <span className={`tx-badge tx-badge--${statusPresentation.tone}`} title={statusPresentation.description}>
                {statusState === 'reconciled' ? (
                  <CheckIcon size={14} aria-hidden="true" />
                ) : (
                  <FlagIcon size={14} aria-hidden="true" />
                )}
                {statusPresentation.label}
              </span>
            </div>
          </header>

          <div className="tx-page__layout">
            <div className="tx-page__main">
              <section className="tx-section">
                <header className="tx-section__header">
                  <h2>Pozycje</h2>
                  {!showItemForm && (
                    <button type="button" className="text-button" onClick={() => setShowItemForm(true)}>
                      <PlusIcon size={14} aria-hidden="true" />
                      Dodaj pozycję
                    </button>
                  )}
                </header>
                {transaction.items.length ? (
                  <div className="table-responsive">
                    <ResizableTable tableId="transaction-items" className="tx-items">
                      <thead>
                        <tr>
                          <th>Produkt</th>
                          <th className="tx-items__number">Ilość</th>
                          <th className="tx-items__number">Cena</th>
                          <th className="tx-items__number">Suma</th>
                        </tr>
                      </thead>
                      <tbody>
                        {transaction.items.map((item) => (
                          <tr key={item.id}>
                            <td>
                              {item.product_name ?? 'Produkt'}
                              {item.raw_name && item.raw_name !== item.product_name && (
                                <div className="tx-page__muted" title="Nazwa z paragonu">
                                  {item.raw_name}
                                </div>
                              )}
                              {item.comment && <div className="tx-page__muted">{item.comment}</div>}
                            </td>
                            <td className="tx-items__number">
                              {item.quantity} {item.unit ?? ''}
                            </td>
                            <td className="tx-items__number">{formatMoney(item.unit_price_cents, transaction.currency)}</td>
                            <td className="tx-items__number">
                              {formatMoney(Math.round(item.unit_price_cents * item.quantity), transaction.currency)}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </ResizableTable>
                  </div>
                ) : (
                  !showItemForm && <p className="tx-section__empty">Ta transakcja nie ma pozycji.</p>
                )}
                {showItemForm && (
                  <form className="tx-item-form" onSubmit={handleAddItem}>
                    {itemError && (
                      <p className="transactions-error tx-item-form__wide" role="alert">
                        {itemError}
                      </p>
                    )}
                    <div className="tx-item-form__wide">
                      <label className="form-label">Produkt</label>
                      <EntityAutocomplete
                        value={newItemForm.product_id}
                        options={productOptions}
                        fallbackLabel={newItemForm.product_name}
                        onSelect={handleSelectProduct}
                        onCreate={handleCreateProduct}
                        placeholder="Wpisz, aby wyszukać lub utworzyć"
                      />
                    </div>
                    <div>
                      <label className="form-label" htmlFor="tx-item-quantity">Ilość</label>
                      <input
                        id="tx-item-quantity"
                        type="number"
                        step="0.01"
                        min="0"
                        className="form-control"
                        value={newItemForm.quantity}
                        onChange={(e) => handleNewItemChange('quantity', e.target.value)}
                        required
                      />
                    </div>
                    <div>
                      <label className="form-label" htmlFor="tx-item-price">Cena jedn.</label>
                      <input
                        id="tx-item-price"
                        type="number"
                        step="0.01"
                        min="0"
                        className="form-control"
                        value={newItemForm.unit_price}
                        onChange={(e) => handleNewItemChange('unit_price', e.target.value)}
                        required
                      />
                    </div>
                    <div>
                      <label className="form-label">Tag produktu</label>
                      <TagPathInput
                        value={newItemForm.tag_path}
                        categories={categories}
                        forAccounts={false}
                        placeholder="np. spożywcze/nabiał"
                        onChange={(value) => handleNewItemChange('tag_path', value)}
                      />
                    </div>
                    <div>
                      <label className="form-label" htmlFor="tx-item-comment">Komentarz</label>
                      <input
                        id="tx-item-comment"
                        type="text"
                        className="form-control"
                        value={newItemForm.comment}
                        onChange={(e) => handleNewItemChange('comment', e.target.value)}
                      />
                    </div>
                    <div className="tx-item-form__actions">
                      <button
                        type="button"
                        className="text-button"
                        onClick={() => {
                          setShowItemForm(false);
                          setItemError(null);
                        }}
                      >
                        Anuluj
                      </button>
                      <Button type="submit" disabled={savingItem}>
                        {savingItem ? 'Dodawanie…' : 'Dodaj pozycję'}
                      </Button>
                    </div>
                  </form>
                )}
              </section>

              <section className="tx-section">
                <header className="tx-section__header">
                  <h2>Załączniki</h2>
                  <button type="button" className="text-button" onClick={openFilePicker} disabled={uploading}>
                    <ImagePlusIcon size={14} aria-hidden="true" />
                    {uploading ? 'Ładowanie…' : 'Dodaj zdjęcie'}
                  </button>
                  <input
                    type="file"
                    accept="image/*"
                    ref={fileInputRef}
                    className="d-none"
                    aria-label="Zdjęcie załącznika"
                    onChange={(event) => void handleFileChange(event)}
                  />
                </header>
                {attachmentError && (
                  <p className="transactions-error tx-section__empty" role="alert">
                    {attachmentError}
                  </p>
                )}
                {sortedAttachments.length ? (
                  <div className="transaction-attachments-grid">
                    {sortedAttachments.map((attachment) => (
                      <AttachmentCard
                        key={attachment.id}
                        attachment={attachment}
                        onDelete={(attachmentId) => void handleDeleteAttachment(attachmentId)}
                        deleting={removingAttachmentId === attachment.id}
                      />
                    ))}
                  </div>
                ) : (
                  <p className="tx-section__empty">Brak załączników.</p>
                )}
              </section>
            </div>

            <form className="tx-section tx-page__form" onSubmit={handleSaveFields}>
              <header className="tx-section__header">
                <h2>Dane</h2>
              </header>
              <div className="tx-page__fields">
                {formError && (
                  <p className="transactions-error" role="alert">
                    {formError}
                  </p>
                )}
                <div>
                  <label className="form-label" htmlFor="tx-date">Data księgowania</label>
                  <DateInput
                    id="tx-date"
                    className="form-control"
                    value={editFields.date}
                    onChange={(value) => handleFieldChange('date', value)}
                    required
                  />
                </div>
                <div>
                  <label className="form-label" htmlFor="tx-time">Godzina (opcjonalnie)</label>
                  <TimeInput
                    id="tx-time"
                    className="form-control"
                    value={editFields.time}
                    onChange={(value) => handleFieldChange('time', value)}
                  />
                </div>
                <div>
                  <label className="form-label">Konto</label>
                  <EntityAutocomplete
                    value={editFields.account_id}
                    options={accountOptions}
                    onSelect={(id) => handleFieldChange('account_id', id)}
                    placeholder="Wpisz, aby wyszukać konto"
                  />
                </div>
                <div>
                  <label className="form-label">Sklep</label>
                  <EntityAutocomplete
                    value={editFields.store_id}
                    options={storeOptions}
                    onSelect={(id) => handleFieldChange('store_id', id)}
                    emptyOptionLabel="Brak"
                    placeholder="Wpisz, aby wyszukać sklep"
                  />
                </div>
                <div>
                  <label className="form-label">Kategoria</label>
                  <TagPathInput
                    value={categoryPath}
                    categories={categories}
                    forAccounts={false}
                    placeholder="np. dom/remont"
                    onChange={setCategoryPath}
                  />
                  {matchedTagCategory && (
                    <div className="mt-1">
                      <TagChip category={matchedTagCategory} byId={categoryMap} label={matchedTagCategory.path} />
                    </div>
                  )}
                </div>
                <div>
                  <label className="form-label" htmlFor="tx-comment">Komentarz</label>
                  <textarea
                    id="tx-comment"
                    className="form-control"
                    rows={2}
                    value={editFields.comment}
                    onChange={(e) => handleFieldChange('comment', e.target.value)}
                  />
                </div>
                <Button type="submit" disabled={savingForm} fullWidth className="justify-content-center">
                  {savingForm ? 'Zapisywanie…' : 'Zapisz zmiany'}
                </Button>
              </div>
            </form>
          </div>
        </>
      )}
    </div>
  );
};

export default TransactionDetailsPage;
