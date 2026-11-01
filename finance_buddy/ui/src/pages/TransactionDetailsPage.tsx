import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import {
  IconAlertCircle,
  IconArrowLeft,
  IconCheck,
  IconCircleDot,
  IconFlag,
  IconLink,
  IconPhoto,
  IconPhotoPlus,
  IconRefresh,
  IconTrash,
  IconPlus
} from '@tabler/icons-react';
import Button from '../components/Button';
import TagPathInput from '../components/tags/TagPathInput';
import { TagChip } from '../components/tags/TagChip';
import { buildCategoryMap } from '../components/tags/tagColors';
import EntityAutocomplete, {
  type EntityOption
} from '../components/transactions/EntityAutocomplete';
import {
  Account,
  Category,
  Item,
  Store,
  TransactionAttachment,
  TransactionDetail,
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
import { useAuthorizedObjectUrl } from '../hooks/useAuthorizedObjectUrl';
import { formatMoney, parseAmountToCents } from '../utils/format';

type RouteParams = {
  transactionId?: string;
};

type TransactionDetailsPageProps = {
  transactionId?: string;
  embedded?: boolean;
};

type TransactionStatusState = 'pending' | 'cleared' | 'reconciled';

type EditableTransactionFields = {
  account_id: string;
  store_id: string;
  comment: string;
  date: string;
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
  cleared: { label: 'Cleared', description: 'Zaksięgowana w banku', tone: 'neutral' },
  reconciled: { label: 'Reconciled', description: 'Sprawdzona i uzgodniona', tone: 'success' }
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
              <IconAlertCircle size={20} aria-hidden="true" />
            ) : (
              <IconPhoto size={20} aria-hidden="true" />
            )}
          </div>
        )}
      </div>
      <div className="attachment-card__body">
        <div className="fw-semibold attachment-card__title" title={attachment.original_name}>
          {attachment.original_name}
        </div>
        <div className="text-secondary small">
          {formatFileSize(attachment.size_bytes)} · {formatDateTime(attachment.created_at)}
        </div>
      </div>
      <div className="attachment-card__actions">
        {url ? (
          <a
            className="btn btn-light btn-sm d-inline-flex align-items-center gap-1 is-focusable"
            href={url}
            target="_blank"
            rel="noreferrer"
          >
            <IconLink size={14} aria-hidden="true" />
            <span>Otwórz</span>
          </a>
        ) : (
          <button
            type="button"
            className="btn btn-light btn-sm d-inline-flex align-items-center gap-1 is-focusable"
            disabled
          >
            <IconLink size={14} aria-hidden="true" />
            <span>{loading ? 'Ładowanie…' : 'Otwórz'}</span>
          </button>
        )}
        <button
          type="button"
          className="btn btn-outline-danger btn-sm d-inline-flex align-items-center gap-1 is-focusable"
          onClick={() => onDelete(attachment.id)}
          disabled={deleting}
        >
          <IconTrash size={14} aria-hidden="true" />
          <span>Usuń</span>
        </button>
      </div>
    </div>
  );
};

const TransactionDetailsPage = ({
  transactionId: transactionIdProp,
  embedded = false
}: TransactionDetailsPageProps = {}) => {
  const { transactionId: routeTransactionId } = useParams<RouteParams>();
  const transactionId = transactionIdProp ?? routeTransactionId;
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
    date: ''
  });
  const [newItemForm, setNewItemForm] = useState<NewItemForm>(EMPTY_ITEM_FORM);
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
    () => accounts.map((account) => ({ id: account.id, label: account.name, hint: account.currency ?? undefined })),
    [accounts]
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
      date: transaction.date ?? ''
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

      const currentItemsPayload = transaction.items.map((item) => ({
        product_id: item.product_id,
        unit_price_cents: item.unit_price_cents,
        quantity: item.quantity,
        unit: item.unit ?? undefined,
        comment: item.comment ?? undefined
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

  const detailsContent = (
    <>
      {error && (
        <div className="alert alert-danger d-flex align-items-center gap-2" role="alert">
          <IconAlertCircle size={18} aria-hidden="true" />
          <div>{error}</div>
        </div>
      )}

      {loading && (
        <div className="card mb-3">
          <div className="card-body">
            <div className="placeholder-glow">
              <div className="placeholder col-6 mb-2" />
              <div className="placeholder col-8 mb-2" />
              <div className="placeholder col-12 mb-2" />
              <div className="placeholder col-9 mb-0" />
            </div>
          </div>
        </div>
      )}

      {!loading && transaction && (
        <>
          <div className="row g-3">
            <div className="col-lg-5">
              <div className="card h-100">
                <form className="card-body d-flex flex-column gap-3" onSubmit={handleSaveFields}>
                  {formError && (
                    <div className="alert alert-danger d-flex align-items-center gap-2 mb-1" role="alert">
                      <IconAlertCircle size={16} aria-hidden="true" />
                      <div>{formError}</div>
                    </div>
                  )}
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
                    <label className="form-label">Sklep / grupa</label>
                    <EntityAutocomplete
                      value={editFields.store_id}
                      options={storeOptions}
                      onSelect={(id) => handleFieldChange('store_id', id)}
                      emptyOptionLabel="Brak"
                      placeholder="Wpisz, aby wyszukać sklep"
                    />
                  </div>
                  <div>
                    <label className="form-label">Tag</label>
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
                    <label className="form-label">Data księgowania</label>
                    <input
                      type="date"
                      className="form-control"
                      value={editFields.date}
                      onChange={(e) => handleFieldChange('date', e.target.value)}
                      required
                    />
                  </div>
                  <div>
                    <label className="form-label">Komentarz</label>
                    <textarea
                      className="form-control"
                      rows={2}
                      value={editFields.comment}
                      onChange={(e) => handleFieldChange('comment', e.target.value)}
                    />
                  </div>
                  <div className="d-flex align-items-center justify-content-between flex-wrap gap-2">
                    <div className="tx-detail__status mb-0">
                      <div className={`tx-badge tx-badge--${statusPresentation.tone}`}>
                        {statusState === 'reconciled' ? (
                          <IconCheck size={14} aria-hidden="true" />
                        ) : (
                          <IconFlag size={14} aria-hidden="true" />
                        )}
                        <span>{statusPresentation.label}</span>
                      </div>
                      <div className="text-secondary small">{statusPresentation.description}</div>
                      <div className="text-secondary small mt-1">
                        Zarejestrowano: {formatDateTime(transaction.recorded_at)}
                      </div>
                    </div>
                    <Button
                      type="submit"
                      variant="outline-primary"
                      size="sm"
                      disabled={savingForm}
                      icon={<IconCheck size={14} aria-hidden="true" />}
                    >
                      {savingForm ? 'Zapisywanie…' : 'Zapisz zmiany'}
                    </Button>
                  </div>
                </form>
              </div>
            </div>

            <div className="col-lg-7">
              <div className="card mb-3">
                <div className="card-header">
                  <div>
                    <div className="card-title mb-0">Pozycje na paragonie</div>
                    <div className="text-secondary small">
                      Szczegóły pozycji z tej transakcji (brutto).
                    </div>
                  </div>
                </div>
                <div className="card-body">
                  {transaction.items.length ? (
                    <div className="table-responsive">
                      <table className="table table-sm mb-0">
                        <thead>
                          <tr>
                            <th>Produkt</th>
                            <th>Ilość</th>
                            <th>Cena jedn.</th>
                            <th className="text-end">Suma</th>
                          </tr>
                        </thead>
                        <tbody>
                          {transaction.items.map((item) => {
                            const totalCents = item.unit_price_cents * item.quantity;
                            return (
                              <tr key={item.id}>
                                <td>
                                  <div className="fw-semibold">{item.product_name ?? 'Produkt'}</div>
                                  {item.comment && <div className="text-secondary small">{item.comment}</div>}
                                </td>
                                <td>
                                  {item.quantity} {item.unit ?? ''}
                                </td>
                                <td>{formatMoney(item.unit_price_cents, transaction.currency)}</td>
                                <td className="text-end">
                                  {formatMoney(Math.round(totalCents), transaction.currency)}
                                </td>
                              </tr>
                            );
                          })}
                        </tbody>
                      </table>
                    </div>
                  ) : (
                    <div className="empty m-0">
                      <div className="empty-icon">
                        <IconCircleDot size={28} aria-hidden="true" />
                      </div>
                      <p className="empty-title">Brak pozycji</p>
                      <p className="empty-subtitle text-secondary">Ta transakcja nie ma pozycji.</p>
                    </div>
                  )}
                  <hr className="my-3" />
                  <form className="row g-2 align-items-end" onSubmit={handleAddItem}>
                    {itemError && (
                      <div className="col-12">
                        <div className="alert alert-danger d-flex align-items-center gap-2 mb-2" role="alert">
                          <IconAlertCircle size={16} aria-hidden="true" />
                          <div>{itemError}</div>
                        </div>
                      </div>
                    )}
                    <div className="col-12 col-md-6 col-lg-5">
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
                    <div className="col-3 col-md-2">
                      <label className="form-label">Ilość</label>
                      <input
                        type="number"
                        step="0.01"
                        min="0"
                        className="form-control"
                        value={newItemForm.quantity}
                        onChange={(e) => handleNewItemChange('quantity', e.target.value)}
                        required
                      />
                    </div>
                    <div className="col-4 col-md-3">
                      <label className="form-label">Cena jedn.</label>
                      <input
                        type="number"
                        step="0.01"
                        min="0"
                        className="form-control"
                        value={newItemForm.unit_price}
                        onChange={(e) => handleNewItemChange('unit_price', e.target.value)}
                        required
                      />
                    </div>
                    <div className="col-12 col-lg-6">
                      <label className="form-label">Tag produktu</label>
                      <TagPathInput
                        value={newItemForm.tag_path}
                        categories={categories}
                        forAccounts={false}
                        placeholder="np. spożywcze/nabiał"
                        onChange={(value) => handleNewItemChange('tag_path', value)}
                      />
                    </div>
                    <div className="col-12 col-lg-6">
                      <label className="form-label">Komentarz</label>
                      <input
                        type="text"
                        className="form-control"
                        value={newItemForm.comment}
                        onChange={(e) => handleNewItemChange('comment', e.target.value)}
                        placeholder="opcjonalnie"
                      />
                    </div>
                    <div className="col-12 d-flex justify-content-end">
                      <Button
                        type="submit"
                        variant="outline-primary"
                        size="sm"
                        disabled={savingItem}
                        icon={<IconPlus size={14} aria-hidden="true" />}
                      >
                        {savingItem ? 'Dodawanie…' : 'Dodaj pozycję'}
                      </Button>
                    </div>
                  </form>
                </div>
              </div>

              <div className="card">
                <div className="card-header align-items-start">
                  <div>
                    <div className="card-title mb-0">Załączniki</div>
                    <div className="text-secondary small">
                      Dodaj zdjęcia paragonu lub potwierdzenia, aby mieć je pod ręką.
                    </div>
                  </div>
                  <div className="btn-list ms-auto">
                    <Button
                      variant="outline-secondary"
                      icon={<IconPhotoPlus size={16} aria-hidden="true" />}
                      onClick={openFilePicker}
                      disabled={uploading}
                    >
                      {uploading ? 'Ładowanie…' : 'Dodaj zdjęcie'}
                    </Button>
                    <input
                      type="file"
                      accept="image/*"
                      ref={fileInputRef}
                      className="d-none"
                      onChange={(event) => void handleFileChange(event)}
                    />
                  </div>
                </div>
                {attachmentError && (
                  <div className="card-body pt-0">
                    <div className="alert alert-danger mb-0 d-flex align-items-center gap-2" role="alert">
                      <IconAlertCircle size={16} aria-hidden="true" />
                      <div>{attachmentError}</div>
                    </div>
                  </div>
                )}
                <div className="card-body">
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
                    <div className="empty m-0">
                      <div className="empty-icon">
                        <IconPhoto size={24} aria-hidden="true" />
                      </div>
                      <p className="empty-title">Brak załączników</p>
                      <p className="empty-subtitle text-secondary">
                        Dodaj zdjęcie paragonu, aby mieć je pod ręką w jednym miejscu.
                      </p>
                    </div>
                  )}
                </div>
              </div>
            </div>
          </div>
        </>
      )}
    </>
  );

  if (embedded) {
    return <div className="page-body p-0">{detailsContent}</div>;
  }

  return (
    <div className="page compact-page">
      <div className="page-shell">
        <div className="page-header d-print-none">
          <div className="row align-items-center">
            <div className="col">
              <div className="page-pretitle text-uppercase text-muted fw-semibold">Szczegóły transakcji</div>
            </div>
            <div className="col-auto ms-auto mt-3 mt-md-0">
              <div className="btn-list">
                <Button
                  variant="outline-secondary"
                  icon={<IconArrowLeft size={16} aria-hidden="true" />}
                  onClick={() => navigate('/transactions')}
                >
                  Wróć do listy
                </Button>
                <Button
                  variant="outline-primary"
                  icon={<IconRefresh size={16} aria-hidden="true" />}
                  onClick={() => void loadTransaction()}
                  disabled={loading}
                >
                  Odśwież
                </Button>
              </div>
            </div>
          </div>
        </div>

        <div className="page-body">{detailsContent}</div>
      </div>
    </div>
  );
};

export default TransactionDetailsPage;
