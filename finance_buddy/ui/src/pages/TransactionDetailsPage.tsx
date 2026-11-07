// Szczegóły transakcji. Dwa układy: z pozycjami (podział na kategorie, tabela, zdjęcia)
// i bez pozycji (pasek „dodaj”, historia ze sklepem). Pola po prawej zapisują się same.
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Link, useNavigate, useParams } from 'react-router-dom';
import { ArrowUpRightIcon, CheckIcon, ChevronLeftIcon, CircleAlertIcon, EllipsisIcon, PlusIcon } from 'lucide-react';
import Button from '../components/Button';
import CategoryPickerMenu from '../components/CategoryPickerMenu';
import PopoverFilter from '../components/PopoverFilter';
import TagPathInput from '../components/tags/TagPathInput';
import EntityAutocomplete, { type EntityOption } from '../components/transactions/EntityAutocomplete';
import { selectableAccounts } from '../components/transactions/lookupOptions';
import {
  Account,
  Category,
  Item,
  Store,
  TransactionDetail,
  type TransactionItemCreate,
  type TransactionUpdate,
  createItem,
  deleteTransaction,
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
import { DateInput, TimeInput } from '../components/ui/DateTimeInput';
import { formatDayHeading, formatRecordedTime, formatShortDate, toRecordedAt } from '../utils/date';
import { formatMoney, parseAmountToCents } from '../utils/format';
import { buildFilterTree, categoryFilterOptions } from './transactions/categoryFilter';
import { CategoryDot, CategoryLabel, PhotosSection, StoreHistory } from './transactionDetails/components';
import {
  UNCATEGORIZED_COLOR,
  UNCATEGORIZED_KEY,
  buildCategorySplit,
  itemCategory,
  itemTotalCents,
  itemsMatchTotal
} from './transactionDetails/model';

type RouteParams = {
  transactionId?: string;
};

type EditableFields = {
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

type SaveState = 'idle' | 'saving' | 'saved' | 'error';

const EMPTY_ITEM_FORM: NewItemForm = {
  product_id: '',
  product_name: '',
  tag_path: '',
  quantity: '1',
  unit_price: '',
  comment: ''
};

const fieldsFrom = (transaction: TransactionDetail): EditableFields => ({
  account_id: transaction.account_id ?? '',
  store_id: transaction.store_id ?? '',
  comment: transaction.comment ?? '',
  date: transaction.date ?? '',
  time: formatRecordedTime(transaction.recorded_at) ?? ''
});

/** „Uzgodniona 2 lis” — status z banku bez osobnej plakietki. */
const statusLabel = (transaction: TransactionDetail) => {
  if (transaction.reconciled) return 'Uzgodniona';
  if (transaction.cleared) return 'Zatwierdzona';
  return 'Przetwarzana';
};

const formatQuantity = (quantity: number, unit?: string | null) =>
  `${String(quantity).replace('.', ',')} ${unit || 'szt'}`;

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
  const [fields, setFields] = useState<EditableFields>({ account_id: '', store_id: '', comment: '', date: '', time: '' });
  const [saveState, setSaveState] = useState<SaveState>('idle');
  const [newItemForm, setNewItemForm] = useState<NewItemForm>(EMPTY_ITEM_FORM);
  const [showItemForm, setShowItemForm] = useState(false);
  const [savingItem, setSavingItem] = useState(false);
  const [itemError, setItemError] = useState<string | null>(null);
  const [itemsBusy, setItemsBusy] = useState(false);
  const [menuOpen, setMenuOpen] = useState(false);

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
      setFields(fieldsFrom(data));
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
  }, [loadTransaction]);

  useEffect(() => {
    void loadReferences();
  }, [loadReferences]);

  const categoriesById = useMemo(() => new Map(categories.map((category) => [category.id, category])), [categories]);
  const productsById = useMemo(() => new Map(products.map((product) => [product.id, product])), [products]);
  const categoryTree = useMemo(() => buildFilterTree(categories), [categories]);
  const pickerOptions = useCallback(
    (search: string) => categoryFilterOptions(categoryTree, search, [], null),
    [categoryTree]
  );

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
  const lastPriceByProduct = useMemo(() => {
    const map = new Map<string, string>();
    transaction?.items.forEach((item) => map.set(item.product_id, (item.unit_price_cents / 100).toFixed(2)));
    return map;
  }, [transaction]);

  const sortedAttachments = useMemo(
    () =>
      transaction
        ? [...transaction.attachments].sort((a, b) => new Date(a.created_at).getTime() - new Date(b.created_at).getTime())
        : [],
    [transaction]
  );

  const split = useMemo(
    () => (transaction ? buildCategorySplit(transaction.items, productsById, categoriesById, transaction.category_id) : []),
    [transaction, productsById, categoriesById]
  );
  const splitColor = useMemo(() => new Map(split.map((entry) => [entry.key, entry.color])), [split]);

  // --- Zapis pól (autozapis) ----------------------------------------------

  const save = async (payload: TransactionUpdate) => {
    if (!transactionId || !Object.keys(payload).length) return;
    setSaveState('saving');
    try {
      const updated = await updateTransaction(transactionId, payload);
      // PATCH zwraca transakcję bez nazw i załączników, więc dokładamy ją do szczegółów
      setTransaction((prev) => (prev ? { ...prev, ...updated, items: prev.items, attachments: prev.attachments } : prev));
      setSaveState('saved');
    } catch (err) {
      console.error(err);
      setSaveState('error');
      // Pola wracają do stanu z serwera, żeby nie udawać zapisanej zmiany
      if (transaction) setFields(fieldsFrom(transaction));
    }
  };

  const commitDateTime = () => {
    if (!transaction || !fields.date) return;
    const payload: TransactionUpdate = {};
    if (fields.date !== (transaction.date ?? '')) payload.date = fields.date;
    // Sama zmiana daty zachowuje godzinę po stronie backendu
    if (fields.time !== (formatRecordedTime(transaction.recorded_at) ?? '')) {
      payload.recorded_at = toRecordedAt(fields.date, fields.time);
    }
    void save(payload);
  };

  const commitComment = () => {
    if (!transaction || fields.comment === (transaction.comment ?? '')) return;
    void save({ comment: fields.comment || null });
  };

  const selectAccount = (id: string) => {
    setFields((prev) => ({ ...prev, account_id: id }));
    if (transaction && id && id !== transaction.account_id) void save({ account_id: id });
  };

  const selectStore = (id: string) => {
    setFields((prev) => ({ ...prev, store_id: id }));
    if (transaction && id !== (transaction.store_id ?? '')) void save({ store_id: id || null });
  };

  const selectCategory = (id: string) => {
    if (transaction && id !== (transaction.category_id ?? '')) void save({ category_id: id || null });
  };

  // --- Kategorie pozycji ----------------------------------------------------

  // Kategoria należy do produktu, więc zmiana obowiązuje też przy kolejnych zakupach
  const setProductsCategory = async (productIds: string[], categoryId: string) => {
    const changed = [...new Set(productIds)].filter((id) => (productsById.get(id)?.category_id ?? null) !== categoryId);
    if (!changed.length) return;
    setItemsBusy(true);
    setItemError(null);
    try {
      const updated = await Promise.all(changed.map((id) => updateItem(id, { category_id: categoryId })));
      const byId = new Map(updated.map((product) => [product.id, product]));
      setProducts((prev) => prev.map((product) => byId.get(product.id) ?? product));
    } catch (err) {
      console.error(err);
      setItemError('Nie udało się zmienić kategorii.');
      void loadReferences();
    } finally {
      setItemsBusy(false);
    }
  };

  // --- Załączniki -----------------------------------------------------------

  const handleFileChange = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    event.target.value = '';
    if (!file || !transactionId) return;
    setAttachmentError(null);
    setUploading(true);
    try {
      const attachment = await uploadTransactionAttachment(transactionId, file);
      setTransaction((prev) => (prev ? { ...prev, attachments: [...prev.attachments, attachment] } : prev));
    } catch (err) {
      console.error(err);
      setAttachmentError('Nie udało się dodać zdjęcia.');
    } finally {
      setUploading(false);
    }
  };

  const handleDeleteAttachment = async (attachmentId: string) => {
    if (!transactionId || !window.confirm('Usunąć to zdjęcie?')) return;
    setAttachmentError(null);
    setRemovingAttachmentId(attachmentId);
    try {
      await deleteTransactionAttachment(transactionId, attachmentId);
      setTransaction((prev) =>
        prev ? { ...prev, attachments: prev.attachments.filter((a) => a.id !== attachmentId) } : prev
      );
    } catch (err) {
      console.error(err);
      setAttachmentError('Nie udało się usunąć zdjęcia.');
    } finally {
      setRemovingAttachmentId(null);
    }
  };

  const handleDeleteTransaction = async () => {
    setMenuOpen(false);
    if (!transactionId || !window.confirm('Usunąć tę transakcję? Tego nie da się cofnąć.')) return;
    try {
      await deleteTransaction(transactionId);
      navigate('/transactions');
    } catch (err) {
      console.error(err);
      setError('Nie udało się usunąć transakcji.');
    }
  };

  // --- Nowa pozycja ---------------------------------------------------------

  const handleNewItemChange = (field: 'quantity' | 'unit_price' | 'comment' | 'tag_path', value: string) => {
    setNewItemForm((prev) => ({ ...prev, [field]: value }));
    setItemError(null);
  };

  // Wybór istniejącego produktu: uzupełnia nazwę, ostatnią cenę i kategorię produktu
  const handleSelectProduct = (productId: string) => {
    const product = productsById.get(productId);
    const tagPath = product?.category_id ? categoriesById.get(product.category_id)?.path ?? '' : '';
    setNewItemForm((prev) => ({
      ...prev,
      product_id: productId,
      product_name: product?.name ?? '',
      tag_path: tagPath,
      unit_price: lastPriceByProduct.get(productId) ?? prev.unit_price
    }));
    setItemError(null);
  };

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
      const tagPath = newItemForm.tag_path.trim();
      const productCategoryId = tagPath ? await ensureCategoryPath(tagPath, categories, false) : null;

      // Nowy produkt tworzymy w locie; przy istniejącym aktualizujemy kategorię, gdy się zmieniła
      let productId = newItemForm.product_id;
      if (!productId) {
        const created = await createItem({ name: productName, category_id: productCategoryId });
        productId = created.id;
      } else if (tagPath) {
        const existing = productsById.get(productId);
        if (existing && (existing.category_id ?? null) !== productCategoryId) {
          await updateItem(productId, { category_id: productCategoryId });
        }
      }

      const items: TransactionItemCreate[] = transaction.items.map((item) => ({
        product_id: item.product_id,
        unit_price_cents: item.unit_price_cents,
        quantity: item.quantity,
        unit: item.unit ?? undefined,
        comment: item.comment ?? undefined,
        raw_name: item.raw_name ?? undefined
      }));
      items.push({
        product_id: productId,
        unit_price_cents: unitPriceCents,
        quantity,
        comment: newItemForm.comment.trim() || undefined
      });

      await updateTransaction(transactionId, { items });
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

  // --- Widok ----------------------------------------------------------------

  const store = stores.find((entry) => entry.id === transaction?.store_id);
  const storeLabel = store?.group_name?.trim() || store?.name || '';
  const accountName = accounts.find((entry) => entry.id === transaction?.account_id)?.name;
  const heading = storeLabel || transaction?.comment || transaction?.bank_description || (transaction ? 'Transakcja' : '');
  const time = formatRecordedTime(transaction?.recorded_at);
  const hasItems = Boolean(transaction?.items.length);
  const hasPhotos = sortedAttachments.length > 0;
  const showItemsSection = hasItems || showItemForm;
  const itemsSumCents = transaction ? transaction.items.reduce((sum, item) => sum + itemTotalCents(item), 0) : 0;
  const itemsMatch = transaction ? itemsMatchTotal(transaction.items, transaction.total_cents) : true;
  const transactionCategoryPath = transaction?.category_id ? categoriesById.get(transaction.category_id)?.path ?? '' : '';

  const openItemForm = () => {
    setShowItemForm(true);
    setItemError(null);
  };

  const itemForm = showItemForm && (
    <form className="tx-item-form" onSubmit={handleAddItem}>
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
          step="0.001"
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
          type="text"
          inputMode="decimal"
          className="form-control"
          value={newItemForm.unit_price}
          onChange={(e) => handleNewItemChange('unit_price', e.target.value)}
          required
        />
      </div>
      <div>
        <label className="form-label">Kategoria produktu</label>
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
  );

  return (
    <div className="page-shell page-stack-layout tx-detail">
      <Link to="/transactions" className="tx-detail__back">
        <ChevronLeftIcon size={14} aria-hidden="true" />
        Transakcje
      </Link>

      {error && (
        <p className="transactions-error" role="alert">
          {error}
        </p>
      )}

      {loading && !transaction && <SkeletonRows rows={6} />}

      {transaction && (
        <>
          <header className="tx-detail__header">
            <div className="tx-detail__identity">
              <span className="tx-detail__avatar" aria-hidden="true">
                {(heading.trim()[0] ?? '?').toUpperCase()}
              </span>
              <div className="tx-detail__title">
                {transaction.store_id ? (
                  <Link to={`/transactions?stores=${encodeURIComponent(transaction.store_id)}`} className="tx-detail__name">
                    <h1>{heading}</h1>
                    <ArrowUpRightIcon size={14} aria-hidden="true" />
                  </Link>
                ) : (
                  <h1 className="tx-detail__name">{heading}</h1>
                )}
                <p className="tx-detail__muted">
                  {transaction.date ? formatDayHeading(transaction.date) : '—'}
                  {time ? `, ${time}` : ''}
                  {accountName ? ` · ${accountName}` : ''}
                </p>
              </div>
            </div>
            <div className="tx-detail__amount">
              <div className="tx-detail__total">
                <Money cents={transaction.total_cents} currency={transaction.currency} />
                <span className="tx-detail__muted tx-detail__status">
                  {statusLabel(transaction)}
                  {transaction.date ? ` ${formatShortDate(transaction.date)}` : ''}
                </span>
              </div>
              <PopoverFilter
                id="tx-detail-actions"
                label="Akcje transakcji"
                className="filter-menu-anchor tx-detail__menu"
                panelClassName="filter-menu tx-detail__menu-panel"
                isOpen={menuOpen}
                onOpenChange={setMenuOpen}
                trigger={({ ref, panelId, isOpen, onToggle }) => (
                  <button
                    ref={ref}
                    type="button"
                    className="icon-button"
                    aria-label="Więcej akcji"
                    aria-haspopup="dialog"
                    aria-expanded={isOpen}
                    aria-controls={panelId}
                    onClick={onToggle}
                  >
                    <EllipsisIcon size={16} aria-hidden="true" />
                  </button>
                )}
              >
                <button type="button" className="filter-menu__item filter-menu__item--pick tx-detail__danger" onClick={() => void handleDeleteTransaction()}>
                  Usuń transakcję
                </button>
              </PopoverFilter>
            </div>
          </header>

          <div className="tx-detail__layout">
            <div className="tx-detail__main">
              {hasItems && (
                <section className="tx-card tx-split" aria-label="Podział na kategorie">
                  <div className="tx-card__row">
                    <h2 className="tx-card__title">
                      Podział na kategorie{' '}
                      <span className="tx-detail__muted tx-card__count">· z {transaction.items.length} pozycji</span>
                    </h2>
                    <CategoryPickerMenu
                      id="tx-detail-all-items-category"
                      label="Jedna kategoria dla wszystkich pozycji"
                      triggerClassName="tx-detail__link-button"
                      triggerLabel="Jedna kategoria dla wszystkich…"
                      searchPlaceholder="Szukaj kategorii…"
                      emptyText="Brak pasujących kategorii"
                      disabled={itemsBusy}
                      options={pickerOptions}
                      onSelect={(categoryId) =>
                        void setProductsCategory(transaction.items.map((item) => item.product_id), categoryId)
                      }
                    />
                  </div>
                  <div className="tx-split__bar" aria-hidden="true">
                    {split.map((entry) => (
                      <span key={entry.key} style={{ flexGrow: Math.max(entry.share, 0.005), background: entry.color }} />
                    ))}
                  </div>
                  <ul className="tx-split__legend">
                    {split.map((entry) => (
                      <li key={entry.key}>
                        <CategoryDot color={entry.color} />
                        <CategoryLabel path={entry.path} />
                        <Money cents={entry.cents} currency={transaction.currency} tone="neutral" className="tx-split__amount" />
                        <span className="tx-detail__muted tx-split__share">{Math.round(entry.share * 100)}%</span>
                      </li>
                    ))}
                  </ul>
                </section>
              )}

              {showItemsSection && (
                <section className="tx-card tx-items-card" aria-label="Pozycje">
                  <header className="tx-card__header">
                    <h2 className="tx-card__title">
                      Pozycje <span className="tx-detail__muted tx-card__count">{transaction.items.length}</span>
                    </h2>
                    {!showItemForm && (
                      <button type="button" className="text-button" onClick={openItemForm}>
                        <PlusIcon size={14} aria-hidden="true" />
                        Dodaj pozycję
                      </button>
                    )}
                  </header>
                  {itemError && (
                    <p className="transactions-error tx-card__error" role="alert">
                      {itemError}
                    </p>
                  )}
                  {hasItems && (
                    <div className="tx-items-grid" role="table" aria-label="Pozycje transakcji">
                      <div className="tx-items-grid__row tx-items-grid__head" role="row">
                        <span role="columnheader">Produkt</span>
                        <span role="columnheader">Kategoria</span>
                        <span role="columnheader" className="tx-items-grid__num">Ilość</span>
                        <span role="columnheader" className="tx-items-grid__num">Cena</span>
                        <span role="columnheader" className="tx-items-grid__num">Suma</span>
                      </div>
                      {transaction.items.map((item) => {
                        const { categoryId, inherited } = itemCategory(
                          item,
                          productsById,
                          categoriesById,
                          transaction.category_id
                        );
                        const path = categoryId ? categoriesById.get(categoryId)?.path ?? '' : '';
                        const color = splitColor.get(categoryId ?? UNCATEGORIZED_KEY) ?? UNCATEGORIZED_COLOR;
                        const name = item.product_name ?? 'Produkt';
                        return (
                          <div key={item.id} className="tx-items-grid__row" role="row">
                            <span role="cell" className="tx-items-grid__product">
                              <span className="tx-items-grid__name">{name}</span>
                              {item.raw_name && item.raw_name !== item.product_name && (
                                <span className="tx-detail__muted tx-items-grid__raw" title="Nazwa z paragonu">
                                  {item.raw_name}
                                </span>
                              )}
                              {item.comment && <span className="tx-detail__muted tx-items-grid__raw">{item.comment}</span>}
                            </span>
                            <span role="cell" className="tx-items-grid__category">
                              <CategoryPickerMenu
                                id={`tx-item-category-${item.id}`}
                                label={
                                  inherited
                                    ? `Kategoria produktu ${name}: brak, liczy się do kategorii transakcji. Wybierz własną`
                                    : `Kategoria produktu ${name}. Zmiana obowiązuje też przy kolejnych zakupach`
                                }
                                triggerClassName={`tx-detail__inline-select${inherited ? ' tx-detail__inline-select--inherited' : ''}`}
                                triggerLabel={
                                  <>
                                    <CategoryDot color={color} />
                                    <CategoryLabel path={path} leafOnly />
                                  </>
                                }
                                searchPlaceholder="Szukaj kategorii…"
                                emptyText="Brak pasujących kategorii"
                                disabled={itemsBusy}
                                options={pickerOptions}
                                onSelect={(id) => void setProductsCategory([item.product_id], id)}
                              />
                            </span>
                            <span role="cell" className="tx-items-grid__num tx-detail__secondary">
                              {formatQuantity(item.quantity, item.unit)}
                            </span>
                            <span role="cell" className="tx-items-grid__num tx-detail__secondary">
                              {formatMoney(item.unit_price_cents, transaction.currency)}
                            </span>
                            <span role="cell" className="tx-items-grid__num tx-items-grid__total">
                              {formatMoney(itemTotalCents(item), transaction.currency)}
                            </span>
                          </div>
                        );
                      })}
                    </div>
                  )}
                  {itemForm}
                  {hasItems && (
                    <footer className="tx-items-card__footer">
                      {itemsMatch ? (
                        <span className="tx-items-card__check">
                          <CheckIcon size={14} aria-hidden="true" />
                          Suma pozycji zgadza się z kwotą transakcji
                        </span>
                      ) : (
                        <span className="tx-items-card__check tx-items-card__check--warn">
                          <CircleAlertIcon size={14} aria-hidden="true" />
                          Różnica{' '}
                          {formatMoney(Math.abs(transaction.total_cents) - Math.abs(itemsSumCents), transaction.currency)}{' '}
                          względem kwoty transakcji
                        </span>
                      )}
                      <span>
                        <span className="tx-detail__muted tx-items-card__sum-label">Suma pozycji</span>
                        <strong>{formatMoney(itemsSumCents, transaction.currency)}</strong>
                      </span>
                    </footer>
                  )}
                </section>
              )}

              {!showItemsSection && (
                <div className="tx-detail__strip">
                  <span>
                    {hasPhotos
                      ? 'Pozycje nie są jeszcze dodane — wpisz je, żeby rozbić wydatek na kategorie.'
                      : 'Bez paragonu i pozycji'}
                  </span>
                  {!hasPhotos && (
                    <button type="button" className="text-button" onClick={() => fileInputRef.current?.click()} disabled={uploading}>
                      <PlusIcon size={14} aria-hidden="true" />
                      {uploading ? 'Ładowanie…' : 'Dodaj zdjęcie paragonu'}
                    </button>
                  )}
                  <button type="button" className="text-button" onClick={openItemForm}>
                    <PlusIcon size={14} aria-hidden="true" />
                    Dodaj pozycje
                  </button>
                </div>
              )}

              {hasPhotos ? (
                <PhotosSection
                  attachments={sortedAttachments}
                  uploading={uploading}
                  removingId={removingAttachmentId}
                  error={attachmentError}
                  onAdd={() => fileInputRef.current?.click()}
                  onDelete={(id) => void handleDeleteAttachment(id)}
                />
              ) : (
                <>
                  {showItemsSection && (
                    <div className="tx-detail__strip">
                      <span>{hasItems ? 'Bez zdjęć paragonu — pozycje dodane ręcznie' : 'Bez zdjęć paragonu'}</span>
                      <button type="button" className="text-button" onClick={() => fileInputRef.current?.click()} disabled={uploading}>
                        <PlusIcon size={14} aria-hidden="true" />
                        {uploading ? 'Ładowanie…' : 'Dodaj zdjęcia'}
                      </button>
                    </div>
                  )}
                  {attachmentError && (
                    <p className="transactions-error" role="alert">
                      {attachmentError}
                    </p>
                  )}
                </>
              )}
              <input
                type="file"
                accept="image/*,application/pdf"
                ref={fileInputRef}
                className="d-none"
                aria-label="Zdjęcie paragonu"
                onChange={(event) => void handleFileChange(event)}
              />

              {!hasItems && transaction.store_id && (
                <StoreHistory
                  transactionId={transaction.id}
                  storeId={transaction.store_id}
                  storeLabel={storeLabel || heading}
                  categoriesById={categoriesById}
                />
              )}
            </div>

            <aside className="tx-card tx-detail__aside" aria-label="Szczegóły transakcji">
              <h2 className="tx-card__title">Szczegóły</h2>
              <div className="tx-detail__field">
                <span className="form-label">Kwota</span>
                <span className="tx-detail__readonly">
                  <Money cents={transaction.total_cents} currency={transaction.currency} tone="neutral" />
                </span>
              </div>
              <div
                className="tx-detail__field tx-detail__datetime"
                onBlur={(event) => {
                  // Przejście między datą a godziną to jeszcze nie koniec edycji
                  if (!event.currentTarget.contains(event.relatedTarget as Node | null)) commitDateTime();
                }}
              >
                <label className="form-label" htmlFor="tx-date">Data i godzina</label>
                <div className="tx-detail__datetime-inputs">
                  <DateInput
                    id="tx-date"
                    className="form-control"
                    value={fields.date}
                    onChange={(value) => setFields((prev) => ({ ...prev, date: value }))}
                    required
                  />
                  <TimeInput
                    id="tx-time"
                    aria-label="Godzina (opcjonalnie)"
                    className="form-control"
                    value={fields.time}
                    onChange={(value) => setFields((prev) => ({ ...prev, time: value }))}
                  />
                </div>
              </div>
              <div className="tx-detail__field">
                <span className="form-label">Konto</span>
                <EntityAutocomplete
                  value={fields.account_id}
                  options={accountOptions}
                  onSelect={selectAccount}
                  placeholder="Wpisz, aby wyszukać konto"
                />
              </div>
              <div className="tx-detail__field">
                <span className="form-label">Sklep</span>
                <EntityAutocomplete
                  value={fields.store_id}
                  options={storeOptions}
                  onSelect={selectStore}
                  emptyOptionLabel="Brak"
                  placeholder="Wpisz, aby wyszukać sklep"
                />
              </div>
              <div className="tx-detail__field">
                <span className="form-label">Kategoria</span>
                <CategoryPickerMenu
                  id="tx-detail-category"
                  label="Kategoria transakcji"
                  triggerClassName="tx-detail__select"
                  triggerLabel={<CategoryLabel path={transactionCategoryPath} empty="Wybierz kategorię" />}
                  searchPlaceholder="Szukaj kategorii…"
                  emptyText="Brak pasujących kategorii"
                  options={pickerOptions}
                  onSelect={selectCategory}
                />
                {hasItems && (
                  <span className="tx-detail__hint">Do niej liczą się pozycje, których produkt nie ma własnej kategorii.</span>
                )}
              </div>
              <div className="tx-detail__field">
                <label className="form-label" htmlFor="tx-comment">Komentarz</label>
                <textarea
                  id="tx-comment"
                  className="form-control"
                  rows={3}
                  placeholder="Dodaj komentarz…"
                  value={fields.comment}
                  onChange={(e) => setFields((prev) => ({ ...prev, comment: e.target.value }))}
                  onBlur={commitComment}
                />
              </div>
              <p className={`tx-detail__save tx-detail__save--${saveState}`} role="status">
                {saveState === 'saving'
                  ? 'Zapisywanie…'
                  : saveState === 'saved'
                    ? 'Zapisano.'
                    : saveState === 'error'
                      ? 'Nie udało się zapisać zmian.'
                      : 'Zmiany zapisują się automatycznie.'}
              </p>
            </aside>
          </div>
        </>
      )}
    </div>
  );
};

export default TransactionDetailsPage;
