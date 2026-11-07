// Strona kategorii: nagłówek z kwotami gałęzi i wykres miesięcy, a pod nimi zakładki
// Transakcje (ostatnie wpisy + najczęstsze sklepy) i Ustawienia.
import { useCallback, useEffect, useMemo, useState } from 'react';
import { Link, useNavigate, useParams, useSearchParams } from 'react-router-dom';
import { toast } from 'sonner';
import { ChevronLeft, Pencil, TagsIcon } from 'lucide-react';
import {
  deleteCategory,
  getAccounts,
  getAllCategories,
  getAllStores,
  getCategoryStats,
  getCategoryTopStores,
  getTransactionsPage,
  updateCategory
} from '../../api';
import { buildCategoryMap, resolveTagColor } from '../../components/tags/tagColors';
import EmptyState from '../../components/ui/EmptyState';
import Money, { formatSignedMoney } from '../../components/ui/Money';
import MonthBars from '../../components/ui/MonthBars';
import SkeletonRows from '../../components/ui/SkeletonRows';
import { copy } from '../../copy/pl';
import { useApi } from '../../hooks/useApi';
import { formatShortDate } from '../../utils/date';
import { monthRangeLabel } from '../../utils/months';
import AccountNameInput from '../accounts/components/AccountNameInput';
import { errorDetail } from '../stores/apiError';
import CategorySettings from './components/CategorySettings';
import CategoryTransactions from './components/CategoryTransactions';
import { buildCategoryTree, findNode, MONTHS_IN_WINDOW, moveTargets, subtreeIds, toStatsMap, windowMonths } from './model';

const labels = copy.categories;
const text = labels.details;
const RECENT_LIMIT = 20;

const CategoryDetailsPage = () => {
  const { categoryId = '' } = useParams();
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();
  const tab = searchParams.get('tab') === 'settings' ? 'settings' : 'transactions';
  const [renaming, setRenaming] = useState(false);

  // Bez filtra zakresu: strona otwiera też kategorie kont
  const categoriesRequest = useApi(() => getAllCategories(200), { errorMessage: labels.loadFailed });
  const statsRequest = useApi(getCategoryStats, { errorMessage: labels.statsFailed });
  const stores = useApi(getAllStores);
  const accounts = useApi(async () => (await getAccounts()).accounts);

  const categories = useMemo(() => categoriesRequest.data ?? [], [categoriesRequest.data]);
  const categoryMap = useMemo(() => buildCategoryMap(categories), [categories]);
  const stats = useMemo(() => (statsRequest.data ? toStatsMap(statsRequest.data) : null), [statsRequest.data]);
  const tree = useMemo(() => buildCategoryTree(categories, stats), [categories, stats]);
  const node = useMemo(() => findNode(tree, categoryId), [tree, categoryId]);
  const category = node?.category ?? null;
  const branchKey = node ? subtreeIds(node).sort().join(',') : '';

  // Wynik niesie klucz gałęzi, żeby po zmianie drzewa nie pokazać danych poprzedniej
  const details = useApi(
    async () => {
      const ids = branchKey.split(',');
      const [recent, topStores] = await Promise.all([
        getTransactionsPage({ category_ids: ids, limit: RECENT_LIMIT }),
        getCategoryTopStores(categoryId)
      ]);
      return { key: branchKey, recent: recent.items, total: recent.total, topStores };
    },
    { errorMessage: text.transactionsFailed, immediate: false }
  );
  const { refetch: refetchDetails } = details;
  useEffect(() => {
    if (branchKey) void refetchDetails();
  }, [branchKey, refetchDetails]);
  const loaded = details.data?.key === branchKey ? details.data : null;

  const months = useMemo(() => {
    const keys = windowMonths();
    return keys.map((month, index) => ({ month, cents: node?.total.monthly[index] ?? 0 }));
  }, [node]);

  const reloadTree = useCallback(async () => {
    await Promise.all([categoriesRequest.refetch(), statsRequest.refetch()]);
  }, [categoriesRequest.refetch, statsRequest.refetch]); // eslint-disable-line react-hooks/exhaustive-deps

  const setTab = (next: 'transactions' | 'settings') =>
    setSearchParams(next === 'settings' ? { tab: 'settings' } : {}, { replace: true });

  const save = async (payload: Parameters<typeof updateCategory>[1]) => {
    if (!category) return;
    try {
      await updateCategory(category.id, payload);
      await reloadTree();
    } catch (err) {
      console.error(err);
      toast.error(errorDetail(err) ?? labels.saveFailed);
    }
  };

  const remove = async () => {
    if (!category || !node) return;
    const descendants = subtreeIds(node).length - 1;
    if (!window.confirm(labels.deleteConfirm(category.name, descendants))) return;
    try {
      await deleteCategory(category.id);
      toast(labels.deleted(category.name));
      navigate('/categories');
    } catch (err) {
      console.error(err);
      toast.error(errorDetail(err) ?? labels.deleteFailed);
    }
  };

  const parent = category?.parent_id ? categoryMap.get(category.parent_id) : undefined;
  const color = category ? resolveTagColor(category, categoryMap).value : undefined;
  const spent = node?.total.spentCents ?? 0;
  const lastMonth = months[months.length - 1].month;

  return (
    <div className="page-shell account-details categories-details">
      <Link to="/categories" className="text-button account-details__back">
        <ChevronLeft size={14} aria-hidden="true" />
        {text.back}
      </Link>
      {!categoriesRequest.data && categoriesRequest.loading && <SkeletonRows rows={4} />}
      {categoriesRequest.error && <p className="accounts-error" role="alert">{categoriesRequest.error}</p>}
      {categoriesRequest.data && !category && <EmptyState icon={TagsIcon} message={text.notFound} />}
      {category && node && (
        <>
          <header className="account-details__head">
            <div className="account-details__identity">
              {parent && (
                <span className="categories-details__parent">
                  <span className="tag-dot" style={{ ['--tag-color' as string]: color }} aria-hidden="true" />
                  <Link to={`/categories/${parent.id}`} className="categories-details__parent-link">
                    {parent.path.split('/').join(' › ')}
                  </Link>
                </span>
              )}
              {renaming ? (
                <AccountNameInput
                  name={category.name}
                  className="account-details__name-input"
                  ariaLabel={labels.rename(category.name)}
                  tooShortMessage={labels.nameTooShort}
                  onCancel={() => setRenaming(false)}
                  onSave={(name) => {
                    setRenaming(false);
                    void save({ name });
                  }}
                />
              ) : (
                <h1 className="page-heading__title account-details__name">
                  <button
                    type="button"
                    className="account-details__name-button"
                    aria-label={labels.rename(category.name)}
                    onClick={() => setRenaming(true)}
                  >
                    {!parent && <span className="tag-dot" style={{ ['--tag-color' as string]: color }} aria-hidden="true" />}
                    {category.name}
                    <Pencil size={14} aria-hidden="true" />
                  </button>
                </h1>
              )}
              {stats && (
                <span className="accounts-name__meta">
                  {text.subtitle(node.total.count, node.total.lastDate ? formatShortDate(node.total.lastDate) : null)}
                </span>
              )}
            </div>
            <div className="categories-details__figures">
              <div className="categories-details__figure">
                <span className="stores-muted">{text.average}</span>
                <span className="categories-details__average">
                  {stats ? formatSignedMoney(Math.round(spent / MONTHS_IN_WINDOW)) : '…'}
                </span>
              </div>
              <div className="categories-details__figure">
                <span className="stores-muted">{text.spent}</span>
                {stats ? (
                  <Money cents={spent} tone="neutral" className="account-details__balance" />
                ) : (
                  <span className="account-details__balance stores-muted">…</span>
                )}
              </div>
            </div>
          </header>

          {statsRequest.error && <p className="accounts-error" role="alert">{statsRequest.error}</p>}
          {stats ? (
            <section className="account-details__chart-card" aria-label={text.chartTitle}>
              <div className="account-details__chart-head">
                <h2 className="home-block__title">{text.chartTitle}</h2>
                <span className="stores-muted stores-chart__range">{monthRangeLabel(months[0].month, lastMonth)}</span>
              </div>
              <MonthBars months={months} highlighted={lastMonth} />
            </section>
          ) : (
            !statsRequest.error && <SkeletonRows rows={4} />
          )}

          <div className="account-tabs" role="tablist" aria-label={text.tabs.label}>
            {(['transactions', 'settings'] as const).map((value) => (
              <button
                key={value}
                type="button"
                role="tab"
                id={`category-tab-${value}`}
                aria-selected={tab === value}
                aria-controls={`category-panel-${value}`}
                className={`account-tabs__tab${tab === value ? ' is-active' : ''}`}
                onClick={() => setTab(value)}
              >
                {text.tabs[value]}
                {value === 'transactions' && stats && <span className="stores-muted"> {node.total.count}</span>}
              </button>
            ))}
          </div>

          <div role="tabpanel" id={`category-panel-${tab}`} aria-labelledby={`category-tab-${tab}`}>
            {tab === 'transactions' ? (
              <>
                {details.error && <p className="accounts-error" role="alert">{details.error}</p>}
                {loaded ? (
                  <CategoryTransactions
                    category={category}
                    hasChildren={node.children.length > 0}
                    transactions={loaded.recent}
                    topStores={loaded.topStores}
                    stores={stores.data ?? []}
                    accounts={accounts.data ?? []}
                    categoryMap={categoryMap}
                  />
                ) : (
                  !details.error && <SkeletonRows rows={6} />
                )}
              </>
            ) : (
              <CategorySettings
                category={category}
                categoryMap={categoryMap}
                transactionCount={stats ? node.total.count : null}
                parentTargets={moveTargets(tree, category.id).filter(
                  (target) => (target.is_for_accounts ?? false) === (category.is_for_accounts ?? false)
                )}
                onSave={save}
                onDelete={() => void remove()}
              />
            )}
          </div>
        </>
      )}
    </div>
  );
};

export default CategoryDetailsPage;
