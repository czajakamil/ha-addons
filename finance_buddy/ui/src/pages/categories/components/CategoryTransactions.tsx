// Zakładka Transakcje strony kategorii: ostatnie wpisy gałęzi i najczęstsze sklepy z 12 miesięcy.
import { Link, useNavigate } from 'react-router-dom';
import { ReceiptText } from 'lucide-react';
import type { Account, Category, CategoryTopStore, Store, Transaction } from '../../../api';
import EmptyState from '../../../components/ui/EmptyState';
import Money from '../../../components/ui/Money';
import TruncatedText from '../../../components/ui/TruncatedText';
import { copy } from '../../../copy/pl';
import { formatShortDate } from '../../../utils/date';
import { maskAccountNumbers } from '../../stores/cleanup';
import { formatCategoryPath } from '../../../utils/categoryPath';

const labels = copy.categories;
const text = labels.details;

type CategoryTransactionsProps = {
  category: Category;
  /** Z podkategoriami pokazujemy kolumnę z kategorią każdej transakcji. */
  hasChildren: boolean;
  transactions: Transaction[];
  topStores: CategoryTopStore[];
  stores: Store[];
  accounts: Account[];
  categoryMap: Map<string, Category>;
};

const CategoryTransactions = ({
  category,
  hasChildren,
  transactions,
  topStores,
  stores,
  accounts,
  categoryMap
}: CategoryTransactionsProps) => {
  const navigate = useNavigate();
  const storeById = new Map(stores.map((store) => [store.id, store]));
  const accountById = new Map(accounts.map((account) => [account.id, account]));
  const peak = Math.max(...topStores.map((store) => store.spent_cents_12m), 1);
  const storeName = (tx: Transaction) => {
    const store = tx.store_id ? storeById.get(tx.store_id) : undefined;
    if (store) return maskAccountNumbers(store.group_name || store.name);
    if (tx.transfer_account_id) return accountById.get(tx.transfer_account_id)?.name ?? '—';
    return '—';
  };
  // Podkategoria względem otwartej: „Na mieście › Restauracja” zamiast pełnej ścieżki
  const relativePath = (tx: Transaction) => {
    const path = tx.category_id ? categoryMap.get(tx.category_id)?.path : undefined;
    if (!path || tx.category_id === category.id) return category.name;
    return formatCategoryPath(path.slice(category.path.length + 1));
  };

  return (
    <div className="categories-details__transactions">
      {transactions.length ? (
        <div className="accounts-table-card categories-details__table">
          <div className="categories-table__scroll">
            <table className="accounts-table categories-tx-table">
              <thead>
                <tr>
                  <th scope="col" className="categories-tx-table__date">{text.columns.date}</th>
                  <th scope="col">{text.columns.store}</th>
                  {hasChildren && <th scope="col">{text.columns.category}</th>}
                  <th scope="col">{text.columns.account}</th>
                  <th scope="col" className="categories-table__num">{text.columns.amount}</th>
                </tr>
              </thead>
              <tbody>
                {transactions.map((tx) => (
                  <tr key={tx.id} className="accounts-table__row" onClick={() => navigate(`/transactions/${tx.id}`)}>
                    <td className="categories-tx-table__date stores-muted">{tx.date ? formatShortDate(tx.date) : '—'}</td>
                    <td className="categories-tx-table__store">
                      <TruncatedText text={storeName(tx)} />
                    </td>
                    {hasChildren && (
                      <td className="stores-muted">
                        <TruncatedText text={relativePath(tx)} />
                      </td>
                    )}
                    <td className="stores-muted">
                      <TruncatedText text={accountById.get(tx.account_id)?.name ?? ''} />
                    </td>
                    <td className="categories-table__num">
                      <Money cents={tx.total_cents} currency={tx.currency} />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="stores-table-foot">
            <Link to={`/transactions?category=${encodeURIComponent(category.id)}`} className="text-button">
              {text.allTransactions}
            </Link>
          </div>
        </div>
      ) : (
        <EmptyState icon={ReceiptText} message={text.noTransactions} />
      )}

      <aside className="categories-stores" aria-label={text.topStores}>
        <h3 className="categories-stores__title">{text.topStores}</h3>
        {topStores.length ? (
          topStores.map((store) => (
            <Link key={store.store_id} to={`/payees/${store.store_id}`} className="categories-stores__row">
              <span className="categories-stores__line">
                <TruncatedText text={maskAccountNumbers(store.name)} />
                <Money cents={store.spent_cents_12m} tone="neutral" />
              </span>
              <span className="categories-stores__bar" aria-hidden="true">
                <span style={{ width: `${Math.round((store.spent_cents_12m / peak) * 100)}%` }} />
              </span>
              <span className="categories-stores__meta">{labels.transactionsCount(store.count)}</span>
            </Link>
          ))
        ) : (
          <p className="stores-muted categories-stores__empty">{text.noStores}</p>
        )}
      </aside>
    </div>
  );
};

export default CategoryTransactions;
