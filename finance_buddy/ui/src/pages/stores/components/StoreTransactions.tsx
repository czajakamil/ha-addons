// Zakładka Transakcje strony sklepu: ostatnie wpisy i przejście do pełnej listy z filtrem po sklepie.
import { Link, useNavigate } from 'react-router-dom';
import { ReceiptText } from 'lucide-react';
import type { Account, Category, Store, Transaction } from '../../../api';
import EmptyState from '../../../components/ui/EmptyState';
import Money from '../../../components/ui/Money';
import TruncatedText from '../../../components/ui/TruncatedText';
import { copy } from '../../../copy/pl';
import { formatShortDate } from '../../../utils/date';
import { maskAccountNumbers } from '../cleanup';
import StoreCategory from './StoreCategory';

const labels = copy.stores;
const RECENT_LIMIT = 20;

type StoreTransactionsProps = {
  transactions: Transaction[];
  members: Store[];
  accounts: Account[];
  categoryMap: Map<string, Category>;
};

const StoreTransactions = ({ transactions, members, accounts, categoryMap }: StoreTransactionsProps) => {
  const navigate = useNavigate();
  if (!transactions.length) return <EmptyState icon={ReceiptText} message={labels.noTransactions} />;
  const memberById = new Map(members.map((store) => [store.id, store]));
  const accountById = new Map(accounts.map((account) => [account.id, account]));
  const allHref = `/transactions?stores=${members.map((store) => encodeURIComponent(store.id)).join(',')}`;

  return (
    <div className="accounts-table-card stores-table-card">
      <table className="accounts-table stores-table stores-table--transactions">
        <thead>
          <tr>
            <th scope="col" className="stores-table__date">{labels.columns.date}</th>
            <th scope="col">{labels.columns.bankName}</th>
            <th scope="col" className="stores-table__account">{labels.columns.account}</th>
            <th scope="col" className="stores-table__category">{labels.columns.transactionCategory}</th>
            <th scope="col" className="stores-table__spent">{labels.columns.amount}</th>
          </tr>
        </thead>
        <tbody>
          {transactions.slice(0, RECENT_LIMIT).map((tx) => (
            <tr key={tx.id} className="accounts-table__row" onClick={() => navigate(`/transactions/${tx.id}`)}>
              <td className="stores-table__date">{tx.date ? formatShortDate(tx.date) : '—'}</td>
              <td className="stores-muted">
                <TruncatedText text={maskAccountNumbers((tx.store_id && memberById.get(tx.store_id)?.name) || '')} />
              </td>
              <td className="stores-table__account">
                <TruncatedText text={accountById.get(tx.account_id)?.name ?? ''} />
              </td>
              <td className="stores-table__category">
                <StoreCategory categoryId={tx.category_id} categoryMap={categoryMap} />
              </td>
              <td className="stores-table__spent">
                <Money cents={tx.total_cents} currency={tx.currency} />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      <div className="stores-table-foot">
        <Link to={allHref} className="text-button">
          {labels.allTransactions}
        </Link>
      </div>
    </div>
  );
};

export default StoreTransactions;
