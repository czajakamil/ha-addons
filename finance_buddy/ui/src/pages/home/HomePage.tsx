import { useMemo, useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import * as DropdownMenu from '@radix-ui/react-dropdown-menu';
import {
  ChevronDown,
  ChevronRight,
  Coins,
  DatabaseBackup,
  Landmark,
  Plus,
  Receipt,
  FileUp,
  Tag as TagIcon,
  TriangleAlert,
  type LucideIcon
} from 'lucide-react';
import NewTransactionModal from '../../components/transactions/NewTransactionModal';
import { TagChip } from '../../components/tags/TagChip';
import { buildCategoryMap } from '../../components/tags/tagColors';
import EnvelopeProgress from '../../components/ui/EnvelopeProgress';
import Money from '../../components/ui/Money';
import PageHeader from '../../components/ui/PageHeader';
import TruncatedText from '../../components/ui/TruncatedText';
import { copy } from '../../copy/pl';
import {
  getAccounts,
  getAllCategories,
  getAllStores,
  getBackupStatus,
  getBudgetMonth,
  getHomeSummary,
  type Account,
  type Store
} from '../../api';
import { useApi } from '../../hooks/useApi';
import { refreshUntaggedCount } from '../../hooks/useUntaggedCount';
import { formatShortDate } from '../../utils/date';
import { BALANCE_ADJUSTMENT_LABEL, isBalanceAdjustment, userComment } from '../../utils/transactionLabels';
import { toMonthKey } from '../budget/model';
import { backupHealth } from '../panel/backupStatus';
import {
  buildTodoItems,
  describeDaysLeft,
  envelopeNote,
  envelopeUsage,
  formatChangePct,
  spendingChangePct,
  type TodoKind
} from './model';

const TODO_ICON: Record<TodoKind, LucideIcon> = {
  backup: DatabaseBackup,
  overspent: TriangleAlert,
  stale: Landmark,
  untagged: TagIcon,
  receipts: Receipt,
  assign: Coins
};

type Lookups = { accounts: Account[]; stores: Store[] };

/**
 * Ekran startowy: „co mam dziś zrobić", a nie „jak mi idzie w tym roku" —
 * wykresy są w Raportach. Każdy alert prowadzi tam, gdzie się go załatwia,
 * i znika, gdy spadnie do zera.
 */
const HomePage = () => {
  const navigate = useNavigate();
  const labels = copy.home;
  const summary = useApi(getHomeSummary, { errorMessage: labels.loadFailed });
  const budget = useApi(() => getBudgetMonth(toMonthKey(new Date())));
  const categories = useApi(() => getAllCategories());
  // Stan kopii to dodatek: błąd (np. brak uprawnień) nie może zasłonić reszty.
  const backup = useApi(() => getBackupStatus().catch(() => null));

  const [lookups, setLookups] = useState<Lookups | null>(null);
  const [showNewTransaction, setShowNewTransaction] = useState(false);

  const categoryById = useMemo(() => buildCategoryMap(categories.data ?? []), [categories.data]);

  const openNewTransaction = async () => {
    if (!lookups) {
      const [{ accounts }, stores] = await Promise.all([getAccounts(), getAllStores()]);
      setLookups({ accounts: accounts.filter((account) => !account.is_closed), stores });
    }
    setShowNewTransaction(true);
  };

  const data = summary.data;
  const todo = data ? buildTodoItems(data, backup.data ? backupHealth(backup.data) : null) : [];
  const change = data ? spendingChangePct(data.spent_month_cents, data.spent_previous_month_cents) : null;
  const canSpend = budget.data?.summary.available_cents ?? 0;

  return (
    <div className="page-shell home-page">
      <PageHeader
        title={labels.title}
        actions={
          <DropdownMenu.Root>
            <DropdownMenu.Trigger className="btn btn-primary home-add">
              <Plus size={16} aria-hidden="true" />
              {labels.add}
              <ChevronDown size={14} aria-hidden="true" />
            </DropdownMenu.Trigger>
            <DropdownMenu.Portal>
              <DropdownMenu.Content className="menu" align="end" sideOffset={4}>
                <DropdownMenu.Item className="menu__item" onSelect={() => void openNewTransaction()}>
                  <Plus size={16} aria-hidden="true" />
                  {labels.addExpense}
                </DropdownMenu.Item>
                <DropdownMenu.Item className="menu__item" onSelect={() => navigate('/receipts')}>
                  <Receipt size={16} aria-hidden="true" />
                  {labels.addReceipt}
                </DropdownMenu.Item>
                <DropdownMenu.Item className="menu__item" onSelect={() => navigate('/bank-import')}>
                  <FileUp size={16} aria-hidden="true" />
                  {labels.addBankCsv}
                </DropdownMenu.Item>
              </DropdownMenu.Content>
            </DropdownMenu.Portal>
          </DropdownMenu.Root>
        }
      />

      {summary.error && (
        <p className="home-error" role="alert">
          {summary.error}
        </p>
      )}

      {!data ? (
        summary.loading && (
          <div className="home-skeleton" aria-hidden="true">
            <span className="skeleton home-skeleton__hero" />
            <span className="skeleton home-skeleton__row" />
            <span className="skeleton home-skeleton__row" />
          </div>
        )
      ) : (
        <div className="home-grid">
          <section className="home-hero">
            <span className="home-hero__label">{labels.canSpend}</span>
            <Money
              cents={canSpend}
              tone={canSpend < 0 ? 'negative' : 'neutral'}
              className="home-hero__value"
            />
            <span className="home-hero__note">{describeDaysLeft(data.today)}</span>
          </section>

          <div className="home-figures">
            <Link to="/reports" className="home-figure">
              <span className="home-figure__label">{labels.spent}</span>
              <Money cents={data.spent_month_cents} tone="neutral" className="home-figure__value" />
              {change !== null && (
                <span className="home-figure__note">
                  {formatChangePct(change)} {labels.comparedToPrevious}
                </span>
              )}
            </Link>
            <Link to="/accounts" className="home-figure">
              <span className="home-figure__label">{labels.onBudgetBalance}</span>
              <Money cents={data.on_budget_balance_cents} tone="neutral" className="home-figure__value" />
            </Link>
          </div>

          <div className="home-main">
            {todo.length > 0 && (
              <section aria-labelledby="home-todo-title" className="home-block">
                <h2 id="home-todo-title" className="home-block__title">
                  {labels.todo}
                </h2>
                <ul className="home-list">
                  {todo.map((item) => {
                    const ItemIcon = TODO_ICON[item.kind];
                    return (
                      <li key={item.key}>
                        <Link to={item.to} className="home-alert">
                          <ItemIcon size={16} className={`home-alert__icon is-${item.tone}`} aria-hidden="true" />
                          <span className="home-alert__label">{item.label}</span>
                          {item.value && <span className="home-alert__value">{item.value}</span>}
                          <ChevronRight size={16} className="home-alert__chevron" aria-hidden="true" />
                        </Link>
                      </li>
                    );
                  })}
                </ul>
              </section>
            )}

            {data.recent.length > 0 && (
              <section aria-labelledby="home-recent-title" className="home-block">
                <div className="home-block__head">
                  <h2 id="home-recent-title" className="home-block__title">
                    {labels.recent}
                  </h2>
                  <Link to="/transactions" className="text-button">
                    {labels.allTransactions}
                  </Link>
                </div>
                <ul className="home-list">
                  {data.recent.map((tx) => {
                    const adjustment = isBalanceAdjustment(tx.store_name);
                    const payee = adjustment
                      ? BALANCE_ADJUSTMENT_LABEL
                      : tx.store_name ||
                        userComment(tx.comment) ||
                        (tx.is_transfer ? labels.transfer : labels.noDescription);
                    return (
                      <li key={tx.id} className="home-recent">
                        <span className="home-recent__date">{formatShortDate(tx.date)}</span>
                        <Link
                          to={`/transactions/${tx.id}`}
                          className={`home-recent__payee${adjustment ? ' is-muted' : ''}`}
                        >
                          <TruncatedText text={payee} />
                          <TruncatedText text={tx.account_name} className="home-recent__account" />
                        </Link>
                        <span className="home-recent__tag">
                          {tx.category_id ? (
                            <TagChip category={categoryById.get(tx.category_id)} byId={categoryById} />
                          ) : tx.needs_tag ? (
                            <Link to="/transactions/uncategorized" className="text-button">
                              {labels.addTag}
                            </Link>
                          ) : null}
                        </span>
                        <Money cents={tx.total_cents} currency={tx.currency} />
                      </li>
                    );
                  })}
                </ul>
              </section>
            )}
          </div>

          {data.envelopes.length > 0 && (
            <aside aria-labelledby="home-envelopes-title" className="home-block home-side">
              <h2 id="home-envelopes-title" className="home-block__title">
                {labels.nearLimit}
              </h2>
              <ul className="home-list">
                {data.envelopes.slice(0, 5).map((envelope) => (
                  <li key={envelope.category_id}>
                    <Link to="/budget" className="home-envelope">
                      <span className="home-envelope__head">
                        <TruncatedText text={envelope.category_name} />
                        <span
                          className={`home-envelope__note${envelope.available_cents < 0 ? ' is-over' : ''}`}
                        >
                          {envelopeNote(envelope)}
                        </span>
                      </span>
                      <EnvelopeProgress percent={envelopeUsage(envelope)} />
                    </Link>
                  </li>
                ))}
              </ul>
            </aside>
          )}
        </div>
      )}

      {lookups && (
        <NewTransactionModal
          open={showNewTransaction}
          accounts={lookups.accounts}
          stores={lookups.stores}
          categories={categories.data ?? []}
          onClose={() => setShowNewTransaction(false)}
          onCreated={async () => {
            await Promise.all([summary.refetch(), budget.refetch(), refreshUntaggedCount()]);
          }}
        />
      )}
    </div>
  );
};

export default HomePage;
