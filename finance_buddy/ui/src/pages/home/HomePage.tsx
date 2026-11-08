import { useMemo, useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import * as DropdownMenu from '@radix-ui/react-dropdown-menu';
import { ArrowRight, ChevronDown, FileUp, Plus, Receipt } from 'lucide-react';
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
  getHomeSummary,
  type Account,
  type Store
} from '../../api';
import { useApi } from '../../hooks/useApi';
import { refreshUntaggedCount } from '../../hooks/useUntaggedCount';
import { formatShortDate } from '../../utils/date';
import { BALANCE_ADJUSTMENT_LABEL, isBalanceAdjustment, userComment } from '../../utils/transactionLabels';
import { backupHealth } from '../panel/backupStatus';
import {
  HOME_CURRENCY,
  buildTodoItems,
  describeDaysLeft,
  describeEnvelopes,
  envelopeNote,
  envelopeUsage,
  formatChangePct,
  perDay,
  spendingChangePct
} from './model';
import { formatSignedMoney } from '../../components/ui/Money';
import { pluralPL } from '../../utils/format';
import { formatCategoryPath } from '../../utils/categoryPath';
import SpendingPace from './SpendingPace';

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
  const categories = useApi(() => getAllCategories());
  // Stan kopii to dodatek: błąd (np. brak uprawnień) nie może zasłonić reszty.
  const backup = useApi(() => getBackupStatus().catch(() => null));

  const [lookups, setLookups] = useState<Lookups | null>(null);
  const [showNewTransaction, setShowNewTransaction] = useState(false);
  // null = jeszcze nie ruszane: rozwinięte, gdy coś jest ponad budżet.
  const [envelopesOpen, setEnvelopesOpen] = useState<boolean | null>(null);

  const categoryById = useMemo(() => buildCategoryMap(categories.data ?? []), [categories.data]);

  const openNewTransaction = async () => {
    if (!lookups) {
      const [{ accounts }, stores] = await Promise.all([getAccounts(), getAllStores()]);
      setLookups({
        accounts: accounts.filter((account) => !account.is_closed),
        stores
      });
    }
    setShowNewTransaction(true);
  };

  const data = summary.data;
  const todo = data ? buildTodoItems(data, backup.data ? backupHealth(backup.data) : null) : [];
  const change = data ? spendingChangePct(data.spent_month_cents, data.spent_previous_month_cents) : null;
  const overAssigned = data ? data.ready_to_assign_cents < 0 : false;
  const daily = data ? perDay(data.spendable_cents, data.today) : null;
  const envelopesSummary = data ? describeEnvelopes(data.overspent_count, data.envelopes) : '';
  const showEnvelopes = data ? (envelopesOpen ?? data.overspent_count > 0) : false;
  const todoCount = todo.length + (envelopesSummary ? 1 : 0);

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
          {overAssigned ? (
            <section className="home-hero is-danger">
              <div className="home-hero__body">
                <span className="home-hero__label">{labels.overAssigned}</span>
                <Money cents={-data.ready_to_assign_cents} tone="negative" className="home-hero__value" />
                <span className="home-hero__note">
                  {labels.realSpendable}{' '}
                  <Money cents={data.spendable_cents} tone="neutral" className="home-hero__inline" />{' '}
                  {labels.realSpendableNote}
                </span>
              </div>
              <div className="home-hero__aside">
                <Link to="/budget" className="btn btn-primary">
                  {labels.fixInBudget}
                  <ArrowRight size={16} aria-hidden="true" />
                </Link>
                <span className="home-hero__note">{describeDaysLeft(data.today)}</span>
              </div>
            </section>
          ) : (
            <section className="home-hero">
              <div className="home-hero__body">
                <span className="home-hero__label">{labels.canSpend}</span>
                <Money
                  cents={data.spendable_cents}
                  tone={data.spendable_cents < 0 ? 'negative' : 'neutral'}
                  className="home-hero__value"
                />
                <span className="home-hero__note">
                  {daily
                    ? labels.perDay(formatSignedMoney(daily.cents, HOME_CURRENCY), daily.label)
                    : describeDaysLeft(data.today)}
                </span>
              </div>
              {data.ready_to_assign_cents > 0 && (
                <div className="home-hero__aside">
                  <Link to="/budget" className="home-hero__note home-hero__link">
                    {labels.toAssign}:{' '}
                    <Money cents={data.ready_to_assign_cents} tone="neutral" className="home-hero__inline" />
                  </Link>
                </div>
              )}
            </section>
          )}

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
              <span className="home-figure__note">
                {labels.onBudgetAccounts(
                  data.on_budget_account_count,
                  pluralPL(data.on_budget_account_count, 'konto', 'konta', 'kont')
                )}
              </span>
            </Link>
            <Link to="/transactions/uncategorized" className="home-figure">
              <span className="home-figure__label">{labels.untagged}</span>
              <span className={`home-figure__value home-figure__count${data.untagged_count > 0 ? ' is-warning' : ''}`}>
                {data.untagged_count}
              </span>
              <span className="home-figure__note">
                {data.untagged_count > 0 ? labels.untaggedNote : labels.untaggedNone}
              </span>
            </Link>
          </div>

          <div className="home-columns">
            <div className="home-main">
              {todoCount > 0 && (
                <section aria-labelledby="home-todo-title" className="home-card">
                  <div className="home-card__head">
                    <h2 id="home-todo-title" className="home-block__title">
                      {labels.todo}
                    </h2>
                    <span className="home-card__count">{todoCount}</span>
                  </div>
                  <ul className="home-todo">
                    {todo.map((item) => {
                      const body = (
                        <>
                          <span className={`home-dot is-${item.tone}`} aria-hidden="true" />
                          <span className="home-todo__text">
                            <span className="home-todo__label">{item.label}</span>
                            {item.detail && <span className="home-todo__detail">{item.detail}</span>}
                          </span>
                        </>
                      );
                      return (
                        <li key={item.key} className="home-todo__row">
                          {item.action ? (
                            <div className="home-todo__item">
                              {body}
                              <Link to={item.to} className="btn btn-secondary home-todo__action">
                                {item.action}
                              </Link>
                            </div>
                          ) : (
                            <Link to={item.to} className="home-todo__item is-link">
                              {body}
                            </Link>
                          )}
                        </li>
                      );
                    })}

                    {envelopesSummary && (
                      <li className="home-todo__row">
                        <button
                          type="button"
                          className="home-todo__item is-link home-todo__toggle"
                          aria-expanded={showEnvelopes}
                          aria-controls="home-envelopes"
                          onClick={() => setEnvelopesOpen(!showEnvelopes)}
                        >
                          <span
                            className={`home-dot is-${data.overspent_count > 0 ? 'danger' : 'warning'}`}
                            aria-hidden="true"
                          />
                          <span className="home-todo__text">
                            <span className="home-todo__label">{envelopesSummary}</span>
                            <span className="home-todo__detail">{labels.envelopesDetail}</span>
                          </span>
                          <span className="home-todo__more">
                            {showEnvelopes ? labels.showLess : labels.showMore}
                            <ChevronDown size={14} aria-hidden="true" className={showEnvelopes ? 'is-open' : ''} />
                          </span>
                        </button>
                        {showEnvelopes && (
                          <ul id="home-envelopes" className="home-envelopes">
                            {data.envelopes.map((envelope) => {
                              const over = envelope.available_cents < 0;
                              return (
                                <li key={envelope.category_id} className="home-envelope">
                                  <span className="home-envelope__head">
                                    <TruncatedText text={envelope.category_name} />
                                    <span className={`home-envelope__note${over ? ' is-over' : ' is-near'}`}>
                                      {over ? envelopeNote(envelope) : `${Math.floor(envelopeUsage(envelope))}%`}
                                    </span>
                                  </span>
                                  <EnvelopeProgress percent={envelopeUsage(envelope)} />
                                  <span className="home-envelope__foot">
                                    <span className="home-envelope__amounts">
                                      {formatSignedMoney(envelope.spent_cents, HOME_CURRENCY)} z{' '}
                                      {formatSignedMoney(envelope.funds_cents, HOME_CURRENCY)}
                                      {!over && ` · ${envelopeNote(envelope)}`}
                                    </span>
                                    {over && (
                                      <Link to="/budget?filter=overspent" className="text-button">
                                        {labels.coverFromOther}
                                      </Link>
                                    )}
                                  </span>
                                </li>
                              );
                            })}
                          </ul>
                        )}
                      </li>
                    )}
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

            <aside className="home-side">
              {data.pace.current_cents.length > 0 && <SpendingPace pace={data.pace} today={data.today} />}

              {data.top_categories.length > 0 && (
                <section className="home-card home-top" aria-labelledby="home-top-title">
                  <div className="home-card__head">
                    <h2 id="home-top-title" className="home-block__title">
                      {labels.topTitle}
                    </h2>
                  </div>
                  <ul className="home-top__list">
                    {data.top_categories.map((category) => (
                      <li key={category.category_id} className="home-top__item">
                        <span className="home-top__head">
                          <TruncatedText text={formatCategoryPath(category.path)} />
                          <Money cents={category.spent_cents} tone="neutral" />
                        </span>
                        <span className="home-top__track" aria-hidden="true">
                          <span
                            className="home-top__fill"
                            style={{
                              width: `${(category.spent_cents / data.top_categories[0].spent_cents) * 100}%`
                            }}
                          />
                        </span>
                      </li>
                    ))}
                  </ul>
                  {data.uncategorized_spent_cents > 0 && (
                    <Link to="/transactions/uncategorized" className="home-top__note">
                      {labels.topUncategorized(formatSignedMoney(data.uncategorized_spent_cents, HOME_CURRENCY))}
                    </Link>
                  )}
                </section>
              )}
            </aside>
          </div>
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
            await Promise.all([summary.refetch(), refreshUntaggedCount()]);
          }}
        />
      )}
    </div>
  );
};

export default HomePage;
