import { useMemo, useState } from 'react';
import type { CSSProperties } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import {
  IconAlertTriangle,
  IconBuildingBank,
  IconCamera,
  IconChecks,
  IconChevronRight,
  IconCoins,
  IconDatabaseExclamation,
  IconFileSpreadsheet,
  IconPlus,
  IconReceipt,
  IconTag
} from '@tabler/icons-react';
import type { Icon } from '@tabler/icons-react';
import Button from '../../components/Button';
import NewTransactionModal from '../../components/transactions/NewTransactionModal';
import { TagChip } from '../../components/tags/TagChip';
import { buildCategoryMap } from '../../components/tags/tagColors';
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
import { formatMoney } from '../../utils/format';
import { backupHealth } from '../panel/backupStatus';
import {
  HOME_CURRENCY,
  buildTodoItems,
  describeMonth,
  envelopeFill,
  envelopeNote,
  formatChangePct,
  shortDate,
  spendingChangePct,
  type TodoKind
} from './model';

const TODO_ICON: Record<TodoKind, Icon> = {
  backup: IconDatabaseExclamation,
  overspent: IconAlertTriangle,
  stale: IconBuildingBank,
  untagged: IconTag,
  receipts: IconReceipt,
  assign: IconCoins
};

type Lookups = { accounts: Account[]; stores: Store[] };

/**
 * Ekran startowy: „co mam dziś zrobić", a nie „jak mi idzie w tym roku" —
 * wykresy są w Raportach. Każdy wiersz „Do zrobienia" prowadzi tam, gdzie się
 * go załatwia, i znika, gdy spadnie do zera.
 */
const HomePage = () => {
  const navigate = useNavigate();
  const summary = useApi(getHomeSummary, { errorMessage: 'Nie udało się wczytać podsumowania.' });
  const categories = useApi(() => getAllCategories());
  // Stan kopii to dodatek: błąd (np. brak uprawnień) nie może zasłonić reszty.
  const backup = useApi(() => getBackupStatus().catch(() => null));

  const [lookups, setLookups] = useState<Lookups | null>(null);
  const [showNewTransaction, setShowNewTransaction] = useState(false);

  const categoryById = useMemo(
    () => buildCategoryMap(categories.data ?? []),
    [categories.data]
  );

  const openNewTransaction = async () => {
    if (!lookups) {
      const [{ accounts }, stores] = await Promise.all([getAccounts(), getAllStores()]);
      setLookups({ accounts: accounts.filter((account) => !account.is_closed), stores });
    }
    setShowNewTransaction(true);
  };

  const data = summary.data;
  const todo = data ? buildTodoItems(data, backup.data ? backupHealth(backup.data) : null) : [];
  const month = data ? describeMonth(data.today) : null;
  const change = data ? spendingChangePct(data.spent_month_cents, data.spent_previous_month_cents) : null;

  return (
    <div className="page-wrapper compact-page">
      <div className="page-shell home-page">
        <div className="page-header d-print-none">
          <div className="home-page__header">
            <div>
              <h2 className="page-title">{month?.title ?? 'Start'}</h2>
              {month ? <div className="text-secondary mt-1">{month.progress}</div> : null}
            </div>
            <div className="home-page__actions">
              <Button
                size="sm"
                icon={<IconPlus size={16} aria-hidden="true" />}
                onClick={() => void openNewTransaction()}
              >
                Wydatek
              </Button>
              <Link to="/receipts" className="btn btn-outline-secondary btn-sm">
                <IconCamera size={16} className="me-1" aria-hidden="true" />
                Paragon
              </Link>
              <Link to="/bank-import" className="btn btn-outline-secondary btn-sm">
                <IconFileSpreadsheet size={16} className="me-1" aria-hidden="true" />
                CSV z banku
              </Link>
            </div>
          </div>
        </div>

        <div className="page-body home-page__body">
          {summary.error ? (
            <div className="alert alert-danger" role="alert">
              {summary.error}
            </div>
          ) : null}

          {!data ? (
            summary.loading ? <p className="text-secondary">Ładuję…</p> : null
          ) : (
            <>
              <section className="card" aria-labelledby="home-todo-title">
                <div className="card-body">
                  <h3 id="home-todo-title" className="home-page__section-title">
                    Do zrobienia
                  </h3>
                  {todo.length ? (
                    <ul className="home-list">
                      {todo.map((item) => {
                        const ItemIcon = TODO_ICON[item.kind];
                        return (
                          <li key={item.key}>
                            <Link to={item.to} className="home-list__row">
                              <ItemIcon
                                size={18}
                                className={`home-list__icon is-${item.tone}`}
                                aria-hidden="true"
                              />
                              <span className="home-list__label">{item.label}</span>
                              {item.value ? <strong className="home-list__value">{item.value}</strong> : null}
                              <IconChevronRight size={16} className="home-list__chevron" aria-hidden="true" />
                            </Link>
                          </li>
                        );
                      })}
                    </ul>
                  ) : (
                    <div className="home-page__done">
                      <IconChecks size={24} aria-hidden="true" />
                      <span>Wszystko ogarnięte.</span>
                    </div>
                  )}
                </div>
              </section>

              <div className="home-metrics">
                <div className="home-metric">
                  <span className="home-metric__label">Wydane w tym miesiącu</span>
                  <span className="home-metric__value">
                    {formatMoney(data.spent_month_cents, HOME_CURRENCY)}
                  </span>
                </div>
                <div className="home-metric">
                  <span className="home-metric__label">Poprzedni miesiąc do tego dnia</span>
                  <span className="home-metric__value">
                    {formatMoney(data.spent_previous_month_cents, HOME_CURRENCY)}
                  </span>
                  {change !== null ? (
                    <span className={`home-metric__note ${change > 0 ? 'is-danger' : 'is-success'}`}>
                      {formatChangePct(change)} w tym miesiącu
                    </span>
                  ) : null}
                </div>
                <div className="home-metric">
                  <span className="home-metric__label">Saldo kont budżetowych</span>
                  <span className="home-metric__value">
                    {formatMoney(data.on_budget_balance_cents, HOME_CURRENCY)}
                  </span>
                </div>
              </div>

              {data.envelopes.length ? (
                <section className="card" aria-labelledby="home-envelopes-title">
                  <div className="card-body">
                    <h3 id="home-envelopes-title" className="home-page__section-title">
                      Koperty blisko limitu
                    </h3>
                    <ul className="home-list">
                      {data.envelopes.map((envelope) => {
                        const overspent = envelope.available_cents < 0;
                        return (
                          <li key={envelope.category_id}>
                            <Link to="/budget" className="home-list__row home-envelope">
                              <span className="home-envelope__name">{envelope.category_name}</span>
                              <span
                                className={`home-envelope__bar ${overspent ? 'is-danger' : 'is-warning'}`}
                                style={{ '--fill': `${envelopeFill(envelope)}%` } as CSSProperties}
                                aria-hidden="true"
                              />
                              <span className={`home-envelope__note${overspent ? ' is-danger' : ''}`}>
                                {envelopeNote(envelope)}
                              </span>
                            </Link>
                          </li>
                        );
                      })}
                    </ul>
                  </div>
                </section>
              ) : null}

              {data.recent.length ? (
                <section className="card" aria-labelledby="home-recent-title">
                  <div className="card-body">
                    <div className="home-page__section-head">
                      <h3 id="home-recent-title" className="home-page__section-title">
                        Ostatnie transakcje
                      </h3>
                      <Link to="/transactions" className="home-page__more">
                        Wszystkie
                      </Link>
                    </div>
                    <ul className="home-list">
                      {data.recent.map((tx) => (
                        <li key={tx.id} className="home-list__row home-recent">
                          <span className="home-recent__date">{shortDate(tx.date)}</span>
                          <Link to={`/transactions/${tx.id}`} className="home-recent__payee">
                            <span>{tx.store_name || tx.comment || (tx.is_transfer ? 'Przelew' : 'Bez opisu')}</span>
                            <span className="home-recent__account">{tx.account_name}</span>
                          </Link>
                          {tx.category_id ? (
                            <span className="home-recent__tag">
                              <TagChip category={categoryById.get(tx.category_id)} byId={categoryById} />
                            </span>
                          ) : tx.needs_tag ? (
                            <span className="home-recent__tag">
                              <button
                                type="button"
                                className="home-recent__add-tag"
                                onClick={() => navigate('/transactions/untagged')}
                              >
                                + tag
                              </button>
                            </span>
                          ) : null}
                          <span className={`home-recent__amount${tx.total_cents > 0 ? ' is-income' : ''}`}>
                            {formatMoney(tx.total_cents, tx.currency)}
                          </span>
                        </li>
                      ))}
                    </ul>
                  </div>
                </section>
              ) : null}
            </>
          )}
        </div>
      </div>

      {lookups ? (
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
      ) : null}
    </div>
  );
};

export default HomePage;
