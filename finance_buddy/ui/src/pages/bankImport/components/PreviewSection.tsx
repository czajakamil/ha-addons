import { useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import type { Account, BankPreviewRow } from '../../../api';
import { formatMoney } from '../../../utils/format';
import type { Decision, RowAction, RowOverride, RowStatus } from '../mapping';
import ResizableTable from '../../../components/ui/ResizableTable';

type Filter = 'all' | 'new' | 'review' | 'done' | 'skipped';

const FILTERS: { value: Filter; label: string; statuses: RowStatus[] | null }[] = [
  { value: 'all', label: 'Wszystkie', statuses: null },
  { value: 'new', label: 'Nowe', statuses: ['new'] },
  { value: 'review', label: 'Do sprawdzenia', statuses: ['match', 'transfer'] },
  { value: 'done', label: 'Już w bazie', statuses: ['already_imported'] },
  { value: 'skipped', label: 'Pominięte', statuses: ['skipped', 'invalid', 'other_currency'] }
];

const STATUS_BADGE: Record<RowStatus, { label: string; tone: string }> = {
  new: { label: 'nowa', tone: 'ok' },
  match: { label: 'jest już w aplikacji', tone: 'warn' },
  transfer: { label: 'przelew własny', tone: 'warn' },
  already_imported: { label: 'już zaimportowana', tone: '' },
  other_currency: { label: 'inna waluta', tone: '' },
  skipped: { label: 'pominięta', tone: '' },
  invalid: { label: 'błąd', tone: 'bad' }
};

const SOURCE_LABEL = { receipt: 'paragon', bank: 'wyciąg', other: 'YNAB / ręczna' } as const;

const ACTION_LABELS: Record<RowStatus, Partial<Record<RowAction, string>>> = {
  new: { create: 'Utwórz', transfer: 'Przelew na konto…', skip: 'Pomiń' },
  match: { merge: 'Scal z istniejącą', create: 'Utwórz mimo to', skip: 'Pomiń' },
  transfer: { transfer: 'Połącz jako przelew', create: 'Utwórz zwykłą', skip: 'Pomiń' },
  already_imported: {},
  other_currency: {},
  skipped: {},
  invalid: {}
};

type Props = {
  rows: BankPreviewRow[];
  currency: string;
  accountId: string;
  accounts: Account[];
  overrides: Record<number, RowOverride>;
  decisions: Decision[];
  onOverride: (index: number, override: RowOverride | null) => void;
};

const PreviewSection = ({
  rows,
  currency,
  accountId,
  accounts,
  overrides,
  decisions,
  onOverride
}: Props) => {
  const [filter, setFilter] = useState<Filter>('all');
  const decided = useMemo(() => new Map(decisions.map((d) => [d.row, d])), [decisions]);
  // Przelew tylko na inne konto w tej samej walucie (v1, §1).
  const transferTargets = accounts.filter(
    (a) => a.id !== accountId && a.currency === currency && !a.is_closed
  );

  const counts = useMemo(() => {
    const result: Record<Filter, number> = { all: rows.length, new: 0, review: 0, done: 0, skipped: 0 };
    for (const row of rows) {
      for (const f of FILTERS) {
        if (f.statuses?.includes(row.status)) result[f.value] += 1;
      }
    }
    return result;
  }, [rows]);

  const active = FILTERS.find((f) => f.value === filter)?.statuses;
  const visible = active ? rows.filter((r) => active.includes(r.status)) : rows;

  return (
    <>
      <div className="bank-import__chips" role="group" aria-label="Filtr wierszy">
        {FILTERS.map((f) => (
          <button
            key={f.value}
            type="button"
            className={`btn btn-sm ${filter === f.value ? 'btn-primary' : 'btn-outline-secondary'}`}
            onClick={() => setFilter(f.value)}
          >
            {f.label} <span className="bank-import__chip-count">{counts[f.value]}</span>
          </button>
        ))}
      </div>

      <div className="table-responsive mt-2">
        <ResizableTable tableId="bank-import-preview" className="table table-sm table-vcenter bank-import__preview">
          <thead>
            <tr>
              <th aria-label="Importuj" />
              <th>Data</th>
              <th>Opis</th>
              <th className="text-end">Kwota</th>
              <th>Status</th>
              <th style={{ minWidth: '11rem' }}>Akcja</th>
            </tr>
          </thead>
          <tbody>
            {visible.map((row) => {
              const actions = ACTION_LABELS[row.status];
              const importable = Object.keys(actions).length > 0;
              const decision = decided.get(row.index);
              const action: RowAction = decision?.action ?? 'skip';
              const badge = STATUS_BADGE[row.status];
              const description = [row.payee, row.memo].filter(Boolean).join(' · ');
              return (
                <tr key={row.index} className={!decision ? 'is-muted' : undefined}>
                  <td>
                    <input
                      type="checkbox"
                      className="form-check-input"
                      aria-label={`Importuj wiersz ${row.line}`}
                      checked={Boolean(decision)}
                      disabled={!importable}
                      onChange={(e) =>
                        onOverride(row.index, e.target.checked ? null : { action: 'skip' })
                      }
                    />
                  </td>
                  <td className="text-nowrap">{row.date ?? '—'}</td>
                  <td>
                    <div className="bank-import__desc">
                      {description || <span className="text-secondary">—</span>}
                    </div>
                    {row.match ? (
                      <div className="small text-secondary">
                        ↔{' '}
                        <Link to={`/transactions/${row.match.transaction_id}`}>
                          {row.match.account_id !== accountId ? `${row.match.account_name} · ` : ''}
                          {row.match.date} · {row.match.label}
                        </Link>{' '}
                        ({SOURCE_LABEL[row.match.source]})
                      </div>
                    ) : null}
                    {row.reason ? <div className="small text-secondary">{row.reason}</div> : null}
                  </td>
                  <td
                    className={`text-end text-nowrap ${
                      (row.amount_cents ?? 0) < 0 ? '' : 'text-success'
                    }`}
                  >
                    {row.amount_cents == null
                      ? '—'
                      : formatMoney(row.amount_cents, row.currency ?? currency)}
                  </td>
                  <td>
                    <span
                      className={`receipt-badge${badge.tone ? ` receipt-badge--${badge.tone}` : ''}`}
                    >
                      {badge.label}
                    </span>
                  </td>
                  <td>
                    {importable ? (
                      <div className="d-flex flex-column gap-1">
                        <select
                          className="form-select form-select-sm"
                          aria-label={`Akcja dla wiersza ${row.line}`}
                          value={action}
                          onChange={(e) =>
                            onOverride(row.index, {
                              action: e.target.value as RowAction,
                              transfer_account_id: overrides[row.index]?.transfer_account_id
                            })
                          }
                        >
                          {(Object.entries(actions) as [RowAction, string][]).map(([value, label]) => (
                            <option key={value} value={value}>
                              {label}
                            </option>
                          ))}
                        </select>
                        {action === 'transfer' && row.status === 'new' ? (
                          <select
                            className="form-select form-select-sm"
                            aria-label={`Konto drugiej strony przelewu, wiersz ${row.line}`}
                            value={decision?.transfer_account_id ?? ''}
                            onChange={(e) =>
                              onOverride(row.index, {
                                action: 'transfer',
                                transfer_account_id: e.target.value || null
                              })
                            }
                          >
                            <option value="">— wybierz konto —</option>
                            {transferTargets.map((a) => (
                              <option key={a.id} value={a.id}>
                                {a.name}
                              </option>
                            ))}
                          </select>
                        ) : null}
                      </div>
                    ) : null}
                  </td>
                </tr>
              );
            })}
            {!visible.length ? (
              <tr>
                <td colSpan={6} className="text-secondary text-center py-3">
                  Brak wierszy w tym filtrze.
                </td>
              </tr>
            ) : null}
          </tbody>
        </ResizableTable>
      </div>
    </>
  );
};

export default PreviewSection;
