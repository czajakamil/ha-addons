// Widok długu dla konta typu „zobowiązanie": warunki kredytu z YNAB-a,
// prognoza spłaty i rozbicie raty na kapitał i odsetki w czasie.
//
// Wszystkie liczby poza saldem i warunkami są LICZONE po stronie backendu
// (services/debt.py) — YNAB nie wystawia ani harmonogramu, ani podziału
// transakcji na kapitał i odsetki.
import { useMemo } from 'react';
import { ResponsiveBar } from '@nivo/bar';
import { Account, getAccountDebt } from '../../../api';
import { useApi } from '../../../hooks/useApi';
import { formatMoney } from '../../../utils/format';
import { useChartTheme } from '../../reports/useChartTheme';
import {
  DEBT_INTEREST_COLOR,
  DEBT_PRINCIPAL_COLOR,
  FORECAST_STATUS_MESSAGES,
  buildDebtChartData,
  buildDebtSummary,
  formatAxisAmount,
  formatInstalments,
  formatMonth,
  pickAxisTicks
} from '../debtPresentation';

type DebtDetailsModalProps = {
  account: Account;
  onClose: () => void;
};

const CHART_KEYS = ['principal', 'interest'] as const;
const CHART_LEGEND: Record<(typeof CHART_KEYS)[number], { label: string; color: string }> = {
  principal: { label: 'Kapitał', color: DEBT_PRINCIPAL_COLOR },
  interest: { label: 'Odsetki', color: DEBT_INTEREST_COLOR }
};

const DebtDetailsModal = ({ account, onClose }: DebtDetailsModalProps) => {
  const chartTheme = useChartTheme();
  const {
    data: debt,
    loading,
    error
  } = useApi(() => getAccountDebt(account.id), {
    errorMessage: 'Nie udało się wczytać danych o długu.'
  });

  const summaryRows = useMemo(
    () => (debt ? buildDebtSummary(debt, account.currency) : []),
    [debt, account.currency]
  );
  const chart = useMemo(() => buildDebtChartData(debt?.schedule ?? []), [debt?.schedule]);
  const axisTicks = useMemo(() => pickAxisTicks(chart.data.map((row) => row.label)), [chart.data]);

  const modalTitleId = 'debt-details-modal-title';
  const forecast = debt?.forecast;

  return (
    <>
      <div
        className="modal modal-blur fade show d-block"
        role="dialog"
        aria-modal="true"
        aria-labelledby={modalTitleId}
        style={{ display: 'block' }}
      >
        <div className="modal-dialog modal-dialog-centered modal-lg" role="document">
          <div className="modal-content">
            <div className="modal-header">
              <h3 className="modal-title" id={modalTitleId}>
                Dług — {account.name}
              </h3>
              <button type="button" className="btn-close is-focusable" aria-label="Zamknij" onClick={onClose} />
            </div>

            <div className="modal-body">
              {loading && <div className="text-secondary">Wczytywanie…</div>}

              {error && !loading && (
                <div className="alert alert-danger mb-0" role="alert">
                  {error}
                </div>
              )}

              {debt && !loading && !error && (
                <>
                  <dl className="debt-summary">
                    {summaryRows.map((row) => (
                      <div className="debt-summary__item" key={row.key}>
                        <dt className="debt-summary__label">{row.label}</dt>
                        <dd className="debt-summary__value">{row.value}</dd>
                      </div>
                    ))}
                  </dl>

                  {forecast?.status === 'ok' && forecast.months !== null && forecast.months > 0 && (
                    <dl className="debt-summary debt-summary--forecast">
                      <div className="debt-summary__item">
                        <dt className="debt-summary__label">Pozostało rat</dt>
                        <dd className="debt-summary__value">{formatInstalments(forecast.months)}</dd>
                      </div>
                      <div className="debt-summary__item">
                        <dt className="debt-summary__label">Spłata do</dt>
                        <dd className="debt-summary__value">{formatMonth(forecast.payoff_month)}</dd>
                      </div>
                      <div className="debt-summary__item">
                        <dt className="debt-summary__label">Odsetki łącznie</dt>
                        <dd className="debt-summary__value">
                          {formatMoney(forecast.total_interest_cents ?? 0, account.currency)}
                        </dd>
                      </div>
                    </dl>
                  )}

                  {forecast?.status === 'ok' && forecast.months === 0 && (
                    <div className="alert alert-success mb-0" role="status">
                      To zobowiązanie jest spłacone — nie ma już czego prognozować.
                    </div>
                  )}

                  {forecast && forecast.status !== 'ok' && (
                    <div
                      className={`alert mb-0 ${
                        forecast.status === 'missing_terms' ? 'alert-info' : 'alert-warning'
                      }`}
                      role="status"
                    >
                      {FORECAST_STATUS_MESSAGES[forecast.status]}
                    </div>
                  )}

                  {chart.data.length > 0 && (
                    <section className="debt-chart">
                      <div className="debt-chart__header">
                        <h4 className="debt-chart__title">Rata: kapitał i odsetki</h4>
                        <p className="debt-chart__hint">
                          {chart.grouped
                            ? 'Sumy roczne — harmonogram jest za długi na ujęcie miesięczne.'
                            : 'Kolejne raty miesiąc po miesiącu.'}
                        </p>
                      </div>
                      <div className="debt-chart__canvas" style={{ height: 280 }}>
                        <ResponsiveBar
                          data={chart.data}
                          keys={[...CHART_KEYS]}
                          indexBy="label"
                          groupMode="stacked"
                          margin={{ top: 8, right: 16, bottom: 56, left: 76 }}
                          padding={0.25}
                          theme={chartTheme.nivoTheme}
                          colors={(bar) => CHART_LEGEND[bar.id as (typeof CHART_KEYS)[number]].color}
                          enableLabel={false}
                          axisLeft={{ format: (value) => formatAxisAmount(Number(value)) }}
                          axisBottom={{ tickRotation: -45, tickValues: axisTicks }}
                          tooltip={({ id, value }) => (
                            <div className="report-tooltip debt-chart__tooltip">
                              <div className="report-tooltip__title report-tooltip__title--sm">
                                {CHART_LEGEND[id as (typeof CHART_KEYS)[number]].label}
                              </div>
                              <div>{formatMoney(Math.round(Number(value) * 100), account.currency)}</div>
                            </div>
                          )}
                          role="img"
                          ariaLabel="Rozbicie rat na kapitał i odsetki"
                        />
                      </div>
                      <ul className="debt-chart__legend">
                        {CHART_KEYS.map((key) => (
                          <li className="debt-chart__legend-item" key={key}>
                            <span
                              className="debt-chart__legend-dot"
                              style={{ backgroundColor: CHART_LEGEND[key].color }}
                              aria-hidden="true"
                            />
                            {CHART_LEGEND[key].label}
                          </li>
                        ))}
                      </ul>
                    </section>
                  )}
                </>
              )}
            </div>

            <div className="modal-footer">
              <button type="button" className="btn btn-outline-secondary is-focusable" onClick={onClose}>
                Zamknij
              </button>
            </div>
          </div>
        </div>
      </div>
      <div className="modal-backdrop fade show" />
    </>
  );
};

export default DebtDetailsModal;
