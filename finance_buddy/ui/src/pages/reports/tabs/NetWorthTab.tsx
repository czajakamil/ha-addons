import { moneyLabel, reportMonthLabel } from '../model';
import MonthPairBars from '../components/MonthPairBars';
import NetWorthComposition from '../components/NetWorthComposition';
import type { ReportsController } from '../useReports';
import ResizableTable from '../../../components/ui/ResizableTable';

type NetWorthTabProps = {
  controller: ReportsController;
};

const NetWorthTab = ({ controller }: NetWorthTabProps) => {
  const { data, accountCategories } = controller;
  const netWorth = data?.net_worth;

  return (
    <section className="card report-card report-card--full">
      <div className="report-card__header">
        <div>
          <h3>Majątek netto</h3>
          <p className="report-card__subtitle">Aktywa minus zobowiązania na koniec każdego miesiąca.</p>
        </div>
        <div className="report-summary-grid">
          <div>
            <p>Aktywa</p>
            <strong>{moneyLabel(netWorth?.latest_assets_cents ?? 0)}</strong>
          </div>
          <div>
            <p>Zobowiązania</p>
            <strong>{moneyLabel(netWorth?.latest_debts_cents ?? 0)}</strong>
          </div>
          <div>
            <p>Netto</p>
            <strong>{moneyLabel(netWorth?.latest_net_worth_cents ?? 0)}</strong>
          </div>
        </div>
      </div>
      {netWorth?.months.length ? (
        <>
          <MonthPairBars
            months={netWorth.months.map((point) => ({
              month: point.month,
              first_cents: point.assets_cents,
              second_cents: point.debts_cents,
              result_cents: point.net_worth_cents
            }))}
            first={{ label: 'Aktywa' }}
            second={{ label: 'Zobowiązania', modifier: 'negative' }}
            resultLabel="netto"
            showResultLine
          />
          <NetWorthComposition balances={netWorth.accounts} categories={accountCategories} />
          <div className="table-responsive">
            <ResizableTable tableId="report-net-worth" className="table report-table">
              <thead>
                <tr>
                  <th>Miesiąc</th>
                  <th>Aktywa</th>
                  <th>Zobowiązania</th>
                  <th>Netto</th>
                  <th>Zmiana</th>
                </tr>
              </thead>
              <tbody>
                {netWorth.months.map((point) => (
                  <tr key={point.month}>
                    <td>{reportMonthLabel(point.month)}</td>
                    <td>{moneyLabel(point.assets_cents)}</td>
                    <td>{moneyLabel(point.debts_cents)}</td>
                    <td>{moneyLabel(point.net_worth_cents)}</td>
                    <td className={point.change_cents >= 0 ? 'is-positive' : 'is-negative'}>
                      {moneyLabel(point.change_cents)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </ResizableTable>
          </div>
        </>
      ) : (
        <p className="report-empty">Brak danych o wartości netto.</p>
      )}
    </section>
  );
};

export default NetWorthTab;
