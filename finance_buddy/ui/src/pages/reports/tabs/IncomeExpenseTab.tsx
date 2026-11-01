import { useMemo } from 'react';
import { aggregateCategories, moneyLabel } from '../model';
import type { ReportsController } from '../useReports';
import ResizableTable from '../../../components/ui/ResizableTable';

type IncomeExpenseTabProps = {
  controller: ReportsController;
};

const IncomeExpenseTab = ({ controller }: IncomeExpenseTabProps) => {
  const { data } = controller;
  const incomeTotals = data?.income_vs_expense;

  const topIncome = useMemo(
    () => (data ? aggregateCategories(data.income_vs_expense.months, 'income') : []),
    [data]
  );
  const topExpenses = useMemo(
    () => (data ? aggregateCategories(data.income_vs_expense.months, 'expense') : []),
    [data]
  );

  return (
    <section className="card report-card report-card--full">
      <div className="report-card__header">
        <div>
          <h3>Przychody vs wydatki</h3>
          <p className="report-card__subtitle">
            Kontroluj miesięczne przychody, wydatki oraz wynik netto.
          </p>
        </div>
        <div className="report-summary-grid">
          <div>
            <p>Łączne przychody</p>
            <strong>{moneyLabel(incomeTotals?.total_income_cents ?? 0)}</strong>
          </div>
          <div>
            <p>Łączne wydatki</p>
            <strong>{moneyLabel(incomeTotals?.total_expense_cents ?? 0)}</strong>
          </div>
        </div>
      </div>
      {incomeTotals?.months.length ? (
        <>
          <div className="table-responsive">
            <ResizableTable tableId="report-income-expense" className="table report-table">
              <thead>
                <tr>
                  <th>Miesiąc</th>
                  <th>Przychody</th>
                  <th>Wydatki</th>
                  <th>Bilans</th>
                </tr>
              </thead>
              <tbody>
                {incomeTotals.months.map((month) => (
                  <tr key={month.month}>
                    <td>{month.label}</td>
                    <td>{moneyLabel(month.income_cents)}</td>
                    <td>{moneyLabel(month.expense_cents)}</td>
                    <td className={month.net_cents >= 0 ? 'is-positive' : 'is-negative'}>
                      {moneyLabel(month.net_cents)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </ResizableTable>
          </div>
          <div className="report-mini-grid">
            <div>
              <p className="report-mini-title">Top źródła przychodu</p>
              <ul className="mini-list">
                {topIncome.map((row) => (
                  <li key={row.id ?? row.name}>
                    <span>{row.name}</span>
                    <strong>{moneyLabel(row.total)}</strong>
                  </li>
                ))}
                {topIncome.length === 0 && <li>Brak danych.</li>}
              </ul>
            </div>
            <div>
              <p className="report-mini-title">Top kategorie wydatków</p>
              <ul className="mini-list">
                {topExpenses.map((row) => (
                  <li key={row.id ?? row.name}>
                    <span>{row.name}</span>
                    <strong>{moneyLabel(row.total)}</strong>
                  </li>
                ))}
                {topExpenses.length === 0 && <li>Brak danych.</li>}
              </ul>
            </div>
          </div>
        </>
      ) : (
        <p className="report-empty">Brak danych o przychodach lub wydatkach.</p>
      )}
    </section>
  );
};

export default IncomeExpenseTab;
