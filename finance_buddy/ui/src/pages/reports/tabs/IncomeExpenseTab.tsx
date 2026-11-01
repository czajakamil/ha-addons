import { useMemo } from 'react';
import { aggregateCategories, moneyLabel, reportMonthLabel } from '../model';
import IncomeExpenseBars from '../components/IncomeExpenseBars';
import ReportBarList from '../components/ReportBarList';
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
          <h3>Przychody i wydatki</h3>
          <p className="report-card__subtitle">
            Miesięczne przychody, wydatki i bilans.
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
          <IncomeExpenseBars months={incomeTotals.months} />
          <div className="table-responsive report-table-spaced">
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
                    <td>{reportMonthLabel(month.month)}</td>
                    <td>{moneyLabel(month.income_cents)}</td>
                    <td>{moneyLabel(month.expense_cents)}</td>
                    <td className={month.net_cents > 0 ? 'is-positive' : month.net_cents < 0 ? 'is-negative' : undefined}>
                      {moneyLabel(month.net_cents)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </ResizableTable>
          </div>
          <div className="report-mini-grid">
            {[
              { title: 'Top źródła przychodu', rows: topIncome, base: incomeTotals.total_income_cents },
              { title: 'Top kategorie wydatków', rows: topExpenses, base: incomeTotals.total_expense_cents }
            ].map(({ title, rows, base }) =>
              rows.length ? (
                <ReportBarList
                  key={title}
                  title={title}
                  rows={rows.map((row) => ({
                    key: row.id ?? row.name,
                    name: row.name,
                    total: Math.abs(row.total),
                    percentage: base ? Math.abs(row.total / base) : 0
                  }))}
                />
              ) : (
                <p key={title} className="report-empty">{title}: brak danych.</p>
              )
            )}
          </div>
        </>
      ) : (
        <p className="report-empty">Brak danych o przychodach lub wydatkach.</p>
      )}
    </section>
  );
};

export default IncomeExpenseTab;
