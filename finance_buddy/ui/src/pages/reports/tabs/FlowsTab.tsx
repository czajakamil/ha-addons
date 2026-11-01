import { useMemo } from 'react';
import { ResponsiveSankey } from '@nivo/sankey';
import { SNAKEY_MAX_TRANSACTIONS, SNAKEY_OTHER_CATEGORY_LABEL, moneyLabel, type SnakeyNode } from '../model';
import { buildFlows } from '../flows';
import { chartColor } from '../../../utils/tokens';
import { useChartTheme } from '../useChartTheme';
import type { ReportsController } from '../useReports';

type FlowsTabProps = {
  controller: ReportsController;
};

const FlowsTab = ({ controller }: FlowsTabProps) => {
  const chartTheme = useChartTheme();
  const {
    snakeyTransactions,
    snakeyLoading,
    snakeyError,
    snakeyLimitReached,
    selectedAccountIds,
    selectedCategoryIds,
    allAccountOptionIds,
    allCategoryOptionIds,
    categories,
    categoryNameLookup
  } = controller;

  const flows = useMemo(
    () =>
      buildFlows({
        transactions: snakeyTransactions,
        categories,
        categoryNameLookup,
        accountIds:
          selectedAccountIds.length > 0 && selectedAccountIds.length !== allAccountOptionIds.length
            ? new Set(selectedAccountIds)
            : null,
        categoryIds:
          selectedCategoryIds.length > 0 && selectedCategoryIds.length !== allCategoryOptionIds.length
            ? new Set(selectedCategoryIds)
            : null
      }),
    [
      allAccountOptionIds,
      allCategoryOptionIds,
      categories,
      categoryNameLookup,
      selectedAccountIds,
      selectedCategoryIds,
      snakeyTransactions
    ]
  );

  return (
    <section className="card report-card report-card--full">
      <div className="report-card__header">
        <div>
          <h3>Przepływy</h3>
          <p className="report-card__subtitle">Źródła pieniędzy, grupy kategorii i kategorie w wybranym okresie.</p>
        </div>
        <div className="report-summary-grid">
          <div>
            <p>Przychody</p>
            <strong>{moneyLabel(flows.income)}</strong>
          </div>
          <div>
            <p>Wydatki</p>
            <strong>{moneyLabel(flows.spending)}</strong>
          </div>
          {flows.fromSavings > 0 && (
            <div>
              <p>Z oszczędności</p>
              <strong>{moneyLabel(flows.fromSavings)}</strong>
            </div>
          )}
        </div>
      </div>
      {snakeyLoading ? (
        <p className="report-empty">Ładowanie diagramu...</p>
      ) : snakeyError ? (
        <p className="report-empty">{snakeyError}</p>
      ) : flows.data.links.length === 0 ? (
        <p className="report-empty">Brak przepływów w wybranym okresie.</p>
      ) : (
        <>
          <div className="snakey-scroll">
          <div
            className="snakey-chart snakey-chart--full"
            role="img"
            aria-label={
              flows.fromSavings > 0
                ? 'Przychody i oszczędności rozdzielone na grupy kategorii i kategorie'
                : 'Przychody rozdzielone na grupy kategorii i kategorie'
            }
          >
            <ResponsiveSankey
              data={flows.data}
              margin={{ top: 16, right: 160, bottom: 16, left: 96 }}
              align="justify"
              sort="descending"
              colors={[1, 2, 3, 4, 5, 6, 7, 8].map((slot) => chartColor(slot - 1))}
              nodeOpacity={1}
              nodeThickness={12}
              nodeSpacing={16}
              label={(node) => String((node as SnakeyNode).name ?? node.id)}
              labelTextColor={chartTheme.text}
              theme={chartTheme.nivoTheme}
              valueFormat={(value) => moneyLabel(Number(value))}
              linkOpacity={0.3}
              linkHoverOthersOpacity={0.1}
            />
          </div>
          </div>
          {flows.hiddenCount > 0 && (
            <p className="snakey-note">
              Mniejsze kategorie ({flows.hiddenCount}) są połączone w „{SNAKEY_OTHER_CATEGORY_LABEL}”.
            </p>
          )}
          {snakeyLimitReached && (
            <p className="snakey-note">
              Diagram oparty na pierwszych {SNAKEY_MAX_TRANSACTIONS} transakcjach z wybranego
              zakresu. Zawęź daty lub filtry, aby zobaczyć pełny obraz.
            </p>
          )}
        </>
      )}
    </section>
  );
};

export default FlowsTab;
