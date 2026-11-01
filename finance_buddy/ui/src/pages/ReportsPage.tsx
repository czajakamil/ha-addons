import { useTheme } from '../hooks/useTheme';
import ReportsTopbar from './reports/components/ReportsTopbar';
import AccountCategoriesTab from './reports/tabs/AccountCategoriesTab';
import FlowsTab from './reports/tabs/FlowsTab';
import IncomeExpenseTab from './reports/tabs/IncomeExpenseTab';
import NetWorthTab from './reports/tabs/NetWorthTab';
import SpendingBreakdownTab from './reports/tabs/SpendingBreakdownTab';
import SpendingTrendsTab from './reports/tabs/SpendingTrendsTab';
import { useReports, type ReportsController } from './reports/useReports';

const renderActiveTab = (controller: ReportsController) => {
  const { activeTab, data } = controller;
  if (!data && activeTab !== 'snakey') {
    return null;
  }

  switch (activeTab) {
    case 'spending-breakdown':
      return <SpendingBreakdownTab controller={controller} />;
    case 'account-categories':
      return <AccountCategoriesTab controller={controller} />;
    case 'spending-trends':
      return <SpendingTrendsTab controller={controller} />;
    case 'income-expense':
      return <IncomeExpenseTab controller={controller} />;
    case 'net-worth':
      return <NetWorthTab controller={controller} />;
    case 'snakey':
      return <FlowsTab controller={controller} />;
    default:
      return null;
  }
};

const ReportsPage = () => {
  const controller = useReports();
  // Kolory serii czytamy z tokenów przy budowaniu danych, więc zmiana motywu przebudowuje zakładkę
  const { theme } = useTheme();
  const { activeTab, data, loading, error } = controller;

  return (
    <div className="page reports-layout">
      <ReportsTopbar controller={controller} />
      <div className="page-shell reports-page__container">
        <div className="page-body page-stack reports-page">
          {loading && <div className="loading">Ładuję raporty...</div>}
          {error && <div className="error">{error}</div>}
          {(data || activeTab === 'snakey') && !loading && !error && (
            <div key={theme} className="reports-tab-panel">{renderActiveTab(controller)}</div>
          )}
          {!data && activeTab !== 'snakey' && !loading && !error && (
            <p className="report-empty">Brak danych do wyświetlenia.</p>
          )}
        </div>
      </div>
    </div>
  );
};

export default ReportsPage;
