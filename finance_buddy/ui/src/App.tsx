// src/App.tsx
import { Suspense, lazy, useEffect, useState } from 'react';
import { Route, Routes, Navigate, useLocation, useNavigate } from 'react-router-dom';
import * as Tooltip from '@radix-ui/react-tooltip';
import { Menu, X } from 'lucide-react';
import { Toaster } from 'sonner';
import AppSidebar from './components/AppSidebar';
import CommandPalette from './components/CommandPalette';
import WelcomeScreen from './components/WelcomeScreen';
import AuthScreen from './components/AuthScreen';
import {
  clearAuthToken,
  devLogin,
  getAccounts,
  getCategories,
  getSession,
  getTransactions,
  loadDemoData,
  logout,
  setAuthToken,
  type AuthUser
} from './api';
import { useResponsiveSidebar } from './hooks/useResponsiveSidebar';
import { useTheme } from './hooks/useTheme';
import { copy } from './copy/pl';
import './App.css';

const HomePage = lazy(() => import('./pages/home/HomePage'));
const AccountsPage = lazy(() => import('./pages/AccountsPage'));
const AccountDetailsPage = lazy(() => import('./pages/accounts/AccountDetailsPage'));
const BudgetPage = lazy(() => import('./pages/BudgetPage'));
const StoresPage = lazy(() => import('./pages/StoresPage'));
const StoreDetailsPage = lazy(() => import('./pages/stores/StoreDetailsPage'));
const TransactionsPage = lazy(() => import('./pages/TransactionsPage'));
const TransactionDetailsPage = lazy(() => import('./pages/TransactionDetailsPage'));
const ItemsPage = lazy(() => import('./pages/ItemsPage'));
const TagsPage = lazy(() => import('./pages/TagsPage'));
const ReportsPage = lazy(() => import('./pages/ReportsPage'));
const ReceiptInboxPage = lazy(() => import('./pages/receipts/ReceiptInboxPage'));
const ReceiptDraftEditorPage = lazy(() => import('./pages/receipts/ReceiptDraftEditorPage'));
const BankImportPage = lazy(() => import('./pages/bankImport/BankImportPage'));
const UsersPage = lazy(() => import('./pages/UsersPage'));
const UntaggedQueuePage = lazy(() => import('./pages/tagging/UntaggedQueuePage'));

const App = () => {
  const navigate = useNavigate();
  const { pathname } = useLocation();
  const { theme, toggleTheme } = useTheme();
  const {
    isDesktop,
    isSidebarOpen,
    isSidebarCollapsed,
    toggleSidebar,
    closeSidebar
  } = useResponsiveSidebar(1024);

  const [shouldShowWelcome, setShouldShowWelcome] = useState<boolean>(false);
  const [isBootstrapping, setIsBootstrapping] = useState(true);
  const [authUser, setAuthUser] = useState<AuthUser | null>(null);
  const [isAuthChecking, setIsAuthChecking] = useState(true);

  useEffect(() => {
    let active = true;

    // Skips the (non-automatable) passkey login by calling the dev-login
    // endpoint. Fires under `vite dev`, and in the packaged desktop build
    // (VITE_DESKTOP_AUTOLOGIN=true) — there the app is a single-user local
    // program talking to its own 127.0.0.1 backend, so passkeys add nothing.
    // Can be turned off with VITE_DEV_AUTOLOGIN=false, and silently 404s when
    // the backend flag FINANCE_DEV_LOGIN is off — the Docker/self-hosted
    // deployment sets neither flag, so it always keeps the passkey login.
    const tryDevAutoLogin = async (): Promise<AuthUser | null> => {
      const autoLoginAllowed =
        import.meta.env.DEV || import.meta.env.VITE_DESKTOP_AUTOLOGIN === 'true';
      if (!autoLoginAllowed || import.meta.env.VITE_DEV_AUTOLOGIN === 'false') {
        return null;
      }
      try {
        const session = await devLogin();
        setAuthToken(session.token);
        return session.user;
      } catch {
        return null;
      }
    };

    const checkSession = async () => {
      try {
        const user = await getSession();
        if (active) {
          setAuthUser(user);
        }
      } catch {
        const devUser = await tryDevAutoLogin();
        if (active) {
          setAuthUser(devUser);
        }
      } finally {
        if (active) {
          setIsAuthChecking(false);
        }
      }
    };

    void checkSession();
    return () => {
      active = false;
    };
  }, []);

  // On startup check if base data exists; if empty, show welcome.
  useEffect(() => {
    if (!authUser) {
      setIsBootstrapping(false);
      setShouldShowWelcome(false);
      return;
    }

    let active = true;
    setIsBootstrapping(true);
    const fallbackTimeout = window.setTimeout(() => {
      if (!active) return;
      setIsBootstrapping(false);
      setShouldShowWelcome(false);
    }, 8000);

    const checkIfDatabaseEmpty = async () => {
      try {
        const [{ accounts }, categories, transactions] = await Promise.all([
          getAccounts(),
          getCategories(),
          getTransactions({ limit: 1 })
        ]);

        if (!active) return;

        const isEmpty = !accounts.length || !categories.length || !transactions.length;
        setShouldShowWelcome(isEmpty);
      } catch (err) {
        console.error('Nie udało się sprawdzić danych początkowych', err);
        if (!active) return;
        setShouldShowWelcome(false);
      } finally {
        if (active) {
          window.clearTimeout(fallbackTimeout);
          setIsBootstrapping(false);
        }
      }
    };

    void checkIfDatabaseEmpty();
    return () => {
      active = false;
      window.clearTimeout(fallbackTimeout);
    };
  }, [authUser]);

  // Zamknięcie sidebaru klawiszem Escape na mobile
  useEffect(() => {
    if (!authUser || isBootstrapping || shouldShowWelcome !== false || !isSidebarOpen || isDesktop)
      return;

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        closeSidebar();
      }
    };

    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [shouldShowWelcome, isSidebarOpen, isDesktop, closeSidebar, isBootstrapping, authUser]);

  const mobileMenuLabel = isSidebarOpen ? copy.nav.close : copy.nav.open;
  const MobileMenuIcon = isSidebarOpen ? X : Menu;

  const shellClasses = [
    'app__shell',
    isDesktop ? 'is-desktop' : '',
    isDesktop && isSidebarCollapsed ? 'is-sidebar-collapsed' : '',
    isDesktop && !isSidebarCollapsed ? 'is-sidebar-expanded' : ''
  ]
    .filter(Boolean)
    .join(' ');

  const completeOnboarding = (targetPath = '/accounts') => {
    setShouldShowWelcome(false);
    navigate(targetPath, { replace: true });
  };

  const handleLoadDemo = async () => {
    await loadDemoData();
    completeOnboarding('/budget');
  };

  const handleAuthenticated = (user: AuthUser) => {
    setAuthUser(user);
  };

  const handleOpenUserPanel = () => {
    navigate('/panel');
    if (!isDesktop) {
      closeSidebar();
    }
  };

  const handleLogout = async () => {
    try {
      await logout();
    } catch (err) {
      console.warn('Logout failed', err);
    } finally {
      clearAuthToken();
      setAuthUser(null);
      setShouldShowWelcome(false);
      setIsBootstrapping(false);
      if (!isDesktop) {
        closeSidebar();
      }
    }
  };

  if (isAuthChecking) {
    return (
      <div className="app app--bootstrapping">
        <div className="container-xl">
          <div className="page-body">
            <div className="card">
              <div className="card-body">
                <div className="placeholder-glow">
                  <div className="placeholder col-12 mb-2" />
                  <div className="placeholder col-10 mb-2" />
                  <div className="placeholder col-12 mb-2" />
                  <div className="placeholder col-8 mb-0" />
                </div>
                <p className="text-secondary small mt-3 mb-0">Sprawdzam sesję…</p>
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  }

  if (!authUser) {
    return <AuthScreen onAuthenticated={handleAuthenticated} />;
  }

  if (isBootstrapping) {
    return (
      <div className="app app--bootstrapping">
        <div className="container-xl">
          <div className="page-body">
            <div className="card">
              <div className="card-body">
                <div className="placeholder-glow">
                  <div className="placeholder col-12 mb-2" />
                  <div className="placeholder col-10 mb-2" />
                  <div className="placeholder col-12 mb-2" />
                  <div className="placeholder col-8 mb-0" />
                </div>
                <p className="text-secondary small mt-3 mb-0">
                  Ładuję aplikację… upewnij się, że backend jest uruchomiony.
                </p>
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  }

  if (shouldShowWelcome) {
    return (
      <WelcomeScreen
        onStartFresh={() => completeOnboarding('/accounts')}
        onImportComplete={() => completeOnboarding('/accounts')}
        onLoadDemo={handleLoadDemo}
      />
    );
  }

  const handleNavLinkClick = () => {
    // Na mobile po kliknięciu w link chowamy menu
    if (!isDesktop) {
      closeSidebar();
    }
  };

  return (
    <Tooltip.Provider delayDuration={300}>
    <div className="app">
      {/* Tło pod sidebar na mobile */}
      <div
        className={`app__sidebar-backdrop ${isSidebarOpen && !isDesktop ? 'is-visible' : ''}`}
        onClick={closeSidebar}
        aria-hidden="true"
      />

      <div className={shellClasses}>
        <AppSidebar
          user={authUser}
          theme={theme}
          isOpen={isSidebarOpen}
          isCollapsed={isSidebarCollapsed}
          isDesktop={isDesktop}
          onToggle={toggleSidebar}
          onToggleTheme={toggleTheme}
          onNavigate={handleNavLinkClick}
          onOpenPanel={handleOpenUserPanel}
          onLogout={() => void handleLogout()}
        />

        <div className="app__workspace">
          {/* Pasek z menu tylko na mobile */}
          {!isDesktop && (
            <div className="app__workspace-toolbar">
              <button
                className="sidebar__icon-button"
                type="button"
                aria-label={mobileMenuLabel}
                aria-expanded={isSidebarOpen}
                aria-controls="app-sidebar"
                onClick={toggleSidebar}
              >
                <MobileMenuIcon size={20} aria-hidden="true" />
              </button>
              <span className="sidebar__brand">{pathname === '/reports' ? 'Raporty' : 'Budget Buddy'}</span>
            </div>
          )}

          <div className="app__main">
            <main className="app__content">
              <Suspense
                fallback={
                  <div className="container-xl">
                    <div className="page-body">
                      <div className="card">
                        <div className="card-body">
                          <div className="placeholder-glow">
                            <div className="placeholder col-12 mb-2" />
                            <div className="placeholder col-10 mb-2" />
                            <div className="placeholder col-12 mb-2" />
                            <div className="placeholder col-8 mb-0" />
                          </div>
                          <p className="text-secondary small mt-3 mb-0">Ładuję ekran…</p>
                        </div>
                      </div>
                    </div>
                  </div>
                }
              >
                <Routes>
                  <Route path="/" element={<HomePage />} />
                  <Route path="/accounts" element={<AccountsPage />} />
                  <Route path="/accounts/:accountId" element={<AccountDetailsPage />} />
                  <Route path="/budget" element={<BudgetPage />} />
                  <Route path="/stores" element={<StoresPage />} />
                  <Route path="/payees/:storeId" element={<StoreDetailsPage />} />
                  <Route path="/transactions" element={<TransactionsPage />} />
                  <Route path="/transactions/uncategorized" element={<UntaggedQueuePage />} />
                  {/* Stary adres kolejki, sprzed zmiany „Tag” na „Kategoria”. */}
                  <Route path="/transactions/untagged" element={<Navigate to="/transactions/uncategorized" replace />} />
                  <Route path="/transactions/:transactionId" element={<TransactionDetailsPage />} />
                  <Route path="/receipts" element={<ReceiptInboxPage />} />
                  <Route path="/receipts/:draftId" element={<ReceiptDraftEditorPage />} />
                  <Route path="/bank-import" element={<BankImportPage />} />
                  {/* Stary adres importu — skrzynka go zastąpiła. */}
                  <Route path="/receipt-import" element={<Navigate to="/receipts" replace />} />
                  <Route path="/items" element={<ItemsPage />} />
                  <Route path="/categories" element={<TagsPage />} />
                  <Route path="/tags" element={<Navigate to="/categories" replace />} />
                  <Route path="/reports" element={<ReportsPage />} />
                  <Route path="/panel" element={<UsersPage />} />
                  <Route path="/users" element={<Navigate to="/panel" replace />} />
                  <Route path="*" element={<Navigate to="/" replace />} />
                </Routes>
              </Suspense>
            </main>
          </div>
        </div>
      </div>
      <CommandPalette />
      <Toaster
        position="bottom-center"
        toastOptions={{ className: 'toast-item', unstyled: true, classNames: { actionButton: 'toast-item__action' } }}
      />
    </div>
    </Tooltip.Provider>
  );
};

export default App;
