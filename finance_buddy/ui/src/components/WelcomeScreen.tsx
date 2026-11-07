import { useEffect, useMemo, useState } from 'react';
import { ArrowRightIcon, BookIcon, EyeIcon, EyeOffIcon, ImportIcon, LockIcon, RocketIcon, SparklesIcon } from 'lucide-react';
import {
  getYnabSettings,
  importYnabBudget,
  listYnabBudgets,
  type YnabBudgetSummary,
  type YnabImportResult,
} from '../api';
import { pluralPL } from '../utils/format';

type WelcomeScreenProps = {
  onStartFresh: () => void;
  onImportComplete: () => void;
  /** Opcjonalnie: załaduj przykładowe dane / demo */
  onLoadDemo?: () => Promise<void>;
};

const resolveYnabError = (err: unknown) => {
  const rawMessage = err instanceof Error ? err.message : 'Nie udało się połączyć z YNAB.';
  let detail = rawMessage;

  if (rawMessage.trim().startsWith('{')) {
    try {
      const parsed = JSON.parse(rawMessage) as Record<string, unknown>;
      if (parsed && typeof parsed === 'object') {
        if (typeof parsed.detail === 'string') {
          detail = parsed.detail;
        } else if (parsed.error && typeof parsed.error === 'object') {
          const apiError = parsed.error as Record<string, unknown>;
          detail =
            (typeof apiError.detail === 'string' && apiError.detail) ||
            (typeof apiError.name === 'string' && apiError.name) ||
            rawMessage;
        }
      }
    } catch {
      detail = rawMessage;
    }
  }

  const normalized = detail.toLowerCase();
  const isInvalidKey =
    normalized.includes('invalid ynab api key') ||
    normalized.includes('api key is required') ||
    normalized.includes('unauthorized') ||
    normalized.includes('access token') ||
    normalized.includes('authentication') ||
    normalized.includes('401');

  return {
    isInvalidKey,
    message: isInvalidKey ? 'Niepoprawny klucz API YNAB.' : detail,
  };
};

const WelcomeScreen = ({ onStartFresh, onImportComplete, onLoadDemo }: WelcomeScreenProps) => {
  const [apiKey, setApiKey] = useState('');
  const [showApiKey, setShowApiKey] = useState(false);
  const [apiKeyInvalid, setApiKeyInvalid] = useState(false);
  const [hasConfiguredKey, setHasConfiguredKey] = useState(false);

  const [budgets, setBudgets] = useState<YnabBudgetSummary[]>([]);
  const [selectedBudgetId, setSelectedBudgetId] = useState('');

  const [loadingBudgets, setLoadingBudgets] = useState(false);
  const [importing, setImporting] = useState(false);

  const [status, setStatus] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [budgetStatus, setBudgetStatus] = useState<string | null>(null);
  const [summary, setSummary] = useState<YnabImportResult | null>(null);
  const [showFaq, setShowFaq] = useState(false);
  const [loadingDemo, setLoadingDemo] = useState(false);
  const [demoError, setDemoError] = useState<string | null>(null);
  const hasDemo = typeof onLoadDemo === 'function';

  const trimmedKey = apiKey.trim();
  const canUseKey = Boolean(trimmedKey) || hasConfiguredKey;
  const busy = loadingBudgets || importing;

  const handleLoadDemo = async () => {
    if (!onLoadDemo || loadingDemo) return;
    setLoadingDemo(true);
    setDemoError(null);
    try {
      await onLoadDemo();
    } catch (err) {
      setDemoError(err instanceof Error ? err.message : 'Nie udało się załadować budżetu testowego.');
    } finally {
      setLoadingDemo(false);
    }
  };

  const budgetsLabel = useMemo(() => {
    const n = budgets.length;
    return `${n} ${pluralPL(n, 'budżet', 'budżety', 'budżetów')}`;
  }, [budgets.length]);

  const importButtonLabel = useMemo(() => {
    return budgets.length > 0 ? 'Importuj' : 'Pobierz i importuj';
  }, [budgets.length]);

  useEffect(() => {
    // Gdy użytkownik zmienia token, nie trzymaj starych budżetów i podsumowania.
    setBudgets([]);
    setSelectedBudgetId('');
    setSummary(null);
    setStatus(null);
    setError(null);
    setBudgetStatus(null);
    setApiKeyInvalid(false);
  }, [trimmedKey]);

  useEffect(() => {
    let cancelled = false;
    getYnabSettings()
      .then((settings) => {
        if (!cancelled) setHasConfiguredKey(settings.api_key_set);
      })
      .catch(() => {
        // Bez ustawień zostaje ręczne wklejenie klucza.
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const applyBudgets = (result: YnabBudgetSummary[]) => {
    setBudgets(result);

    if (result.length > 0) {
      setSelectedBudgetId('');
      setBudgetStatus(
        `Znalazłem ${result.length} ${pluralPL(result.length, 'budżet', 'budżety', 'budżetów')}.`
      );
      setStatus(null);
    } else {
      setBudgetStatus(null);
      setError('Nie znaleziono żadnych budżetów w YNAB.');
    }
  };

  const fetchBudgets = async (key?: string) => {
    const result = await listYnabBudgets(key);
    applyBudgets(result);
    return result;
  };

  const showBudgetsError = (err: unknown) => {
    const { isInvalidKey, message } = resolveYnabError(err);
    setStatus(null);
    setError(message);
    setBudgets([]);
    setSelectedBudgetId('');
    setBudgetStatus(null);
    setApiKeyInvalid(isInvalidKey);
  };

  useEffect(() => {
    // Klucz z konfiguracji: budżety ładują się same, chyba że użytkownik wkleja inny klucz.
    if (!hasConfiguredKey || trimmedKey) return;

    let cancelled = false;
    setLoadingBudgets(true);
    setStatus('Ładuję budżety z YNAB…');

    listYnabBudgets()
      .then((result) => {
        if (!cancelled) applyBudgets(result);
      })
      .catch((err) => {
        if (!cancelled) showBudgetsError(err);
      })
      .finally(() => {
        if (!cancelled) setLoadingBudgets(false);
      });

    return () => {
      cancelled = true;
      setLoadingBudgets(false);
    };
  }, [hasConfiguredKey, trimmedKey]);

  const handleFetchBudgets = async () => {
    if (!canUseKey) {
      setError('Wklej klucz API YNAB, aby pobrać listę budżetów.');
      return;
    }

    setLoadingBudgets(true);
    setError(null);
    setSummary(null);
    setStatus('Ładuję budżety z YNAB…');

    try {
      await fetchBudgets(trimmedKey || undefined);
      setApiKeyInvalid(false);
    } catch (err) {
      showBudgetsError(err);
    } finally {
      setLoadingBudgets(false);
    }
  };

  const handleImport = async () => {
    if (!canUseKey) {
      setError('Dodaj klucz API YNAB, aby rozpocząć import.');
      return;
    }

    setImporting(true);
    setError(null);
    setSummary(null);
    setStatus('Importuję dane z YNAB. To może potrwać chwilę…');

    try {
      let list = budgets;
      if (!list.length) {
        setLoadingBudgets(true);
        list = await fetchBudgets(trimmedKey || undefined);
      }

      const chosen = list.some((b) => b.id === selectedBudgetId) ? selectedBudgetId : '';

      if (!chosen) throw new Error('Wybierz budżet do importu.');

      setSelectedBudgetId(chosen);

      const result = await importYnabBudget(chosen, trimmedKey || undefined);
      setSummary(result);
      setStatus('Import zakończony. Wszystkie dane są gotowe.');
    } catch (err) {
      const { isInvalidKey, message } = resolveYnabError(err);
      setStatus(null);
      setError(message);
      if (isInvalidKey) {
        setBudgets([]);
        setSelectedBudgetId('');
        setBudgetStatus(null);
      }
      setApiKeyInvalid(isInvalidKey);
    } finally {
      setImporting(false);
      setLoadingBudgets(false);
    }
  };

  return (
    <div className="welcome">
      <div className="welcome__halo" aria-hidden="true" />
      <div className="welcome__panel">
        <div className="welcome__header">
          <div className="welcome__eyebrow">Budget Buddy</div>
          <h1>Twój budżet, Twoje zasady</h1>
          <p className="welcome__lead">
            Kontroluj wydatki, planuj budżet i śledź każdy grosz. Wybierz ścieżkę startu i przejdź do
            działania.
          </p>
        </div>

        <div className="welcome__grid">
          <section className="welcome__card welcome__card--primary">
            <header className="welcome__card-header">
              <div className="welcome__icon">
                <RocketIcon size={22} strokeWidth={1.6} />
              </div>
              <div>
                <p className="welcome__card-eyebrow">Szybki start</p>
                <h2>Rozpocznij od zera</h2>
              </div>
            </header>

            <p className="welcome__text">
              Utwórz pustą bazę, dodaj pierwsze konto lub kategorię i zacznij prowadzić budżet bez
              zbędnych formalności.
            </p>

            <div className="welcome__actions">
              <button
                type="button"
                className="btn btn-primary d-inline-flex align-items-center gap-2"
                onClick={onStartFresh}
              >
                Rozpocznij od zera
                <ArrowRightIcon size={18} aria-hidden="true" />
              </button>

              <button
                type="button"
                className="btn btn-outline-secondary d-inline-flex align-items-center gap-2"
                onClick={handleLoadDemo}
                disabled={!hasDemo || loadingDemo}
              >
                {loadingDemo ? 'Ładuję…' : 'Zobacz budżet testowy'}
                <SparklesIcon size={16} aria-hidden="true" />
              </button>
            </div>

            {demoError && (
              <div className="welcome__alert welcome__alert--error" role="alert" aria-live="polite">
                {demoError}
              </div>
            )}
          </section>

          <section className="welcome__card welcome__card--accent">
            <header className="welcome__card-header">
              <div className="welcome__icon welcome__icon--accent">
                <ImportIcon size={22} strokeWidth={1.6} />
              </div>
              <div>
                <p className="welcome__card-eyebrow">Automatyczny import</p>
                <h2>Importuj dane z YNAB</h2>
              </div>
            </header>

            <p className="welcome__text">
              Wklej klucz API, wybierz budżet i przenieś konta, kategorie oraz transakcje bez ręcznego
              przepisywania.
            </p>

            <label className="welcome__label" htmlFor="ynab-api-key">
              Klucz API YNAB
            </label>

            <div className={`welcome__control ${apiKeyInvalid ? 'welcome__control--error' : ''}`}>
              <div className="welcome__input-icon" aria-hidden="true">
                <LockIcon size={16} strokeWidth={1.6} />
              </div>

              <input
                id="ynab-api-key"
                type={showApiKey ? 'text' : 'password'}
                autoComplete="off"
                autoCapitalize="none"
                spellCheck={false}
                placeholder={hasConfiguredKey ? 'Klucz z konfiguracji' : 'ynab-api-key-...'}
                value={apiKey}
                aria-invalid={apiKeyInvalid || undefined}
                onChange={(event) => setApiKey(event.target.value)}
                onKeyDown={(event) => {
                  if (event.key !== 'Enter') return;
                  if (busy) return;
                  if (budgets.length > 0 && selectedBudgetId) void handleImport();
                  else void handleFetchBudgets();
                }}
              />

              <button
                className="btn btn-outline-secondary btn-sm ms-2 d-inline-flex align-items-center gap-1"
                type="button"
                onClick={() => setShowApiKey((v) => !v)}
                disabled={!apiKey || busy}
                aria-label={showApiKey ? 'Ukryj klucz API' : 'Pokaż klucz API'}
                title={showApiKey ? 'Ukryj' : 'Pokaż'}
              >
                {showApiKey ? <EyeOffIcon size={16} /> : <EyeIcon size={16} />}
              </button>

              <button
                className="btn btn-outline-secondary btn-sm ms-2"
                type="button"
                onClick={handleFetchBudgets}
                disabled={busy || !canUseKey}
              >
                {loadingBudgets ? 'Ładowanie…' : 'Pobierz budżety'}
              </button>
            </div>

            {hasConfiguredKey && (
              <p className="welcome__text">
                Używam klucza z konfiguracji. Wklej inny tylko wtedy, gdy chcesz go zastąpić.
              </p>
            )}

            {budgets.length > 0 && (
              <>
                {budgetStatus && <p className="welcome__text">{budgetStatus}</p>}

                <label className="welcome__label" htmlFor="ynab-budget">
                  Budżet do importu
                </label>

                <div className="welcome__control">
                  <div className="welcome__input-icon" aria-hidden="true">
                    <ImportIcon size={16} strokeWidth={1.6} />
                  </div>

                  <select
                    id="ynab-budget"
                    value={selectedBudgetId}
                    onChange={(event) => setSelectedBudgetId(event.target.value)}
                    disabled={busy}
                  >
                    <option value="" disabled>
                      Wybierz budżet…
                    </option>
                    {budgets.map((budget) => (
                      <option key={budget.id} value={budget.id}>
                        {budget.name}
                      </option>
                    ))}
                  </select>

                  <span className="badge bg-secondary-lt text-secondary ms-2">{budgetsLabel}</span>
                </div>
              </>
            )}

            <div className="welcome__actions welcome__actions--import">
              <button
                type="button"
                className="btn btn-primary d-inline-flex align-items-center gap-2"
                onClick={handleImport}
                disabled={busy || !canUseKey || !selectedBudgetId}
              >
                {importing ? 'Importuję…' : importButtonLabel}
                <ArrowRightIcon size={18} aria-hidden="true" />
              </button>

              <button
                type="button"
                className="btn btn-link text-decoration-none d-inline-flex align-items-center gap-2"
                onClick={(event) => {
                  event.preventDefault();
                  event.stopPropagation();
                  setShowFaq((open) => !open);
                }}
              >
                <BookIcon size={16} aria-hidden="true" />
                Dowiedz się więcej o imporcie YNAB
              </button>
            </div>

            {(status || error) && (
              <div
                className={`welcome__alert ${error ? 'welcome__alert--error' : 'welcome__alert--success'}`}
                role={error ? 'alert' : 'status'}
                aria-live="polite"
              >
                {error || status}
              </div>
            )}

            {showFaq && (
              <div className="welcome__faq">
                <p>
                  <strong>Gdzie znaleźć klucz API?</strong> Zaloguj się do YNAB i wejdź w{' '}
                  <em>Developer Settings</em>. Wygeneruj Personal Access Token i wklej go tutaj.
                </p>
                <p>
                  <strong>Co jest importowane?</strong> Konta, sklepy (payees), kategorie oraz
                  transakcje wraz ze statusem cleared/reconciled.
                </p>
                <p>
                  <strong>Czy klucz jest zapisywany?</strong> Używany jest tylko do
                  pobrania danych z YNAB podczas importu.
                </p>
              </div>
            )}

            {summary && (
              <div className="welcome__summary">
                <div className="welcome__summary-header">
                  <h3>Podsumowanie importu</h3>
                  <p>{summary.budget_name || 'Budżet YNAB'}</p>
                </div>

                <div className="welcome__summary-grid">
                  <div className="welcome__stat">
                    <span>Transakcje</span>
                    <strong>{summary.transactions_inserted}</strong>
                    <small>
                      {summary.transactions_skipped > 0
                        ? `+ ${summary.transactions_skipped} pominięto`
                        : 'dodane'}
                    </small>
                  </div>

                  <div className="welcome__stat">
                    <span>Konta</span>
                    <strong>{summary.accounts}</strong>
                    <small>załadowane</small>
                  </div>

                  <div className="welcome__stat">
                    <span>Tagi</span>
                    <strong>{summary.categories}</strong>
                    <small>Załadowane tagi</small>
                  </div>

                  <div className="welcome__stat">
                    <span>Sklepy</span>
                    <strong>{summary.stores}</strong>
                    <small>
                      {summary.stores_categorized
                        ? `${summary.stores_categorized} z kategorią z historii`
                        : 'z payees'}
                    </small>
                  </div>
                </div>

                <button
                  type="button"
                  className="btn btn-primary w-100 d-inline-flex justify-content-center align-items-center gap-2"
                  onClick={onImportComplete}
                >
                  Przejdź do aplikacji
                  <ArrowRightIcon size={18} aria-hidden="true" />
                </button>
              </div>
            )}
          </section>
        </div>

        <div className="welcome__footer">
          <div className="welcome__trust">
            <LockIcon size={16} aria-hidden="true" />
            <span>
              Dane aplikacji są lokalnie. Import łączy się bezpośrednio z YNAB tylko po to, żeby je
              pobrać.
            </span>
          </div>
          <div className="welcome__footnote">
            Brak logowania, brak zakładania kont. Po onboardingu trafisz prosto do kont i raportów.
          </div>
        </div>
      </div>
    </div>
  );
};

export default WelcomeScreen;
