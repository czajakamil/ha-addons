// Integracja YNAB: klucz, wybór budżetu i import transakcji.
import { useEffect, useState } from 'react';
import { Download, Eye, EyeOff, RefreshCw } from 'lucide-react';
import { toast } from 'sonner';
import {
  getYnabSettings,
  importYnabBudget,
  listYnabBudgets,
  updateYnabSettings,
  type YnabBudgetSummary,
  type YnabImportResult,
  type YnabSettings
} from '../../api';
import { formatDateTime } from './format';

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
    message: isInvalidKey ? 'Niepoprawny klucz API YNAB.' : detail
  };
};

const YnabSection = () => {
  const [settings, setSettings] = useState<YnabSettings | null>(null);
  const [apiKey, setApiKey] = useState('');
  const [showKey, setShowKey] = useState(false);
  const [budgets, setBudgets] = useState<YnabBudgetSummary[]>([]);
  const [budgetId, setBudgetId] = useState('');
  const [loadingSettings, setLoadingSettings] = useState(true);
  const [budgetsLoading, setBudgetsLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [importing, setImporting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [summary, setSummary] = useState<YnabImportResult | null>(null);

  const loadSettings = async (activeRef?: { current: boolean }) => {
    const isActive = () => (activeRef ? activeRef.current : true);
    try {
      const loaded = await getYnabSettings();
      if (isActive()) {
        setSettings(loaded);
        setBudgetId(loaded.budget_id ?? '');
        setBudgets([]);
      }
    } catch (err) {
      if (isActive()) setError(resolveYnabError(err).message);
    } finally {
      if (isActive()) setLoadingSettings(false);
    }
  };

  useEffect(() => {
    const activeRef = { current: true };
    void loadSettings(activeRef);
    return () => {
      activeRef.current = false;
    };
  }, []);

  const trimmedKey = apiKey.trim();
  const hasKey = Boolean(trimmedKey) || Boolean(settings?.api_key_set);
  const isDirty = Boolean(trimmedKey) || budgetId !== (settings?.budget_id ?? '');
  // Bez pobranej listy znamy tylko identyfikator zapisanego budżetu, a nie jego nazwę.
  const savedBudgetMissing = Boolean(budgetId) && !budgets.some((budget) => budget.id === budgetId);

  const handleFailure = (err: unknown) => {
    const { message, isInvalidKey } = resolveYnabError(err);
    setError(message);
    if (isInvalidKey) setBudgets([]);
  };

  const handleFetchBudgets = async () => {
    if (!hasKey) {
      setError('Wklej klucz API YNAB, aby pobrać listę budżetów.');
      return;
    }

    setBudgetsLoading(true);
    setError(null);
    try {
      const result = await listYnabBudgets(trimmedKey || undefined);
      setBudgets(result);
      if (result.length) {
        toast('Pobrano listę budżetów');
      } else {
        setError('Nie znaleziono żadnych budżetów w YNAB.');
      }
    } catch (err) {
      handleFailure(err);
    } finally {
      setBudgetsLoading(false);
    }
  };

  const handleSave = async () => {
    setSaving(true);
    setError(null);
    try {
      const saved = await updateYnabSettings({
        api_key: trimmedKey || undefined,
        budget_id: budgetId || undefined
      });
      setSettings(saved);
      setApiKey('');
      toast('Zapisano');
    } catch (err) {
      handleFailure(err);
    } finally {
      setSaving(false);
    }
  };

  const handleImport = async () => {
    if (!budgetId) {
      setError('Wybierz budżet do importu.');
      return;
    }
    if (!hasKey) {
      setError('Dodaj klucz API YNAB przed importem.');
      return;
    }

    setImporting(true);
    setError(null);
    setSummary(null);
    try {
      const result = await importYnabBudget(budgetId, trimmedKey || undefined);
      setSummary(result);
      toast('Zaimportowano dane z YNAB');
      setApiKey('');
      void loadSettings();
    } catch (err) {
      handleFailure(err);
    } finally {
      setImporting(false);
    }
  };

  return (
    <section className="panel-section">
      <div className="panel-section__head">
        <h2 className="panel-section__title">Integracja YNAB</h2>
      </div>
      <div className="account-settings__card">
        {loadingSettings ? (
          <div className="placeholder-glow">
            <div className="placeholder col-6 mb-2" />
            <div className="placeholder col-8" />
          </div>
        ) : (
          <>
            <div className="account-settings__row">
              {settings?.api_key_from_config ? (
                <>
                  <span className="account-field-label">Klucz API</span>
                  <p className="account-hint panel-value">
                    Pochodzi z konfiguracji dodatku w Home Assistant.
                  </p>
                </>
              ) : (
                <>
                  <label className="account-field-label" htmlFor="ynab-api-key-input">
                    Klucz API
                    {settings?.api_key_set && (
                      <span className="account-hint">Wklej nowy, aby nadpisać zapisany.</span>
                    )}
                  </label>
                  <div className="panel-control">
                    <input
                      id="ynab-api-key-input"
                      type={showKey ? 'text' : 'password'}
                      className="form-control"
                      value={apiKey}
                      placeholder={settings?.api_key_set ? 'Klucz zapisany' : 'ynab-api-key-...'}
                      onChange={(event) => setApiKey(event.target.value)}
                    />
                    <button
                      className="account-button panel-icon-button"
                      type="button"
                      onClick={() => setShowKey((prev) => !prev)}
                      aria-label={showKey ? 'Ukryj klucz API' : 'Pokaż klucz API'}
                    >
                      {showKey ? <EyeOff size={14} aria-hidden="true" /> : <Eye size={14} aria-hidden="true" />}
                    </button>
                  </div>
                </>
              )}
            </div>

            <div className="account-settings__row">
              <label className="account-field-label" htmlFor="ynab-budget-select">
                Budżet
                <span className="account-hint">Z niego pobierzemy transakcje.</span>
              </label>
              <div className="panel-stack">
                <div className="panel-control">
                  <select
                    id="ynab-budget-select"
                    className="form-select"
                    value={budgetId}
                    onChange={(event) => setBudgetId(event.target.value)}
                    disabled={budgetsLoading}
                  >
                    <option value="">Wybierz budżet…</option>
                    {savedBudgetMissing && <option value={budgetId}>Zapisany budżet</option>}
                    {budgets.map((budget) => (
                      <option key={budget.id} value={budget.id}>
                        {budget.name}
                      </option>
                    ))}
                  </select>
                  <button
                    className="account-button"
                    type="button"
                    onClick={() => void handleFetchBudgets()}
                    disabled={budgetsLoading}
                  >
                    <RefreshCw size={14} aria-hidden="true" />
                    {budgetsLoading ? 'Pobieram…' : 'Pobierz listę'}
                  </button>
                </div>
                {isDirty && (
                  <div>
                    <button
                      className="account-button"
                      type="button"
                      onClick={() => void handleSave()}
                      disabled={saving}
                    >
                      {saving ? 'Zapisuję…' : 'Zapisz zmiany'}
                    </button>
                  </div>
                )}
              </div>
            </div>

            <div className="account-settings__row">
              <span className="account-field-label">
                Import
                <span className="account-hint">
                  Ostatni: {formatDateTime(settings?.last_imported_at)}
                </span>
              </span>
              <div className="panel-stack">
                <div>
                  <button
                    className="account-button"
                    type="button"
                    onClick={() => void handleImport()}
                    disabled={importing}
                  >
                    <Download size={14} aria-hidden="true" />
                    {importing ? 'Pobieram transakcje…' : 'Pobierz transakcje'}
                  </button>
                </div>
                {summary && (
                  <p className="account-hint panel-numbers">
                    Konta: {summary.accounts}, sklepy: {summary.stores}, kategorie: {summary.categories}.
                    Transakcje: dodano {summary.transactions_inserted}, pominięto{' '}
                    {summary.transactions_skipped}.
                    {summary.stores_categorized
                      ? ` Sklepy z kategorią z historii transakcji: ${summary.stores_categorized}.`
                      : ''}
                  </p>
                )}
              </div>
            </div>
          </>
        )}
        {error && (
          <div className="alert alert-danger mt-3 mb-0" role="alert">
            {error}
          </div>
        )}
      </div>
    </section>
  );
};

export default YnabSection;
