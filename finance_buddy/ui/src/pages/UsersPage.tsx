import { useEffect, useState } from 'react';
import { useApi } from '../hooks/useApi';
import { CopyIcon, DownloadIcon, EyeIcon, EyeOffIcon, RefreshCwIcon, UserPlusIcon } from 'lucide-react';
import {
  createUserInvite,
  deleteUser,
  getYnabSettings,
  getUsers,
  getSession,
  importYnabBudget,
  listYnabBudgets,
  resetUserAccess,
  updateYnabSettings,
  type AuthUser,
  type UserAdmin,
  type UserInviteResponse,
  type YnabBudgetSummary,
  type YnabImportResult,
  type YnabSettings
} from '../api';
import BackupsCard from './panel/BackupsCard';
import ResizableTable from '../components/ui/ResizableTable';

const resolveErrorMessage = (err: unknown, fallback = 'Nie udało się wykonać akcji.') => {
  if (!(err instanceof Error)) return fallback;
  const message = err.message || fallback;
  if (message.trim().startsWith('{')) {
    try {
      const parsed = JSON.parse(message) as { detail?: string };
      if (parsed?.detail) return parsed.detail;
    } catch {
      return message;
    }
  }
  return message;
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
    message: isInvalidKey ? 'Niepoprawny klucz API YNAB.' : detail
  };
};

const UsersPage = () => {
  const { data: currentUser, loading } = useApi<AuthUser | null>(getSession, {
    errorMessage: 'Nie udało się pobrać sesji.'
  });
  const [username, setUsername] = useState('');
  const [displayName, setDisplayName] = useState('');
  const [result, setResult] = useState<UserInviteResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [status, setStatus] = useState<string | null>(null);
  const [users, setUsers] = useState<UserAdmin[]>([]);
  const [usersLoading, setUsersLoading] = useState(false);
  const [usersError, setUsersError] = useState<string | null>(null);
  const [resettingUserId, setResettingUserId] = useState<string | null>(null);
  const [deletingUserId, setDeletingUserId] = useState<string | null>(null);
  const [ynabSettings, setYnabSettings] = useState<YnabSettings | null>(null);
  const [ynabApiKey, setYnabApiKey] = useState('');
  const [ynabShowKey, setYnabShowKey] = useState(false);
  const [ynabBudgets, setYnabBudgets] = useState<YnabBudgetSummary[]>([]);
  const [ynabBudgetId, setYnabBudgetId] = useState('');
  const [ynabLoadingSettings, setYnabLoadingSettings] = useState(false);
  const [ynabBudgetsLoading, setYnabBudgetsLoading] = useState(false);
  const [ynabSaving, setYnabSaving] = useState(false);
  const [ynabImporting, setYnabImporting] = useState(false);
  const [ynabStatus, setYnabStatus] = useState<string | null>(null);
  const [ynabError, setYnabError] = useState<string | null>(null);
  const [ynabSummary, setYnabSummary] = useState<YnabImportResult | null>(null);

  const isAdmin = Boolean(currentUser?.is_admin);

  const loadUsers = async (activeRef?: { current: boolean }) => {
    const isActive = () => (activeRef ? activeRef.current : true);
    if (isActive()) {
      setUsersLoading(true);
      setUsersError(null);
    }
    try {
      const list = await getUsers();
      if (isActive()) setUsers(list);
    } catch (err) {
      if (isActive()) setUsersError(resolveErrorMessage(err, 'Nie udało się pobrać użytkowników.'));
    } finally {
      if (isActive()) setUsersLoading(false);
    }
  };

  const loadYnabSettings = async (activeRef?: { current: boolean }) => {
    const isActive = () => (activeRef ? activeRef.current : true);
    if (isActive()) {
      setYnabLoadingSettings(true);
      setYnabError(null);
    }
    try {
      const settings = await getYnabSettings();
      if (isActive()) {
        setYnabSettings(settings);
        setYnabBudgetId(settings.budget_id ?? '');
        setYnabBudgets([]);
        setYnabSummary(null);
      }
    } catch (err) {
      if (isActive()) setYnabError(resolveYnabError(err).message);
    } finally {
      if (isActive()) setYnabLoadingSettings(false);
    }
  };

  useEffect(() => {
    if (!currentUser) return;
    const activeRef = { current: true };
    void loadYnabSettings(activeRef);
    return () => {
      activeRef.current = false;
    };
  }, [currentUser?.id]);

  useEffect(() => {
    if (!isAdmin) return;
    const activeRef = { current: true };
    void loadUsers(activeRef);
    return () => {
      activeRef.current = false;
    };
  }, [isAdmin]);

  const handleSubmit = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setError(null);
    setStatus('Tworzę zaproszenie…');
    setResult(null);

    try {
      const invite = await createUserInvite({
        username: username.trim(),
        display_name: displayName.trim()
      });
      setResult(invite);
      setStatus('Zaproszenie utworzone.');
      setUsername('');
      setDisplayName('');
      void loadUsers();
    } catch (err) {
      setStatus(null);
      setError(resolveErrorMessage(err, 'Nie udało się utworzyć użytkownika.'));
    }
  };

  const handleCopy = async () => {
    if (!result?.invite_token) return;
    try {
      await navigator.clipboard.writeText(result.invite_token);
      setStatus('Kod zaproszenia skopiowany.');
    } catch {
      setStatus('Nie udało się skopiować kodu.');
    }
  };

  const handleFetchBudgets = async () => {
    const trimmedKey = ynabApiKey.trim();
    if (!trimmedKey && !ynabSettings?.api_key_set) {
      setYnabError('Wklej klucz API YNAB, aby pobrać listę budżetów.');
      return;
    }

    setYnabBudgetsLoading(true);
    setYnabError(null);
    setYnabStatus('Ładuję budżety z YNAB…');

    try {
      const result = await listYnabBudgets(trimmedKey || undefined);
      setYnabBudgets(result);
      if (!result.length) {
        setYnabError('Nie znaleziono żadnych budżetów w YNAB.');
        setYnabStatus(null);
      } else {
        setYnabStatus(`Znaleziono ${result.length} budżetów.`);
      }
    } catch (err) {
      const { message, isInvalidKey } = resolveYnabError(err);
      setYnabStatus(null);
      setYnabError(message);
      if (isInvalidKey) {
        setYnabBudgets([]);
      }
    } finally {
      setYnabBudgetsLoading(false);
    }
  };

  const handleSaveYnabSettings = async () => {
    const trimmedKey = ynabApiKey.trim();
    if (!trimmedKey && !ynabBudgetId) {
      setYnabError('Podaj klucz API lub wybierz budżet.');
      return;
    }

    setYnabSaving(true);
    setYnabError(null);
    setYnabStatus('Zapisuję ustawienia YNAB…');

    try {
      const payload = {
        api_key: trimmedKey || undefined,
        budget_id: ynabBudgetId || undefined
      };
      const settings = await updateYnabSettings(payload);
      setYnabSettings(settings);
      setYnabApiKey('');
      setYnabStatus('Ustawienia zapisane.');
    } catch (err) {
      const { message, isInvalidKey } = resolveYnabError(err);
      setYnabStatus(null);
      setYnabError(message);
      if (isInvalidKey) {
        setYnabBudgets([]);
      }
    } finally {
      setYnabSaving(false);
    }
  };

  const handleImportYnab = async () => {
    if (!ynabBudgetId) {
      setYnabError('Wybierz budżet do importu.');
      return;
    }

    const trimmedKey = ynabApiKey.trim();
    if (!trimmedKey && !ynabSettings?.api_key_set) {
      setYnabError('Dodaj klucz API YNAB przed importem.');
      return;
    }

    setYnabImporting(true);
    setYnabError(null);
    setYnabSummary(null);
    setYnabStatus('Pobieram transakcje z YNAB…');

    try {
      const result = await importYnabBudget(ynabBudgetId, trimmedKey || undefined);
      setYnabSummary(result);
      setYnabStatus('Import zakończony.');
      setYnabApiKey('');
      void loadYnabSettings();
    } catch (err) {
      const { message, isInvalidKey } = resolveYnabError(err);
      setYnabStatus(null);
      setYnabError(message);
      if (isInvalidKey) {
        setYnabBudgets([]);
      }
    } finally {
      setYnabImporting(false);
    }
  };

  const handleResetAccess = async (user: UserAdmin) => {
    const label = user.display_name || user.username || 'użytkownika';
    const confirmed = window.confirm(
      `Zresetować dostęp dla ${label}?\nSpowoduje to usunięcie passkeys i aktywnych sesji.`
    );
    if (!confirmed) return;

    setError(null);
    setStatus('Resetuję dostęp…');
    setResult(null);
    setResettingUserId(user.id);

    try {
      const invite = await resetUserAccess(user.id);
      setResult(invite);
      setStatus('Dostęp zresetowany.');
      void loadUsers();
    } catch (err) {
      setStatus(null);
      setError(resolveErrorMessage(err, 'Nie udało się zresetować dostępu.'));
    } finally {
      setResettingUserId(null);
    }
  };

  const handleDeleteUser = async (user: UserAdmin) => {
    const label = user.display_name || user.username || 'użytkownika';
    const confirmed = window.confirm(
      `Usunąć konto ${label}?\nSpowoduje to usunięcie wszystkich danych użytkownika.`
    );
    if (!confirmed) return;

    setError(null);
    setStatus('Usuwam konto…');
    setResult(null);
    setDeletingUserId(user.id);

    try {
      await deleteUser(user.id);
      setStatus('Konto usunięte.');
      void loadUsers();
    } catch (err) {
      setStatus(null);
      setError(resolveErrorMessage(err, 'Nie udało się usunąć konta.'));
    } finally {
      setDeletingUserId(null);
    }
  };

  if (loading) {
    return (
      <div className="page compact-page">
        <div className="page-shell">
          <div className="page-body">
            <div className="card">
              <div className="card-body">
                <div className="placeholder-glow">
                  <div className="placeholder col-6 mb-2" />
                  <div className="placeholder col-8 mb-2" />
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  }

  if (!currentUser) {
    return (
      <div className="page compact-page">
        <div className="page-shell">
          <div className="page-body">
            <div className="alert alert-danger">
              Brak sesji. Zaloguj się, aby zobaczyć panel użytkownika.
            </div>
          </div>
        </div>
      </div>
    );
  }

  const adminCount = users.reduce((count, user) => (user.is_admin ? count + 1 : count), 0);

  return (
    <div className="page compact-page">
      <div className="page-shell">
        <div className="page-body">
          <div className="card mb-3">
            <div className="card-body">
              <h3 className="card-title mb-1">
                {isAdmin
                  ? currentUser.display_name || currentUser.username || 'Użytkownik'
                  : 'Panel użytkownika'}
              </h3>
              <p className="text-secondary mb-2">
                Zalogowano jako {currentUser.display_name || currentUser.username || 'Użytkownik'}.
              </p>
              <div className="d-flex flex-wrap gap-2">
                <span className="badge bg-azure-lt">
                  Rola: {isAdmin ? 'Administrator' : 'Użytkownik'}
                </span>
                {currentUser.username && (
                  <span className="badge bg-blue-lt">@{currentUser.username}</span>
                )}
              </div>
            </div>
          </div>

          <div className="card mb-3">
            <div className="card-header">
              <h3 className="card-title mb-0">Integracja YNAB</h3>
            </div>
            <div className="card-body">
              {ynabLoadingSettings ? (
                <div className="placeholder-glow">
                  <div className="placeholder col-6 mb-2" />
                  <div className="placeholder col-8 mb-2" />
                  <div className="placeholder col-5" />
                </div>
              ) : (
                <>
                  <div className="mb-3">
                    <label className="form-label" htmlFor="ynab-api-key-input">
                      Klucz API YNAB
                    </label>
                    <div className="input-group">
                      <input
                        id="ynab-api-key-input"
                        type={ynabShowKey ? 'text' : 'password'}
                        className="form-control"
                        value={ynabApiKey}
                        placeholder={
                          ynabSettings?.api_key_set
                            ? 'Klucz zapisany — wklej nowy, aby zmienić'
                            : 'ynab-api-key-...'
                        }
                        onChange={(event) => setYnabApiKey(event.target.value)}
                      />
                      <button
                        className="btn btn-outline-secondary"
                        type="button"
                        onClick={() => setYnabShowKey((prev) => !prev)}
                        aria-label={ynabShowKey ? 'Ukryj klucz API' : 'Pokaż klucz API'}
                      >
                        {ynabShowKey ? <EyeOffIcon size={16} /> : <EyeIcon size={16} />}
                      </button>
                    </div>
                    {ynabSettings?.api_key_set && (
                      <div className="text-secondary small mt-1">
                        Klucz zapisany. Wklej nowy, aby nadpisać.
                      </div>
                    )}
                  </div>

                  <div className="mb-3">
                    <label className="form-label" htmlFor="ynab-budget-select">
                      Budżet
                    </label>
                    <div className="d-flex flex-wrap gap-2">
                      <select
                        id="ynab-budget-select"
                        className="form-select"
                        value={ynabBudgetId}
                        onChange={(event) => setYnabBudgetId(event.target.value)}
                        disabled={ynabBudgetsLoading}
                      >
                        <option value="">Wybierz budżet…</option>
                        {ynabBudgets.map((budget) => (
                          <option key={budget.id} value={budget.id}>
                            {budget.name}
                          </option>
                        ))}
                      </select>
                      <button
                        className="btn btn-outline-primary"
                        type="button"
                        onClick={handleFetchBudgets}
                        disabled={ynabBudgetsLoading}
                      >
                        <RefreshCwIcon size={16} className="me-2" />
                        Pobierz budżety
                      </button>
                      <button
                        className="btn btn-outline-secondary"
                        type="button"
                        onClick={handleSaveYnabSettings}
                        disabled={ynabSaving}
                      >
                        Zapisz ustawienia
                      </button>
                    </div>
                    {ynabSettings?.budget_id && !ynabBudgets.length && (
                      <div className="text-secondary small mt-1">
                        Aktualny budżet: {ynabSettings.budget_id}
                      </div>
                    )}
                  </div>

                  <div className="d-flex flex-wrap align-items-center gap-2">
                    <button
                      className="btn btn-primary"
                      type="button"
                      onClick={handleImportYnab}
                      disabled={ynabImporting}
                    >
                      <DownloadIcon size={18} className="me-2" />
                      Pobierz transakcje
                    </button>
                    {ynabSettings?.last_imported_at && (
                      <span className="text-secondary small">
                        Ostatni import:{' '}
                        {new Date(ynabSettings.last_imported_at).toLocaleString()}
                      </span>
                    )}
                  </div>

                  {ynabSummary && (
                    <div className="mt-3 text-secondary small">
                      Import: konta {ynabSummary.accounts}, sklepy {ynabSummary.stores}, kategorie{' '}
                      {ynabSummary.categories}, transakcje +{ynabSummary.transactions_inserted} /{' '}
                      {ynabSummary.transactions_skipped} pominiętych.
                    </div>
                  )}

                  {(ynabStatus || ynabError) && (
                    <div className={`alert mt-3 ${ynabError ? 'alert-danger' : 'alert-success'}`}>
                      {ynabError ?? ynabStatus}
                    </div>
                  )}
                </>
              )}
            </div>
          </div>

      {isAdmin && (
        <div className="card mb-3">
          <div className="card-header d-flex align-items-center">
            <h3 className="card-title mb-0">Użytkownicy</h3>
            <button
              className="btn btn-outline-primary btn-sm ms-auto"
              type="button"
              onClick={() => void loadUsers()}
              disabled={usersLoading}
            >
              <RefreshCwIcon size={16} className="me-1" />
              Odśwież
            </button>
          </div>
          <div className="card-body p-0">
            {usersError && <div className="alert alert-danger m-3">{usersError}</div>}
            {usersLoading ? (
              <div className="p-3">
                <div className="placeholder-glow">
                  <div className="placeholder col-6 mb-2" />
                  <div className="placeholder col-8 mb-2" />
                  <div className="placeholder col-5" />
                </div>
              </div>
            ) : users.length === 0 ? (
              <div className="p-3 text-secondary">Brak użytkowników do wyświetlenia.</div>
            ) : (
              <div className="table-responsive">
                <ResizableTable tableId="users" className="table table-vcenter mb-0">
                  <thead>
                    <tr>
                      <th>Użytkownik</th>
                      <th>Rola</th>
                      <th>Passkeys</th>
                      <th>Utworzony</th>
                      <th />
                    </tr>
                  </thead>
                  <tbody>
                    {users.map((user) => (
                      <tr key={user.id}>
                        <td>
                          <div className="fw-semibold">
                            {user.display_name || user.username || 'Użytkownik'}
                          </div>
                          {user.username && (
                            <div className="text-secondary small">@{user.username}</div>
                          )}
                        </td>
                        <td>
                          <span className={`badge ${user.is_admin ? 'bg-azure-lt' : 'bg-blue-lt'}`}>
                            {user.is_admin ? 'Administrator' : 'Użytkownik'}
                          </span>
                        </td>
                        <td>{user.passkey_count}</td>
                        <td>{new Date(user.created_at).toLocaleDateString()}</td>
                        <td className="text-end">
                          <div className="d-inline-flex flex-wrap gap-2 justify-content-end">
                            <button
                              className="btn btn-outline-warning btn-sm"
                              type="button"
                              onClick={() => void handleResetAccess(user)}
                              disabled={
                                resettingUserId === user.id || deletingUserId === user.id
                              }
                            >
                              Reset hasła
                            </button>
                            <button
                              className="btn btn-outline-danger btn-sm"
                              type="button"
                              onClick={() => void handleDeleteUser(user)}
                              disabled={
                                deletingUserId === user.id ||
                                user.id === currentUser.id ||
                                (user.is_admin && adminCount <= 1)
                              }
                              title={
                                user.id === currentUser.id
                                  ? 'Nie można usunąć własnego konta.'
                                  : user.is_admin && adminCount <= 1
                                    ? 'Nie można usunąć ostatniego administratora.'
                                    : undefined
                              }
                            >
                              Usuń
                            </button>
                          </div>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </ResizableTable>
              </div>
            )}
          </div>
        </div>
      )}

      {isAdmin && <BackupsCard />}

      {isAdmin ? (
        <div className="card">
          <div className="card-header">
            <h3 className="card-title mb-0">Dodaj użytkownika</h3>
          </div>
          <form className="card-body" onSubmit={handleSubmit}>
            <div className="mb-3">
              <label className="form-label">Username</label>
              <input
                className="form-control"
                value={username}
                onChange={(event) => setUsername(event.target.value)}
                required
              />
            </div>
            <div className="mb-3">
              <label className="form-label">Nazwa wyświetlana</label>
              <input
                className="form-control"
                value={displayName}
                onChange={(event) => setDisplayName(event.target.value)}
                required
              />
            </div>
            <button
              className="btn btn-primary"
              type="submit"
              disabled={!username.trim() || !displayName.trim()}
            >
              <UserPlusIcon size={18} className="me-2" />
              Utwórz zaproszenie
            </button>
          </form>
        </div>
      ) : (
        <div className="alert alert-info">
          Tylko administrator może dodawać nowych użytkowników. Poproś admina o zaproszenie.
        </div>
      )}

      {result && (
        <div className="card mt-3">
          <div className="card-body">
            <h4 className="card-title">Kod zaproszenia</h4>
            <div className="text-secondary small mb-2">
              Dla: {result.user.display_name || result.user.username || 'Użytkownika'}
            </div>
            <div className="d-flex flex-column gap-2">
              <code className="text-break">{result.invite_token}</code>
              <div className="text-secondary small">
                Ważny do: {new Date(result.invite_expires_at).toLocaleString()}
              </div>
              <button className="btn btn-outline-primary" type="button" onClick={handleCopy}>
                <CopyIcon size={16} className="me-2" />
                Skopiuj
              </button>
            </div>
          </div>
        </div>
      )}

      {(status || error) && (
        <div className={`alert mt-3 ${error ? 'alert-danger' : 'alert-success'}`}>
          {error ?? status}
        </div>
      )}
        </div>
      </div>
    </div>
  );
};

export default UsersPage;
