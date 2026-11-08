// Połączenia z bankiem przez Enable Banking: klucz aplikacji, zgody, powiązania kont, pobieranie.
import { useEffect, useState } from 'react';
import { Download, ExternalLink, Link2, RefreshCw, Trash2 } from 'lucide-react';
import { toast } from 'sonner';
import {
  completeBankConnection,
  deleteBankConnection,
  deleteBankSyncSettings,
  getAccounts,
  getBankSyncSettings,
  listBankConnections,
  listBanks,
  startBankConnection,
  syncBankConnection,
  updateBankAccountLink,
  updateBankSyncSettings,
  type Account,
  type BankAspsp,
  type BankConnection,
  type BankConnectionAccount,
  type BankSyncResult,
  type BankSyncSettings
} from '../../api';
import { formatSignedMoney } from '../../components/ui/Money';
import { errorDetail, formatDate, formatDateTime } from './format';

type Pending = { connectionId: string; url: string; bank: string };

const syncSummary = (result: BankSyncResult) => {
  const created = result.accounts.reduce((sum, a) => sum + a.created, 0);
  const merged = result.accounts.reduce((sum, a) => sum + a.merged, 0);
  if (!created && !merged) return 'Brak nowych transakcji';
  const parts = [];
  if (created) parts.push(`dodano ${created}`);
  if (merged) parts.push(`połączono z istniejącymi ${merged}`);
  return `Pobrano: ${parts.join(', ')}`;
};

const KeyForm = ({
  settings,
  onSaved,
  onCancel
}: {
  settings: BankSyncSettings | null;
  onSaved: (s: BankSyncSettings) => void;
  onCancel?: () => void;
}) => {
  const [appId, setAppId] = useState(settings?.app_id ?? '');
  const [pem, setPem] = useState('');
  const [fileName, setFileName] = useState('');
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleFile = async (file: File | undefined) => {
    if (!file) return;
    setFileName(file.name);
    setPem(await file.text());
    // Plik z panelu Enable Banking nazywa się `<Application ID>.pem`.
    const stem = file.name.replace(/\.pem$/i, '');
    if (!appId.trim() && /^[0-9a-f-]{36}$/i.test(stem)) setAppId(stem);
  };

  const handleSave = async () => {
    setSaving(true);
    setError(null);
    try {
      const saved = await updateBankSyncSettings(appId.trim(), pem);
      toast('Zapisano klucz Enable Banking');
      onSaved(saved);
    } catch (err) {
      setError(errorDetail(err, 'Nie udało się zapisać klucza.'));
    } finally {
      setSaving(false);
    }
  };

  return (
    <>
      <div className="account-settings__row">
        <label className="account-field-label" htmlFor="eb-app-id">
          Application ID
          <span className="account-hint">Z panelu enablebanking.com, zakładka Aplikacje.</span>
        </label>
        <input
          id="eb-app-id"
          className="form-control"
          value={appId}
          placeholder="np. 3f2c…"
          onChange={(event) => setAppId(event.target.value)}
        />
      </div>
      <div className="account-settings__row">
        <label className="account-field-label" htmlFor="eb-key">
          Klucz prywatny
          <span className="account-hint">Plik .pem pobrany przy rejestracji aplikacji.</span>
        </label>
        <div className="panel-stack">
          <input
            id="eb-key"
            type="file"
            accept=".pem,application/x-pem-file,text/plain"
            className="form-control"
            onChange={(event) => void handleFile(event.target.files?.[0])}
          />
          {fileName && <span className="account-hint">Wybrano {fileName}.</span>}
          <div className="panel-section__actions">
            <button
              className="account-button"
              type="button"
              onClick={() => void handleSave()}
              disabled={saving || !appId.trim() || !pem}
            >
              {saving ? 'Sprawdzam klucz…' : 'Zapisz klucz'}
            </button>
            {onCancel && (
              <button className="account-button" type="button" onClick={onCancel}>
                Anuluj
              </button>
            )}
          </div>
          {error && (
            <div className="alert alert-danger mb-0" role="alert">
              {error}
            </div>
          )}
        </div>
      </div>
    </>
  );
};

const balanceNote = (link: BankConnectionAccount) => {
  if (link.bank_balance_cents == null || link.account_balance_cents == null) return null;
  const currency = link.currency || 'PLN';
  const diff = link.account_balance_cents - link.bank_balance_cents;
  const bank = formatSignedMoney(link.bank_balance_cents, currency);
  if (diff === 0) return `Saldo w banku ${bank} — zgadza się z aplikacją.`;
  return `Saldo w banku ${bank}, w aplikacji ${formatSignedMoney(
    link.account_balance_cents,
    currency
  )} (różnica ${formatSignedMoney(diff, currency)}).`;
};

const AccountLinkRow = ({
  link,
  accounts,
  onChanged
}: {
  link: BankConnectionAccount;
  accounts: Account[];
  onChanged: (connection: BankConnection) => void;
}) => {
  const [saving, setSaving] = useState(false);
  const options = accounts.filter((a) => !link.currency || a.currency === link.currency);
  const label = [link.name, link.iban_masked].filter(Boolean).join(' ') || 'Rachunek';

  const save = async (payload: { account_id?: string | null; sync_from?: string | null }) => {
    setSaving(true);
    try {
      onChanged(await updateBankAccountLink(link.id, payload));
      toast('Zapisano');
    } catch (err) {
      toast.error(errorDetail(err, 'Nie udało się zapisać powiązania.'));
    } finally {
      setSaving(false);
    }
  };

  const note = balanceNote(link);
  return (
    <div className="account-settings__row">
      <label className="account-field-label" htmlFor={`eb-link-${link.id}`}>
        {label}
        <span className="account-hint">
          {link.currency ?? ''}
          {link.last_synced_at ? ` · pobrano ${formatDateTime(link.last_synced_at)}` : ''}
        </span>
      </label>
      <div className="panel-stack">
        <select
          id={`eb-link-${link.id}`}
          className="form-select"
          value={link.account_id ?? ''}
          disabled={saving}
          onChange={(event) => void save({ account_id: event.target.value || null })}
        >
          <option value="">Nie pobieraj</option>
          {options.map((account) => (
            <option key={account.id} value={account.id}>
              {account.name}
            </option>
          ))}
        </select>
        {link.account_id && (
          <>
            <label className="account-hint" htmlFor={`eb-from-${link.id}`}>
              Pobieraj od
            </label>
            <input
              id={`eb-from-${link.id}`}
              type="date"
              className="form-control"
              value={link.sync_from ?? ''}
              disabled={saving}
              onChange={(event) => void save({ sync_from: event.target.value || null })}
            />
            <span className="account-hint">
              {link.sync_from
                ? 'Starszych transakcji nie pobierzemy — są już w aplikacji z wyciągu.'
                : 'Pierwsze pobranie sięga 90 dni wstecz.'}
            </span>
          </>
        )}
        {note && <span className="account-hint panel-numbers">{note}</span>}
      </div>
    </div>
  );
};

const ConnectionCard = ({
  connection,
  accounts,
  busy,
  onChanged,
  onSync,
  onReconnect,
  onDelete
}: {
  connection: BankConnection;
  accounts: Account[];
  busy: boolean;
  onChanged: (connection: BankConnection) => void;
  onSync: () => void;
  onReconnect: () => void;
  onDelete: () => void;
}) => {
  const expired = connection.status === 'expired';
  const linked = connection.accounts.some((a) => a.account_id);
  // Bez rachunków „Pobierz teraz” dopytuje bank o listę (Revolut oddaje ją z opóźnieniem).
  const canSync = linked || connection.accounts.length === 0;
  return (
    <div className="account-settings__card">
      <div className="panel-section__head">
        <span className="panel-table__name">
          {connection.aspsp_name}{' '}
          <span
            className={`panel-badge ${expired ? 'panel-badge--warning' : 'panel-badge--positive'}`}
          >
            {expired ? 'Wygasło' : 'Połączono'}
          </span>
        </span>
        <div className="panel-section__actions">
          {!expired && (
            <button
              className="account-button"
              type="button"
              onClick={onSync}
              disabled={busy || !canSync}
              title={canSync ? undefined : 'Najpierw wybierz konto dla rachunku.'}
            >
              <Download size={14} aria-hidden="true" />
              {busy ? 'Pobieram…' : 'Pobierz teraz'}
            </button>
          )}
          <button className="account-button" type="button" onClick={onReconnect} disabled={busy}>
            <RefreshCw size={14} aria-hidden="true" />
            Połącz ponownie
          </button>
          <button
            className="account-button panel-icon-button"
            type="button"
            onClick={onDelete}
            disabled={busy}
            aria-label={`Odłącz ${connection.aspsp_name}`}
          >
            <Trash2 size={14} aria-hidden="true" />
          </button>
        </div>
      </div>
      <p className="account-hint panel-numbers">
        {connection.valid_until
          ? `Zgoda do ${formatDate(connection.valid_until)}`
          : 'Zgoda aktywna'}
        {' · '}
        Ostatnie pobranie: {formatDateTime(connection.last_synced_at)}
      </p>
      {connection.last_error && (
        <div className="alert alert-warning mb-2" role="alert">
          {connection.last_error}
        </div>
      )}
      {connection.accounts.length === 0 ? (
        <p className="account-hint">
          Bank nie udostępnił jeszcze żadnego rachunku. „Pobierz teraz” zapyta go ponownie.
        </p>
      ) : (
        connection.accounts.map((link) => (
          <AccountLinkRow key={link.id} link={link} accounts={accounts} onChanged={onChanged} />
        ))
      )}
    </div>
  );
};

const BankSyncSection = () => {
  const [settings, setSettings] = useState<BankSyncSettings | null>(null);
  const [connections, setConnections] = useState<BankConnection[]>([]);
  const [accounts, setAccounts] = useState<Account[]>([]);
  const [loading, setLoading] = useState(true);
  const [editingKey, setEditingKey] = useState(false);
  const [banks, setBanks] = useState<BankAspsp[] | null>(null);
  const [bank, setBank] = useState('');
  const [pending, setPending] = useState<Pending | null>(null);
  const [redirect, setRedirect] = useState('');
  const [working, setWorking] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const load = async (isActive: () => boolean = () => true) => {
    try {
      const [loaded, list, accountList] = await Promise.all([
        getBankSyncSettings(),
        listBankConnections(),
        getAccounts()
      ]);
      if (!isActive()) return;
      setSettings(loaded);
      setConnections(list);
      setAccounts(accountList.accounts.filter((a) => !a.is_closed));
    } catch (err) {
      if (isActive()) setError(errorDetail(err, 'Nie udało się wczytać połączeń z bankiem.'));
    } finally {
      if (isActive()) setLoading(false);
    }
  };

  useEffect(() => {
    const active = { current: true };
    void load(() => active.current);
    return () => {
      active.current = false;
    };
  }, []);

  const replace = (updated: BankConnection) =>
    setConnections((list) => list.map((c) => (c.id === updated.id ? updated : c)));

  const loadBanks = async () => {
    setWorking('banks');
    setError(null);
    try {
      setBanks(await listBanks('PL'));
    } catch (err) {
      setError(errorDetail(err, 'Nie udało się pobrać listy banków.'));
    } finally {
      setWorking(null);
    }
  };

  const start = async (name: string) => {
    setWorking('start');
    setError(null);
    try {
      const started = await startBankConnection(name);
      setPending({
        connectionId: started.connection_id,
        url: started.url,
        bank: name
      });
      setRedirect('');
    } catch (err) {
      setError(errorDetail(err, 'Nie udało się zacząć łączenia z bankiem.'));
    } finally {
      setWorking(null);
    }
  };

  const complete = async () => {
    if (!pending) return;
    setWorking('complete');
    setError(null);
    try {
      await completeBankConnection(pending.connectionId, redirect);
      toast(`Połączono z ${pending.bank}`);
      setPending(null);
      setBanks(null);
      setBank('');
      setConnections(await listBankConnections());
    } catch (err) {
      setError(errorDetail(err, 'Nie udało się dokończyć połączenia.'));
    } finally {
      setWorking(null);
    }
  };

  const sync = async (connection: BankConnection) => {
    setWorking(connection.id);
    try {
      const result = await syncBankConnection(connection.id);
      replace(result.connection);
      const failed = result.accounts.find((a) => a.error);
      if (failed) toast.error(failed.error);
      else if (result.connection.last_error) toast.error(result.connection.last_error);
      else if (connection.accounts.length === 0)
        toast(
          result.connection.accounts.length
            ? 'Bank udostępnił rachunki — wybierz dla nich konta.'
            : 'Bank nadal nie udostępnia rachunków. Spróbuj połączyć ponownie.'
        );
      else toast(syncSummary(result));
    } catch (err) {
      toast.error(errorDetail(err, 'Nie udało się pobrać transakcji.'));
      void load();
    } finally {
      setWorking(null);
    }
  };

  const remove = async (connection: BankConnection) => {
    if (
      !window.confirm(
        `Odłączyć ${connection.aspsp_name}? Pobrane transakcje zostaną, ale nowe przestaną spływać.`
      )
    ) {
      return;
    }
    setWorking(connection.id);
    try {
      await deleteBankConnection(connection.id);
      setConnections((list) => list.filter((c) => c.id !== connection.id));
      toast(`Odłączono ${connection.aspsp_name}`);
    } catch (err) {
      toast.error(errorDetail(err, 'Nie udało się odłączyć banku.'));
    } finally {
      setWorking(null);
    }
  };

  const removeKey = async () => {
    if (!window.confirm('Usunąć klucz Enable Banking? Pobieranie z banków się zatrzyma.')) return;
    try {
      await deleteBankSyncSettings();
      setSettings({ configured: false });
      toast('Usunięto klucz');
    } catch (err) {
      toast.error(errorDetail(err, 'Nie udało się usunąć klucza.'));
    }
  };

  const configured = Boolean(settings?.configured);

  return (
    <section className="panel-section">
      <div className="panel-section__head">
        <h2 className="panel-section__title">Połączenia z bankiem</h2>
      </div>

      <div className="account-settings__card">
        {loading ? (
          <div className="placeholder-glow">
            <div className="placeholder col-6 mb-2" />
            <div className="placeholder col-8" />
          </div>
        ) : !configured || editingKey ? (
          <>
            {!configured && (
              <p className="account-hint mb-3">
                Transakcje spływają z banku same, przez Enable Banking (PSD2, bezpłatnie dla
                własnych kont). Załóż aplikację na enablebanking.com, jako Redirect URL wpisz
                dowolny adres, np. https://czajakamil.github.io/ha-addons/eb-callback, i wgraj tu
                jej klucz.
              </p>
            )}
            <KeyForm
              settings={settings}
              onSaved={(saved) => {
                setSettings(saved);
                setEditingKey(false);
              }}
              onCancel={editingKey ? () => setEditingKey(false) : undefined}
            />
          </>
        ) : (
          <>
            <div className="account-settings__row">
              <span className="account-field-label">Aplikacja</span>
              <div className="panel-stack">
                <p className="panel-text panel-break">
                  {settings?.app_name || 'Enable Banking'} · {settings?.app_id}
                </p>
                <span className="account-hint panel-break">
                  Powrót z banku: {settings?.redirect_url}
                </span>
                <div className="panel-section__actions">
                  <button
                    className="account-button"
                    type="button"
                    onClick={() => setEditingKey(true)}
                  >
                    Zmień klucz
                  </button>
                  <button className="account-button" type="button" onClick={() => void removeKey()}>
                    Usuń klucz
                  </button>
                </div>
              </div>
            </div>

            <div className="account-settings__row">
              <label className="account-field-label" htmlFor="eb-bank-select">
                Dodaj bank
                <span className="account-hint">Zalogujesz się w banku i wyrazisz zgodę.</span>
              </label>
              <div className="panel-stack">
                {pending ? (
                  <>
                    <p className="panel-text">
                      1. Otwórz bank i zaloguj się:{' '}
                      <a href={pending.url} target="_blank" rel="noreferrer">
                        {pending.bank} <ExternalLink size={12} aria-hidden="true" />
                      </a>
                    </p>
                    <p className="panel-text">
                      2. Po powrocie strona może się nie wczytać — to normalne. Skopiuj cały adres z
                      paska przeglądarki i wklej go tutaj.
                    </p>
                    <input
                      className="form-control"
                      aria-label="Adres po powrocie z banku"
                      value={redirect}
                      placeholder="https://…?code=…&state=…"
                      onChange={(event) => setRedirect(event.target.value)}
                    />
                    <div className="panel-section__actions">
                      <button
                        className="account-button"
                        type="button"
                        onClick={() => void complete()}
                        disabled={working === 'complete' || !redirect.trim()}
                      >
                        <Link2 size={14} aria-hidden="true" />
                        {working === 'complete' ? 'Łączę…' : 'Dokończ'}
                      </button>
                      <button
                        className="account-button"
                        type="button"
                        onClick={() => setPending(null)}
                      >
                        Anuluj
                      </button>
                    </div>
                  </>
                ) : banks ? (
                  <div className="panel-control">
                    <select
                      id="eb-bank-select"
                      className="form-select"
                      value={bank}
                      onChange={(event) => setBank(event.target.value)}
                    >
                      <option value="">Wybierz bank…</option>
                      {banks.map((b) => (
                        <option key={b.name} value={b.name}>
                          {b.name}
                        </option>
                      ))}
                    </select>
                    <button
                      className="account-button"
                      type="button"
                      onClick={() => void start(bank)}
                      disabled={!bank || working === 'start'}
                    >
                      {working === 'start' ? 'Łączę…' : 'Dalej'}
                    </button>
                  </div>
                ) : (
                  <div>
                    <button
                      className="account-button"
                      type="button"
                      onClick={() => void loadBanks()}
                      disabled={working === 'banks'}
                    >
                      {working === 'banks' ? 'Pobieram listę…' : 'Wybierz bank'}
                    </button>
                  </div>
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

      {connections.map((connection) => (
        <ConnectionCard
          key={connection.id}
          connection={connection}
          accounts={accounts}
          busy={working === connection.id}
          onChanged={replace}
          onSync={() => void sync(connection)}
          onReconnect={() => void start(connection.aspsp_name)}
          onDelete={() => void remove(connection)}
        />
      ))}
      {configured && connections.length > 0 && (
        <p className="account-hint">
          Transakcje pobieramy same co 12 godzin. Każde pobranie widać w historii importów i można
          je cofnąć.
        </p>
      )}
    </section>
  );
};

export default BankSyncSection;
