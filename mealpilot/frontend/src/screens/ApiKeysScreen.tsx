import { useEffect, useState } from 'react';
import type { ApiKey, ApiKeyCreated } from '../auth';
import { createApiKey, deleteApiKey, listApiKeys } from '../auth';

export function ApiKeysScreen() {
  const [keys, setKeys] = useState<ApiKey[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  const [newName, setNewName] = useState('');
  const [creating, setCreating] = useState(false);
  const [justCreated, setJustCreated] = useState<ApiKeyCreated | null>(null);
  const [copied, setCopied] = useState(false);

  async function refresh() {
    setLoading(true);
    try {
      setKeys(await listApiKeys());
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Błąd');
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void refresh();
  }, []);

  async function onCreate(e: React.FormEvent) {
    e.preventDefault();
    const name = newName.trim();
    if (!name) return;
    setCreating(true);
    try {
      const created = await createApiKey(name);
      setNewName('');
      setJustCreated(created);
      setCopied(false);
      setError(null);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Błąd');
    } finally {
      setCreating(false);
    }
  }

  async function onDelete(k: ApiKey) {
    if (!window.confirm(`Odwołać klucz "${k.name}"? Ta operacja jest nieodwracalna.`)) return;
    try {
      await deleteApiKey(k.id);
      setError(null);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Błąd');
    }
  }

  async function onCopy() {
    if (!justCreated) return;
    try {
      await navigator.clipboard.writeText(justCreated.key);
      setCopied(true);
    } catch {
      setCopied(false);
    }
  }

  function fmtDate(s: string | null): string {
    if (!s) return '—';
    try {
      return new Date(s).toLocaleString('pl-PL');
    } catch {
      return s;
    }
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 28 }}>
      <header className="page-head">
        <div>
          <div className="eyebrow">Administracja</div>
          <h1>Klucze API</h1>
          <div className="sub">
            Klucze służą do uwierzytelniania agenta MCP i innych integracji w nagłówku
            <code style={{ marginLeft: 6 }}>X-MealPilot-Token</code>.
          </div>
        </div>
      </header>

      {error && <div className="auth-error">{error}</div>}

      <section className="card" style={{ padding: 20 }}>
        <div className="eyebrow" style={{ marginBottom: 12 }}>Nowy klucz</div>
        <form onSubmit={onCreate} className="user-add-form">
          <label className="user-add-field" style={{ flex: '3 1 240px' }}>
            <span className="field-label">Nazwa</span>
            <input
              className="edit-input"
              value={newName}
              onChange={(e) => setNewName(e.target.value)}
              required
              minLength={1}
              maxLength={100}
              placeholder="np. agent-mcp"
            />
          </label>
          <div className="user-add-submit">
            <button className="btn primary" type="submit" disabled={creating || !newName.trim()}>
              {creating ? 'Generowanie…' : 'Wygeneruj klucz'}
            </button>
          </div>
        </form>

        {justCreated && (
          <div
            className="card"
            style={{
              marginTop: 16,
              padding: 16,
              borderColor: 'var(--accent, #c5a572)',
              background: 'var(--bg-2, #f8f4ec)',
            }}
          >
            <div className="eyebrow" style={{ marginBottom: 8 }}>
              Klucz wygenerowany — skopiuj go teraz
            </div>
            <div className="sub" style={{ marginBottom: 10 }}>
              Pokazujemy go tylko raz. Po zamknięciu tej karty zobaczysz tylko prefix.
            </div>
            <div style={{ display: 'flex', gap: 8, alignItems: 'center', flexWrap: 'wrap' }}>
              <code
                style={{
                  flex: 1,
                  minWidth: 240,
                  padding: '8px 12px',
                  background: 'var(--bg-3, #fff)',
                  borderRadius: 6,
                  fontSize: 13,
                  wordBreak: 'break-all',
                }}
              >
                {justCreated.key}
              </code>
              <button className="btn" type="button" onClick={onCopy}>
                {copied ? 'Skopiowano' : 'Kopiuj'}
              </button>
              <button
                className="btn ghost"
                type="button"
                onClick={() => {
                  setJustCreated(null);
                  setCopied(false);
                }}
              >
                Zamknij
              </button>
            </div>
          </div>
        )}
      </section>

      <section className="card" style={{ padding: 20 }}>
        <div className="eyebrow" style={{ marginBottom: 12 }}>Jak podłączyć agenta MCP</div>
        <p style={{ marginBottom: 12, lineHeight: 1.6 }}>
          Serwer MCP dostępny jest przez HTTP/SSE pod adresem <code>/mcp/sse</code>. Wygeneruj
          klucz powyżej, a następnie dodaj wpis do konfiguracji MCP swojego klienta (np. Claude
          Desktop lub VS Code):
        </p>
        <pre
          style={{
            background: 'var(--bg-3, #fff)',
            border: '1px solid var(--border, #e0d8cc)',
            borderRadius: 8,
            padding: '14px 16px',
            fontSize: 12,
            lineHeight: 1.6,
            overflowX: 'auto',
            whiteSpace: 'pre',
          }}
        >{`{
  "mcpServers": {
    "MealPilot": {
      "command": "/opt/homebrew/bin/npx",
      "args": [
        "-y",
        "mcp-remote@latest",
        "http://<adres-HA>:8000/mcp/sse",
        "--allow-http",
        "--transport",
        "sse-only",
        "--header",
        "X-MealPilot-Token:\${MEALPILOT_TOKEN}"
      ],
      "env": {
        "MEALPILOT_TOKEN": "mp_twój_klucz_tutaj"
      }
    }
  }
}`}</pre>
        <ul
          style={{
            marginTop: 14,
            paddingLeft: 20,
            lineHeight: 1.8,
            color: 'var(--ink-2, #555)',
            fontSize: 14,
          }}
        >
          <li>
            <strong>&lt;adres-HA&gt;</strong> — zastąp adresem IP lub nazwą hosta swojego Home
            Assistant, np. <code>192.168.1.10</code>.
          </li>
          <li>
            <strong>MEALPILOT_TOKEN</strong> — wklej wartość klucza skopiowaną zaraz po
            wygenerowaniu (pokazywana tylko raz).
          </li>
          <li>
            Wymagana jest zainstalowana paczka <code>npx</code> (Node.js). Jeśli{' '}
            <code>npx</code> jest w innej lokalizacji, zmień ścieżkę w polu{' '}
            <code>command</code>.
          </li>
        </ul>
      </section>

      <section>
        <div className="eyebrow" style={{ marginBottom: 12 }}>Aktywne klucze</div>
        {loading ? (
          <div style={{ color: 'var(--ink-3)' }}>Ładowanie…</div>
        ) : keys.length === 0 ? (
          <div className="card" style={{ padding: 20, color: 'var(--ink-3)' }}>
            Brak kluczy. Wygeneruj pierwszy powyżej.
          </div>
        ) : (
          <div className="card" style={{ overflow: 'hidden' }}>
            <div className="table-scroll">
            <table className="admin-users-table">
              <thead>
                <tr>
                  <th>Nazwa</th>
                  <th>Prefix</th>
                  <th>Utworzony</th>
                  <th>Ostatnio użyty</th>
                  <th style={{ textAlign: 'right' }}>Akcje</th>
                </tr>
              </thead>
              <tbody>
                {keys.map((k) => (
                  <tr key={k.id}>
                    <td style={{ fontWeight: 500 }}>{k.name}</td>
                    <td>
                      <code style={{ fontSize: 12 }}>{k.prefix}…</code>
                    </td>
                    <td>{fmtDate(k.created_at)}</td>
                    <td>{fmtDate(k.last_used_at)}</td>
                    <td>
                      <div style={{ display: 'flex', gap: 6, justifyContent: 'flex-end' }}>
                        <button className="btn" onClick={() => onDelete(k)}>
                          Odwołaj
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            </div>
          </div>
        )}
      </section>
    </div>
  );
}
