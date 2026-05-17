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
          Serwer MCP działa jako proces <strong>stdio</strong>. Wygeneruj klucz powyżej, a
          następnie dodaj wpis do konfiguracji MCP swojego klienta (np. Claude Desktop lub VS
          Code):
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
    "mealpilot": {
      "command": "python",
      "args": ["/ścieżka/do/backend/mcp_server.py"],
      "env": {
        "MEALPILOT_API_KEY": "mp_twój_klucz_tutaj",
        "MEALPILOT_BASE_URL": "http://localhost:8000"
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
            <strong>MEALPILOT_API_KEY</strong> — wklej wartość klucza skopiowaną zaraz po
            wygenerowaniu (pokazywana tylko raz).
          </li>
          <li>
            <strong>MEALPILOT_BASE_URL</strong> — adres backendu MealPilot; domyślnie{' '}
            <code>http://localhost:8000</code>.
          </li>
          <li>
            Klucz jest przekazywany w nagłówku <code>X-MealPilot-Token</code> przy każdym
            żądaniu.
          </li>
        </ul>
      </section>

      <ApiDocsSection />

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

const API_GROUPS: {
  label: string;
  color: string;
  endpoints: { method: 'GET' | 'POST' | 'PUT' | 'PATCH' | 'DELETE'; path: string; desc: string }[];
}[] = [
  {
    label: 'Przepisy',
    color: '#b45309',
    endpoints: [
      { method: 'GET',    path: '/api/recipes',                      desc: 'Lista przepisów (filtrowanie po tagach, typie posiłku, frazie)' },
      { method: 'POST',   path: '/api/recipes',                      desc: 'Utwórz nowy przepis' },
      { method: 'GET',    path: '/api/recipes/{id}',                 desc: 'Pobierz jeden przepis' },
      { method: 'PUT',    path: '/api/recipes/{id}',                 desc: 'Zaktualizuj przepis' },
      { method: 'DELETE', path: '/api/recipes/{id}',                 desc: 'Usuń przepis' },
      { method: 'POST',   path: '/api/recipes/estimate-macros',      desc: 'Oszacuj makroskładniki na podstawie składników' },
      { method: 'GET',    path: '/api/recipes/meta/tags',            desc: 'Dostępne tagi' },
      { method: 'GET',    path: '/api/recipes/meta/meal_types',      desc: 'Typy posiłków' },
    ],
  },
  {
    label: 'Plan tygodniowy',
    color: '#15803d',
    endpoints: [
      { method: 'GET', path: '/api/plan/{week_start}', desc: 'Pobierz plan na tydzień (format daty: YYYY-MM-DD)' },
      { method: 'PUT', path: '/api/plan/{week_start}', desc: 'Zapisz / nadpisz plan tygodniowy' },
    ],
  },
  {
    label: 'Lista zakupów',
    color: '#0369a1',
    endpoints: [
      { method: 'GET',    path: '/api/shopping/{week_start}',              desc: 'Pobierz listę zakupów na tydzień' },
      { method: 'POST',   path: '/api/shopping/{week_start}/items',        desc: 'Dodaj pozycję do listy' },
      { method: 'PATCH',  path: '/api/shopping/{week_start}/items/{id}',   desc: 'Zaznacz pozycję jako kupioną / edytuj' },
      { method: 'DELETE', path: '/api/shopping/{week_start}',              desc: 'Wyczyść całą listę zakupów' },
    ],
  },
  {
    label: 'Agent AI',
    color: '#7c3aed',
    endpoints: [
      { method: 'POST', path: '/api/agent/conversations',          desc: 'Rozpocznij nową konwersację z agentem' },
      { method: 'GET',  path: '/api/agent/conversations',          desc: 'Lista konwersacji' },
      { method: 'POST', path: '/api/agent/conversations/{id}/run', desc: 'Wyślij wiadomość i uzyskaj odpowiedź' },
      { method: 'GET',  path: '/api/agent/usage',                  desc: 'Stan limitu AI (tokeny, resetowanie)' },
    ],
  },
  {
    label: 'Szablony planów',
    color: '#a16207',
    endpoints: [
      { method: 'GET',  path: '/api/templates',                        desc: 'Lista szablonów tygodniowych' },
      { method: 'POST', path: '/api/templates',                        desc: 'Utwórz szablon z bieżącego planu' },
      { method: 'POST', path: '/api/templates/{id}/apply/{week_start}', desc: 'Zastosuj szablon do tygodnia' },
    ],
  },
];

const METHOD_BG: Record<string, string> = {
  GET:    'var(--accent-soft, #f0ebe3)',
  POST:   '#dcfce7',
  PUT:    '#dbeafe',
  PATCH:  '#ede9fe',
  DELETE: '#fee2e2',
};
const METHOD_COLOR: Record<string, string> = {
  GET:    'var(--accent-deep, #7c4f2b)',
  POST:   '#15803d',
  PUT:    '#1d4ed8',
  PATCH:  '#7c3aed',
  DELETE: '#b91c1c',
};

function ApiDocsSection() {
  const [open, setOpen] = useState(false);

  const exampleCurl = `curl -X GET "http://localhost:8000/api/recipes" \\
  -H "X-MealPilot-Token: mp_twój_klucz"`;

  return (
    <section className="card" style={{ padding: 20 }}>
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        style={{
          all: 'unset',
          cursor: 'pointer',
          display: 'flex',
          width: '100%',
          justifyContent: 'space-between',
          alignItems: 'center',
        }}
      >
        <div className="eyebrow">Co możesz zrobić przez REST API</div>
        <span style={{ fontSize: 12, color: 'var(--ink-3)', userSelect: 'none' }}>
          {open ? '▲ zwiń' : '▼ rozwiń'}
        </span>
      </button>

      {open && (
        <div style={{ marginTop: 16, display: 'flex', flexDirection: 'column', gap: 20 }}>
          <p style={{ fontSize: 14, lineHeight: 1.6, color: 'var(--ink-2)', margin: 0 }}>
            Każde żądanie musi zawierać nagłówek <code>X-MealPilot-Token</code> z wartością
            wygenerowanego klucza. Poniżej znajdziesz przegląd dostępnych grup endpointów.
          </p>

          <pre
            style={{
              background: 'var(--bg-3, #fff)',
              border: '1px solid var(--border, #e0d8cc)',
              borderRadius: 8,
              padding: '12px 16px',
              fontSize: 12,
              lineHeight: 1.6,
              overflowX: 'auto',
              margin: 0,
            }}
          >{exampleCurl}</pre>

          {API_GROUPS.map((group) => (
            <div key={group.label}>
              <div
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: 8,
                  marginBottom: 10,
                }}
              >
                <span
                  style={{
                    width: 10,
                    height: 10,
                    borderRadius: '50%',
                    background: group.color,
                    flexShrink: 0,
                  }}
                />
                <span style={{ fontWeight: 600, fontSize: 14 }}>{group.label}</span>
              </div>
              <div
                style={{
                  display: 'flex',
                  flexDirection: 'column',
                  gap: 4,
                }}
              >
                {group.endpoints.map((ep) => (
                  <div
                    key={ep.method + ep.path}
                    style={{
                      display: 'flex',
                      gap: 10,
                      alignItems: 'baseline',
                      flexWrap: 'wrap',
                      fontSize: 13,
                      lineHeight: 1.5,
                    }}
                  >
                    <span
                      style={{
                        background: METHOD_BG[ep.method],
                        color: METHOD_COLOR[ep.method],
                        borderRadius: 4,
                        padding: '1px 6px',
                        fontFamily: 'monospace',
                        fontSize: 11,
                        fontWeight: 700,
                        flexShrink: 0,
                        minWidth: 52,
                        textAlign: 'center',
                      }}
                    >
                      {ep.method}
                    </span>
                    <code
                      style={{
                        fontSize: 12,
                        color: 'var(--ink-2)',
                        flexShrink: 0,
                      }}
                    >
                      {ep.path}
                    </code>
                    <span style={{ color: 'var(--ink-3)', fontSize: 12 }}>— {ep.desc}</span>
                  </div>
                ))}
              </div>
            </div>
          ))}

          <p style={{ fontSize: 12, color: 'var(--ink-faint)', margin: 0 }}>
            Pełna dokumentacja OpenAPI dostępna pod{' '}
            <code>/docs</code> lub <code>/redoc</code> na adresie backendu.
          </p>
        </div>
      )}
    </section>
  );
}
