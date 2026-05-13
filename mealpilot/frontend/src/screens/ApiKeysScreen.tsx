import { useEffect, useState } from 'react';
import type { ApiKey, ApiKeyCreated } from '../auth';
import { createApiKey, deleteApiKey, listApiKeys } from '../auth';

function CodeBlock({ code, lang }: { code: string; lang?: string }) {
  const [copied, setCopied] = useState(false);
  async function copy() {
    try {
      await navigator.clipboard.writeText(code);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      /* ignore */
    }
  }
  return (
    <div style={{ position: 'relative' }}>
      <pre
        style={{
          background: 'var(--paper-2)',
          border: '1px solid var(--line-soft)',
          borderRadius: 'var(--r)',
          padding: '14px 16px',
          paddingRight: 72,
          fontFamily: 'var(--mono)',
          fontSize: 12,
          lineHeight: 1.7,
          overflowX: 'auto',
          margin: 0,
          color: 'var(--ink)',
        }}
        data-lang={lang}
      >
        {code}
      </pre>
      <button
        type="button"
        className="chip-btn"
        onClick={copy}
        style={{
          position: 'absolute',
          top: 8,
          right: 8,
          padding: '3px 10px',
          borderRadius: 999,
          background: copied ? 'var(--olive-soft)' : 'var(--card)',
          border: '1px solid var(--line-soft)',
          color: copied ? 'oklch(0.38 0.07 115)' : 'var(--ink-3)',
          fontSize: 11,
          fontFamily: 'var(--sans)',
          transition: 'background .2s, color .2s',
        }}
      >
        {copied ? '✓ skopiowano' : 'kopiuj'}
      </button>
    </div>
  );
}

function MethodChip({ method }: { method: string }) {
  const cls =
    method === 'GET' ? 'chip olive' :
    method === 'POST' ? 'chip terra' :
    method === 'PUT' ? 'chip butter' :
    'chip';
  return (
    <span
      className={cls}
      style={{ fontFamily: 'var(--mono)', fontSize: 11, flexShrink: 0, minWidth: 46, justifyContent: 'center' }}
    >
      {method}
    </span>
  );
}

const ENDPOINTS = [
  { method: 'GET',    path: '/api/recipes',                   desc: 'lista przepisów' },
  { method: 'GET',    path: '/api/recipes/{id}',              desc: 'szczegóły przepisu' },
  { method: 'POST',   path: '/api/recipes',                   desc: 'nowy przepis' },
  { method: 'PUT',    path: '/api/recipes/{id}',              desc: 'aktualizacja przepisu' },
  { method: 'GET',    path: '/api/plan/{week_start}',         desc: 'plan tygodnia (format: YYYY-MM-DD)' },
  { method: 'PUT',    path: '/api/plan/{week_start}',         desc: 'zapisz plan tygodnia' },
  { method: 'GET',    path: '/api/shopping/{week_start}',     desc: 'lista zakupów' },
  { method: 'POST',   path: '/api/shopping/{week_start}/generate', desc: 'generuj listę zakupów z planu' },
  { method: 'GET',    path: '/docs',                          desc: 'Swagger UI — interaktywna dokumentacja' },
];

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

  const divider = (
    <div style={{ borderTop: '1px dashed var(--line-soft)', margin: '4px 0' }} />
  );

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 28 }}>
      <header className="page-head">
        <div>
          <div className="eyebrow">Ustawienia</div>
          <h1>Klucze API</h1>
          <div className="sub">
            Uwierzytelniaj zewnętrzne integracje nagłówkiem{' '}
            <code>X-MealPilot-Token</code>.
          </div>
        </div>
      </header>

      {error && <div className="auth-error">{error}</div>}

      {/* ── Tworzenie klucza ── */}
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
              placeholder="np. agent-mcp, integracja-n8n"
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
              borderColor: 'var(--accent)',
              background: 'var(--accent-soft)',
            }}
          >
            <div className="eyebrow" style={{ marginBottom: 8 }}>
              Klucz wygenerowany — skopiuj go teraz
            </div>
            <div style={{ color: 'var(--ink-3)', fontSize: 13, marginBottom: 10 }}>
              Pokazujemy go tylko raz. Po zamknięciu tej karty zobaczysz tylko prefix.
            </div>
            <div style={{ display: 'flex', gap: 8, alignItems: 'center', flexWrap: 'wrap' }}>
              <code
                style={{
                  flex: 1,
                  minWidth: 240,
                  padding: '8px 12px',
                  background: 'var(--card)',
                  border: '1px solid var(--line-soft)',
                  borderRadius: 'var(--r-sm)',
                  fontSize: 13,
                  wordBreak: 'break-all',
                  fontFamily: 'var(--mono)',
                }}
              >
                {justCreated.key}
              </code>
              <button className="btn" type="button" onClick={onCopy}>
                {copied ? '✓ Skopiowano' : 'Kopiuj'}
              </button>
              <button
                className="btn ghost"
                type="button"
                onClick={() => { setJustCreated(null); setCopied(false); }}
              >
                Zamknij
              </button>
            </div>
          </div>
        )}
      </section>

      {/* ── Dokumentacja integracji ── */}
      <section className="card" style={{ padding: 24, display: 'flex', flexDirection: 'column', gap: 20 }}>
        <div className="eyebrow">Integracja z zewnętrznymi programami</div>

        {/* Uwierzytelnianie */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
          <h3 style={{ margin: 0, fontSize: 15 }}>Uwierzytelnianie</h3>
          <p style={{ margin: 0, color: 'var(--ink-2)', fontSize: 13, lineHeight: 1.6 }}>
            Każde żądanie do API wymaga nagłówka HTTP z wygenerowanym kluczem:
          </p>
          <CodeBlock code="X-MealPilot-Token: mp_twój_klucz_tutaj" />
        </div>

        {divider}

        {/* Endpointy */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
          <div style={{ display: 'flex', alignItems: 'baseline', gap: 12 }}>
            <h3 style={{ margin: 0, fontSize: 15 }}>Endpointy</h3>
            <a
              href="/docs"
              target="_blank"
              rel="noreferrer"
              className="chip terra chip-btn"
              style={{
                textDecoration: 'none',
                fontSize: 11,
                padding: '2px 10px',
              }}
            >
              Swagger UI ↗
            </a>
          </div>
          <p style={{ margin: 0, color: 'var(--ink-2)', fontSize: 13, lineHeight: 1.6 }}>
            Adres bazowy: <code style={{ fontFamily: 'var(--mono)', fontSize: 12 }}>http://homeassistant.local:8000</code>{' '}
            (lub adres twojej instancji Home Assistant z portem <code style={{ fontFamily: 'var(--mono)', fontSize: 12 }}>8000</code>).
          </p>
          <div
            style={{
              background: 'var(--paper-2)',
              border: '1px solid var(--line-soft)',
              borderRadius: 'var(--r)',
              overflow: 'hidden',
            }}
          >
            {ENDPOINTS.map((ep, i) => (
              <div
                key={ep.path + ep.method}
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: 12,
                  padding: '9px 14px',
                  borderBottom: i < ENDPOINTS.length - 1 ? '1px dotted var(--line-soft)' : 'none',
                  flexWrap: 'wrap',
                  rowGap: 4,
                }}
              >
                <MethodChip method={ep.method} />
                <code
                  style={{
                    fontFamily: 'var(--mono)',
                    fontSize: 12,
                    color: 'var(--ink)',
                    flex: '1 1 200px',
                  }}
                >
                  {ep.path}
                </code>
                <span style={{ fontSize: 12, color: 'var(--ink-3)', flexShrink: 0 }}>
                  {ep.desc}
                </span>
              </div>
            ))}
          </div>
        </div>

        {divider}

        {/* curl */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
          <h3 style={{ margin: 0, fontSize: 15 }}>curl</h3>
          <CodeBlock
            lang="bash"
            code={`# Pobierz listę przepisów
curl -s \\
  -H "X-MealPilot-Token: mp_twój_klucz" \\
  http://homeassistant.local:8000/api/recipes | python3 -m json.tool

# Utwórz przepis (uproszczony przykład)
curl -s -X POST \\
  -H "X-MealPilot-Token: mp_twój_klucz" \\
  -H "Content-Type: application/json" \\
  -d '{"id":"jajecznica","title":"Jajecznica","servings":1,"ingredients":[],"steps":[],"tags":[],"meal_type":"śniadanie"}' \\
  http://homeassistant.local:8000/api/recipes`}
          />
        </div>

        {divider}

        {/* Python */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
          <h3 style={{ margin: 0, fontSize: 15 }}>Python <span style={{ fontWeight: 400, color: 'var(--ink-3)', fontSize: 13 }}>(biblioteka requests)</span></h3>
          <CodeBlock
            lang="python"
            code={`import requests

BASE = "http://homeassistant.local:8000"
HEADERS = {"X-MealPilot-Token": "mp_twój_klucz"}

# Lista przepisów
recipes = requests.get(f"{BASE}/api/recipes", headers=HEADERS).json()
for r in recipes:
    print(r["id"], r["title"])

# Plan na bieżący tydzień
from datetime import date, timedelta
monday = date.today() - timedelta(days=date.today().weekday())
plan = requests.get(f"{BASE}/api/plan/{monday}", headers=HEADERS).json()

# Lista zakupów
shopping = requests.get(f"{BASE}/api/shopping/{monday}", headers=HEADERS).json()`}
          />
        </div>

        {divider}

        {/* MCP */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
          <h3 style={{ margin: 0, fontSize: 15 }}>
            MCP{' '}
            <span style={{ fontWeight: 400, color: 'var(--ink-3)', fontSize: 13 }}>
              (Claude Desktop, VS Code Copilot, Cursor…)
            </span>
          </h3>
          <p style={{ margin: 0, color: 'var(--ink-2)', fontSize: 13, lineHeight: 1.6 }}>
            Pobierz plik{' '}
            <code style={{ fontFamily: 'var(--mono)', fontSize: 12 }}>mcp_server.py</code>{' '}
            z repozytorium, zainstaluj zależności (<code style={{ fontFamily: 'var(--mono)', fontSize: 12 }}>pip install mcp httpx</code>),
            a następnie dodaj wpis do konfiguracji MCP swojego klienta:
          </p>
          <CodeBlock
            lang="json"
            code={`{
  "mcpServers": {
    "mealpilot": {
      "command": "python",
      "args": ["/ścieżka/do/mcp_server.py"],
      "env": {
        "MEALPILOT_API_KEY": "mp_twój_klucz",
        "MEALPILOT_BASE_URL": "http://homeassistant.local:8000"
      }
    }
  }
}`}
          />
          <ul
            style={{
              margin: 0,
              paddingLeft: 20,
              color: 'var(--ink-2)',
              fontSize: 13,
              lineHeight: 1.8,
            }}
          >
            <li>
              <strong>MEALPILOT_API_KEY</strong> — wklej wartość klucza skopiowaną zaraz po wygenerowaniu.
            </li>
            <li>
              <strong>MEALPILOT_BASE_URL</strong> — adres MealPilot; zamień{' '}
              <code style={{ fontFamily: 'var(--mono)', fontSize: 12 }}>homeassistant.local</code> na adres swojej instancji HA.
            </li>
          </ul>
        </div>
      </section>

      {/* ── Aktywne klucze ── */}
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
                        <code style={{ fontFamily: 'var(--mono)', fontSize: 12 }}>{k.prefix}…</code>
                      </td>
                      <td>{fmtDate(k.created_at)}</td>
                      <td>{fmtDate(k.last_used_at)}</td>
                      <td>
                        <div style={{ display: 'flex', gap: 6, justifyContent: 'flex-end' }}>
                          <button className="btn" onClick={() => void onDelete(k)}>
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
