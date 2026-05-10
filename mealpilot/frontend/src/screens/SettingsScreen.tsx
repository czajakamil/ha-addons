import { useEffect, useState } from 'react';
import {
  DEFAULT_SYSTEM_PROMPT,
  fetchSettings,
  getSettings,
  isSettingsLoaded,
  persistSettings,
  type AgentSettings,
} from '../agent/settings';

export function SettingsScreen() {
  const [s, setS] = useState<AgentSettings>(() => getSettings());
  const [loading, setLoading] = useState(!isSettingsLoaded());
  const [showKey, setShowKey] = useState(false);
  const [saved, setSaved] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (isSettingsLoaded()) return;
    void (async () => {
      try {
        const fresh = await fetchSettings();
        setS(fresh);
      } catch (e) {
        setError(e instanceof Error ? e.message : String(e));
      } finally {
        setLoading(false);
      }
    })();
  }, []);

  function update<K extends keyof AgentSettings>(k: K, v: AgentSettings[K]) {
    setS((prev) => ({ ...prev, [k]: v }));
    setSaved(false);
  }

  async function onSave(e: React.FormEvent) {
    e.preventDefault();
    setSaving(true);
    setError(null);
    try {
      await persistSettings(s);
      setSaved(true);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setSaving(false);
    }
  }

  function onResetPrompt() {
    update('systemPrompt', DEFAULT_SYSTEM_PROMPT);
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 28 }}>
      <header className="page-head">
        <div>
          <div className="eyebrow">Ustawienia</div>
          <h1>Asystent AI</h1>
          <div className="sub">
            Endpoint, klucz API i model agenta. Ustawienia są zapisywane na serwerze
            i powiązane z Twoim kontem.
          </div>
        </div>
      </header>

      {loading ? (
        <div className="card" style={{ padding: 20 }}>Ładowanie…</div>
      ) : (
      <form onSubmit={onSave} className="card" style={{ padding: 20, display: 'flex', flexDirection: 'column', gap: 16 }}>
        <label>
          <div className="field-label" style={{ marginBottom: 6 }}>
            Endpoint API
          </div>
          <input
            className="edit-input"
            type="url"
            value={s.endpoint}
            onChange={(e) => update('endpoint', e.target.value)}
            placeholder="https://api.example.com/v1/messages"
            autoComplete="off"
          />
        </label>

        <label>
          <div className="field-label" style={{ marginBottom: 6 }}>
            Klucz API{' '}
            <span className="field-hint">zapisany na serwerze</span>
          </div>
          <div className="input-with-affix">
            <input
              className="edit-input"
              type={showKey ? 'text' : 'password'}
              value={s.apiKey}
              onChange={(e) => update('apiKey', e.target.value)}
              placeholder="Klucz API"
              autoComplete="off"
            />
            <button
              type="button"
              className="btn ghost input-affix-btn"
              onClick={() => setShowKey((v) => !v)}
            >
              {showKey ? 'Ukryj' : 'Pokaż'}
            </button>
          </div>
        </label>

        <label>
          <div className="field-label" style={{ marginBottom: 6 }}>
            Model
          </div>
          <input
            className="edit-input"
            value={s.model}
            onChange={(e) => update('model', e.target.value)}
            placeholder="Nazwa modelu"
          />
        </label>

        <label>
          <div className="field-label" style={{ marginBottom: 6 }}>
            System prompt{' '}
            <span className="field-hint">
              instrukcje dla agenta · <button type="button" className="linklike" onClick={onResetPrompt}>przywróć domyślny</button>
            </span>
          </div>
          <textarea
            className="edit-input"
            value={s.systemPrompt}
            onChange={(e) => update('systemPrompt', e.target.value)}
            rows={10}
            style={{ fontFamily: 'inherit' }}
          />
        </label>

        <div style={{ display: 'flex', gap: 10, alignItems: 'center' }}>
          <button className="btn primary" type="submit" disabled={saving}>
            {saving ? 'Zapisywanie…' : 'Zapisz'}
          </button>
          {saved && <span style={{ color: 'var(--ink-3)', fontSize: 13 }}>Zapisano.</span>}
          {error && <span style={{ color: 'var(--terra, #b34)', fontSize: 13 }}>{error}</span>}
        </div>
      </form>
      )}
    </div>
  );
}
