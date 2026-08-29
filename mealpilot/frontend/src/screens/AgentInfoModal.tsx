import { useEffect, useState } from 'react';
import { Icon } from '../components/Icon';
import { apiFetch } from '../data';

interface Props {
  onClose: () => void;
}

interface ToolGroupDTO {
  label: string;
  icon: string;
}

interface JsonSchema {
  properties?: Record<string, unknown>;
  required?: string[];
}

interface AgentToolDTO {
  name: string;
  title: string;
  group: string;
  summary: string;
  description: string;
  input_schema: JsonSchema | null;
  output_schema: JsonSchema | null;
  read_only: boolean;
  destructive: boolean;
  idempotent: boolean;
  confirm: boolean;
  changed: string[];
}

interface AgentToolsDTO {
  groups: ToolGroupDTO[];
  tools: AgentToolDTO[];
}

async function fetchTools(): Promise<AgentToolsDTO> {
  const res = await apiFetch('/agent/tools');
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return (await res.json()) as AgentToolsDTO;
}

function paramsOf(tool: AgentToolDTO): { name: string; required: boolean }[] {
  const schema = tool.input_schema ?? {};
  const required = new Set(schema.required ?? []);
  return Object.keys(schema.properties ?? {}).map((name) => ({
    name,
    required: required.has(name),
  }));
}

const badgeStyle: React.CSSProperties = { fontSize: 10, padding: '1px 7px', flexShrink: 0 };

function ToolCard({ tool }: { tool: AgentToolDTO }) {
  const params = paramsOf(tool);
  return (
    <div
      style={{
        background: 'var(--paper-2)',
        borderRadius: 'var(--r-sm)',
        padding: '10px 12px',
      }}
    >
      <div
        style={{
          display: 'flex',
          alignItems: 'baseline',
          gap: 8,
          marginBottom: 3,
          flexWrap: 'wrap',
        }}
      >
        <span style={{ fontSize: 13, fontWeight: 600 }}>{tool.title}</span>
        <code className="mono" style={{ fontSize: 12, color: 'var(--accent)', flexShrink: 0 }}>
          {tool.name}
        </code>
        {tool.destructive ? (
          <span className="chip terra" style={badgeStyle}>
            usuwa dane
          </span>
        ) : tool.read_only ? (
          <span className="chip olive" style={badgeStyle}>
            tylko odczyt
          </span>
        ) : (
          <span className="chip butter" style={badgeStyle}>
            zapisuje
          </span>
        )}
      </div>
      <div style={{ fontSize: 13, color: 'var(--ink-2)', lineHeight: 1.45 }}>{tool.summary}</div>
      <div
        className="mono"
        style={{ fontSize: 11, color: 'var(--ink-3)', marginTop: 4, lineHeight: 1.5 }}
      >
        {params.length === 0
          ? 'brak parametrów'
          : params.map((p, i) => (
              <span key={p.name}>
                {i > 0 && ', '}
                <span style={p.required ? { color: 'var(--ink-2)', fontWeight: 600 } : undefined}>
                  {p.name}
                </span>
                {p.required && '*'}
              </span>
            ))}
      </div>
    </div>
  );
}

export function AgentInfoModal({ onClose }: Props) {
  const [data, setData] = useState<AgentToolsDTO | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    fetchTools()
      .then((d) => {
        if (cancelled) return;
        setData(d);
        setError(null);
      })
      .catch((e: unknown) => {
        if (cancelled) return;
        setError(e instanceof Error ? e.message : 'Błąd');
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const groups = data
    ? data.groups
        .map((g) => ({ ...g, tools: data.tools.filter((t) => t.group === g.label) }))
        .filter((g) => g.tools.length > 0)
    : [];
  // Tools whose group is missing from `groups` would silently disappear.
  const known = new Set(data?.groups.map((g) => g.label) ?? []);
  const ungrouped = data?.tools.filter((t) => !known.has(t.group)) ?? [];

  return (
    <div className="modal-bg" onClick={onClose}>
      <div
        className="modal"
        style={{ maxWidth: 600, maxHeight: '85vh' }}
        onClick={(e) => e.stopPropagation()}
      >
        <div className="modal-head">
          <div>
            <div className="eyebrow">Asystent AI</div>
            <h2 style={{ margin: 0, fontSize: 18 }}>Co potrafi agent?</h2>
          </div>
          <button className="icon-btn" onClick={onClose} aria-label="Zamknij">
            <Icon name="x" size={16} />
          </button>
        </div>

        <div style={{ overflowY: 'auto', padding: '12px 20px 20px' }}>
          <p style={{ fontSize: 13, color: 'var(--ink-2)', margin: '0 0 16px' }}>
            Agent ma dostęp do poniższych narzędzi, które wywołuje automatycznie w odpowiedzi na
            Twoje wiadomości. Możesz pisać po polsku — agent sam dobierze odpowiednie akcje.
            Gwiazdką (*) oznaczono parametry wymagane.
          </p>

          {loading && (
            <div style={{ color: 'var(--ink-3)', fontSize: 13 }}>Ładowanie narzędzi…</div>
          )}

          {!loading && error && (
            <div className="auth-error">Nie udało się pobrać listy narzędzi: {error}</div>
          )}

          {!loading &&
            !error &&
            groups.map((group) => (
              <div key={group.label} style={{ marginBottom: 20 }}>
                <div
                  className="eyebrow"
                  style={{ marginBottom: 8, display: 'flex', alignItems: 'center', gap: 6 }}
                >
                  <span>{group.icon}</span>
                  <span>{group.label}</span>
                </div>
                <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                  {group.tools.map((tool) => (
                    <ToolCard key={tool.name} tool={tool} />
                  ))}
                </div>
              </div>
            ))}

          {!loading && !error && ungrouped.length > 0 && (
            <div style={{ marginBottom: 20 }}>
              <div
                className="eyebrow"
                style={{ marginBottom: 8, display: 'flex', alignItems: 'center', gap: 6 }}
              >
                <span>🧩</span>
                <span>Pozostałe</span>
              </div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                {ungrouped.map((tool) => (
                  <ToolCard key={tool.name} tool={tool} />
                ))}
              </div>
            </div>
          )}

          {!loading && !error && groups.length === 0 && ungrouped.length === 0 && (
            <div style={{ color: 'var(--ink-3)', fontSize: 13 }}>
              Agent nie ma obecnie żadnych narzędzi.
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
