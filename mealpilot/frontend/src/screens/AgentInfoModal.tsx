import { Icon } from '../components/Icon';
import { TOOLS, GROUP_ORDER, type ToolDef, type ToolGroup } from '../agent/tools';

interface Props {
  onClose: () => void;
}

const GROUPS: { label: ToolGroup; icon: string; tools: ToolDef[] }[] = GROUP_ORDER.map((g) => ({
  label: g.label,
  icon: g.icon,
  tools: TOOLS.filter((t) => t.group === g.label),
})).filter((g) => g.tools.length > 0);

function paramList(names: string[]): string {
  if (!names.length) return 'brak parametrów';
  return names.join(', ');
}

export function AgentInfoModal({ onClose }: Props) {
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
            Agent ma dostęp do poniższych narzędzi, które wywołuje automatycznie w odpowiedzi na Twoje wiadomości.
            Możesz pisać po polsku — agent sam dobierze odpowiednie akcje.
          </p>

          {GROUPS.map((group) => {
            const tools = group.tools;
            return (
              <div key={group.label} style={{ marginBottom: 20 }}>
                <div
                  className="eyebrow"
                  style={{ marginBottom: 8, display: 'flex', alignItems: 'center', gap: 6 }}
                >
                  <span>{group.icon}</span>
                  <span>{group.label}</span>
                </div>
                <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                  {tools.map((tool) => {
                    if (!tool) return null;
                    const params = Object.keys(
                      (tool.parameters as { properties?: Record<string, unknown> }).properties ?? {},
                    );
                    return (
                      <div
                        key={tool.name}
                        style={{
                          background: 'var(--paper-2)',
                          borderRadius: 'var(--r-sm)',
                          padding: '10px 12px',
                        }}
                      >
                        <div style={{ display: 'flex', alignItems: 'baseline', gap: 8, marginBottom: 3 }}>
                          <code
                            className="mono"
                            style={{ fontSize: 12, color: 'var(--accent)', flexShrink: 0 }}
                          >
                            {tool.name}
                          </code>
                          {params.length > 0 && (
                            <span
                              className="mono"
                              style={{ fontSize: 11, color: 'var(--ink-3)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}
                            >
                              ({paramList(params)})
                            </span>
                          )}
                        </div>
                        <div style={{ fontSize: 13, color: 'var(--ink-2)', lineHeight: 1.45 }}>
                          {tool.description}
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
