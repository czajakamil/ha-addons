import { useEffect, useRef, useState } from 'react';
import { Icon } from '../components/Icon';
import { runAgent, type ChatTurn, type ToolEvent } from '../agent/llm';
import { getSettings, isConfigured } from '../agent/settings';
import { TOOLS } from '../agent/tools';
import { AgentInfoModal } from './AgentInfoModal';
import {
  appendMessage,
  createConversation,
  deleteConversation as apiDeleteConv,
  editMessage,
  getConversation,
  listConversations,
  patchConversation,
  type ConversationDTO,
  type ConversationDetailDTO,
  type MessageDTO,
  type ToolUseInput,
} from '../agent/api';

const SUGGESTIONS = [
  'Zaplanuj 3 dni vege',
  'Pokaż dostępne tagi i typy posiłków',
  'Plan na ~1800 kcal/dzień',
  'Wygeneruj listę zakupów na ten tydzień',
];

const GREETING =
  'Cześć! Jestem agentem MealPilot. Mam dostęp do Twoich przepisów, planu tygodnia i listy zakupów. Co planujemy?';

function formatDate(iso: string): string {
  const d = new Date(iso);
  if (isNaN(d.getTime())) return '';
  return d.toLocaleString('pl-PL', { dateStyle: 'short', timeStyle: 'short' });
}

function toToolUseInputs(events: ToolEvent[]): ToolUseInput[] {
  return events.map((ev, idx) => ({
    tool_use_id: ev.toolUseId || `local_${Date.now()}_${idx}`,
    tool_name: ev.name,
    input:
      typeof ev.input === 'object' && ev.input !== null
        ? (ev.input as Record<string, unknown>)
        : { value: ev.input },
    output: ev.error ? ev.error : ev.output ?? null,
    is_error: Boolean(ev.error),
    finished_at: new Date().toISOString(),
  }));
}

function messageToTurn(m: MessageDTO): ChatTurn {
  return { role: m.role, text: m.content };
}

export function ChatScreen() {
  const [conversations, setConversations] = useState<ConversationDTO[]>([]);
  const [activeId, setActiveId] = useState<number | null>(null);
  const [activeDetail, setActiveDetail] = useState<ConversationDetailDTO | null>(null);
  const [input, setInput] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [showInfo, setShowInfo] = useState(false);
  const [editingId, setEditingId] = useState<number | null>(null);
  const [editingText, setEditingText] = useState('');
  const [loading, setLoading] = useState(true);
  const [mobileView, setMobileView] = useState<'list' | 'thread'>('list');
  const endRef = useRef<HTMLDivElement | null>(null);

  const settings = getSettings();
  const configured = isConfigured(settings);

  useEffect(() => {
    void (async () => {
      try {
        const list = await listConversations();
        setConversations(list);
        if (list.length > 0) {
          setActiveId(list[0].id);
        } else {
          const fresh = await createConversation(settings.model);
          setConversations([fresh]);
          setActiveId(fresh.id);
        }
      } catch (e) {
        setError(e instanceof Error ? e.message : String(e));
      } finally {
        setLoading(false);
      }
    })();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    if (activeId === null) {
      setActiveDetail(null);
      return;
    }
    void (async () => {
      try {
        const detail = await getConversation(activeId);
        setActiveDetail(detail);
      } catch (e) {
        setError(e instanceof Error ? e.message : String(e));
      }
    })();
  }, [activeId]);

  useEffect(() => {
    if (endRef.current) endRef.current.scrollTop = endRef.current.scrollHeight;
  }, [activeDetail, busy]);

  const newConvo = async () => {
    setError(null);
    try {
      const fresh = await createConversation(settings.model);
      setConversations((prev) => [fresh, ...prev]);
      setActiveId(fresh.id);
      setInput('');
      setMobileView('thread');
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const runAndPersistAssistant = async (
    convId: number,
    history: ChatTurn[],
  ): Promise<void> => {
    const toolEvents: ToolEvent[] = [];
    try {
      const reply = await runAgent(getSettings(), history, TOOLS, {
        onTool: (ev) => {
          toolEvents.push(ev);
        },
      });
      const persisted = await appendMessage(
        convId,
        'assistant',
        reply || '(brak odpowiedzi)',
        toToolUseInputs(toolEvents),
      );
      setActiveDetail((prev) =>
        prev && prev.id === convId
          ? { ...prev, messages: [...prev.messages, persisted] }
          : prev,
      );
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      setError(msg);
      try {
        const persisted = await appendMessage(
          convId,
          'assistant',
          `❗ Błąd: ${msg}`,
          toToolUseInputs(toolEvents),
        );
        setActiveDetail((prev) =>
          prev && prev.id === convId
            ? { ...prev, messages: [...prev.messages, persisted] }
            : prev,
        );
      } catch {
        // swallow secondary error; the main error is already shown
      }
    }
  };

  const send = async () => {
    if (!input.trim() || busy || activeId === null || !activeDetail) return;
    if (!configured) {
      setError('Dodaj klucz API i model w Ustawieniach.');
      return;
    }
    const userText = input.trim();
    setInput('');
    setBusy(true);
    setError(null);
    const convId = activeId;

    try {
      const persistedUser = await appendMessage(convId, 'user', userText);
      const updated: ConversationDetailDTO = {
        ...activeDetail,
        messages: [...activeDetail.messages, persistedUser],
      };
      setActiveDetail(updated);

      const isFirstMessage = activeDetail.messages.length === 0;
      if (isFirstMessage) {
        try {
          const newTitle = userText.slice(0, 40);
          await patchConversation(convId, newTitle);
          setConversations((prev) =>
            prev.map((c) => (c.id === convId ? { ...c, title: newTitle } : c)),
          );
        } catch {
          // title update is non-critical
        }
      }

      const history: ChatTurn[] = updated.messages.map(messageToTurn);
      await runAndPersistAssistant(convId, history);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const deleteConvo = async (id: number) => {
    try {
      await apiDeleteConv(id);
      const next = conversations.filter((c) => c.id !== id);
      if (next.length === 0) {
        const fresh = await createConversation(settings.model);
        setConversations([fresh]);
        setActiveId(fresh.id);
      } else {
        setConversations(next);
        if (id === activeId) setActiveId(next[0].id);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const startEdit = (m: MessageDTO) => {
    setEditingId(m.id);
    setEditingText(m.content);
  };

  const cancelEdit = () => {
    setEditingId(null);
    setEditingText('');
  };

  const saveEdit = async () => {
    if (editingId === null || !editingText.trim() || busy || activeId === null) return;
    if (!configured) {
      setError('Dodaj klucz API i model w Ustawieniach.');
      return;
    }
    const convId = activeId;
    const newContent = editingText.trim();
    setBusy(true);
    setError(null);
    try {
      const truncated = await editMessage(editingId, newContent);
      setActiveDetail(truncated);
      setEditingId(null);
      setEditingText('');
      const history: ChatTurn[] = truncated.messages.map(messageToTurn);
      await runAndPersistAssistant(convId, history);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const renderedMessages: MessageDTO[] = activeDetail?.messages ?? [];
  const showGreeting = !loading && renderedMessages.length === 0;

  return (
    <div className={`chat-layout chat-mobile-${mobileView}`}>
      <aside className="chat-sidebar">
        <button
          className="btn primary"
          style={{ width: '100%', justifyContent: 'center' }}
          onClick={() => void newConvo()}
        >
          <Icon name="plus" size={14} /> Nowa konwersacja
        </button>
        <div className="eyebrow" style={{ marginTop: 18, marginBottom: 8, paddingLeft: 6 }}>
          Historia
        </div>
        <div className="convo-list">
          {conversations.map((c) => (
            <div
              key={c.id}
              className={`convo-item ${activeId === c.id ? 'active' : ''}`}
              onClick={() => { setActiveId(c.id); setMobileView('thread'); }}
            >
              <div style={{ flex: 1, minWidth: 0 }}>
                <div className="convo-title">{c.title || 'Nowa konwersacja'}</div>
                <div className="convo-date mono">{formatDate(c.updated_at)}</div>
              </div>
              <button
                className="convo-x"
                onClick={(e) => {
                  e.stopPropagation();
                  void deleteConvo(c.id);
                }}
              >
                <Icon name="x" size={11} />
              </button>
            </div>
          ))}
        </div>
      </aside>

      <div className="chat-wrap">
        <div className="page-head">
          <button
            className="btn chat-mobile-back"
            onClick={() => setMobileView('list')}
            aria-label="Wróć do listy"
            type="button"
          >
            <Icon name="x" size={14} /> Konwersacje
          </button>
          <div>
            <div className="eyebrow">
              Asystent · {configured ? settings.model : 'nieskonfigurowany'}
            </div>
            <h1 className="serif" style={{ fontStyle: 'italic' }}>
              {activeDetail?.title || 'Asystent AI'}
            </h1>
            <div className="sub">
              Planowanie tygodnia w języku naturalnym · agent zna twoje przepisy, plan i listę zakupów.
            </div>
          </div>
          <button
            className="btn"
            onClick={() => setShowInfo(true)}
            title="Co potrafi agent?"
            style={{ alignSelf: 'flex-start', gap: 6 }}
          >
            <Icon name="info" size={14} />
            Co potrafi?
          </button>
        </div>

        {!configured && (
          <div className="auth-error" style={{ marginBottom: 12 }}>
            Klucz API nie jest skonfigurowany. Otwórz <strong>Ustawienia</strong> w menu, aby dodać klucz i model.
          </div>
        )}
        {error && <div className="auth-error" style={{ marginBottom: 12 }}>{error}</div>}

        <div className="chat-thread" ref={endRef}>
          {showGreeting && (
            <div className="bubble bubble-agent">
              <div className="bubble-avatar">
                <span className="serif" style={{ fontStyle: 'italic' }}>m</span>
              </div>
              <div className="bubble-body">
                <div className="bubble-text" style={{ whiteSpace: 'pre-wrap' }}>{GREETING}</div>
              </div>
            </div>
          )}
          {renderedMessages.map((m) => {
            const isUser = m.role === 'user';
            const isEditing = editingId === m.id;
            return (
              <div key={m.id} className={`bubble bubble-${isUser ? 'user' : 'agent'}`}>
                {!isUser && (
                  <div className="bubble-avatar">
                    <span className="serif" style={{ fontStyle: 'italic' }}>m</span>
                  </div>
                )}
                <div className="bubble-body">
                  {isEditing ? (
                    <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                      <textarea
                        value={editingText}
                        onChange={(e) => setEditingText(e.target.value)}
                        rows={3}
                        style={{ width: '100%', resize: 'vertical' }}
                        onKeyDown={(e) => {
                          if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) {
                            e.preventDefault();
                            void saveEdit();
                          } else if (e.key === 'Escape') {
                            e.preventDefault();
                            cancelEdit();
                          }
                        }}
                        autoFocus
                      />
                      <div style={{ display: 'flex', gap: 6, justifyContent: 'flex-end' }}>
                        <button className="btn" onClick={cancelEdit} disabled={busy}>
                          Anuluj
                        </button>
                        <button
                          className="btn primary"
                          onClick={() => void saveEdit()}
                          disabled={busy || !editingText.trim()}
                        >
                          Zapisz i wyślij
                        </button>
                      </div>
                    </div>
                  ) : (
                    <>
                      <div className="bubble-text" style={{ whiteSpace: 'pre-wrap' }}>{m.content}</div>
                      {isUser && (
                        <div style={{ marginTop: 4, display: 'flex', justifyContent: 'flex-end' }}>
                          <button
                            className="btn"
                            style={{ padding: '2px 8px', fontSize: 11, gap: 4 }}
                            onClick={() => startEdit(m)}
                            disabled={busy}
                            title="Edytuj wiadomość"
                          >
                            <Icon name="pencil" size={11} /> Edytuj
                          </button>
                        </div>
                      )}
                      {m.tool_uses && m.tool_uses.length > 0 && (
                        <details className="bubble-card" style={{ marginTop: 8 }}>
                          <summary className="eyebrow" style={{ cursor: 'pointer' }}>
                            narzędzia ({m.tool_uses.length})
                          </summary>
                          <div className="mono" style={{ fontSize: 11, color: 'var(--ink-2)', marginTop: 8, lineHeight: 1.5 }}>
                            {m.tool_uses.map((t) => (
                              <div key={t.id} style={{ marginBottom: 6 }}>
                                <div style={{ color: t.is_error ? 'var(--terra, #b34)' : 'var(--ink-2)' }}>
                                  → {t.tool_name}({JSON.stringify(t.input)})
                                </div>
                                {t.is_error && t.output != null && (
                                  <div style={{ paddingLeft: 12, color: 'var(--terra, #b34)' }}>
                                    {typeof t.output === 'string' ? t.output : JSON.stringify(t.output)}
                                  </div>
                                )}
                              </div>
                            ))}
                          </div>
                        </details>
                      )}
                    </>
                  )}
                </div>
              </div>
            );
          })}
          {busy && (
            <div className="bubble bubble-agent">
              <div className="bubble-avatar">
                <span className="serif" style={{ fontStyle: 'italic' }}>m</span>
              </div>
              <div className="bubble-body">
                <div className="bubble-text" style={{ display: 'flex', gap: 4 }}>
                  <span className="typing-dot" />
                  <span className="typing-dot" />
                  <span className="typing-dot" />
                </div>
              </div>
            </div>
          )}
        </div>

        <div className="chat-suggestions">
          {SUGGESTIONS.map((s) => (
            <button
              key={s}
              className="chip"
              style={{ cursor: 'pointer' }}
              onClick={() => setInput(s)}
            >
              {s}
            </button>
          ))}
        </div>

        <div className="chat-input">
          <textarea
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault();
                void send();
              }
            }}
            placeholder="Napisz po polsku — np. „zaplanuj 4 dni z kurczakiem na ~2000 kcal"
            rows={2}
          />
          <button className="btn primary" onClick={() => void send()} disabled={!input.trim() || busy}>
            <Icon name="send" size={14} /> Wyślij
          </button>
        </div>
      </div>

      {showInfo && <AgentInfoModal onClose={() => setShowInfo(false)} />}
    </div>
  );
}
