import { useEffect, useRef, useState } from 'react';
import { Icon } from '../components/Icon';
import { getSettings, isConfigured } from '../agent/settings';
import {
  appendMessage,
  createConversation,
  deleteConversation as apiDeleteConv,
  editMessage,
  getConversation,
  listConversations,
  streamAgent,
  type ConversationDTO,
  type ConversationDetailDTO,
  type MessageDTO,
} from '../agent/api';
import {
  emitPlanChanged,
  emitRecipesChanged,
  emitShoppingChanged,
  loadPlan,
  loadRecipes,
  loadShopping,
  currentWeekStart,
} from '../data';
import { AgentInfoModal } from './AgentInfoModal';

const SUGGESTIONS = [
  'Zaplanuj 3 dni vege',
  'Pokaż dostępne tagi i typy posiłków',
  'Plan na ~1800 kcal/dzień',
  'Wygeneruj listę zakupów na ten tydzień',
];

const GREETING =
  'Cześć! Jestem agentem MealPilot. Mam dostęp do Twoich przepisów, planu tygodnia i listy zakupów. Co planujemy?';

interface PendingTool {
  tool_use_id: string;
  name: string;
  input: Record<string, unknown>;
  done: boolean;
  is_error?: boolean;
}

interface StreamingMsg {
  text: string;
  tools: PendingTool[];
}

function formatDate(iso: string): string {
  const d = new Date(iso);
  if (isNaN(d.getTime())) return '';
  return d.toLocaleString('pl-PL', { dateStyle: 'short', timeStyle: 'short' });
}

export function ChatScreen() {
  const [conversations, setConversations] = useState<ConversationDTO[]>([]);
  const [activeId, setActiveId] = useState<number | null>(null);
  const [activeDetail, setActiveDetail] = useState<ConversationDetailDTO | null>(null);
  const [input, setInput] = useState('');
  const [busy, setBusy] = useState(false);
  const [streamingMsg, setStreamingMsg] = useState<StreamingMsg | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [showInfo, setShowInfo] = useState(false);
  const [editingId, setEditingId] = useState<number | null>(null);
  const [editingText, setEditingText] = useState('');
  const [loading, setLoading] = useState(true);
  const [mobileView, setMobileView] = useState<'list' | 'thread'>('list');
  const endRef = useRef<HTMLDivElement | null>(null);
  const inputRef = useRef<HTMLTextAreaElement | null>(null);

  useEffect(() => {
    const el = inputRef.current;
    if (!el) return;
    el.style.height = 'auto';
    const maxH = Math.floor(window.innerHeight / 3);
    el.style.height = `${Math.min(el.scrollHeight, maxH)}px`;
  }, [input]);

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
  }, [activeDetail, streamingMsg]);

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

  const refreshChanged = async (changed: string[]): Promise<void> => {
    const tasks: Promise<unknown>[] = [];
    if (changed.includes('recipes')) {
      tasks.push(loadRecipes().then(() => emitRecipesChanged()));
    }
    if (changed.includes('plan')) {
      tasks.push(loadPlan(currentWeekStart()).then(() => emitPlanChanged()));
    }
    if (changed.includes('shopping')) {
      tasks.push(loadShopping(currentWeekStart()).then(() => emitShoppingChanged()));
    }
    await Promise.all(tasks);
  };

  const runAndPersistAssistant = async (convId: number): Promise<void> => {
    let current: StreamingMsg = { text: '', tools: [] };
    setStreamingMsg(current);

    try {
      await streamAgent(convId, (event) => {
        if (event.type === 'text_delta') {
          current = { ...current, text: current.text + event.text };
          setStreamingMsg({ ...current });
        } else if (event.type === 'tool_start') {
          current = {
            ...current,
            tools: [
              ...current.tools,
              { tool_use_id: event.tool_use_id, name: event.name, input: event.input, done: false },
            ],
          };
          setStreamingMsg({ ...current });
        } else if (event.type === 'tool_result') {
          current = {
            ...current,
            tools: current.tools.map((t) =>
              t.tool_use_id === event.tool_use_id
                ? { ...t, done: true, is_error: event.is_error }
                : t,
            ),
          };
          setStreamingMsg({ ...current });
        } else if (event.type === 'done') {
          const assistantMsg: MessageDTO = {
            id: event.message_id,
            role: 'assistant',
            content: current.text || '(brak odpowiedzi)',
            created_at: new Date().toISOString(),
            tool_uses: current.tools.map((t, idx) => ({
              id: idx,
              tool_use_id: t.tool_use_id,
              tool_name: t.name,
              input: t.input,
              output: null,
              is_error: t.is_error ?? false,
              started_at: new Date().toISOString(),
              finished_at: new Date().toISOString(),
            })),
          };
          setStreamingMsg(null);
          setActiveDetail((prev) =>
            prev && prev.id === convId
              ? {
                  ...prev,
                  title: event.title || prev.title,
                  messages: [...prev.messages, assistantMsg],
                }
              : prev,
          );
          if (event.title) {
            setConversations((prev) =>
              prev.map((c) => (c.id === convId ? { ...c, title: event.title! } : c)),
            );
          }
          void refreshChanged(event.changed);
        } else if (event.type === 'error') {
          throw new Error(event.message);
        }
      });
    } catch (e) {
      setStreamingMsg(null);
      const msg = e instanceof Error ? e.message : String(e);
      setError(msg);
      try {
        const persisted = await appendMessage(convId, 'assistant', `❗ Błąd: ${msg}`);
        setActiveDetail((prev) =>
          prev && prev.id === convId
            ? { ...prev, messages: [...prev.messages, persisted] }
            : prev,
        );
      } catch {
        // swallow secondary error
      }
    }
  };

  const send = async () => {
    if (!input.trim() || busy || activeId === null || !activeDetail) return;
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

      await runAndPersistAssistant(convId);
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
    const convId = activeId;
    const newContent = editingText.trim();
    setBusy(true);
    setError(null);
    try {
      const truncated = await editMessage(editingId, newContent);
      setActiveDetail(truncated);
      setEditingId(null);
      setEditingText('');
      await runAndPersistAssistant(convId);
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
            aria-label="Wróć do listy konwersacji"
            title="Wróć do listy konwersacji"
            type="button"
          >
            <Icon name="chev-l" size={14} />
            <span className="chat-mobile-back-label">Konwersacje</span>
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
            className="btn chat-info-btn"
            onClick={() => setShowInfo(true)}
            title="Co potrafi agent?"
            aria-label="Co potrafi agent?"
            style={{ alignSelf: 'flex-start', gap: 6 }}
          >
            <Icon name="info" size={14} />
            <span className="chat-info-btn-label">Co potrafi?</span>
          </button>
        </div>

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
          {streamingMsg !== null && (
            <div className="bubble bubble-agent">
              <div className="bubble-avatar">
                <span className="serif" style={{ fontStyle: 'italic' }}>m</span>
              </div>
              <div className="bubble-body">
                {streamingMsg.tools.length > 0 && (
                  <div style={{ display: 'flex', flexDirection: 'column', gap: 4, marginBottom: streamingMsg.text ? 8 : 0 }}>
                    {streamingMsg.tools.map((t) => (
                      <span
                        key={t.tool_use_id}
                        className={`tool-chip ${t.done ? (t.is_error ? 'tool-chip-error' : 'tool-chip-done') : 'tool-chip-running'}`}
                      >
                        {t.done ? (t.is_error ? '✗' : '✓') : '⚙'} {t.name}
                      </span>
                    ))}
                  </div>
                )}
                {streamingMsg.text ? (
                  <div className="bubble-text" style={{ whiteSpace: 'pre-wrap' }}>
                    {streamingMsg.text}<span className="cursor-blink" />
                  </div>
                ) : streamingMsg.tools.length === 0 ? (
                  <div className="bubble-text" style={{ display: 'flex', gap: 4 }}>
                    <span className="typing-dot" />
                    <span className="typing-dot" />
                    <span className="typing-dot" />
                  </div>
                ) : null}
              </div>
            </div>
          )}
        </div>

        {!renderedMessages.some((m) => m.role === 'user') && (
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
        )}

        <div className="chat-input">
          <textarea
            ref={inputRef}
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault();
                void send();
              }
            }}
            placeholder="Co gotujemy?"
            rows={2}
            style={{ overflowY: 'auto' }}
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
