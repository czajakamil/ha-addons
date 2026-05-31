import { apiFetch } from '../data';

export interface ToolUseDTO {
  id: number;
  tool_use_id: string;
  tool_name: string;
  input: Record<string, unknown>;
  output: unknown;
  is_error: boolean;
  started_at: string;
  finished_at: string | null;
}

export interface MessageDTO {
  id: number;
  role: 'user' | 'assistant';
  content: string;
  created_at: string;
  tool_uses: ToolUseDTO[];
}

export interface ConversationDTO {
  id: number;
  title: string | null;
  model: string;
  created_at: string;
  updated_at: string;
}

export interface ConversationDetailDTO extends ConversationDTO {
  messages: MessageDTO[];
}

export interface ToolUseInput {
  tool_use_id: string;
  tool_name: string;
  input: Record<string, unknown>;
  output: unknown;
  is_error: boolean;
  finished_at?: string;
}

async function ok<T>(res: Response): Promise<T> {
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`${res.status} ${res.statusText}: ${text}`);
  }
  return (await res.json()) as T;
}

export async function listConversations(): Promise<ConversationDTO[]> {
  return ok(await apiFetch('/agent/conversations'));
}

export async function createConversation(model: string): Promise<ConversationDTO> {
  return ok(
    await apiFetch('/agent/conversations', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ model }),
    }),
  );
}

export async function getConversation(id: number): Promise<ConversationDetailDTO> {
  return ok(await apiFetch(`/agent/conversations/${id}`));
}

export async function patchConversation(id: number, title: string): Promise<ConversationDTO> {
  return ok(
    await apiFetch(`/agent/conversations/${id}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ title }),
    }),
  );
}

export async function deleteConversation(id: number): Promise<void> {
  const res = await apiFetch(`/agent/conversations/${id}`, { method: 'DELETE' });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
}

export async function appendMessage(
  convId: number,
  role: 'user' | 'assistant',
  content: string,
  toolUses: ToolUseInput[] = [],
): Promise<MessageDTO> {
  return ok(
    await apiFetch(`/agent/conversations/${convId}/messages`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ role, content, tool_uses: toolUses }),
    }),
  );
}

export async function editMessage(
  msgId: number,
  content: string,
): Promise<ConversationDetailDTO> {
  return ok(
    await apiFetch(`/agent/messages/${msgId}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ content }),
    }),
  );
}

export interface AgentToolEventDTO {
  tool_use_id: string;
  name: string;
  input: Record<string, unknown>;
  output: unknown;
  error: string | null;
}

export interface AgentRunResponseDTO {
  reply: string;
  tool_events: AgentToolEventDTO[];
  changed: string[];
  message_id: number;
  title?: string | null;
}

export async function runAgentOnServer(convId: number): Promise<AgentRunResponseDTO> {
  return ok(
    await apiFetch(`/agent/conversations/${convId}/run`, {
      method: 'POST',
    }),
  );
}

export type AgentSSEEvent =
  | { type: 'text_delta'; text: string }
  | { type: 'tool_start'; tool_use_id: string; name: string; input: Record<string, unknown> }
  | { type: 'tool_result'; tool_use_id: string; output: unknown; is_error: boolean }
  | { type: 'done'; message_id: number; changed: string[]; title?: string | null }
  | { type: 'error'; message: string };

export async function streamAgent(
  convId: number,
  onEvent: (event: AgentSSEEvent) => void,
  signal?: AbortSignal,
): Promise<void> {
  const res = await apiFetch(`/agent/conversations/${convId}/stream`, {
    method: 'POST',
    signal,
  });

  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`${res.status} ${res.statusText}: ${text}`);
  }

  const reader = res.body!.getReader();
  const decoder = new TextDecoder();
  let buffer = '';

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop() ?? '';
      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        const raw = line.slice(6).trim();
        if (!raw) continue;
        try {
          const event = JSON.parse(raw) as AgentSSEEvent;
          onEvent(event);
        } catch {
          // ignore malformed chunk
        }
      }
    }
  } finally {
    reader.releaseLock();
  }
}
