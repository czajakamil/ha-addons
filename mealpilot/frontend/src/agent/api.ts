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
}

export async function runAgentOnServer(convId: number): Promise<AgentRunResponseDTO> {
  return ok(
    await apiFetch(`/agent/conversations/${convId}/run`, {
      method: 'POST',
    }),
  );
}

export type StreamEvent =
  | { type: 'token'; text: string }
  | { type: 'tool_call'; name: string; input: Record<string, unknown> }
  | { type: 'tool_done'; name: string; ok: boolean }
  | { type: 'done'; message_id: number; changed: string[]; tokens_used: number; reply: string }
  | { type: 'error'; detail: string; status?: number };

export async function* streamAgentRun(convId: number): AsyncGenerator<StreamEvent> {
  const response = await apiFetch(`/agent/conversations/${convId}/stream`, { method: 'POST' });

  if (!response.ok) {
    const text = await response.text().catch(() => '');
    throw new Error(`${response.status}: ${text}`);
  }

  const reader = response.body!.getReader();
  const decoder = new TextDecoder();
  let buffer = '';

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });

      const parts = buffer.split('\n\n');
      buffer = parts.pop()!;

      for (const part of parts) {
        if (!part.trim()) continue;
        let event = 'message';
        let data = '';
        for (const line of part.split('\n')) {
          if (line.startsWith('event: ')) event = line.slice(7).trim();
          else if (line.startsWith('data: ')) data = line.slice(6);
        }
        if (!data) continue;
        try {
          const payload = JSON.parse(data) as Record<string, unknown>;
          yield { type: event, ...payload } as StreamEvent;
        } catch {
          // ignore malformed events
        }
      }
    }
  } finally {
    reader.cancel().catch(() => {});
  }
}
