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

export async function createConversation(): Promise<ConversationDTO> {
  return ok(
    await apiFetch('/agent/conversations', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({}),
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
