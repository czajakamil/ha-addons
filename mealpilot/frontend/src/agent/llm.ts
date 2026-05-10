import type { AgentSettings } from './settings';
import type { ToolDef } from './tools';
import { TOOLS_BY_NAME } from './tools';

export interface ChatTurn {
  role: 'user' | 'assistant';
  text: string;
}

export interface ToolEvent {
  kind: 'tool';
  toolUseId: string;
  name: string;
  input: unknown;
  output?: unknown;
  error?: string;
}

export interface AgentRunCallbacks {
  onText?: (text: string) => void;
  onTool?: (ev: ToolEvent) => void;
}

const MAX_STEPS = 10;

function isAnthropicEndpoint(endpoint: string): boolean {
  return endpoint.includes('anthropic.com') || endpoint.includes('/v1/messages');
}

export async function runAgent(
  settings: AgentSettings,
  history: ChatTurn[],
  tools: ToolDef[],
  callbacks: AgentRunCallbacks = {},
): Promise<string> {
  if (isAnthropicEndpoint(settings.endpoint)) {
    return runAnthropic(settings, history, tools, callbacks);
  }
  return runOpenAI(settings, history, tools, callbacks);
}

// ---------------- Anthropic ----------------

interface AnthropicContentBlock {
  type: string;
  text?: string;
  id?: string;
  name?: string;
  input?: unknown;
}

interface AnthropicMessage {
  role: 'user' | 'assistant';
  content: string | AnthropicContentBlock[];
}

async function runAnthropic(
  s: AgentSettings,
  history: ChatTurn[],
  tools: ToolDef[],
  cb: AgentRunCallbacks,
): Promise<string> {
  const messages: AnthropicMessage[] = history.map((h) => ({ role: h.role, content: h.text }));
  const toolDefs = tools.map((t) => ({
    name: t.name,
    description: t.description,
    input_schema: t.parameters,
  }));

  let finalText = '';

  for (let step = 0; step < MAX_STEPS; step++) {
    const res = await fetch(s.endpoint, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': s.apiKey,
        'anthropic-version': '2023-06-01',
        'anthropic-dangerous-direct-browser-access': 'true',
      },
      body: JSON.stringify({
        model: s.model,
        max_tokens: 2048,
        system: s.systemPrompt,
        messages,
        tools: toolDefs,
      }),
    });

    if (!res.ok) {
      const text = await res.text();
      throw new Error(`Anthropic ${res.status}: ${text}`);
    }
    const data = (await res.json()) as {
      content?: AnthropicContentBlock[];
      stop_reason?: string;
      error?: { message?: string } | string;
    };
    if (!data.content) {
      const err =
        typeof data.error === 'string'
          ? data.error
          : data.error?.message ?? JSON.stringify(data);
      throw new Error(`Anthropic: brak pola "content" w odpowiedzi — ${err}`);
    }

    const blocks = data.content;
    const textBlocks = blocks
      .filter((b) => b.type === 'text' && b.text)
      .map((b) => b.text as string);
    if (textBlocks.length) {
      finalText = textBlocks.join('\n').trim();
      cb.onText?.(finalText);
    }

    const toolUses = blocks.filter((b) => b.type === 'tool_use');

    if (data.stop_reason !== 'tool_use' || toolUses.length === 0) {
      return finalText;
    }

    messages.push({ role: 'assistant', content: blocks });

    const toolResults: AnthropicContentBlock[] = [];
    for (const tu of toolUses) {
      const def = TOOLS_BY_NAME.get(tu.name || '');
      let resultText: string;
      let isError = false;
      try {
        if (!def) throw new Error(`Unknown tool: ${tu.name}`);
        const out = await def.handler((tu.input as Record<string, unknown>) || {});
        resultText = JSON.stringify(out ?? null);
        cb.onTool?.({ kind: 'tool', toolUseId: tu.id || '', name: tu.name || '', input: tu.input, output: out });
      } catch (e) {
        isError = true;
        resultText = e instanceof Error ? e.message : String(e);
        cb.onTool?.({ kind: 'tool', toolUseId: tu.id || '', name: tu.name || '', input: tu.input, error: resultText });
      }
      toolResults.push({
        type: 'tool_result',
        // Anthropic expects `tool_use_id`; cast through unknown.
        ...({ tool_use_id: tu.id, content: resultText, is_error: isError } as unknown as object),
      } as AnthropicContentBlock);
    }
    messages.push({ role: 'user', content: toolResults });
  }
  return finalText || '(agent przekroczył limit kroków)';
}

// ---------------- OpenAI ----------------

interface OpenAIToolCall {
  id: string;
  type: 'function';
  function: { name: string; arguments: string };
}

interface OpenAIMessage {
  role: 'system' | 'user' | 'assistant' | 'tool';
  content?: string | null;
  tool_calls?: OpenAIToolCall[];
  tool_call_id?: string;
  name?: string;
}

async function runOpenAI(
  s: AgentSettings,
  history: ChatTurn[],
  tools: ToolDef[],
  cb: AgentRunCallbacks,
): Promise<string> {
  const messages: OpenAIMessage[] = [
    { role: 'system', content: s.systemPrompt },
    ...history.map<OpenAIMessage>((h) => ({ role: h.role, content: h.text })),
  ];
  const toolDefs = tools.map((t) => ({
    type: 'function' as const,
    function: {
      name: t.name,
      description: t.description,
      parameters: t.parameters,
    },
  }));

  let finalText = '';

  for (let step = 0; step < MAX_STEPS; step++) {
    const res = await fetch(s.endpoint, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${s.apiKey}`,
      },
      body: JSON.stringify({
        model: s.model,
        messages,
        tools: toolDefs,
      }),
    });

    if (!res.ok) {
      const text = await res.text();
      throw new Error(`OpenAI ${res.status}: ${text}`);
    }
    const data = (await res.json()) as {
      choices?: { message: OpenAIMessage; finish_reason: string }[];
      error?: { message?: string } | string;
    };
    if (!data.choices || data.choices.length === 0) {
      const err =
        typeof data.error === 'string'
          ? data.error
          : data.error?.message ?? JSON.stringify(data);
      throw new Error(`OpenAI: brak pola "choices" w odpowiedzi — ${err}`);
    }
    const msg = data.choices[0].message;

    if (msg.content) {
      finalText = msg.content.trim();
      cb.onText?.(finalText);
    }

    const calls = msg.tool_calls || [];
    if (calls.length === 0) {
      return finalText;
    }

    messages.push(msg);
    for (const call of calls) {
      const def = TOOLS_BY_NAME.get(call.function.name);
      let resultText: string;
      try {
        if (!def) throw new Error(`Unknown tool: ${call.function.name}`);
        const args = JSON.parse(call.function.arguments || '{}') as Record<string, unknown>;
        const out = await def.handler(args);
        resultText = JSON.stringify(out ?? null);
        cb.onTool?.({ kind: 'tool', toolUseId: call.id, name: call.function.name, input: args, output: out });
      } catch (e) {
        resultText = e instanceof Error ? e.message : String(e);
        cb.onTool?.({ kind: 'tool', toolUseId: call.id, name: call.function.name, input: call.function.arguments, error: resultText });
      }
      messages.push({
        role: 'tool',
        tool_call_id: call.id,
        content: resultText,
      });
    }
  }
  return finalText || '(agent przekroczył limit kroków)';
}
