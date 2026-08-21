import { AsyncLocalStorage } from "node:async_hooks";

export const OMNIA_RESPONSE_CONTRACT_HEADER = "x-omnia-response-contract";
export const VISIBLE_TEXT_RESPONSE_CONTRACT = "visible-text";

export type ClientResponseContract = "default" | "visible-text";
export type ResponseOutputKind = "visible_text" | "reasoning" | "tool" | "metadata";

const responseContractStore = new AsyncLocalStorage<ClientResponseContract>();

function readHeader(
  headers:
    | Headers
    | Record<string, string | string[] | undefined>
    | { get?: (name: string) => string | null }
    | null
    | undefined,
  name: string
): string {
  if (!headers) return "";
  if (typeof (headers as Headers).get === "function") {
    return String((headers as Headers).get(name) ?? "").trim();
  }
  const record = headers as Record<string, string | string[] | undefined>;
  const value =
    record[name] ??
    record[name.toLowerCase()] ??
    Object.entries(record).find(([key]) => key.toLowerCase() === name.toLowerCase())?.[1];
  return Array.isArray(value) ? String(value[0] ?? "").trim() : String(value ?? "").trim();
}

export function resolveClientResponseContract(
  headers:
    | Headers
    | Record<string, string | string[] | undefined>
    | { get?: (name: string) => string | null }
    | null
    | undefined
): ClientResponseContract {
  return readHeader(headers, OMNIA_RESPONSE_CONTRACT_HEADER).toLowerCase() ===
    VISIBLE_TEXT_RESPONSE_CONTRACT
    ? "visible-text"
    : "default";
}

export function runWithClientResponseContract<T>(
  contract: ClientResponseContract,
  handler: () => T
): T {
  return responseContractStore.run(contract, handler);
}

export function getClientResponseContract(): ClientResponseContract {
  return responseContractStore.getStore() ?? "default";
}

export function requiresVisibleTextResponse(): boolean {
  return getClientResponseContract() === "visible-text";
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === "object" && !Array.isArray(value);
}

function isNonEmptyText(value: unknown): boolean {
  return typeof value === "string" && value.trim().length > 0;
}

function contentHasVisibleText(value: unknown): boolean {
  if (isNonEmptyText(value)) return true;
  if (!Array.isArray(value)) return false;
  return value.some((part) => {
    if (isNonEmptyText(part)) return true;
    if (!isRecord(part)) return false;
    return isNonEmptyText(part.text) || isNonEmptyText(part.output_text);
  });
}

function addOpenAIMessageKinds(kinds: Set<ResponseOutputKind>, value: unknown): void {
  if (!isRecord(value)) return;
  if (contentHasVisibleText(value.content)) kinds.add("visible_text");
  if (
    isNonEmptyText(value.reasoning_content) ||
    isNonEmptyText(value.reasoning_text) ||
    isNonEmptyText(value.reasoning)
  ) {
    kinds.add("reasoning");
  }
  if (
    (Array.isArray(value.tool_calls) && value.tool_calls.length > 0) ||
    isRecord(value.function_call)
  ) {
    kinds.add("tool");
  }
}

function addResponsesKinds(
  kinds: Set<ResponseOutputKind>,
  payload: Record<string, unknown>,
  eventType: string
): void {
  const type = typeof payload.type === "string" ? payload.type : eventType;
  if (
    type === "response.output_text.delta" &&
    (isNonEmptyText(payload.delta) || isNonEmptyText(payload.text))
  ) {
    kinds.add("visible_text");
  } else if (
    (type === "response.reasoning_text.delta" ||
      type === "response.reasoning_summary_text.delta") &&
    (isNonEmptyText(payload.delta) || isNonEmptyText(payload.text))
  ) {
    kinds.add("reasoning");
  } else if (
    type === "response.function_call_arguments.delta" ||
    type === "response.output_item.added" ||
    type === "response.output_item.done"
  ) {
    const item = isRecord(payload.item) ? payload.item : null;
    if (type === "response.function_call_arguments.delta" || item?.type === "function_call") {
      kinds.add("tool");
    }
  }

  const response = isRecord(payload.response) ? payload.response : payload;
  const output = Array.isArray(response.output) ? response.output : [];
  for (const item of output) {
    if (!isRecord(item)) continue;
    if (item.type === "reasoning") kinds.add("reasoning");
    if (item.type === "function_call" || item.type === "computer_call") kinds.add("tool");
    if (item.type !== "message" || !Array.isArray(item.content)) continue;
    for (const part of item.content) {
      if (!isRecord(part)) continue;
      if ((part.type === "output_text" || part.type === "text") && isNonEmptyText(part.text)) {
        kinds.add("visible_text");
      }
      if (part.type === "reasoning_text" && isNonEmptyText(part.text)) {
        kinds.add("reasoning");
      }
    }
  }
}

function addClaudeKinds(kinds: Set<ResponseOutputKind>, payload: Record<string, unknown>): void {
  const block = isRecord(payload.content_block) ? payload.content_block : null;
  const delta = isRecord(payload.delta) ? payload.delta : null;
  if (block?.type === "text" && contentHasVisibleText(block.text)) kinds.add("visible_text");
  if (block?.type === "thinking" || block?.type === "redacted_thinking") kinds.add("reasoning");
  if (block?.type === "tool_use") kinds.add("tool");
  if (delta?.type === "text_delta" && isNonEmptyText(delta.text)) kinds.add("visible_text");
  if (
    delta?.type === "thinking_delta" &&
    (isNonEmptyText(delta.thinking) || isNonEmptyText(delta.text))
  ) {
    kinds.add("reasoning");
  }
  if (delta?.type === "input_json_delta") kinds.add("tool");
}

function addGeminiKinds(kinds: Set<ResponseOutputKind>, payload: Record<string, unknown>): void {
  const candidates = Array.isArray(payload.candidates)
    ? payload.candidates
    : isRecord(payload.response) && Array.isArray(payload.response.candidates)
      ? payload.response.candidates
      : [];
  for (const candidate of candidates) {
    if (!isRecord(candidate) || !isRecord(candidate.content)) continue;
    const parts = Array.isArray(candidate.content.parts) ? candidate.content.parts : [];
    for (const part of parts) {
      if (!isRecord(part)) continue;
      if (part.thought === true && isNonEmptyText(part.text)) kinds.add("reasoning");
      else if (isNonEmptyText(part.text)) kinds.add("visible_text");
      if (isRecord(part.functionCall) || isRecord(part.executableCode)) kinds.add("tool");
    }
  }
}

export function classifyResponseOutputKinds(
  payload: unknown,
  eventType = ""
): ResponseOutputKind[] {
  if (!isRecord(payload)) return ["metadata"];
  const kinds = new Set<ResponseOutputKind>();

  const choices = Array.isArray(payload.choices) ? payload.choices : [];
  for (const choice of choices) {
    if (!isRecord(choice)) continue;
    addOpenAIMessageKinds(kinds, choice.delta);
    addOpenAIMessageKinds(kinds, choice.message);
  }

  addResponsesKinds(kinds, payload, eventType);
  addClaudeKinds(kinds, payload);
  addGeminiKinds(kinds, payload);

  if (kinds.size === 0) kinds.add("metadata");
  return [...kinds];
}

export function responseHasVisibleText(payload: unknown, eventType = ""): boolean {
  return classifyResponseOutputKinds(payload, eventType).includes("visible_text");
}
