import test from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:http";
import { once } from "node:events";

import { handleComboChat } from "../../open-sse/services/combo.ts";
import { validateResponseQuality } from "../../open-sse/services/combo/validateQuality.ts";
import {
  resolveClientResponseContract,
  runWithClientResponseContract,
} from "../../open-sse/services/responseContract.ts";
import { applyDirectVisibleTextContract } from "../../src/sse/handlers/chat/visibleTextContract.ts";
import { resetAllCircuitBreakers } from "../../src/shared/utils/circuitBreaker.ts";
import {
  __test as diagnosticTest,
  markRouteSelected,
  markRouteSelectionStarted,
  observeClientResponse,
  startPublicFunnelRequest,
} from "../../src/shared/utils/publicFunnelDiagnostics.ts";

const encoder = new TextEncoder();
const REQUEST_ID = "189c15ca-09e9-4af6-b919-c4654fd06d1b";

test.beforeEach(() => {
  resetAllCircuitBreakers();
  diagnosticTest.clear();
});

function createLog() {
  const entries: unknown[] = [];
  return {
    info: (...args: unknown[]) => entries.push(["info", ...args]),
    warn: (...args: unknown[]) => entries.push(["warn", ...args]),
    error: (...args: unknown[]) => entries.push(["error", ...args]),
    debug: (...args: unknown[]) => entries.push(["debug", ...args]),
    entries,
  };
}

function sseResponse(chunks: Record<string, unknown>[]): Response {
  const frames = [
    ...chunks.map((chunk) => `data: ${JSON.stringify(chunk)}\n\n`),
    "data: [DONE]\n\n",
  ];
  return new Response(
    new ReadableStream<Uint8Array>({
      start(controller) {
        for (const frame of frames) controller.enqueue(encoder.encode(frame));
        controller.close();
      },
    }),
    { status: 200, headers: { "content-type": "text/event-stream" } }
  );
}

function reasoningChunk(finishReason: string | null = null): Record<string, unknown> {
  return {
    id: "fixture",
    object: "chat.completion.chunk",
    choices: [
      {
        delta: { reasoning_content: "opaque-reasoning-fixture" },
        finish_reason: finishReason,
      },
    ],
  };
}

function visibleChunk(content = "OK", finishReason: string | null = null): Record<string, unknown> {
  return {
    id: "fixture",
    object: "chat.completion.chunk",
    choices: [{ delta: { content }, finish_reason: finishReason }],
  };
}

function combo(models: string[]) {
  return {
    name: `visible-contract-${models.join("-")}`,
    strategy: "priority",
    models: models.map((model) => ({ model, weight: 0 })),
    config: { maxRetries: 0, retryDelayMs: 0, fallbackDelayMs: 0 },
  };
}

async function runVisibleCombo(
  models: string[],
  handleSingleModel: (body: Record<string, unknown>, model: string) => Promise<Response>,
  correlationId: string | null = null
): Promise<Response> {
  return runWithClientResponseContract("visible-text", () =>
    handleComboChat({
      body: { stream: true, messages: [{ role: "user", content: "fixture" }] },
      combo: combo(models),
      handleSingleModel,
      isModelAvailable: async () => true,
      log: createLog(),
      settings: null,
      allCombos: null,
      relayOptions: null,
      correlationId,
    })
  );
}

test("response contract header is explicit and unknown values preserve default behavior", () => {
  assert.equal(
    resolveClientResponseContract(new Headers({ "X-Omnia-Response-Contract": "visible-text" })),
    "visible-text"
  );
  assert.equal(
    resolveClientResponseContract(new Headers({ "X-Omnia-Response-Contract": "reasoning" })),
    "default"
  );
  assert.equal(resolveClientResponseContract(new Headers()), "default");
});

test("visible-text combo falls back after a settled reasoning-only target", async () => {
  const calls: string[] = [];
  let active = 0;
  let maxActive = 0;
  const response = await runVisibleCombo(
    ["gemini/reasoning-only", "groq/visible"],
    async (_body, model) => {
      calls.push(model);
      active++;
      maxActive = Math.max(maxActive, active);
      const result =
        model === "gemini/reasoning-only"
          ? sseResponse([reasoningChunk(), reasoningChunk("stop")])
          : sseResponse([visibleChunk("OK"), visibleChunk("", "stop")]);
      active--;
      return result;
    }
  );

  assert.equal(response.ok, true);
  assert.deepEqual(calls, ["gemini/reasoning-only", "groq/visible"]);
  assert.equal(maxActive, 1);
  const text = await response.text();
  assert.match(text, /"content":"OK"/);
  assert.doesNotMatch(text, /reasoning_content/);
  assert.equal(text.match(/data: \[DONE\]/g)?.length, 1);
});

test("reasoning followed by ordinary content succeeds on the same target", async () => {
  const calls: string[] = [];
  const response = await runVisibleCombo(
    ["gemini/reasoning-then-visible", "groq/unused"],
    async (_body, model) => {
      calls.push(model);
      return sseResponse([reasoningChunk(), visibleChunk("OK"), visibleChunk("", "stop")]);
    }
  );

  assert.equal(response.ok, true);
  assert.deepEqual(calls, ["gemini/reasoning-then-visible"]);
  const text = await response.text();
  assert.match(text, /reasoning_content/);
  assert.match(text, /"content":"OK"/);
});

test("reasoning-only combo with no usable target returns a controlled terminal failure", async () => {
  const response = await runVisibleCombo(["gemini/reasoning-a", "gemini/reasoning-b"], async () =>
    sseResponse([reasoningChunk(), reasoningChunk("stop")])
  );

  assert.equal(response.ok, false);
  assert.equal(response.status, 502);
  const json = (await response.json()) as { error?: { message?: string } };
  assert.match(json.error?.message ?? "", /quality|visible/i);
});

test("direct visible-text validation rejects reasoning-only without provider substitution", async () => {
  let providerCalls = 0;
  const response = await runWithClientResponseContract("visible-text", async () => {
    providerCalls++;
    return applyDirectVisibleTextContract(
      sseResponse([reasoningChunk(), reasoningChunk("stop")]),
      {
        isStreaming: true,
        requestId: REQUEST_ID,
        provider: "gemini",
        model: "gemini/direct",
      },
      createLog()
    );
  });

  assert.equal(providerCalls, 1);
  assert.equal(response.status, 502);
  const body = (await response.json()) as { error?: { message?: string } };
  assert.match(body.error?.message ?? "", /user-visible assistant content/i);
});

test("reasoning-capable clients preserve reasoning-only success", async () => {
  const result = await runWithClientResponseContract("default", () =>
    validateResponseQuality(
      sseResponse([reasoningChunk(), reasoningChunk("stop")]),
      true,
      createLog()
    )
  );
  assert.equal(result.valid, true);
});

test("visible-text non-streaming validation rejects reasoning-only and accepts message.content", async () => {
  const reasoningOnly = new Response(
    JSON.stringify({
      choices: [{ message: { content: null, reasoning_content: "opaque" } }],
    }),
    { headers: { "content-type": "application/json" } }
  );
  const visible = new Response(
    JSON.stringify({
      choices: [{ message: { content: "OK", reasoning_content: "opaque" } }],
    }),
    { headers: { "content-type": "application/json" } }
  );

  const invalid = await runWithClientResponseContract("visible-text", () =>
    validateResponseQuality(reasoningOnly, false, createLog())
  );
  const valid = await runWithClientResponseContract("visible-text", () =>
    validateResponseQuality(visible, false, createLog())
  );

  assert.equal(invalid.valid, false);
  assert.equal(invalid.failureCategory, "no_visible_content");
  assert.equal(valid.valid, true);
});

test("visible-text validation rejects empty and tool-only completions", async () => {
  const empty = new Response(JSON.stringify({ choices: [{ message: { content: "   " } }] }), {
    headers: { "content-type": "application/json" },
  });
  const toolOnly = new Response(
    JSON.stringify({
      choices: [
        {
          message: {
            content: null,
            tool_calls: [{ id: "call-1", type: "function", function: { name: "x" } }],
          },
        },
      ],
    }),
    { headers: { "content-type": "application/json" } }
  );

  for (const response of [empty, toolOnly]) {
    const result = await runWithClientResponseContract("visible-text", () =>
      validateResponseQuality(response, false, createLog())
    );
    assert.equal(result.valid, false);
    assert.equal(result.failureCategory, "no_visible_content");
  }
});

test("visible-text validation rejects empty choices and inspects JSON returned to a streaming request", async () => {
  const emptyChoices = new Response(JSON.stringify({ choices: [] }), {
    headers: { "content-type": "application/json" },
  });
  const reasoningJsonForStream = new Response(
    JSON.stringify({
      choices: [{ message: { content: null, reasoning_content: "opaque" } }],
    }),
    { headers: { "content-type": "application/json" } }
  );

  const emptyResult = await runWithClientResponseContract("visible-text", () =>
    validateResponseQuality(emptyChoices, false, createLog())
  );
  const streamingJsonResult = await runWithClientResponseContract("visible-text", () =>
    validateResponseQuality(reasoningJsonForStream, true, createLog())
  );

  assert.equal(emptyResult.failureCategory, "no_visible_content");
  assert.equal(streamingJsonResult.failureCategory, "no_visible_content");
});

test("no-visible fallback keeps one requestId and emits explicit safe diagnostics", async () => {
  const events: Array<Record<string, unknown>> = [];
  diagnosticTest.setSink((event) => events.push(event));
  startPublicFunnelRequest(REQUEST_ID, "public.example.ts.net");
  markRouteSelectionStarted(REQUEST_ID, "combo/fixture");
  markRouteSelected(REQUEST_ID, { strategy: "priority" });

  const response = await runVisibleCombo(
    ["gemini/reasoning", "groq/visible"],
    async (_body, model) =>
      model === "gemini/reasoning"
        ? sseResponse([reasoningChunk(), reasoningChunk("stop")])
        : sseResponse([visibleChunk("OK"), visibleChunk("", "stop")]),
    REQUEST_ID
  );
  assert.equal(response.ok, true);

  const noVisible = events.filter(
    (event) => event.event === "target_completed_without_visible_content"
  );
  assert.equal(noVisible.length, 1);
  assert.equal(noVisible[0].requestId, REQUEST_ID);
  assert.equal(noVisible[0].failureCategory, "no_visible_content");
  assert.equal(JSON.stringify(events).includes("opaque-reasoning-fixture"), false);
});

test("visible-text diagnostics never classify a reasoning-only client stream as success", async () => {
  const events: Array<Record<string, unknown>> = [];
  diagnosticTest.setSink((event) => events.push(event));
  startPublicFunnelRequest(REQUEST_ID, "public.example.ts.net");

  const observed = observeClientResponse(
    sseResponse([reasoningChunk(), reasoningChunk("stop")]),
    REQUEST_ID,
    undefined,
    { visibleTextRequired: true }
  );
  await observed.text();

  assert.equal(events.filter((event) => event.event === "request_completed").length, 0);
  assert.equal(events.filter((event) => event.event === "request_failed").length, 1);
  assert.equal(
    events.some(
      (event) =>
        event.event === "response_output_kind" &&
        event.direction === "client" &&
        event.responseOutputKind === "reasoning"
    ),
    true
  );
  assert.equal(
    events.some((event) => event.event === "response_first_visible_content_sent_to_client"),
    false
  );
});

test("visible content already committed prevents fallback after later stream failure", async () => {
  const calls: string[] = [];
  const response = await runVisibleCombo(
    ["groq/partial", "gemini/must-not-run"],
    async (_body, model) => {
      calls.push(model);
      if (model !== "groq/partial") return sseResponse([visibleChunk("WRONG")]);
      let pull = 0;
      return new Response(
        new ReadableStream<Uint8Array>({
          pull(controller) {
            if (pull++ === 0) {
              controller.enqueue(
                encoder.encode(`data: ${JSON.stringify(visibleChunk("partial"))}\n\n`)
              );
            } else {
              controller.error(new Error("fixture stream reset"));
            }
          },
        }),
        { status: 200, headers: { "content-type": "text/event-stream" } }
      );
    }
  );

  assert.equal(response.ok, true);
  assert.deepEqual(calls, ["groq/partial"]);
  await assert.rejects(response.text(), /fixture stream reset/);
});

test("real local SSE fixture settles reasoning-only POST before visible fallback starts", async (t) => {
  let activePosts = 0;
  let maxActivePosts = 0;
  const paths: string[] = [];
  const server = createServer((request, response) => {
    if (request.method !== "POST") {
      response.writeHead(405).end();
      return;
    }
    paths.push(request.url ?? "");
    activePosts++;
    maxActivePosts = Math.max(maxActivePosts, activePosts);
    let settled = false;
    const settle = () => {
      if (settled) return;
      settled = true;
      activePosts--;
    };
    response.once("finish", settle);
    response.once("close", settle);
    response.writeHead(200, { "content-type": "text/event-stream" });
    if (request.url === "/reasoning") {
      response.write(`data: ${JSON.stringify(reasoningChunk())}\n\n`);
      response.write(`data: ${JSON.stringify(reasoningChunk("stop"))}\n\n`);
    } else {
      response.write(`data: ${JSON.stringify(visibleChunk("OK"))}\n\n`);
      response.write(`data: ${JSON.stringify(visibleChunk("", "stop"))}\n\n`);
    }
    response.end("data: [DONE]\n\n");
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  t.after(() => server.close());
  const address = server.address();
  assert.ok(address && typeof address === "object");
  const baseUrl = `http://127.0.0.1:${address.port}`;

  const result = await runVisibleCombo(["local/reasoning", "local/visible"], async (_body, model) =>
    fetch(`${baseUrl}/${model.endsWith("reasoning") ? "reasoning" : "visible"}`, {
      method: "POST",
      body: "{}",
      headers: { "content-type": "application/json" },
    })
  );
  const text = await result.text();

  assert.deepEqual(paths, ["/reasoning", "/visible"]);
  assert.equal(maxActivePosts, 1);
  assert.equal(activePosts, 0);
  assert.match(text, /"content":"OK"/);
  assert.doesNotMatch(text, /reasoning_content/);
  assert.equal(text.match(/data: \[DONE\]/g)?.length, 1);
});
