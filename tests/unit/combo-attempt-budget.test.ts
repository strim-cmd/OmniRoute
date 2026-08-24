import test from "node:test";
import assert from "node:assert/strict";

import {
  ComboAttemptBudgetExceededError,
  ComboAttemptBudgetGuard,
  resolveComboAttemptBudgetProfile,
} from "../../open-sse/services/combo/attemptBudget.ts";
import { handleComboChat } from "../../open-sse/services/combo.ts";
import { runWithClientResponseContract } from "../../open-sse/services/responseContract.ts";
import { resetAllCircuitBreakers } from "../../src/shared/utils/circuitBreaker.ts";

const wait = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
const encoder = new TextEncoder();
const log = { info() {}, warn() {}, error() {}, debug() {} };

test.beforeEach(() => resetAllCircuitBreakers());

function testComboSettings(headersMs = 1000, visibleMs = 1000) {
  return {
    resilienceSettings: {
      comboAttemptBudget: {
        enabled: true,
        recheckIntervalMs: 100,
        default: { responseHeadersMs: headersMs, firstVisibleContentMs: visibleMs },
        providers: {},
        models: {},
      },
    },
  };
}

function visibleResponse(content = "OK") {
  return new Response(
    `data: ${JSON.stringify({ choices: [{ delta: { content } }] })}\n\ndata: [DONE]\n\n`,
    { status: 200, headers: { "content-type": "text/event-stream" } }
  );
}

test("headers budget aborts only after a later target is admissible", async () => {
  const controller = new AbortController();
  let alternateReady = false;
  const events: string[] = [];
  const guard = new ComboAttemptBudgetGuard({
    controller,
    profile: { responseHeadersMs: 20, firstVisibleContentMs: 40 },
    recheckIntervalMs: 10,
    findAdmissibleAlternate: async () =>
      alternateReady ? { targetIndex: 1, provider: "groq", model: "groq/fast" } : null,
    onEvent: (event) => events.push(event.event),
  });

  guard.markRequestHeadersSent();
  await wait(35);
  assert.equal(controller.signal.aborted, false, "the only viable generation must remain alive");
  assert.ok(events.includes("expired_no_alternate"));

  alternateReady = true;
  await wait(25);
  assert.equal(controller.signal.aborted, true);
  assert.ok(controller.signal.reason instanceof ComboAttemptBudgetExceededError);
  assert.equal(guard.getTrigger()?.phase, "response_headers");
  guard.dispose();
});

test("response headers transition to the absolute first-visible budget", async () => {
  const controller = new AbortController();
  const guard = new ComboAttemptBudgetGuard({
    controller,
    profile: { responseHeadersMs: 20, firstVisibleContentMs: 45 },
    recheckIntervalMs: 10,
    findAdmissibleAlternate: async () => ({
      targetIndex: 2,
      provider: "groq",
      model: "groq/fast",
    }),
  });
  guard.markRequestHeadersSent();
  await wait(10);
  guard.markResponseHeaders();
  await wait(45);
  assert.equal(guard.getTrigger()?.phase, "visible_content");
  assert.equal(controller.signal.aborted, true);
});

test("first visible content permanently disarms both budgets", async () => {
  const controller = new AbortController();
  const guard = new ComboAttemptBudgetGuard({
    controller,
    profile: { responseHeadersMs: 15, firstVisibleContentMs: 25 },
    recheckIntervalMs: 5,
    findAdmissibleAlternate: async () => ({
      targetIndex: 1,
      provider: "groq",
      model: "groq/fast",
    }),
  });
  guard.markRequestHeadersSent();
  guard.markResponseHeaders();
  await wait(5);
  guard.markVisibleContent();
  await wait(35);
  assert.equal(controller.signal.aborted, false);
  assert.equal(guard.getTrigger(), null);
});

test("model override wins over provider and default profiles", () => {
  const settings = {
    enabled: true,
    recheckIntervalMs: 500,
    default: { responseHeadersMs: 15000, firstVisibleContentMs: 20000 },
    providers: { gemini: { responseHeadersMs: 12000, firstVisibleContentMs: 15000 } },
    models: {
      "gemini/gemini-3.5-flash": {
        responseHeadersMs: 8000,
        firstVisibleContentMs: 8000,
      },
    },
  };
  assert.deepEqual(
    resolveComboAttemptBudgetProfile(
      settings,
      "Gemini",
      "gemini/gemini-3.5-flash",
      "gemini-3.5-flash"
    ),
    { responseHeadersMs: 8000, firstVisibleContentMs: 8000 }
  );
});

test("combo headers budget settles target A before sequential target B", async () => {
  const calls: string[] = [];
  let active = 0;
  let maxActive = 0;
  let headersObserved = false;
  let abortObserved = false;
  const comboPromise = runWithClientResponseContract("visible-text", () =>
    handleComboChat({
      body: { stream: true, messages: [{ role: "user", content: "fixture" }] },
      combo: {
        name: "budget-headers-fixture",
        strategy: "priority",
        models: [
          { model: "budget-h-a/slow", connectionId: "budget-h-a-fixture" },
          { model: "budget-h-b/fast", connectionId: "budget-h-b-fixture" },
        ],
        config: { maxRetries: 0, fallbackDelayMs: 0 },
      },
      settings: testComboSettings(),
      allCombos: [],
      isModelAvailable: async () => true,
      log,
      handleSingleModel: async (_body, model, target) => {
        calls.push(model);
        active++;
        maxActive = Math.max(maxActive, active);
        target?.upstreamTransportObserver?.onRequestHeadersSent();
        headersObserved = target?.upstreamTransportObserver !== undefined;
        if (model === "budget-h-a/slow") {
          return new Promise<Response>((resolve) => {
            target?.modelAbortSignal?.addEventListener(
              "abort",
              () => {
                abortObserved = true;
                active--;
                resolve(new Response("budget aborted", { status: 504 }));
              },
              { once: true }
            );
          });
        }
        target?.upstreamTransportObserver?.onResponseHeaders(200);
        active--;
        return visibleResponse();
      },
    })
  );
  const response = await Promise.race([
    comboPromise,
    wait(5000).then(() => {
      throw new Error(
        `combo did not settle: calls=${calls.join(",")} active=${active} headersObserved=${headersObserved} abortObserved=${abortObserved}`
      );
    }),
  ]);

  assert.equal(response.ok, true);
  assert.match(await response.text(), /"content":"OK"/);
  assert.deepEqual(calls, ["budget-h-a/slow", "budget-h-b/fast"]);
  assert.equal(maxActive, 1);
});

test("combo visible budget closes reasoning-only A before target B", async () => {
  const calls: string[] = [];
  let active = 0;
  let maxActive = 0;
  const response = await runWithClientResponseContract("visible-text", () =>
    handleComboChat({
      body: { stream: true, messages: [{ role: "user", content: "fixture" }] },
      combo: {
        name: "budget-visible-fixture",
        strategy: "priority",
        models: [
          { model: "budget-v-a/reasoning", connectionId: "budget-v-a-fixture" },
          { model: "budget-v-b/fast", connectionId: "budget-v-b-fixture" },
        ],
        config: { maxRetries: 0, fallbackDelayMs: 0 },
      },
      settings: testComboSettings(),
      allCombos: [],
      isModelAvailable: async () => true,
      log,
      handleSingleModel: async (_body, model, target) => {
        calls.push(model);
        active++;
        maxActive = Math.max(maxActive, active);
        target?.upstreamTransportObserver?.onRequestHeadersSent();
        target?.upstreamTransportObserver?.onResponseHeaders(200);
        if (model === "budget-v-a/reasoning") {
          const signal = target?.modelAbortSignal;
          return new Response(
            new ReadableStream<Uint8Array>({
              start(controller) {
                controller.enqueue(
                  encoder.encode('data: {"choices":[{"delta":{"reasoning_content":"opaque"}}]}\n\n')
                );
                signal?.addEventListener(
                  "abort",
                  () => {
                    active--;
                    controller.error(signal.reason);
                  },
                  { once: true }
                );
              },
            }),
            { status: 200, headers: { "content-type": "text/event-stream" } }
          );
        }
        active--;
        return visibleResponse();
      },
    })
  );

  assert.equal(response.ok, true);
  assert.match(await response.text(), /"content":"OK"/);
  assert.deepEqual(calls, ["budget-v-a/reasoning", "budget-v-b/fast"]);
  assert.equal(maxActive, 1);
});
