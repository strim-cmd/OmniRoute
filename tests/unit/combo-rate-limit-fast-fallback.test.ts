import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const TEST_DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), "omniroute-combo-fast-fallback-"));
const ORIGINAL_DATA_DIR = process.env.DATA_DIR;
process.env.DATA_DIR = TEST_DATA_DIR;

const { handleComboChat } = await import("../../open-sse/services/combo.ts");
const rateLimitManager = await import("../../open-sse/services/rateLimitManager.ts");
const { clearAllModelLockouts } = await import("../../open-sse/services/accountFallback.ts");
const { resetAllComboMetrics } = await import("../../open-sse/services/comboMetrics.ts");
const { resetAllCircuitBreakers } = await import("../../src/shared/utils/circuitBreaker.ts");
const { resetAll: resetAllSemaphores } =
  await import("../../open-sse/services/rateLimitSemaphore.ts");
const diagnostics = await import("../../src/shared/utils/publicFunnelDiagnostics.ts");
const core = await import("../../src/lib/db/core.ts");

const noop = () => {};
const log = { info: noop, warn: noop, debug: noop, error: noop };

type Target = { provider: string; model: string; connectionId: string };

function comboOf(name: string, targets: Target[]) {
  return {
    name,
    strategy: "priority",
    models: targets.map((target) => ({
      model: `${target.provider}/${target.model}`,
      connectionId: target.connectionId,
    })),
    config: {
      maxRetries: 0,
      maxSetRetries: 3,
      setRetryDelayMs: 2000,
      retryDelayMs: 0,
      fallbackDelayMs: 0,
    },
  };
}

function settings(waitOverrides: Record<string, unknown> = {}) {
  return {
    resilienceSettings: {
      comboCooldownWait: {
        enabled: true,
        maxWaitMs: 5000,
        maxAttempts: 1,
        budgetMs: 5000,
        ...waitOverrides,
      },
    },
    modelLockout: { enabled: false },
  };
}

function okResponse(model: string) {
  return new Response(JSON.stringify({ choices: [{ message: { content: `ok:${model}` } }] }), {
    status: 200,
    headers: { "content-type": "application/json" },
  });
}

function rateLimitedResponse(connectionId: string, waitSeconds = 40) {
  return new Response(JSON.stringify({ error: { message: "rate limited" } }), {
    status: 429,
    headers: {
      "content-type": "application/json",
      "retry-after": `${waitSeconds}s`,
      "X-OmniRoute-Selected-Connection-Id": connectionId,
    },
  });
}

function markCooldown(target: Target, waitSeconds: number) {
  rateLimitManager.enableRateLimitProtection(target.connectionId);
  rateLimitManager.updateFromHeaders(
    target.provider,
    target.connectionId,
    { "retry-after": `${waitSeconds}s` },
    429,
    target.model
  );
}

async function dispatch(
  name: string,
  targets: Target[],
  handleSingleModel: (body: unknown, model: string, options?: unknown) => Promise<Response>,
  options: {
    signal?: AbortSignal;
    correlationId?: string;
    waitOverrides?: Record<string, unknown>;
  } = {}
) {
  return handleComboChat({
    body: { model: name, messages: [{ role: "user", content: "test" }] },
    combo: comboOf(name, targets),
    handleSingleModel,
    isModelAvailable: async () => true,
    log,
    settings: settings(options.waitOverrides),
    allCombos: null,
    signal: options.signal,
    correlationId: options.correlationId,
  });
}

test.beforeEach(async () => {
  await rateLimitManager.__resetRateLimitManagerForTests();
  clearAllModelLockouts();
  resetAllComboMetrics();
  resetAllCircuitBreakers();
  resetAllSemaphores();
  diagnostics.__test.clear();
});

test.after(async () => {
  await rateLimitManager.__resetRateLimitManagerForTests();
  clearAllModelLockouts();
  resetAllComboMetrics();
  resetAllCircuitBreakers();
  resetAllSemaphores();
  diagnostics.__test.clear();
  core.resetDbInstance();
  fs.rmSync(TEST_DATA_DIR, { recursive: true, force: true });
  if (ORIGINAL_DATA_DIR === undefined) delete process.env.DATA_DIR;
  else process.env.DATA_DIR = ORIGINAL_DATA_DIR;
});

test("A: known 40s cooldown is skipped and the ready second target starts immediately", async () => {
  const gemini = { provider: "gemini", model: "gemini-a", connectionId: "conn-a" };
  const groq = { provider: "groq", model: "groq-ready", connectionId: "conn-b" };
  markCooldown(gemini, 40);
  const calls: string[] = [];
  const startedAt = Date.now();
  const response = await dispatch("fast-a", [gemini, groq], async (_body, model) => {
    calls.push(model);
    return okResponse(model);
  });
  assert.equal(response.status, 200);
  assert.deepEqual(calls, ["groq/groq-ready"]);
  assert.ok(Date.now() - startedAt < 1000, "must not sleep for the first target cooldown");
});

test("B: an actual Gemini 429 records cooldown and falls through to Groq without sleeping", async () => {
  const gemini = { provider: "gemini", model: "gemini-b", connectionId: "conn-b1" };
  const groq = { provider: "groq", model: "groq-b", connectionId: "conn-b2" };
  rateLimitManager.enableRateLimitProtection(gemini.connectionId);
  const calls: string[] = [];
  const startedAt = Date.now();
  const response = await dispatch("fast-b", [gemini, groq], async (_body, model) => {
    calls.push(model);
    if (model.startsWith("gemini/")) {
      const rejected = rateLimitedResponse(gemini.connectionId);
      rateLimitManager.updateFromHeaders(
        gemini.provider,
        gemini.connectionId,
        rejected.headers,
        rejected.status,
        gemini.model
      );
      return rejected;
    }
    return okResponse(model);
  });
  assert.equal(response.status, 200);
  assert.deepEqual(calls, ["gemini/gemini-b", "groq/groq-b"]);
  assert.equal(
    rateLimitManager.getRateLimitReadiness("gemini", "conn-b1", "gemini-b").state,
    "cooldown"
  );
  assert.ok(Date.now() - startedAt < 1000, "fallback must not enter the new cooldown");
});

test("C: two cooling candidates are skipped before the ready third target", async () => {
  const first = { provider: "gemini", model: "gemini-c1", connectionId: "conn-c1" };
  const second = { provider: "gemini", model: "gemini-c2", connectionId: "conn-c2" };
  const third = { provider: "groq", model: "groq-c", connectionId: "conn-c3" };
  markCooldown(first, 40);
  markCooldown(second, 20);
  const calls: string[] = [];
  const response = await dispatch("fast-c", [first, second, third], async (_body, model) => {
    calls.push(model);
    return okResponse(model);
  });
  assert.equal(response.status, 200);
  assert.deepEqual(calls, ["groq/groq-c"]);
});

test("D: all targets at 40s/20s/30s fail within the 5s combo budget without upstream calls", async () => {
  const targets = [
    { provider: "gemini", model: "gemini-d1", connectionId: "conn-d1" },
    { provider: "gemini", model: "gemini-d2", connectionId: "conn-d2" },
    { provider: "groq", model: "groq-d", connectionId: "conn-d3" },
  ];
  [40, 20, 30].forEach((seconds, index) => markCooldown(targets[index], seconds));
  let calls = 0;
  const startedAt = Date.now();
  const response = await dispatch("fast-d", targets, async () => {
    calls += 1;
    return okResponse("unexpected");
  });
  assert.equal(response.status, 429);
  assert.equal(calls, 0);
  assert.ok(Date.now() - startedAt < 1000, "must not sleep for the 20s shortest cooldown");
});

test("E: when every target cools and the shortest wait is 1s, it waits once and dispatches once", async () => {
  const first = { provider: "gemini", model: "gemini-e1", connectionId: "conn-e1" };
  const second = { provider: "groq", model: "groq-e2", connectionId: "conn-e2" };
  markCooldown(first, 1);
  markCooldown(second, 2);
  const calls: string[] = [];
  const startedAt = Date.now();
  const response = await dispatch("fast-e", [first, second], async (_body, model) => {
    calls.push(model);
    return okResponse(model);
  });
  const elapsed = Date.now() - startedAt;
  assert.equal(response.status, 200);
  assert.deepEqual(calls, ["gemini/gemini-e1"]);
  assert.ok(elapsed >= 900 && elapsed < 3000, `expected one bounded wait, got ${elapsed}ms`);
});

test("F: cancellation during bounded wait aborts promptly and starts no upstream call", async () => {
  const target = { provider: "gemini", model: "gemini-f", connectionId: "conn-f" };
  markCooldown(target, 1);
  const controller = new AbortController();
  let calls = 0;
  const timer = setTimeout(() => controller.abort(), 25);
  const startedAt = Date.now();
  const response = await dispatch(
    "fast-f",
    [target],
    async () => {
      calls += 1;
      return okResponse("unexpected");
    },
    { signal: controller.signal }
  );
  clearTimeout(timer);
  assert.equal(response.status, 499);
  assert.equal(calls, 0);
  assert.ok(Date.now() - startedAt < 500, "abort must interrupt the bounded wait");
});

test("G: direct withRateLimit semantics do not consult combo readiness", async () => {
  const target = { provider: "openai", model: "direct-g", connectionId: "conn-g" };
  markCooldown(target, 40);
  let calls = 0;
  const result = await rateLimitManager.withRateLimit(
    target.provider,
    target.connectionId,
    target.model,
    async () => {
      calls += 1;
      return "direct-result";
    }
  );
  assert.equal(result, "direct-result");
  assert.equal(calls, 1);
  assert.equal(
    rateLimitManager.getRateLimitReadiness("openai", "conn-g", "direct-g").state,
    "cooldown"
  );
});

test("H: one requestId spans skip, fallback, and exactly one client terminal response", async () => {
  const requestId = "11111111-1111-4111-8111-111111111111";
  const first = { provider: "gemini", model: "gemini-h", connectionId: "conn-h1" };
  const second = { provider: "groq", model: "groq-h", connectionId: "conn-h2" };
  markCooldown(first, 40);
  const events: Array<Record<string, unknown>> = [];
  diagnostics.__test.setSink((event) => events.push(event));
  diagnostics.startPublicFunnelRequest(requestId, "example.ts.net");
  diagnostics.markPublicFunnelRequestValidated(requestId, "fast-h");
  diagnostics.markRouteSelectionStarted(requestId, "fast-h");
  const response = await dispatch(
    "fast-h",
    [first, second],
    async (_body, model) => {
      const attempt = diagnostics.markUpstreamRequestStarted(requestId, "groq", model);
      diagnostics.markUpstreamResponseHeaders(requestId, attempt, 200);
      diagnostics.markAttemptCompleted(requestId, attempt);
      return okResponse(model);
    },
    { correlationId: requestId }
  );
  const observed = diagnostics.observeClientResponse(response, requestId);
  await observed.text();
  assert.equal(observed.status, 200);
  assert.ok(events.some((event) => event.event === "target_skipped_cooldown"));
  assert.ok(events.some((event) => event.event === "fallback_started"));
  assert.equal(events.filter((event) => event.event === "request_completed").length, 1);
  assert.ok(events.every((event) => event.requestId === requestId));
});
