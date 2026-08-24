import test from "node:test";
import assert from "node:assert/strict";

import {
  createBodyTimeoutError,
  createUpstreamStartTimeoutError,
  createAbortError,
  computeBillableTokens,
  executeWithUpstreamStartTimeout,
  getExecutorTimeoutMs,
  normalizeExecutorResult,
} from "../../open-sse/handlers/chatCore/upstreamTimeouts.ts";

test("error factories set name and message", () => {
  const body = createBodyTimeoutError(1234);
  assert.equal(body.name, "BodyTimeoutError");
  assert.match(body.message, /1234ms/);

  const start = createUpstreamStartTimeoutError(500, "openai", "gpt-4o");
  assert.equal(start.name, "TimeoutError");
  assert.match(start.message, /openai\/gpt-4o/);

  const ctrl = new AbortController();
  ctrl.abort("nope");
  const ab = createAbortError(ctrl.signal);
  assert.equal(ab.name, "AbortError");
});

test("computeBillableTokens sums input+output+reasoning (no cache double-count)", () => {
  const total = computeBillableTokens({
    prompt_tokens: 10,
    completion_tokens: 5,
    reasoning_tokens: 2,
  });
  assert.equal(total, 17);
});

test("getExecutorTimeoutMs floors valid values and falls back to default", () => {
  assert.equal(getExecutorTimeoutMs({ getTimeoutMs: () => 1234.9 }), 1234);
  assert.equal(getExecutorTimeoutMs({ getTimeoutMs: () => NaN }), getExecutorTimeoutMs(null));
  assert.ok(Number.isFinite(getExecutorTimeoutMs(null)));
});

test("normalizeExecutorResult wraps bare Response and passes through rich result", () => {
  const r = new Response("x");
  const wrapped = normalizeExecutorResult(r);
  assert.equal(wrapped.response, r);
  assert.equal(wrapped.url, "");
  const rich = normalizeExecutorResult({ response: r, url: "u", headers: { a: "b" } });
  assert.equal(rich.url, "u");
  assert.equal(rich.headers.a, "b");
});

test("upstream start timeout waits for aborted execution to settle", async () => {
  const events: string[] = [];
  const promise = executeWithUpstreamStartTimeout({
    executor: { getTimeoutMs: () => 10 },
    provider: "gemini",
    model: "slow",
    signal: new AbortController().signal,
    execute: (signal) =>
      new Promise<Response>((_resolve, reject) => {
        signal.addEventListener(
          "abort",
          () => {
            events.push("abort");
            setTimeout(() => {
              events.push("settled");
              reject(signal.reason);
            }, 20);
          },
          { once: true }
        );
      }),
  });

  await assert.rejects(promise, { name: "TimeoutError" });
  events.push("returned");
  assert.deepEqual(events, ["abort", "settled", "returned"]);
});

test("parent abort still reaches a live body after response headers", async () => {
  const parent = new AbortController();
  let transportSignal: AbortSignal | null = null;
  const response = await executeWithUpstreamStartTimeout({
    executor: { getTimeoutMs: () => 1000 },
    provider: "gemini",
    model: "streaming",
    signal: parent.signal,
    execute: async (signal) => {
      transportSignal = signal;
      return new Response(new ReadableStream({ start() {} }), {
        headers: { "content-type": "text/event-stream" },
      });
    },
  });
  assert.ok(response.body);
  assert.equal(transportSignal?.aborted, false);
  parent.abort(new Error("post-headers-budget"));
  assert.equal(transportSignal?.aborted, true);
  assert.match(String(transportSignal?.reason), /post-headers-budget/);
  await response.body.cancel();
});
