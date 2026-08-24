import { test } from "node:test";
import assert from "node:assert/strict";
import { buildTargetTimeoutRunner } from "../../open-sse/services/combo/targetTimeoutRunner.ts";
import type { ComboLogger, SingleModelTarget } from "../../open-sse/services/combo/types.ts";

const noopLog: ComboLogger = { warn() {}, info() {}, error() {}, debug() {} };

test("timeout<=0: passthrough direto (sem timer)", async () => {
  let called = false;
  const runner = buildTargetTimeoutRunner({
    handleSingleModel: async () => {
      called = true;
      return new Response("ok");
    },
    comboTargetTimeoutMs: 0,
    log: noopLog,
  });
  const res = await runner({}, "m");
  assert.equal(called, true);
  assert.equal(await res.text(), "ok");
});

test("timeout<=0: erro do upstream vira errorResponse 502", async () => {
  const runner = buildTargetTimeoutRunner({
    handleSingleModel: async () => {
      throw new Error("boom");
    },
    comboTargetTimeoutMs: 0,
    log: noopLog,
  });
  const res = await runner({}, "m");
  assert.equal(res.status, 502);
});

test("excede o limite: aborta e retorna 524 timed out", async () => {
  const runner = buildTargetTimeoutRunner({
    handleSingleModel: (_b, _m, target) =>
      new Promise<Response>((resolve) => {
        // resolve só se abortado (simula um upstream que respeita o signal)
        const sig = (target as { modelAbortSignal?: AbortSignal } | undefined)?.modelAbortSignal;
        sig?.addEventListener("abort", () => resolve(new Response(null, { status: 599 })));
      }),
    comboTargetTimeoutMs: 20,
    log: noopLog,
  });
  const res = await runner({}, "slow-model");
  assert.equal(res.status, 524);
  const body = await res.json();
  assert.match(JSON.stringify(body), /timed out/i);
});

test("timeout waits for the aborted target to settle before fallback may continue", async () => {
  const events: string[] = [];
  const runner = buildTargetTimeoutRunner({
    handleSingleModel: (_b, _m, target) =>
      new Promise<Response>((resolve) => {
        const signal = (target as { modelAbortSignal?: AbortSignal } | undefined)?.modelAbortSignal;
        assert.ok(signal);
        signal.addEventListener(
          "abort",
          () => {
            events.push("abort");
            setTimeout(() => {
              events.push("settled");
              resolve(new Response(null, { status: 599 }));
            }, 20);
          },
          { once: true }
        );
      }),
    comboTargetTimeoutMs: 10,
    log: noopLog,
  });

  const response = await runner({}, "slow-model");
  events.push("returned");
  assert.equal(response.status, 524);
  assert.deepEqual(events, ["abort", "settled", "returned"]);
});

test("sucesso rápido vence a corrida do timeout", async () => {
  const runner = buildTargetTimeoutRunner({
    handleSingleModel: async () => new Response("fast", { status: 200 }),
    comboTargetTimeoutMs: 1000,
    log: noopLog,
  });
  const res = await runner({}, "m");
  assert.equal(res.status, 200);
  assert.equal(await res.text(), "fast");
});

test("hedge do parent já abortado propaga o abort ao filho", async () => {
  const parent = new AbortController();
  parent.abort(new Error("hedge-cancelled"));
  let sawAbort = false;
  const runner = buildTargetTimeoutRunner({
    handleSingleModel: (_b, _m, target) =>
      new Promise<Response>((resolve) => {
        const sig = (target as { modelAbortSignal?: AbortSignal } | undefined)?.modelAbortSignal;
        if (sig?.aborted) sawAbort = true;
        resolve(new Response("ok"));
      }),
    comboTargetTimeoutMs: 1000,
    log: noopLog,
  });
  const parentTarget: SingleModelTarget = { modelAbortSignal: parent.signal };
  await runner({}, "m", parentTarget);
  assert.equal(sawAbort, true);
});

test("parent target abort remains linked after streaming headers", async () => {
  const parent = new AbortController();
  let childSignal: AbortSignal | null = null;
  const runner = buildTargetTimeoutRunner({
    handleSingleModel: async (_body, _model, target) => {
      childSignal = (target as { modelAbortSignal?: AbortSignal })?.modelAbortSignal ?? null;
      return new Response(new ReadableStream({ start() {} }), {
        headers: { "content-type": "text/event-stream" },
      });
    },
    comboTargetTimeoutMs: 1000,
    log: noopLog,
  });
  const parentTarget: SingleModelTarget = { modelAbortSignal: parent.signal };
  const response = await runner({}, "m", parentTarget);
  assert.equal(childSignal?.aborted, false);
  parent.abort(new Error("visible-content-budget"));
  assert.equal(childSignal?.aborted, true);
  assert.match(String(childSignal?.reason), /visible-content-budget/);
  await response.body?.cancel();
});
