/**
 * #4165 — surface a clear error when the request-queue (Bottleneck) drops a job.
 *
 * maxWaitMs limits time waiting for admission. It must never become Bottleneck's
 * `expiration`, because expiration rejects an executing Promise without cancelling
 * its network operation (the root cause of overlapping streaming POSTs).
 */
import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const TEST_DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), "omniroute-rl-queue-timeout-"));
process.env.DATA_DIR = TEST_DATA_DIR;

const core = await import("../../src/lib/db/core.ts");
const resilienceSettings = await import("../../src/lib/resilience/settings.ts");
const rateLimitManager = await import("../../open-sse/services/rateLimitManager.ts");

function wait(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

test.afterEach(async () => {
  await rateLimitManager.__resetRateLimitManagerForTests();
});

test.after(() => {
  core.resetDbInstance();
  fs.rmSync(TEST_DATA_DIR, { recursive: true, force: true });
});

async function triggerQueueTimeout() {
  await rateLimitManager.applyRequestQueueSettings({
    ...resilienceSettings.DEFAULT_RESILIENCE_SETTINGS.requestQueue,
    autoEnableApiKeyProviders: false,
    concurrentRequests: 1,
    requestsPerMinute: 100000,
    minTimeBetweenRequestsMs: 0,
    maxWaitMs: 40,
  });
  rateLimitManager.enableRateLimitProtection("conn-queue-timeout");
  rateLimitManager.__setLimiterSettingsForTests("openai", "conn-queue-timeout", "gpt-4o", {
    reservoir: 0,
    reservoirRefreshAmount: 1,
    reservoirRefreshInterval: 1000,
  });
  let upstreamCalls = 0;
  const pending = rateLimitManager.withRateLimit(
    "openai",
    "conn-queue-timeout",
    "gpt-4o",
    async () => {
      upstreamCalls += 1;
      return "must-not-run";
    }
  );
  return { pending, upstreamCalls: () => upstreamCalls };
}

test("#4165 queue-timeout surfaces a clear OmniRoute error, not the raw upstream-looking string", async () => {
  let caught: (Error & { code?: string; cause?: { message?: string } }) | undefined;
  try {
    const triggered = await triggerQueueTimeout();
    await triggered.pending;
    assert.fail("expected the queued job to be dropped");
  } catch (err) {
    caught = err as Error & { code?: string; cause?: { message?: string } };
  }
  assert.ok(caught, "an error should have been thrown");

  // Tagged so combo / callers can classify it as a local queue drop.
  assert.equal(caught.code, "RATE_LIMIT_QUEUE_TIMEOUT", "error must carry the queue-timeout code");

  // The surfaced message must read as a local queue limit, naming the knob,
  // and must NOT masquerade as an upstream "This job timed out" gateway error.
  assert.match(caught.message, /maxWaitMs/, "message should name the maxWaitMs knob");
  assert.match(
    caught.message,
    /not an upstream/i,
    "message should explicitly disclaim an upstream timeout"
  );
  assert.doesNotMatch(
    caught.message,
    /This job timed out/,
    "raw Bottleneck/upstream-looking string must not leak into the surfaced message"
  );

  const triggered = await triggerQueueTimeout();
  await assert.rejects(triggered.pending);
  await wait(80);
  assert.equal(triggered.upstreamCalls(), 0, "expired queued wrapper must never start upstream");
});

test("a running upstream may exceed maxWaitMs without rejection or duplicate execution", async () => {
  await rateLimitManager.applyRequestQueueSettings({
    ...resilienceSettings.DEFAULT_RESILIENCE_SETTINGS.requestQueue,
    autoEnableApiKeyProviders: false,
    concurrentRequests: 1,
    requestsPerMinute: 100000,
    minTimeBetweenRequestsMs: 0,
    maxWaitMs: 40,
  });
  rateLimitManager.enableRateLimitProtection("conn-fast");

  let calls = 0;
  const result = await rateLimitManager.withRateLimit("openai", "conn-fast", "gpt-4o", async () => {
    calls += 1;
    await wait(120);
    return "ok";
  });
  assert.equal(result, "ok");
  assert.equal(calls, 1);
});
