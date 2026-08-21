import test from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const TEST_DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), "omniroute-rl-real-transport-"));
process.env.DATA_DIR = TEST_DATA_DIR;

const { BaseExecutor } = await import("../../open-sse/executors/base.ts");
const rateLimitManager = await import("../../open-sse/services/rateLimitManager.ts");
const resilienceSettings = await import("../../src/lib/resilience/settings.ts");
const diagnostics = await import("../../src/shared/utils/publicFunnelDiagnostics.ts");
const core = await import("../../src/lib/db/core.ts");

function listen(server: http.Server): Promise<number> {
  return new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string") reject(new Error("missing server port"));
      else resolve(address.port);
    });
  });
}

function close(server: http.Server): Promise<void> {
  return new Promise((resolve) => server.close(() => resolve()));
}

test.afterEach(async () => {
  diagnostics.__test.clear();
  await rateLimitManager.__resetRateLimitManagerForTests();
});

test.after(() => {
  core.resetDbInstance();
  fs.rmSync(TEST_DATA_DIR, { recursive: true, force: true });
});

test("a timed-out streaming POST is closed before fallback URL B starts", async () => {
  let activePosts = 0;
  let maxActivePosts = 0;
  let aCalls = 0;
  let bCalls = 0;
  let aClosed = false;
  let aClosedAt = 0;
  let bStartedAt = 0;
  let lateWriteAttempted = false;

  const server = http.createServer((req, res) => {
    if (req.method !== "POST") {
      res.writeHead(405).end();
      return;
    }
    activePosts += 1;
    maxActivePosts = Math.max(maxActivePosts, activePosts);
    let released = false;
    const release = () => {
      if (released) return;
      released = true;
      activePosts -= 1;
    };

    if (req.url === "/a") {
      aCalls += 1;
      res.on("close", () => {
        aClosed = true;
        aClosedAt = performance.now();
        release();
      });
      setTimeout(() => {
        lateWriteAttempted = true;
        if (!res.destroyed) {
          res.writeHead(200, { "content-type": "text/event-stream" });
          res.end('data: {"choices":[{"delta":{"content":"late"}}]}\n\n');
        }
      }, 220).unref();
      return;
    }

    bCalls += 1;
    bStartedAt = performance.now();
    assert.equal(aClosed, true, "fallback B must not start until server A observes close");
    assert.equal(activePosts, 1, "only B may be active when B starts");
    res.on("close", release);
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.end('data: {"choices":[{"delta":{"content":"ok"}}]}\n\n');
  });

  const port = await listen(server);
  try {
    await rateLimitManager.applyRequestQueueSettings({
      ...resilienceSettings.DEFAULT_RESILIENCE_SETTINGS.requestQueue,
      autoEnableApiKeyProviders: false,
      concurrentRequests: 1,
      requestsPerMinute: 100000,
      minTimeBetweenRequestsMs: 0,
      maxWaitMs: 40,
    });
    const connectionId = "real-transport-conn";
    rateLimitManager.enableRateLimitProtection(connectionId);
    const executor = new BaseExecutor("local-transport", {
      id: "local-transport",
      format: "openai",
      baseUrls: [`http://127.0.0.1:${port}/a`, `http://127.0.0.1:${port}/b`],
      timeoutMs: 80,
    });

    const requestId = "22222222-2222-4222-8222-222222222222";
    const events: Array<Record<string, unknown>> = [];
    diagnostics.__test.setSink((event) => events.push(event));
    diagnostics.startPublicFunnelRequest(requestId, "local.test");
    diagnostics.markPublicFunnelRequestValidated(requestId, "transport-test");
    diagnostics.markRouteSelectionStarted(requestId, "transport-test");
    diagnostics.markRouteSelected(requestId, {
      provider: "local-transport",
      model: "test-model",
    });
    const diagnosticContext = diagnostics.createUpstreamDiagnosticContext(
      requestId,
      "local-transport",
      "test-model"
    );

    const result = await diagnostics.runWithUpstreamDiagnosticContext(diagnosticContext, () =>
      rateLimitManager.withRateLimit("local-transport", connectionId, "test-model", () =>
        executor.execute({
          model: "test-model",
          body: { messages: [{ role: "user", content: "not logged" }] },
          stream: true,
          credentials: { connectionId, apiKey: "test-only" },
          signal: new AbortController().signal,
          skipUpstreamRetry: false,
        })
      )
    );
    const response = "response" in result ? result.response : result;
    const observed = diagnostics.observeClientResponse(response, requestId);
    assert.match(await observed.text(), /ok/);
    await new Promise((resolve) => setTimeout(resolve, 280));

    assert.equal(aCalls, 1);
    assert.equal(bCalls, 1);
    assert.equal(maxActivePosts, 1, "logical request must never overlap upstream POSTs");
    assert.ok(aClosedAt > 0 && bStartedAt >= aClosedAt);
    assert.equal(lateWriteAttempted, true, "server A attempted its deliberately late write");
    const terminalIndex = events.findIndex(
      (event) => event.event === "request_completed" || event.event === "request_failed"
    );
    assert.ok(terminalIndex >= 0, "logical request must have a terminal diagnostic event");
    const forbidden = new Set([
      "upstream_request_started",
      "upstream_response_headers",
      "first_upstream_sse_event",
      "first_upstream_content_token",
    ]);
    assert.equal(
      events.slice(terminalIndex + 1).filter((event) => forbidden.has(String(event.event))).length,
      0,
      "no upstream event may occur after logical terminal"
    );
  } finally {
    await close(server);
  }
});
