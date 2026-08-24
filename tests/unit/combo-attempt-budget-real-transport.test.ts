import test from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import { once } from "node:events";
import { randomUUID } from "node:crypto";

import proxyFetch from "../../open-sse/utils/proxyFetch.ts";
import { closeDispatcherCache } from "../../open-sse/utils/proxyDispatcher.ts";
import {
  ComboAttemptBudgetGuard,
  type ComboAttemptBudgetAlternate,
} from "../../open-sse/services/combo/attemptBudget.ts";
import {
  releaseQualityClone,
  settleRejectedQualityResponse,
  validateResponseQuality,
} from "../../open-sse/services/combo/validateQuality.ts";
import { runWithClientResponseContract } from "../../open-sse/services/responseContract.ts";
import * as diagnostics from "../../src/shared/utils/publicFunnelDiagnostics.ts";

async function listen(server: http.Server): Promise<string> {
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  assert.ok(address && typeof address === "object");
  return `http://127.0.0.1:${address.port}`;
}

async function closeServer(server: http.Server): Promise<void> {
  await new Promise<void>((resolve) => {
    server.close(() => resolve());
    server.closeAllConnections?.();
  });
}

function observedFetch(
  url: string,
  controller: AbortController,
  guard: ComboAttemptBudgetGuard
): Promise<Response> {
  const requestId = randomUUID();
  diagnostics.startPublicFunnelRequest(requestId, "local.test");
  diagnostics.markRouteSelectionStarted(requestId, "combo/fixture");
  diagnostics.markRouteSelected(requestId, { provider: "fixture", model: "fixture/a" });
  const context = diagnostics.createUpstreamDiagnosticContext(
    requestId,
    "fixture",
    "fixture/a",
    guard.transportObserver
  );
  return diagnostics.runWithUpstreamDiagnosticContext(context, () =>
    diagnostics.observeDiagnosticFetch(() =>
      proxyFetch(url, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: "{}",
        signal: controller.signal,
      })
    )
  );
}

function guardFor(
  controller: AbortController,
  alternate: () => Promise<ComboAttemptBudgetAlternate | null>,
  profile = { responseHeadersMs: 80, firstVisibleContentMs: 120 }
) {
  return new ComboAttemptBudgetGuard({
    controller,
    profile,
    recheckIntervalMs: 20,
    findAdmissibleAlternate: alternate,
  });
}

test.afterEach(async () => {
  diagnostics.__test.clear();
  await closeDispatcherCache();
});

test("real no-headers POST is terminated before healthy fallback starts", async () => {
  let active = 0;
  let maxActive = 0;
  let aClosed = false;
  let bCalls = 0;
  let resolveAClosed!: () => void;
  const aClosedPromise = new Promise<void>((resolve) => (resolveAClosed = resolve));
  const server = http.createServer((request, response) => {
    active++;
    maxActive = Math.max(maxActive, active);
    let released = false;
    const release = () => {
      if (released) return;
      released = true;
      active--;
    };
    response.once("close", release);

    if (request.url === "/a") {
      response.once("close", () => {
        aClosed = true;
        resolveAClosed();
      });
      setTimeout(() => {
        if (!response.destroyed) {
          response.writeHead(200, { "content-type": "text/event-stream" });
          response.end('data: {"choices":[{"delta":{"content":"late"}}]}\n\n');
        }
      }, 300).unref();
      return;
    }

    bCalls++;
    assert.equal(aClosed, true, "B started before server A observed transport close");
    response.writeHead(200, { "content-type": "text/event-stream" });
    response.end('data: {"choices":[{"delta":{"content":"OK"}}]}\n\ndata: [DONE]\n\n');
  });

  const origin = await listen(server);
  try {
    const controller = new AbortController();
    const guard = guardFor(controller, async () => ({
      targetIndex: 1,
      provider: "fixture",
      model: "fixture/b",
    }));
    await assert.rejects(observedFetch(`${origin}/a`, controller, guard));
    await aClosedPromise;
    assert.equal(guard.getTrigger()?.phase, "response_headers");

    const fallback = await proxyFetch(`${origin}/b`, { method: "POST", body: "{}" });
    assert.match(await fallback.text(), /OK/);
    assert.equal(bCalls, 1);
    assert.equal(maxActive, 1);
    guard.dispose();
  } finally {
    await closeServer(server);
  }
});

test("real headers-without-visible stream is closed before fallback", async () => {
  let active = 0;
  let maxActive = 0;
  let aClosed = false;
  let resolveAClosed!: () => void;
  const aClosedPromise = new Promise<void>((resolve) => (resolveAClosed = resolve));
  const server = http.createServer((request, response) => {
    active++;
    maxActive = Math.max(maxActive, active);
    let released = false;
    const release = () => {
      if (released) return;
      released = true;
      active--;
    };
    response.once("close", release);
    if (request.url === "/a") {
      response.once("close", () => {
        aClosed = true;
        resolveAClosed();
      });
      response.writeHead(200, { "content-type": "text/event-stream" });
      response.write('data: {"choices":[{"delta":{"reasoning_content":"opaque-fixture"}}]}\n\n');
      return;
    }
    assert.equal(aClosed, true, "visible fallback overlapped the reasoning-only stream");
    response.writeHead(200, { "content-type": "text/event-stream" });
    response.end('data: {"choices":[{"delta":{"content":"OK"}}]}\n\ndata: [DONE]\n\n');
  });

  const origin = await listen(server);
  try {
    const controller = new AbortController();
    const guard = guardFor(controller, async () => ({
      targetIndex: 1,
      provider: "fixture",
      model: "fixture/b",
    }));
    const response = await observedFetch(`${origin}/a`, controller, guard);
    const clone = response.clone();
    await runWithClientResponseContract("visible-text", () =>
      validateResponseQuality(clone, true, {}, null, () => guard.markVisibleContent())
    );
    assert.equal(guard.getTrigger()?.phase, "visible_content");
    await settleRejectedQualityResponse(clone, response);
    await aClosedPromise;

    const fallback = await proxyFetch(`${origin}/b`, { method: "POST", body: "{}" });
    assert.match(await fallback.text(), /OK/);
    assert.equal(maxActive, 1);
    guard.dispose();
  } finally {
    await closeServer(server);
  }
});

test("visible content disarms budget while the same long stream completes", async () => {
  const server = http.createServer((_request, response) => {
    response.writeHead(200, { "content-type": "text/event-stream" });
    response.write('data: {"choices":[{"delta":{"content":"OK"}}]}\n\n');
    setTimeout(() => response.end("data: [DONE]\n\n"), 180).unref();
  });
  const origin = await listen(server);
  try {
    const controller = new AbortController();
    const guard = guardFor(controller, async () => ({
      targetIndex: 1,
      provider: "fixture",
      model: "fixture/b",
    }));
    const response = await observedFetch(`${origin}/visible`, controller, guard);
    const clone = response.clone();
    const quality = await runWithClientResponseContract("visible-text", () =>
      validateResponseQuality(clone, true, {}, null, () => guard.markVisibleContent())
    );
    releaseQualityClone(clone, response, quality);
    assert.match(await response.text(), /OK/);
    assert.equal(controller.signal.aborted, false);
    assert.equal(guard.getTrigger(), null);
    guard.dispose();
  } finally {
    await closeServer(server);
  }
});

test("slow only target is not sacrificed when no alternate is admissible", async () => {
  const server = http.createServer((_request, response) => {
    setTimeout(() => {
      response.writeHead(200, { "content-type": "text/event-stream" });
      response.end('data: {"choices":[{"delta":{"content":"ONLY"}}]}\n\ndata: [DONE]\n\n');
    }, 140).unref();
  });
  const origin = await listen(server);
  try {
    const controller = new AbortController();
    const guard = guardFor(controller, async () => null, {
      responseHeadersMs: 40,
      firstVisibleContentMs: 80,
    });
    const response = await observedFetch(`${origin}/only`, controller, guard);
    const clone = response.clone();
    const quality = await runWithClientResponseContract("visible-text", () =>
      validateResponseQuality(clone, true, {}, null, () => guard.markVisibleContent())
    );
    releaseQualityClone(clone, response, quality);
    assert.match(await response.text(), /ONLY/);
    assert.equal(controller.signal.aborted, false);
    guard.dispose();
  } finally {
    await closeServer(server);
  }
});
