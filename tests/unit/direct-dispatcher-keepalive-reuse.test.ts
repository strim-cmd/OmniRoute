import test from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import { once } from "node:events";

import proxyFetch from "../../open-sse/utils/proxyFetch.ts";
import { closeDispatcherCache } from "../../open-sse/utils/proxyDispatcher.ts";

type FixtureState = {
  active: number;
  maxActive: number;
  sockets: Set<object>;
  requestSockets: object[];
  requestConnectionHeaders: Array<string | undefined>;
  cancelledObserved: Promise<void> | null;
};

async function listen(server: http.Server): Promise<string> {
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  assert.ok(address && typeof address === "object");
  return `http://127.0.0.1:${address.port}`;
}

async function closeServer(server: http.Server): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    server.close((error) => {
      if (error) reject(error);
      else resolve();
    });
    server.closeAllConnections?.();
  });
}

function createKeepAliveFixture() {
  const state: FixtureState = {
    active: 0,
    maxActive: 0,
    sockets: new Set(),
    requestSockets: [],
    requestConnectionHeaders: [],
    cancelledObserved: null,
  };
  const server = http.createServer((request, response) => {
    state.active++;
    state.maxActive = Math.max(state.maxActive, state.active);
    state.sockets.add(request.socket);
    state.requestSockets.push(request.socket);
    state.requestConnectionHeaders.push(request.headers.connection);
    let finished = false;
    const finish = () => {
      if (finished) return;
      finished = true;
      state.active--;
    };
    response.once("finish", finish);
    response.once("close", finish);

    if (request.url === "/cancel") {
      let resolveCancelled!: () => void;
      state.cancelledObserved = new Promise<void>((resolve) => {
        resolveCancelled = resolve;
      });
      request.socket.once("close", resolveCancelled);
      response.writeHead(200, {
        "Content-Type": "text/event-stream",
        Connection: "keep-alive",
      });
      response.write('data: {"choices":[{"delta":{"content":"partial"}}]}\n\n');
      return;
    }

    response.writeHead(200, {
      "Content-Type": "text/event-stream",
      Connection: "keep-alive",
    });
    response.end('data: {"choices":[{"delta":{"content":"OK"}}]}\n\ndata: [DONE]\n\n');
  });
  server.keepAliveTimeout = 30_000;
  return { server, state };
}

test.afterEach(async () => {
  await closeDispatcherCache();
});

test("20 sequential SSE POSTs reuse a small bounded set of keep-alive sockets", async () => {
  const { server, state } = createKeepAliveFixture();
  const origin = await listen(server);
  try {
    for (let index = 0; index < 20; index++) {
      const response = await proxyFetch(`${origin}/stream`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: "{}",
      });
      assert.equal(response.status, 200);
      const payload = await response.text();
      assert.match(payload, /delta/);
      assert.match(payload, /\[DONE\]/);
    }

    assert.equal(state.maxActive, 1);
    assert.ok(state.sockets.size < 20, `expected reuse, saw ${state.sockets.size} TCP sockets`);
    assert.equal(state.sockets.size, 1, "sequential traffic should stay on one warm socket");
    assert.ok(
      state.requestConnectionHeaders.every((value) => value === "keep-alive"),
      "direct Undici requests must not emit Connection: close"
    );
  } finally {
    await closeServer(server);
    await closeDispatcherCache();
  }
});

test("a cancelled streaming POST is discarded before the next request succeeds", async () => {
  const { server, state } = createKeepAliveFixture();
  const origin = await listen(server);
  try {
    const controller = new AbortController();
    const cancelled = await proxyFetch(`${origin}/cancel`, {
      method: "POST",
      body: "{}",
      signal: controller.signal,
    });
    const cancelledBody = cancelled.text();
    controller.abort(new DOMException("cancel fixture", "AbortError"));
    await assert.rejects(cancelledBody);
    assert.ok(state.cancelledObserved);
    await state.cancelledObserved;

    const successful = await proxyFetch(`${origin}/stream`, { method: "POST", body: "{}" });
    assert.match(await successful.text(), /OK/);

    assert.equal(state.maxActive, 1, "the next POST must not overlap the cancelled POST");
    assert.equal(state.requestSockets.length, 2);
    assert.notEqual(
      state.requestSockets[0],
      state.requestSockets[1],
      "an aborted socket must not return to the reusable pool"
    );
  } finally {
    await closeServer(server);
    await closeDispatcherCache();
  }
});
