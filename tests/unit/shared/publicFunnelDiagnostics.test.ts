import assert from "node:assert/strict";
import http from "node:http";
import { afterEach, test } from "node:test";
import proxyFetch from "../../../open-sse/utils/proxyFetch.ts";
import { closeDispatcherCache } from "../../../open-sse/utils/proxyDispatcher.ts";
import {
  __test,
  classifyFailure,
  markPublicFunnelRequestValidated,
  markRouteSelected,
  markRouteSelectionStarted,
  markWaitCompleted,
  createUpstreamDiagnosticContext,
  markUpstreamRequestStarted,
  markUpstreamResponseHeaders,
  monotonicNow,
  observeClientResponse,
  observeDiagnosticFetch,
  observeUpstreamResponse,
  resolveOmniaRequestId,
  runWithUpstreamDiagnosticContext,
  startPublicFunnelRequest,
} from "@/shared/utils/publicFunnelDiagnostics";

afterEach(() => __test.clear());

test("accepts only a canonical UUID request id", () => {
  const valid = "123e4567-e89b-42d3-a456-426614174000";
  assert.equal(resolveOmniaRequestId(new Headers({ "X-Omnia-Request-ID": valid })), valid);

  const generated = resolveOmniaRequestId({ get: () => "not-a-uuid\r\nAuthorization: secret" });
  assert.match(generated, /^[0-9a-f-]{36}$/);
  assert.notEqual(generated, valid);
});

test("classifies stable diagnostic failure categories without exposing error text", () => {
  assert.equal(classifyFailure({ code: "ENOTFOUND" }), "dns");
  assert.equal(classifyFailure({ code: "ECONNREFUSED" }), "connect_refused");
  assert.equal(classifyFailure(undefined, 429), "http_429");
  assert.equal(classifyFailure({ code: "STREAM_READINESS_TIMEOUT" }), "first_token_timeout");
});

test("records upstream and client first content while forwarding bytes unchanged", async () => {
  const requestId = "123e4567-e89b-42d3-a456-426614174000";
  const events: Array<Record<string, unknown>> = [];
  __test.setSink((event) => events.push(event));
  startPublicFunnelRequest(requestId, "example.ts.net");
  markPublicFunnelRequestValidated(requestId, "omnia-coding");
  markRouteSelectionStarted(requestId, "omnia-coding");
  markRouteSelected(requestId, { strategy: "priority", provider: "test", model: "test/m" });
  const attempt = markUpstreamRequestStarted(requestId, "test", "m");
  assert.equal(attempt, 1);

  const secretText = "prompt-and-key-must-not-appear";
  const frame = `data: ${JSON.stringify({ choices: [{ delta: { content: secretText } }] })}\n\n`;
  markUpstreamResponseHeaders(requestId, attempt, 200);
  const upstream = observeUpstreamResponse(
    new Response(frame, { headers: { "Content-Type": "text/event-stream" } }),
    requestId,
    attempt
  );
  assert.equal(await upstream.text(), frame);

  const client = observeClientResponse(
    new Response(frame, { headers: { "Content-Type": "text/event-stream" } }),
    requestId
  );
  assert.equal(await client.text(), frame);

  const names = events.map((event) => event.event);
  assert.ok(names.includes("first_upstream_sse_event"));
  assert.ok(names.includes("first_upstream_content_token"));
  assert.ok(names.includes("response_first_event_sent_to_client"));
  assert.ok(names.includes("response_first_content_sent_to_client"));
  assert.ok(names.includes("request_completed"));
  assert.equal(JSON.stringify(events).includes(secretText), false);

  const headers = events.find((event) => event.event === "upstream_response_headers");
  const firstEvent = events.find((event) => event.event === "first_upstream_sse_event");
  const firstContent = events.find((event) => event.event === "first_upstream_content_token");
  const completed = events.find((event) => event.event === "attempt_completed");
  assert.equal(typeof headers?.timeToHeadersMs, "number");
  assert.equal(typeof firstEvent?.headersToFirstEventMs, "number");
  assert.equal(typeof firstContent?.headersToFirstContentMs, "number");
  assert.equal(typeof firstContent?.attemptTtftMs, "number");
  assert.equal(typeof completed?.attemptTotalMs, "number");
});

test("records client abort as an explicit cancelled terminal event", () => {
  const requestId = "123e4567-e89b-42d3-a456-426614174001";
  const events: Array<Record<string, unknown>> = [];
  const controller = new AbortController();
  __test.setSink((event) => events.push(event));
  startPublicFunnelRequest(requestId, "example.ts.net");

  observeClientResponse(
    new Response(new ReadableStream<Uint8Array>(), {
      headers: { "Content-Type": "text/event-stream" },
    }),
    requestId,
    controller.signal
  );
  controller.abort();

  const terminal = events.find((event) => event.event === "request_failed");
  assert.equal(terminal?.failureCategory, "cancelled");
  assert.equal(
    events.some((event) => event.event === "request_completed"),
    false
  );
});

test("records a contentless SSE close as unexpected EOF", async () => {
  const requestId = "123e4567-e89b-42d3-a456-426614174002";
  const events: Array<Record<string, unknown>> = [];
  __test.setSink((event) => events.push(event));
  startPublicFunnelRequest(requestId, "example.ts.net");

  const response = observeClientResponse(
    new Response('data: {"type":"response.created"}\n\n', {
      headers: { "Content-Type": "text/event-stream" },
    }),
    requestId
  );
  await response.text();

  const terminal = events.find((event) => event.event === "request_failed");
  assert.equal(terminal?.failureCategory, "unexpected_eof");
});

test("records fallback after a pre-attempt rate-limit failure", () => {
  const requestId = "123e4567-e89b-42d3-a456-426614174003";
  const events: Array<Record<string, unknown>> = [];
  __test.setSink((event) => events.push(event));
  startPublicFunnelRequest(requestId, "example.ts.net");
  markRouteSelectionStarted(requestId, "omnia-coding");
  markRouteSelected(requestId, {
    strategy: "priority",
    provider: "gemini",
    model: "gemini-3.7-flash",
  });
  markWaitCompleted(requestId, "rate_limit", monotonicNow(), undefined, "stream_reset");
  markUpstreamRequestStarted(requestId, "groq", "openai/gpt-oss-120b");

  const fallback = events.find((event) => event.event === "fallback_started");
  assert.equal(fallback?.previousProvider, "gemini");
  assert.equal(fallback?.previousModel, "gemini-3.7-flash");
  assert.equal(fallback?.previousFailureCategory, "stream_reset");
  assert.equal(fallback?.nextProvider, "groq");
  assert.equal(fallback?.nextModel, "openai/gpt-oss-120b");
});

test("observes new and reused Undici connections without logging headers or bodies", async () => {
  const events: Array<Record<string, unknown>> = [];
  __test.setSink((event) => events.push(event));
  const server = http.createServer((_request, response) => {
    response.writeHead(200, { "content-type": "text/event-stream" });
    response.end('data: {"choices":[{"delta":{"content":"ok"}}]}\n\n');
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  assert.ok(address && typeof address === "object");
  const url = `http://127.0.0.1:${address.port}/chat`;

  try {
    for (let index = 0; index < 3; index += 1) {
      const requestId = `123e4567-e89b-42d3-a456-42661417410${index}`;
      startPublicFunnelRequest(requestId, "example.ts.net");
      markRouteSelectionStarted(requestId, "omnia-coding");
      markRouteSelected(requestId, { provider: "test", model: "test/m" });
      const context = createUpstreamDiagnosticContext(requestId, "test", "m");
      const response = await runWithUpstreamDiagnosticContext(context, () =>
        observeDiagnosticFetch(() => proxyFetch(url, { method: "POST", body: "{}" }))
      );
      await response.text();
    }
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve()))
    );
    await closeDispatcherCache();
  }

  const acquired = events.filter((event) => event.event === "upstream_connection_acquired");
  assert.equal(acquired.length, 3);
  assert.ok(acquired.some((event) => event.connectionReuse === false));
  assert.ok(acquired.some((event) => event.connectionReuse === true));
  assert.ok(acquired.every((event) => event.connectionCreated === !event.connectionReuse));
  assert.ok(acquired.every((event) => event.httpProtocol === "http/1.1"));
  assert.ok(acquired.every((event) => typeof event.socketId === "string"));
  const selections = events.filter((event) => event.event === "upstream_dispatcher_selected");
  assert.equal(selections.length, 3);
  assert.ok(selections.every((event) => event.dispatcherId === "direct-reuse-aware-v1"));
  assert.ok(selections.every((event) => typeof event.poolId === "string"));
  assert.ok(selections.every((event) => typeof event.originHash === "string"));
  assert.equal(
    events.filter(
      (event) =>
        event.event === "upstream_response_body_terminal" &&
        event.responseBodyTerminal === "completed"
    ).length,
    3
  );
  assert.equal(
    events.filter(
      (event) => event.event === "upstream_connection_released" && event.connectionReleased === true
    ).length,
    3
  );
  assert.equal(events.filter((event) => event.event === "upstream_request_headers_sent").length, 3);
  assert.equal(JSON.stringify(events).includes("authorization"), false);
  assert.equal(JSON.stringify(events).includes("choices"), false);
});
