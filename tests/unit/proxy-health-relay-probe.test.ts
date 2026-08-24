import test from "node:test";

import assert from "node:assert/strict";

import { shouldUseDispatcherHealthProbe } from "../../src/lib/proxyHealth/probeMode.ts";

test("edge relays skip generic dispatcher health probe", () => {
  assert.equal(shouldUseDispatcherHealthProbe("cloudflare"), false);

  assert.equal(shouldUseDispatcherHealthProbe("vercel"), false);

  assert.equal(shouldUseDispatcherHealthProbe("deno"), false);
});

test("normal proxies keep generic dispatcher health probe", () => {
  assert.equal(shouldUseDispatcherHealthProbe("http"), true);

  assert.equal(shouldUseDispatcherHealthProbe("https"), true);

  assert.equal(shouldUseDispatcherHealthProbe("socks5"), true);
});
