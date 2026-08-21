import type { Dispatcher } from "undici";
import { createHash } from "node:crypto";
import { channel } from "node:diagnostics_channel";

const DISPATCHER_CACHE_KEY = Symbol.for("omniroute.proxyDispatcher.cache");
const DEFAULT_DISPATCHER_KEY = Symbol.for("omniroute.proxyDispatcher.default");
const RETRY_DISPATCHER_KEY = Symbol.for("omniroute.proxyDispatcher.retry");

type DispatcherCache = Map<string, Dispatcher>;
type GlobalWithDispatcherCache = typeof globalThis & {
  [DISPATCHER_CACHE_KEY]?: DispatcherCache;
  [DEFAULT_DISPATCHER_KEY]?: Dispatcher;
  [RETRY_DISPATCHER_KEY]?: Dispatcher;
};

/**
 * Direct upstream fan-out dispatcher.
 *
 * A single Undici Agent configured with `connections > 1` should be enough in
 * theory, but real Codex `/backend-api/codex/responses` streams on Node 24 have
 * still been observed queuing every subsequent same-origin request until the
 * previous stream emits trailers. Using several one-connection Agents gives
 * each long SSE stream an independent pool/client and prevents one stream from
 * monopolizing the effective queue. This legacy round-robin helper is retained
 * for callers/tests; production direct egress uses ReuseAwareDispatcher below.
 */
class RoundRobinDispatcher {
  private readonly dispatchers: Dispatcher[];
  private nextIndex = 0;

  constructor(dispatchers: Dispatcher[]) {
    this.dispatchers = dispatchers;
  }

  dispatch(options: Dispatcher.DispatchOptions, handler: Dispatcher.DispatchHandler): boolean {
    const dispatcher = this.dispatchers[this.nextIndex % this.dispatchers.length];
    this.nextIndex = (this.nextIndex + 1) % this.dispatchers.length;
    return dispatcher.dispatch(options, handler);
  }

  close(callback?: () => void): Promise<void> | void {
    const done = Promise.all(this.dispatchers.map((dispatcher) => dispatcher.close())).then(
      () => undefined
    );
    if (callback) {
      done.then(callback);
      return;
    }
    return done;
  }

  destroy(
    errorOrCallback?: Error | null | (() => void),
    callback?: () => void
  ): Promise<void> | void {
    const callbackFn = typeof errorOrCallback === "function" ? errorOrCallback : callback;
    const error = typeof errorOrCallback === "function" ? null : (errorOrCallback ?? null);
    const done = Promise.all(this.dispatchers.map((dispatcher) => dispatcher.destroy(error))).then(
      () => undefined
    );
    if (callbackFn) {
      done.then(callbackFn);
      return;
    }
    return done;
  }
}

const MAX_ORIGIN_AFFINITIES = 256;
const ORIGIN_AFFINITY_IDLE_TTL_MS = 5 * 60_000;
const dispatcherSelectionChannel = channel("omniroute:dispatcher:selected");

export function publishDispatcherSelection(fields: {
  dispatcherId: string;
  poolId: string;
  originHash: string;
  dispatcherKind: string;
}): void {
  dispatcherSelectionChannel.publish(fields);
}

type OriginAffinity = {
  activeBySlot: number[];
  lastUsedAt: number;
};

function safeOriginKey(origin: Dispatcher.DispatchOptions["origin"]): string {
  try {
    const parsed = new URL(String(origin));
    return parsed.origin;
  } catch {
    return "invalid-origin";
  }
}

function hashSafeIdentity(value: string): string {
  return createHash("sha256").update(value).digest("hex").slice(0, 16);
}

/**
 * Keeps sequential same-origin traffic on a warm Agent while retaining the
 * independent one-connection fan-out required by concurrent long SSE streams.
 */
class ReuseAwareDispatcher {
  private readonly dispatchers: Dispatcher[];
  private readonly origins = new Map<string, OriginAffinity>();
  private overflowCursor = 0;

  constructor(dispatchers: Dispatcher[]) {
    this.dispatchers = dispatchers;
  }

  private pruneOrigins(now: number): void {
    if (this.origins.size < MAX_ORIGIN_AFFINITIES) return;
    for (const [origin, state] of this.origins) {
      if (
        now - state.lastUsedAt >= ORIGIN_AFFINITY_IDLE_TTL_MS &&
        state.activeBySlot.every((active) => active === 0)
      ) {
        this.origins.delete(origin);
      }
    }
    while (this.origins.size >= MAX_ORIGIN_AFFINITIES) {
      const oldestIdle = [...this.origins.entries()]
        .filter(([, state]) => state.activeBySlot.every((active) => active === 0))
        .sort((a, b) => a[1].lastUsedAt - b[1].lastUsedAt)[0];
      if (!oldestIdle) break;
      this.origins.delete(oldestIdle[0]);
    }
  }

  private affinity(origin: string, now: number): OriginAffinity {
    const existing = this.origins.get(origin);
    if (existing) {
      existing.lastUsedAt = now;
      return existing;
    }
    this.pruneOrigins(now);
    const state = {
      activeBySlot: Array.from({ length: this.dispatchers.length }, () => 0),
      lastUsedAt: now,
    };
    if (this.origins.size < MAX_ORIGIN_AFFINITIES) this.origins.set(origin, state);
    return state;
  }

  private selectSlot(state: OriginAffinity): number {
    // Slot zero is the stable warm path for sequential traffic. Only fan out
    // when it is genuinely occupied by an active response body.
    const idle = state.activeBySlot.findIndex((active) => active === 0);
    if (idle >= 0) return idle;

    const minimum = Math.min(...state.activeBySlot);
    for (let offset = 0; offset < state.activeBySlot.length; offset++) {
      const index = (this.overflowCursor + offset) % state.activeBySlot.length;
      if (state.activeBySlot[index] === minimum) {
        this.overflowCursor = (index + 1) % state.activeBySlot.length;
        return index;
      }
    }
    return 0;
  }

  dispatch(options: Dispatcher.DispatchOptions, handler: Dispatcher.DispatchHandler): boolean {
    const origin = safeOriginKey(options.origin);
    const now = Date.now();
    const state = this.affinity(origin, now);
    const slot = this.selectSlot(state);
    state.activeBySlot[slot]++;
    state.lastUsedAt = now;

    publishDispatcherSelection({
      dispatcherId: "direct-reuse-aware-v1",
      poolId: `direct-${hashSafeIdentity(origin)}-slot-${slot}`,
      originHash: hashSafeIdentity(origin),
      dispatcherKind: "direct",
    });

    let released = false;
    const release = () => {
      if (released) return;
      released = true;
      state.activeBySlot[slot] = Math.max(0, state.activeBySlot[slot] - 1);
      state.lastUsedAt = Date.now();
    };
    const terminalCallbacks = new Set<PropertyKey>([
      "onResponseEnd",
      "onResponseError",
      "onRequestUpgrade",
      // Compatibility with Dispatcher implementations that still use the
      // legacy callback names. release() is idempotent if both generations fire.
      "onComplete",
      "onError",
      "onUpgrade",
    ]);
    const boundCallbacks = new Map<PropertyKey, unknown>();
    const wrapped = new Proxy(handler as object, {
      get(target, property) {
        if (boundCallbacks.has(property)) return boundCallbacks.get(property);
        const value = Reflect.get(target, property, target);
        if (typeof value !== "function" && !terminalCallbacks.has(property)) return value;
        const callback = (...args: unknown[]) => {
          if (terminalCallbacks.has(property)) release();
          return typeof value === "function" ? value.apply(handler, args) : undefined;
        };
        boundCallbacks.set(property, callback);
        return callback;
      },
    }) as Dispatcher.DispatchHandler;

    try {
      return this.dispatchers[slot].dispatch(options, wrapped);
    } catch (error) {
      release();
      throw error;
    }
  }

  close(callback?: () => void): Promise<void> | void {
    const done = Promise.all(this.dispatchers.map((dispatcher) => dispatcher.close())).then(
      () => undefined
    );
    if (callback) {
      done.then(callback);
      return;
    }
    return done;
  }

  destroy(
    errorOrCallback?: Error | null | (() => void),
    callback?: () => void
  ): Promise<void> | void {
    const callbackFn = typeof errorOrCallback === "function" ? errorOrCallback : callback;
    const error = typeof errorOrCallback === "function" ? null : (errorOrCallback ?? null);
    const done = Promise.all(this.dispatchers.map((dispatcher) => dispatcher.destroy(error))).then(
      () => undefined
    );
    if (callbackFn) {
      done.then(callbackFn);
      return;
    }
    return done;
  }
}

export function createRoundRobinDispatcher(dispatchers: Dispatcher[]): Dispatcher {
  return new RoundRobinDispatcher(dispatchers) as unknown as Dispatcher;
}

export function createReuseAwareDispatcher(dispatchers: Dispatcher[]): Dispatcher {
  if (dispatchers.length === 0) throw new Error("At least one dispatcher is required");
  return new ReuseAwareDispatcher(dispatchers) as unknown as Dispatcher;
}

export function getDispatcherCache(): DispatcherCache {
  const globalWithCache = globalThis as GlobalWithDispatcherCache;
  if (!globalWithCache[DISPATCHER_CACHE_KEY]) {
    globalWithCache[DISPATCHER_CACHE_KEY] = new Map();
  }
  return globalWithCache[DISPATCHER_CACHE_KEY];
}

export function getDefaultCachedDispatcher(): Dispatcher | undefined {
  return (globalThis as GlobalWithDispatcherCache)[DEFAULT_DISPATCHER_KEY];
}

export function setDefaultCachedDispatcher(dispatcher: Dispatcher): void {
  (globalThis as GlobalWithDispatcherCache)[DEFAULT_DISPATCHER_KEY] = dispatcher;
}

export function getRetryCachedDispatcher(): Dispatcher | undefined {
  return (globalThis as GlobalWithDispatcherCache)[RETRY_DISPATCHER_KEY];
}

export function setRetryCachedDispatcher(dispatcher: Dispatcher): void {
  (globalThis as GlobalWithDispatcherCache)[RETRY_DISPATCHER_KEY] = dispatcher;
}

async function closeDispatcher(dispatcher: Dispatcher | undefined): Promise<void> {
  if (!dispatcher) return;
  try {
    await dispatcher.close();
  } catch {}
}

function takeCachedDispatchers(): Dispatcher[] {
  const cache = getDispatcherCache();
  const dispatchers = [...cache.values()];
  cache.clear();

  const globalWithCache = globalThis as GlobalWithDispatcherCache;
  if (globalWithCache[DEFAULT_DISPATCHER_KEY]) {
    dispatchers.push(globalWithCache[DEFAULT_DISPATCHER_KEY]);
  }
  if (globalWithCache[RETRY_DISPATCHER_KEY]) {
    dispatchers.push(globalWithCache[RETRY_DISPATCHER_KEY]);
  }
  delete globalWithCache[DEFAULT_DISPATCHER_KEY];
  delete globalWithCache[RETRY_DISPATCHER_KEY];
  return [...new Set(dispatchers)];
}

/**
 * Clear all cached proxy dispatchers.
 * Call this when proxy configuration changes to avoid stale connections.
 */
export function clearDispatcherCache(): void {
  for (const dispatcher of takeCachedDispatchers()) void closeDispatcher(dispatcher);
}

/** Awaitable shutdown variant used after in-flight requests have drained. */
export async function closeDispatcherCache(): Promise<void> {
  await Promise.all(takeCachedDispatchers().map((dispatcher) => closeDispatcher(dispatcher)));
}

export function __cacheProxyDispatcherForTest(key: string, dispatcher: Dispatcher): void {
  getDispatcherCache().set(key, dispatcher);
}
