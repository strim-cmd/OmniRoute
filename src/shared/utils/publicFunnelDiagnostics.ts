import { randomUUID } from "node:crypto";
import { AsyncLocalStorage } from "node:async_hooks";
import { channel } from "node:diagnostics_channel";
import { performance } from "node:perf_hooks";
import { createLogger } from "@/shared/utils/logger";
import {
  classifyResponseOutputKinds,
  requiresVisibleTextResponse,
  type ResponseOutputKind,
} from "../../../open-sse/services/responseContract.ts";

type FailureCategory =
  | "dns"
  | "connect_timeout"
  | "connect_refused"
  | "tls"
  | "http_4xx"
  | "http_429"
  | "http_5xx"
  | "upstream_timeout"
  | "first_token_timeout"
  | "stream_reset"
  | "unexpected_eof"
  | "sse_parse"
  | "provider_error"
  | "rate_limit_wait"
  | "queue_wait"
  | "cancelled"
  | "no_visible_content"
  | "unknown";

type SafeEvent = Record<string, string | number | boolean | null | undefined> & {
  event: string;
  requestId: string;
};

type AttemptState = {
  index: number;
  provider: string;
  model: string;
  startedAt: number;
  connectionStartedAt?: number;
  connectionAcquiredAt?: number;
  connectionReused?: boolean;
  dispatcherId?: string;
  poolId?: string;
  originHash?: string;
  dispatcherKind?: string;
  socketId?: string;
  httpProtocol?: string;
  upstreamConnectionHeader?: "keep-alive" | "close" | "absent";
  responseBodyTerminal?: "completed" | "cancelled" | "errored";
  connectionReleased?: boolean;
  requestHeadersSentAt?: number;
  headersAt?: number;
  firstEventAt?: number;
  firstAnyOutputAt?: number;
  firstVisibleContentAt?: number;
  firstContentAt?: number;
  terminal?: "completed" | "failed";
  failureCategory?: FailureCategory;
};

type RequestState = {
  requestId: string;
  endpointHost: string;
  startedAt: number;
  logicalModel?: string;
  routeStartedAt?: number;
  routeSelectedAt?: number;
  routeProvider?: string;
  routeModel?: string;
  pendingFailureCategory?: FailureCategory;
  pendingFailureProvider?: string;
  pendingFailureModel?: string;
  firstClientEventAt?: number;
  firstClientAnyOutputAt?: number;
  firstClientVisibleContentAt?: number;
  firstClientContentAt?: number;
  terminal?: "completed" | "failed";
  attempts: AttemptState[];
};

const log = createLogger("public-funnel-timeline");
const requests = new Map<string, RequestState>();
type UpstreamDiagnosticContext = {
  requestId: string;
  provider: string;
  model: string;
  currentAttemptIndex: number | null;
  lastAttemptIndex: number | null;
  networkObserved: boolean;
  activeNetworkAttemptIndex: number | null;
  transportObserver?: {
    onRequestHeadersSent?: () => void;
    onResponseHeaders?: (status?: number) => void;
  } | null;
};
const upstreamDiagnosticStore = new AsyncLocalStorage<UpstreamDiagnosticContext>();
type UndiciRequestBinding = {
  requestId: string;
  attemptIndex: number;
  socket?: object;
  transportObserver?: UpstreamDiagnosticContext["transportObserver"];
};
const undiciRequests = new WeakMap<object, UndiciRequestBinding>();
const usedUndiciSockets = new WeakSet<object>();
type UndiciSocketState = {
  id: string;
  lastBinding?: UndiciRequestBinding;
  upstreamConnectionHeader?: "keep-alive" | "close" | "absent";
  released?: boolean;
  requestErrored?: boolean;
};
const undiciSocketStates = new WeakMap<object, UndiciSocketState>();
let nextSocketId = 1;
const MAX_ACTIVE_REQUESTS = 2_000;
const COMPLETED_RETENTION_MS = 5 * 60_000;
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
let testSink: ((event: SafeEvent) => void) | null = null;

function now(): number {
  return performance.now();
}

function elapsed(state: RequestState, at = now()): number {
  return Math.max(0, Math.round(at - state.startedAt));
}

function duration(from?: number, to?: number): number | undefined {
  if (from === undefined || to === undefined) return undefined;
  return Math.max(0, Math.round(to - from));
}

function safeLabel(value: unknown, maxLength: number): string | undefined {
  if (typeof value !== "string") return undefined;
  const normalized = value.replace(/[^a-zA-Z0-9_.:-]/g, "_").slice(0, maxLength);
  return normalized || undefined;
}

function emit(
  state: RequestState,
  event: string,
  fields: Omit<SafeEvent, "event" | "requestId"> = {}
) {
  const payload: SafeEvent = {
    event,
    requestId: state.requestId,
    endpointHost: state.endpointHost,
    logicalModel: state.logicalModel,
    elapsedMs: elapsed(state),
    ...fields,
  };
  if (testSink) testSink(payload);
  else log.info(payload, "public Funnel request timeline");
}

function getState(requestId: string): RequestState | undefined {
  return requests.get(requestId);
}

function trimStates() {
  while (requests.size >= MAX_ACTIVE_REQUESTS) {
    const oldest = requests.keys().next().value;
    if (!oldest) break;
    requests.delete(oldest);
  }
}

function scheduleCleanup(requestId: string) {
  const timer = setTimeout(() => requests.delete(requestId), COMPLETED_RETENTION_MS);
  timer.unref?.();
}

function headerValue(
  headers: Headers | { get?: (name: string) => string | null } | null,
  name: string
) {
  const value = headers?.get?.(name);
  return typeof value === "string" ? value.trim() : "";
}

export function resolveOmniaRequestId(
  headers: Headers | { get?: (name: string) => string | null } | null
): string {
  const incoming = headerValue(headers, "x-omnia-request-id");
  return UUID_RE.test(incoming) ? incoming.toLowerCase() : randomUUID();
}

export function startPublicFunnelRequest(requestId: string, endpointHost: string) {
  trimStates();
  const state: RequestState = {
    requestId,
    endpointHost: String(endpointHost || "unknown").slice(0, 255),
    startedAt: now(),
    attempts: [],
  };
  requests.set(requestId, state);
  emit(state, "request_received");
}

export function markPublicFunnelRequestValidated(requestId: string, logicalModel?: string | null) {
  const state = getState(requestId);
  if (!state) return;
  if (typeof logicalModel === "string" && logicalModel.trim()) {
    state.logicalModel = logicalModel.trim().slice(0, 255);
  }
  emit(state, "request_validated");
}

export function markRouteSelectionStarted(requestId: string, logicalModel?: string | null) {
  const state = getState(requestId);
  if (!state || state.routeStartedAt !== undefined) return;
  if (typeof logicalModel === "string" && logicalModel.trim()) {
    state.logicalModel = logicalModel.trim().slice(0, 255);
  }
  state.routeStartedAt = now();
  emit(state, "route_selection_started");
}

export function markRouteSelected(
  requestId: string,
  fields: { strategy?: string | null; provider?: string | null; model?: string | null } = {}
) {
  const state = getState(requestId);
  if (!state || state.routeSelectedAt !== undefined) return;
  state.routeSelectedAt = now();
  state.routeProvider = fields.provider?.slice(0, 120) ?? undefined;
  state.routeModel = fields.model?.slice(0, 255) ?? undefined;
  emit(state, "route_selected", {
    strategy: fields.strategy?.slice(0, 80),
    provider: state.routeProvider,
    model: state.routeModel,
    routeMs: duration(state.routeStartedAt, state.routeSelectedAt),
  });
}

export function markWaitCompleted(
  requestId: string,
  waitKind: "account_semaphore" | "rate_limit",
  startedAt: number,
  attempt?: number,
  failureCategory?: FailureCategory
) {
  const state = getState(requestId);
  if (!state) return;
  emit(state, waitKind === "rate_limit" ? "rate_limit_wait" : "queue_wait", {
    attempt,
    waitKind,
    durationMs: duration(startedAt, now()),
    failureCategory,
  });
  const previous = state.attempts.at(-1);
  if (failureCategory && (!previous || previous.terminal !== "failed")) {
    state.pendingFailureCategory = failureCategory;
    state.pendingFailureProvider = previous?.provider ?? state.routeProvider;
    state.pendingFailureModel = previous?.model ?? state.routeModel;
  }
}

export function monotonicNow(): number {
  return now();
}

type ComboTargetFields = {
  provider: string;
  model: string;
  targetIndex: number;
  remainingWaitMs?: number;
  readinessKey?: string | null;
};

export function markTargetRateLimitState(
  requestId: string,
  fields: ComboTargetFields & {
    state: "ready" | "cooldown" | "busy" | "unavailable";
    queueDepth?: number;
    reasonForWait?: string;
  }
) {
  const state = getState(requestId);
  if (!state) return;
  emit(state, "target_rate_limit_state", {
    provider: fields.provider.slice(0, 120),
    model: fields.model.slice(0, 255),
    targetIndex: fields.targetIndex,
    rateLimitState: fields.state,
    remainingWaitMs: fields.remainingWaitMs,
    readinessKey: fields.readinessKey?.slice(0, 80),
    queueDepth: fields.queueDepth,
    reasonForWait: fields.reasonForWait?.slice(0, 80),
  });
}

export function markRateLimitAdmission(
  requestId: string,
  fields: {
    provider: string;
    model: string;
    targetIndex?: number | null;
    readinessKey?: string | null;
    admissionKey?: string | null;
    readinessState?: string | null;
    admissionState: string;
    limiterStateAtAdmission: string;
    queueDepth: number;
    computedWaitMs?: number | null;
    reasonForWait: string;
  }
) {
  const state = getState(requestId);
  if (!state) return;
  emit(state, "rate_limit_admission", {
    provider: fields.provider.slice(0, 120),
    model: fields.model.slice(0, 255),
    targetIndex: fields.targetIndex ?? undefined,
    readinessKey: fields.readinessKey?.slice(0, 80),
    admissionKey: fields.admissionKey?.slice(0, 80),
    readinessState: fields.readinessState?.slice(0, 40),
    admissionState: fields.admissionState.slice(0, 40),
    limiterStateAtAdmission: fields.limiterStateAtAdmission.slice(0, 80),
    queueDepth: fields.queueDepth,
    computedWaitMs: fields.computedWaitMs ?? undefined,
    reasonForWait: fields.reasonForWait.slice(0, 80),
  });
}

export function markTargetSkippedCooldown(requestId: string, fields: ComboTargetFields) {
  const state = getState(requestId);
  if (!state) return;
  emit(state, "target_skipped_cooldown", {
    provider: fields.provider.slice(0, 120),
    model: fields.model.slice(0, 255),
    targetIndex: fields.targetIndex,
    remainingWaitMs: fields.remainingWaitMs,
    failureCategory: "rate_limit_wait",
  });
  state.pendingFailureCategory = "rate_limit_wait";
  state.pendingFailureProvider = fields.provider.slice(0, 120);
  state.pendingFailureModel = fields.model.slice(0, 255);
}

export function markTargetCompletedWithoutVisibleContent(
  requestId: string,
  fields: {
    provider: string;
    model: string;
    targetIndex?: number | null;
    outputKinds?: ResponseOutputKind[];
  }
) {
  const state = getState(requestId);
  if (!state) return;
  emit(state, "target_completed_without_visible_content", {
    provider: fields.provider.slice(0, 120),
    model: fields.model.slice(0, 255),
    targetIndex: fields.targetIndex ?? undefined,
    responseOutputKinds: fields.outputKinds?.join(",").slice(0, 120),
    failureCategory: "no_visible_content",
  });
  state.pendingFailureCategory = "no_visible_content";
  state.pendingFailureProvider = fields.provider.slice(0, 120);
  state.pendingFailureModel = fields.model.slice(0, 255);
}

export function markComboWaitStarted(
  requestId: string,
  fields: ComboTargetFields & { waitMs: number }
) {
  const state = getState(requestId);
  if (!state) return;
  emit(state, "combo_wait_started", {
    provider: fields.provider.slice(0, 120),
    model: fields.model.slice(0, 255),
    targetIndex: fields.targetIndex,
    remainingWaitMs: fields.remainingWaitMs,
    waitMs: fields.waitMs,
  });
}

export function markComboWaitCompleted(
  requestId: string,
  fields: ComboTargetFields & { waitMs: number }
) {
  const state = getState(requestId);
  if (!state) return;
  emit(state, "combo_wait_completed", {
    provider: fields.provider.slice(0, 120),
    model: fields.model.slice(0, 255),
    targetIndex: fields.targetIndex,
    remainingWaitMs: fields.remainingWaitMs,
    waitMs: fields.waitMs,
  });
}

export function markComboWaitBudgetExceeded(
  requestId: string,
  fields: ComboTargetFields & { budgetMs: number }
) {
  const state = getState(requestId);
  if (!state) return;
  emit(state, "combo_wait_budget_exceeded", {
    provider: fields.provider.slice(0, 120),
    model: fields.model.slice(0, 255),
    targetIndex: fields.targetIndex,
    remainingWaitMs: fields.remainingWaitMs,
    budgetMs: fields.budgetMs,
    failureCategory: "rate_limit_wait",
  });
}

export function markComboAttemptBudgetEvent(
  requestId: string,
  fields: {
    event: "started" | "expired_no_alternate" | "aborted";
    phase: "response_headers" | "visible_content";
    budgetMs: number;
    elapsedMs?: number;
    provider: string;
    model: string;
    targetIndex: number;
    alternateProvider?: string;
    alternateModel?: string;
    alternateTargetIndex?: number;
  }
) {
  const state = getState(requestId);
  if (!state) return;
  emit(state, `combo_attempt_budget_${fields.event}`, {
    provider: fields.provider.slice(0, 120),
    model: fields.model.slice(0, 255),
    targetIndex: fields.targetIndex,
    budgetPhase: fields.phase,
    budgetMs: fields.budgetMs,
    elapsedMs: fields.elapsedMs,
    alternateProvider: fields.alternateProvider?.slice(0, 120),
    alternateModel: fields.alternateModel?.slice(0, 255),
    alternateTargetIndex: fields.alternateTargetIndex,
    failureCategory:
      fields.event === "aborted"
        ? fields.phase === "visible_content"
          ? "first_token_timeout"
          : "upstream_timeout"
        : undefined,
  });
}

export function markUpstreamRequestStarted(
  requestId: string,
  provider: string,
  model: string
): number | null {
  const state = getState(requestId);
  if (!state) return null;
  if (state.routeSelectedAt === undefined) {
    markRouteSelected(requestId, { provider, model });
  }
  const previous = state.attempts.at(-1);
  const index = state.attempts.length + 1;
  if (previous && previous.terminal === "failed") {
    emit(state, "fallback_started", {
      previousAttempt: previous.index,
      previousFailureCategory: previous.failureCategory ?? "unknown",
      nextProvider: provider.slice(0, 120),
      nextModel: model.slice(0, 255),
    });
    state.pendingFailureCategory = undefined;
    state.pendingFailureProvider = undefined;
    state.pendingFailureModel = undefined;
  } else if (state.pendingFailureCategory) {
    emit(state, "fallback_started", {
      previousAttempt: previous?.index,
      previousProvider: state.pendingFailureProvider,
      previousModel: state.pendingFailureModel,
      previousFailureCategory: state.pendingFailureCategory,
      nextProvider: provider.slice(0, 120),
      nextModel: model.slice(0, 255),
    });
    state.pendingFailureCategory = undefined;
    state.pendingFailureProvider = undefined;
    state.pendingFailureModel = undefined;
  }
  const attempt: AttemptState = {
    index,
    provider: provider.slice(0, 120),
    model: model.slice(0, 255),
    startedAt: now(),
  };
  state.attempts.push(attempt);
  emit(state, "attempt_started", {
    attempt: index,
    provider: attempt.provider,
    model: attempt.model,
  });
  emit(state, "upstream_request_started", {
    attempt: index,
    provider: attempt.provider,
    model: attempt.model,
    queueBeforeUpstreamMs: duration(state.routeSelectedAt, attempt.startedAt),
  });
  return index;
}

function findAttempt(state: RequestState, attemptIndex: number | null | undefined) {
  return attemptIndex
    ? state.attempts.find((attempt) => attempt.index === attemptIndex)
    : state.attempts.at(-1);
}

function attemptTimingFields(attempt: AttemptState, at = now()) {
  return {
    dispatcherId: attempt.dispatcherId,
    poolId: attempt.poolId,
    originHash: attempt.originHash,
    dispatcherKind: attempt.dispatcherKind,
    connectionReuse: attempt.connectionReused,
    connectionCreated:
      attempt.connectionReused === undefined ? undefined : !attempt.connectionReused,
    socketLocalReuse: attempt.connectionReused,
    socketId: attempt.socketId,
    httpProtocol: attempt.httpProtocol,
    upstreamConnectionHeader: attempt.upstreamConnectionHeader,
    responseBodyTerminal: attempt.responseBodyTerminal,
    connectionReleased: attempt.connectionReleased,
    connectionSetupMs: duration(attempt.connectionStartedAt, attempt.connectionAcquiredAt),
    requestHeadersSentMs: duration(attempt.startedAt, attempt.requestHeadersSentAt),
    requestHeadersToResponseHeadersMs: duration(attempt.requestHeadersSentAt, attempt.headersAt),
    timeToHeadersMs: duration(attempt.startedAt, attempt.headersAt),
    headersToFirstEventMs: duration(attempt.headersAt, attempt.firstEventAt),
    headersToFirstAnyOutputMs: duration(attempt.headersAt, attempt.firstAnyOutputAt),
    headersToFirstVisibleContentMs: duration(attempt.headersAt, attempt.firstVisibleContentAt),
    headersToFirstContentMs: duration(attempt.headersAt, attempt.firstContentAt),
    attemptAnyOutputMs: duration(attempt.startedAt, attempt.firstAnyOutputAt),
    attemptTtftMs: duration(attempt.startedAt, attempt.firstContentAt),
    attemptTotalMs: duration(attempt.startedAt, at),
  };
}

function markUpstreamDispatcherSelected(
  requestId: string,
  attemptIndex: number | null,
  fields: {
    dispatcherId?: unknown;
    poolId?: unknown;
    originHash?: unknown;
    dispatcherKind?: unknown;
  }
) {
  const state = getState(requestId);
  const attempt = state ? findAttempt(state, attemptIndex) : undefined;
  if (!state || !attempt || attempt.dispatcherId !== undefined) return;
  attempt.dispatcherId = safeLabel(fields.dispatcherId, 80);
  attempt.poolId = safeLabel(fields.poolId, 100);
  attempt.originHash = safeLabel(fields.originHash, 32);
  attempt.dispatcherKind = safeLabel(fields.dispatcherKind, 40);
  emit(state, "upstream_dispatcher_selected", {
    attempt: attempt.index,
    provider: attempt.provider,
    model: attempt.model,
    dispatcherId: attempt.dispatcherId,
    poolId: attempt.poolId,
    originHash: attempt.originHash,
    dispatcherKind: attempt.dispatcherKind,
  });
}

export function markUpstreamConnectionStarted(requestId: string, attemptIndex: number | null) {
  const state = getState(requestId);
  const attempt = state ? findAttempt(state, attemptIndex) : undefined;
  if (!state || !attempt || attempt.connectionStartedAt !== undefined) return;
  attempt.connectionStartedAt = now();
  emit(state, "upstream_connection_started", {
    attempt: attempt.index,
    provider: attempt.provider,
    model: attempt.model,
  });
}

export function markUpstreamConnectionAcquired(
  requestId: string,
  attemptIndex: number | null,
  reused: boolean,
  metadata: { socketId?: string; httpProtocol?: string } = {}
) {
  const state = getState(requestId);
  const attempt = state ? findAttempt(state, attemptIndex) : undefined;
  if (!state || !attempt || attempt.connectionAcquiredAt !== undefined) return;
  attempt.connectionAcquiredAt = now();
  attempt.connectionReused = reused;
  attempt.socketId = metadata.socketId;
  attempt.httpProtocol = metadata.httpProtocol;
  emit(state, "upstream_connection_acquired", {
    attempt: attempt.index,
    provider: attempt.provider,
    model: attempt.model,
    connectionReuse: reused,
    connectionCreated: !reused,
    socketLocalReuse: reused,
    socketId: attempt.socketId,
    httpProtocol: attempt.httpProtocol,
    dispatcherId: attempt.dispatcherId,
    poolId: attempt.poolId,
    originHash: attempt.originHash,
    connectionSetupMs: duration(attempt.connectionStartedAt, attempt.connectionAcquiredAt),
  });
}

export function markUpstreamRequestHeadersSent(requestId: string, attemptIndex: number | null) {
  const state = getState(requestId);
  const attempt = state ? findAttempt(state, attemptIndex) : undefined;
  if (!state || !attempt || attempt.requestHeadersSentAt !== undefined) return;
  attempt.requestHeadersSentAt = now();
  emit(state, "upstream_request_headers_sent", {
    attempt: attempt.index,
    provider: attempt.provider,
    model: attempt.model,
    connectionReuse: attempt.connectionReused,
    requestHeadersSentMs: duration(attempt.startedAt, attempt.requestHeadersSentAt),
  });
}

export function markUpstreamResponseHeaders(
  requestId: string,
  attemptIndex: number | null,
  status: number,
  metadata: {
    upstreamConnectionHeader?: "keep-alive" | "close" | "absent";
  } = {}
) {
  const state = getState(requestId);
  if (!state) return;
  const attempt = findAttempt(state, attemptIndex);
  if (!attempt) return;
  if (attempt.headersAt !== undefined) return;
  attempt.headersAt = now();
  attempt.upstreamConnectionHeader = metadata.upstreamConnectionHeader;
  emit(state, "upstream_response_headers", {
    attempt: attempt.index,
    provider: attempt.provider,
    model: attempt.model,
    upstreamHttpStatus: status,
    upstreamHeadersMs: duration(attempt.startedAt, attempt.headersAt),
    ...attemptTimingFields(attempt, attempt.headersAt),
  });
  emit(state, "upstream_http_status", {
    attempt: attempt.index,
    provider: attempt.provider,
    model: attempt.model,
    httpStatus: status,
  });
  if (status < 200 || status >= 300) {
    markAttemptFailed(requestId, attempt.index, classifyFailure(undefined, status));
  }
}

function markResponseBodyTerminal(
  requestId: string,
  attemptIndex: number | null,
  terminal: "completed" | "cancelled" | "errored"
) {
  const state = getState(requestId);
  const attempt = state ? findAttempt(state, attemptIndex) : undefined;
  if (!state || !attempt || attempt.responseBodyTerminal !== undefined) return;
  attempt.responseBodyTerminal = terminal;
  emit(state, "upstream_response_body_terminal", {
    attempt: attempt.index,
    provider: attempt.provider,
    model: attempt.model,
    responseBodyTerminal: terminal,
    dispatcherId: attempt.dispatcherId,
    poolId: attempt.poolId,
    socketId: attempt.socketId,
  });
}

function markConnectionReleased(requestId: string, attemptIndex: number | null) {
  const state = getState(requestId);
  const attempt = state ? findAttempt(state, attemptIndex) : undefined;
  if (!state || !attempt || attempt.connectionReleased) return;
  attempt.connectionReleased = true;
  emit(state, "upstream_connection_released", {
    attempt: attempt.index,
    provider: attempt.provider,
    model: attempt.model,
    connectionReleased: true,
    dispatcherId: attempt.dispatcherId,
    poolId: attempt.poolId,
    socketId: attempt.socketId,
  });
}

function markConnectionClosed(
  binding: UndiciRequestBinding,
  socket: UndiciSocketState,
  hadError: boolean
) {
  const state = getState(binding.requestId);
  const attempt = state ? findAttempt(state, binding.attemptIndex) : undefined;
  if (!state || !attempt) return;
  const closeReason =
    hadError || socket.requestErrored
      ? "error"
      : socket.upstreamConnectionHeader === "close"
        ? "upstream_close"
        : socket.released
          ? "idle_or_peer_close"
          : "unreleased_close";
  emit(state, "connection_closed", {
    attempt: attempt.index,
    provider: attempt.provider,
    model: attempt.model,
    socketId: socket.id,
    closeReason,
    afterLogicalTerminal: state.terminal !== undefined,
  });
}

export function markAttemptFailed(
  requestId: string,
  attemptIndex: number | null,
  category: FailureCategory
) {
  const state = getState(requestId);
  if (!state) return;
  const attempt = findAttempt(state, attemptIndex);
  if (!attempt || attempt.terminal) return;
  attempt.terminal = "failed";
  attempt.failureCategory = category;
  const failedAt = now();
  const failurePhase =
    attempt.firstEventAt !== undefined
      ? "during_sse"
      : attempt.headersAt !== undefined
        ? "at_headers"
        : "before_headers";
  emit(state, "attempt_failed", {
    attempt: attempt.index,
    provider: attempt.provider,
    model: attempt.model,
    failureCategory: category,
    failurePhase,
    durationMs: duration(attempt.startedAt, failedAt),
    timeUntilFailureMs: duration(attempt.startedAt, failedAt),
    ...attemptTimingFields(attempt, failedAt),
  });
}

export function markAttemptCompleted(requestId: string, attemptIndex: number | null) {
  const state = getState(requestId);
  if (!state) return;
  const attempt = findAttempt(state, attemptIndex);
  if (!attempt || attempt.terminal) return;
  attempt.terminal = "completed";
  const completedAt = now();
  emit(state, "attempt_completed", {
    attempt: attempt.index,
    provider: attempt.provider,
    model: attempt.model,
    durationMs: duration(attempt.startedAt, completedAt),
    ...attemptTimingFields(attempt, completedAt),
  });
}

export function classifyFailure(error?: unknown, status?: number): FailureCategory {
  const code = String((error as { code?: unknown })?.code ?? "").toLowerCase();
  const name = String((error as { name?: unknown })?.name ?? "").toLowerCase();
  const message = String((error as { message?: unknown })?.message ?? error ?? "").toLowerCase();
  const text = `${code} ${name} ${message}`;
  if (text.includes("combo_rate_limit") || text.includes("rate_limit_queue")) {
    return "rate_limit_wait";
  }
  if (text.includes("abort") || text.includes("cancel")) return "cancelled";
  if (text.includes("enotfound") || text.includes("eai_again") || text.includes("dns"))
    return "dns";
  if (text.includes("econnrefused")) return "connect_refused";
  if (text.includes("tls") || text.includes("certificate") || text.includes("ssl")) return "tls";
  if (text.includes("first_token") || text.includes("stream_readiness_timeout")) {
    return "first_token_timeout";
  }
  if (text.includes("connect") && text.includes("timeout")) return "connect_timeout";
  if (text.includes("timeout")) return "upstream_timeout";
  if (text.includes("reset")) return "stream_reset";
  if (text.includes("eof") || text.includes("premature")) return "unexpected_eof";
  if (text.includes("sse") && text.includes("parse")) return "sse_parse";
  if (text.includes("provider")) return "provider_error";
  if (status === 429) return "http_429";
  if (typeof status === "number" && status >= 400 && status < 500) return "http_4xx";
  if (typeof status === "number" && status >= 500) return "http_5xx";
  return "unknown";
}

export function createUpstreamDiagnosticContext(
  requestId: string,
  provider: string,
  model: string,
  transportObserver?: UpstreamDiagnosticContext["transportObserver"]
): UpstreamDiagnosticContext {
  const initialAttempt = markUpstreamRequestStarted(requestId, provider, model);
  return {
    requestId,
    provider,
    model,
    currentAttemptIndex: initialAttempt,
    lastAttemptIndex: initialAttempt,
    networkObserved: false,
    activeNetworkAttemptIndex: null,
    transportObserver,
  };
}

export function runWithUpstreamDiagnosticContext<T>(
  context: UpstreamDiagnosticContext | null,
  handler: () => T
): T {
  return context ? upstreamDiagnosticStore.run(context, handler) : handler();
}

export async function observeDiagnosticFetch(handler: () => Promise<Response>): Promise<Response> {
  const context = upstreamDiagnosticStore.getStore();
  if (!context) return handler();
  context.networkObserved = true;
  const attemptIndex =
    context.currentAttemptIndex ??
    markUpstreamRequestStarted(context.requestId, context.provider, context.model);
  context.currentAttemptIndex = null;
  context.lastAttemptIndex = attemptIndex;
  context.activeNetworkAttemptIndex = attemptIndex;
  try {
    const response = await handler();
    markUpstreamResponseHeaders(context.requestId, attemptIndex, response.status);
    context.transportObserver?.onResponseHeaders?.(response.status);
    return response.ok && response.body
      ? observeUpstreamResponse(response, context.requestId, attemptIndex)
      : response;
  } catch (error) {
    markAttemptFailed(context.requestId, attemptIndex, classifyFailure(error));
    throw error;
  } finally {
    context.activeNetworkAttemptIndex = null;
  }
}

function activeUndiciBinding(): UndiciRequestBinding | null {
  const context = upstreamDiagnosticStore.getStore();
  if (!context || context.activeNetworkAttemptIndex === null) return null;
  return {
    requestId: context.requestId,
    attemptIndex: context.activeNetworkAttemptIndex,
    transportObserver: context.transportObserver,
  };
}

function undiciSocketState(socket: object): UndiciSocketState {
  const existing = undiciSocketStates.get(socket);
  if (existing) return existing;
  const state: UndiciSocketState = { id: `socket-${nextSocketId++}` };
  undiciSocketStates.set(socket, state);
  const closeEmitter = socket as {
    once?: (event: string, listener: (hadError?: boolean) => void) => void;
  };
  closeEmitter.once?.("close", (hadError = false) => {
    if (state.lastBinding) markConnectionClosed(state.lastBinding, state, hadError);
  });
  return state;
}

function responseHeaderValue(headers: unknown, targetName: string): string | undefined {
  if (!headers) return undefined;
  const lowerTarget = targetName.toLowerCase();
  if (Array.isArray(headers)) {
    for (let index = 0; index + 1 < headers.length; index += 2) {
      const name = Buffer.isBuffer(headers[index])
        ? headers[index].toString("latin1")
        : String(headers[index]);
      if (name.toLowerCase() !== lowerTarget) continue;
      return Buffer.isBuffer(headers[index + 1])
        ? headers[index + 1].toString("latin1")
        : String(headers[index + 1]);
    }
    return undefined;
  }
  if (typeof headers === "object") {
    for (const [name, value] of Object.entries(headers as Record<string, unknown>)) {
      if (name.toLowerCase() !== lowerTarget || value == null) continue;
      return Array.isArray(value) ? value.join(",") : String(value);
    }
  }
  return undefined;
}

function classifyConnectionHeader(headers: unknown): "keep-alive" | "close" | "absent" {
  const value = responseHeaderValue(headers, "connection")?.toLowerCase();
  if (!value) return "absent";
  if (value.includes("close")) return "close";
  if (value.includes("keep-alive")) return "keep-alive";
  return "absent";
}

function socketHttpProtocol(socket: object): string {
  const alpn = (socket as { alpnProtocol?: string | false }).alpnProtocol;
  return typeof alpn === "string" && alpn ? alpn : "http/1.1";
}

function subscribeUndiciDiagnostics() {
  channel("omniroute:dispatcher:selected").subscribe((message: unknown) => {
    try {
      const binding = activeUndiciBinding();
      if (binding) {
        markUpstreamDispatcherSelected(
          binding.requestId,
          binding.attemptIndex,
          message as {
            dispatcherId?: unknown;
            poolId?: unknown;
            originHash?: unknown;
            dispatcherKind?: unknown;
          }
        );
      }
    } catch {
      // Diagnostics are strictly best effort and must never affect fetch.
    }
  });
  channel("undici:request:create").subscribe((message: unknown) => {
    try {
      const request = (message as { request?: object })?.request;
      const binding = activeUndiciBinding();
      if (request && binding) undiciRequests.set(request, binding);
    } catch {
      // Diagnostics are strictly best effort and must never affect fetch.
    }
  });
  channel("undici:client:beforeConnect").subscribe(() => {
    try {
      const binding = activeUndiciBinding();
      if (binding) markUpstreamConnectionStarted(binding.requestId, binding.attemptIndex);
    } catch {
      // Diagnostics are strictly best effort and must never affect fetch.
    }
  });
  channel("undici:client:connected").subscribe((message: unknown) => {
    try {
      const socket = (message as { socket?: object })?.socket;
      const socketState = socket ? undiciSocketState(socket) : undefined;
      const binding = activeUndiciBinding();
      if (binding) {
        if (socketState) socketState.lastBinding = binding;
        markUpstreamConnectionAcquired(binding.requestId, binding.attemptIndex, false, {
          socketId: socketState?.id,
          httpProtocol: socket ? socketHttpProtocol(socket) : undefined,
        });
      }
    } catch {
      // Diagnostics are strictly best effort and must never affect fetch.
    }
  });
  channel("undici:client:sendHeaders").subscribe((message: unknown) => {
    try {
      const record = message as { request?: object; socket?: object };
      const binding = record.request ? undiciRequests.get(record.request) : undefined;
      const reused = record.socket ? usedUndiciSockets.has(record.socket) : false;
      const socketState = record.socket ? undiciSocketState(record.socket) : undefined;
      if (binding) {
        binding.socket = record.socket;
        if (socketState) {
          socketState.lastBinding = binding;
          socketState.released = false;
          socketState.requestErrored = false;
        }
        markUpstreamConnectionAcquired(binding.requestId, binding.attemptIndex, reused, {
          socketId: socketState?.id,
          httpProtocol: record.socket ? socketHttpProtocol(record.socket) : undefined,
        });
        markUpstreamRequestHeadersSent(binding.requestId, binding.attemptIndex);
        binding.transportObserver?.onRequestHeadersSent?.();
      }
      if (record.socket) usedUndiciSockets.add(record.socket);
    } catch {
      // Diagnostics are strictly best effort and must never affect fetch.
    }
  });
  channel("undici:request:headers").subscribe((message: unknown) => {
    try {
      const record = message as {
        request?: object;
        response?: { statusCode?: number; headers?: unknown };
      };
      const binding = record.request ? undiciRequests.get(record.request) : undefined;
      const status = record.response?.statusCode;
      if (binding && typeof status === "number") {
        const connectionHeader = classifyConnectionHeader(record.response?.headers);
        if (binding.socket) {
          undiciSocketState(binding.socket).upstreamConnectionHeader = connectionHeader;
        }
        markUpstreamResponseHeaders(binding.requestId, binding.attemptIndex, status, {
          upstreamConnectionHeader: connectionHeader,
        });
        binding.transportObserver?.onResponseHeaders?.(status);
      }
    } catch {
      // Diagnostics are strictly best effort and must never affect fetch.
    }
  });
  channel("undici:request:trailers").subscribe((message: unknown) => {
    try {
      const request = (message as { request?: object })?.request;
      const binding = request ? undiciRequests.get(request) : undefined;
      if (!binding) return;
      if (binding.socket) undiciSocketState(binding.socket).released = true;
      markConnectionReleased(binding.requestId, binding.attemptIndex);
    } catch {
      // Diagnostics are strictly best effort and must never affect fetch.
    }
  });
  channel("undici:request:error").subscribe((message: unknown) => {
    try {
      const request = (message as { request?: object })?.request;
      const binding = request ? undiciRequests.get(request) : undefined;
      if (!binding) return;
      if (binding.socket) undiciSocketState(binding.socket).requestErrored = true;
      markResponseBodyTerminal(binding.requestId, binding.attemptIndex, "errored");
    } catch {
      // Diagnostics are strictly best effort and must never affect fetch.
    }
  });
}

subscribeUndiciDiagnostics();

function containsContent(value: unknown): boolean {
  return classifyResponseOutputKinds(value).some((kind) => kind !== "metadata");
}

function createSseObserver(
  onEvent: () => void,
  onOutputKinds: (kinds: ResponseOutputKind[]) => void
) {
  const decoder = new TextDecoder();
  let buffer = "";
  return (chunk: Uint8Array) => {
    buffer += decoder.decode(chunk, { stream: true });
    if (buffer.length > 256 * 1024) buffer = buffer.slice(-64 * 1024);
    const frames = buffer.split(/\r?\n\r?\n/);
    buffer = frames.pop() ?? "";
    for (const frame of frames) {
      const dataLines = frame
        .split(/\r?\n/)
        .filter((line) => line.startsWith("data:"))
        .map((line) => line.slice(5).trim());
      if (dataLines.length === 0) continue;
      const data = dataLines.join("\n");
      if (!data || data === "[DONE]") continue;
      onEvent();
      try {
        onOutputKinds(classifyResponseOutputKinds(JSON.parse(data)));
      } catch {
        // The observer never changes stream semantics. Malformed frames are handled
        // by the existing protocol parser and are intentionally not logged verbatim.
      }
    }
  };
}

function wrapBody(
  body: ReadableStream<Uint8Array>,
  observe: (chunk: Uint8Array) => void,
  onDone: () => void,
  onFailure: (error: unknown) => void
) {
  const reader = body.getReader();
  let terminal = false;
  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      try {
        const result = await reader.read();
        if (result.done) {
          if (!terminal) {
            terminal = true;
            onDone();
          }
          controller.close();
          return;
        }
        observe(result.value);
        controller.enqueue(result.value);
      } catch (error) {
        if (!terminal) {
          terminal = true;
          onFailure(error);
        }
        controller.error(error);
      }
    },
    async cancel(reason) {
      if (!terminal) {
        terminal = true;
        onFailure(reason ?? new DOMException("Cancelled", "AbortError"));
      }
      await reader.cancel(reason).catch(() => undefined);
    },
  });
}

export function observeUpstreamResponse(
  response: Response,
  requestId: string,
  attemptIndex: number | null
): Response {
  const state = getState(requestId);
  const attempt = state ? findAttempt(state, attemptIndex) : undefined;
  if (!state || !attempt || !response.body) return response;
  let eventSeen = false;
  const outputKindsSeen = new Set<ResponseOutputKind>();
  const observer = createSseObserver(
    () => {
      if (eventSeen || attempt.firstEventAt !== undefined) return;
      eventSeen = true;
      attempt.firstEventAt = now();
      emit(state, "first_upstream_sse_event", {
        attempt: attempt.index,
        provider: attempt.provider,
        model: attempt.model,
        durationMs: duration(attempt.startedAt, attempt.firstEventAt),
        timeToFirstEventMs: duration(attempt.startedAt, attempt.firstEventAt),
        headersToFirstEventMs: duration(attempt.headersAt, attempt.firstEventAt),
      });
    },
    (kinds) => {
      for (const kind of kinds) {
        if (!outputKindsSeen.has(kind)) {
          outputKindsSeen.add(kind);
          emit(state, "response_output_kind", {
            attempt: attempt.index,
            provider: attempt.provider,
            model: attempt.model,
            direction: "upstream",
            responseOutputKind: kind,
          });
        }
      }
      if (kinds.some((kind) => kind !== "metadata") && attempt.firstAnyOutputAt === undefined) {
        attempt.firstAnyOutputAt = now();
        emit(state, "first_upstream_any_output", {
          attempt: attempt.index,
          provider: attempt.provider,
          model: attempt.model,
          attemptAnyOutputMs: duration(attempt.startedAt, attempt.firstAnyOutputAt),
          headersToFirstAnyOutputMs: duration(attempt.headersAt, attempt.firstAnyOutputAt),
        });
      }
      if (!kinds.includes("visible_text") || attempt.firstVisibleContentAt !== undefined) {
        return;
      }
      attempt.firstVisibleContentAt = now();
      attempt.firstContentAt = attempt.firstVisibleContentAt;
      const fields = {
        attempt: attempt.index,
        provider: attempt.provider,
        model: attempt.model,
        upstreamTtftMs: duration(attempt.startedAt, attempt.firstVisibleContentAt),
        attemptTtftMs: duration(attempt.startedAt, attempt.firstVisibleContentAt),
        headersToFirstContentMs: duration(attempt.headersAt, attempt.firstVisibleContentAt),
      };
      emit(state, "first_upstream_visible_content", fields);
      emit(state, "first_upstream_content_token", fields);
    }
  );
  const body = wrapBody(
    response.body,
    observer,
    () => {
      markResponseBodyTerminal(requestId, attempt.index, "completed");
      markAttemptCompleted(requestId, attempt.index);
    },
    (error) => {
      const terminal = classifyFailure(error) === "cancelled" ? "cancelled" : "errored";
      markResponseBodyTerminal(requestId, attempt.index, terminal);
      markAttemptFailed(requestId, attempt.index, classifyFailure(error));
    }
  );
  return new Response(body, {
    status: response.status,
    statusText: response.statusText,
    headers: response.headers,
  });
}

function terminalFields(state: RequestState, at: number) {
  const firstAttempt = state.attempts[0];
  const contentAttempt = state.attempts.find((attempt) => attempt.firstContentAt !== undefined);
  const anyOutputAttempt = state.attempts.find((attempt) => attempt.firstAnyOutputAt !== undefined);
  return {
    routeMs: duration(state.routeStartedAt, state.routeSelectedAt),
    queueBeforeUpstreamMs: duration(state.routeSelectedAt, firstAttempt?.startedAt),
    upstreamHeadersMs: duration(firstAttempt?.startedAt, firstAttempt?.headersAt),
    upstreamTtftMs: duration(contentAttempt?.startedAt, contentAttempt?.firstContentAt),
    upstreamAnyOutputMs: duration(anyOutputAttempt?.startedAt, anyOutputAttempt?.firstAnyOutputAt),
    serverToClientFirstAnyOutputMs: duration(state.startedAt, state.firstClientAnyOutputAt),
    serverToClientFirstTokenMs: duration(state.startedAt, state.firstClientContentAt),
    totalMs: duration(state.startedAt, at),
    attemptCount: state.attempts.length,
  };
}

export function observeClientResponse(
  response: Response,
  requestId: string,
  clientSignal?: AbortSignal,
  options: { visibleTextRequired?: boolean } = {}
): Response {
  const state = getState(requestId);
  if (!state) return response;
  emit(state, "response_headers_received", { httpStatus: response.status });
  const headers = new Headers(response.headers);
  headers.set("X-Correlation-Id", requestId);
  if (!response.body) {
    const at = now();
    state.terminal = response.ok ? "completed" : "failed";
    emit(state, response.ok ? "request_completed" : "request_failed", {
      httpStatus: response.status,
      failureCategory: response.ok ? undefined : classifyFailure(undefined, response.status),
      ...terminalFields(state, at),
    });
    scheduleCleanup(requestId);
    return new Response(null, {
      status: response.status,
      statusText: response.statusText,
      headers,
    });
  }

  let eventSeen = false;
  let anyOutputSeen = false;
  let visibleContentSeen = false;
  const outputKindsSeen = new Set<ResponseOutputKind>();
  const visibleTextRequired = options.visibleTextRequired === true || requiresVisibleTextResponse();
  const isSse = (headers.get("content-type") || "").toLowerCase().includes("text/event-stream");
  const observer = isSse
    ? createSseObserver(
        () => {
          if (eventSeen) return;
          eventSeen = true;
          state.firstClientEventAt = now();
          emit(state, "response_first_event_sent_to_client", {
            serverToClientFirstEventMs: duration(state.startedAt, state.firstClientEventAt),
          });
        },
        (kinds) => {
          for (const kind of kinds) {
            if (!outputKindsSeen.has(kind)) {
              outputKindsSeen.add(kind);
              emit(state, "response_output_kind", {
                direction: "client",
                responseOutputKind: kind,
              });
            }
          }
          if (!anyOutputSeen && kinds.some((kind) => kind !== "metadata")) {
            anyOutputSeen = true;
            state.firstClientAnyOutputAt = now();
            emit(state, "response_first_any_output_sent_to_client", {
              serverToClientFirstAnyOutputMs: duration(
                state.startedAt,
                state.firstClientAnyOutputAt
              ),
            });
          }
          if (visibleContentSeen || !kinds.includes("visible_text")) return;
          visibleContentSeen = true;
          state.firstClientVisibleContentAt = now();
          state.firstClientContentAt = state.firstClientVisibleContentAt;
          const fields = {
            serverToClientFirstTokenMs: duration(
              state.startedAt,
              state.firstClientVisibleContentAt
            ),
          };
          emit(state, "response_first_visible_content_sent_to_client", fields);
          emit(state, "response_first_content_sent_to_client", fields);
        }
      )
    : (chunk: Uint8Array) => {
        if (visibleContentSeen || chunk.byteLength === 0) return;
        anyOutputSeen = true;
        visibleContentSeen = true;
        state.firstClientAnyOutputAt = now();
        state.firstClientVisibleContentAt = state.firstClientAnyOutputAt;
        state.firstClientContentAt = now();
        emit(state, "response_first_any_output_sent_to_client", {
          serverToClientFirstAnyOutputMs: duration(state.startedAt, state.firstClientAnyOutputAt),
        });
        emit(state, "response_first_visible_content_sent_to_client", {
          serverToClientFirstTokenMs: duration(state.startedAt, state.firstClientContentAt),
        });
        emit(state, "response_first_content_sent_to_client", {
          serverToClientFirstTokenMs: duration(state.startedAt, state.firstClientContentAt),
        });
      };

  let clientAborted = clientSignal?.aborted ?? false;
  const finish = (error?: unknown) => {
    if (state.terminal) return;
    clientSignal?.removeEventListener("abort", onClientAbort);
    const endedWithoutContent =
      isSse && (visibleTextRequired ? !visibleContentSeen : !anyOutputSeen);
    const failed = error !== undefined || clientAborted || !response.ok || endedWithoutContent;
    state.terminal = failed ? "failed" : "completed";
    const at = now();
    const failureCategory = !failed
      ? undefined
      : error !== undefined
        ? classifyFailure(error, response.status)
        : clientAborted
          ? "cancelled"
          : !response.ok
            ? classifyFailure(undefined, response.status)
            : "unexpected_eof";
    emit(state, failed ? "request_failed" : "request_completed", {
      httpStatus: response.status,
      failureCategory,
      ...terminalFields(state, at),
    });
    scheduleCleanup(requestId);
  };
  const onClientAbort = () => {
    clientAborted = true;
    finish(new DOMException("Client cancelled request", "AbortError"));
  };
  clientSignal?.addEventListener("abort", onClientAbort, { once: true });
  const body = wrapBody(
    response.body,
    observer,
    () => finish(),
    (error) => finish(error)
  );
  return new Response(body, { status: response.status, statusText: response.statusText, headers });
}

export function markPublicFunnelRequestFailed(requestId: string, error: unknown) {
  const state = getState(requestId);
  if (!state || state.terminal) return;
  state.terminal = "failed";
  const at = now();
  emit(state, "request_failed", {
    failureCategory: classifyFailure(error),
    ...terminalFields(state, at),
  });
  scheduleCleanup(requestId);
}

export const __test = {
  clear() {
    requests.clear();
    testSink = null;
  },
  setSink(sink: ((event: SafeEvent) => void) | null) {
    testSink = sink;
  },
  containsContent,
};
