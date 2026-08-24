import type {
  ComboAttemptBudgetProfileSettings,
  ComboAttemptBudgetSettings,
} from "../../../src/lib/resilience/settings/types.ts";

export type ComboAttemptBudgetPhase = "response_headers" | "visible_content";

export type ComboAttemptBudgetAlternate = {
  targetIndex: number;
  provider: string;
  model: string;
};

export type ComboAttemptBudgetTrigger = {
  phase: ComboAttemptBudgetPhase;
  budgetMs: number;
  elapsedMs: number;
  alternate: ComboAttemptBudgetAlternate;
};

export type ComboAttemptBudgetEvent =
  | { event: "started"; phase: ComboAttemptBudgetPhase; budgetMs: number }
  | {
      event: "expired_no_alternate";
      phase: ComboAttemptBudgetPhase;
      budgetMs: number;
      elapsedMs: number;
    }
  | ({ event: "aborted" } & ComboAttemptBudgetTrigger);

export type UpstreamTransportPhaseObserver = {
  onRequestHeadersSent: () => void;
  onResponseHeaders: (status?: number) => void;
};

export class ComboAttemptBudgetExceededError extends Error {
  readonly code = "COMBO_ATTEMPT_BUDGET_EXCEEDED";

  constructor(readonly trigger: ComboAttemptBudgetTrigger) {
    super(`combo_${trigger.phase}_budget_timeout`);
    this.name = "TimeoutError";
  }
}

function positiveMs(value: unknown, fallback: number): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : fallback;
}

/** Resolve the most-specific combo policy without mutating shared settings. */
export function resolveComboAttemptBudgetProfile(
  settings: ComboAttemptBudgetSettings,
  provider: string,
  modelStr: string,
  rawModel: string
): ComboAttemptBudgetProfileSettings {
  const providerKey = provider.trim().toLowerCase();
  const modelKeys = [
    modelStr.trim().toLowerCase(),
    providerKey && rawModel ? `${providerKey}/${rawModel.trim().toLowerCase()}` : "",
    rawModel.trim().toLowerCase(),
  ].filter(Boolean);
  const modelProfile = modelKeys.map((key) => settings.models[key]).find(Boolean);
  return modelProfile ?? settings.providers[providerKey] ?? settings.default;
}

/**
 * Supervises one real upstream POST. Timers are armed by the transport''s
 * request-headers event, never by route selection or connection setup.
 *
 * Expiry is fail-open until a later combo target is presently admissible. The
 * owner must await the aborted transport/response-body settlement before it
 * starts that target.
 */
export class ComboAttemptBudgetGuard {
  private requestHeadersSentAt: number | null = null;
  private responseHeadersSeen = false;
  private visibleContentSeen = false;
  private disposed = false;
  private timer: ReturnType<typeof setTimeout> | null = null;
  private timerGeneration = 0;
  private triggered: ComboAttemptBudgetTrigger | null = null;

  readonly transportObserver: UpstreamTransportPhaseObserver = {
    onRequestHeadersSent: () => this.markRequestHeadersSent(),
    onResponseHeaders: () => this.markResponseHeaders(),
  };

  constructor(
    private readonly options: {
      controller: AbortController;
      profile: ComboAttemptBudgetProfileSettings;
      recheckIntervalMs: number;
      findAdmissibleAlternate: () => Promise<ComboAttemptBudgetAlternate | null>;
      onEvent?: (event: ComboAttemptBudgetEvent) => void;
      now?: () => number;
    }
  ) {}

  markRequestHeadersSent(): void {
    if (this.disposed || this.visibleContentSeen || this.options.controller.signal.aborted) return;
    this.requestHeadersSentAt = this.now();
    this.responseHeadersSeen = false;
    this.schedule("response_headers", this.options.profile.responseHeadersMs, true);
  }

  markResponseHeaders(): void {
    if (
      this.disposed ||
      this.responseHeadersSeen ||
      this.visibleContentSeen ||
      this.requestHeadersSentAt === null
    )
      return;
    this.responseHeadersSeen = true;
    this.scheduleAtAbsoluteBudget("visible_content", this.options.profile.firstVisibleContentMs);
  }

  markVisibleContent(): void {
    if (this.visibleContentSeen) return;
    this.visibleContentSeen = true;
    this.clearTimer();
  }

  getTrigger(): ComboAttemptBudgetTrigger | null {
    return this.triggered;
  }

  dispose(): void {
    this.disposed = true;
    this.clearTimer();
  }

  private now(): number {
    return this.options.now ? this.options.now() : performance.now();
  }

  private clearTimer(): void {
    if (this.timer !== null) clearTimeout(this.timer);
    this.timer = null;
    this.timerGeneration++;
  }

  private scheduleAtAbsoluteBudget(phase: ComboAttemptBudgetPhase, budgetMs: number): void {
    if (this.requestHeadersSentAt === null) return;
    const remaining = Math.max(0, budgetMs - (this.now() - this.requestHeadersSentAt));
    this.schedule(phase, remaining, true);
  }

  private schedule(phase: ComboAttemptBudgetPhase, delayMs: number, emitStarted = false): void {
    this.clearTimer();
    if (this.disposed || this.visibleContentSeen || this.triggered) return;
    const generation = this.timerGeneration;
    const budgetMs =
      phase === "response_headers"
        ? this.options.profile.responseHeadersMs
        : this.options.profile.firstVisibleContentMs;
    if (emitStarted) this.options.onEvent?.({ event: "started", phase, budgetMs });
    this.timer = setTimeout(
      () => {
        if (generation !== this.timerGeneration) return;
        this.timer = null;
        void this.onTimer(phase, budgetMs, generation);
      },
      Math.max(0, delayMs)
    );
  }

  private async onTimer(
    phase: ComboAttemptBudgetPhase,
    budgetMs: number,
    generation: number
  ): Promise<void> {
    if (
      generation !== this.timerGeneration ||
      this.disposed ||
      this.visibleContentSeen ||
      this.triggered ||
      this.requestHeadersSentAt === null ||
      (phase === "response_headers" && this.responseHeadersSeen) ||
      this.options.controller.signal.aborted
    ) {
      return;
    }

    const elapsedMs = Math.max(0, Math.round(this.now() - this.requestHeadersSentAt));
    let alternate: ComboAttemptBudgetAlternate | null = null;
    try {
      alternate = await this.options.findAdmissibleAlternate();
    } catch {
      // Readiness is best effort. A probe failure must never destroy the only
      // active generation; retry while the same phase remains unproductive.
    }

    if (
      generation !== this.timerGeneration ||
      this.disposed ||
      this.visibleContentSeen ||
      this.triggered ||
      (phase === "response_headers" && this.responseHeadersSeen) ||
      this.options.controller.signal.aborted
    ) {
      return;
    }

    if (alternate) {
      const trigger = { phase, budgetMs, elapsedMs, alternate };
      this.triggered = trigger;
      this.options.onEvent?.({ event: "aborted", ...trigger });
      this.options.controller.abort(new ComboAttemptBudgetExceededError(trigger));
      return;
    }

    this.options.onEvent?.({ event: "expired_no_alternate", phase, budgetMs, elapsedMs });
    this.schedule(phase, positiveMs(this.options.recheckIntervalMs, 500));
  }
}
