// Trigger pack-driver SDK — the emit half of the pack-source contract
// (ADR 0025 §6). A trigger pack's out-of-process **driver** receives its
// context by environment (`NANOBPMN_HOOK_URL`, `NANOBPMN_WEBHOOK_TOKEN`, …) and
// POSTs each source event to the trigger ingress. This module is the checked,
// retrying emit helper a driver uses instead of a bare `fetch`, and the single
// source of truth for the contract's delivery rules:
//
//   - **2xx means durable.** The ingress persists the event into the durable
//     inbox *before* it acks (ADR 0025 §2: persist-before-ack), so a 2xx
//     response is proof the event cannot be lost across a crash.
//   - **A non-2xx means the event was NOT persisted.** The driver must not
//     move on as if it were. `emitEvent` treats any non-2xx (and any transport
//     error) as a failed emit: it retries idempotently (the `Idempotency-Key`
//     header collapses a retry of an event that *was* persisted to a no-op),
//     and after the attempts are exhausted it **throws** — a failed emit is
//     loud, never a silent drop (#1543: a driver that ignored the response
//     lost one of two events with no trace).
//
// A driver that cannot afford to throw (its source has no redelivery) should
// catch and surface the error itself — but it must not swallow it.

/** Options for one {@link emitEvent} call. */
export interface EmitOptions {
  /** Shared secret for a trigger declaring `auth` (the driver receives it as
   * `NANOBPMN_WEBHOOK_TOKEN`); sent as `X-Webhook-Token`. */
  token?: string;
  /** Dedup key; sent as `Idempotency-Key`. Supply a stable key per source
   * event so a retry after an ambiguous outcome (the response was lost after
   * the event was persisted) collapses to a no-op instead of a duplicate. */
  idempotencyKey?: string;
  /** Total attempts before giving up (default 5). The first retry waits
   * `baseDelayMs`; each subsequent retry doubles, capped at `maxDelayMs`. */
  maxAttempts?: number;
  /** First retry delay in ms (default 250). */
  baseDelayMs?: number;
  /** Retry-delay cap in ms (default 4000). */
  maxDelayMs?: number;
  /** Sleep override (tests inject a no-op). */
  sleep?: (ms: number) => Promise<void>;
}

/** The error {@link emitEvent} throws once every attempt failed. Carries the
 * last observed HTTP status (0 for a transport error) and response detail so
 * the failure names its cause. */
export class EmitError extends Error {
  readonly status: number;
  readonly attempts: number;
  constructor(message: string, status: number, attempts: number) {
    super(message);
    this.name = "EmitError";
    this.status = status;
    this.attempts = attempts;
  }
}

const DEFAULT_MAX_ATTEMPTS = 5;
const DEFAULT_BASE_DELAY_MS = 250;
const DEFAULT_MAX_DELAY_MS = 4_000;

const defaultSleep = (ms: number): Promise<void> =>
  new Promise((r) => setTimeout(r, ms));

/** POST one event to the trigger ingress, throwing unless it is durably
 * persisted. `url` is the driver's `NANOBPMN_HOOK_URL`. Resolves with the
 * parsed ack body (`{ enqueued, id? }`) on 2xx; retries a non-2xx / transport
 * failure with capped exponential backoff and throws {@link EmitError} after
 * `maxAttempts` failures. */
export async function emitEvent(
  url: string,
  body: unknown,
  opts: EmitOptions = {},
): Promise<unknown> {
  const maxAttempts = Math.max(1, opts.maxAttempts ?? DEFAULT_MAX_ATTEMPTS);
  const baseDelay = Math.max(0, opts.baseDelayMs ?? DEFAULT_BASE_DELAY_MS);
  const maxDelay = Math.max(baseDelay, opts.maxDelayMs ?? DEFAULT_MAX_DELAY_MS);
  const sleep = opts.sleep ?? defaultSleep;

  const headers: Record<string, string> = { "content-type": "application/json" };
  if (opts.token) headers["x-webhook-token"] = opts.token;
  if (opts.idempotencyKey) headers["idempotency-key"] = opts.idempotencyKey;
  const payload = JSON.stringify(body ?? {});

  let lastStatus = 0;
  let lastDetail = "no attempt made";
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const res = await fetch(url, { method: "POST", headers, body: payload });
      const text = await res.text();
      if (res.ok) {
        try {
          return text ? JSON.parse(text) : {};
        } catch {
          return {};
        }
      }
      lastStatus = res.status;
      lastDetail = `HTTP ${res.status}: ${text.slice(0, 200)}`;
    } catch (err) {
      lastStatus = 0;
      lastDetail = `transport: ${String((err as Error)?.message ?? err)}`;
    }
    if (attempt < maxAttempts) {
      const shift = Math.min(attempt - 1, 20);
      await sleep(Math.min(baseDelay * 2 ** shift, maxDelay));
    }
  }
  throw new EmitError(
    `emit to trigger ingress failed after ${maxAttempts} attempt(s): ${lastDetail}`,
    lastStatus,
    maxAttempts,
  );
}
