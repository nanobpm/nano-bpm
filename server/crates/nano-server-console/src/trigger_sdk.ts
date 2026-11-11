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
  /** Fetch override (tests inject a stub transport). */
  fetchFn?: typeof fetch;
  /** Per-attempt deadline in ms covering the POST **and** the response-body
   * read (default 10 000). Without it a stalled body blocks the emit forever:
   * after a non-2xx it would prevent the retry, and after a 2xx it would
   * prevent the return even though the event is already durably persisted. On
   * expiry the body read is abandoned — a 2xx still resolves (durability was
   * confirmed by the status), a non-2xx/transport attempt fails and retries.
   * Set to `Infinity` to disable. */
  timeoutMs?: number;
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
const DEFAULT_TIMEOUT_MS = 10_000;

const defaultSleep = (ms: number): Promise<void> =>
  new Promise((r) => setTimeout(r, ms));

/** Race `read` against an abort deadline so a stalled response body can never
 * block the emit: the timer aborts the request (which rejects the pending
 * read) and the timeout wins the race even if the abort rejection is delayed.
 * Resolves `undefined` on expiry — the caller decides what a timed-out read
 * means for the status it already has. */
async function readWithDeadline(
  read: Promise<string>,
  ctrl: AbortController,
  timeoutMs: number,
): Promise<string | undefined> {
  if (!Number.isFinite(timeoutMs)) return await read.catch(() => undefined);
  let timedOut = false;
  const timer = setTimeout(() => {
    timedOut = true;
    ctrl.abort();
  }, timeoutMs);
  try {
    const winner = await Promise.race([
      read.then((text) => ({ text })),
      new Promise<{ expired: true }>((resolve) =>
        setTimeout(() => resolve({ expired: true }), timeoutMs),
      ),
    ]);
    if ("expired" in winner || timedOut) return undefined;
    return winner.text;
  } catch {
    return undefined;
  } finally {
    clearTimeout(timer);
  }
}

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
  // The cap is clamped to zero independently — NOT raised to `baseDelay`:
  // `Math.min` in the retry loop enforces it even below the base delay, so a
  // caller asking for `maxDelayMs: 100` with the default 250 ms base gets a
  // genuine 100 ms ceiling, not a silent override of its advertised cap.
  const maxDelay = Math.max(0, opts.maxDelayMs ?? DEFAULT_MAX_DELAY_MS);
  const timeoutMs = opts.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const sleep = opts.sleep ?? defaultSleep;
  const fetchImpl = opts.fetchFn ?? fetch;

  const headers: Record<string, string> = { "content-type": "application/json" };
  if (opts.token) headers["x-webhook-token"] = opts.token;
  if (opts.idempotencyKey) headers["idempotency-key"] = opts.idempotencyKey;
  const payload = JSON.stringify(body === undefined ? {} : body);

  let lastStatus = 0;
  let lastDetail = "no attempt made";
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    // One deadline per attempt, covering the POST and the response-body read
    // (the read is the unbounded part: undici streams it without a timeout).
    const ctrl = new AbortController();
    try {
      const res = await fetchImpl(url, {
        method: "POST",
        headers,
        body: payload,
        signal: ctrl.signal,
      });
      // Check the status BEFORE touching the body: a 2xx already confirms the
      // event is durably persisted (persist-before-ack), so an interrupted,
      // stalled, or unparseable acknowledgement body must never trigger a
      // retrying second POST — reading it is strictly best-effort.
      if (res.ok) {
        // `arrayBuffer()` (not `text()`): undici rejects a text() decode when
        // the socket dies mid-body, but a 2xx is already durable, so the read
        // must tolerate the truncation instead of failing the emit. The
        // deadline abandons a body that never finishes; a timed-out or failed
        // read yields `undefined` and the durable 2xx still resolves.
        const text = await readWithDeadline(
          res.arrayBuffer().then((buf) => new TextDecoder().decode(buf)),
          ctrl,
          timeoutMs,
        );
        if (text === undefined || text === "") return {};
        try {
          return JSON.parse(text);
        } catch {
          return {};
        }
      }
      lastStatus = res.status;
      const detail = await readWithDeadline(res.text(), ctrl, timeoutMs);
      lastDetail =
        detail === undefined
          ? `HTTP ${res.status}: <response body unreadable or timed out>`
          : `HTTP ${res.status}: ${detail.slice(0, 200)}`;
    } catch (err) {
      lastStatus = 0;
      lastDetail = `transport: ${String((err as Error)?.message ?? err)}`;
    } finally {
      // The attempt is settled — never leave the abort timer (or a pending
      // signal) holding resources past the retry backoff.
      ctrl.abort();
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
