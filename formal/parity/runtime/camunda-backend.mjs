// Camunda 8 backend for the two-backend parity runner (#1260).
//
// Speaks the Camunda v2 REST API against a live Camunda 8 (c8run or a
// Testcontainers/Docker Zeebe gateway). This is a REAL client — never a stub. It
// is exercised only when a runtime is provisioned (`CAMUNDA_REST_ADDRESS` set);
// the runner is skip-tolerant otherwise (see run.mjs / the CI job).
//
// Determinism (the #1240 constraint "the driver serialises timing so Zeebe runs
// are deterministic", "no retries"):
//   * The Zeebe clock is PINNED at the start of every scenario (`PUT /v2/clock`),
//     so wall-clock time never advances under a running instance; `advanceTime`
//     moves the pinned clock by an exact delta. No timer fires except by an
//     explicit scenario tick.
//   * Job activation LONG-POLLS (`requestTimeout`): a single blocking call waits
//     for the job to become activatable, serialising the step ordering without a
//     retry loop. A driver that has to retry to see a job is a defect to
//     root-cause, not to paper over.
//   * `createProcessInstance` runs with `awaitCompletion`, so the final variables
//     and completion are read from the broker directly — no Elasticsearch /
//     secondary storage is required. That is why this backend only `provides`
//     `completed` + `variables`; the richer element/flow history needs the query
//     API (secondary storage) and is the extension surface a future slice wires
//     in.

import { emptyObservation, MAX_JOBS_TO_ACTIVATE } from "./observation.mjs";

/**
 * A backend CLEANUP failure (a process-instance cancellation or the clock reset
 * inside `reset()`), distinguished from a scenario divergence. A reset that
 * fails has NOT restored isolation — an uncancelled instance's reused job types
 * stay activatable and would corrupt later scenarios' observations and any
 * minimal reproduction shrunk from them — so the runner must ABORT the Camunda
 * phase on this, not record one more `camunda:error` divergence and continue
 * (#1246 review). `exhaustive.mjs`'s runtimePhase rethrows on `instanceof
 * BackendCleanupError` instead of swallowing it as a per-scenario divergence;
 * run.mjs's per-scenario loop has no such catch, so a cleanup failure already
 * propagates out of `runScenario` and aborts the run there.
 */
export class BackendCleanupError extends Error {
  constructor(message, options) {
    super(message, options);
    this.name = "BackendCleanupError";
  }
}

const DEFAULT_ACTIVATE_TIMEOUT_MS = 60_000;
const DEFAULT_LONG_POLL_MS = 30_000;
// The `awaitCompletion` create call blocks until the instance completes. A
// serialised scenario drives several long-polling job activations back-to-back
// (each up to DEFAULT_LONG_POLL_MS), so the completion wait must cover the whole
// scenario, not Camunda's short default (5s here) — otherwise the create times
// out (504) before `observe()` can read the final result (#1260 review).
const DEFAULT_COMPLETION_TIMEOUT_MS = 120_000;
// How long a `publishMessage` step keeps its message buffered for a subscription
// still being opened (#1246). Bounded by the instance completion deadline: a
// message nobody consumed by then belongs to a scenario that already failed.
const PUBLISH_TTL_MS = DEFAULT_COMPLETION_TIMEOUT_MS;
// The liveness probe must have a deadline. A reachable-but-wedged endpoint (a
// provisioned service that accepts the TCP connection but never answers
// `/topology`, or a network path that blackholes the request) would otherwise
// leave `fetch()` pending forever, hanging the supposedly skip-tolerant CI job
// indefinitely. Bounding the probe turns that hang into the documented
// unreachable-runtime skip (#1260 review).
const DEFAULT_PING_TIMEOUT_MS = 10_000;
// Poll interval while a `settled.ack` correlation waits for its subscription.
const ACK_READINESS_POLL_MS = 50;
// Every ordinary (non-long-poll) REST call — deploy, clock ops, message/signal
// correlation, job completion — must also carry a deadline. `ping()` alone is
// not enough: without a client-side timeout a reachable-but-wedged endpoint that
// accepts the TCP connection but never answers any of THESE requests would leave
// `fetch()` pending forever, hanging the change-gated (skip-tolerant) CI job
// exactly as an unbounded probe would (#1260 review).
const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;
// A long-polling call (job activation, the awaitCompletion create) is bounded
// too, but its CLIENT deadline must exceed the server-side long-poll window so
// the broker's own timeout response returns normally — the client abort is only
// a backstop for a connection that wedges mid-poll. Add a fixed margin on top of
// the server window rather than racing it (which would turn a legitimate
// long-poll into a spurious client abort).
const REQUEST_TIMEOUT_MARGIN_MS = 15_000;

/** A non-2xx Camunda REST response, carrying its status and parsed body. */
export class CamundaHttpError extends Error {
  constructor(message, { status, body }) {
    super(message);
    this.name = "CamundaHttpError";
    this.status = status;
    this.body = body;
  }
}

function requireOk(res, body, what) {
  if (!res.ok) {
    throw new CamundaHttpError(
      `camunda ${what} failed: HTTP ${res.status} ${res.statusText} — ${
        typeof body === "string" ? body : JSON.stringify(body)
      }`,
      { status: res.status, body },
    );
  }
}

/**
 * Whether a create-instance failure is the awaitCompletion deadline (HTTP 504
 * `DEADLINE_EXCEEDED`) — the instance is still running, not a broken backend.
 */
export function isAwaitCompletionTimeout(err) {
  return (
    err instanceof CamundaHttpError &&
    err.status === 504 &&
    err.body?.title === "DEADLINE_EXCEEDED"
  );
}

export class CamundaBackend {
  static id = "camunda";

  /**
   * @param {object} opts
   * @param {string} opts.address base REST address, e.g. `http://localhost:8080`
   * @param {string} [opts.token] optional bearer token
   * @param {string} [opts.basicAuth] optional `user:pass` for Basic auth
   * @param {number} [opts.pingTimeoutMs] liveness-probe deadline (default
   *   DEFAULT_PING_TIMEOUT_MS); a wedged endpoint that never answers within it is
   *   treated as the unreachable-runtime skip.
   * @param {number} [opts.requestTimeoutMs] deadline for ordinary (non-long-poll)
   *   REST calls (default DEFAULT_REQUEST_TIMEOUT_MS); long-poll calls extend it
   *   by REQUEST_TIMEOUT_MARGIN_MS over their server-side window.
   */
  constructor({ address, token, basicAuth, pingTimeoutMs, requestTimeoutMs } = {}) {
    if (!address) throw new Error("CamundaBackend requires a REST address");
    // `CAMUNDA_REST_ADDRESS` is documented repo-wide as the full REST base ending
    // in `/v2` (e.g. `http://localhost:8080/v2`, agent_brief.rs / USERGUIDE), but
    // a bare host (`http://localhost:8080`) is also accepted for convenience.
    // Normalise both to a single `/v2` base — appending `/v2` unconditionally
    // would turn the documented value into `/v2/v2` and miss the gateway (#1260).
    const trimmed = address.replace(/\/+$/, "");
    this.base = /\/v2$/.test(trimmed) ? trimmed : `${trimmed}/v2`;
    this.token = token;
    this.basicAuth = basicAuth;
    this.pingTimeoutMs = pingTimeoutMs ?? DEFAULT_PING_TIMEOUT_MS;
    this.requestTimeoutMs = requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS;
    // Only the fields a bare gateway (no secondary storage) authoritatively
    // exposes via the v2 REST API.
    this.provides = new Set(["completed", "variables"]);
    this.pinnedClockMs = null;
  }

  headers(extra = {}) {
    const h = { "content-type": "application/json", accept: "application/json", ...extra };
    if (this.token) h.authorization = `Bearer ${this.token}`;
    else if (this.basicAuth) {
      h.authorization = `Basic ${Buffer.from(this.basicAuth).toString("base64")}`;
    }
    return h;
  }

  // Handles whose awaitCompletion create is still in flight OR whose instance
  // may still be running. `reset()` aborts the create (so a scenario that fails
  // before `observe()` cannot keep the process alive until the create's ~135 s
  // deadline) and cancels every instance key the scenario learned from its
  // activated jobs, so a still-running instance cannot leave jobs for a later
  // run of the same job type (#1260 review). A handle whose create REJECTED
  // (e.g. an awaitCompletion timeout on a stuck scenario) STAYS in the set
  // until `reset()` confirms the cancellation: dropping it on rejection would
  // orphan a still-running instance whose leftover jobs (generated graphs reuse
  // job types like `T1`) a later scenario could activate, contaminating results
  // (#1246 review). An instance that never yielded a job has no known key (the
  // bare gateway exposes no instance search); it is parked before its first
  // job, which the next run's first activation of that type would complete
  // along with its own (the activation cap leaves room), so the residue is
  // bounded and never silent.
  #inflight = new Set();

  async #json(method, path, body, what, { timeoutMs, signal } = {}) {
    // Bound every REST call. A caller may override the deadline for a long-poll
    // (job activation, awaitCompletion create) whose server-side window exceeds
    // the ordinary request timeout; otherwise a wedged endpoint hangs CI (#1260).
    const res = await fetch(`${this.base}${path}`, {
      method,
      headers: this.headers(),
      body: body === undefined ? undefined : JSON.stringify(body),
      signal: signal
        ? AbortSignal.any([signal, AbortSignal.timeout(timeoutMs ?? this.requestTimeoutMs)])
        : AbortSignal.timeout(timeoutMs ?? this.requestTimeoutMs),
    });
    const text = await res.text();
    let parsed = text;
    if (text) {
      try {
        parsed = JSON.parse(text);
      } catch {
        // keep raw text for the error message
      }
    }
    requireOk(res, parsed, what);
    return parsed;
  }

  /**
   * Liveness probe used by the runner to decide whether to run this backend.
   * A TRANSPORT failure (DNS, refused connection, TLS) means the runtime is
   * unreachable — the documented skip case — so it resolves `false`. An HTTP-level
   * error (a reachable-but-misconfigured gateway, e.g. 401/403) is NOT swallowed:
   * it throws so a configured-runtime problem fails loudly instead of silently
   * skipping the differential (#1260 review).
   *
   * The probe is bounded by DEFAULT_PING_TIMEOUT_MS: a wedged endpoint that
   * accepts the connection but never responds aborts and is treated as the same
   * unreachable-runtime skip (a hang would otherwise stall the skip-tolerant CI
   * job forever). An actual HTTP response still propagates its status.
   */
  async ping() {
    let res;
    try {
      res = await fetch(`${this.base}/topology`, {
        headers: this.headers(),
        signal: AbortSignal.timeout(this.pingTimeoutMs),
      });
    } catch {
      // Transport failure (DNS, refused connection, TLS) OR the probe deadline
      // firing on a wedged endpoint — both mean "runtime unreachable" → skip.
      return false;
    }
    requireOk(res, await res.text().catch(() => ""), "topology");
    return true;
  }

  async init() {
    return this;
  }

  async deploy(xml) {
    const form = new FormData();
    form.append(
      "resources",
      new Blob([xml], { type: "application/xml" }),
      "process.bpmn",
    );
    const headers = {};
    if (this.token) headers.authorization = `Bearer ${this.token}`;
    else if (this.basicAuth) {
      headers.authorization = `Basic ${Buffer.from(this.basicAuth).toString("base64")}`;
    }
    const res = await fetch(`${this.base}/deployments`, {
      method: "POST",
      headers,
      body: form,
      signal: AbortSignal.timeout(this.requestTimeoutMs),
    });
    const text = await res.text();
    let body = text;
    try {
      body = JSON.parse(text);
    } catch {
      /* raw */
    }
    requireOk(res, body, "deploy");
  }

  async #pinClock(epochMs) {
    this.pinnedClockMs = epochMs;
    await this.#json("PUT", "/clock", { timestamp: epochMs }, "pin clock");
  }

  async start(processId, variables) {
    // Pin the clock to a fixed epoch so every scenario runs from an identical,
    // deterministic starting time regardless of when CI executes it.
    await this.#pinClock(0);
    // Fire awaitCompletion WITHOUT awaiting the promise: the HTTP call blocks
    // until the instance completes, but we still need to drive its jobs
    // concurrently. `observe` awaits the promise to read the final state.
    //
    // No early CREATION BARRIER is possible on a bare gateway: `awaitCompletion`
    // holds the HTTP response until the instance COMPLETES, so it exposes no
    // "instance created / subscription open" acknowledgement to await here.
    // Getting one would require either dropping awaitCompletion (then the final
    // variables `observe` needs are only readable via the query API) or polling
    // the query API for the subscription — both the secondary storage this
    // bare-gateway backend deliberately excludes (see the file header). It is the
    // same extension surface the richer element/flow history is deferred to.
    //
    // Why the current corpus is safe without it, and why the gap is LOUD (never a
    // silent wrong differential) if a future scenario hits it:
    //   * The driver serialises steps and every seed scenario's FIRST step is
    //     `activateAndComplete`, whose job-activation LONG-POLL only returns once
    //     the instance has reached that job's wait-state — a natural creation
    //     barrier before any later step runs.
    //   * A message/signal-first scenario (`correlateMessage`/`broadcastSignal`
    //     as the first step) has no such barrier: v2 correlation does not buffer,
    //     so a dispatch racing ahead of the subscription is dropped. But then the
    //     instance simply never leaves its catch, the `awaitCompletion` create
    //     times out (DEFAULT_COMPLETION_TIMEOUT_MS) and is observed as
    //     `completed: false` (#1988) — a loud differential mismatch against a
    //     completing nano run, never a bogus match. Adding such a scenario is
    //     gated on wiring the query-API barrier above (#1260 review).
    const handle = { processId, abort: new AbortController(), instanceKeys: new Set() };
    this.#inflight.add(handle);
    const pending = this.#json(
      "POST",
      "/process-instances",
      {
        processDefinitionId: processId,
        variables: variables ?? {},
        awaitCompletion: true,
        requestTimeout: DEFAULT_COMPLETION_TIMEOUT_MS,
      },
      "create process instance",
      {
        timeoutMs: DEFAULT_COMPLETION_TIMEOUT_MS + REQUEST_TIMEOUT_MARGIN_MS,
        signal: handle.abort.signal,
      },
    );
    // A create that SETTLED (completed instance) leaves nothing to cancel —
    // drop it. A REJECTED create (awaitCompletion timeout on a stuck scenario,
    // a transport failure) may still have a running instance: keep the handle
    // in #inflight so `reset()` cancels every key it learned (#1246 review).
    pending.then(
      () => this.#inflight.delete(handle),
      () => {},
    );
    // Surface a create-time rejection eagerly rather than as an unhandled
    // rejection; the stored promise is still awaited in `observe`.
    pending.catch(() => {});
    handle.pending = pending;
    return handle;
  }

  async activateAndComplete(handle, jobType, variables) {
    const activated = await this.#json(
      "POST",
      "/jobs/activation",
      {
        type: jobType,
        maxJobsToActivate: MAX_JOBS_TO_ACTIVATE,
        timeout: DEFAULT_ACTIVATE_TIMEOUT_MS,
        worker: "parity-runner",
        requestTimeout: DEFAULT_LONG_POLL_MS,
      },
      "activate jobs",
      { timeoutMs: DEFAULT_LONG_POLL_MS + REQUEST_TIMEOUT_MARGIN_MS },
    );
    const jobs = activated.jobs ?? [];
    if (jobs.length === 0) {
      throw new Error(
        `no '${jobType}' job became activatable within ${DEFAULT_LONG_POLL_MS}ms — ` +
          `serialisation defect, not a transient (do not retry)`,
      );
    }
    for (const job of jobs) {
      if (job.processInstanceKey != null) handle.instanceKeys.add(String(job.processInstanceKey));
      const jobKey = job.jobKey ?? job.key;
      await this.#json(
        "POST",
        `/jobs/${jobKey}/completion`,
        { variables: variables ?? {} },
        "complete job",
      );
    }
    return jobs.length;
  }

  // Complete EXACTLY ONE job of `jobType` (#1246): the bounded-exhaustive
  // scenarios address a single job activation, so a run that holds two
  // same-type jobs (a parallel split through an exclusive merge) is still
  // driven deterministically — one completion per step, in the reference's
  // order. `maxJobsToActivate: 1` keeps the step to one job even when more are
  // activatable.
  async completeOneJob(handle, jobType, variables) {
    const job = await this.#activateOne(jobType);
    if (job.processInstanceKey != null) handle.instanceKeys.add(String(job.processInstanceKey));
    await this.#json(
      "POST",
      `/jobs/${job.jobKey ?? job.key}/completion`,
      { variables: variables ?? {} },
      "complete job",
    );
    return 1;
  }

  // Set (override) process-instance variables mid-run (#1246): a per-activation
  // route schedule is delivered as the gateway's `route_<id>` list before the
  // activation that reads it. The v2 API updates a scope's variables via
  // `PUT /element-instances/{elementInstanceKey}/variables`, which accepts the
  // PROCESS-instance key for the root scope (spec/element-instances.yaml:127).
  async setVariables(handle, variables) {
    const key = [...handle.instanceKeys][0];
    if (key == null) {
      throw new Error(
        "setVariables before the instance key is known (no job activated yet) — " +
          "the bare gateway exposes no instance lookup; schedule the route in the " +
          "start variables or after the first job activation",
      );
    }
    await this.#json(
      "PUT",
      `/element-instances/${key}/variables`,
      { variables: variables ?? {} },
      "set variables",
    );
  }

  async correlateMessage(_handle, name, correlationKey, variables) {
    await this.#json(
      "POST",
      "/messages/correlation",
      { name, correlationKey, variables: variables ?? {} },
      "correlate message",
    );
  }

  // A BUFFERED correlation (#1246). v2 `/messages/correlation` never buffers,
  // and a bare gateway exposes no barrier for "the previous step's subscription
  // is open" (see `start`), so correlating right after a job completion races
  // the subscription. Publishing with a time-to-live closes the race without a
  // sleep or a retry: the broker keeps the message until the subscription opens
  // and correlates it then. A scenario using this op must give every scenario
  // its own correlation key, so a buffered message can never reach another
  // scenario's instance on the shared runtime.
  //
  // The publication endpoint does not wait for correlation
  // (spec/messages.yaml), so the response alone is no barrier: the NEXT step
  // could run while the message is still buffered, and an opposite
  // message/job command order would execute in the same order on Camunda —
  // missing coverage or producing a misleading difference from the settled
  // reference execution. `settled` is the driver's barrier: when the scenario
  // knows the correlation resumes the instance into a job of type
  // `settled.jobType`, publish then LONG-POLL one activation of that type —
  // the job exists only after the message correlated and the follow-on work
  // activated — then FAIL the activation back (with retries) so the driver's
  // own completion step re-activates and completes it. A correlation that
  // leads straight to the end event has no job to barrier on; there the
  // awaitCompletion create settling IS the barrier, and the driver waits it
  // via `observe`.
  //
  // `settled.ack` (#1246 review): the correlation creates NO job but the
  // instance SURVIVES (a parallel branch keeps running), so there is no
  // correlation-created job to barrier on and no completion to await. Without
  // an acknowledgement the driver advances to the next command while the
  // buffered message is still in flight, and Camunda can run that command
  // (e.g. complete the still-waiting T1) BEFORE the message correlates —
  // inverting the scripted order. So the message is delivered through the
  // SYNCHRONOUS `/messages/correlation` INSTEAD of a buffered publication: it
  // returns only once a subscription correlated, so the delivery is its own
  // acknowledgement and no second message is ever sent.
  //
  // An explicit `timeToLive` / `messageId` (#1969) is published AS GIVEN: the
  // scenario is exercising Zeebe message buffering itself (TTL expiry, TTL 0,
  // messageId dedup). `expectRejected` asserts the publish is refused with
  // HTTP 409 (ALREADY_EXISTS: the messageId is still buffered).
  async publishMessage(handle, name, correlationKey, variables, settled, publication = {}) {
    const { timeToLive, messageId, expectRejected } = publication;
    if (timeToLive != null || messageId != null) {
      if (settled != null) {
        throw new Error("a timeToLive/messageId publication takes no `settled` barrier");
      }
      const body = { name, correlationKey, variables: variables ?? {} };
      body.timeToLive = timeToLive ?? 0;
      if (messageId != null) body.messageId = messageId;
      if (expectRejected) {
        const res = await fetch(`${this.base}/messages/publication`, {
          method: "POST",
          headers: this.headers(),
          body: JSON.stringify(body),
          signal: AbortSignal.timeout(this.requestTimeoutMs),
        });
        const text = await res.text();
        if (res.status !== 409) {
          throw new Error(
            `publish of '${name}' (messageId ${messageId}) was expected to be rejected ` +
              `with 409 ALREADY_EXISTS, got HTTP ${res.status}: ${text}`,
          );
        }
        return;
      }
      await this.#json("POST", "/messages/publication", body, "publish message");
      return;
    }
    if (expectRejected) throw new Error("expectRejected needs a messageId or timeToLive publication");
    if (settled?.ack) {
      // Correlate SYNCHRONOUSLY instead of publishing: `/messages/correlation`
      // is itself the delivery and returns only once the message correlated,
      // so it is the acknowledgement. Publishing first and then correlating
      // would send a SECOND message (404 once the first consumed the
      // subscription, or a stray buffered message if it wins — #1246 review).
      //
      // Unbuffered correlation answers 404 — and delivers NOTHING — while the
      // subscription is not open yet (e.g. a message-first order racing the
      // instance's start; spec/messages.yaml). So wait for subscription
      // readiness by re-attempting ONLY on 404, bounded by the completion
      // deadline: each 404 is a no-op, so exactly one message is ever
      // delivered (the first 2xx). Any other error fails loudly at once.
      const deadline = Date.now() + DEFAULT_COMPLETION_TIMEOUT_MS;
      for (;;) {
        const res = await fetch(`${this.base}/messages/correlation`, {
          method: "POST",
          headers: this.headers(),
          body: JSON.stringify({ name, correlationKey, variables: variables ?? {} }),
          signal: AbortSignal.timeout(this.requestTimeoutMs),
        });
        const text = await res.text();
        if (res.status === 404 && Date.now() < deadline) {
          await new Promise((r) => setTimeout(r, ACK_READINESS_POLL_MS));
          continue;
        }
        requireOk(res, text, "correlate message");
        return;
      }
    }
    await this.#json(
      "POST",
      "/messages/publication",
      {
        name,
        correlationKey,
        timeToLive: PUBLISH_TTL_MS,
        variables: variables ?? {},
      },
      "publish message",
    );
    if (settled?.jobType) {
      const job = await this.#activateOne(settled.jobType);
      if (job.processInstanceKey != null) handle.instanceKeys.add(String(job.processInstanceKey));
      await this.#json(
        "POST",
        `/jobs/${job.jobKey ?? job.key}/failure`,
        // JobFailRequest (spec/jobs.yaml) names the back-off field
        // `retryBackOff` (int64) and sets `additionalProperties: false`, so the
        // v1-style `retryBackOffMs` is schema-invalid and a conforming endpoint
        // rejects the fail-back — stalling the barrier mid-scenario (#1246).
        { retries: 1, retryBackOff: 0 },
        "fail back barrier job",
      );
    }
  }

  async #activateOne(jobType) {
    const activated = await this.#json(
      "POST",
      "/jobs/activation",
      {
        type: jobType,
        maxJobsToActivate: 1,
        timeout: DEFAULT_ACTIVATE_TIMEOUT_MS,
        worker: "parity-runner",
        requestTimeout: DEFAULT_LONG_POLL_MS,
      },
      "activate jobs",
      { timeoutMs: DEFAULT_LONG_POLL_MS + REQUEST_TIMEOUT_MARGIN_MS },
    );
    const jobs = activated.jobs ?? [];
    if (jobs.length === 0) {
      throw new Error(
        `no '${jobType}' job became activatable within ${DEFAULT_LONG_POLL_MS}ms — ` +
          `the published message did not correlate into the expected job (a ` +
          `correlation barrier miss), or the scenario is mis-ordered (do not retry)`,
      );
    }
    return jobs[0];
  }

  async broadcastSignal(_handle, name, variables) {
    await this.#json(
      "POST",
      "/signals/broadcast",
      { signalName: name, variables: variables ?? {} },
      "broadcast signal",
    );
  }

  async advanceTime(_handle, ms) {
    await this.#pinClock((this.pinnedClockMs ?? 0) + ms);
  }

  // Cancel the running instance (#1673). The bare gateway learns an instance's
  // key only from the jobs it activates (`handle.instanceKeys`); a scenario
  // that cancels before any job was activated has no key to cancel, so it
  // fails LOUDLY rather than leaving a live instance behind. Cancelled
  // scenarios are nano-only in the generated corpus (the awaitCompletion
  // create never answers `processCompleted` for them), so this is the
  // interface-completeness path, not a differential one.
  async cancelInstance(handle) {
    if (handle.instanceKeys.size === 0) {
      throw new Error("cancelInstance: no process instance key learned yet (no job was activated)");
    }
    for (const key of handle.instanceKeys) {
      await this.#json("POST", `/process-instances/${key}/cancellation`, {}, "cancel process instance");
    }
  }

  async observe(handle) {
    const obs = emptyObservation();
    let result;
    try {
      result = await handle.pending;
    } catch (err) {
      // An awaitCompletion create that hits its deadline on an instance that is
      // still ACTIVE (a token correctly parked on a task — e.g. Zeebe
      // correlating one publish once per process, #1988) is an OBSERVATION, not
      // a backend error: Camunda reports it as HTTP 504 DEADLINE_EXCEEDED, so
      // the instance did not complete. Its final variables were never read, so
      // they are marked `unobserved` and the differential skips them (#1794).
      // Every other create failure still fails loudly.
      if (!isAwaitCompletionTimeout(err)) throw err;
      obs.completed = false;
      obs.unobserved = ["variables"];
      return obs;
    }
    // A successful Camunda v2 awaitCompletion create response IS the completion
    // signal: HTTP 200 carries the result variables but NOT a `processCompleted`
    // field (an await-completion timeout surfaces as an error status, not a 200 +
    // flag). So an ABSENT flag means completed — defaulting it to `true` avoids a
    // spurious `completed` mismatch on every successful Camunda scenario. Only an
    // explicitly supplied `false` marks an incomplete run, preserved for
    // forward-compat with a server that does report the flag (#1260 review).
    obs.completed = result?.processCompleted !== false;
    obs.variables = result?.variables ?? {};
    // The bounded-exhaustive scenarios drive routing through `route_*` start /
    // set variables — the driver's own schedule, not a process outcome. Drop
    // them from the observation so the oracle compares the variables the
    // PROCESS produced. NanoBackend.observe does the same; without symmetry the
    // differential reports a false mismatch on every routed scenario (#1246).
    for (const k of Object.keys(obs.variables)) {
      if (k.startsWith("route_")) delete obs.variables[k];
    }
    return obs;
  }

  async reset() {
    // Abort any create still in flight (a scenario that failed before
    // `observe()`); otherwise it holds the process open until its deadline.
    const unfinished = [...this.#inflight];
    this.#inflight.clear();
    for (const handle of unfinished) {
      handle.abort.abort();
      for (const key of handle.instanceKeys) {
        try {
          await this.#json(
            "POST",
            `/process-instances/${key}/cancellation`,
            {},
            "cancel process instance",
          );
        } catch (err) {
          // 404: the instance finished in the meantime — nothing left to cancel.
          // Any other failure means the instance may STILL be live: its reused
          // job types stay activatable and would corrupt later scenarios, so
          // surface it as a cleanup failure the runner aborts on (#1246 review).
          if (!/HTTP 404/.test(String(err?.message))) {
            throw new BackendCleanupError(
              `cancel process instance ${key} failed: ${err?.message ?? err}`,
              { cause: err },
            );
          }
        }
      }
    }
    // A clock reset is the correct — and sufficient — per-scenario reset for this
    // backend's execution model, NOT a papered-over engine wipe:
    //   * Scenarios are SERIALISED and run to completion-or-throw: `start` fires
    //     the create with `awaitCompletion`, and `runScenario` awaits `observe`
    //     (hence the create) before the next scenario begins, so scenario N is
    //     fully finished before N+1's reset runs.
    //   * A COMPLETED instance leaves nothing to drain: Camunda removes a
    //     terminated instance's remaining jobs/tokens, and `activateAndComplete`
    //     completes every job it activates in the same call — so no activatable
    //     job of a shared type survives into the next scenario.
    //   * An INCOMPLETE scenario cannot silently contaminate the next: a create
    //     that can't complete (e.g. more same-type jobs than the activation cap,
    //     or a stuck token) times out and THROWS, failing the run loudly instead
    //     of proceeding to the next scenario. Isolation failures are loud here,
    //     never silent.
    // The only sticky global mutation a completed scenario leaves is the PINNED
    // clock — which is exactly what we rewind. That failure must be loud too: a
    // swallowed `/clock/reset` (401/500/503) could leave the global clock pinned
    // while the runner reports success, so — unlike the transport-only skip in
    // `ping()` — it is NOT caught (#1260 review). Surface it as a cleanup
    // failure so the runner aborts the phase rather than record a divergence
    // (#1246 review).
    try {
      await this.#json("POST", "/clock/reset", undefined, "reset clock");
    } catch (err) {
      throw new BackendCleanupError(`reset clock failed: ${err?.message ?? err}`, {
        cause: err,
      });
    }
    this.pinnedClockMs = null;
  }

  async close() {
    await this.reset();
  }
}
