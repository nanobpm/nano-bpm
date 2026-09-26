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

const DEFAULT_ACTIVATE_TIMEOUT_MS = 60_000;
const DEFAULT_LONG_POLL_MS = 30_000;
// The `awaitCompletion` create call blocks until the instance completes. A
// serialised scenario drives several long-polling job activations back-to-back
// (each up to DEFAULT_LONG_POLL_MS), so the completion wait must cover the whole
// scenario, not Camunda's short default (5s here) — otherwise the create times
// out (504) before `observe()` can read the final result (#1260 review).
const DEFAULT_COMPLETION_TIMEOUT_MS = 120_000;

function requireOk(res, body, what) {
  if (!res.ok) {
    throw new Error(
      `camunda ${what} failed: HTTP ${res.status} ${res.statusText} — ${
        typeof body === "string" ? body : JSON.stringify(body)
      }`,
    );
  }
}

export class CamundaBackend {
  static id = "camunda";

  /**
   * @param {object} opts
   * @param {string} opts.address base REST address, e.g. `http://localhost:8080`
   * @param {string} [opts.token] optional bearer token
   * @param {string} [opts.basicAuth] optional `user:pass` for Basic auth
   */
  constructor({ address, token, basicAuth } = {}) {
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

  async #json(method, path, body, what) {
    const res = await fetch(`${this.base}${path}`, {
      method,
      headers: this.headers(),
      body: body === undefined ? undefined : JSON.stringify(body),
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
   */
  async ping() {
    let res;
    try {
      res = await fetch(`${this.base}/topology`, { headers: this.headers() });
    } catch {
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
    );
    // Surface a create-time rejection eagerly rather than as an unhandled
    // rejection; the stored promise is still awaited in `observe`.
    pending.catch(() => {});
    return { processId, pending };
  }

  async activateAndComplete(_handle, jobType, variables) {
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
    );
    const jobs = activated.jobs ?? [];
    if (jobs.length === 0) {
      throw new Error(
        `no '${jobType}' job became activatable within ${DEFAULT_LONG_POLL_MS}ms — ` +
          `serialisation defect, not a transient (do not retry)`,
      );
    }
    for (const job of jobs) {
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

  async correlateMessage(_handle, name, correlationKey, variables) {
    await this.#json(
      "POST",
      "/messages/correlation",
      { name, correlationKey, variables: variables ?? {} },
      "correlate message",
    );
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

  async observe(handle) {
    const obs = emptyObservation();
    const result = await handle.pending;
    // Preserve the create response's `processCompleted` flag verbatim: this repo
    // deliberately returns HTTP 200 with `processCompleted: false` when an
    // await-completion request times out (server/src/main.rs), so hard-coding
    // `true` would turn an incomplete run into a false-match observation (#1260).
    obs.completed = result?.processCompleted === true;
    obs.variables = result?.variables ?? {};
    return obs;
  }

  async reset() {
    // Each scenario deploys its own process and pins the clock afresh; a shared
    // gateway keeps prior deployments, which is harmless (distinct process ids).
    await this.#json("POST", "/clock/reset", undefined, "reset clock").catch(() => {});
    this.pinnedClockMs = null;
  }

  async close() {
    await this.reset();
  }
}
