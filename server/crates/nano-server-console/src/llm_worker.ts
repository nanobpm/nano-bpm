// Urban LLM-as-job-worker runtime (ADR 0022 §E role 1).
//
// This file is materialised verbatim into <workspace>/nano-generated/llm-worker.ts by
// the console supervisor (like worker-sdk.ts / data-sdk.ts) and started from the
// App entrypoint via `startLlmWorkers()`. It is the *batteries-included*,
// offline-capable implementation of the manifest's `llm` seam: for every
// `workers[]` entry that carries an `llm` binding (instead of a `handler` file),
// it registers a Falcon job worker on that `taskType` whose handler is an LLM
// call — no external connector runtime required.
//
// A worker's contract (ADR 0022 §E):
//   { "taskType": "classify", "llm": "classifier" }          // workers[]
//   "llm": { "classifier": { "provider": "env",              // llm registry
//                            "model": "${NANO_APP_LLM_MODEL}",
//                            "output": { "decision": "risk" } } }
//
// The job's input variables drive the prompt (`prompt` string and/or `messages`
// array, optional `system`); the model's reply becomes the job's output
// variables. When the binding declares `output.decision`, the model's JSON is
// fed through that DMN decision (the "rails") and the decision's output is
// returned instead — the LLM proposes, the decision disposes.
//
// Provider is configuration, not code: `provider: "env"` resolves an
// OpenAI-compatible endpoint from the environment, so a fully-local model
// (Ollama, LM Studio, llama.cpp, vLLM, …) or a hosted API both work unchanged.

// This runtime reads only a small, stable slice of a worker job and writes only
// `{ type, handle }` back to the registrar, so it declares those shapes locally
// (structural mirrors of @nanobpm/worker's WorkerJob/WorkerOptions) and needs no
// sibling import — which keeps it a single self-contained file that type-checks
// identically in the source tree and once materialised into `nano-generated/`.

/** Job variables/headers default to untyped JSON. */
export type LlmVars = Record<string, unknown>;

/** The subset of a worker job this runtime reads (structural mirror of
 *  @nanobpm/worker's `WorkerJob`). */
export interface LlmJob {
  readonly variables: LlmVars;
  readonly customHeaders: Record<string, unknown>;
}

/** The subset of `defineWorker`'s options this runtime writes (structural mirror
 *  of @nanobpm/worker's `WorkerOptions`); the real registrar accepts a superset. */
export interface LlmWorkerOptions {
  type: string;
  handle: (job: LlmJob, ctx?: unknown) => Promise<unknown> | unknown;
}

/** Expand `${VAR}` / `${VAR:-default}` env templates (mirrors data-sdk's helper;
 * inlined to keep this a self-contained, unit-testable module). */
function resolveEnvTemplate(
  tpl: string,
  env: (key: string) => string | undefined,
): string {
  return tpl.replace(
    /\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}/g,
    (_m, name: string, dflt: string | undefined) => {
      const v = env(name);
      if (v !== undefined && v !== "") return v;
      return dflt ?? "";
    },
  );
}

// --- Manifest shapes (the subset this runtime reads) -----------------------

/** One `llm` registry entry (`nano.app.json` → `llm.<id>`). */
export interface LlmBinding {
  /** Provider selector. `"env"` resolves the endpoint from NANO_APP_LLM_* env. */
  provider: string;
  /** Model id, typically an env template (e.g. `${NANO_APP_LLM_MODEL}`). */
  model: string;
  /** Constrains the structured output — currently a DMN decision id (the rails). */
  output?: { decision?: string };
  /** Action-API tools the agent may call (chat-agent role; unused by role 1). */
  tools?: string[];
}

/** One `workers[]` entry. Role 1 handles those with an `llm` binding. */
export interface WorkerDef {
  taskType: string;
  handler?: string;
  llm?: string;
  outputType?: string;
}

/** The manifest subset `startLlmWorkers` needs. */
export interface AppManifest {
  workers?: WorkerDef[];
  llm?: Record<string, LlmBinding>;
}

/** A resolved provider endpoint (OpenAI-compatible chat completions). */
export interface ProviderConfig {
  baseUrl: string;
  apiKey?: string;
  model: string;
}

/** One chat message on the wire. */
export interface ChatMessage {
  role: "system" | "user" | "assistant";
  content: string;
}

type Env = (key: string) => string | undefined;

// Default environment reader: Node's `process.env`. Mirrors worker-sdk's RT
// adapter, which is Node-only since ADR 0065 removed Deno.
const defaultEnv: Env = (key) => {
  const g = globalThis as unknown as { process?: { env: Record<string, string | undefined> } };
  return g.process?.env[key];
};

// --- Provider resolution ---------------------------------------------------

/**
 * Resolve an OpenAI-compatible endpoint for a binding. `provider: "env"` (the
 * only built-in today) reads:
 *   - NANO_APP_LLM_BASE_URL  (default http://localhost:11434/v1 — Ollama's
 *                             OpenAI-compatible endpoint; override for any host)
 *   - NANO_APP_LLM_API_KEY   (optional; sent as `Authorization: Bearer` if set)
 *   - NANO_APP_LLM_MODEL     (fallback when the binding's `model` resolves empty)
 * The binding's `model` is env-templated (`${VAR}` / `${VAR:-default}`).
 */
export function resolveProvider(
  binding: LlmBinding,
  env: Env = defaultEnv,
): ProviderConfig {
  const baseUrl = (env("NANO_APP_LLM_BASE_URL") ?? "http://localhost:11434/v1")
    .replace(
      /\/+$/,
      "",
    );
  const apiKey = env("NANO_APP_LLM_API_KEY") || undefined;
  const model = resolveEnvTemplate(binding.model ?? "", env) ||
    env("NANO_APP_LLM_MODEL") || "";
  if (!model) {
    throw new Error(
      `llm worker: no model resolved for binding (model="${binding.model}"); set NANO_APP_LLM_MODEL or a literal model`,
    );
  }
  return { baseUrl, apiKey, model };
}

// --- Prompt construction ---------------------------------------------------

/**
 * Build the chat messages for a job. Input contract (job variables):
 *   - `messages`: a ready `{role,content}[]` (used verbatim), or
 *   - `prompt`:   a user-message string.
 * An optional `system` string (variable or custom header) is prepended.
 */
export function buildMessages(job: LlmJob): ChatMessage[] {
  const v = job.variables;
  const system = typeof v.system === "string"
    ? v.system
    : typeof job.customHeaders.system === "string"
    ? (job.customHeaders.system as string)
    : undefined;

  let msgs: ChatMessage[];
  if (Array.isArray(v.messages)) {
    msgs = (v.messages as unknown[]).map((m, i) => {
      const o = m as { role?: unknown; content?: unknown };
      if (
        typeof o.content !== "string" ||
        (o.role !== "user" && o.role !== "assistant" && o.role !== "system")
      ) {
        throw new Error(
          `llm worker: messages[${i}] must be { role: user|assistant|system, content: string }`,
        );
      }
      return { role: o.role, content: o.content };
    });
  } else if (typeof v.prompt === "string") {
    msgs = [{ role: "user", content: v.prompt }];
  } else {
    throw new Error(
      "llm worker: job needs a 'prompt' string or a 'messages' array in its variables",
    );
  }

  return system ? [{ role: "system", content: system }, ...msgs] : msgs;
}

// --- LLM + decision calls --------------------------------------------------

/**
 * Call the model's chat-completions endpoint. When `json` is set, requests a
 * JSON object response (`response_format`) so structured output can be parsed.
 * Returns the assistant message text.
 */
export async function callLlm(
  provider: ProviderConfig,
  messages: ChatMessage[],
  opts: { json?: boolean; fetch?: typeof fetch } = {},
): Promise<string> {
  const doFetch = opts.fetch ?? fetch;
  const headers: Record<string, string> = {
    "content-type": "application/json",
  };
  if (provider.apiKey) headers.authorization = `Bearer ${provider.apiKey}`;
  const body: Record<string, unknown> = { model: provider.model, messages };
  if (opts.json) body.response_format = { type: "json_object" };

  const res = await doFetch(`${provider.baseUrl}/chat/completions`, {
    method: "POST",
    headers,
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const detail = await res.text().catch(() => "");
    throw new Error(
      `llm provider returned ${res.status}: ${detail.slice(0, 500)}`,
    );
  }
  const data = (await res.json()) as {
    choices?: Array<{ message?: { content?: string } }>;
  };
  const content = data.choices?.[0]?.message?.content;
  if (typeof content !== "string") {
    throw new Error("llm provider returned no message content");
  }
  return content;
}

/**
 * Evaluate a DMN decision on the engine (the output "rails"), returning its
 * `output`. Uses the same engine base URL the workers connect to.
 */
export async function evaluateDecision(
  engineBaseUrl: string,
  decisionId: string,
  variables: Record<string, unknown>,
  doFetch: typeof fetch = fetch,
): Promise<unknown> {
  const base = engineBaseUrl.replace(/\/+$/, "").replace(/\/v2$/, "");
  const res = await doFetch(`${base}/v2/decision-definitions/evaluation`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ decisionDefinitionId: decisionId, variables }),
  });
  if (!res.ok) {
    const detail = await res.text().catch(() => "");
    throw new Error(
      `decision '${decisionId}' evaluation failed ${res.status}: ${
        detail.slice(0, 500)
      }`,
    );
  }
  const data = (await res.json()) as { output?: unknown };
  return data.output;
}

// --- Job handler -----------------------------------------------------------

export interface LlmWorkerRuntime {
  engineBaseUrl: string;
  env?: Env;
  fetch?: typeof fetch;
}

/**
 * Run one job through the LLM (and optional decision rails). Returns the output
 * variables the job is completed with.
 *
 * - No `output` binding → returns `{ text }` (the raw completion).
 * - `output` set → requests JSON, parses it; the parsed object is the output.
 * - `output.decision` set → the parsed JSON is fed to that DMN decision and the
 *   decision's output is returned (object as-is, scalar wrapped as `{ result }`).
 */
export async function runLlmJob(
  job: LlmJob,
  binding: LlmBinding,
  rt: LlmWorkerRuntime,
): Promise<Record<string, unknown>> {
  const wantJson = !!binding.output;
  const provider = resolveProvider(binding, rt.env ?? defaultEnv);
  const messages = buildMessages(job);
  const text = await callLlm(provider, messages, {
    json: wantJson,
    fetch: rt.fetch,
  });

  if (!wantJson) return { text };

  let parsed: Record<string, unknown>;
  try {
    parsed = JSON.parse(text) as Record<string, unknown>;
  } catch {
    throw new Error(
      `llm worker: expected JSON output but got: ${text.slice(0, 300)}`,
    );
  }

  if (binding.output?.decision) {
    const out = await evaluateDecision(
      rt.engineBaseUrl,
      binding.output.decision,
      parsed,
      rt.fetch,
    );
    return out !== null && typeof out === "object"
      ? (out as Record<string, unknown>)
      : { result: out };
  }
  return parsed;
}

// --- Registration ----------------------------------------------------------

/** Injectable worker registrar, so `startLlmWorkers` is unit-testable. Structural
 * so this module needs no runtime import of the worker SDK. */
export type DefineWorkerFn = (opts: LlmWorkerOptions) => void;

/** Register one LLM worker on `taskType`. `define` is the worker registrar
 * (`defineWorker` from the materialised worker SDK, or a test double). */
export function defineLlmWorker(
  taskType: string,
  binding: LlmBinding,
  rt: LlmWorkerRuntime,
  define: DefineWorkerFn,
): void {
  define({
    type: taskType,
    handle: (job) => runLlmJob(job, binding, rt),
  });
}

export interface StartLlmWorkersOptions {
  /** The manifest object; when omitted, read from `manifestPath`. */
  manifest?: AppManifest;
  /** Path to `nano.app.json` (default `./nano.app.json`). */
  manifestPath?: string;
  /** Engine base URL (default env NANOBPMN_BASE_URL or http://127.0.0.1:8080). */
  baseUrl?: string;
  /** Env reader (default `process.env`). */
  env?: Env;
  /** Fetch impl (default global). */
  fetch?: typeof fetch;
  /** Worker registrar (default defineWorker; injected in tests). */
  define?: DefineWorkerFn;
}

/**
 * Start every LLM-as-worker declared in the App manifest. For each `workers[]`
 * entry with an `llm` binding, resolves `llm.<id>` and registers a Falcon worker
 * on its `taskType`. Returns the task types started. Missing bindings throw so a
 * misconfigured manifest fails loudly at startup rather than at first job.
 */
export async function startLlmWorkers(
  opts: StartLlmWorkersOptions = {},
): Promise<string[]> {
  const env = opts.env ?? defaultEnv;
  let manifest = opts.manifest;
  if (!manifest) {
    const path = opts.manifestPath ?? "./nano.app.json";
    const { readFile } = await import("node:fs/promises");
    const text = await readFile(path, "utf8");
    manifest = JSON.parse(text) as AppManifest;
  }

  const engineBaseUrl = opts.baseUrl ?? env("NANOBPMN_BASE_URL") ??
    "http://127.0.0.1:8080";
  // Default registrar: load the materialised worker SDK lazily via a *variable*
  // specifier so this static reference is not analysed in the source tree (where
  // the hyphenated sibling does not exist). Tests inject `define` and never hit
  // this path.
  let define = opts.define;
  if (!define) {
    const spec = "./worker-sdk.ts";
    const mod = (await import(spec)) as { defineWorker: DefineWorkerFn };
    define = mod.defineWorker;
  }
  const rt: LlmWorkerRuntime = { engineBaseUrl, env, fetch: opts.fetch };

  const started: string[] = [];
  for (const w of manifest.workers ?? []) {
    if (!w.llm) continue; // handler workers are started by startWorkers()
    const binding = manifest.llm?.[w.llm];
    if (!binding) {
      throw new Error(
        `worker '${w.taskType}' references unknown llm binding '${w.llm}'`,
      );
    }
    defineLlmWorker(w.taskType, binding, rt, define);
    started.push(w.taskType);
  }
  return started;
}
