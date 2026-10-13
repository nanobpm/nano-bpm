// EmbeddedEngine — a plain WebAssembly.instantiate wrapper around the Nano BPMN
// engine's FFI cdylib (engine-core --features ffi --target wasm32-unknown-unknown).
//
// Codename: **Bernd**, after Bernd Ruecker (co-founder of Camunda, evangelist of
// the Saga/compensation pattern that makes "same source, embedded or remote"
// tenable). See ADR 0005 (`docs/adr/0005-embedded-u-nano.md`) for the design
// position; see ADR 0015 for the naming policy. The class is EmbeddedEngine so
// its purpose is obvious in unfamiliar code; the codename lives in the package
// name (`@nanobpm/nano-bernd`), the CODENAME constant, boot logs and the
// NANO_BERND_* env prefix.
//
// The wasm ships with zero imports; we host it directly with no wasm-bindgen
// glue. The same `nano_engine.wasm` binary powers the JVM (via Chicory) and
// future Python/Go/.NET embedded hosts — this file is its JS-side twin.
//
// v0.1.0 → v0.2.0 surface (matches engine-core FFI ABI v2):
//   deploy(bpmnXml) → { count }
//   createInstance(processId) → { processInstanceKey }
//   correlateMessage(name, correlationKey) → i64
//   triggerTimers(nowEpochMs) → i64        // fires due timers
//   expireJobs(nowEpochMs) → i64           // releases expired activation locks
//   activateJobs({ type, worker, maxJobs, timeoutMs, now }) → ActivatedJob[]
//   completeJob(jobKey) → void
//   failJob(jobKey, retries, message?) → void
//   isCompleted(key) → boolean
//   instanceCount() → number
//
// Variables-on-complete deferred to a future ABI (engine-core would need a
// JSON parser, which breaks the dep-free promise — v3 route is a separate
// `setVariables` command before completion).

// Note: the packaged-wasm loader uses `node:*` modules, so it is defined
// inline and imported dynamically only when needed. That way this module can
// be bundled for the browser as long as callers pass their own `wasmBytes +
// manifest` to `EmbeddedEngine.create({...})`.

/** JSON manifest emitted alongside the wasm by `make engine-wasm-ffi-dist`. */
export interface WasmManifest {
  artifact: string;
  abi_version: number;
  engine_version: string;
  sha256: string;
  size_bytes: number;
  raw_size_bytes: number;
  exports: Array<{ name: string; kind: string }>;
  imports: Array<{ module: string; name: string; kind: string }>;
}

/** ABI version this JS host is written against. Bumped when the C-ABI changes. */
export const EXPECTED_ABI_VERSION = 2;

/** An activated job as returned by {@link EmbeddedEngine.activateJobs}. */
export interface ActivatedJob {
  /** Job key, u64 stringified so callers round-trip without BigInt woes. */
  key: string;
  type: string;
  /** Process instance key (stringified). */
  instanceKey: string;
  /** Element instance key of the token parked on this job (stringified). */
  elementInstanceKey: string;
  /** BPMN element id of the service task (or equivalent) that produced the job. */
  elementId: string;
  /** The worker id the activation was locked to. */
  worker: string;
  /** Epoch-ms instant at which the activation lock expires. */
  deadline: number;
  /** Remaining retries; 0 means the next failure raises an incident. */
  retries: number;
  /** Snapshot of the instance variables at activation time. */
  variables: Record<string, unknown>;
}

/** Parameters to {@link EmbeddedEngine.activateJobs}. */
export interface ActivateJobsRequest {
  type: string;
  worker: string;
  /** Upper bound on activations for this call. Defaults to 1. */
  maxJobs?: number;
  /** Lock timeout in ms from `now`. Defaults to 30_000. */
  timeoutMs?: number;
  /** Logical `now` in epoch-ms. Defaults to `Date.now()`. */
  now?: number;
}

/** Coarse exports of the FFI cdylib. */
interface NanoExports {
  memory: WebAssembly.Memory;
  nbpmn_alloc(len: number): number;
  nbpmn_free(ptr: number, len: number): void;
  nbpmn_engine_new(): number;
  nbpmn_engine_free(engine: number): void;
  nbpmn_deploy_bpmn(engine: number, ptr: number, len: number): bigint;
  nbpmn_create_instance(engine: number, idPtr: number, idLen: number, now: bigint): bigint;
  nbpmn_correlate_message(engine: number, namePtr: number, nameLen: number, keyPtr: number, keyLen: number, now: bigint): bigint;
  nbpmn_trigger_timers(engine: number, now: bigint): bigint;
  nbpmn_is_completed(engine: number, instanceKey: bigint): number;
  nbpmn_instance_count(engine: number): bigint;
  // ABI v2 job worker surface.
  nbpmn_activate_jobs(
    engine: number,
    typePtr: number,
    typeLen: number,
    workerPtr: number,
    workerLen: number,
    maxJobs: number,
    timeoutMs: bigint,
    now: bigint,
    outPtrPtr: number,
    outLenPtr: number,
  ): number;
  nbpmn_complete_job(engine: number, jobKey: bigint): number;
  nbpmn_fail_job(engine: number, jobKey: bigint, retries: number, msgPtr: number, msgLen: number): number;
  nbpmn_expire_jobs(engine: number, now: bigint): bigint;
}

export interface CreateOptions {
  /** Override the default packaged wasm (e.g., to load from a URL or a custom path). */
  wasmBytes?: BufferSource;
  /** Override the packaged manifest. If omitted the packaged one is used. */
  manifest?: WasmManifest;
}

/**
 * Load the wasm blob + manifest packaged alongside this module.
 *
 * Uses dynamic `node:*` imports so the module can also be bundled for the
 * browser (browser hosts must pass their own `wasmBytes + manifest` to
 * {@link EmbeddedEngine.create} and never reach this path).
 */
async function loadPackagedWasm(): Promise<{ bytes: Uint8Array; manifest: WasmManifest }> {
  const { readFile } = await import('node:fs/promises');
  const { fileURLToPath } = await import('node:url');
  const { dirname, join } = await import('node:path');
  const here = dirname(fileURLToPath(import.meta.url));
  const wasmDir = join(here, '..', 'wasm');
  const [bytes, manifestJson] = await Promise.all([
    readFile(join(wasmDir, 'nano_engine.wasm')),
    readFile(join(wasmDir, 'manifest.json'), 'utf8'),
  ]);
  return { bytes, manifest: JSON.parse(manifestJson) as WasmManifest };
}

/**
 * In-process Nano engine backed by wasm. Own the lifecycle: `close()` frees
 * the wasm engine handle. The WebAssembly instance and its linear memory are
 * owned by the JS runtime and are reclaimed by the garbage collector once
 * this object is unreachable.
 *
 * Codename: **Bernd** (see file header). `EmbeddedEngine.CODENAME` exposes it
 * programmatically; boot logs, topology advertisement and env-var prefix all
 * carry it.
 */
export class EmbeddedEngine {
  /**
   * Feature codename: `"Bernd"` (after Bernd Ruecker; see ADR 0005). Public so
   * downstream code — banners, /v2/topology handlers, telemetry — can surface
   * the codename without hard-coding the string.
   */
  static readonly CODENAME = 'Bernd';

  private readonly exports: NanoExports;
  private readonly engine: number;
  private readonly encoder = new TextEncoder();
  private closed = false;

  readonly manifest: WasmManifest;

  private constructor(exports: NanoExports, engine: number, manifest: WasmManifest) {
    this.exports = exports;
    this.engine = engine;
    this.manifest = manifest;
  }

  /**
   * Instantiate the wasm engine.
   *
   * With no arguments, loads the wasm packaged with this module via `node:fs`
   * (Node-only).
   *
   * Pass **both** `wasmBytes` and `manifest` to load from elsewhere (browser
   * fetch, tests, hosts embedding their own build) — the packaged-wasm loader
   * is not invoked in that case, so browsers work without a Node polyfill.
   * Passing only one of the two is a programmer error.
   */
  static async create(options: CreateOptions = {}): Promise<EmbeddedEngine> {
    if ((options.wasmBytes === undefined) !== (options.manifest === undefined)) {
      throw new Error(
        'EmbeddedEngine.create: wasmBytes and manifest must be provided together, or both omitted.',
      );
    }
    let bytes: BufferSource;
    let manifest: WasmManifest;
    if (options.wasmBytes && options.manifest) {
      bytes = options.wasmBytes;
      manifest = options.manifest;
    } else {
      const packaged = await loadPackagedWasm();
      bytes = packaged.bytes as BufferSource;
      manifest = packaged.manifest;
    }

    if (manifest.abi_version !== EXPECTED_ABI_VERSION) {
      throw new Error(
        `nano_engine.wasm ABI mismatch: manifest reports v${manifest.abi_version}, ` +
          `this host requires v${EXPECTED_ABI_VERSION}. Upgrade @nanobpm/nano-bernd.`,
      );
    }

    const { instance } = await WebAssembly.instantiate(bytes, {});
    const exports = instance.exports as unknown as NanoExports;
    const engine = exports.nbpmn_engine_new();
    if (engine === 0) throw new Error('nbpmn_engine_new returned null');
    return new EmbeddedEngine(exports, engine, manifest);
  }

  /** Deploy BPMN XML. Returns the number of process definitions the engine parsed (>=0). */
  deploy(bpmnXml: string): { count: number } {
    this.assertOpen();
    const [ptr, len] = this.writeStr(bpmnXml);
    try {
      const count = this.exports.nbpmn_deploy_bpmn(this.engine, ptr, len);
      const n = Number(count);
      if (n < 0) throw new Error(`deploy failed (${n})`);
      return { count: n };
    } finally {
      this.exports.nbpmn_free(ptr, len);
    }
  }

  /**
   * Start a process instance. Returns the process instance key as a string
   * (u64 in wasm; stringified so callers can round-trip without BigInt woes).
   *
   * `nowEpochMs` defaults to `Date.now()` — override for deterministic tests.
   */
  createInstance(processId: string, nowEpochMs?: number): { processInstanceKey: string } {
    this.assertOpen();
    const [ptr, len] = this.writeStr(processId);
    try {
      const now = BigInt(nowEpochMs ?? Date.now());
      const key = this.exports.nbpmn_create_instance(this.engine, ptr, len, now);
      if (key === 0n) throw new Error(`create_instance failed (no such process id: ${processId})`);
      return { processInstanceKey: String(key) };
    } finally {
      this.exports.nbpmn_free(ptr, len);
    }
  }

  /**
   * Correlate a message and dispatch it into the running engine.
   *
   * Returns the number of engine events produced (>=0). Positive means at
   * least one subscription matched and one or more instances advanced; 0
   * means no subscription matched. Returns -1 only on invalid input (null
   * engine / non-UTF-8 bytes).
   */
  correlateMessage(name: string, correlationKey: string, nowEpochMs?: number): bigint {
    this.assertOpen();
    const [namePtr, nameLen] = this.writeStr(name);
    const [keyPtr, keyLen] = this.writeStr(correlationKey);
    try {
      const now = BigInt(nowEpochMs ?? Date.now());
      return this.exports.nbpmn_correlate_message(this.engine, namePtr, nameLen, keyPtr, keyLen, now);
    } finally {
      this.exports.nbpmn_free(namePtr, nameLen);
      this.exports.nbpmn_free(keyPtr, keyLen);
    }
  }

  /**
   * Fire due timers as of `nowEpochMs`. The engine is clock-free; the host
   * decides cadence. Returns the number of timers fired.
   *
   * EmbeddedEngine wrappers (JVM, Node) call this from a scheduled ticker;
   * direct callers can call it on demand from tests with fake time.
   */
  triggerTimers(nowEpochMs: number): bigint {
    this.assertOpen();
    return this.exports.nbpmn_trigger_timers(this.engine, BigInt(nowEpochMs));
  }

  /**
   * Release job activations whose lock has expired as of `nowEpochMs`.
   * Pair with {@link triggerTimers} on the same tick to drive wall-clock
   * progress; returns the number of events written (0 = nothing to do).
   */
  expireJobs(nowEpochMs: number): bigint {
    this.assertOpen();
    return this.exports.nbpmn_expire_jobs(this.engine, BigInt(nowEpochMs));
  }

  /**
   * Activate up to `maxJobs` jobs of the given type on behalf of `worker`.
   * Returns the activated jobs (deserialised from the JSON blob the engine
   * writes into a caller-owned allocation).
   */
  activateJobs(req: ActivateJobsRequest): ActivatedJob[] {
    this.assertOpen();
    const [typePtr, typeLen] = this.writeStr(req.type);
    const [workerPtr, workerLen] = this.writeStr(req.worker);
    // Scratch region for the two out-params (ptr: u32, len: u32) — 8 bytes.
    const outPtrPtr = this.exports.nbpmn_alloc(8);
    if (outPtrPtr === 0) throw new Error('nbpmn_alloc(8) failed for activate out-params');
    const outLenPtr = outPtrPtr + 4;
    try {
      const rc = this.exports.nbpmn_activate_jobs(
        this.engine,
        typePtr,
        typeLen,
        workerPtr,
        workerLen,
        req.maxJobs ?? 1,
        BigInt(req.timeoutMs ?? 30_000),
        BigInt(req.now ?? Date.now()),
        outPtrPtr,
        outLenPtr,
      );
      if (rc < 0) throw new Error(`activate_jobs failed (${rc})`);
      const view = new DataView(this.exports.memory.buffer);
      const jsonPtr = view.getUint32(outPtrPtr, true);
      const jsonLen = view.getUint32(outLenPtr, true);
      // The engine always writes a JSON blob for rc>=0 (at minimum `[]` for
      // rc=0). If rc>0 but the blob is empty, the engine locked activations
      // it couldn't serialise — jobs unreachable from the host. Treat as an
      // error rather than silently returning [] (which would lose the locks).
      if (jsonLen === 0) {
        if (rc > 0) {
          throw new Error(
            `activate_jobs returned ${rc} jobs but wrote no JSON payload — ` +
              `jobs are locked but unreachable from the host; ABI mismatch or wasm OOM likely.`,
          );
        }
        return [];
      }
      const jsonBytes = new Uint8Array(this.exports.memory.buffer, jsonPtr, jsonLen).slice();
      try {
        const text = new TextDecoder().decode(jsonBytes);
        return JSON.parse(text) as ActivatedJob[];
      } finally {
        this.exports.nbpmn_free(jsonPtr, jsonLen);
      }
    } finally {
      this.exports.nbpmn_free(outPtrPtr, 8);
      this.exports.nbpmn_free(typePtr, typeLen);
      this.exports.nbpmn_free(workerPtr, workerLen);
    }
  }

  /** Complete an activated job. Throws if the job is unknown or already resolved. */
  completeJob(jobKey: string): void {
    this.assertOpen();
    const rc = this.exports.nbpmn_complete_job(this.engine, BigInt(jobKey));
    if (rc !== 0) throw new Error(`complete_job(${jobKey}) failed (${rc})`);
  }

  /**
   * Fail an activated job with `retries` attempts remaining. 0 retries parks
   * the job with an incident. `message` is optional operator-facing context.
   */
  failJob(jobKey: string, retries: number, message?: string): void {
    this.assertOpen();
    let msgPtr = 0;
    let msgLen = 0;
    if (message !== undefined && message.length > 0) {
      [msgPtr, msgLen] = this.writeStr(message);
    }
    try {
      const rc = this.exports.nbpmn_fail_job(this.engine, BigInt(jobKey), retries, msgPtr, msgLen);
      if (rc !== 0) throw new Error(`fail_job(${jobKey}) failed (${rc})`);
    } finally {
      if (msgLen > 0) this.exports.nbpmn_free(msgPtr, msgLen);
    }
  }

  isCompleted(processInstanceKey: string): boolean {
    this.assertOpen();
    return this.exports.nbpmn_is_completed(this.engine, BigInt(processInstanceKey)) === 1;
  }

  instanceCount(): number {
    this.assertOpen();
    return Number(this.exports.nbpmn_instance_count(this.engine));
  }

  close(): void {
    if (this.closed) return;
    this.exports.nbpmn_engine_free(this.engine);
    this.closed = true;
  }

  private writeStr(s: string): [number, number] {
    const bytes = this.encoder.encode(s);
    const ptr = this.exports.nbpmn_alloc(bytes.length);
    if (ptr === 0 && bytes.length > 0) throw new Error(`nbpmn_alloc(${bytes.length}) failed`);
    new Uint8Array(this.exports.memory.buffer).set(bytes, ptr);
    return [ptr, bytes.length];
  }

  private assertOpen(): void {
    if (this.closed) throw new Error('EmbeddedEngine has been closed');
  }
}
