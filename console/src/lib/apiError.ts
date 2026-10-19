// Single seam that turns every failed generated-client request into a real
// `Error` (issue #1350).
//
// Both generated fetch clients (`src/gen`, `src/gen-c8`) run with
// `throwOnError: true`. On a non-2xx response the hey-api client throws the
// *parsed response body*: a plain object such as
// `{ error: "app_running", message: "Stop the app before updating it." }`, or a
// bare string. Catch sites across the console are written as
// `e instanceof Error ? e.message : String(e)`, so a structured body rendered as
// "[object Object]" and the server's message was lost.
//
// Rather than patch every catch site (and leave the next one free to repeat the
// mistake), `installApiErrorInterceptor` registers an error interceptor on each
// client so a failed request ALWAYS rejects with an `ApiError` whose `message`
// is human-readable. Existing catch sites then work unchanged. The interceptor
// is installed once from `main.tsx` via `installApiErrorInterceptors`
// (`apiClients.ts`).

/** A failed console/Camunda API request, normalised to a real `Error`. */
export class ApiError extends Error {
  /** HTTP status, when the request got a response. */
  readonly status: number | undefined;
  /** Machine-readable code from the body (`error` / `code`), when present. */
  readonly code: string | undefined;
  /** The original thrown value (parsed JSON body or text). */
  readonly body: unknown;

  constructor(
    message: string,
    opts: { status?: number; code?: string; body?: unknown } = {},
  ) {
    super(message);
    this.name = "ApiError";
    this.status = opts.status;
    this.code = opts.code;
    this.body = opts.body;
  }
}

function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}

function nonBlank(v: unknown): string | undefined {
  return typeof v === "string" && v.trim() !== "" ? v.trim() : undefined;
}

/**
 * The best human-readable message in an error body. Prefers the conventional
 * `message`, then RFC 7807 `detail`/`title`, then a string `error`/`code`, then
 * a nested `{ error: { message } }`, and finally the compact JSON (never
 * "[object Object]").
 */
export function apiErrorMessage(body: unknown): string | undefined {
  if (body == null) return undefined;
  if (typeof body === "string") return nonBlank(body);
  if (body instanceof Error) return nonBlank(body.message);
  if (isRecord(body)) {
    for (const key of ["message", "detail", "title", "error", "code"]) {
      const s = nonBlank(body[key]);
      if (s) return s;
    }
    if (isRecord(body.error)) {
      const nested = apiErrorMessage(body.error);
      if (nested) return nested;
    }
  }
  try {
    const json = JSON.stringify(body);
    return json && json !== "{}" ? json : undefined;
  } catch {
    return undefined;
  }
}

/** Minimal shape of the `Response` the interceptor receives. */
type ResponseLike = { status: number; statusText?: string };

/**
 * Normalise anything a generated client threw into an `Error`.
 *
 * - An `Error` is returned unchanged, so its type and stack survive — even
 *   when a response exists: the hey-api client passes the populated `response`
 *   to error interceptors when reading the body fails after headers arrive,
 *   and wrapping that abort/connection error in an `ApiError` would lose its
 *   identity (and could even report status 200). Parsed HTTP error bodies
 *   (plain objects / strings) are still normalised below.
 * - A failed HTTP response becomes an `ApiError` carrying the body's message,
 *   with `status`/`code`/`body` for callers that branch on them.
 */
export function toApiError(error: unknown, response?: ResponseLike): Error {
  if (error instanceof ApiError) return error;
  if (error instanceof Error) return error;
  const status = response?.status;
  let code: string | undefined;
  if (isRecord(error)) {
    code = nonBlank(error.error) ?? nonBlank(error.code);
  }
  const message =
    apiErrorMessage(error) ??
    (status !== undefined
      ? `HTTP ${status}${response?.statusText ? ` ${response.statusText}` : ""}`
      : "Request failed");
  return new ApiError(message, { status, code, body: error });
}

/** The slice of a hey-api client that `installApiErrorInterceptor` touches. */
export interface ClientWithErrorInterceptors {
  interceptors: {
    error: {
      use(
        fn: (error: unknown, response: Response | undefined) => unknown,
      ): unknown;
    };
  };
}

const installed = new WeakSet<object>();

/** Register the normalising interceptor on a client (idempotent). */
export function installApiErrorInterceptor(
  client: ClientWithErrorInterceptors,
): void {
  if (installed.has(client)) return;
  installed.add(client);
  client.interceptors.error.use((error, response) =>
    toApiError(error, response ?? undefined),
  );
}

/** Whether `installApiErrorInterceptor` has run for this client. */
export function hasApiErrorInterceptor(client: object): boolean {
  return installed.has(client);
}
