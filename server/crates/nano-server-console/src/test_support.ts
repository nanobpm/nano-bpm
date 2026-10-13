// Test support for the console gateway's Node-native unit suites.
//
// The suites import `test` from `node:test` directly; this module is the single
// place the shared assertion and host-IO helpers they use live, mapped onto
// Node's standard library:
//
//   - the assertion surface the suites use, built on `node:assert/strict`;
//   - the host-IO calls the suites make (temporary directories, recursive
//     removal, and text-file IO), built on `node:fs/promises`.
//
// Import `test` from `node:test` directly in each suite; import the assertion
// and IO helpers from here.

import nodeAssert from "node:assert/strict";
import { mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

export type ErrorClass = new (...args: never[]) => Error;

/** Truthiness assertion. */
export function assert(expr: unknown, msg?: string): asserts expr {
  nodeAssert.ok(expr, msg);
}

/** Deep structural equality (`assert.deepStrictEqual`).
 *
 * Normalises plain objects (including the `[Object: null prototype]` rows that
 * `node:sqlite` returns, which `deepStrictEqual` would not treat as equal to
 * object literals) before comparing. Non-plain values (arrays, class instances,
 * typed arrays, dates, …) pass through. */
export function assertEquals<T>(actual: T, expected: T, msg?: string): void {
  nodeAssert.deepStrictEqual(normalizePlain(actual), normalizePlain(expected), msg);
}

function normalizePlain(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(normalizePlain);
  if (value && typeof value === "object") {
    const proto = Object.getPrototypeOf(value);
    if (proto === null || proto === Object.prototype) {
      const out: Record<string, unknown> = {};
      for (const key of Object.keys(value)) {
        out[key] = normalizePlain((value as Record<string, unknown>)[key]);
      }
      return out;
    }
  }
  return value;
}

/** Substring assertion. */
export function assertStringIncludes(actual: string, expected: string, msg?: string): void {
  nodeAssert.ok(
    actual.includes(expected),
    msg ?? `expected ${JSON.stringify(actual)} to include ${JSON.stringify(expected)}`,
  );
}

function describeThrown(ErrorClass: ErrorClass | undefined, msgIncludes: string | undefined): string {
  const parts: string[] = [];
  if (ErrorClass) parts.push(`an instance of ${ErrorClass.name}`);
  if (msgIncludes) parts.push(`a message including ${JSON.stringify(msgIncludes)}`);
  return parts.length ? parts.join(" and ") : "an error";
}

function checkThrown(err: unknown, ErrorClass: ErrorClass | undefined, msgIncludes: string | undefined): void {
  if (ErrorClass) {
    nodeAssert.ok(
      err instanceof ErrorClass,
      `expected ${describeThrown(ErrorClass, msgIncludes)}, got ${String(err)}`,
    );
  }
  if (msgIncludes) {
    const message = err instanceof Error ? err.message : String(err);
    nodeAssert.ok(
      message.includes(msgIncludes),
      `expected message ${JSON.stringify(message)} to include ${JSON.stringify(msgIncludes)}`,
    );
  }
}

/** Synchronous throw assertion (substring message match). */
export function assertThrows(
  fn: () => unknown,
  ErrorClass?: ErrorClass,
  msgIncludes?: string,
): void {
  let threw = false;
  let err: unknown;
  try {
    fn();
  } catch (e) {
    threw = true;
    err = e;
  }
  if (!threw) nodeAssert.fail(`expected the call to throw ${describeThrown(ErrorClass, msgIncludes)}`);
  checkThrown(err, ErrorClass, msgIncludes);
}

/** Async rejection assertion (substring message match). */
export async function assertRejects(
  fn: () => Promise<unknown>,
  ErrorClass?: ErrorClass,
  msgIncludes?: string,
): Promise<void> {
  let threw = false;
  let err: unknown;
  try {
    await fn();
  } catch (e) {
    threw = true;
    err = e;
  }
  if (!threw) {
    nodeAssert.fail(`expected the call to reject with ${describeThrown(ErrorClass, msgIncludes)}`);
  }
  checkThrown(err, ErrorClass, msgIncludes);
}

// --- Host IO (temporary dirs, recursive removal, text-file IO) -------------

/** A fresh unique directory under the OS temp root. */
export function makeTempDir(prefix = "nanobpm-test-"): Promise<string> {
  return mkdtemp(join(tmpdir(), prefix));
}

/** Recursively remove a path (no error if it is already absent). */
export function remove(path: string): Promise<void> {
  return rm(path, { recursive: true, force: true });
}

/** Create a directory tree (re-exported from `node:fs/promises`). */
export { mkdir } from "node:fs/promises";

/** Write `data` to `path` as UTF-8 text. */
export function writeTextFile(path: string, data: string): Promise<void> {
  return writeFile(path, data);
}

/** Read `path` as UTF-8 text. */
export function readTextFile(path: string): Promise<string> {
  return readFile(path, "utf8");
}

/** Async-iterable of `{ name, isFile, isDirectory, isSymlink }` entries. */
export async function* readDir(
  path: string,
): AsyncGenerator<{ name: string; isFile: boolean; isDirectory: boolean; isSymlink: boolean }> {
  const entries = await readdir(path, { withFileTypes: true });
  for (const e of entries) {
    yield {
      name: e.name,
      isFile: e.isFile(),
      isDirectory: e.isDirectory(),
      isSymlink: e.isSymbolicLink(),
    };
  }
}
