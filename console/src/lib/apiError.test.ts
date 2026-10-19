import { test } from "node:test";
import assert from "node:assert/strict";
import { readdirSync, readFileSync, existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  ApiError,
  apiErrorMessage,
  hasApiErrorInterceptor,
  installApiErrorInterceptor,
  toApiError,
} from "./apiError.ts";

// Issue #1350: the generated clients threw the parsed body (a plain object), so
// `e instanceof Error ? e.message : String(e)` rendered "[object Object]".

const SRC = join(dirname(fileURLToPath(import.meta.url)), "..");

/** The shape every catch site in the console uses. */
const display = (e: unknown) => (e instanceof Error ? e.message : String(e));

test("the app_running 409 body surfaces its message, never [object Object]", () => {
  const body = {
    error: "app_running",
    message: "Stop the app before updating it.",
  };
  const err = toApiError(body, { status: 409, statusText: "Conflict" });
  assert.ok(err instanceof ApiError);
  assert.equal(display(err), "Stop the app before updating it.");
  assert.equal(err.status, 409);
  assert.equal(err.code, "app_running");
  assert.equal(err.body, body);
});

test("message precedence: message > detail > title > error > code > nested > JSON", () => {
  assert.equal(apiErrorMessage({ message: "m", detail: "d" }), "m");
  assert.equal(apiErrorMessage({ detail: "d", title: "t" }), "d");
  assert.equal(apiErrorMessage({ title: "t", error: "e" }), "t");
  assert.equal(apiErrorMessage({ error: "e", code: "c" }), "e");
  assert.equal(apiErrorMessage({ code: "c" }), "c");
  assert.equal(apiErrorMessage({ error: { message: "inner" } }), "inner");
  assert.equal(
    apiErrorMessage({ message: "  ", other: 1 }),
    '{"message":"  ","other":1}',
  );
  assert.equal(apiErrorMessage("plain text body"), "plain text body");
  assert.equal(apiErrorMessage(""), undefined);
  assert.equal(apiErrorMessage({}), undefined);
});

test("an empty / non-JSON body falls back to the HTTP status, never [object Object]", () => {
  assert.equal(
    display(toApiError({}, { status: 409, statusText: "Conflict" })),
    "HTTP 409 Conflict",
  );
  assert.equal(display(toApiError("", { status: 502 })), "HTTP 502");
  assert.equal(display(toApiError(undefined)), "Request failed");
});

test("a non-HTTP Error (network failure, abort) passes through unchanged", () => {
  const net = new TypeError("Failed to fetch");
  assert.equal(toApiError(net), net);
  const already = new ApiError("x", { status: 400 });
  assert.equal(toApiError(already, { status: 400 }), already);
});

test("no thrown shape stringifies to [object Object] (defect-class sweep)", () => {
  const shapes: unknown[] = [
    {},
    { a: 1 },
    { error: {} },
    { error: { code: 3 } },
    [],
    [1, 2],
    42,
    true,
    null,
    undefined,
    { message: 7 },
    Object.create(null),
  ];
  for (const shape of shapes) {
    for (const response of [undefined, { status: 500 }]) {
      const msg = display(toApiError(shape, response));
      assert.ok(
        !msg.includes("[object Object]"),
        `${JSON.stringify(shape)} → ${msg}`,
      );
      assert.ok(msg.length > 0);
    }
  }
});

test("installApiErrorInterceptor drives the generated client's error chain and is idempotent", async () => {
  // Mirror of the hey-api fetch client's error path (src/gen/client/client.gen.ts):
  // run every error interceptor over the thrown body, then throw the result.
  const fns: Array<(e: unknown, r: Response | undefined) => unknown> = [];
  const client = {
    interceptors: {
      error: {
        use: (fn: (e: unknown, r: Response | undefined) => unknown) =>
          fns.push(fn),
      },
    },
  };
  assert.equal(hasApiErrorInterceptor(client), false);
  installApiErrorInterceptor(client);
  installApiErrorInterceptor(client);
  assert.equal(fns.length, 1, "installing twice must not double-register");
  assert.equal(hasApiErrorInterceptor(client), true);

  const response = new Response(null, { status: 409, statusText: "Conflict" });
  let final: unknown = {
    error: "app_running",
    message: "Stop the app before updating it.",
  };
  for (const fn of fns) final = await fn(final, response);
  assert.ok(final instanceof ApiError);
  assert.equal(display(final), "Stop the app before updating it.");
});

test("guard: the generated clients still route thrown bodies through error interceptors", () => {
  // If a codegen upgrade changes this contract, the interceptor silently stops
  // applying and [object Object] comes back. Fail loudly instead.
  for (const dir of ["gen", "gen-c8"]) {
    const src = readFileSync(join(SRC, dir, "client", "client.gen.ts"), "utf8");
    assert.match(
      src,
      /interceptors\.error\.fns/,
      `${dir}: error interceptors not applied`,
    );
    assert.match(
      src,
      /throw finalError/,
      `${dir}: interceptor result is not what gets thrown`,
    );
  }
});

test("guard: every generated client is registered in apiClients.ts", () => {
  const registry = readFileSync(join(SRC, "lib", "apiClients.ts"), "utf8");
  const genDirs = readdirSync(SRC).filter(
    (d) => d.startsWith("gen") && existsSync(join(SRC, d, "client.gen.ts")),
  );
  assert.ok(
    genDirs.length >= 2,
    `expected the generated clients, found ${genDirs.join(", ")}`,
  );
  for (const dir of genDirs) {
    assert.ok(
      registry.includes(`from "../${dir}/client.gen"`) &&
        // Key may be quoted or bare (prettier strips quotes where it can).
        new RegExp(
          `^\\s*["']?${dir.replace(/[-]/g, "\\-")}["']?\\s*:`,
          "m",
        ).test(registry),
      `src/${dir}/client.gen.ts is not registered in GENERATED_CLIENTS (lib/apiClients.ts)`,
    );
  }
});

test("guard: main.tsx installs the interceptors before the first render", () => {
  const main = readFileSync(join(SRC, "main.tsx"), "utf8");
  const install = main.indexOf("installApiErrorInterceptors();");
  const render = main.indexOf(".render(");
  assert.ok(install > -1, "main.tsx must call installApiErrorInterceptors()");
  assert.ok(
    render > -1 && install < render,
    "the interceptors must be installed before render",
  );
});
