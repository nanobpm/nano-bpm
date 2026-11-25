// ProblemDetail error contract for @nanobpm/engine-wasm (#1937).
//
// An engine rejection is thrown as a JS `Error` carrying `status`, `title`,
// `detail` and `nonRetryable`, derived from the ONE mapping the server's REST
// gateway also uses (`EngineError::problem` in engine-core/src/problem.rs) —
// not a free-text string. Checked for the lean (`.`) and read-model
// (`/readmodel`) entrypoints of the freshly built engine-wasm/pkg.
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);

let failed = false;
function assert(cond, msg) {
  if (cond) {
    console.log(`\u2713 ${msg}`);
  } else {
    failed = true;
    console.error(`\u2717 ${msg}`);
  }
}

function rejection(fn) {
  try {
    fn();
  } catch (err) {
    return err;
  }
  return undefined;
}

function checkProblem(label, err, { status, title, nonRetryable, context }) {
  assert(err instanceof Error, `${label}: throws an Error (got ${typeof err}: ${String(err)})`);
  assert(err?.status === status, `${label}: status ${status} (got ${err?.status})`);
  assert(err?.title === title, `${label}: title ${title} (got ${err?.title})`);
  assert(
    err?.nonRetryable === nonRetryable,
    `${label}: nonRetryable ${nonRetryable} (got ${err?.nonRetryable})`,
  );
  assert(
    typeof err?.detail === "string" && err.detail.length > 0,
    `${label}: has a detail (got ${JSON.stringify(err?.detail)})`,
  );
  assert(
    typeof err?.message === "string" && err.message.startsWith(`${context}: `),
    `${label}: message keeps the "${context}: " context (got ${JSON.stringify(err?.message)})`,
  );
}

async function checkVariant(label, glueSpecifier, wasmSpecifier) {
  const { initSync, TestEngine } = await import(glueSpecifier);
  initSync({ module: readFileSync(require.resolve(wasmSpecifier)) });
  const engine = new TestEngine();

  checkProblem(
    `${label}: createInstance(unknown process)`,
    rejection(() => engine.createInstance("no-such-process", "{}")),
    { status: 400, title: "INVALID_ARGUMENT", nonRetryable: true, context: "create error" },
  );
  checkProblem(
    `${label}: cancelInstance(unknown key)`,
    rejection(() => engine.cancelInstance("424242")),
    { status: 404, title: "NOT_FOUND", nonRetryable: true, context: "cancel instance error" },
  );
  engine.free();
}

await checkVariant(
  "lean (.)",
  "@nanobpm/engine-wasm",
  "@nanobpm/engine-wasm/lean/nanobpmn_engine_bg.wasm",
);
await checkVariant(
  "readmodel (/readmodel)",
  "@nanobpm/engine-wasm/readmodel",
  "@nanobpm/engine-wasm/readmodel/nanobpmn_engine_bg.wasm",
);

if (failed) {
  console.error("\nProblemDetail error contract FAILED");
  process.exit(1);
}
console.log("\nProblemDetail error contract passed");
