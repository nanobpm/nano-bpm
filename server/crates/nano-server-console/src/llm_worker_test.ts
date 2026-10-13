// Node unit tests for the Urban LLM-as-job-worker runtime (ADR 0022 §E role 1).
//
// Run in CI by the `console-ts` job, and locally with:
//   node --experimental-strip-types --test server/crates/nano-server-console/src/llm_worker_test.ts
//
// They cover provider resolution, prompt construction, the chat-completions call
// (mocked), the DMN-decision "rails", and manifest-driven worker registration —
// all without a live model or engine (fetch + the worker registrar are injected).

import { test } from "node:test";
import {
  assertEquals,
  assertRejects,
  assertStringIncludes,
} from "./test_support.ts";
import {
  type AppManifest,
  buildMessages,
  callLlm,
  defineLlmWorker,
  type DefineWorkerFn,
  evaluateDecision,
  type LlmJob,
  type LlmWorkerOptions,
  resolveProvider,
  runLlmJob,
  startLlmWorkers,
} from "./llm_worker.ts";

// A minimal job stub (only the fields the runtime reads).
function job(
  variables: Record<string, unknown>,
  customHeaders: Record<string, unknown> = {},
): LlmJob {
  return { variables, customHeaders };
}

// A fetch double that records the last request and returns a canned response.
function fakeFetch(handler: (url: string, init: RequestInit) => unknown) {
  const calls: Array<{ url: string; init: RequestInit }> = [];
  const fn = ((url: string | URL | Request, init?: RequestInit) => {
    const u = String(url);
    calls.push({ url: u, init: init ?? {} });
    const body = handler(u, init ?? {});
    return Promise.resolve(
      new Response(typeof body === "string" ? body : JSON.stringify(body), {
        status: 200,
      }),
    );
  }) as unknown as typeof fetch;
  return { fn, calls };
}

test("resolveProvider reads env + templates the model", () => {
  const env = (k: string) =>
    ({
      NANO_APP_LLM_BASE_URL: "http://host:1234/v1/",
      NANO_APP_LLM_API_KEY: "sk-test",
      NANO_APP_LLM_MODEL: "fallback-model",
    })[k];
  const p = resolveProvider(
    { provider: "env", model: "${NANO_APP_LLM_MODEL}" },
    env,
  );
  assertEquals(p, {
    baseUrl: "http://host:1234/v1",
    apiKey: "sk-test",
    model: "fallback-model",
  });

  // A literal model wins; base URL defaults to the local Ollama endpoint.
  const p2 = resolveProvider(
    { provider: "env", model: "llama3.1" },
    () => undefined,
  );
  assertEquals(p2.baseUrl, "http://localhost:11434/v1");
  assertEquals(p2.model, "llama3.1");
  assertEquals(p2.apiKey, undefined);
});

test("resolveProvider throws when no model resolves", () => {
  let threw = false;
  try {
    resolveProvider({ provider: "env", model: "${MISSING}" }, () => undefined);
  } catch (e) {
    threw = true;
    assertStringIncludes(String(e), "no model resolved");
  }
  assertEquals(threw, true);
});

test("buildMessages: prompt, system, and explicit messages", () => {
  assertEquals(buildMessages(job({ prompt: "hi" })), [{
    role: "user",
    content: "hi",
  }]);
  assertEquals(buildMessages(job({ prompt: "hi", system: "be terse" })), [
    { role: "system", content: "be terse" },
    { role: "user", content: "hi" },
  ]);
  // system may come from a custom header.
  assertEquals(buildMessages(job({ prompt: "hi" }, { system: "hdr" }))[0], {
    role: "system",
    content: "hdr",
  });
  // an explicit messages array is used verbatim.
  const msgs = [{ role: "user", content: "a" }, {
    role: "assistant",
    content: "b",
  }];
  assertEquals(buildMessages(job({ messages: msgs })), msgs);
});

test("buildMessages rejects a job with neither prompt nor messages", () => {
  let threw = false;
  try {
    buildMessages(job({}));
  } catch (e) {
    threw = true;
    assertStringIncludes(String(e), "prompt");
  }
  assertEquals(threw, true);
});

test("callLlm posts an OpenAI-compatible request + returns content", async () => {
  const { fn, calls } = fakeFetch(() => ({
    choices: [{ message: { content: "pong" } }],
  }));
  const out = await callLlm(
    { baseUrl: "http://host/v1", apiKey: "sk", model: "m" },
    [{ role: "user", content: "ping" }],
    { json: true, fetch: fn },
  );
  assertEquals(out, "pong");
  assertEquals(calls.length, 1);
  assertEquals(calls[0].url, "http://host/v1/chat/completions");
  const body = JSON.parse(String(calls[0].init.body));
  assertEquals(body.model, "m");
  assertEquals(body.response_format, { type: "json_object" });
  assertEquals(body.messages, [{ role: "user", content: "ping" }]);
  assertEquals(
    (calls[0].init.headers as Record<string, string>).authorization,
    "Bearer sk",
  );
});

test("callLlm throws on a non-200 response", async () => {
  const fn = (() =>
    Promise.resolve(
      new Response("nope", { status: 500 }),
    )) as unknown as typeof fetch;
  await assertRejects(
    () =>
      callLlm({ baseUrl: "http://h/v1", model: "m" }, [{
        role: "user",
        content: "x",
      }], { fetch: fn }),
    Error,
    "500",
  );
});

test("evaluateDecision posts to the engine + returns output", async () => {
  const { fn, calls } = fakeFetch(() => ({ output: { risk: "high" } }));
  const out = await evaluateDecision("http://engine:8080/v2", "risk", {
    score: 9,
  }, fn);
  assertEquals(out, { risk: "high" });
  assertEquals(
    calls[0].url,
    "http://engine:8080/v2/decision-definitions/evaluation",
  );
  const body = JSON.parse(String(calls[0].init.body));
  assertEquals(body, { decisionDefinitionId: "risk", variables: { score: 9 } });
});

test("runLlmJob: no output binding returns { text }", async () => {
  const { fn } = fakeFetch(() => ({
    choices: [{ message: { content: "hello" } }],
  }));
  const out = await runLlmJob(job({ prompt: "hi" }), {
    provider: "env",
    model: "m",
  }, {
    engineBaseUrl: "http://e",
    fetch: fn,
  });
  assertEquals(out, { text: "hello" });
});

test("runLlmJob: output binding parses JSON", async () => {
  const { fn } = fakeFetch(() => ({
    choices: [{ message: { content: '{"label":"spam","score":0.9}' } }],
  }));
  const out = await runLlmJob(
    job({ prompt: "classify" }),
    { provider: "env", model: "m", output: {} },
    { engineBaseUrl: "http://e", fetch: fn },
  );
  assertEquals(out, { label: "spam", score: 0.9 });
});

test("runLlmJob: output.decision feeds the JSON through the DMN rails", async () => {
  const { fn, calls } = fakeFetch((url) =>
    url.includes("/chat/completions")
      ? { choices: [{ message: { content: '{"score":9}' } }] }
      : { output: { rating: "high" } }
  );
  const out = await runLlmJob(
    job({ prompt: "rate" }),
    { provider: "env", model: "m", output: { decision: "risk" } },
    { engineBaseUrl: "http://engine:8080", fetch: fn },
  );
  assertEquals(out, { rating: "high" });
  // The decision was called with the model's parsed JSON as its variables.
  const decisionCall = calls.find((c) =>
    c.url.includes("/decision-definitions/evaluation")
  )!;
  assertEquals(JSON.parse(String(decisionCall.init.body)).variables, {
    score: 9,
  });
});

test("runLlmJob: a scalar decision output is wrapped as { result }", async () => {
  const { fn } = fakeFetch((url) =>
    url.includes("/chat/completions")
      ? { choices: [{ message: { content: "{}" } }] }
      : { output: 42 }
  );
  const out = await runLlmJob(
    job({ prompt: "x" }),
    { provider: "env", model: "m", output: { decision: "d" } },
    { engineBaseUrl: "http://e", fetch: fn },
  );
  assertEquals(out, { result: 42 });
});

test("startLlmWorkers registers only llm workers, resolving their bindings", async () => {
  const registered: LlmWorkerOptions[] = [];
  const define: DefineWorkerFn = (opts) => registered.push(opts);
  const manifest: AppManifest = {
    workers: [
      { taskType: "save", handler: "workers/save/worker.ts" }, // handler → skipped
      { taskType: "classify", llm: "classifier" },
      { taskType: "summarize", llm: "concierge" },
    ],
    llm: {
      classifier: { provider: "env", model: "m1" },
      concierge: { provider: "env", model: "m2" },
    },
  };
  const started = await startLlmWorkers({
    manifest,
    define,
    baseUrl: "http://e",
  });
  assertEquals(started, ["classify", "summarize"]);
  assertEquals(registered.map((r) => r.type), ["classify", "summarize"]);
});

test("startLlmWorkers throws on an unknown binding reference", async () => {
  const manifest: AppManifest = {
    workers: [{ taskType: "x", llm: "ghost" }],
    llm: {},
  };
  await assertRejects(
    () => startLlmWorkers({ manifest, define: () => {}, baseUrl: "http://e" }),
    Error,
    "unknown llm binding 'ghost'",
  );
});

test("defineLlmWorker wires taskType → a handler that runs the model", async () => {
  const registered: LlmWorkerOptions[] = [];
  const { fn } = fakeFetch(() => ({
    choices: [{ message: { content: "ok" } }],
  }));
  defineLlmWorker(
    "greet",
    { provider: "env", model: "m" },
    { engineBaseUrl: "http://e", fetch: fn },
    (opts) => registered.push(opts),
  );
  assertEquals(registered[0].type, "greet");
  const out = await registered[0].handle(job({ prompt: "hi" }), {
    data: () => Promise.reject(),
  });
  assertEquals(out, { text: "ok" });
});
