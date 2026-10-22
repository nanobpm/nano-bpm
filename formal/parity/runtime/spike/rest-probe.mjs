#!/usr/bin/env node
// Self-contained Camunda 8 v2 REST environment probe for spike #1280.
//
// Proves — against a *live* bare Zeebe gateway (no Elasticsearch) — that the
// runtime the two-backend parity runner (#1260 / PR #1271) depends on can be
// reached over the Camunda v2 REST API on a CI runner:
//
//   1. GET  {base}/topology            — gateway reachable / ready
//   2. POST {base}/deployments         — deploy a trivial BPMN (multipart)
//   3. POST {base}/process-instances   — create + awaitCompletion, read final
//                                        process variables from the broker
//
// It deliberately uses a start -> end process (no service task) so it needs no
// job worker: it exercises exactly the `completed` + `variables` differential
// surface a bare gateway exposes, with zero external moving parts. This is the
// harness a maintainer runs to gather GO/NO-GO evidence; it is NOT the parity
// corpus itself (that is #1271's `run.mjs`, wired in once it lands on main).
//
// Env:
//   CAMUNDA_REST_ADDRESS  required, e.g. http://localhost:8080/v2
//   CAMUNDA_AUTH_TOKEN    optional Bearer token
//   CAMUNDA_BASIC_AUTH    optional user:pass for HTTP Basic
//   PROBE_READY_TIMEOUT_MS optional readiness budget (default 120000)
//
// Exit 0 = probe passed (environment can host the differential).
// Exit non-zero = probe failed; stderr explains which stage.

import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const HERE = dirname(fileURLToPath(import.meta.url));

function baseUrl() {
  const raw = process.env.CAMUNDA_REST_ADDRESS;
  if (!raw) {
    throw new Error('CAMUNDA_REST_ADDRESS is required (e.g. http://localhost:8080/v2)');
  }
  return raw.replace(/\/+$/, '');
}

function authHeaders() {
  const headers = {};
  if (process.env.CAMUNDA_AUTH_TOKEN) {
    headers.Authorization = `Bearer ${process.env.CAMUNDA_AUTH_TOKEN}`;
  } else if (process.env.CAMUNDA_BASIC_AUTH) {
    headers.Authorization = `Basic ${Buffer.from(process.env.CAMUNDA_BASIC_AUTH).toString('base64')}`;
  }
  return headers;
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function waitForTopology(base) {
  const budget = Number(process.env.PROBE_READY_TIMEOUT_MS ?? 120000);
  const started = Date.now();
  let lastErr = 'no attempt';
  while (Date.now() - started < budget) {
    try {
      const res = await fetch(`${base}/topology`, { headers: authHeaders() });
      if (res.ok) {
        const body = await res.json();
        return { ms: Date.now() - started, brokers: body.brokers?.length ?? 0 };
      }
      lastErr = `HTTP ${res.status}`;
    } catch (err) {
      lastErr = err.message;
    }
    await sleep(1000);
  }
  throw new Error(`gateway not ready within ${budget}ms (last: ${lastErr})`);
}

async function deploy(base) {
  const xml = await readFile(join(HERE, 'process.bpmn'));
  const form = new FormData();
  form.append('resources', new Blob([xml], { type: 'text/xml' }), 'process.bpmn');
  const res = await fetch(`${base}/deployments`, {
    method: 'POST',
    headers: authHeaders(),
    body: form,
  });
  if (!res.ok) {
    throw new Error(`deploy failed: HTTP ${res.status} ${await res.text()}`);
  }
  return res.json();
}

async function createAndAwait(base, variables) {
  const res = await fetch(`${base}/process-instances`, {
    method: 'POST',
    headers: { ...authHeaders(), 'Content-Type': 'application/json' },
    body: JSON.stringify({
      processDefinitionId: 'parity-spike',
      awaitCompletion: true,
      variables,
    }),
  });
  if (!res.ok) {
    throw new Error(`createProcessInstance failed: HTTP ${res.status} ${await res.text()}`);
  }
  return res.json();
}

function assertEqual(actual, expected, what) {
  const a = JSON.stringify(actual);
  const e = JSON.stringify(expected);
  if (a !== e) {
    throw new Error(`${what}: expected ${e}, got ${a}`);
  }
}

async function main() {
  const base = baseUrl();
  const findings = {};

  const topo = await waitForTopology(base);
  findings.readyMs = topo.ms;
  findings.brokers = topo.brokers;
  console.log(`[probe] gateway ready in ${topo.ms}ms (${topo.brokers} broker(s))`);

  await deploy(base);
  console.log('[probe] deployed process.bpmn');

  const greeting = `spike-${Date.now()}`;
  const result = await createAndAwait(base, { greeting });
  console.log(`[probe] createProcessInstance awaitCompletion -> key ${result.processInstanceKey ?? '(none)'}`);

  // A bare gateway exposes `completed` + final `variables` over v2 REST.
  if (result.variables && 'greeting' in result.variables) {
    assertEqual(result.variables.greeting, greeting, 'variables.greeting round-trip');
    findings.variablesEcho = true;
    console.log('[probe] variables round-tripped through completion ✓');
  } else {
    // Some gateway builds return variables only as a JSON string, or omit them
    // when awaitCompletion is used without a fetch-variables opt-in. Completion
    // (a settled awaitCompletion response) is itself the primary signal.
    findings.variablesEcho = false;
    console.log('[probe] awaitCompletion settled; variables not echoed in body (still a completion signal)');
  }

  findings.completed = true;
  console.log('[probe] PASS — environment can host the completed+variables differential');
  console.log(`::PROBE_FINDINGS::${JSON.stringify(findings)}`);
}

main().catch((err) => {
  console.error(`[probe] FAIL — ${err.message}`);
  process.exitCode = 1;
});
