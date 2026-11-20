// End-to-end: the differential fuzzer's extended vocabulary (#1673) runs on
// the nano engine. For a minimal graph per new label — a timer catch, an
// embedded subprocess, interrupting timer / message boundaries — and for
// instance cancellation, EVERY explored command order's scenario (BPMN from
// `bpmnFor`, steps from `scenariosFor`) must satisfy its Reference `expect`
// block on nano. The pure Reference / emitter tests live in
// formal/corpus/vocabulary.test.mjs.

import { test } from "node:test";
import assert from "node:assert/strict";

import { NanoBackend } from "./nano-backend.mjs";
import { CamundaBackend } from "./camunda-backend.mjs";
import { runScenario } from "./driver.mjs";
import { checkExpectations } from "./observation.mjs";
import { materialize, exploreOrders, scenariosFor } from "../../corpus/enumerate.mjs";
import { bpmnFor } from "../../corpus/generate.mjs";

const CASES = {
  timer: { kinds: [], edges: [[0, 1, "timer"]] },
  sub: { kinds: [], edges: [[0, 1, "sub"]] },
  btimer: { kinds: [], edges: [[0, 1, "btimer"]] },
  bmsg: { kinds: [], edges: [[0, 1, "bmsg"]] },
  "boundaries into a merge": {
    kinds: ["xor"],
    edges: [[0, 1, "btimer"], [1, 2, "bmsg"]],
  },
  "parallel timer + subprocess": {
    kinds: ["and", "and"],
    edges: [[0, 1, "direct"], [1, 2, "timer"], [1, 2, "sub"], [2, 3, "direct"]],
  },
};

async function runAll(skel, opts) {
  const graph = materialize(skel);
  const xml = bpmnFor(graph, { routing: "variables" });
  const { scenarios } = scenariosFor(graph, exploreOrders(graph, {}, opts));
  assert.ok(scenarios.length > 0);
  const nano = await new NanoBackend().init();
  try {
    for (const sc of scenarios) {
      const obs = await runScenario(nano, { ...sc, xml });
      const check = checkExpectations(obs, sc.expect);
      assert.ok(
        check.ok,
        `${sc.name} ${JSON.stringify(sc.steps)}: ${JSON.stringify(check.mismatches)}`,
      );
    }
  } finally {
    await nano.close();
  }
  return scenarios;
}

for (const [name, skel] of Object.entries(CASES)) {
  test(`#1673 vocabulary on nano: ${name} — every command order matches the Reference`, async () => {
    await runAll(skel);
  });
}

test("#1673 vocabulary on nano: instance cancellation matches the Reference", async () => {
  const scenarios = await runAll(
    { kinds: ["and", "and"], edges: [[0, 1, "direct"], [1, 2, "btimer"], [1, 2, "sub"], [2, 3, "direct"]] },
    { cancel: true },
  );
  assert.ok(scenarios.some((sc) => sc.expect.cancelled === true));
});

test("#1673 CamundaBackend.cancelInstance fails loudly with no learned instance key", async () => {
  const backend = new CamundaBackend({ address: "http://127.0.0.1:1" });
  await assert.rejects(
    backend.cancelInstance({ instanceKeys: new Set() }),
    /no process instance key learned/,
  );
});
