// Package-level end-to-end guard (#1168): the advertised `sendTask` and
// `inclusiveGateway` support must actually work through the COMMITTED
// `@nanobpm/engine-wasm` artifacts — the browser/console execution surface —
// not merely the native Rust `Engine`. The native `parsed_inclusive_sendtask_exec.rs`
// integration test can pass while a stale or un-regenerated committed `pkg`
// wasm still rejects these element kinds; this probe deploys the same canonical
// diagrams (shared verbatim from `engine-core/tests/fixtures/`, so there is no
// second copy to drift) and creates instances against BOTH shipped subpath
// variants (lean + read-model), failing if either wasm cannot handle them.
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);

const sendTask = readFileSync(new URL(
  "../../../engine-core/tests/fixtures/send-task.bpmn", import.meta.url,
), "utf8");
const inclusive = readFileSync(new URL(
  "../../../engine-core/tests/fixtures/inclusive-gateway.bpmn", import.meta.url,
), "utf8");
const escalation = readFileSync(new URL(
  "../../../engine-core/tests/fixtures/escalation-boundary.bpmn", import.meta.url,
), "utf8");

for (const variant of ["lean", "readmodel"]) {
  const entry = variant === "lean" ? "@nanobpm/engine-wasm" : "@nanobpm/engine-wasm/readmodel";
  const { initSync, TestEngine } = await import(entry);
  initSync({ module: readFileSync(require.resolve(
    `@nanobpm/engine-wasm/${variant}/nanobpmn_engine_bg.wasm`,
  )) });

  // sendTask: deploy, start an instance, and confirm it activates a worker job
  // exactly like a service task (the advertised sendTask execution semantics),
  // then complete that job and assert the instance drives to `Completed` — a
  // stale/broken committed wasm could activate the job yet fail the completion
  // path, so the smoke test must exercise the full run, matching the native
  // `parsed_inclusive_sendtask_exec.rs` coverage.
  {
    const engine = new TestEngine();
    engine.deploy(sendTask);
    engine.createInstance("notify", "{}");
    const jobs = JSON.parse(engine.activateJobs("notifier", 1, 1000, "W", true));
    assert.equal(jobs.length, 1, `${variant}: sendTask must activate one 'notifier' job`);
    assert.equal(jobs[0].elementId, "send", `${variant}: sendTask job carries its element id`);
    const snap = JSON.parse(engine.completeJob(jobs[0].key, "{}", jobs[0].jobLeaseToken ?? null));
    const inst = snap.instances.find((i) => i.processId === "notify");
    assert.equal(inst?.state, "Completed",
      `${variant}: completing the sendTask job must drive the instance to Completed`);
  }

  // inclusiveGateway split/join: deploy and start an instance with the branch
  // condition satisfied — the committed wasm must accept the gateway kind, route
  // the conditional branch, and (critically) synchronise the join and complete
  // the instance once that branch's job finishes. Stopping at job activation
  // would let a broken join/completion path pass, so drive it to `Completed`.
  {
    const engine = new TestEngine();
    engine.deploy(inclusive);
    engine.createInstance("review", JSON.stringify({ go: true }));
    const jobs = JSON.parse(engine.activateJobs("ta", 1, 1000, "W", true));
    assert.equal(jobs.length, 1, `${variant}: inclusiveGateway must route the conditional branch`);
    const snap = JSON.parse(engine.completeJob(jobs[0].key, "{}", jobs[0].jobLeaseToken ?? null));
    const inst = snap.instances.find((i) => i.processId === "review");
    assert.equal(inst?.state, "Completed",
      `${variant}: the inclusive join must synchronise and complete the instance`);
  }

  console.log(`[${variant}] sendTask + inclusiveGateway deploy/createInstance OK`);

  // escalation: the committed wasm must now *execute* escalation events (#1173),
  // not reject them. Deploy an embedded sub-process whose escalation throw is
  // caught by an interrupting escalation boundary routing to a `handle` service
  // task; creating an instance raises + catches the escalation, tears the
  // sub-process down, and arms the boundary handler job. Drive that job to
  // completion and assert the instance reaches `Completed` — a stale/broken
  // committed wasm that still rejected the carrier (or demoted the throw to a
  // none pass-through) would fail here through both shipped variants. Mirrors
  // the native `should_interrupt_a_subprocess_via_an_interrupting_escalation_boundary`.
  {
    const engine = new TestEngine();
    engine.deploy(escalation);
    engine.createInstance("escalate", "{}");
    const jobs = JSON.parse(engine.activateJobs("handle", 1, 1000, "W", true));
    assert.equal(jobs.length, 1,
      `${variant}: the interrupting escalation boundary must arm one 'handle' handler job`);
    assert.equal(jobs[0].elementId, "handle",
      `${variant}: the escalation handler job carries its element id`);
    const snap = JSON.parse(engine.completeJob(jobs[0].key, "{}", jobs[0].jobLeaseToken ?? null));
    const inst = snap.instances.find((i) => i.processId === "escalate");
    assert.equal(inst?.state, "Completed",
      `${variant}: completing the escalation handler job must drive the instance to Completed`);
  }

  console.log(`[${variant}] escalation boundary deploy/execute OK`);
}

console.log("element-support: all variants OK");
