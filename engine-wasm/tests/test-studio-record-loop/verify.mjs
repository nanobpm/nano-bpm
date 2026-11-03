// Test-Studio-style record loop, run against the built `@nanobpm/engine-wasm`
// TestEngine — a worked example AND a regression guard.
//
// Camunda's "Test Studio" recording feature needs per-instance isolation so a
// recording session can stop at each job and each call activity and ask the user
// "mock this, or run the real thing?". In a *shared cluster* that requires two
// bolt-on engine switches (see the mock-architecture design doc):
//
//   * RESERVE_JOBS      — stamp a token so no worker/connector races to grab the job.
//   * stubCallActivities — activate a call activity with no child, waiting on a
//                          stub job instead of starting the real child cluster-wide.
//
// The nanobpmn WASM engine is embedded and single-session: there is no shared
// cluster and no external worker pool, so that isolation is *structural*. This
// probe proves the two Test-Studio switches collapse into behaviour the engine
// already has:
//
//   LOOP 1 (replaces RESERVE_JOBS):     jobs sit `Created` (== Zeebe ACTIVATABLE)
//     with no worker taking them; the recorder drives them by key. `zeebe:ioMapping`
//     still runs engine-side (the connector *runtime* does not) — so a mocked
//     connector value must be the raw runtime result (pre-mapping), exactly as the
//     doc warns.
//   LOOP 2 (replaces stubCallActivities): an `elementActivated` breakpoint on the
//     call activity is the wait state. Resume to run the real child
//     (runCalledProcess); reset without driving the child to mock it.
//     An `everyStep` breakpoint re-parks the run at nested call activities too
//     (an id-scoped `elementActivated` breakpoint only matches its own element id).
//
// Run: `make console-wasm` (from the repo root; engine-wasm/pkg is a gitignored
// build output, #668), then `npm install && npm test` here.
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const j = (s) => JSON.parse(s);

// A process with a service task (job worker) followed by a connector-style task,
// so the record loop has two distinct "waiting for a decision" jobs. The connector
// task carries a zeebe:ioMapping so we can show the engine applies output mappings
// even though no connector runtime runs.
const orderXml = `<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="d" targetNamespace="x">
 <bpmn:process id="order" isExecutable="true">
  <bpmn:startEvent id="s"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
  <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="charge"/>
  <bpmn:serviceTask id="charge" name="Charge card">
   <bpmn:extensionElements><zeebe:taskDefinition type="payment"/></bpmn:extensionElements>
   <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing></bpmn:serviceTask>
  <bpmn:sequenceFlow id="f2" sourceRef="charge" targetRef="ship"/>
  <bpmn:serviceTask id="ship" name="Ship (HTTP connector)">
   <bpmn:extensionElements>
    <zeebe:taskDefinition type="io.camunda:http-json:1"/>
    <zeebe:ioMapping><zeebe:output source="=response.statusCode" target="shipStatus"/></zeebe:ioMapping>
   </bpmn:extensionElements>
   <bpmn:incoming>f2</bpmn:incoming><bpmn:outgoing>f3</bpmn:outgoing></bpmn:serviceTask>
  <bpmn:sequenceFlow id="f3" sourceRef="ship" targetRef="hold"/>
  <bpmn:serviceTask id="hold" name="Hold (keeps instance active for inspection)">
   <bpmn:extensionElements><zeebe:taskDefinition type="hold"/></bpmn:extensionElements>
   <bpmn:incoming>f3</bpmn:incoming><bpmn:outgoing>f4</bpmn:outgoing></bpmn:serviceTask>
  <bpmn:sequenceFlow id="f4" sourceRef="hold" targetRef="e"/>
  <bpmn:endEvent id="e"><bpmn:incoming>f4</bpmn:incoming></bpmn:endEvent>
 </bpmn:process>
</bpmn:definitions>`;

const childXml = `<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="dc" targetNamespace="x">
 <bpmn:process id="child" isExecutable="true">
  <bpmn:startEvent id="cs"><bpmn:outgoing>cf1</bpmn:outgoing></bpmn:startEvent>
  <bpmn:sequenceFlow id="cf1" sourceRef="cs" targetRef="ct"/>
  <bpmn:serviceTask id="ct" name="Child work">
   <bpmn:extensionElements><zeebe:taskDefinition type="childjob"/></bpmn:extensionElements>
   <bpmn:incoming>cf1</bpmn:incoming><bpmn:outgoing>cf2</bpmn:outgoing></bpmn:serviceTask>
  <bpmn:sequenceFlow id="cf2" sourceRef="ct" targetRef="ce"/>
  <bpmn:endEvent id="ce"><bpmn:incoming>cf2</bpmn:incoming></bpmn:endEvent>
 </bpmn:process>
</bpmn:definitions>`;

const parentXml = `<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="dp" targetNamespace="x">
 <bpmn:process id="parent" isExecutable="true">
  <bpmn:startEvent id="ps"><bpmn:outgoing>pf1</bpmn:outgoing></bpmn:startEvent>
  <bpmn:sequenceFlow id="pf1" sourceRef="ps" targetRef="call"/>
  <bpmn:callActivity id="call" name="Do child">
   <bpmn:extensionElements><zeebe:calledElement processId="child" propagateAllChildVariables="true"/></bpmn:extensionElements>
   <bpmn:incoming>pf1</bpmn:incoming><bpmn:outgoing>pf2</bpmn:outgoing></bpmn:callActivity>
  <bpmn:sequenceFlow id="pf2" sourceRef="call" targetRef="pe"/>
  <bpmn:endEvent id="pe"><bpmn:incoming>pf2</bpmn:incoming></bpmn:endEvent>
 </bpmn:process>
</bpmn:definitions>`;

// LOOP 1 — record loop: mock every job. No RESERVE_JOBS token, no worker race:
// the jobs are simply never activated by anyone but the recorder.
function recordByMockingEveryJob(TestEngine) {
  const eng = new TestEngine();
  eng.deploy(orderXml);
  let snap = j(eng.createInstance("order", "{}"));

  const recording = [];
  let guard = 0;
  while (guard++ < 50) {
    // The recorder's single inbox: everything waiting for a decision. Jobs sit in
    // `Created` (Zeebe ACTIVATABLE) — proof there is no competing worker.
    const waiting = snap.jobs.filter((x) => x.state === "Created");
    if (!waiting.length) break;
    const job = waiting[0];
    // The user decides. Here we always mock. For the connector, the mock is the
    // engine's view AFTER the (nonexistent) runtime would have run — i.e. the raw
    // shape the model's zeebe:ioMapping consumes.
    const mock =
      job.jobType === "payment" ? { chargeStatus: "ok" }
      : job.jobType === "io.camunda:http-json:1" ? { response: { statusCode: 200 } }
      : {}; // hold: complete with nothing, just to drain the instance
    snap = j(eng.completeJob(job.key, JSON.stringify(mock)));
    recording.push({ elementId: job.elementId, mock });
  }

  // Exactly the three jobs were surfaced and mocked, in order — no job was ever
  // taken by a phantom worker, and none was missed.
  assert.deepEqual(
    recording.map((r) => r.elementId),
    ["charge", "ship", "hold"],
    "recorder saw every job, unraced, in order",
  );
  assert.equal(snap.completedInstances, 1, "process completed via mocks alone");
  assert.equal(snap.activeElementIds.length, 0, "no element left waiting");
  eng.free();
}

// The connector caveat, made concrete: `zeebe:ioMapping` DOES run engine-side even
// though the connector runtime never does — so a mocked connector value must be the
// raw runtime result (pre-mapping). We pause on the task AFTER `ship` to read the mapped var
// while the instance is still active (a completed instance reports no variables).
function connectorOutputMappingRunsEngineSide(TestEngine) {
  const eng = new TestEngine();
  eng.deploy(orderXml);
  let snap = j(eng.createInstance("order", "{}"));

  const complete = (type, vars) => {
    const job = snap.jobs.find((x) => x.jobType === type && x.state === "Created");
    assert.ok(job, `job ${type} is waiting`);
    snap = j(eng.completeJob(job.key, JSON.stringify(vars)));
  };
  complete("payment", { chargeStatus: "ok" });
  // Mock the connector's RAW response; the model's output mapping should derive
  // `shipStatus` from `=response.statusCode` — engine-side, no runtime involved.
  complete("io.camunda:http-json:1", { response: { statusCode: 200 } });

  const vars = snap.instances[0]?.variables ?? {};
  assert.equal(vars.shipStatus, 200, "engine applied zeebe:ioMapping output on completion");
  assert.deepEqual(snap.activeElementIds, ["hold"], "paused on the task after the connector");
  eng.free();
}

// LOOP 2 — the call-activity wait state, via the debug stepper. An
// `elementActivated` breakpoint on the call activity pauses the run there, which is
// the decision point stubCallActivities engineers into Zeebe.
//
// Fidelity note: the WASM engine spawns the child *instance* eagerly when the call
// activity activates, so at the pause the child root already exists — but it is
// IDLE (no token, no job) until you resume. This differs from Zeebe's stub, which
// creates no child at all. The recorder's decision point is nonetheless the same:
// resume to let the child do real work, or don't (mock). A clean "complete the call
// element with mocked child outputs" is the residual documented in the README.
function callActivityWaitState(TestEngine) {
  const bps = JSON.stringify([{ kind: "elementActivated", id: "call" }]);

  // The wait state itself: paused on the call activity, with the child spawned but
  // IDLE — no child job is served yet, so nothing has raced ahead of the decision.
  const assertParkedAtDecision = (eng) => {
    const st = j(eng.debugCreateInstance("parent", "{}", bps));
    assert.equal(st.paused, true, "paused at the call activity");
    assert.deepEqual(st.activeElements, ["call"], "the call activity is the pause point");
    const snap = j(eng.snapshot());
    const childInst = snap.instances.find((i) => i.processId === "child");
    assert.ok(childInst, "child instance root exists (engine spawns it eagerly)");
    assert.deepEqual(childInst.activeElements ?? [], [], "child is idle — no token yet");
    assert.equal(
      snap.jobs.filter((x) => x.jobType === "childjob").length,
      0,
      "no child job is served while the recorder is deciding",
    );
  };

  // 2a — RUN the real child (the "runCalledProcess: true" branch). Resuming past
  // the breakpoint lets the child advance; its job also waits unraced.
  {
    const eng = new TestEngine();
    eng.deploy(childXml);
    eng.deploy(parentXml);
    assertParkedAtDecision(eng);

    eng.debugResume(); // let the real child do its work
    let snap = j(eng.snapshot());
    const cj = snap.jobs.find((x) => x.jobType === "childjob");
    assert.equal(cj?.state, "Created", "child's job waits unraced too");
    snap = j(eng.completeJob(cj.key, "{}"));
    assert.equal(snap.completedInstances, 2, "parent + child both completed");
    eng.debugClear();
    eng.free();
  }

  // 2b — the MOCK branch reaches the identical wait state and, crucially, no child
  // job has been served: the child has done no observable work, so the recorder
  // holds the decision. (Applying mocked outputs + completing the call element is
  // the residual — see the README.) We tear down with `reset()`, not `debugClear()`:
  // `debugClear()` on a paused run drains the in-flight command (resuming the call
  // activity and running the real child), which would defeat the point of the mock
  // branch; `reset()` discards the paused state so the child never runs.
  {
    const eng = new TestEngine();
    eng.deploy(childXml);
    eng.deploy(parentXml);
    assertParkedAtDecision(eng);
    eng.reset();
    eng.free();
  }
}

// The advertised behaviour must hold on BOTH wasm variants, so a stale or
// un-regenerated pkg cannot pass the native Rust tests while regressing here.
for (const variant of ["lean", "readmodel"]) {
  const entry = variant === "lean" ? "@nanobpm/engine-wasm" : "@nanobpm/engine-wasm/readmodel";
  const { initSync, TestEngine } = await import(entry);
  initSync({
    module: readFileSync(require.resolve(`@nanobpm/engine-wasm/${variant}/nanobpmn_engine_bg.wasm`)),
  });

  recordByMockingEveryJob(TestEngine);
  connectorOutputMappingRunsEngineSide(TestEngine);
  callActivityWaitState(TestEngine);

  console.log(`test-studio-record-loop: OK (${variant})`);
}
