import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const source = readFileSync(
  new URL("../../../engine-core/tests/fixtures/external-agent-job-type.bpmn", import.meta.url),
  "utf8",
);

for (const variant of ["lean", "readmodel"]) {
  const entry = variant === "lean" ? "@nanobpm/engine-wasm" : "@nanobpm/engine-wasm/readmodel";
  const { initSync, TestEngine } = await import(entry);
  initSync({ module: readFileSync(require.resolve(
    `@nanobpm/engine-wasm/${variant}/nanobpmn_engine_bg.wasm`,
  )) });
  for (const marker of [null, "external", "aiAgentTask", "execution-listener"]) {
    let xml = source.replace('<zeebe:agentDefinition agentType="external"/>',
      marker === null || marker === "execution-listener" ? "" : `<zeebe:agentDefinition agentType="${marker}"/>`);
    const type = marker === "execution-listener" ? "before" : "senior:rebase";
    if (marker === "execution-listener") {
      xml = xml.replace('<zeebe:taskDefinition type="senior:rebase"/>',
        '<zeebe:taskDefinition type="senior:rebase"/><zeebe:executionListeners><zeebe:executionListener eventType="start" type="before"/></zeebe:executionListeners>');
    }
    for (const withLease of [undefined, null, false, true]) {
      const engine = new TestEngine();
      try {
        engine.deploy(xml);
        engine.createInstance("external-agent-routing", "{}");
        const [job] = JSON.parse(engine.activateJobs(type, 1, 100, "W", withLease));
        assert.ok(Object.hasOwn(job, "jobLeaseToken"), "jobLeaseToken is required even without a lease");
        assert.ok(!Object.hasOwn(job, "leaseToken") && !Object.hasOwn(job, "jobLease"), "activation exposes only the canonical jobLeaseToken name");
        if (withLease !== true) {
          assert.equal(job.jobLeaseToken, null, "leasing is opt-in for every marker");
          engine.completeJob(job.key, "{}");
          continue;
        }
        assert.equal(typeof job.jobLeaseToken, "string");
        assert.ok(job.jobLeaseToken.length > 0);
        for (const token of [undefined, `${job.jobLeaseToken}:stale`]) {
          assert.throws(() => engine.completeJob(job.key, "{}", token));
          assert.throws(() => engine.failJob(job.key, 2, "stale", token));
          assert.throws(() => engine.throwError(job.key, "ERR", "stale", token));
        }
        assert.throws(() => engine.updateRetries(job.key, 2, `${job.jobLeaseToken}:stale`));
        assert.throws(() => engine.updateTimeout(job.key, 100, `${job.jobLeaseToken}:stale`));
        for (const invalid of [1.5, Number.NaN, Number.POSITIVE_INFINITY, 2 ** 63, -(2 ** 64)]) {
          assert.throws(() => engine.updateTimeout(job.key, invalid, job.jobLeaseToken));
        }
        engine.updateTimeout(job.key, 100);
        engine.updateRetries(job.key, 3);
        engine.failJob(job.key, 2, "retry", job.jobLeaseToken);
        assert.deepEqual(JSON.parse(engine.activateJobs(type, 1, 100, "W", false)), [],
          "leased jobs cannot be reactivated by unleased workers");
        const [retry] = JSON.parse(engine.activateJobs(type, 1, 100, "W", true));
        assert.equal(typeof retry.jobLeaseToken, "string", "lease mode stays enabled after failure");
        assert.notEqual(retry.jobLeaseToken, job.jobLeaseToken);
        assert.throws(() => engine.completeJob(retry.key, "{}", job.jobLeaseToken));
        engine.advanceTime(101);
        const replayed = new TestEngine();
        replayed.replayEvents(engine.events());
        assert.deepEqual(JSON.parse(engine.activateJobs(type, 1, 100, "W")), []);
        const [expired] = JSON.parse(engine.activateJobs(type, 1, 100, "W", true));
        const [replayedJob] = JSON.parse(replayed.activateJobs(type, 1, 100, "W", true));
        assert.equal(replayedJob.jobLeaseToken, expired.jobLeaseToken, "event replay retains lease generation");
        replayed.completeJob(replayedJob.key, "{}", replayedJob.jobLeaseToken);
        replayed.free();
        assert.equal(typeof expired.jobLeaseToken, "string", "lease mode stays enabled after timeout");
        assert.notEqual(expired.jobLeaseToken, retry.jobLeaseToken);
        assert.throws(() => engine.completeJob(expired.key, "{}", retry.jobLeaseToken));
        engine.completeJob(expired.key, "{}", expired.jobLeaseToken);
      } finally {
        engine.free();
      }
      for (const timeout of [-5, 0, 5]) {
        const engine = new TestEngine();
        try {
          engine.deploy(source);
          engine.createInstance("external-agent-routing", "{}");
          engine.advanceTime(10);
          const [job] = JSON.parse(engine.activateJobs("senior:rebase", 1, 100, "W", true));
          engine.updateTimeout(job.key, timeout, job.jobLeaseToken);
          const snapshot = JSON.parse(engine.advanceTime(0));
          assert.equal(snapshot.jobs.find((entry) => entry.key === job.key).state,
            timeout <= 0 ? "Created" : "Activated");
          engine.completeJob(job.key, "{}", job.jobLeaseToken);
        } finally {
          engine.free();
        }
      }
    }
    const engine = new TestEngine();
    try {
      engine.deploy(source);
      engine.createInstance("external-agent-routing", "{}");
      const { jobs } = JSON.parse(engine.createInstance("external-agent-routing", "{}"));
      const remaining = JSON.parse(engine.completeJob(jobs[1].key, "{}")).jobs;
      assert.equal(remaining.find((job) => job.key === jobs[0].key).state, "Created",
        "implicit completion must not activate unrelated jobs of the same type");
    } finally {
      engine.free();
    }
  }
}
console.log("Generic opt-in leasing, sticky retries/timeouts, and opaque fencing passed in both variants.");
