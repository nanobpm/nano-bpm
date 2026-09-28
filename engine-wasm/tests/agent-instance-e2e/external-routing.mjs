import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const model = readFileSync(
  new URL("../../../engine-core/tests/fixtures/external-agent-job-type.bpmn", import.meta.url),
  "utf8",
);

for (const variant of ["lean", "readmodel"]) {
  const entrypoint = variant === "lean" ? "@nanobpm/engine-wasm" : "@nanobpm/engine-wasm/readmodel";
  const { initSync, TestEngine } = await import(entrypoint);
  initSync({
    module: readFileSync(require.resolve(`@nanobpm/engine-wasm/${variant}/nanobpmn_engine_bg.wasm`)),
  });
  const cases = ["external", "aiAgentTask"].flatMap((agentType) =>
    ["senior:rebase", "= localRoute", null].map((type) => [agentType, type]));
  for (const [agentType, type] of cases) {
    const engine = new TestEngine();
    try {
      const xml = model.replace('agentType="external"', `agentType="${agentType}"`).replace(
        '<zeebe:taskDefinition type="senior:rebase"/>',
        `${type === null ? "" : `<zeebe:taskDefinition type="${type}" retries="= attempts"/>`}
        <zeebe:priorityDefinition priority="= importance"/>
        <zeebe:taskHeaders><zeebe:header key="channel" value="agent"/></zeebe:taskHeaders>
        <zeebe:linkedResources>
          <zeebe:linkedResource resourceId="prompt.md" bindingType="latest"
            resourceType="GenericScript" linkName="systemPrompt"/>
        </zeebe:linkedResources>`,
      );
      engine.deploy(xml);
      engine.deployResource("prompt.md", "first prompt");
      const prompt = JSON.parse(engine.deployResource("prompt.md", "latest prompt"));
      engine.createInstance("external-agent-routing", '{"route":"senior:rebase","attempts":7,"importance":80}');
      if (variant === "readmodel") {
        assert.deepEqual(JSON.parse(engine.searchAgentInstances("{}")).items, []);
      }
      const expected = type === null ? "agent" : "senior:rebase";
      if (type !== null) {
        assert.deepEqual(JSON.parse(engine.activateJobs("agent", 1, 1000, "W")), []);
      }
      const jobs = JSON.parse(engine.activateJobs(expected, 1, 1000, "W", true));
      assert.equal(jobs.length, 1, `${variant}: ${agentType}: ${type ?? "element-id fallback"}`);
      const job = jobs[0];
      assert.equal(job.type, expected);
      assert.equal(job.elementId, "agent");
      assert.equal(job.priority, 80);
      assert.equal(job.retries, type === null ? 3 : 7);
      assert.equal(job.customHeaders.channel, "agent");
      const resources = JSON.parse(job.customHeaders.linkedResources);
      assert.equal(resources.length, 1);
      assert.equal(resources[0].resourceKey, prompt.resourceKey);
      assert.equal(resources[0].linkName, "systemPrompt");
      if (variant === "readmodel") {
        const resolved = JSON.parse(engine.getResourceByKey(prompt.resourceKey));
        assert.equal(resolved.resourceId, "prompt.md");
        assert.equal(resolved.version, 2);
      }
      assert.equal(typeof job.jobLeaseToken, "string");
      const request = {
        elementInstanceKey: job.elementInstanceKey,
        jobKey: job.key,
        jobLeaseToken: job.jobLeaseToken,
        history: [{
          historyItemId: "configuration", loopIteration: 1,
          producedAt: "2026-01-02T03:04:05Z", role: "CONFIGURATION",
          content: [], model: "gpt", provider: "openai",
          systemPrompt: [{ contentType: "TEXT", text: "Use the linked prompt" }],
        }],
      };
      assert.throws(() => engine.createAgentInstance(JSON.stringify({
        ...request,
        jobLeaseToken: `${job.jobLeaseToken}:stale`,
      })));
      // #1283 deprecation window: a CONFLICTING dual-send (canonical + legacy
      // with different values) is rejected loudly rather than fencing on a
      // stale token.
      assert.throws(() => engine.createAgentInstance(JSON.stringify({
        ...request,
        jobLease: `${job.jobLeaseToken}:legacy`,
      })), "conflicting jobLeaseToken/jobLease pair must be rejected");
      // An EQUAL dual-send (both names, same value) is ACCEPTED — the previous
      // serde-alias shape folded both onto one field and rejected an equal
      // dual-send as a duplicate field, making the WASM window inconsistent
      // with REST. Exercise the successful create through the dual-send path.
      const created = JSON.parse(engine.createAgentInstance(JSON.stringify({
        ...request,
        jobLease: job.jobLeaseToken,
      })));
      assert.equal(created.createdHistory.length, 1);
      if (variant === "readmodel") {
        const agents = JSON.parse(engine.searchAgentInstances("{}")).items;
        assert.equal(agents.length, 1);
        assert.deepEqual(agents[0].elementInstanceKeys, [job.elementInstanceKey]);
        const update = {
          elementInstanceKey: job.elementInstanceKey,
          jobKey: job.key, jobLeaseToken: job.jobLeaseToken,
        };
        assert.throws(() => engine.updateAgentInstance(created.agentInstanceKey, JSON.stringify({
          ...update,
          jobKey: "7788990011",
        })), "supplied unknown job attribution must not be ignored");
        assert.throws(() => engine.updateAgentInstance(created.agentInstanceKey, JSON.stringify({
          ...update,
          jobLeaseToken: `${job.jobLeaseToken}:stale`,
        })), "supplied stale lease must not be ignored");
        // #1283: conflicting dual-send on UPDATE is rejected loudly.
        assert.throws(() => engine.updateAgentInstance(created.agentInstanceKey, JSON.stringify({
          ...update,
          jobLease: `${job.jobLeaseToken}:legacy`,
        })), "conflicting update jobLeaseToken/jobLease pair must be rejected");
        // A legacy-only `jobLease` is honored through reconcile — a stale one
        // reaches the fence and is rejected, proving it is not silently dropped.
        assert.throws(() => engine.updateAgentInstance(created.agentInstanceKey, JSON.stringify({
          elementInstanceKey: job.elementInstanceKey, jobKey: job.key,
          jobLease: `${job.jobLeaseToken}:stale`,
        })), "legacy-only jobLease is honored (a stale one is rejected, not ignored)");
      }
      const completed = JSON.parse(engine.completeJob(job.key, "{}", job.jobLeaseToken));
      assert.equal(completed.instances[0].state, "Completed");
      if (variant === "readmodel") {
        assert.equal(JSON.parse(engine.searchAgentInstances("{}")).items[0].status, "COMPLETED");
      }
    } finally {
      engine.free();
    }
  }
}
console.log("External and aiAgentTask routing and metadata passed for both WASM entrypoints.");
