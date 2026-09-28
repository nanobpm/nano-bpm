import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const source = readFileSync(new URL(
  "../../../engine-core/tests/fixtures/external-agent-job-type.bpmn", import.meta.url,
), "utf8");
const contract = readFileSync(new URL("../../../spec/agent-instances.yaml", import.meta.url), "utf8");

// Read field names directly from the vendored schema, not a second DTO checklist.
function assertFields(value, schema) {
  const start = contract.indexOf(`    ${schema}:\n`);
  assert.ok(start >= 0, `canonical schema ${schema} exists`);
  const block = contract.slice(start).split(/\n    (?=[A-Za-z]\w*:)/)[0];
  const properties = block.slice(block.indexOf("\n      properties:\n"));
  const fields = [...properties.matchAll(/^        ([\w.]+):/gm)].map((m) => m[1]).sort();
  assert.ok(fields.length > 0);
  assert.deepEqual(Object.keys(value).sort(), fields, schema);
}

const text = (value) => ({ contentType: "TEXT", text: value });
const turn = (id, role, extra = {}) => ({
  historyItemId: id, loopIteration: 1, role, content: [],
  producedAt: "2026-01-02T03:04:05.250Z", ...extra,
});
const configuration = (id) => turn(id, "CONFIGURATION", {
  model: "gpt", provider: "openai", systemPrompt: [text("Be helpful")],
  limits: { maxTokens: 1000, maxModelCalls: 10, maxToolCalls: 10 },
});

for (const variant of ["lean", "readmodel"]) {
  const entry = variant === "lean" ? "@nanobpm/engine-wasm" : "@nanobpm/engine-wasm/readmodel";
  const { initSync, TestEngine } = await import(entry);
  initSync({ module: readFileSync(require.resolve(
    `@nanobpm/engine-wasm/${variant}/nanobpmn_engine_bg.wasm`,
  )) });
  for (const marker of ["external", "aiAgentTask"]) {
    const engine = new TestEngine();
    try {
      engine.deploy(source.replace('agentType="external"', `agentType="${marker}"`));
      engine.createInstance("external-agent-routing", "{}");
      const [job] = JSON.parse(engine.activateJobs("senior:rebase", 1, 1000, "W", true));
      const attribution = {
        elementInstanceKey: job.elementInstanceKey, jobKey: job.key, jobLeaseToken: job.jobLeaseToken,
      };
      const request = { ...attribution, history: [configuration("initial")] };
      for (const field of ["elementInstanceKey", "jobKey", "jobLeaseToken", "history"]) {
        const missing = { ...request };
        delete missing[field];
        assert.throws(() => engine.createAgentInstance(JSON.stringify(missing)));
      }
      assert.throws(() => engine.createAgentInstance(JSON.stringify({ ...request, history: [] })));
      const created = JSON.parse(engine.createAgentInstance(JSON.stringify(request)));
      assertFields(created, "AgentInstanceCreationResult");
      assertFields(created.createdHistory[0], "AgentInstanceCreatedHistoryItem");
      assert.equal(created.createdHistory[0].isDuplicate, false);
      assert.throws(() => engine.createAgentInstance(JSON.stringify(request)), "duplicate CREATE rejects");
      const key = created.agentInstanceKey;
      const assistant = turn("attempt-one", "ASSISTANT", {
        content: [
          { contentType: "OBJECT", object: [1, true, { nested: null }] },
          text("hello"),
          {
            contentType: "DOCUMENT",
            documentReference: {
              "camunda.document.type": "camunda", storeId: "store", documentId: "doc",
              contentHash: null, metadata: {
                fileName: "note.txt", expiresAt: null, size: 5, contentType: "text/plain",
                customProperties: {}, processDefinitionId: null, processInstanceKey: null,
              },
            },
          },
        ],
        metrics: { inputTokens: 2, outputTokens: null, durationMs: 7 },
        toolCalls: [{ toolCallId: "call", toolName: "lookup", elementId: null, arguments: { id: 1 } }],
      });
      const update = { ...attribution, status: "THINKING", history: [assistant] };
      for (const field of ["elementInstanceKey", "jobKey", "jobLeaseToken"]) {
        const missing = { ...update };
        delete missing[field];
        assert.throws(() => engine.updateAgentInstance(key, JSON.stringify(missing)));
      }
      for (const field of ["metrics", "tools", "definition", "limits"]) {
        assert.throws(() => engine.updateAgentInstance(key, JSON.stringify({ ...update, [field]: {} })));
      }
      const updated = JSON.parse(engine.updateAgentInstance(key, JSON.stringify(update)));
      assertFields(updated, "AgentInstanceUpdateResult");
      const duplicate = JSON.parse(engine.updateAgentInstance(key, JSON.stringify(update)));
      assert.equal(duplicate.createdHistory[0].isDuplicate, true);
      assert.equal(duplicate.createdHistory[0].historyItemKey, updated.createdHistory[0].historyItemKey);

      if (variant === "readmodel") {
        assert.deepEqual(JSON.parse(engine.searchAgentInstanceHistory(key, "{}")).items, []);
        const pending = JSON.parse(engine.searchAgentInstanceHistory(
          key, '{"filter":{"commitStatus":{"$in":["PENDING"]}}}',
        )).items;
        assert.equal(pending.length, 2, "dedup does not append a second history record");
        const recorded = pending.find((item) => item.historyItemId === assistant.historyItemId);
        assertFields(recorded, "AgentInstanceHistoryItemResult");
        assert.deepEqual(recorded.content, assistant.content);
        assert.deepEqual(recorded.metrics, assistant.metrics);
        assert.deepEqual(recorded.toolCalls, assistant.toolCalls);
        assert.equal(recorded.jobLeaseToken, job.jobLeaseToken);
        assert.equal(JSON.parse(engine.searchAgentInstanceHistory(key, JSON.stringify({
          filter: { historyItemKey: recorded.historyItemKey, commitStatus: "PENDING", jobKey: job.key },
        }))).items.length, 1);
      }

      const metricShapes = [
        null,
        { inputTokens: null, outputTokens: null, durationMs: null },
        { inputTokens: -1, outputTokens: -2, durationMs: -1 },
        { inputTokens: 0, outputTokens: 0, durationMs: 0 },
      ];
      engine.updateAgentInstance(key, JSON.stringify({
        ...attribution,
        history: metricShapes.map((metrics, index) => turn(`metrics-${index}`, "ASSISTANT", { metrics })),
      }));
      if (variant === "readmodel") {
        const rows = JSON.parse(engine.searchAgentInstanceHistory(key, '{"commitStatus":"PENDING"}')).items;
        metricShapes.forEach((metrics, index) => {
          assert.deepEqual(rows.find((row) => row.historyItemId === `metrics-${index}`).metrics, metrics);
        });
      }

      engine.failJob(job.key, 2, "retry", job.jobLeaseToken);
      assert.deepEqual(JSON.parse(engine.activateJobs("senior:rebase", 1, 1000, "W", false)), []);
      const [retry] = JSON.parse(engine.activateJobs("senior:rebase", 1, 1000, "W", true));
      assert.notEqual(retry.jobLeaseToken, job.jobLeaseToken);
      assert.throws(() => engine.updateAgentInstance(key, JSON.stringify(update)), "old lease is fenced");
      const winner = {
        elementInstanceKey: retry.elementInstanceKey, jobKey: retry.key, jobLeaseToken: retry.jobLeaseToken,
        history: [
          turn("winning-config", "CONFIGURATION", { model: "new-model", tools: [] }),
          { ...assistant, historyItemId: "winning-turn" },
        ],
      };
      engine.updateAgentInstance(key, JSON.stringify(winner));
      engine.completeJob(retry.key, "{}", retry.jobLeaseToken);

      if (variant === "readmodel") {
        const agent = JSON.parse(engine.searchAgentInstances(JSON.stringify({
          filter: { agentInstanceKey: key, elementInstanceKeys: [retry.elementInstanceKey] },
        }))).items[0];
        assertFields(agent, "AgentInstanceResult");
        assertFields(agent.definition, "AgentInstanceDefinitionResult");
        assertFields(agent.metrics, "AgentInstanceMetrics");
        assert.equal(agent.status, "COMPLETED", "process cleanup completes its agent");
        assert.equal(agent.definition.model, "new-model");
        assert.deepEqual(agent.definition.systemPrompt, [text("Be helpful")]);
        assert.equal(agent.metrics.inputTokens, 2 * assistant.metrics.inputTokens,
          "accepted attempts count immediately; discarding history does not refund usage");
        assert.equal(agent.metrics.modelCalls, 2 + metricShapes.length,
          "each accepted ASSISTANT counts once, including absent/null metrics; duplicates do not");
        assert.equal(agent.metrics.toolCalls, 2 * assistant.toolCalls.length);
        const committed = JSON.parse(engine.searchAgentInstanceHistory(key, "{}")).items;
        assert.equal(committed.length, 2, "only the winning activation's history commits");
        assert.ok(committed.every((item) => item.jobLeaseToken === retry.jobLeaseToken));
        const discarded = JSON.parse(engine.searchAgentInstanceHistory(
          key, '{"commitStatus":"DISCARDED"}',
        )).items;
        assert.equal(discarded.length, 2 + metricShapes.length);
      }
    } finally {
      engine.free();
    }
  }
}
console.log("Canonical agent requests/results, opaque leases, pending history, dedup, and cleanup passed.");
