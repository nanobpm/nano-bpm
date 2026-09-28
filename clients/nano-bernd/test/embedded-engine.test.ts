import { describe, it, expect, afterEach } from 'vitest';
import { readFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { EmbeddedEngine, EXPECTED_ABI_VERSION, type WasmManifest } from '../src/index.js';

const HERE = dirname(fileURLToPath(import.meta.url));
const WASM_DIR = join(HERE, '..', 'wasm');

const TRIVIAL_BPMN = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f" sourceRef="s" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>`;

describe('EmbeddedEngine (nano_engine.wasm FFI)', () => {
  let host: EmbeddedEngine | undefined;

  afterEach(() => {
    host?.close();
    host = undefined;
  });

  it('loads the packaged wasm, deploys, and completes a straight-through process', async () => {
    host = await EmbeddedEngine.create();

    expect(host.manifest.abi_version).toBe(EXPECTED_ABI_VERSION);
    expect(host.manifest.imports).toEqual([]); // engine must stay self-contained
    expect(host.instanceCount()).toBe(0);

    const { count } = host.deploy(TRIVIAL_BPMN);
    expect(count).toBeGreaterThanOrEqual(1);

    const { processInstanceKey } = host.createInstance('p');
    expect(processInstanceKey).toMatch(/^\d+$/);
    expect(processInstanceKey).not.toBe('0');

    // Start -> End: engine should complete synchronously.
    expect(host.isCompleted(processInstanceKey)).toBe(true);
    expect(host.instanceCount()).toBe(1);
  });

  it('fails cleanly on ABI version mismatch', async () => {
    const bytes = await readFile(join(WASM_DIR, 'nano_engine.wasm'));
    const realManifest = JSON.parse(
      await readFile(join(WASM_DIR, 'manifest.json'), 'utf8'),
    ) as WasmManifest;

    const bogusManifest: WasmManifest = { ...realManifest, abi_version: 9999 };
    await expect(
      EmbeddedEngine.create({ wasmBytes: bytes, manifest: bogusManifest }),
    ).rejects.toThrow(/ABI mismatch/);
  });

  it('rejects createInstance for an unknown process id', async () => {
    host = await EmbeddedEngine.create();
    host.deploy(TRIVIAL_BPMN);
    expect(() => host!.createInstance('does-not-exist')).toThrow(/no such process id/);
  });

  it('reports the injected clock through triggerTimers without error', async () => {
    host = await EmbeddedEngine.create();
    host.deploy(TRIVIAL_BPMN);
    // The trivial process has no timers; triggering must be a safe no-op returning 0.
    expect(host.triggerTimers(Date.now())).toBe(0n);
  });

  it('refuses further use after close()', async () => {
    host = await EmbeddedEngine.create();
    host.close();
    expect(() => host!.deploy(TRIVIAL_BPMN)).toThrow(/closed/);
  });

  it.each(['external', 'aiAgentTask'])('runs %s as an ordinary job through the FFI pass-through', async (agentType) => {
    host = await EmbeddedEngine.create();
    const fixture = await readFile(
      join(HERE, '../../../engine-core/tests/fixtures/external-agent-job-type.bpmn'),
      'utf8',
    );
    const { count } = host.deploy(
      fixture.replace('agentType="external"', `agentType="${agentType}"`),
    );
    expect(count).toBeGreaterThanOrEqual(1);

    const { processInstanceKey } = host.createInstance('external-agent-routing', 1000);
    expect(processInstanceKey).toMatch(/^\d+$/);
    expect(processInstanceKey).not.toBe('0');
    expect(host.instanceCount()).toBe(1);

    expect(host.isCompleted(processInstanceKey)).toBe(false);
    const jobs = host.activateJobs({
      type: 'senior:rebase',
      worker: 'w1',
      maxJobs: 10,
      timeoutMs: 30_000,
      now: 1000,
    });
    expect(jobs).toHaveLength(1);
    expect(jobs[0]).toMatchObject({
      type: 'senior:rebase',
      instanceKey: processInstanceKey,
      elementId: 'agent',
      worker: 'w1',
      deadline: 31000,
      jobLeaseToken: null,
    });
    host.completeJob(jobs[0]!.key);
    expect(host.isCompleted(processInstanceKey)).toBe(true);
  });

  it('drives the full job worker lifecycle: activate → complete', async () => {
    host = await EmbeddedEngine.create();
    const SERVICE_BPMN = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="svc" isExecutable="true">
    <bpmn:startEvent id="s"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
    <bpmn:serviceTask id="t" name="Do work">
      <bpmn:extensionElements><zeebe:taskDefinition type="do-work" /></bpmn:extensionElements>
      <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:endEvent id="e"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="t" />
    <bpmn:sequenceFlow id="f2" sourceRef="t" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>`;
    host.deploy(SERVICE_BPMN);
    const { processInstanceKey } = host.createInstance('svc', 1000);
    expect(host.isCompleted(processInstanceKey)).toBe(false);

    const activated = host.activateJobs({
      type: 'do-work',
      worker: 'w1',
      maxJobs: 10,
      timeoutMs: 30_000,
      now: 1000,
    });
    expect(activated).toHaveLength(1);
    const job = activated[0]!;
    expect(job.type).toBe('do-work');
    expect(job.worker).toBe('w1');
    expect(job.elementId).toBe('t');
    expect(job.retries).toBeGreaterThan(0);
    expect(job.deadline).toBe(31000);
    expect(job.key).toMatch(/^\d+$/);

    host.completeJob(job.key);
    expect(host.isCompleted(processInstanceKey)).toBe(true);
  });

  it('returns an empty array when no jobs of the requested type are available', async () => {
    host = await EmbeddedEngine.create();
    host.deploy(TRIVIAL_BPMN);
    const activated = host.activateJobs({ type: 'nothing', worker: 'w', maxJobs: 5, now: 1 });
    expect(activated).toEqual([]);
  });

  it('fails a job with retries and allows re-activation on the next tick', async () => {
    host = await EmbeddedEngine.create();
    const SERVICE_BPMN = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="svc" isExecutable="true">
    <bpmn:startEvent id="s"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
    <bpmn:serviceTask id="t">
      <bpmn:extensionElements><zeebe:taskDefinition type="flaky" retries="3" /></bpmn:extensionElements>
      <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:endEvent id="e"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="t" />
    <bpmn:sequenceFlow id="f2" sourceRef="t" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>`;
    host.deploy(SERVICE_BPMN);
    host.createInstance('svc', 1000);

    const first = host.activateJobs({ type: 'flaky', worker: 'w', maxJobs: 1, now: 1000 });
    expect(first).toHaveLength(1);
    host.failJob(first[0]!.key, 2, 'transient upstream error');

    // After expiring the failed activation lock, the job must be re-activatable.
    host.expireJobs(2_000_000);
    const second = host.activateJobs({ type: 'flaky', worker: 'w', maxJobs: 1, now: 2_000_000 });
    expect(second).toHaveLength(1);
    expect(second[0]!.retries).toBe(2);
  });
});
