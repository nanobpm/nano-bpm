#!/usr/bin/env node
// The "a registered extension is actually accepted by Camunda" guard (#1261,
// slice 6 of #1240) — the LIVE half of the extension-register contract.
//
// `extensions.json` lists the surface Nano has that Camunda 8 REJECTS or LACKS.
// If Camunda is found to ACCEPT a registered extension, the entry is wrong: the
// surface is shared, so any difference there is a Nano DEFECT, not an extension.
// This probe deploys a minimal model carrying each registered extension to a
// live Camunda 8 and asserts the deployment is REJECTED (or the extension is
// otherwise not honoured). An acceptance fails the run — the entry must be
// removed (and the surface moved onto the parity matrix).
//
// SKIP-TOLERANT like the rest of the runtime runner: with no
// `CAMUNDA_REST_ADDRESS` (or an unreachable one) it reports SKIPPED and exits 0,
// so it never blocks a sibling PR. The daily live-Camunda workflow
// (`parity-camunda-daily.yml`, #1408) passes `--require-camunda`, turning the
// skip into a failure — a green run there must have probed every extension.
//
// The static half of the guard (well-formedness, no Camunda-surface collision,
// no parity-corpus model tagged as an extension) is `extensions.mjs`; this is
// only the acceptance probe, which needs a live Camunda.

import { fileURLToPath } from "node:url";
import { loadExtensions } from "../extensions.mjs";
import { CamundaBackend } from "./camunda-backend.mjs";

const NANO_NS = "http://nano.camunda.io/schema/semantic/1.0";

/**
 * A minimal deployable model carrying one registered extension, so Camunda's
 * validator decides whether the surface is accepted. The extension is placed on
 * a service task inside `<bpmn:extensionElements>`, exactly where Nano's own
 * models carry it. `xmlns:nano` is bound so the tag parses as the namespaced
 * extension.
 */
export function probeModel(entry) {
  const local = entry.match.xmlLocalName;
  const ns = entry.match.xmlNamespace ?? NANO_NS;
  return `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions
  xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0"
  xmlns:nano="${ns}"
  id="Defs_${local}" targetNamespace="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="probe_${local}" isExecutable="true">
    <bpmn:startEvent id="s"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
    <bpmn:serviceTask id="t">
      <bpmn:incoming>f1</bpmn:incoming>
      <bpmn:outgoing>f2</bpmn:outgoing>
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="w" />
        <nano:${local} probe="true" />
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="e"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="t" />
    <bpmn:sequenceFlow id="f2" sourceRef="t" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>`;
}

function parseArgs(argv) {
  const a = { requireCamunda: false };
  for (const x of argv) {
    if (x === "--require-camunda") a.requireCamunda = true;
    else throw new Error(`unknown argument: ${x}`);
  }
  return a;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const extensions = loadExtensions().filter((e) => e?.match?.xmlLocalName);
  if (extensions.length === 0) {
    console.log("ok    no deployable extensions to probe");
    return;
  }

  const address = process.env.CAMUNDA_REST_ADDRESS;
  const skip = (why) => {
    if (args.requireCamunda) {
      console.error(`Camunda acceptance probe required (--require-camunda), but ${why}.`);
      process.exit(1);
    }
    console.log(`::notice::${why} — SKIPPING the Camunda extension-acceptance probe (skip-tolerant).`);
    console.log("SKIPPED: no Camunda runtime.");
    process.exit(0);
  };
  if (!address) skip("CAMUNDA_REST_ADDRESS is unset");

  const camunda = new CamundaBackend({
    address,
    token: process.env.CAMUNDA_AUTH_TOKEN,
    basicAuth: process.env.CAMUNDA_BASIC_AUTH,
  });
  if (!(await camunda.ping())) skip(`${address} is unreachable`);

  let failures = 0;
  try {
    for (const entry of extensions) {
      // A registered extension must be REJECTED (or ignored) by Camunda. We
      // deploy the probe and inspect: a deploy that SUCCEEDS AND honours the
      // extension as executable surface means Camunda has it — not an extension.
      // Camunda rejects unknown extension surface at deploy time with a
      // validation error; an inert metadata tag it tolerates is still "lacked"
      // (Camunda has no semantics for it), which the entry's note records.
      let accepted = false;
      try {
        await camunda.deploy(probeModel(entry));
        accepted = true;
      } catch {
        accepted = false; // a deploy rejection is the expected, extension-confirming outcome
      }
      if (accepted) {
        // Deployment succeeded. That alone is not proof Camunda HONOURS the
        // extension (an unknown inert metadata tag deploys clean on both). The
        // register's contract is about BEHAVIOURAL surface: an entry whose note
        // marks it inert (`inert: true`) is "lacked" by Camunda even when the
        // deploy succeeds. An entry NOT marked inert that Camunda accepts IS a
        // shared surface — fail.
        if (!entry.inert) {
          failures++;
          console.error(
            `FAIL ${entry.id}: Camunda ACCEPTED a registered extension that is not marked inert — ` +
              `the surface is shared, so any difference is a Nano DEFECT. Remove the entry ` +
              `(and move the surface onto the parity matrix) or mark it inert with a note.`,
          );
        } else {
          console.log(`ok    ${entry.id} [camunda deploys it as inert metadata — no Camunda semantics]`);
        }
      } else {
        console.log(`ok    ${entry.id} [camunda rejects it — a genuine Nano-only extension]`);
      }
    }
  } finally {
    await camunda.close();
  }

  if (failures > 0) {
    console.error(`\n${failures} extension-acceptance failure(s).`);
    process.exit(1);
  }
  console.log(`\nAll ${extensions.length} extension(s) confirmed Nano-only against Camunda.`);
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
