#!/usr/bin/env node
// CLI entry point for the two-backend parity runner (#1260).
//
//   node formal/parity/runtime/run.mjs [--backend nano|camunda|both] [--corpus DIR]
//
// Behaviour:
//   * `nano`   — run every scenario against the nano `TestEngine` and check each
//     against its `expect` block (the always-on reference oracle). No runtime
//     needed; runs everywhere.
//   * `camunda`— run every scenario against a live Camunda 8 (v2 REST). Requires
//     `CAMUNDA_REST_ADDRESS`; SKIP-TOLERANT — if it is unset or the endpoint is
//     unreachable, the runner reports SKIPPED and exits 0 (the CI job stays
//     green for every sibling PR).
//   * `both`   — run each scenario against BOTH and assert the differential
//     oracle finds no difference on the fields both backends provide. Also
//     skip-tolerant on the Camunda side.
//
// Exit non-zero on any expectation mismatch or cross-backend difference. There
// are NO retries: a nondeterministic scenario is a driver defect to root-cause.

import { readdirSync, statSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { join } from "node:path";
import { NanoBackend } from "./nano-backend.mjs";
import { CamundaBackend } from "./camunda-backend.mjs";
import { loadScenario, runScenario } from "./driver.mjs";
import { checkExpectations, diffObservations } from "./observation.mjs";

function parseArgs(argv) {
  const args = { backend: "nano", corpus: null };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--backend") args.backend = argv[++i];
    else if (a === "--corpus") args.corpus = argv[++i];
    else if (a.startsWith("--backend=")) args.backend = a.slice("--backend=".length);
    else if (a.startsWith("--corpus=")) args.corpus = a.slice("--corpus=".length);
    else throw new Error(`unknown argument: ${a}`);
  }
  if (!["nano", "camunda", "both"].includes(args.backend)) {
    throw new Error(`--backend must be nano|camunda|both, got ${args.backend}`);
  }
  return args;
}

export function discoverScenarios(corpusDir) {
  return readdirSync(corpusDir)
    .map((name) => join(corpusDir, name))
    .filter((p) => {
      try {
        return statSync(join(p, "scenario.json")).isFile();
      } catch {
        return false;
      }
    })
    .sort()
    .map(loadScenario);
}

function camundaFromEnv() {
  const address = process.env.CAMUNDA_REST_ADDRESS;
  if (!address) return null;
  return new CamundaBackend({
    address,
    token: process.env.CAMUNDA_AUTH_TOKEN,
    basicAuth: process.env.CAMUNDA_BASIC_AUTH,
  });
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const corpusDir =
    args.corpus ?? fileURLToPath(new URL("./corpus", import.meta.url));
  const scenarios = discoverScenarios(corpusDir);
  if (scenarios.length === 0) {
    console.error(`no scenarios found under ${corpusDir}`);
    process.exit(1);
  }

  const wantNano = args.backend === "nano" || args.backend === "both";
  const wantCamunda = args.backend === "camunda" || args.backend === "both";

  const nano = wantNano ? await new NanoBackend().init() : null;

  let camunda = null;
  if (wantCamunda) {
    camunda = camundaFromEnv();
    if (!camunda) {
      console.log(
        "::notice::CAMUNDA_REST_ADDRESS unset — SKIPPING the Camunda 8 backend " +
          "(skip-tolerant). Provision a live Camunda 8 and set it to run the " +
          "differential.",
      );
      if (args.backend === "camunda") {
        console.log("SKIPPED: no Camunda runtime.");
        process.exit(0);
      }
    } else if (!(await camunda.ping())) {
      console.log(
        `::notice::Camunda REST at ${process.env.CAMUNDA_REST_ADDRESS} unreachable — ` +
          "SKIPPING the Camunda 8 backend (skip-tolerant).",
      );
      camunda = null;
      if (args.backend === "camunda") {
        console.log("SKIPPED: Camunda runtime unreachable.");
        process.exit(0);
      }
    }
  }

  let failures = 0;
  try {
    for (const scenario of scenarios) {
      const results = {};
      const provides = {};

      if (nano) {
        const obs = await runScenario(nano, scenario);
        results.nano = obs;
        provides.nano = nano.provides;
        const check = checkExpectations(obs, scenario.expect);
        if (!check.ok) {
          failures++;
          console.error(`FAIL ${scenario.name} [nano expect]`);
          for (const m of check.mismatches) {
            console.error(`  ${JSON.stringify(m)}`);
          }
        } else {
          console.log(`ok   ${scenario.name} [nano]`);
        }
      }

      if (camunda) {
        const obs = await runScenario(camunda, scenario);
        results.camunda = obs;
        provides.camunda = camunda.provides;
        console.log(`ok   ${scenario.name} [camunda]`);
      }

      if (results.nano && results.camunda) {
        const diff = diffObservations(
          results.nano,
          results.camunda,
          provides.nano,
          provides.camunda,
        );
        if (!diff.ok) {
          failures++;
          console.error(
            `FAIL ${scenario.name} [differential over ${diff.comparedFields.join(", ")}]`,
          );
          for (const m of diff.mismatches) {
            console.error(
              `  ${m.field}: nano=${JSON.stringify(m.a)} camunda=${JSON.stringify(m.b)}`,
            );
          }
        } else {
          console.log(
            `ok   ${scenario.name} [differential: ${diff.comparedFields.join(", ")}]`,
          );
        }
      }
    }
  } finally {
    if (nano) await nano.close();
    if (camunda) await camunda.close();
  }

  if (failures > 0) {
    console.error(`\n${failures} failure(s).`);
    process.exit(1);
  }
  console.log(`\nAll ${scenarios.length} scenario(s) passed.`);
}

// Only run when invoked as the CLI entry point, not when imported (e.g. by the
// test suite, which reuses `discoverScenarios`).
if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
