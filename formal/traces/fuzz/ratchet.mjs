#!/usr/bin/env node
// The fixed-seed budget's allowlist ratchet (#1521).
//
// A generated budget case (g-<seed>) may diverge from Zeebe only through an
// allowlist.json entry citing an issue (enforced by the nano runner, which also
// fails a STALE entry — so a fix forces the entry out). This adds the growth
// half, like formal/parity/gaps.json: comparing with the target branch, a NEW
// allowlist entry for a g-* case is accepted only when its seed is new to the
// budget too. A seed that already passed on the target branch and now needs an
// entry is a regression being masked — the re-introduced #1518 shape — and fails.
//
//   node formal/traces/fuzz/ratchet.mjs --base-allowlist <file> --base-budget <file>
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

import { GENERATED_PREFIX, TRACES_DIR, loadBudget, scenarioName } from './generate.mjs'

const read = (f) => JSON.parse(fs.readFileSync(f, 'utf8'))

/** Generated-case allowlist entries (scenario names). */
export function generatedEntries (allowlist) {
  return new Set((allowlist?.entries ?? []).map((e) => e.scenario).filter((s) => s.startsWith(GENERATED_PREFIX)))
}

/** Violations of the ratchet: head entries masking a seed the base budget already ran clean. */
export function ratchetViolations ({ baseAllowlist, baseBudget, headAllowlist }) {
  const baseEntries = generatedEntries(baseAllowlist)
  const baseSeeds = new Set((baseBudget?.seeds ?? []).map(scenarioName))
  return [...generatedEntries(headAllowlist)]
    .filter((s) => !baseEntries.has(s) && baseSeeds.has(s))
    .map((s) => `${s} passed on the target branch but is newly allow-listed: a regression is being masked — fix nano (red/green) instead`)
}

function main (argv) {
  const arg = (k) => {
    const i = argv.indexOf(k)
    return i >= 0 ? argv[i + 1] : null
  }
  const baseAllowlist = arg('--base-allowlist')
  const baseBudget = arg('--base-budget')
  if (!baseAllowlist || !baseBudget) throw new Error('usage: ratchet.mjs --base-allowlist <file> --base-budget <file>')
  const readOrNull = (f) => {
    try { return read(f) } catch { return null }
  }
  const errors = ratchetViolations({
    baseAllowlist: readOrNull(baseAllowlist),
    baseBudget: readOrNull(baseBudget),
    headAllowlist: read(path.join(TRACES_DIR, 'allowlist.json'))
  })
  loadBudget() // the head budget must parse
  for (const e of errors) console.error(`ratchet: ${e}`)
  if (errors.length) process.exit(1)
  console.log('ratchet: generated-budget allowlist did not grow over passing seeds')
}

if (process.argv[1] && fileURLToPath(import.meta.url) === path.resolve(process.argv[1])) {
  main(process.argv.slice(2))
}
