# feel-scala reference for the FEEL differential fuzz

Part of #1391 (step 5). The FEEL differential fuzz (`formal/lean/feel-diff.sh`)
generates a deterministic corpus from the Lean reference semantics and proves
nano's Rust evaluator agrees with it on every row. This directory adds a second
reference: **feel-scala**, the FEEL engine Zeebe embeds, at the exact version
the pinned Zeebe uses.

| File | Role |
| --- | --- |
| `fetch-feel-scala.sh` | Derives the version from `<version.feel-scala>` in `parent/pom.xml` at the commit pinned by `formal/parity/zeebe-pin.json` (no duplicated version), downloads `feel-engine-<v>-complete.jar` from Maven Central into `~/.cache/nanobpm-feel-scala` and checks its SHA-1. |
| `FeelScalaEval.java` | A single-file Java program (no build tool) that evaluates each corpus row and prints the canonical outcome (`err` / `ok:null` / `ok:bool:…` / `ok:num:…` / `ok:str:…`). An error versus a `null` result is decided the same way Zeebe's `FeelExpressionLanguage` decides it: `EvaluationResult.isFailure()` is `err`, and a `null` result with suppressed failures is `ok:null`. |
| `compare.mjs` | The gate. It compares the corpus reference column (which equals nano's outcome, because `feel_diff` already passed) with feel-scala's outcome. |
| `known-divergences.json` | Every accepted divergence, one entry per `(expr, context)` row, each citing an issue. |
| `check.sh` | fetch → evaluate → compare. `feel-diff.sh` calls it after `feel_diff` passes. |

The gate fails on any of these:
- a divergence that is not listed;
- a listed divergence whose outcomes changed;
- a **stale** entry: the row now agrees, or it is no longer in the corpus;
- an entry whose issue reference is missing or malformed.

The entries are tied to the default corpus (3000 cases, default seed). Running
with a different `FEEL_FUZZ_CASES` makes entries go stale on purpose.

## Current known divergences

| Issue | Class |
| --- | --- |
| #1392 | Type errors (`1 + true`, `-false`, `not(5)`): nano gives `err`, feel-scala gives `null` (suppressed failure). |
| #1393 | A non-boolean `if` condition: nano gives `null`, feel-scala takes the `else` branch. |
| #1394 | `=` / `!=` across mismatched types: nano gives a boolean, feel-scala gives `null`. |

When you fix one of these in nano and in the Lean reference, its entries go
stale and the gate tells you to remove them.

## Workflow

```bash
formal/lean/feel-diff.sh                      # whole pipeline, as the CI job runs it
node --test formal/feel-scala/*.test.mjs      # gate tests (incl. the seeded-divergence failure)
node formal/feel-scala/compare.mjs --stubs corpus.tsv outcomes.txt  # divergences NOT already listed, as JSON stubs to append
```

A Zeebe pin bump can change the feel-scala version. The CI cache key is the hash
of `zeebe-pin.json`, so the new jar is fetched and any new or vanished
divergences show up as gate failures you can review.
