# Dual-engine parity-corpus coverage (#1247)

Measures how much of **each** engine the parity corpus actually exercises, and
ratchets it so coverage cannot silently drop. Part of #1240 / epic #1224.

| Side | How | Unit |
|---|---|---|
| **nano** | `nano-coverage.sh` — `cargo llvm-cov` over `engine-core` while `engine-core/tests/parity_corpus.rs` replays the corpus natively (the same `scenario.json` files and step vocabulary the wasm runner plays) | LLVM code regions (every branch arm is a region; `branch` is used automatically when the export carries branch data, e.g. a nightly `--branch` run) |
| **zeebe** | `zeebe-coverage.sh` — a live Camunda 8 (`runtime/start-camunda.sh`) with the JaCoCo agent attached to the broker JVM (`JACOCO_DIR`, `io.camunda.zeebe.engine.*`), the corpus played through `--backend both`, dump on graceful stop, `jacococli report --xml` | bytecode branches |

The corpus on both sides is the committed seed corpus
(`formal/parity/runtime/corpus/`) plus the bounded-exhaustive scenarios
(`formal/corpus/exhaustive.mjs --out`, bound `ci` by default).

```bash
formal/parity/coverage/nano-coverage.sh  target/parity-coverage/nano.json      # needs cargo-llvm-cov
formal/parity/coverage/zeebe-coverage.sh target/parity-coverage/zeebe.xml      # needs docker, java, engine-wasm/pkg
node formal/parity/coverage/report.mjs --nano target/parity-coverage/nano.json \
  --zeebe target/parity-coverage/zeebe.xml --out report.json --check
```

## Triage — the tags in `report.json`

- **Uncovered Zeebe branch → `missing-scenario`.** Each is tagged against the
  coverage matrix: the `zeebe-surface.json` cells whose source is that exact
  line (`scope: "line"`), else the cells sourced in the same file
  (`scope: "file"`), else none (`scope: "none"`), each with its `coverage.json`
  status (`parity` / `nano-tested` / `gap` / `out-of-scope` / `unmapped`). Line
  scope is exact only when the Camunda image is built from the pinned sha; the
  image is derived from the pin's *version* (`runtime/camunda-image.mjs`), so
  treat a line tag as a pointer and the file tag as authoritative.
- **Uncovered Nano region → `nano-extension:<id>` or `missing-scenario`.** A
  path matched by `nano-extensions.json` (the registry of Nano-only extensions —
  code with no Camunda behaviour to scenario against) is an extension; anything
  else is a missing scenario. Registering a path is a reviewed claim.

## Ratchet — `baseline.json`

Each side has a `floorPercent`. `report.mjs --check` fails when a measured side
drops below it (or is measured in a different unit). Raise floors after adding
scenarios with `--update` (floors only rise; lowering needs `--allow-lower` and
review). CI:

- **nano** — `parity-coverage (nano llvm-cov)` in `ci.yml` (deterministic: the
  replay is single-threaded and the toolchain is pinned by
  `rust-toolchain.toml`; a toolchain bump may need a re-baseline).
- **zeebe** — `parity-coverage (zeebe JaCoCo)` in `parity-camunda-daily.yml`
  (needs a live broker, so it is not a PR gate). Determinism: the periodic
  message-TTL checker is set to 1 s and the run settles before shutdown, so
  background expiry passes are always measured (they previously depended on
  whether a 1-minute tick landed before stop).

### Deterministic measured set — `zeebe-excludes.json` (#1707)

The zeebe side measures a **live** broker, and some of `zeebe/engine`'s branch
arms are decided by wall-clock timing, not by the corpus: background scheduler
sweeps (message TTL, lock-release, dedup-expiry), the message-correlation race,
and the FEEL `typeCheck` failure branch. On identical inputs (same commit, same
image digest) two runs measured **1037 vs 1033** covered branches — ±4 branches
of noise that flipped the 12.5 % floor (#1707). A 30-run census found exactly
15 branch-carrying lines ever vary, all in the two message packages plus
`ExpressionProcessor.java:507`.

`zeebe-excludes.json` removes that timing-dependent surface from the measured
set — whole packages (`packages`, recursive) and individual lines (`lines`),
each entry naming the issue that justifies it. With those exclusions every one
of the 30 census runs is byte-identical (951/7872 = 12.08 %), so the floor is a
**hard** ratchet again (no `slackPercent` absorption band — that was the
palliative this replaces). An exclusion is a reviewed claim; shrink the list as
#1679 makes those paths deterministic.

### Gate self-test — `--self-test N` (#1707)

Re-running the measurement on identical inputs must give an identical covered
count. `--self-test N` re-parses each input N times and fails loudly with the
differing branches when any re-parse disagrees, so a nondeterministic
measurement fails the gate instead of flapping it. The daily job runs
`--self-test 3 --check`.
