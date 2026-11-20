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
  whether a 1-minute tick landed before stop). Residual variance in the job
  activation scan is absorbed by `slackPercent` (applied once, at `--check`:
  effective floor = `floorPercent − slackPercent`) and tracked in #1679.
