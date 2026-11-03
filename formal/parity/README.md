# Zeebe parity coverage matrix

This directory holds the derived definition of "behaviour Nano must match
Zeebe on" (#1245) and the guard that keeps it honest. `extract.mjs` reads the
Zeebe sources pinned in `zeebe-pin.json` and emits one **cell** per behaviour
into `zeebe-surface.json`; `coverage.json` maps every cell to Nano evidence;
`check.mjs` fails the build when a cell is unmapped, unverifiable, or newly
appeared. See `formal/README.md#zeebe-parity-coverage-matrix` for the full
workflow (mapping cells, the gap ratchet, bumping the pin). This file is the
reference for the **cell-id grammar**.

## Cell-id grammar

Every cell id is `<family>:<key>`, where `<family>` is one of the families
`extract.mjs` produces (`element`, `event`, `lifecycle`, `guard`, `incident`,
`intent`, `validation`, `failure`, `rejection`). Most families key a cell by a
behaviour name; two families key a cell by the **Zeebe source site** that
produces the behaviour.

### Per-site cells: `failure:` and `rejection:`

These two families are a **shared contract** with the Zeebe trace harness in
`formal/traces/` (#1391): the harness captures a running Zeebe, attributes each
raised failure/rejection to the source site that constructed it, and emits these
same ids. Do not deviate from this grammar — a drift between the static
extractor and the captured traces silently breaks the join.

- **Failure cells** — `failure:<Class>.<method>:<ErrorType>`, one per
  `new Failure(...)` construction site under `zeebe/engine/.../processing`:
  - `<Class>` — the simple (unqualified) name of the enclosing **top-level**
    Java class of the construction site.
  - `<method>` — the enclosing **named method** name. A construction inside a
    lambda is attributed to the enclosing named method (a lambda's own
    parameter list is not a method). A construction in a field initializer or
    `static {}` block has no enclosing method, so `<method>` is empty.
  - `<ErrorType>` — the Zeebe `ErrorType` enum constant the constructor is
    given. A message-only `Failure(message)` (a deploy-time rejection, which
    carries no type) yields an empty `<ErrorType>`, so the id ends in `:`; a
    site that re-wraps another `Failure` and forwards its type unchanged
    (`new Failure(f.getMessage(), f.getErrorType(), …)`) is marked `forwarded`.
  - Example: `failure:BpmnJobBehavior.evalTypeExp:EXTRACT_VALUE_ERROR`.

- **Rejection cells** — `rejection:<Class>.<method>:<RejectionType>`, one per
  command-rejection site under `zeebe/engine/.../processing`:
  - `<Class>` / `<method>` — the same scope rules as `failure:`.
  - `<RejectionType>` — the Zeebe `RejectionType` enum constant (read from the
    SBE schema, `protocol.xml`). Comparisons against a type (`==`, `!=`,
    `.equals`) are reads, not constructions, and yield no cell; nor do SBE's
    `NULL_VAL` / `SBE_UNKNOWN` sentinels.
  - Example: `rejection:AdHocSubProcessInstructionActivateProcessor.processRecord:NOT_FOUND`.

Because one site is one behaviour, a **wildcard coverage rule can never claim a
`failure:` or `rejection:` cell** — `check.mjs` rejects any such cell whose
first matching rule contains a `*`. Each site needs an exact, wildcard-free
rule. This is what stops one broad rule (or one test) from silently absorbing
dozens of distinct raising sites — the drift the per-site cells exist to
expose. Uncovered sites go into the ratcheted `gaps.json` baseline, so CI stays
green on the current pin, but any **new** site a Zeebe pin bump adds fails the
drift guard until it is mapped.

### Coarser families

The remaining families key a cell by behaviour, not by source site, and may be
claimed by wildcard rules: `element:<BpmnClass>`, `event:<position>:<kind>`,
`lifecycle:<BpmnElementType>:<command>`, `guard:<method>:<message>`,
`incident:<ErrorType>`, `intent:<Record>:<INTENT>`,
`validation:<Validator>:<message>`. `incident:<ErrorType>` is the coarse
counterpart of the per-site `failure:` cells: it records that an error type
exists, while `failure:` records every site that raises it.

## Files

| File | Role |
|---|---|
| `zeebe-pin.json` | the pinned Zeebe repository/ref/sha and the sparse paths extracted from |
| `fetch-zeebe.sh` | fetches the pinned sources into a local cache and prints the checkout dir |
| `extract.mjs` | derives `zeebe-surface.json` from the pinned sources (fail-loud anchors) |
| `zeebe-surface.json` | **derived, committed** — every cell + its source lines; never hand-edit |
| `coverage.json` | ordered rules mapping each cell to evidence; `reviewedAt` pins the reviewed sha |
| `gaps.json` | **derived, committed** ratchet baseline of `gap` cells; may only shrink |
| `nano-tested.json` | **derived, committed** ratchet baseline of `nano-tested` cells; may only shrink |
| `check.mjs` | the guard: mapping, evidence, ratchets, per-site wildcard ban, parity-requires-trace |
| `*.test.mjs` | extractor and guard tests (`node --test formal/parity/*.test.mjs`) |

Regenerate the derived artifacts after changing the extractor or bumping the
pin:

```bash
node formal/parity/extract.mjs "$(formal/parity/fetch-zeebe.sh)"   # zeebe-surface.json
node formal/parity/check.mjs --update-gaps                          # shrink gaps.json
node formal/parity/check.mjs --update-ratchet                       # shrink nano-tested.json
node formal/parity/check.mjs                                        # guard + report
```

## `parity` requires a captured Zeebe trace (#1391)

A cell may carry the `parity` status **only while a captured Zeebe trace
touches it**. The trace harness (`formal/traces/`) runs each scenario on the
pinned Zeebe and records the cells it touched in
`formal/traces/goldens/<scenario>.cells.json`; `check.mjs` unions those files
and rejects any `parity`-claimed cell absent from the union. Two evidence
forms count:

- **`traces:<scenario>`** — the scenario's own cells file. This is the primary
  form for runtime behaviour (element lifecycle, intents, incidents,
  per-site `failure:`/`rejection:` cells).
- a **conformance-corpus fixture** with a captured Zeebe verdict
  (accept/reject) whose declared `zeebe-cells` are each also touched by some
  trace — the deploy-validation cells.

Because the capture harness (Java) and `extract.mjs` (JS) implement one shared
cell-id grammar, `check.mjs` also rejects any id in a cells file that the
extractor does not emit for the pinned Zeebe — grammar drift fails the build
instead of silently breaking the join. The goldens must have been captured at
the pinned revision (`goldens/PIN` == `zeebe-pin.json`'s sha); a pin bump
fails the guard until `formal/traces/capture.sh` refreshes them.

## `nano-tested` is transitional and ratcheted (#1391)

`nano-tested` means "a nano test asserts this, but no captured Zeebe trace
covers it yet" — a **transitional** status on the way to `parity`.
`nano-tested.json` commits the exact set of `nano-tested` cells:

- `check.mjs` fails when the assigned set and the baseline disagree (a new
  `nano-tested` cell, or one promoted/removed without regenerating).
- CI (`--base-nano-tested`) fails when the set **grows** against the target
  branch — same pin. On a pin bump, brand-new cells may enter as
  `nano-tested` while their traces are captured.
- Promote a cell to `parity` (with `traces:<scenario>` evidence) as soon as a
  captured trace covers it, then run `check.mjs --update-ratchet` to shrink
  the baseline.

## The gap ratchet is pin-aware and permits only refinements

CI runs `check.mjs --base-gaps <target-branch gaps.json> --base-pin
<target-branch zeebe-pin.json>` so the `gaps.json` baseline may only **shrink**.
The ratchet exists to stop a Zeebe **pin bump** hiding newly-uncovered cells, so
it keys on the pin:

- **Pin bump** (the base pin's `sha` differs from the head's, or `--base-pin` is
  absent — fail-closed): any head gap id the base does not list is rejected.
- **Same pin** — a head gap id absent from the base is allowed **only** as a
  *refinement*: a per-site cell whose coarse parent (per `coarseCellId` in
  `extract.mjs`, the single source of truth for the grammar) was a base gap the
  head no longer lists. So `rejection:<Class>.<method>:<CODE>` may appear only
  when `rejection:<Class>:<CODE>` was a base gap and is gone from head. Any
  other same-pin growth still fails.
- **`failure:` seed** — a `failure:` cell's coarse parent is `incident:<TYPE>`,
  a *covered* cell rather than a base gap, so the refinement rule can never
  admit the first `failure:` cells. They may appear only when the base baseline
  contains **no** `failure:` cells at all (the one-time introduction of the
  family); once any `failure:` cell is in the base, the family is shrink-only
  like everything else.

