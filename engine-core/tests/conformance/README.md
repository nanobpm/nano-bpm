# Differential conformance harness — Nano vs Zeebe deploy verdicts

Epic #850 (deploy-validation parity), final slice #857.

This directory holds a **checked-in corpus** of `.bpmn` fixtures and a harness
(`engine-core/tests/conformance.rs`) that runs each model through Nano's
`parse_bpmn` and asserts its verdict — **ACCEPT**, or **REJECT with a category** —
**matches Zeebe**. It converts deploy-validation parity from per-bug
whack-a-mole into a permanently-guarded invariant: a new parity gap fails CI
(`engine-core (clippy + test)`) instead of shipping silently.

The harness aggregates the fixtures and error categories introduced by every
prior slice of the epic:

| Sub-class | Slices | Guards |
|-----------|--------|--------|
| A | #849, #851 | unresolved `<incoming>/<outgoing>` and other QName references |
| B | #853 | unsupported flow elements / event definitions |
| C | #854, #855, #856 | gateway condition-or-default, start-event counts, end-event outgoing, duplicate typed starts, `zeebe:taskDefinition` attrs |

Because it depends on all of them, it only goes green once every fix is on
`main`. Reverting any one slice's fix flips one of its REJECT fixtures to ACCEPT
and turns the harness **RED** — that is the regression guarantee.

The harness is **hermetic**: it reads the checked-in corpus off disk
(`CARGO_MANIFEST_DIR`), does no network I/O, and runs in the normal
`cargo test -p nanobpmn-engine-core` suite.

## How to add a corpus entry

1. Drop a well-formed (or deliberately ill-formed) `.bpmn` model into
   `corpus/`, named `accept-*.bpmn`, `reject-*.bpmn`, or `diverge-*.bpmn` by its
   verdict.
2. Put a **declarative verdict directive** as a leading XML comment, plus an
   `oracle:` comment recording *why* Zeebe returns that verdict (the Zeebe
   validator / test it came from):

   ```xml
   <!-- verdict: accept -->
   <!-- oracle: <how the Zeebe verdict was established> -->
   ```

   or, for a rejection:

   ```xml
   <!-- verdict: reject | category: InvalidEndEvent -->
   <!-- oracle: EndEventValidator — an end event must have no outgoing flow (#856) -->
   ```

   or, for an **intentional Nano-only divergence** — a model Zeebe *accepts* but
   Nano deliberately *rejects* because it does not implement the feature:

   ```xml
   <!-- verdict: diverge | category: UnsupportedUserTaskFormBinding -->
   <!-- oracle: Zeebe ACCEPTS this model; Nano rejects it (deliberate divergence, #1190) -->
   ```

   The `category` **must** be one of the Nano `ParseError` category keys in the
   appropriate registry below — the Zeebe-parity mapping table for `reject`, the
   Nano-only divergence table for `diverge` (it is the `ParseError` variant
   name). The harness reads the directive, runs `parse_bpmn`, and asserts:
   - it **accepts iff** the verdict is `accept`; and
   - on `reject`, the actual `ParseError` category equals the declared
     `category`, and that category is present in the mapping table; and
   - on `diverge`, Nano still **rejects** (Zeebe would accept), the actual
     category equals the declared `category`, and that category is present in the
     Nano-only divergence table.
3. If the entry is evidence for Zeebe parity coverage matrix cells
   (`formal/README.md#zeebe-parity-coverage-matrix`), list those cell ids,
   separated by whitespace, in a `zeebe-cells:` comment:

   ```xml
   <!-- zeebe-cells: validation:EndEventValidator:end-events-must-not-have-outgoing-sequence-flows-to-other-elements -->
   ```

   `formal/parity/check.mjs` only accepts a `parity` claim for a cell that an
   `accept`/`reject` fixture declares here. It also rejects a declared id that
   is not in `zeebe-surface.json`. This harness ignores the comment.
4. Run `cargo test -p nanobpmn-engine-core --test conformance`. If you added a
   new reject category, the coverage ratchet requires a REJECT entry for it (and
   a `ParseError` variant that maps to a Zeebe class — see below); a new
   divergence category likewise requires a DIVERGE entry.

> **Tip:** to discover the category Nano actually emits for a model, run the
> harness with `-- --nocapture`; a mismatch prints both the expected and actual
> category and the full `ParseError`.

## How the Zeebe verdicts were captured

Nano cannot run Zeebe in-process, so each fixture's verdict is captured **once**
as a declarative expectation committed alongside the model, and regrown as Zeebe
evolves. The verdicts here were established from two oracles:

1. **The prior slices of this epic.** Each slice (#849/#851/#853/#854/#855/#856)
   fixed a concrete Nano-vs-Zeebe divergence with its own red/green fixtures and
   cited the exact Zeebe validator it matched (see the `origin` column below).
   Those scenarios are re-asserted here in one place as the class guard.
2. **Camunda's own validation suite.** Camunda ships an executable oracle of
   `{model -> expected error}` at
   `zeebe/bpmn-model/src/test/java/io/camunda/zeebe/model/bpmn/validation/zeebe/Zeebe*ValidationTest.java`
   (and the sibling `ProcessValidationTest` / `SequenceFlowValidator` /
   `EndEventValidator` sources). To capture or refresh a verdict:
   - find the model shape in the relevant `*ValidationTest`/`*Validator`;
   - a model the test expects to pass → `verdict: accept`; a model it expects to
     fail → `verdict: reject`, with the `category` set to the Nano `ParseError`
     that mirrors the Zeebe validator via the mapping table;
   - record the Zeebe source in the fixture's `oracle:` comment so the capture is
     auditable and reproducible when Zeebe changes.

No Zeebe runtime, JAR, or network is needed at test time — only the committed
directive.

## Nano ↔ Zeebe reject-category mapping table

This table is the **single source of truth** for how a Nano `ParseError`
category maps to Zeebe's rejection class. It lives in code as
`NANO_ZEEBE_MAPPING` in `engine-core/tests/conformance.rs`; this section mirrors
it for readers. The texts need not be identical — only the *class* must
correspond. All Zeebe deploy rejections surface as gRPC `INVALID_ARGUMENT`
(HTTP 400); the parenthetical names the concrete validator.

| Nano `ParseError` category | Zeebe rejection class | Origin |
|----------------------------|-----------------------|--------|
| `MalformedXml` | not well-formed XML / `SAXParseException` | baseline |
| `ProcessWithoutId` | Process must have an id / `bpmnProcessId` | baseline |
| `IncompleteSequenceFlow` | `SequenceFlow` source/target QName unresolved | baseline |
| `NoProcess` | resource contains no executable process | baseline |
| `InvalidProcess` | `ProcessValidator`: no start event / unresolved flow target | baseline + #855 |
| `InvalidBoundaryEvent` | `BoundaryEvent` `attachedToRef` unresolved | baseline |
| `InvalidMessageEvent` | unresolved `messageRef` / missing `correlationKey` | baseline |
| `InvalidLinkedResource` | `zeebe:linkedResource` missing required attribute | baseline |
| `UnresolvedReference` | camunda-xml-model eager QName resolution failure | #849 / #851 |
| `UnsupportedElement` | element type has no Zeebe transformer | #853 |
| `InvalidGateway` | `SequenceFlowValidator`: condition-or-default | #854 |
| `InvalidStartEvents` | `ProcessValidator`: multiple none start events | #855 |
| `InvalidEndEvent` | `EndEventValidator`: end event has outgoing flow | #856 |
| `DuplicateStartEvent` | `ModelUtil.verifyNoDuplicate{Message,Signal}StartEvents` | #856 |
| `InvalidTaskDefinition` | `ZeebeElementValidator.hasNonEmptyAttribute` | #856 |

## Intentional Nano/Zeebe divergences (`verdict: diverge`)

The mapping table above records genuine **parity**: Nano rejects a model **iff**
Zeebe rejects it. A handful of cases are deliberately *not* parity — Nano rejects
a model Zeebe **accepts**, because Nano does not (yet) implement the feature and
refuses to silently degrade it. Tagging such a model `verdict: reject` and giving
it a fabricated Zeebe rejection class would make the oracle assert a falsehood
(that Zeebe rejected a valid model) and blind it to the very divergence it should
record. These carry `verdict: diverge` instead, and live in their own
single-source-of-truth registry, `NANO_ONLY_DIVERGENCES` in
`engine-core/tests/conformance.rs` — with **no** Zeebe rejection class, only the
rationale for the deliberate difference. The two registries partition the full
`ParseError` surface (a category is in exactly one; enforced by
`mapping_covers_every_parse_error_category`), and every divergence category is
exercised by at least one DIVERGE corpus entry
(`every_divergence_category_has_a_diverge_corpus_entry`).

| Nano `ParseError` category | Zeebe behaviour | Nano behaviour | Origin |
|----------------------------|-----------------|----------------|--------|
| `UnsupportedUserTaskFormBinding` | **accepts** `deployment`/`versionTag` user-task form bindings | rejects the deploy (only `latest` implemented) rather than silently degrading | #1190 |
| `UnsupportedExecutionListener` | **accepts** `zeebe:executionListener`s on multi-incoming parallel/inclusive joins, compensation boundary events, surplus signal start events, ad-hoc sub-process tools, and sequence flows | rejects those it cannot enact (a parallel join runs neither the activation body nor the end-listener chain, so both phases are dead; an inclusive join's `start` listener never fires — but its `end` listener IS supported and accepted, since an accepted join runs the end-listener chain before routing; a compensation boundary is a passive marker never entered by token flow; a surplus signal start is demoted to an inert throw event that is never activated; an ad-hoc tool is pruned or activated/completed with direct lifecycle events that bypass the listener gate; a sequence flow is an edge with no lifecycle) rather than silently storing or dropping a dead listener | #1197 |
| `UnsupportedTaskListener` | **accepts** a `zeebe:taskListener` only on a user task (but does not reject a misplaced one) | rejects a task listener on any non-user-task element (e.g. a `receiveTask`, which rides the `io_stack` for its execution listeners): task-listener jobs are created only on the user-task runtime path, so it could never fire — rejected loudly rather than stored dead | #1197 |

### Notes on a few Nano-specific categorisations

Some verdicts Zeebe attributes to a dedicated validator, Nano surfaces from an
equivalent but differently-named check that fires earlier in its pipeline. The
*verdict* (reject) always matches; only the internal category differs, and the
mapping table records the correspondence:

- A **dangling `messageRef`** is rejected by Nano's build-time
  `InvalidMessageEvent` check (before the generic references validator runs), so
  its fixture is tagged `InvalidMessageEvent` — still Zeebe's unresolved-QName
  rejection.
- **"A process must have at least one start event"** is enforced by Nano's
  builder as `InvalidProcess` (the `start_events` validator owns only the
  *multiple none* case, `InvalidStartEvents`).
- An **unsupported element that is a sequence-flow target** would fail Nano's
  builder first as `InvalidProcess` ("unknown target element"); to exercise the
  `UnsupportedElement` validator specifically, the unsupported-element fixture
  keeps the unmodelled element off the flow path (mirroring #853's own tests).
- **Duplicate *signal* start** detection is a *capture-derived* verdict, not a
  built-definition one: a surplus signal start has no dedicated element kind and
  is demoted to an inert throw by #855, so #856 reads pre-demotion `signalRef`
  capture sites. The harness only asserts the verdict + category, so it is
  agnostic to that internal detail.

## Coverage ratchets

The harness enforces two ratchets so parity coverage cannot silently erode:

1. **Reject-category coverage** — every category in the mapping table must have
   at least one REJECT corpus entry, and `nano_category`'s exhaustive match plus
   `mapping_covers_every_parse_error_category` guarantee the two registries
   (Zeebe-parity `NANO_ZEEBE_MAPPING` + Nano-only `NANO_ONLY_DIVERGENCES`)
   partition the full shared `ParseError` enum. Adding a `ParseError` variant
   forces a row in exactly one registry and a corpus entry — a REJECT entry for a
   parity category, a DIVERGE entry
   (`every_divergence_category_has_a_diverge_corpus_entry`) for a divergence.
2. **Element-family coverage** — `element_kind_family` maps every modelled
   `ElementKind` (the canonical registry mirrored by `processos`'
   `ELEMENT_KIND_SPECS`) to a coarse family via an exhaustive, no-wildcard match.
   Every family must be exercised by an ACCEPT corpus entry or be explicitly
   parked in `BASELINE_UNCOVERED_FAMILIES`. Enforcement is at *family*
   granularity: teaching the engine a new element kind that introduces a new,
   uncovered family *flags* here (ties into slice #853's derive-from-registry
   guard) until it is either covered by a fixture or deliberately baselined — a
   new kind that maps into an already-covered family is not separately flagged.
