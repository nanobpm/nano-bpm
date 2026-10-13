# ProcessOS — The Domain-Signal Plane (process + content)

> Status: analysis / direction only. No code changes are made by this document.
> Sixth in the ProcessOS design series, after `process-optimization-design.md`
> (substrate: Tier-A trace, Tier-B replay, cost/value, transform space),
> `processos-design.md` (where the optimizer lives + its contracts),
> `processos-latent-process-exploration.md` (pattern taxonomy + the data each
> pattern needs), `processos-deployment-cooptimization.md` (resourcing layers),
> and `processos-worker-and-semantic-layers.md` (worker internals + the
> business-semantic layer).
>
> Grounded in: the canonical trace (`process-optimization-design.md` §3), the
> `__cost` structured completion channel + variable lineage (§6), the reasoning
> interface's report→typed-transform→verifier discipline (§8), the pattern
> catalogue and its "data each pattern needs" table
> (`processos-latent-process-exploration.md` §1–§2), the **inverted safety model**
> for outcome-affecting change (`processos-worker-and-semantic-layers.md` §2.2),
> the read/ingest contracts and `DatasetSource` (`processos-design.md` §1–§2), and
> the app-tier / mechanism-not-policy posture of ADR 0056 (agentic layer) and ADR
> 0051 (nano-workforce datasource row registry).

## 0. The gap: the numbers optimize a proxy, the content is where the domain waste hides

The series covers the full **technical** stack — process structure, task binding,
worker internals, resources — and, at the very top, the **business-semantic**
layer (doc 5 §2), whose inputs it sources from *outside* execution: BPMN docs,
policy RAG, downstream business systems, SME feedback (§2.4).

There is a plane **between** those two that no document gives apps a mechanism to
feed: the **application-domain content emitted _during_ execution**. For an
agentic or data-rich Urban app this content is first-class and abundant — the
coordination the agents wrote to each other, the rows the workers persisted, the
work products they produced — and it carries **domain inefficiency signal that the
timing/resource trace cannot see**. The Tier-A trace faithfully records that
`convergence-loop` ran four rounds and escalated; it cannot record that rounds 2–4
each re-discovered the *same* constraint, because that "why" was written to an
advisory store the engine deliberately never reads.

> **The load-bearing claim.** Process optimization is not only timing and
> resource. A second, equally large class of win is **domain inefficiency
> identified from execution data in the application domain** — rework loops,
> re-derived knowledge, contention/false-sharing between parallel actors,
> recurring defect classes, wasted approvals. These are invisible to a
> `serviceMs`/`__cost` trace and are exactly what the **content** carries.

This document proposes the **domain-signal plane**: a generic ProcessOS surface
that lets any Urban app project its application-domain execution data into the
trace — join-keyed to instances/elements, typed, namespaced — so the reasoner can
assess **outcome- and quality-affecting** patterns (catalogue classes B and G)
with the same report→transform→evidence discipline it uses for classes A/E, held
to the semantic layer's inverted safety model. nano-workforce is the first target.

## 1. Genericity: a fully-generic mechanism + a thin, declared, per-domain adapter

The recurring question — *"a content projector must be tied to an application
domain, surely?"* — resolves the same way `__cost` (§6) already resolved it for
cost: **the mechanism is fully generic; only a thin adapter is domain-specific,
and the domain-specificity is quarantined behind a declared seam that emits into a
generic schema.** A worker reports its own cost into a reserved, domain-agnostic
`__cost` slot; the trace projection, the objective function, and the reasoner
never learn what the cost *was for*. The domain-signal plane is that pattern,
generalized from one scalar to arbitrary typed, namespaced content.

### 1.1 The genericity spectrum — the mechanism supports all three

| Tier | What the app supplies | Domain knowledge lives in… | Signal quality | Adapter cost |
|---|---|---|---|---|
| **G1 — convention** | nothing (opt-in flag) | — | presence / volume / timing of domain rows, opaque | zero |
| **G2 — schema-tagged** | column/table **role annotations** (`actor`, `kind`, `rework-event`, `scope`, `outcome`) | **declarative metadata** | typed, correlatable, pattern-classifiable | tiny (a manifest block) |
| **G3 — custom projector** | a projector **function** for derived/aggregate signals | code (thin) | anything (e.g. per-wave collision count) | small, per derived signal |

- **G1** attaches any DataLayer table carrying an instance/business-key column as
  opaque `domainSignals`, keyed to the instance. Universal, zero-authoring, and
  already enough for the reasoner to see *"N rows of kind X landed against the
  slow element."*
- **G2** is the sweet spot: the app annotates its own schema with **signal
  roles** (§2.2). A single generic projector reads the annotations — no per-app
  code — and the reasoner receives typed, role-tagged signals it can correlate to
  outcomes. Domain knowledge is **declared, not compiled**.
- **G3** is only for *derived* signals a query can't express as a row projection
  (aggregations, cross-row joins). It is the escape hatch, not the default.

The domain tie is therefore **real but minimized, declared, and graceful**: an app
with no adapter still yields G1; most apps stop at G2; G3 is rare.

### 1.2 The deeper move: map domain signals onto the *generic* pattern vocabulary

Keeping the *mechanism* generic is not enough — the **reasoner** must stay
domain-agnostic too, or every app forks the optimizer. The trick: the adapter's
job is not to name a domain fix; it is to **classify its signals into the generic
pattern primitives the catalogue (doc 3 §1) already enumerates.** Then the generic
reasoner does the rest.

| Domain observation (nano-workforce) | Generic catalogue pattern (doc 3 §1) | Class |
|---|---|---|
| a `learning` re-derived by every wave | **redundant-recompute / memoize a repeated pure result** | B (data/computation) |
| `file-claim` collisions between wave siblings | **contention / false-sharing between parallel scopes** | E-adjacent (scheduling/partitioning) |
| a recurring review-nit class across rounds | **defect-class shift-left** (move a check earlier / to a gate) | B/G |
| escalations clustering on one root cause | **guard upstream of a repeated failure** | D (reliability/tail) |

The reasoner sees *"a class-B redundant-recompute across sibling scopes, evidenced
by these signals, correlated with +2 rounds of latency"* — never *"a blackboard
learning."* This is the §0 "the moat is not the LLM" thesis applied to content:
the domain adapter grounds a **generic** optimizer in **domain** execution data.

## 2. The domain-signal schema — an additive extension of the Tier-A trace (§3)

The Tier-A `InstanceTrace` (`process-optimization-design.md` §3,
`processos/src/contracts.rs::InstanceTrace`) grows one **optional, namespaced**
slot. Nothing existing changes; absent-safe by construction.

```jsonc
{
  "instanceKey": "...", "processId": "convergence-loop", "version": 3,
  "businessId": "PR-482",                 // human key; join is on the ambient __instance stamp (§3.1)
  "outcome": "completed",
  "elements": [ /* … unchanged, incl. job.cost, varsRead/Written … */ ],

  // NEW — additive. Namespaced, typed, role-tagged content projected by the app.
  "domainSignals": [
    {
      "ns": "nano-workforce/blackboard",   // app-declared namespace
      "kind": "learning",                  // app value, mapped to a role (§2.2)
      "role": "knowledge",                 // GENERIC role the reasoner keys on
      "scope": { "wave": 2, "actor": "senior:feature#red" },
      "elementId": "ReviewRound",          // best-effort element anchor (may be null)
      "at": 1719000003000,
      "body": "state.rs now owns retry budget; siblings must not set it",
      "dedupeKey": "…"                     // idempotent, mirrors blackboard.ts
    }
  ],

  // NEW — the delayed correctness/value signal (doc 3 §2 "outcome-truth").
  "outcomeTruth": {
    "status": "merged",                    // merged | escalated | abandoned
    "roundsToConverge": 4,
    "reworkEvents": 3,
    "value": null                          // optional scalar for the §6 objective
  }
}
```

Two generic dimensions carry the weight and keep the reasoner domain-free:

- **`role`** — a small **closed vocabulary** the mechanism owns (`knowledge`,
  `claim`, `constraint`, `defect`, `handoff`, `decision`, `note`). The app's own
  `kind` maps onto a role via its annotations (§2.2). Patterns are expressed over
  `role`, not over app `kind`s.
- **`scope`** — a generic `{ instance, element?, actor?, partition? }` bag. It is
  what lets the reasoner say *"repeated across sibling `actor`s within one
  `instance`"* (the redundant-recompute/contention shape) without any domain word.

## 3. Capture — two ingest paths, both app-tier, mechanism-not-policy

Exactly the two shapes ProcessOS ingest already knows (`processos-design.md` §2:
`live` NanoClient / `dataset` `DatasetSource`), plus the `__cost` push precedent.

1. **Push (`__signal.<ns>`).** Reserve a namespaced structured completion channel,
   the sibling of `__cost` (§6): a worker MAY emit typed domain facts on
   `CompleteJob { variables }`. The Tier-A projection folds them into
   `domainSignals` on that element. Best for signals a worker *produces*.
2. **Pull (content projector).** An app registers — in its manifest — a **projector
   binding**: `{ table, keyColumn → businessId, roleMap, elementAnchor? }` (G2) or
   a projector function (G3). At ingest (live) or attach time (dataset), the
   generic projector resolves each instance's `businessId`, queries the bound
   rows, and folds them into `domainSignals`. Best for content the app
   *persisted* (the nano-workforce record-gateway rows, the blackboard).

Both are **app-tier**. The engine and `engine-core` learn nothing; the projector
lives in the host/ProcessOS ingest, hanging off the same exporter batches the
Tier-A `TraceStore` consumes (mirrors the §3 projection-gap framing in
`processos-design.md`). This preserves the series invariant: *zero hot-path cost
when disabled; absent-safe; `engine-core` unchanged*.

### 3.1 The urban lift — domain observability for free, one small mechanism

The integration question — *"do we modify the app, or urban?"* — has a decisive
answer: **lift the mechanism and the substrate bindings to urban; the app supplies
only its irreducible domain semantics, and even those are optional.** This mirrors
how urban already gives every app *pages and workers* for free, and how epic #124
gives every agentic app *networks + visibility* for free. The domain-signal plane
adds **domain observability** to that list.

The lift rests on one observation about what already exists in urban:

- **`EngineJob` already carries `processInstanceKey` + `elementId`**
  (`@nanobpm/urban` `runtime/core/host.ts`), so every worker already *holds the
  join key* at the moment it writes.
- **Every app write funnels through one generic choke point** —
  `DataLayer.table(name, pk).insert(row)` (`runtime/core/modules/datasource.ts`).
  The blackboard and the record tables all go through it.
- **Urban already derives the schema registry** the projector needs — the **fused
  domain model** (ADR 0040): every app's tables + FKs, introspected, no app input.

**The single enabling urban change: ambient instance/element stamping.** Thread the
active job's context into the DataLayer (an ambient `data.forJob(job)` scope, or
`AsyncLocalStorage` — a Node builtin) so `insert()` auto-stamps two
**domain-agnostic** columns, `__instance` and `__element`, on every row written
during a job. It is purely mechanical and knows nothing about any domain. With it:

| Capability | Where it now lives | App authoring |
|---|---|---|
| Ambient join key (`row.__instance` ↔ `instance.instanceKey`) | **urban** (stamping) | **none** |
| G1 convention projection of any DataLayer table | **urban** | **none** |
| Blackboard signals (`knowledge`/`claim`/`constraint`) | **urban** (via #124 **S7**), binding shipped once | **none** |
| Transcript signals | **urban** (via #124 **S6**) | **none** |
| Generic outcome-truth = instance terminal status (completed/terminated/incident) | **urban** / engine | **none** |
| **Role semantics of bespoke app tables** (`persist-escalation` → `defect`) | **the app** manifest (G2) | thin, **optional** → degrades to G1 |
| **Rich outcome-truth** (rounds-to-converge, merged/escalated) | **the app** (G3 hook) | thin, **optional** → degrades to instance status |

**What can and cannot be lifted, stated plainly:** the *mechanism* (schema slot,
stamping, join, projection fold, report, transforms) and the *substrate bindings*
(blackboard, transcripts, DataLayer convention) are **fully generic and live in
urban** — every urban app gets them for free. Only the **domain semantics** — what
an app's own bespoke rows *mean* and what *success* means — are app-specific,
because they are the domain by definition; and both are **thin, declared, and
optional**, degrading gracefully to the free generic tier when absent.

**Consequence for nano-workforce:** for the free tier it needs **essentially no
change** (and can *delete* its bespoke `app/blackboard.ts` hook once S7 lands, as
epic #124 already intends). Its *optional* uplift to typed signals + real
outcome-truth is a small manifest annotation block plus one `outcomeTruth` hook —
authored once, incremental, never blocking.

**This also retires the join-key worry below:** once the key is urban's ambient
`__instance`, the plane no longer depends on the *app* preserving
`plan_key`/`author_task`/`wave` through S6/S7 — though S6/S7 should still carry
`wave`/`author_task` into `scope` for the sibling-scope patterns (§1.2).

### 3.2 Two non-negotiable boundaries

- **Join-key discipline.** The projection is a **join** on the ambient `__instance`
  stamp (§3.1); `wave` / `author_task` still ride into `scope` so the
  sibling-scope patterns (§1.2) resolve. Absent the urban lift, an app falls back
  to joining on its own business key (nano-workforce: `plan_key`), which then must
  survive every hop — so the urban lift is what removes a per-app fragility, not a
  nicety.
- **Determinism boundary — content is observation, never replay input.** The
  blackboard is *explicitly non-replayable and advisory* (`app/blackboard.ts`
  invariant); domain signals inherit that. They **annotate** the trace for
  reasoning and for **scoring** backtests — they do **not** feed Tier-B
  deterministic replay (`process-optimization-design.md` §4), and the engine never
  gates a sequence flow on one. A content **counterfactual** is therefore *"re-run
  the agent step with this signal injected, then measure"* — an act-and-measure
  experiment (doc 4 §2), **not** a deterministic re-run. Keeping this boundary is
  what lets the plane be rich without contaminating determinism ("determinism is
  sacred", index invariants).

## 4. Reasoning — a domain-pattern class under the *inverted* safety model

The domain-signal plane extends the §8 reasoning interface, unchanged in shape:

- **Report, not firehose.** The §8 performance report gains **content
  aggregations**: signals-per-role histograms, *repeated-role-across-sibling-scope*
  detectors (the generic redundant-recompute/contention finder), role↔outcomeTruth
  correlations, and a handful of **worst-case exemplar instances** with their
  signals inlined.
- **Typed transforms, but a new class.** Alongside the §8 performance transforms
  (parallelize, cache, model-swap, retry-tune, batch), a **domain/outcome class**:
  *promote a recurring `knowledge` signal to a plan-time input*, *re-partition a
  parallel fan-out to avoid `claim` contention*, *shift a recurring `defect` class
  to an earlier gate*, *guard upstream of a clustered escalation*. These change
  **business behavior**, so:
- **The safety model inverts (doc 5 §2.2), by nature not policy.** The verifier can
  prove technical/data-flow equivalence; it **cannot** prove business soundness,
  and the §6 objective is **incomplete** at this layer (it does not encode the
  tail-risk a "redundant" check manages). Therefore domain-class candidates are
  **suggest-only, human-in-the-loop decision support — never an autonomous
  canary.** The measurement layer is the *credibility engine* for the suggestion
  (doc 5 §2.3: *"here is the redesign, and here is the simulated/measured evidence
  it saves X at Y risk on your real traffic"*), not its approver.

This slots domain optimization onto the index's inverted ladder exactly one rung
below business semantics: **more automatable than pure BPR (the signals are
concrete and measured), less automatable than a performance transform (the
verifier cannot bless the outcome).**

```
  business semantics      advisory — human decides; external/RAG evidence
  DOMAIN CONTENT   ← NEW  advisory — human decides; execution-data evidence
  process structure       verifier-gated transforms
  task binding            model/provider routing, quality-gated
  worker internals        observe → configure → transform
  resources / deployment  simulate (queueing) → canary → scale
```

## 5. Outcome-truth — the delayed correctness signal the catalogue already asked for

Doc 3 §2 flags **outcome-truth (correctness, with delay)** as one of the two big
missing investments (alongside side-effect/purity). The domain-signal plane is its
natural home: the app supplies, per instance, *"how did this actually turn out?"*
via the same projector seam (`outcomeTruth`, §2). For nano-workforce:
`status ∈ {merged, escalated, abandoned}`, `roundsToConverge`, `reworkEvents`. This
is the ground-truth that lets a domain hypothesis be **scored** ("did forward-
feeding the learning reduce `roundsToConverge` on replayed-then-re-run plans?")
rather than merely asserted — the §6 objective, generalized above latency/cost.

## 6. nano-workforce — the first target (free tier ≈ no change; uplift is thin & optional)

With the urban lift (§3.1), nano-workforce's **required** change for the free tier
is **essentially none**: ambient stamping makes its blackboard and record rows
join-projectable as G1 today, and the S7 blackboard lift gives it typed
`knowledge`/`claim`/`constraint` signals without app code (it can then *delete* its
bespoke `app/blackboard.ts` hook). Everything below is the **optional** uplift to
typed bespoke-table signals + rich outcome-truth — authored once, incremental.

- **Namespaces / roles (G2 annotations, optional):**
  - `nano-workforce/blackboard`: shipped by urban via S7 (`learning`→`knowledge`,
    `file-claim`→`claim`, `constraint-change`/`scope-change`→`constraint`,
    `note`→`note`); `scope` ← `{ wave, actor: author_task }`. **No app authoring.**
  - `nano-workforce/records` (bespoke tables, app-authored): `pr.record-plan` /
    `pr.record-plan-review` / `pr.persist-round` / `pr.persist-escalation` rows →
    `decision`/`defect`/`note` as appropriate, anchored to their producing element.
- **Outcome-truth (G3, thin, optional):** derive `{ status, roundsToConverge,
  reworkEvents }` from the convergence-/merge-loop record rows; absent this, the
  plane falls back to the generic instance terminal status (§3.1).
- **Derived signal (G3, optional):** per-wave `claim` **collision count** (rows
  touching the same file within a wave) — the one aggregate a plain row projection
  can't express.
- **Patterns it unlocks** (all via the generic vocabulary of §1.2, none hardcoded
  in the reasoner):
  1. `knowledge` re-derived across waves → **forward-feed as a plan-time
     constraint** at `pr.record-plan` (redundant-recompute, class B). *The
     headline win.*
  2. `claim` collision hotspots → **re-levelize wave partitioning** (contention).
  3. recurring `defect` class in review rounds → **shift-left to a lint/plan gate**.
  4. escalation clustering → **upstream guard** (reliability).

All four are **suggest-only** (§4): ProcessOS proposes with measured evidence; a
human decides. The prototype's success bar is narrow and honest: **does attaching
content change the answer?** — i.e. surface win #1, which a timing-only trace of
the same plans cannot see.

## 7. Where it lives + invariants preserved

- **Crate:** additive to `processos/` ingest/store/report only (`ingest.rs` gains
  the projector fold; `contracts.rs::InstanceTrace` gains `domainSignals` /
  `outcomeTruth`; `report.rs` gains the content aggregations; `reason.rs` gains the
  domain transform class). No new engine seam beyond the existing exporter tap and
  the app-declared projector binding.
- **One-way dependency, absent-safe.** Nano still never imports ProcessOS; the
  projector binding is plain manifest data consumed by ProcessOS/host ingest. A
  cluster using none of this pays nothing.
- **`engine-core` unchanged.** No control-flow, no engine read of content, no new
  hot-path field. `__signal.<ns>` rides the existing `CompleteJob { variables }`
  exactly as `__cost` does.
- **Determinism sacred / verifier authoritative within reach.** Content never
  enters replay; the domain transform class is explicitly advisory where the
  verifier cannot reach (§3.1, §4).

## 8. Staged rollout

| Stage | Deliverable | Depends on |
|---|---|---|
| **D0** | **Urban lift** (§3.1): ambient `__instance`/`__element` stamping on `DataLayer.insert`; expose the fused-domain schema registry to ingest. Domain-free; benefits every urban app. | urban (Node ALS) |
| **D1** | `domainSignals`/`outcomeTruth` schema slot + G1 convention projection (any DataLayer table, joined on `__instance`) + generic outcome-truth = instance terminal status | Tier-A trace (exists); D0 for the ambient key |
| **D2** | **Prototype**: nano-workforce over one recorded plan corpus — blackboard signals (free via S7 or a stand-in binding) + optional G3 outcome/collision; content aggregations in the report; demonstrate win #1 | D1; a recorded plan corpus |
| **D3** | `__signal.<ns>` push channel (worker-emitted signals) | D1 |
| **D4** | Domain transform class + the inverted-safety **suggest-only** report card | D2 |
| **D5** | Scoring loop: act-and-measure counterfactual (re-run agent step with signal injected) closed against `outcomeTruth` | D4; doc 4 §2 act-and-measure |

Start at **D2** as a spike behind **D0/D1**: it is the smallest end-to-end proof
that *process + content* beats *process alone*, and it needs no engine change. D0
is small, domain-free, and independently valuable (every urban app gets a join key
and G1 for free), so it can land ahead of the rest.

## 9. Open questions

- **Element anchoring for pulled content.** Push signals anchor naturally
  (they ride a job); pulled rows often only carry an instance/business key, not an
  element. Best-effort anchor via `scope.actor`/timestamp, or leave
  `elementId: null` and attach at instance scope? (Mirrors the §8 anchor-map
  problem for cross-version compare.)
- **Role vocabulary size.** How closed should the generic `role` set be? Too small
  loses signal; too large re-domainifies the reasoner. Start minimal
  (`knowledge/claim/constraint/defect/handoff/decision/note`), grow only on
  evidence.
- **Privacy / volume.** Content can be large and sensitive (review bodies, agent
  transcripts). Reuse the Tier-A caps/opt-in posture (`NANOBPMN_TRACE_VARIABLES`
  byte caps): signals are opt-in, byte-capped, and redaction-hookable at the
  projector seam.
- **Two products or one?** Doc 5 §3 asks whether the semantic layer is a separate
  product; the domain plane sits just under it. Likely the **same** product — it is
  measured, not RAG'd — but worth confirming the boundary.
