# ProcessOS — Process–Deployment Co-Optimization

> Status: analysis / direction only. No code changes are made by this document.
> Companion to `docs/process-optimization-design.md` (the "what/why"),
> `docs/processos-design.md` (where the optimizer lives), and
> `docs/processos-latent-process-exploration.md` (the pattern taxonomy + how we
> discover patterns). That sibling reasons mostly about **process structure** and
> **task substitution**. This document widens the frame again: the realization
> that those are two *layers* of a single mapping — from a process specified
> *logically* down to a process *physically running* — and that the **client /
> server architecture and the resources required to service a process** is a
> third layer that is coupled to the other two.
>
> Grounded in: the cost/value model (`process-optimization-design.md` §6), the
> experimentation plane — canary / guardrails / sequential tests (§7), the
> staged simulation harness (§5, §7), the ProcessOS control + read contracts
> (`processos-design.md` §1), the autonomy spectrum (§9), and the pattern
> families of `processos-latent-process-exploration.md` (§1).

## 0. The space: one mapping, several layers

The optimizer's three visible aspects —

1. **client / server architecture & required resources** — how many workers a
   process needs, consolidation of tasks into workers, e2e latency vs cluster
   scaling;
2. **execution cost of tasks & substitution** — routing an LLM task to a
   different model/provider (in the process, or via a client-side org router);
3. **process structure & optimization via process modification** — rewriting the
   BPMN graph;

— are not three problems. They are three **layers of a single mapping** from a
process *specified logically* down to a process *physically running*, optimized
under one policy objective and closed by a control loop. The cleanest analogy is
a mature field that already solved this shape: **database query optimization,
fused with capacity planning and autoscaling.**

| Layer | DB-optimizer analogue | Aspect |
|---|---|---|
| **Logical plan** — control/data-flow graph | relational algebra | (3) process structure |
| **Physical operators** — how each step is realized | hash-join vs merge-join | (2) task binding: model/provider/version |
| **Resource allocation** — placement, parallelism, capacity | degree of parallelism, buffer pool | (1) worker count, consolidation, cluster scaling |
| **Cost model** — the objective | cardinality / cost estimator | §6 utility `U` |
| **Control loop** — act + measure | adaptive query execution | "command the fleet, measure latency" |

**The load-bearing claim:** the objective is defined over the **joint
configuration**, and the layers are **coupled**, so optimizing any one in
isolation is locally optimal at best. The same
`U = wₜ·throughput − w_$·cost − w_L·latencyPenalty(p99) − w_I·incidentRate`
(`process-optimization-design.md` §6) is contested by all layers over the same
finite resources. This is *why* the aspects intersect.

## 1. The couplings — wins move the bill

The intersections are mostly **trade-offs, not free wins**: a gain at one layer
routinely relocates the cost to another. A few that recur:

- **Parallelize two sequential tasks** (structure win, latency↓) → both now run
  at once → **peak worker demand↑** → may *require* scaling. A structure change
  cashes out as a resource bill.
- **Swap to a cheaper-but-slower model** (binding, $↓) → service time↑ → worker
  occupancy↑ → need more workers to hold throughput → cost partially↑ again.
- **Consolidate two job types into one worker** (resource overhead↓) →
  head-of-line blocking: a slow job type stalls the other → latency↑ unless
  concurrency is also tuned.
- **Scale the cluster** (nodes / partitions↑) → create-path throughput↑ but
  ordering / replication cost↑.

So the reasoner's real job is **navigating the Pareto frontier of the joint
configuration**, not maximizing a single knob. Per-layer optimization that
ignores the coupling is how you "win" latency and silently blow the resource
budget.

## 2. The orthogonal axis: observability / actuation regime

There is a second structural insight, *orthogonal* to the layers, and it is what
explains why the resource dimension "is difficult to measure in the isolated
engines." A change's effect falls into one of three regimes:

- **(a) Offline-simulable** — the *isolated engine* (the native / WASM
  `SimRunner` of `processos-design.md` §7.3). Models the **logic**: how a
  structure change alters the work graph (path, service time, compute cost).
  Deterministic, cheap, already built.
- **(b) Model-simulable** — a *discrete-event / queueing simulator*, **not** the
  isolated engine. Models **contention**: queues, worker pools, concurrency,
  backpressure. The isolated engine has *no contention model* — `queueMs`
  (`process-optimization-design.md` §3, broker wait) is a **system** property,
  not an engine property — so scaling and consolidation effects live here. **This
  is a missing simulator:** a DES fitted from the §3 distributions (arrival
  process, service-time histograms, queueMs).
- **(c) Only-live-measurable** — *act-and-measure*. Real-world effects that
  resist modeling: network jitter, provider behavior under load, **human**
  throughput, nonstationary demand. This is "ProcessOS scales the fleet down and
  measures real-time latency" — the §7 canary / guardrail / sequential-test
  machinery applied to **infrastructure**, not just process versions.

Mapping the three aspects onto this axis explains why they *feel* different:
aspect (3) is mostly regime (a); aspect (2) straddles (a)/(c); aspect (1) is
squarely (b)/(c). The latent-exploration doc built only for (a). **Reasoning over
the resource layer at all requires the regime-(b) simulator and the regime-(c)
infra control loop.**

> Discipline preserved: this is still "simulate, rank, then canary," just one
> layer down. Use the DES (b) to rank scaling/consolidation candidates *before*
> the live act-and-measure of (c) — never scale production blind, exactly as we
> never deploy a process version blind.

## 3. The full layer taxonomy (the other categories)

The three named aspects are *structure*, *binding*, and *resource/scaling*. The
complete decision space adds several more. Each is a place the optimizer can act;
each couples to the others through §1.

1. **Process structure** — control + data-flow graph *(aspect 3)*.
2. **Task implementation binding** — executor / model / provider / version per
   task, in-process or via a client-side org router *(aspect 2)*.
3. **Resource allocation & cluster scaling** — worker count per job type, gateway
   / partition count, CPU/mem *(aspect 1)*.
4. **Worker granularity** — consolidation *vs decomposition* as its own
   dimension (the microservices-vs-monolith / thread-per-task-vs-pool decision).
   Consolidation reduces overhead and warms caches; it also couples scaling and
   introduces head-of-line blocking. The lever is *fleet granularity*, and it
   interacts hard with batching (5) and concurrency (6).
5. **Scheduling & concurrency policy** — priority, admission control, batching
   windows, concurrency caps, backpressure thresholds. Temporal control over the
   same workers (`process-optimization-design.md` §7 `Pressure` is the existing
   instinct).
6. **Data / payload layer** — variable size, what is carried vs fetched,
   serialization, shared cache. Drives both broker and worker cost; usually
   invisible until profiled.
7. **Reliability / SLA policy** — retry / timeout / hedge / fallback /
   circuit-break. Trades cost and resource for tail latency and robustness.
8. **Cluster topology** — node count, partitions, replication factor,
   geo / region placement, worker locality relative to the broker and to external
   APIs (LLM region, data residency). Latency + egress cost.
9. **Cross-process / portfolio optimization** — the big one not implied by the
   three aspects. Everything else is *per-process*, but **workers and cluster
   resources are shared across all processes.** Allocating a finite worker /
   compute budget to maximize *aggregate* utility across a fleet of processes is a
   distinct, multi-tenant contention problem — and it is where aspect (1) really
   lives, because a worker pool serves many processes at once. Per-process
   optimization that ignores this merely shifts contention onto neighbours.
10. **Human-resource layer** — for human tasks, "workers" are *people*; scaling =
    staffing / shift / skill-routing. The same queueing math, a different
    actuator, usually recommend-only.
11. **Temporal / demand-shaping** — deferring or batching work to fit capacity
    (off-peak, windows). Trades latency for resource cost; sits between layers
    (3) and (5).
12. **Objective / policy negotiation** *(meta-layer)* — not a lever but a
    *category of reasoning*: surface the achievable Pareto frontier so a human
    picks the trade ("p99 < X costs $Y more — worth it?"). "Make scaling
    recommendations" is this in advisory mode, distinct from autonomous
    actuation.

## 4. Implications for ProcessOS

This widening stretches the existing ProcessOS design in three concrete ways.

### 4.1 A third control verb

`processos-design.md` §1.2 gives ProcessOS exactly two control verbs — **Deploy**
and the **Routing table**. Acting on the resource layer needs a **third: a
fleet / scaling control contract** (scale a worker pool by job type; possibly
cluster node / partition hints). This is a new public write surface and a real
boundary-design decision — heavier than routing because it touches infrastructure
the customer may own and operate. It should follow the autonomy spectrum
(`processos-design.md` §9): **recommend-by-default, actuate-opt-in**, with the
same guardrails / auto-rollback as a process canary. Per §1.3 of that doc, this
is a *Nano (or worker-fleet) public-API* design on its own merits, not a
privileged ProcessOS hook.

> Note the actuation target differs from the engine: scaling commands the
> **worker side** (and possibly the cluster), not the process. For embedded Node
> workers (`server/src/console/workers.rs`) ProcessOS could drive the supervisor;
> for customer-owned workers it emits a recommendation or a signal the customer's
> autoscaler consumes. Either way it then **measures the real-time effect on
> latency** through the §1.1 read contract (metrics + traces) — closing the loop.

### 4.2 A second simulator

Add the regime-(b) discrete-event / queueing simulator alongside the
logic-faithful `SimRunner`. It is fitted from the §3 distributions (arrival
process, per-job service-time histograms, queue depth) and answers the questions
the isolated engine structurally cannot: *how many workers does this process need
to hold p99 < X? what does consolidating these two job types do to tail latency?
where is the cluster the bottleneck vs the workers?* It ranks scaling /
consolidation candidates **before** the live act-and-measure of regime (c).

### 4.3 Portfolio scope

The shared-resource reality (layer 9) means the optimization object may not be a
single process but a **set of processes contending for one worker fleet.** That
changes:

- **Ingest** — needs cross-process resource attribution (which process consumed
  which worker-seconds), not just per-instance traces.
- **Objective** — aggregate utility across the portfolio under a global resource
  budget, not per-process `U` in isolation.
- **Experiment design** — a scaling change for process A perturbs B's latency;
  guardrails must watch *neighbours*, not only the target.

## 5. Naming the space

If a name helps anchor it: **process–deployment co-optimization** — the BPM
analogue of full-stack / hardware–software co-design. The artifact under
optimization is not "this BPMN" but **"this process, as bound, placed, scaled,
and scheduled"**; the optimizer reasons across all of it under one policy, using
three observability regimes (offline engine sim, queueing model sim, live
act-and-measure) and acting through (eventually) three control verbs (deploy,
route, scale).

The earlier documents are slices of this:

- `process-optimization-design.md` — the substrate (trace, replay, cost,
  canary) and the *structure/binding* transform space.
- `processos-design.md` — where the optimizer lives and its public contracts.
- `processos-latent-process-exploration.md` — the *pattern* taxonomy and pattern
  *discovery*, within the logical (a) regime.
- **this document** — the realization that those are layers of one mapping, the
  resource/architecture layer and its (b)/(c) regimes, the missing categories,
  and what they imply for ProcessOS.

## 6. Invariants this must not break

The widening changes the *scope of reasoning*, not the architecture's spine:

- **One-way dependency holds.** Nano (and the worker fleet) never depend on
  ProcessOS; ProcessOS acts only through public contracts — now including a
  scaling/fleet contract designed on its own merits (`processos-design.md` §1.3,
  §8).
- **Verifier + statistics remain authoritative.** A scaling or consolidation
  change is a hypothesis like any other: simulate (b), then canary (c) with
  sequential-test guardrails and auto-rollback (`process-optimization-design.md`
  §7). The reasoner only proposes.
- **Determinism stays where it matters.** The logic regime (a) is still the exact
  `(state, command, now) → events` engine; the queueing regime (b) is explicitly
  a *statistical* model and is labelled as such — never confused with the
  faithful engine replay.
- **Absent-safe.** A cluster with no ProcessOS runs exactly as today; the scaling
  contract is opt-in and recommend-by-default.

---

*This document reframes structure, binding, and resourcing as coupled layers of a
single process→deployment mapping, adds the observability/actuation axis that
explains why the resource layer escapes isolated-engine simulation, enumerates the
remaining categories (notably cross-process/portfolio contention), and derives the
ProcessOS additions they imply: a third (scaling) control verb, a second (queueing)
simulator, and portfolio-scoped ingest and objective. It changes no engine code.*
