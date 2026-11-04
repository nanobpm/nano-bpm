# nano — Vision

nano is a high-performance, highly scalable process automation engine.

It exists to carry the ideas pioneered by Zeebe and Camunda 8 forward — to honour
them, extend them, and make them reachable by every team that has ever wanted them
but been unable to adopt, afford, or scale them. We build with reverence for the
people and the ideas that came before, and with the confidence that the next order
of magnitude of scale, simplicity, and delight is still ahead of us.

## What we are building

A process automation engine that is:

- **Fast** — designed around a single-writer engine core, a purpose-built Raft log,
  and a command-stream protocol that keeps latency low and throughput high.
- **Highly scalable — in both directions** — partitioned, replicated, and
  memory-disciplined so that a single cluster can absorb workloads that would
  previously have required painful vertical scaling. And, just as importantly, nano
  scales *down*: it runs in small, resource-constrained environments where a heavy
  engine simply cannot go, opening opportunities at the edge and in constrained
  deployments that were never addressable before.
- **Compatible** — a drop-in replacement for teams already running Camunda 8, so
  that adopting nano is a migration measured in hours, not quarters.

## Our four missions

### 1. Unblock migration to Camunda 8

Many teams want the Camunda 8 model but are blocked from getting there. We will
identify, name, and systematically remove those blockers — operational complexity,
resource footprint, cost, migration friction — so that the path onto the modern
process-orchestration model is open to everyone who wants it.

### 2. Be a drop-in replacement

nano speaks the language teams already use. Existing BPMN models, existing client
SDKs, existing job workers, and existing operational habits should continue to work.
Switching to nano should feel like an upgrade, not a rewrite. Compatibility is a
feature we defend deliberately, not an accident we tolerate.

### 3. Overmatch the competition on every feature

We will analyse, honestly and continuously, where sales are lost to competitors and
why. Then we will overmatch them — not by matching a checklist, but by being clearly,
demonstrably better on the features that decide real evaluations. Where a competitor
is strong, we aim to be stronger; where they set the bar, we clear it and raise it.

### 4. Enter new markets through scale — up and down

nano's scalability is not just a bigger number on a benchmark — it changes which
problems are solvable and which customers are addressable. Scaling *up*, workloads
that were once "too high volume," "too cost-sensitive," or "too operationally heavy"
for a process engine become viable. Scaling *down*, nano runs in small,
resource-constrained environments — the edge, embedded contexts, modest hardware —
where a traditional engine cannot fit at all. Both directions open markets that were
previously closed to us.

## Dogfooding: Nano Workforce

Urban — nano's application tier — earns its keep when it can express a serious, stateful,
human-in-the-loop automation entirely from its own primitives: a process, a DMN table, forms, a
datasource, connectors, and agent-backed workers. **Nano Workforce** is that proof, and we build it by
turning it on ourselves first.

We already develop nano with a crew of coding agents: an epic fans a dozen slices out across parallel
git worktrees, coordinated today by a hand-run claim mutex and one agent playing coordinator. Nano
Workforce makes that a durable Urban app — you talk to one orchestrator, and a "Crew Task" process runs
the crew: dispatching each unit of work to an agent-as-worker in a clean worktree, holding the claim as
an engine-leased mutex, supervising with engine timers, and escalating only real decisions to the
captain as user tasks. The engine owns the durability, the mutex, the supervision, and the recovery
that filesystem-and-shell agent orchestrators re-implement by hand.

It is a flagship, not a toy: the state-in-motion ↔ state-at-rest story told as a shipping application,
and the integrating pressure that keeps forms, DMN, the datasource, connectors, and the console honest
together. See [ADR 0051](docs/adr/0051-nano-workforce.md).

## How we work

nano engineering is **fast, fluid, and fun.**

We are engineers who operate in — and actively maintain — a frictionless environment.
That environment is built from simple engineering disciplines, not top-down
bureaucracy. It rests on **simplicity** and **trust**: fast tooling, clean feedback
loops, honest benchmarks, and a bias toward removing obstacles for each other. These
are part of the product, not overhead around it. The quality of the engine and the
quality of the experience of building it are the same discipline viewed from two
sides. We keep both sharp.

## Principles

- **Reverence, not critique.** We build on the shoulders of Zeebe and Camunda 8. We
  extend their ideas with gratitude and hindsight, not disdain.
- **Compatibility is sacred.** Drop-in means drop-in. We protect the migration
  promise on every change.
- **Scale is a market strategy — both ways.** Every order of magnitude we unlock,
  up or down, is a door into a market that was previously shut.
- **Frictionless by design.** A frictionless environment is built from simple
  engineering disciplines, simplicity, and trust — never top-down bureaucracy. We
  build and defend it on purpose, for our users and for ourselves.
- **Overmatch, don't match.** We measure ourselves against the best and aim to clear
  the bar with room to spare.

## Design principles

Any system that is powerful enough to be more than trivially useful is necessarily
complex. Our job is to make that complexity serve the user, not burden them:

- **Remove unnecessary complexity.**
- **Progressively disclose necessary complexity.**
- **Provide opinionated pathways with sensible defaults.** There are many ways to
  accomplish a goal with the system; here is one that works out of the box.
- **Provide discoverable affordances that leverage users' existing intuitions.**
