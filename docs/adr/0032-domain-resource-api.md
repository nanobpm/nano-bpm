# ADR 0032 — The domain-resource API (the Kogito seam: process instances as first-class REST resources)

Status: **Proposed.**
Date: 2026-07-22.
Relates to: ADR 0026 (`0026-urban-human-surfaces-and-run-model.md`, the **action API** — the *generic*
`POST /app/actions/start/<process>` substrate; this ADR generates a *domain-shaped* tier **over** it,
not beside it, §5),
ADR 0030 (`0030-domain-process-duality.md`, the **charter** — "domain object = record + a lifecycle"
and the **director** (legal transitions); this ADR is that object exposed as an addressable REST
resource whose verbs are the lifecycle),
ADR 0031 (`0031-process-relational-mapper.md`, the **PRM** — the projection that *backs* the resource;
its Open #1 "per-instance vs per-domain-entity grain" **is** this resource's identity question, §3),
ADR 0029 (`0029-urban-bindings-domain-model.md`, the domain model + symbol index — the types the
endpoints are generated from, and the enumeration the routes resolve against),
ADR 0027 (`0027-urban-app-manifest-spec.md`, spec-first codegen — `generate-app-manifest.sh` emits the
routes + an OpenAPI fragment; the manifest declares the resource bindings),
ADR 0024 (`0024-urban-data-layer-datasource-abstraction.md`, the **rest bank** — `GET`/list query the
domain-shaped read model via Drizzle),
ADR 0028 (`0028-urban-app-user-auth-identity-authorization.md`, `security.rules` — each verb maps to a
role rule, enforced by the same middleware, §6),
ADR 0025 (`0025-urban-trigger-runtime.md`, the unattended peer — a webhook `start` and a
`POST /orders` are the two faces of the same `createInstance`),
ADR 0022 (`0022-nano-rad-application.md`, the product keystone — where the generated resources ship;
its "App as MCP server" open question, §6),
`~/workspace/camunda/zeebe` (Camunda 8's **generic** `POST /process-instances {processDefinitionId,
variables}` — the untyped, engine-noun baseline this ADR types over), and — the prior art this ADR
names — Red Hat **Kogito** (Quarkus/jBPM/Drools: **build-time codegen of domain-specific REST
endpoints** from a BPMN process + its data model, so a process *is* a typed microservice — the move
this ADR adopts and completes).

## Context

ADR 0026 §1 gives an App a small, uniform **action API** — `POST /app/actions/start/<process>`,
`/message/<name>`, `/tasks/<key>/complete`, `GET /app/tasks`. It is deliberately **generic and
engine-noun-shaped**: the caller says "start the process named `orders` with this body." That is the
right *substrate* — one uniform surface every human UI and integrator can call — but it is the same
shape Camunda 8 ships (`POST /process-instances {processDefinitionId, variables}`): the **domain is
absent at the API**, exactly as it is absent in the untyped payload (ADR 0030 §Context). You address
the *engine's* noun (a "process instance"), not the *business's* noun (an "order").

Red Hat's **Kogito** made the move that closes this. From a BPMN process and its data model, Kogito
**generates, at build time**, a domain-shaped typed service: `POST /orders`, `GET /orders/{id}`,
`DELETE /orders/{id}`, with user tasks and signals as sub-resources. The process instance **is** the
order resource; its lifecycle **is** the order's lifecycle. This is the **API-layer analog** of the
two things Urban already decided: 0030 §5's *domain-shaped read model* ("Orders awaiting payment", not
"instances parked at `Task_3`") and 0031's *domain object* (face + motion + rest). Kogito is
independent, cross-vendor evidence for the 0030 charter — it reached "the process as a first-class
domain object" from the jBPM tradition.

Two facts decide how Urban should take this idea rather than copy it:

1. **CRUD-over-process is a *glove*, not free mutation.** `PUT /orders/{id}` on a running instance
   cannot mean "overwrite the order"; it means "evolve it via a **legal transition**," and the legal
   set is the process graph — the **director** (0030 §1/§2). A naïve CRUD facade hides the director and
   invites callers to treat a governed lifecycle as a setter.
2. **Kogito typed the *motion*, but left the *rest* a wiring exercise.** A Kogito service still
   persists to a store *you* configure and wire per service (Infinispan/Mongo/JDBC) — domain-typed at
   the API, still decomposed at the data layer. Urban already owns the rest bank (0024) and generates
   the motion↔rest projection (0031), so it can back the resource with *owned* state and reconcile it
   across tense — the half Kogito leaves to the customer.

The gap this ADR closes: ADR 0026 stops at the generic tier; nothing yet exposes the domain object as
a **first-class, typed, self-describing REST resource** — and 0029/0030/0031 have already produced the
exact material (the domain type + its three projections + the director) to generate one, more complete
than Kogito's.

## Decision (proposed)

Generate a **domain-resource API**: a typed, domain-shaped REST tier — created at build time from a
process bound to a domain type — that exposes the domain object as a first-class resource whose CRUD
verbs are its lifecycle, layered **over** the generic action API (0026) and backed by the PRM (0031).

### 1. The generated resource

For each domain type that is **process-backed**, generate (resource name from §Open):

| Verb | Endpoint | Semantics | Underlying primitive |
|---|---|---|---|
| **create** | `POST /orders` | start an instance seeded from the *typed* body | `createInstance` (0026 §1) |
| **read** | `GET /orders/{id}` | the domain object, reconciled across tense (0031) | PRM projection (motion if in-flight, rest if settled) |
| **list/query** | `GET /orders?…` | the domain-shaped read model, typed filters | Drizzle query over the rest bank (0024/0031) |
| **update-as-transition** | `POST /orders/{id}/<move>` | advance via a **legal** transition | `complete`-task / `CorrelateMessage` / signal (0025 §5) |
| **delete-as-abort** | `DELETE /orders/{id}` | terminate the lifecycle (policy-gated, §Open) | cancel-instance |
| **tasks** (sub-resource) | `GET /orders/{id}/tasks` | the object's open human tasks | human-task API (0026) |

### 2. The resource is self-describing — CRUD is a glove that shows the director

The verbs are CRUD-familiar so integrators feel at home, but the semantics are **lifecycle-constrained
(the director, 0030)**. Therefore `GET /orders/{id}` **includes the currently-legal transitions** — a
HATEOAS-style `_actions` block ("this order may be `approve`d or `cancel`led *now*"). "Update" is never
a blind setter: it names a legal move, and an illegal move is a `409`, not a silent overwrite. This is
the honest correction to a naïve CRUD-over-process facade — the CRUD glove **surfaces** the director
rather than hiding it, which is exactly the affordance a generic engine API cannot offer.

### 3. Resource identity is the *domain key*; grain follows PRM Open #1

`{id}` is the **domain key** (e.g. `orderId`), resolved through the PRM (0031), **not** necessarily the
raw process-instance key. Whether `GET /orders/{id}` addresses **one domain entity** (possibly several
motions across its life) or **one process instance** is precisely **ADR 0031's Open #1** (per-entity
vs per-instance grain). This ADR does **not** re-decide that — but it requires the API be expressed in
*domain-key* terms from the start, so the surface survives whichever grain 0031 settles. (Concretely:
the resource contract cannot be frozen before 0031 Open #1 is answered — they are one decision seen
from two layers.)

### 4. Generated, build-time, spec-first — Kogito's discipline on Urban's toolchain

Like Kogito, the resource API is **generated at build time, not a runtime metamodel dispatcher.** The
0027 generator emits, from the process + domain type (0029), **plain typed route handlers plus an
OpenAPI fragment**, checked at author/`deno compile`/boot and then erased (ADR 0029 §3 discipline). No
reflection on the hot path; the engine still moves untyped JSON; **switch-over parity is preserved**
because everything the tier adds lives *above* the engine contract.

### 5. Two tiers, one substrate — the domain resource *composes* the action API

The domain-resource endpoints are **generated wrappers** that call the generic 0026 action API / engine
primitives underneath (create → `createInstance`; transition → `complete`/correlate; read → PRM). There
is **one runtime substrate** (0026 + engine + PRM) and a **generated typed face** — not a second engine
API. The generic tier remains for dynamic/reflective callers and internal use; bespoke GUIs and
external integrators prefer the typed domain resource. Sugar-and-types over a substrate, never a fork.

### 6. Auth, completeness, and forward hooks

- **Auth (0028).** Each verb maps to a `security.rules` role rule (`create`/`read`/`transition`/
  `delete` → roles), enforced by the same middleware seam. The domain resource is where role rules read
  most naturally: "who may **create** an Order", "who may **approve** one."
- **Not all matter has motion (0030).** A **rest-primary** domain type (a product catalog, config — no
  lifecycle) gets a *plain* CRUD resource straight over the datasource (Drizzle), while a
  **motion-primary** type gets the lifecycle-constrained resource above. The generator picks by whether
  the type is process-backed — so the *same* domain-resource API spans reference data **and** live
  processes uniformly, a completeness Kogito's process-centric view does not reach.
- **Switch-over (additive, per 0022 §E.1 discipline).** The generic tier stays Camunda-compatible; the
  domain tier is Urban sugar generated above it. Lifting an App onto Camunda 8 loses the generated typed
  face (regenerate, or fall back to generic) but never the process — additive, never a substitute.
- **MCP hook (0022 open question).** A typed, self-describing domain resource is the natural surface to
  *also* expose as an **MCP server** — each resource a typed tool, each legal transition a tool call —
  making the App itself an agent tool. Noted as a forward seam, not decided here.

## Consequences

- Urban adopts Kogito's best idea — the process as a **first-class, typed, domain-shaped REST resource**
  — **generated, not hand-wired**, and strictly more complete: backed by *owned* rest (0024/0031),
  **surfacing the director** (legal moves), gated by *unified* auth (0028), and spanning rest-primary
  types too.
- Integrators get a **familiar CRUD-shaped API** whose semantics are honestly lifecycle-constrained —
  the comfort of REST without the lie that a governed lifecycle is a setter.
- One substrate, one generated face: no second runtime, parity preserved, types erased.
- **Honest gaps.** (a) The resource contract is **blocked on 0031 Open #1** (grain). (b) `DELETE`
  semantics need policy (hard abort vs. a lifecycle end-state vs. compensation, §Open). (c) The typed
  face is **lost on switch-over** to a bare Camunda 8 (regenerate). (d) `GET` during flight inherits
  0031's read-freshness question (motion vs. projected rest).

## Open questions

1. **Resource naming** — derived from the process id, the domain-type name, or an explicit manifest
   `resource` alias? Pluralization and collision rules when several processes touch one domain type.
2. **Update-verb shape** — a sub-resource per transition (`POST /orders/{id}/approve`, REST-idiomatic)
   vs. a single `PATCH /orders/{id}` carrying a target action/state (fewer routes). Likely sub-resource,
   enumerated from the process's legal moves (§2).
3. **`DELETE` meaning** — hard abort (cancel the instance), a domain **soft-delete** (a modelled
   end-state), or a **compensation** trigger? Probably declared per type; default = abort with policy.
4. **Rest-primary CRUD routing** — do pure reference-data resources bypass the engine entirely and hit
   Drizzle directly (§6)? If so, how do auth/validation stay uniform with the process-backed routes?
5. **Read consistency** — `GET /orders/{id}` mid-flight: read **motion** (freshest, from the engine) or
   **rest** (projected, possibly stale)? Ties to ADR 0031 Open #4 (when persist fires).
6. **Create idempotency** — a client-supplied idempotency key on `POST` to dedup double-submit,
   shared with ADR 0026's UI idempotency key and ADR 0025's inbox keys.
7. **MCP exposure** — is the domain-resource tier auto-published as an MCP toolset, and do transitions
   map one-to-one onto MCP tools (resolving part of ADR 0022's "App as MCP server")?
   **Settled in part (2026-11-11, epic #1499):** ADR 0022's question is answered — Nano exposes
   Camunda's MCP contract (`/mcp/cluster`, `/mcp/processes`); any domain-resource MCP toolset
   would be additive over it.
