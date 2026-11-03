# Test-Studio record loop — worked example + guard

A runnable example that drives a **Test-Studio-style recording session** entirely
through the `@nanobpm/engine-wasm` `TestEngine` built from source, and asserts
the behaviour so it cannot silently regress.

## Why this exists

Camunda's "Test Studio" recorder needs **per-instance isolation**: it stops at
each job and each call activity and asks the user *"mock this, or run the real
thing?"*. In a **shared cluster** that requires two bolt-on engine switches:

| Test-Studio switch | What it buys | Why a shared cluster needs it |
| --- | --- | --- |
| `RESERVE_JOBS` (+ `jobReservationToken`) | jobs are hidden from every worker so the recorder can mock them by key | otherwise a real worker/connector runtime races and grabs the job first |
| `stubCallActivities` | a call activity waits on a stub job instead of starting the real child | otherwise the real child starts, and the only alternative (a stub process definition) pollutes the whole cluster |

The nanobpmn engine compiled to WASM is **embedded and single-session** — there
is no shared cluster and no external worker pool — so that isolation is
*structural*, not a feature to add. This probe demonstrates the two switches
collapse into behaviour the engine already has:

- **`RESERVE_JOBS` → nothing needed.** Jobs sit in `Created` (Zeebe's
  `ACTIVATABLE`) with no one to take them; the recorder drives them by key with
  `completeJob` / `failJob` / `throwError`. No token, no lease, no fencing.
- **`stubCallActivities` → the debug stepper.** An `elementActivated` breakpoint
  on the call activity *is* the wait state. Resume to let the child do real work
  (`runCalledProcess`); hold the pause to mock it. An `everyStep` breakpoint
  pauses at every element, so nested call activities re-park the run — the
  mechanism behind Zeebe's "auto-propagated" recursion, without a root flag.
  (`elementActivated` requires an explicit `id` and matches only that element;
  `everyStep` is the wildcard.)

## What the probe asserts

`verify.mjs`, run against **both** wasm variants (`lean` + `readmodel`):

1. **Record by mocking every job** — a service task and a connector-style task
   both surface as `Created` jobs, in order, and are mocked to completion. No job
   is ever taken by a phantom worker; none is missed.
2. **Connector output-mapping caveat, made concrete** — `zeebe:ioMapping` *does*
   run engine-side (the connector *runtime* does not), so a mocked connector value
   must be the **raw runtime result (pre-mapping)**; the engine applies the model's
   output mapping itself. The probe mocks a raw `{ response: { statusCode: 200 } }`
   and asserts the model's `=response.statusCode` output mapping yields
   `shipStatus: 200`.
3. **Call-activity wait state** — an `elementActivated` breakpoint pauses the run
   at the call activity. The child instance root exists (the engine spawns it
   eagerly) but is **idle** — no token, no child job — so nothing has raced ahead
   of the recorder's decision. Resuming lets the child advance (its job again
   waits unraced) and both instances complete.

## Fidelity note (eager child spawn)

Unlike Zeebe's `stubCallActivities` — which creates **no** child — the WASM engine
spawns the child *instance* when the call activity activates, then holds it idle at
the breakpoint. The decision point is the same, but a clean "complete the call
element with mocked child outputs and never run the child" needs either a
`cancelInstance` on the idle child subtree or a future stub hook; it is called out
below as a residual rather than asserted here.

## Not covered here (the shared-cluster design still earns these)

- **Propagate-gate parity when *mocking* a child** (only fire the parent event
  trigger under the model's output-mapping/propagate conditions). Safest is to run
  the real child once and capture its outputs to derive the mock.
- **"Hand to a real external worker"** (`release`) — no native equivalent; that is
  a bridge to real infra / CPT REMOTE mode.

## Run

```sh
make -C ../../.. console-wasm   # builds ../../pkg (a gitignored build output)
npm install                     # links ../../pkg
npm test
```

Like the sibling probes, this runs against `engine-wasm/pkg/` as last built, so
re-run `make console-wasm` after an engine change. CI always builds it from the
PR's own source.
