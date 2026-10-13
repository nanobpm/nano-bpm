# Guided journeys

Onboarding for the console, per [ADR 0049](../../../../docs/adr/0049-guided-journeys.md).
Epic: [#406](https://github.com/nanobpm/nano-bpm/issues/406). This directory is the
framework; the journeys themselves live in [`journeys/`](./journeys).

## What changed, and why

The original tour keyed its step list on `CONSOLE_PROFILE`. That is the wrong branch:
`studio` vs `observe` is a **build-time** split, while the users who matter most all
land in the _same_ studio build wanting materially different first sessions — someone
orchestrating coding-agent harnesses, someone who just wants a local
Camunda-compatible engine, and someone prototyping a fullstack app. One step array
cannot serve them.

Two further defects the framework fixes structurally:

- Steps **described the UI** ("Explorer, Traces, Metrics and Workers give full
  visibility…") instead of leaving a result behind. A journey now declares a
  `successEvent` — what must actually have happened — and completion and success are
  recorded separately, because walking every step is not the same as achieving the
  outcome.
- One step **promised what the console knew it could not keep**: "hit Run to boot the
  engine" on a host with no JavaScript runtime, when the very `listProjects()`
  response that feeds the tour carries `nodeAvailable`. Steps now
  declare preconditions, and a `repair` step replaces the promise with an install
  hint.

## The pieces

| File                 | Role                                                                                                                                                                                                                                                                          |
| -------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `types.ts`           | The contract: `Journey`, `Step` (`spotlight` \| `note` \| `handoff`), `Precondition`, `Predicate`, `TourContext`. Type-only imports, so it never pulls `../profile` (and its `__STUDIO__` global) into a Node test.                                                           |
| `preconditions.ts`   | The shared gate library — `hasJsRuntime`, `hasProject`, `hasCluster`, `hasTraces`, plus `isPolling(jobType)` for handoff verification. Import these rather than hand-rolling a check.                                                                                         |
| `registry.ts`        | `registerJourney` / `getJourney` / `journeysFor(profile, ctx)` and `resolveSteps` (repair substitution + skips). No central import list, so parallel journey slices never contend on one file.                                                                                |
| `context.ts`         | Builds a `TourContext` from ONE `listProjects()` call, plus `registerContextSource` for slices that need more (`nodeCount`, `runState`, `consumers`).                                                                                                                         |
| `state.ts`           | Versioned, resumable state (`nano.tour.v2`), migrating the old `nano.tour.v1.seen` boolean. Every access is guarded — onboarding is never worth breaking the app over.                                                                                                        |
| `runner.ts`          | The driver.js adapter — **the only module importing driver.js**.                                                                                                                                                                                                              |
| `deepLink.ts`        | `?tour=<journeyId>` parsing and stripping.                                                                                                                                                                                                                                    |
| `useProductTour.ts`  | The React seam: router, one fetch, and the rail button's label.                                                                                                                                                                                                               |
| `tourAnchors.ts`     | The `data-tour` anchor names + `navAnchor()` / `tourSelector()` helpers. Both the journeys (selectors) and the views (attributes) derive from here, and nav anchors key off the stable **route** (`item.to`) not the display label, so a rename cannot silently break a step. |
| `journeys.test.ts`   | `node --test` guard: unknown selectors, cross-profile anchors, over-long journeys, missing repair steps.                                                                                                                                                                      |
| `resolution.test.ts` | Precondition resolution + registry filtering, including the repair path.                                                                                                                                                                                                      |
| `state.test.ts`      | Persistence, the v1 → v2 migration, deep links, context assembly.                                                                                                                                                                                                             |
| `tour.css`           | Popover + handoff theming via the app's own `--nano-*` tokens, so it tracks dark/light mode.                                                                                                                                                                                  |

## Adding a journey

1. Create `journeys/<name>.ts`, build a `Journey`, and call `registerJourney(...)` at
   module scope.
2. Import it for its side effect in `useProductTour.ts` (the one line a new journey
   adds outside its own file).
3. Keep it to **five steps or fewer**. A journey that needs a detour outside the
   console is _split at that boundary_ — see the `agentic-author` / `agentic-hire`
   split in ADR 0049 §6 — not padded.
4. Derive every spotlight selector with `tourSelector(TOUR_ANCHOR.x)`. Never write a
   literal `[data-tour="…"]` string: `journeys.test.ts` fails if you do, which is what
   stops a renamed attribute from silently skipping a step.
5. Give it a real `successEvent`. The overview journey's `() => true` is the deliberate
   exception, and the reason it is the fallback rather than the default.

### Anchors

Add anchors in the view you own: tag the element with `data-tour={TOUR_ANCHOR.x}` and
add the key to `TOUR_ANCHOR`. The convention is `<area>-<thing>`; nav items derive
theirs from the route via `navAnchor()`.

A selector that does not resolve **yet** is fine: driver.js's `skipMissingElement`
drops the step, so a journey can be written against an anchor a sibling slice has not
landed and will light up when it does.

### Imports and Node tests

Runtime imports between modules **reached by a Node test need an explicit `.ts`
extension** (`from "../registry.ts"`) — Node's ESM resolver does not guess, and
`allowImportingTsExtensions` makes it valid for the Vite build too. Type-only imports
are erased and need no extension. Same rule as `lib/pageComposer.ts`.

## Preconditions

`ok` shows the step, `skip` drops it, `repair` substitutes `step.repair`.

Use `repair` when the target is present but the action would fail — the user is about
to press that button, so they need the honest alternative, not a silent omission. Use
`skip` when there is simply nothing worth showing (no traces captured yet). A `repair`
verdict with no repair step authored degrades to a skip, because showing the original
would assert exactly what the precondition just said is untrue.

Preconditions resolve **once, when the journey starts**: they describe the environment,
which does not usually change over the ninety seconds a journey takes. Liveness that
_does_ change — a handoff waiting for a worker to connect — is handled by `verify`,
polled every 2s while that step is showing.

## Handoff steps

The step kind that makes the headless and agent-hiring journeys expressible, since
neither happens entirely inside the console: a copyable command or URL.

- With `verify`, the step auto-advances when the console observes the _result_ of the
  command (e.g. `isPolling(jobType)` once
  [#404](https://github.com/nanobpm/nano-bpm/issues/404) supplies `consumers`).
- Without it, the Next button reads "I've done it" — honest, since the console cannot
  watch a terminal.
- `copy` is rendered with `textContent` and is **never executed**. Nothing here may
  become an execution vector, which matters once packs can contribute journeys
  (ADR 0049 §7).
- Clipboard writes fall back to select-and-copy: `navigator.clipboard` needs a secure
  context, and this console is routinely served over plain HTTP on a LAN address.

## Why driver.js

The journeys **span react-router routes** and target elements that **mount
asynchronously** over CSS-transformed canvases (bpmn-js / monaco). driver.js gives two
things that fit exactly, for free:

- `waitForElement` — waits for a step's target to appear before showing it (the
  generalised "canvas ready" gate).
- `skipMissingElement` — skips a step whose target never appears, so a journey degrades
  gracefully (e.g. the overview's **Run** step on a fresh install with no open project).

What it cannot do is drive react-router or wait on application state; `runner.ts` owns
both. Keeping driver.js behind that single adapter is also what would let the runner be
swapped without touching a journey file.

### Chosen over @reactour/tour (issue #393)

driver.js won the A/B: ~½ the bundle (~7 KB gz, **zero deps**), framework-agnostic (no
provider), `waitForElement` + `skipMissingElement` built in, finer positioning (side +
align), and far healthier upstream (1.4M dl/wk vs 167k; released weekly vs last publish

> 1 yr ago). reactour's only edge — built-in focus-lock a11y — didn't outweigh it. The
> reactour spike lived on `feat/console-tour-reactour` (PR #397, closed).

## Entry points

The front door is the **startup persona panel** ([#464](https://github.com/nanobpm/nano-bpm/issues/464)):
a modal shown when the console is opened, listing the offerable journeys as
first-person personas ("I want to …") the person self-selects. A **"Show at
startup" checkbox** (default checked, persisted as `showStartupPanel` in
`nano.tour.v2`) turns it off for returning users. This replaced the earlier
design where `c8ctl` sprayed `…/console?tour=<id>` links across every
`start` / `hire` / `work` — the persona is now chosen _in_ the console, by the
person, not encoded in whichever link they happened to click.

Other ways in:

- **`JourneyPicker`** — the same journeys as cards on the Projects/Topology empty
  state ([#411](https://github.com/nanobpm/nano-bpm/issues/411)).
- **Take a tour** in the rail (**Resume tour** when a journey was interrupted).
- **`?tour=<journeyId>`** deep links still work (parsed by `deepLink.ts`, stripped
  once started) — a valid, no-longer-advertised entry, kept for links already in
  the wild. The CLI stopped emitting them in
  [jwulf/c8ctl-plugin-nano#22](https://github.com/jwulf/c8ctl-plugin-nano/pull/22)
  (part of [#464](https://github.com/nanobpm/nano-bpm/issues/464)); the startup
  panel is the front door now.

## Try it

```bash
npm run dev
```

To replay: **Take a tour** in the rail (**Resume tour** when a journey was interrupted).
To forget everything: `localStorage.removeItem("nano.tour.v2")`, or `resetTour()`.

```bash
npm test          # unit tests (node:test, no browser needed)
npm run typecheck
```

[#417](https://github.com/nanobpm/nano-bpm/issues/417) adds the Playwright suite
that asserts every journey's selectors actually resolve in a browser.
