// The overview journeys — "just show me around".
//
// This is the spike's original tour, ported onto the journey contract and
// deliberately DEMOTED. ADR 0049's finding is that an orientation tour naming
// the tabs serves none of the three target users, so it is no longer what a
// first-timer gets by default: it becomes the zero-commitment fallback for
// someone who does not want to pick a journey, offered alongside the real ones
// in the picker (#411) and behind the rail's "Take a tour" button.
//
// It is also the one journey with no outcome — see `successEvent` below. That is
// precisely why it is the fallback and not the default.

import { registerJourney } from "../registry.ts";
import { TOUR_ANCHOR, tourSelector } from "../tourAnchors.ts";
import type { Journey } from "../types";

/**
 * The overview has nothing to achieve: it is orientation, by design. Every other
 * journey asserts a real outcome (a project that ran, an instance parked on a
 * catch event, a served app that answered), and the contrast is the point — so
 * this returns true rather than inventing a proxy metric that would make an
 * orientation tour look like an accomplishment.
 */
const noOutcome = () => true;

export const overviewStudio: Journey = {
  id: "overview",
  title: "Show me around",
  blurb: "A 60-second orientation: projects, templates, running a model.",
  profiles: ["studio"],
  successEvent: noOutcome,
  steps: [
    {
      kind: "note",
      id: "welcome",
      title: "Welcome to Nano",
      body: "A self-contained BPMN engine and rapid-application IDE. This 60-second tour walks the maker loop: create a project, model a process, run it.",
    },
    {
      kind: "spotlight",
      id: "projects-nav",
      route: "/projects",
      selector: tourSelector(TOUR_ANCHOR.projectsNav),
      title: "Studio",
      body: "Everything starts here. A project is a folder of BPMN/DMN models, forms and a pack that says how to run them.",
      side: "right",
      align: "start",
    },
    {
      kind: "spotlight",
      id: "new-project",
      route: "/projects",
      selector: tourSelector(TOUR_ANCHOR.newProject),
      title: "Scaffold a project",
      body: "Pick a template — each one wires up a runnable toolchain (Node, JVM, Rust, …) so Run works with zero setup.",
      side: "bottom",
      align: "end",
    },
    {
      kind: "spotlight",
      id: "run",
      // No route: Run exists only inside an open project workspace, and this
      // journey deliberately does not navigate there (it would need a project to
      // open, which a first-timer does not have). driver.js's
      // skipMissingElement drops the step when the anchor is absent, so the
      // journey degrades cleanly on a fresh install instead of stalling.
      selector: tourSelector(TOUR_ANCHOR.run),
      optional: true,
      title: "Run your model",
      body: "Open a project and hit Run to boot the engine and execute it. Tokens animate live on the canvas as instances progress.",
      side: "bottom",
      align: "start",
    },
    {
      kind: "spotlight",
      id: "observe-nav",
      route: "/projects",
      selector: tourSelector(TOUR_ANCHOR.explorerNav),
      title: "Observe everything",
      body: "Explorer, Traces, Metrics and Workers give full visibility into running instances — the operator surface is always one click away.",
      side: "right",
      align: "start",
    },
  ],
};

export const overviewObserve: Journey = {
  id: "overview-observe",
  title: "Show me around",
  blurb: "A quick pass over topology, instances and the numbers that matter.",
  profiles: ["observe"],
  successEvent: noOutcome,
  steps: [
    {
      kind: "note",
      id: "welcome-observe",
      title: "Welcome — Operator console",
      body: "This is the lean operator surface: topology, metrics, traces and worker health for a running Nano cluster.",
    },
    {
      kind: "spotlight",
      id: "topology-nav",
      route: "/topology",
      selector: tourSelector(TOUR_ANCHOR.topologyNav),
      title: "Topology",
      body: "Your nodes, partitions and their health at a glance.",
      side: "right",
      align: "start",
    },
    {
      kind: "spotlight",
      id: "explorer-nav",
      route: "/topology",
      selector: tourSelector(TOUR_ANCHOR.explorerNav),
      title: "Explorer",
      body: "Inspect individual process instances — their variables and current state.",
      side: "right",
      align: "start",
    },
    {
      kind: "spotlight",
      id: "metrics-nav",
      route: "/topology",
      selector: tourSelector(TOUR_ANCHOR.metricsNav),
      title: "Metrics",
      body: "Throughput, backlog and latency — the numbers that matter under load.",
      side: "right",
      align: "start",
    },
  ],
};

registerJourney(overviewStudio);
registerJourney(overviewObserve);

/** The overview journey id for a profile — what "Take a tour" runs. */
export function overviewJourneyId(profile: "studio" | "observe"): string {
  return profile === "studio" ? overviewStudio.id : overviewObserve.id;
}
