// Compilable source of the nanobpm.io landing example for Nano Agent Graph Engine.
//
// The `flow` region between the //#region / //#endregion markers below is
// extracted VERBATIM by website/build.mjs and rendered on the landing page, so
// what visitors see is exactly this code. It is type-checked against the
// published `@nanobpm/nano-agent-graph-engine` in CI (the "website snippet
// (typecheck)" job), so the landing example can never drift from the real API.

//#region flow
import { defineAgents, defineFlow, defineForms, sh } from "@nanobpm/nano-agent-graph-engine";

// The challenger argues the other side, so suggest a different CLI for it.
export const agents = defineAgents({ challenge: { use: "opencode" } });

export const forms = defineForms({
  escalation: {
    title: "The fix agent needs you",
    description: "{{question}}",
    fields: {
      decision: { type: "string", enum: ["fix", "ship"] },
      guidance: { type: "string", title: "Guidance for the fix agent" },
    },
    required: ["decision"],
  },
});

export const review = defineFlow("review", (w) => {
  w.loop((b) => {
    // A script step: real code, in your repo. Every reviewer gets the whole
    // changeset, and reads the rest of the codebase around it.
    b.run("changeset", async () => {
      const files = (await sh("git diff --name-only main")).stdout.split("\n").filter(Boolean);
      return { files, lenses: ["correctness", "contracts and callers", "tests", "duplicated logic"] };
    });

    // Fan out by what to look for, not by file: one reviewer per lens, in parallel.
    b.forEach("lenses", "lens", (l) => {
      l.task("review-lens", { jobType: "senior:review", prompt: { resourceId: "review.md" } });
    }, { name: "Review by lens", outputCollection: "reviews", outputElement: "found" });

    // Code, not an agent: merge every lens's findings and drop duplicates.
    b.run("triage", (job) => {
      const reviews = (job.variables.reviews ?? []) as string[][];
      return { findings: [...new Set(reviews.flat())] };
    });

    // Then fan out per finding: an adversarial challenger, on a different CLI,
    // tries to refute each one against the codebase.
    b.forEach("findings", "finding", (f) => {
      f.task("challenge", { jobType: "senior:challenge", prompt: { resourceId: "challenge.md" } });
    }, { name: "Challenge each finding", outputCollection: "verdicts", outputElement: "verdict" });

    // The fix agent fixes what survived. It reports "clean" (nothing to fix),
    // "fixed" (review again) or "stuck" (it needs a decision from you).
    b.task("fix", { jobType: "senior:fix", prompt: { resourceId: "fix.md" } });
    b.switch("fixState", {
      clean: (c) => c.break(),
      fixed: (c) => c.continue(),
      default: (c) => {
        // Escalate: wait for you, an hour or over the weekend. The run survives.
        c.human("escalate", { form: "escalation" });
        c.switch("decision", { ship: (d) => d.break(), default: (d) => d.continue() });
      },
    });
  });
});
//#endregion flow
