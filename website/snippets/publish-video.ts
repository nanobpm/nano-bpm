// Compilable source of the nanobpm.io "Publish a video" example for Nano Agent
// Graph Engine — a public, trimmed version of a flow its author really runs.
//
// The `flow` region below is extracted VERBATIM by website/build.mjs and shown on
// the landing page; the same file is compiled to the BPMN the in-browser demo
// runs (website/demo/src/publish-video.bpmn, via website/scripts/compile-demo-flows.mjs),
// so the code a visitor reads and the run they watch are one flow. It is
// type-checked against the published package in CI ("website snippet (typecheck)").

//#region flow
import { defineFlow, defineForms, envelope } from "@nanobpm/nano-agent-graph-engine";
import { checkDescription, fixTranscript, saveDescription, transcribeLocally, uploadPrivate } from "./video-publisher.ts";

export const forms = defineForms({
  "video-review": {
    title: "Review: {{ytTitle}}",
    description: "{{ytDescription}}",
    fields: {
      decision: { type: "string", enum: ["upload", "save", "redo"] },
      thumbnail: { type: "string", enum: ["1", "2", "3", "none"] },
      feedback: { type: "string", title: "Feedback for a redo" },
    },
    required: ["decision"],
  },
});

const Input = envelope("PublishVideoInput", { videoPath: "string" });

export const publishVideo = defineFlow("publish-video", { Start: { in: Input } }, (w) => {
  // A local Whisper model (whisper.cpp): the audio never leaves your machine.
  w.run("transcribe", (job) => transcribeLocally(String(job.variables.videoPath)));
  w.run("correct", (job) => fixTranscript(String(job.variables.videoPath)));

  w.loop((b) => {
    // An agent drafts the title, description, chapters and tags. Code checks
    // the draft; a bad one goes back to the agent with the errors.
    b.loop((d) => {
      d.task("describe", { jobType: "senior:describe", prompt: { resourceId: "describe.md" } });
      d.run("check-description", (job) => checkDescription(job.variables));
      d.switch("describeState", { done: (e) => e.break(), default: (e) => e.continue() });
    });
    b.task("thumbnails", { jobType: "senior:thumbnails", prompt: { resourceId: "thumbnails.md" } });

    // You review the draft and pick a thumbnail. Redo loops back with your feedback.
    b.human("review", { form: "video-review" });
    b.switch("decision", {
      upload: (c) => {
        c.run("upload", (job) => uploadPrivate(job.variables));
        c.break();
      },
      save: (c) => {
        c.run("save", (job) => saveDescription(job.variables));
        c.break();
      },
      default: (c) => c.continue(),
    });
  });
});
//#endregion flow
