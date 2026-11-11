// The landing page's flow examples: single source of truth for the home page's
// example tabs (website/build.mjs), the BPMN the in-browser demo runs for each
// (website/scripts/compile-demo-flows.mjs) and the drift guard between the two
// (website/demo-flows.test.mjs). `id` is the flow id in the snippet, the compiled
// demo/src/<id>.bpmn, and the demo scenario id (/demo/?scenario=<id>).

export const DEMO_FLOWS = [
  {
    id: "review",
    label: "Review a branch",
    snippet: "nage-review.ts",
    caption: "Adversarial review: reviewers by lens, a challenger per finding, a fix agent that fixes or asks you.",
    // What it does and how, in plain words (HTML: inline <code> only).
    summary:
      "Reviews a branch before you merge it, adversarially. A script collects the changeset against <code>main</code>. " +
      "Reviewers run in parallel, one per lens (correctness, contracts and callers, tests, duplicated logic), and each " +
      "sees the whole changeset and reads the codebase around it, because the bugs that matter cross files. Code merges " +
      "their findings and drops duplicates. Then, one per finding, a challenger on a different CLI tries to refute it; " +
      "only findings that survive go on. A fix agent fixes them and the loop reviews again, until the review comes back " +
      "clean. When the fix agent can't decide on its own, it escalates to you, and your answer goes back into the loop.",
  },
  {
    id: "publish-video",
    label: "Publish a video",
    snippet: "publish-video.ts",
    deps: ["video-publisher.ts"],
    caption: "Transcribe locally, let agents draft the metadata and thumbnails, review, then upload.",
    summary:
      "Turns a screen recording into a YouTube upload. A local Whisper model (whisper.cpp) extracts the transcript, " +
      "so the audio never leaves your machine, and a script fixes product names it misheard. " +
      "An agent drafts the title, description, chapters and tags; code checks the draft and sends a bad one back " +
      "with the errors. Another agent proposes thumbnails. You review the draft: upload it as a private video, " +
      "save the description, or send it back with feedback for a redo.",
  },
];
