// Type-only stand-in for the user's own video-publisher module that the
// publish-video example (publish-video.ts) calls into. On the site only the
// flow is shown; in a real project these are your functions, in your repo.
// They throw here: this file exists so the example type-checks and compiles.
import type { JsonObject } from "@nanobpm/nano-agent-graph-engine";

const standIn = (name: string): never => {
  throw new Error(`${name}: stand-in for the example; call your own code here`);
};

/** ffmpeg → 16 kHz WAV → a local whisper.cpp model. Returns { title, srtPath }. */
export async function transcribeLocally(_videoPath: string): Promise<JsonObject> {
  return standIn("transcribeLocally");
}
/** Fix product names and jargon the transcript got wrong. */
export async function fixTranscript(_videoPath: string): Promise<JsonObject> {
  return standIn("fixTranscript");
}
/** Validate the agent's draft (title length, CHAPTERS block, tags). Sets describeState. */
export async function checkDescription(_vars: JsonObject): Promise<JsonObject> {
  return standIn("checkDescription");
}
/** Upload as a private YouTube video with the chosen thumbnail and subtitles. */
export async function uploadPrivate(_vars: JsonObject): Promise<JsonObject> {
  return standIn("uploadPrivate");
}
/** Write the reviewed description next to the video, without uploading. */
export async function saveDescription(_vars: JsonObject): Promise<JsonObject> {
  return standIn("saveDescription");
}
