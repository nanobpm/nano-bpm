// Single source of truth for the Nano Agent Graph Engine claims on nanobpm.io
// (home hero + install tabs, nav, architecture). A module rather than a function
// inside build.mjs so the tests can derive their expectations from it too.
//
// Published facts (version, CLI name, Node requirement) are DERIVED from the
// published package as locked by website/snippets/package-lock.json (npm records
// the package's `bin` and `engines` there), so a NAGE bump that changes them
// flows to the site with no hand edit. Every install command is built from the
// derived package + CLI names. The host list itself is site presentation: the
// package publishes no machine-readable host manifest (only its README table),
// and `tested` marks the hosts we have verified end to end (the rest render as
// "also works with").
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const PKG = "@nanobpm/nano-agent-graph-engine";
const LOCK = join(dirname(fileURLToPath(import.meta.url)), "snippets", "package-lock.json");

/** The published package's locked metadata; fails closed if any fact is missing. */
export function publishedNage(lockPath = LOCK) {
  const entry = JSON.parse(readFileSync(lockPath, "utf8")).packages?.[`node_modules/${PKG}`];
  if (!entry) throw new Error(`${PKG} is not in ${lockPath}`);
  const version = entry.version;
  const nodeRange = entry.engines?.node;
  const m = /^>=\s*(\d+(?:\.\d+)*)$/.exec(nodeRange ?? "");
  // The CLI the README documents is `nage`; take it from `bin` so a rename fails here.
  const cli = Object.keys(entry.bin ?? {}).find((b) => b === "nage");
  if (!version || !m || !cli) {
    throw new Error(`${PKG} lock entry lacks version / engines.node (>=x.y) / bin.nage: ${JSON.stringify(entry)}`);
  }
  return { version, node: `Node ${m[1]} or newer`, cli };
}

export function nageFacts() {
  const pkg = PKG;
  const { version, node, cli } = publishedNage();
  return {
    name: "Nano Agent Graph Engine",
    short: "NAGE",
    pkg,
    version,
    cli,
    node,
    repo: "https://github.com/nanobpm/nano-agent-graph-engine",
    hosts: [
      {
        id: "claude-code",
        label: "Claude Code",
        tested: true,
        prompt: false, // typed into Claude Code, not a shell
        commands: [
          "/plugin marketplace add nanobpm/nano-agent-graph-engine",
          "/plugin install nano-agent-graph-engine@nanobpm",
        ],
        note: "Brings the MCP server, skills, hooks, and the <code>/flows</code> pane.",
      },
      {
        id: "claude-desktop",
        label: "Claude Desktop",
        tested: true,
        prompt: true,
        commands: [`npx -y ${pkg} install claude-desktop`],
        note:
          'Or open the <code>.mcpb</code> from <a href="https://github.com/nanobpm/nano-agent-graph-engine/releases">Releases</a>. ' +
          "Restart Desktop (⌘Q) afterwards. No server to run, no workers to configure.",
      },
      {
        id: "opencode",
        label: "OpenCode",
        tested: true,
        prompt: true,
        commands: [`npx -y ${pkg} install opencode`],
        note: "Adds the server to <code>~/.config/opencode/opencode.json</code> and installs the skills. Works in the OpenCode desktop app too.",
      },
      {
        id: "terminal",
        label: "Terminal",
        tested: true,
        prompt: true,
        commands: [`npm install -g ${pkg}`, cli],
        note: "The <code>nage</code> CLI: check, start and inspect flows from a shell.",
      },
      { id: "codex", label: "Codex", tested: false, commands: [`npx -y ${pkg} install codex`] },
      { id: "cursor", label: "Cursor", tested: false, commands: [`npx -y ${pkg} install cursor`] },
    ],
  };
}
