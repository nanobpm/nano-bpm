// Single source of truth for the Nano Agent Graph Engine claims on nanobpm.io
// (home hero + install tabs, nav, architecture). A module rather than a function
// inside build.mjs so the tests can derive their expectations from it too.
// Hosts and commands mirror the "Install" table of the
// nanobpm/nano-agent-graph-engine README; `tested` marks the hosts we have
// verified end to end (the rest render as "also works with").
export function nageFacts() {
  const pkg = "@nanobpm/nano-agent-graph-engine";
  return {
    name: "Nano Agent Graph Engine",
    short: "NAGE",
    pkg,
    cli: "nage",
    node: "Node 22.15 or newer",
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
        commands: [`npm install -g ${pkg}`, "nage"],
        note: "The <code>nage</code> CLI: check, start and inspect flows from a shell.",
      },
      { id: "codex", label: "Codex", tested: false, commands: [`npx -y ${pkg} install codex`] },
      { id: "cursor", label: "Cursor", tested: false, commands: [`npx -y ${pkg} install cursor`] },
    ],
  };
}
