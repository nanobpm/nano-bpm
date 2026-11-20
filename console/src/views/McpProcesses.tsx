import { useQuery } from "@tanstack/react-query";
import { PageHeader, SectionLabel } from "@nanobpm/process-explorer/ui";
import {
  getMcpProcesses,
  inputRows,
  type McpProcessTool,
} from "../lib/mcpProcesses";

// Processes exposed as MCP tools (issue #1612) — parity with Camunda's admin
// "MCP processes" page. Lists what `/mcp/processes` offers an AI agent: one
// tool per message start event declaring `io.camunda.tool:name`. The catalog
// is built server-side from the same resolver the MCP server uses, so this
// view is presentation only.

export default function McpProcesses() {
  const { data, error, isLoading } = useQuery({
    queryKey: ["mcp-processes"],
    queryFn: getMcpProcesses,
    refetchInterval: 5000,
  });

  return (
    <div className="flex h-full flex-col">
      <div className="border-b border-edge px-6 pt-4">
        <PageHeader
          title="MCP Processes"
          subtitle="Processes exposed as MCP tools — what an AI agent connected to the MCP processes server is offered."
        />
      </div>
      <div
        className="flex-1 overflow-auto px-6 py-4"
        data-testid="mcp-processes"
      >
        {error && (
          <div className="mb-3 rounded border border-danger/30 bg-danger/10 px-3 py-2 text-xs text-danger">
            {error instanceof Error ? error.message : String(error)}
          </div>
        )}
        {data && (
          <div
            data-testid="mcp-status"
            data-enabled={data.enabled ? "true" : "false"}
            className={`mb-4 rounded border px-3 py-2 text-xs ${
              data.enabled
                ? "border-ok/30 bg-ok/10 text-ok"
                : "border-warn/30 bg-warn/10 text-warn"
            }`}
          >
            {data.enabled ? (
              <>
                MCP is enabled — tools are served at{" "}
                <code className="font-mono">{data.path}</code>.
              </>
            ) : (
              <>
                MCP is disabled (
                <code className="font-mono">{data.envVar}</code>
                ). The processes below would be exposed at{" "}
                <code className="font-mono">{data.path}</code> once it is
                enabled.
              </>
            )}
            {data.staticTools.length > 0 && (
              <span className="ml-1 text-fg-muted">
                Also served: {data.staticTools.join(", ")}.
              </span>
            )}
          </div>
        )}
        {isLoading && <div className="text-sm text-fg-muted">Loading…</div>}
        {data && data.tools.length === 0 && (
          <div data-testid="mcp-empty" className="text-sm text-fg-muted">
            No processes are exposed as MCP tools. Add an{" "}
            <code className="font-mono">io.camunda.tool:name</code> property to
            a message start event and deploy the process.
          </div>
        )}
        <div className="flex flex-col gap-4">
          {data?.tools.map((t) => (
            <ToolCard key={t.name} tool={t} />
          ))}
        </div>
      </div>
    </div>
  );
}

function ToolCard({ tool }: { tool: McpProcessTool }) {
  const rows = inputRows(tool.inputSchema);
  return (
    <div
      data-testid="mcp-tool"
      className="rounded-lg border border-edge bg-panel p-4"
    >
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <code
          data-testid="mcp-tool-name"
          className="font-mono text-sm font-medium text-fg"
        >
          {tool.name}
        </code>
        <span data-testid="mcp-tool-process" className="text-xs text-fg-muted">
          {tool.processName ? `${tool.processName} · ` : ""}
          {tool.processDefinitionId} v{tool.processDefinitionVersion}
        </span>
      </div>
      <div className="mt-1 text-xs text-fg-muted">
        message <code className="font-mono">{tool.messageName}</code>
      </div>
      {tool.description && (
        <pre
          data-testid="mcp-tool-description"
          className="mt-3 whitespace-pre-wrap font-sans text-sm text-fg"
        >
          {tool.description}
        </pre>
      )}
      <SectionLabel>Input schema</SectionLabel>
      <table
        className="mt-1 w-full text-left text-xs"
        data-testid="mcp-tool-inputs"
      >
        <thead className="text-fg-muted">
          <tr>
            <th className="py-1 pr-3 font-medium">Name</th>
            <th className="py-1 pr-3 font-medium">Type</th>
            <th className="py-1 pr-3 font-medium">Required</th>
            <th className="py-1 font-medium">Description</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr
              key={r.name}
              className="border-t border-edge"
              data-testid="mcp-input-row"
            >
              <td className="py-1 pr-3 font-mono">
                {r.name}
                {r.reserved && (
                  <span className="ml-1 text-fg-muted">(reserved)</span>
                )}
              </td>
              <td className="py-1 pr-3 font-mono">{r.type}</td>
              <td className="py-1 pr-3">{r.required ? "yes" : ""}</td>
              <td className="py-1 text-fg-muted">{r.description}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
