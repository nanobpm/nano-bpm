// Console view of the processes exposed as MCP tools (issue #1612) — parity
// with Camunda's admin "MCP processes" page.
//
// The catalog is built SERVER-SIDE by the same resolver + descriptor the
// `/mcp/processes` MCP server uses for `tools/list`
// (`server/src/mcp/processes_tools.rs::console_catalog`); this module only
// fetches and presents it. Never re-derive tool names, descriptions or input
// schemas here — that would be a second source of truth that can drift from
// what an agent is actually offered.

/** One JSON-Schema property of a tool's `inputSchema` (nested for a
 * data-envelope-derived schema, issue #1738). */
export interface McpInputProperty {
  type?: string;
  format?: string;
  description?: string;
  properties?: Record<string, McpInputProperty>;
  required?: string[];
  items?: McpInputProperty;
}

/** A tool's `inputSchema` exactly as advertised in `tools/list`. */
export interface McpInputSchema {
  type?: string;
  properties?: Record<string, McpInputProperty>;
  required?: string[];
}

/** One process exposed as an MCP tool (`GET /console/api/mcp/processes`). */
export interface McpProcessTool {
  /** `<toolName>_<subscriptionKey>` — the name an agent calls. */
  name: string;
  title: string;
  description: string;
  inputSchema: McpInputSchema;
  outputSchema?: unknown;
  toolName: string;
  messageName: string;
  subscriptionKey: string;
  processDefinitionKey: string;
  processDefinitionId: string;
  processDefinitionVersion: number;
  processName?: string | null;
  /** Where `inputSchema` came from: the start message's data envelope, or the
   * `io.camunda.tool:input_N_*` properties (issue #1738). */
  inputSchemaSource?: "dataEnvelope" | "toolProperties";
  /** The envelope shape id, when the schema derives from one. */
  dataEnvelope?: string | null;
  /** Authoring conflicts (e.g. envelope AND input_N_* declared). */
  warnings?: string[];
}

/** The catalog payload. */
export interface McpProcessesCatalog {
  /** Whether the MCP gateway is mounted (`NANOBPM_MCP_ENABLED`). */
  enabled: boolean;
  /** The env var that toggles it. */
  envVar: string;
  /** The MCP endpoint path, e.g. `/mcp/processes`. */
  path: string;
  /** Static tools served alongside the per-process ones. */
  staticTools: string[];
  tools: McpProcessTool[];
}

/** The reserved, non-variable tool argument (always present in the schema). */
export const BUSINESS_ID_ARGUMENT = "businessId";

/** One row of the rendered input table. */
export interface McpInputRow {
  name: string;
  type: string;
  description: string;
  required: boolean;
  /** The reserved `businessId` argument (not a process variable). */
  reserved: boolean;
}

/** A property's display type: `array<item>` for arrays, the `format` when set. */
function displayType(p: McpInputProperty): string {
  if (p.type === "array")
    return `array<${p.items ? displayType(p.items) : "any"}>`;
  if (p.type && p.format) return `${p.type} (${p.format})`;
  return p.type ?? "any";
}

/**
 * Flatten an advertised input schema into display rows: declared inputs
 * first in schema order (nested object fields — also array items' — follow
 * their parent as dotted `parent.child` / `parent[].child` rows), the
 * reserved `businessId` last. Pure presentation — the schema itself comes
 * from the server.
 */
export function inputRows(schema: McpInputSchema | undefined): McpInputRow[] {
  const rows: McpInputRow[] = [];
  const walk = (
    s: McpInputSchema | McpInputProperty | undefined,
    prefix: string,
  ) => {
    const required = new Set(s?.required ?? []);
    for (const [name, p] of Object.entries(s?.properties ?? {})) {
      const path = prefix ? `${prefix}.${name}` : name;
      rows.push({
        name: path,
        type: displayType(p),
        description: p.description ?? "",
        required: required.has(name),
        reserved: !prefix && name === BUSINESS_ID_ARGUMENT,
      });
      walk(p, path);
      if (p.items) walk(p.items, `${path}[]`);
    }
  };
  walk(schema, "");
  return [
    ...rows.filter((r) => !r.reserved),
    ...rows.filter((r) => r.reserved),
  ];
}

/**
 * Normalize a (possibly partial) response body so the view never crashes on a
 * missing field.
 */
export function normalizeCatalog(body: unknown): McpProcessesCatalog {
  const b = (body ?? {}) as Partial<McpProcessesCatalog>;
  return {
    enabled: b.enabled ?? false,
    envVar: b.envVar ?? "NANOBPM_MCP_ENABLED",
    path: b.path ?? "/mcp/processes",
    staticTools: Array.isArray(b.staticTools) ? b.staticTools : [],
    tools: Array.isArray(b.tools) ? b.tools : [],
  };
}

/** Fetch the MCP processes catalog. */
export async function getMcpProcesses(): Promise<McpProcessesCatalog> {
  const res = await fetch("/console/api/mcp/processes");
  if (!res.ok) {
    const detail = await res.text().catch(() => "");
    throw new Error(detail || `mcp/processes → HTTP ${res.status}`);
  }
  return normalizeCatalog(await res.json());
}
