# MCP cluster-tools schema snapshot (vendored from Camunda)

`tools-schema-snapshot.json` is a byte-for-byte copy of Camunda's MCP gateway
schema snapshot:

- **Upstream repo:** `camunda/camunda`
- **Upstream path:** `gateways/gateway-mcp/src/test/resources/schema/tools-schema-snapshot.json`
- **Upstream commit SHA:** `f6acaf4d8431fd592c7f125bc37f1c2cbe334193` (2026-11-09)
- **Camunda version line:** 8.9 (`gateways/gateway-mcp`)

It captures the exact `tools/list` metadata — the 18 static cluster tool names,
titles, descriptions, `inputSchema` (properties + required), and `annotations`
(`readOnlyHint`) — that Camunda's own schema test asserts against.

Nano's parity-guard test (`server/src/mcp.rs`,
`tools_list_matches_camunda_snapshot`) builds Nano's `tools/list` response and
compares it against this file, failing on any unexplained drift in the tool set,
names, required params, or types. When Camunda changes the tool surface, re-copy
the snapshot from the new upstream commit and update the SHA above.
