import { defineConfig } from "@hey-api/openapi-ts";
import { $RefParser } from "@hey-api/json-schema-ref-parser";
import { selectPaths, type OpenApiDoc } from "./src/lib/specSubset.ts";

// The Camunda REST (`/v2`) operations the console calls with a generated client.
// Their types derive from the canonical engine spec (spec/rest-api.yaml), cut
// down to these paths and the components they reach (see src/lib/specSubset.ts).
// Add a path here to expose another operation; never hand-write its types.
const C8_PATHS = [
  "/agent-instances/search",
  "/agent-instances/{agentInstanceKey}/history/search",
];

const c8Spec = selectPaths(
  (await new $RefParser().bundle({
    pathOrUrlOrSchema: "../spec/rest-api.yaml",
  })) as OpenApiDoc,
  C8_PATHS,
);

export default defineConfig([
  // Generates the typed console API client (types + per-operation SDK functions +
  // fetch client) from the single source of truth: spec-console/console-api.yaml.
  // Output lands in src/gen/ and is imported directly by the console SPA.
  {
    input: "../spec-console/console-api.yaml",
    output: {
      path: "src/gen",
    },
    plugins: [
      {
        name: "@hey-api/client-fetch",
        // The console is served same-origin under /console/api (the spec's
        // server URL), so no runtime baseUrl override is needed.
        runtimeConfigPath: "./src/lib/apiClientConfig.ts",
      },
      {
        name: "@hey-api/sdk",
        // Throw on non-2xx so callers can use try/catch like the old client.
        throwOnError: true,
      },
      "@hey-api/typescript",
    ],
  },
  // The Camunda REST subset (C8_PATHS), served same-origin under /v2.
  {
    input: c8Spec,
    output: {
      path: "src/gen-c8",
    },
    plugins: [
      {
        name: "@hey-api/client-fetch",
        runtimeConfigPath: "./src/lib/c8ApiClientConfig.ts",
      },
      { name: "@hey-api/sdk", throwOnError: true },
      "@hey-api/typescript",
    ],
  },
]);
