import type { CreateClientConfig } from "../gen-c8/client.gen";

// Runtime configuration for the generated Camunda REST subset client (src/gen-c8,
// see openapi-ts.config.ts). The gateway serves the Camunda API same-origin under
// /v2 (the spec's `servers` URL is host-templated, so pin the path here). Reject
// on any non-2xx, matching the console API client.
export const createClientConfig: CreateClientConfig = (config) => ({
  ...config,
  baseUrl: "/v2",
  throwOnError: true,
});
