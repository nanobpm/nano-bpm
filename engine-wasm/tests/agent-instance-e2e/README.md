# Canonical job lease and agent probes

These probes exercise real `@nanobpm/engine-wasm` lean and read-model artifacts:

- `lease-fencing.mjs`: opt-in leasing for ordinary/agent-marked service jobs and
  execution listeners; required nullable `jobLeaseToken`; opaque-token lifecycle
  fencing; optional update tokens; sticky leased-job eligibility after failure
  and timeout; event replay.
- `verify.mjs`: required CREATE/UPDATE attribution, history-based configuration,
  duplicate-CREATE rejection, deduplication results, nullable history metrics,
  TEXT/OBJECT/DOCUMENT content, pending/committed/discarded history, and process
  cleanup. Response field lists come directly from `spec/agent-instances.yaml`.
- `external-routing.mjs`: external/aiAgentTask routing, expression and element-id
  fallback, priority/retries, headers, and linked prompt resources.
- `element-support.mjs`: deploy + createInstance for `sendTask` and
  `inclusiveGateway` (#1168) against both wasm variants, so the
  advertised element support cannot regress in the wasm build
  even while the native Rust tests pass. Uses the shared canonical diagrams in
  `engine-core/tests/fixtures/`.

Build the package first (`engine-wasm/pkg/` is a gitignored build output),
then run:

```sh
make -C ../../.. console-wasm
npm install
npm test
```

`make engine-wasm-agent-instance-e2e` does both. These tests are part of
`make engine-wasm-ffi-dist`, which CI runs against a package built from the
PR's own source.
