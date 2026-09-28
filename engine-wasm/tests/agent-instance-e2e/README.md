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
  `inclusiveGateway` (#1168) against both committed wasm variants, so the
  advertised element support cannot regress on a stale/un-regenerated `pkg`
  even while the native Rust tests pass. Uses the shared canonical diagrams in
  `engine-core/tests/fixtures/`.

Run against freshly generated artifacts:

```sh
npm install
npm test
```

The parent build owns `engine-wasm/pkg/`. These tests are part of
`make engine-wasm-ffi-dist`; source-only changes are not verified by running them
against an older package.
