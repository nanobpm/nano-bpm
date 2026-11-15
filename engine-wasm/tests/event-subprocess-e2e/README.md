# event-subprocess-e2e

Real-WASM end-to-end guard for event sub-processes (#1555, #1616). Deploys
`event-subprocess.bpmn` (service task `work`, non-interrupting `PT1M` timer ESP
→ `notify`, interrupting `cancel` message ESP) into the `@nanobpm/engine-wasm`
`TestEngine` (lean **and** read-model entrypoints) and asserts:

1. the `work` job is created;
2. after `advanceTime` past 1m, `work` and `notify` are both active;
3. after `correlateMessage("cancel")` the instance completes via
   `esp_cancel_end` (not `main_end`), with `work`/`notify` terminated.

Runs in CI in the `engine-wasm-ffi (dist + verify)` job against the package
built from the PR source. Locally: `make engine-wasm-event-subprocess-e2e`
(builds the package via `make console-wasm`). `engine-wasm/pkg` is a build
output — never commit it (#668).
