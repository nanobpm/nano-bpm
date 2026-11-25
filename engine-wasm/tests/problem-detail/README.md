# engine-wasm ProblemDetail error test

Freezes the error contract from
[#1937](https://github.com/nanobpm/nano-bpm/issues/1937): an engine rejection
from a `TestEngine` method is thrown as a JS `Error` with

- `status` — HTTP status (400 / 404 / 409 / 503),
- `title` — the Zeebe rejection class (`INVALID_ARGUMENT`, `NOT_FOUND`,
  `INVALID_STATE`, `ALREADY_EXISTS`, `UNAVAILABLE`),
- `detail` — the human-readable message,
- `nonRetryable` — `false` only for transient (`UNAVAILABLE`) failures,

all derived from `EngineError::problem` (`engine-core/src/problem.rs`), the
same mapping the server's REST gateway uses. `message` keeps the
`"<operation> error: <detail>"` form.

## Run

```
npm install && npm test
```

(needs a built `engine-wasm/pkg` — `make console-wasm`).
