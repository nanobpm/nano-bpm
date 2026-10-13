# @nanobpm/nano-bernd

Embedded Nano BPMN engine for Node and the browser. Codename **Bernd** — see [ADR 0005](../../docs/adr/0005-embedded-u-nano.md).

Runs `nano_engine.wasm` (ABI v2) in-process — no server, no network, no native binaries. Same engine, same lifecycle model as the `nanobpm-gateway-rest-server` binary, just embedded.

## Signature

```
      _   _                       ____                     _
     | \ | | __ _ _ __   ___     | __ )  ___ _ __ _ __  __| |
     |  \| |/ _` | '_ \ / _ \    |  _ \ / _ \ '__| '_ \/ _` |
     | |\  | (_| | | | | (_) |   | |_) |  __/ |  | | | | (_| |
     |_| \_|\__,_|_| |_|\___/    |____/ \___|_|  |_| |_|\__,_|

     Named for Bernd Ruecker, whose talks on decoupled workers,
     Sagas and the compensation pattern are the intellectual source
     of the embedded-engine design realised here.
     Artists sign their work. See ADR 0015.
```

## Usage (Node)

```ts
import { EmbeddedEngine } from '@nanobpm/nano-bernd';

const engine = await EmbeddedEngine.create();
try {
  engine.deploy(bpmnXml);
  const { processInstanceKey } = engine.createInstance('order-fulfilment');

  for (const job of engine.activateJobs({ type: 'charge-card', worker: 'worker-1', maxJobs: 10, timeoutMs: 30_000 })) {
    // ...do work...
    engine.completeJob(job.key);
  }
} finally {
  engine.close();
}
```

## Usage (browser)

The default `create()` uses `node:fs` to load the packaged wasm. In the browser, pass your own `wasmBytes + manifest`:

```ts
const [wasm, manifest] = await Promise.all([
  fetch('/nano_engine.wasm').then((r) => r.arrayBuffer()),
  fetch('/manifest.json').then((r) => r.json()),
]);
const engine = await EmbeddedEngine.create({ wasmBytes: wasm, manifest });
```

`EmbeddedEngine.CODENAME === 'Bernd'`.
