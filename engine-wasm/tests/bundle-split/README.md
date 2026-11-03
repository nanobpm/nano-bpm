# engine-wasm bundle-split test

Proves the acceptance-criterion for the dual-publish slice (#825): a bundler
following the static module graph emits **only** the wasm for the subpath that is
actually imported.

```sh
make -C ../../.. console-wasm   # builds ../../pkg (a gitignored build output)
npm install   # resolves @nanobpm/engine-wasm from ../../pkg (file:) + esbuild
npm test      # builds both entrypoints and asserts the emitted asset set
```

- `lean-app.mjs` imports only `@nanobpm/engine-wasm` (the `.` / lean subpath).
- `readmodel-app.mjs` imports only `@nanobpm/engine-wasm/readmodel`.
- `verify.mjs` bundles each with esbuild (`bundle`, `loader:{.wasm:'file'}`,
  `metafile`) and asserts that the lean build references **only** the lean
  `_bg.wasm` and the readmodel build references **only** the readmodel one.
