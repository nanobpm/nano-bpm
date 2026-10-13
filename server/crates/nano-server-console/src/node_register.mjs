// nanobpmn Node loader bootstrap (ADR 0065).
//
// Runs on the main thread via `--import ./nano-generated/node-register.mjs` when
// a project runs under Node (the sole JS/TS runtime). It registers the import-map
// loader (node-loader.mjs) on the module resolution thread so the project's
// `node.json` import map resolves bare specifiers.
import { register } from "node:module";

register("./node-loader.mjs", import.meta.url);
