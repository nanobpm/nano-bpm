// Public entry point for the Urban App manifest library: the JSON-Schema-derived
// AppManifest types, the project symbol index (ADR 0029), and the fail-closed
// validator (ADR 0027 §4). Consumed by the console App panels and the Node App
// loader/compile gate.

export * from "./symbol-index.ts";
export * from "./domain-types.ts";
export * from "./feel.ts";
export * from "./manifest-completion.ts";
export * from "./validate.ts";
export * from "./form-data-binding.ts";
export * from "./data-query.ts";
export * from "./page-nodes.ts";
export * from "./page-presentation.ts";
// Re-export the full generated type module (AppManifest + every sub-interface:
// Surfaces, PagesSurface, ActionDecl, DataSource, …) so downstream consumers
// (e.g. @nanobpm/urban) need not reconstruct sub-types via indexed access.
export type * from "../gen/nano-app.d.ts";
