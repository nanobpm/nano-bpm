// Select a subset of an OpenAPI document: keep only the given paths, plus the
// components they reach transitively through local `$ref`s.
//
// The console consumes a handful of Camunda REST (`/v2`) operations directly.
// Their types must derive from the canonical engine spec (`spec/rest-api.yaml`)
// rather than be hand-written (AGENTS.md "Derivation Over Duplication"), but
// generating the whole ~250-operation spec into the SPA would commit tens of
// thousands of unused lines. `@hey-api/openapi-ts`'s own operation filters do
// not apply to this multi-file spec, so the selection happens here on the
// bundled document before it is handed to the generator.
//
// Pure: no I/O, no dependency on the bundler. Used by openapi-ts.config.ts;
// lives under src/ so it is typechecked and unit-tested (specSubset.test.ts).

type Json = unknown;

export interface OpenApiDoc {
  paths?: Record<string, Json>;
  components?: Record<string, Record<string, Json>>;
  [key: string]: Json;
}

const COMPONENT_REF = "#/components/";

function decodePointerSegment(segment: string): string {
  return segment.replace(/~1/g, "/").replace(/~0/g, "~");
}

function resolveLocal(doc: OpenApiDoc, ref: string): Json {
  return ref
    .slice(2)
    .split("/")
    .reduce<Json>((node, segment) => {
      if (node === null || typeof node !== "object") return undefined;
      return (node as Record<string, Json>)[decodePointerSegment(segment)];
    }, doc);
}

/** Returns a new document holding only `keepPaths` and the components they reach.
 *  Throws if a requested path is missing (a renamed operation must fail loudly). */
export function selectPaths(
  doc: OpenApiDoc,
  keepPaths: readonly string[],
): OpenApiDoc {
  const paths: Record<string, Json> = {};
  for (const path of keepPaths) {
    const item = doc.paths?.[path];
    if (item === undefined) {
      throw new Error(`specSubset: path ${path} is not in the source spec`);
    }
    paths[path] = item;
  }

  // Decoded `section/name` keys of every component reached, so a name holding
  // `/` or `~` (escaped as `~1` / `~0` in the pointer) still matches.
  const visited = new Set<string>();
  const reached = new Set<string>();
  const walk = (node: Json): void => {
    if (Array.isArray(node)) {
      node.forEach(walk);
      return;
    }
    if (node === null || typeof node !== "object") return;
    for (const [key, value] of Object.entries(node)) {
      if (
        key === "$ref" &&
        typeof value === "string" &&
        value.startsWith(COMPONENT_REF)
      ) {
        if (!visited.has(value)) {
          visited.add(value);
          const [section, name] = value
            .slice(COMPONENT_REF.length)
            .split("/")
            .map(decodePointerSegment);
          reached.add(`${section}/${name}`);
          walk(resolveLocal(doc, value));
        }
      } else {
        walk(value);
      }
    }
  };
  walk(paths);

  const components: Record<string, Record<string, Json>> = {};
  for (const [section, items] of Object.entries(doc.components ?? {})) {
    for (const [name, value] of Object.entries(items)) {
      if (reached.has(`${section}/${name}`)) {
        (components[section] ??= {})[name] = value;
      }
    }
  }

  return { ...doc, paths, components };
}
