// Loading Urban components (element templates) from the open project (ADR 0033
// increment 2, resolving OQ1). A component's design-time face is a Zeebe element
// template; increment 1 bundled a sample set as a constant. Here we load the
// real set from the project so the palette *is* the project's installed
// component library — the Delphi move: your components come from what's installed,
// not a hard-coded list.
//
// SOURCE (OQ1, "both"): we scan two directories and merge by template id —
//   - `.camunda/element-templates/` — the Camunda-standard location the Desktop
//     Modeler uses, so a connector catalog / Marketplace template drops in as-is
//     (ADR 0033 §4, two-way compat); and
//   - `components/` — the Urban-native dir (the Delphi-VCL framing).
// On an id collision `components/` (Urban-native) wins over `.camunda/`, and
// within a directory the last file read wins. Pack-contributed components (§4)
// are increment 6 and layer on top of this project source.

import type { FileNode } from "../gen";
import { getExtensions } from "../gen";
import { projectFileEx } from "./api";
import { BUILTIN_COMPONENTS } from "./builtinComponents";
import type { ElementTemplate } from "./urbanComponents";

export type { ElementTemplate } from "./urbanComponents";

/** Project-relative directories scanned for component element templates, in
 *  increasing precedence (later dirs override earlier on an id collision). */
export const COMPONENT_DIRS = [
  ".camunda/element-templates",
  "components",
] as const;

/** Whether a parsed JSON value looks like a Zeebe element template: a string
 *  `id` and a non-empty `appliesTo` array. Loose on purpose — the modeler's
 *  `elementTemplates.set()` runs the authoritative
 *  `@camunda/zeebe-element-templates-json-schema` validation; this only screens
 *  out obviously-unrelated JSON so one stray file can't abort the load. */
function isElementTemplate(v: unknown): v is ElementTemplate {
  if (typeof v !== "object" || v === null) return false;
  const t = v as Record<string, unknown>;
  return (
    typeof t.id === "string" &&
    Array.isArray(t.appliesTo) &&
    t.appliesTo.length > 0
  );
}

/** Parses one component file's text. A file may hold a single template object or
 *  an array of templates (both are valid Camunda element-template files);
 *  returns every valid template it contains, skipping malformed entries. */
function parseComponentFile(text: string): ElementTemplate[] {
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    return [];
  }
  const candidates = Array.isArray(parsed) ? parsed : [parsed];
  return candidates.filter(isElementTemplate);
}

/** Collects the paths of every `.json` file under any `COMPONENT_DIRS` entry,
 *  grouped by the precedence order of `COMPONENT_DIRS` (index 0 = lowest). */
function componentFilePaths(files: FileNode[]): string[][] {
  const byDir: string[][] = COMPONENT_DIRS.map(() => []);
  const walk = (nodes: FileNode[]) => {
    for (const node of nodes) {
      if (node.kind === "dir") {
        if (node.children) walk(node.children);
        continue;
      }
      if (!node.path.endsWith(".json")) continue;
      COMPONENT_DIRS.forEach((dir, i) => {
        if (
          node.path === `${dir}/${node.name}` ||
          node.path.startsWith(`${dir}/`)
        ) {
          byDir[i].push(node.path);
        }
      });
    }
  };
  walk(files);
  // Deterministic order within a dir so "last wins" is stable across loads.
  return byDir.map((paths) => [...paths].sort());
}

/** Merges templates by `id` in `COMPONENT_DIRS` precedence order: a later dir's
 *  template (and, within a dir, a later file's) overrides an earlier one sharing
 *  the same id. Preserves first-seen insertion order for the palette. */
export function mergeComponentsById(
  byDir: ElementTemplate[][],
): ElementTemplate[] {
  const byId = new Map<string, ElementTemplate>();
  for (const dir of byDir) {
    for (const tpl of dir) byId.set(tpl.id, tpl);
  }
  return [...byId.values()];
}

/**
 * Loads the open project's components: scans `COMPONENT_DIRS` in the already-
 * fetched `files` tree, reads + parses each `.json`, and merges by id. Reads run
 * in parallel; a file that fails to fetch or parse is skipped (best-effort — a
 * single bad component never blanks the whole palette). Returns `[]` when the
 * project defines no components, which correctly yields an empty component
 * palette (nothing installed).
 */
export async function loadProjectComponents(
  name: string,
  files: FileNode[],
): Promise<ElementTemplate[]> {
  const byDirPaths = componentFilePaths(files);
  const byDir = await Promise.all(
    byDirPaths.map(async (paths) => {
      const perFile = await Promise.all(
        paths.map(async (path) => {
          try {
            const f = await projectFileEx(name, path);
            return f.binary ? [] : parseComponentFile(f.text);
          } catch {
            return [];
          }
        }),
      );
      return perFile.flat();
    }),
  );
  return mergeComponentsById(byDir);
}

/**
 * Loads the components contributed by installed packs (ADR 0033 §4, increment 6),
 * flattened across every extension in the overview and deduped by template id.
 * These sit at the **lowest** precedence: `combineComponents` layers a project's
 * own components on top so a project can shadow/customise a pack's component.
 * Best-effort — a failed overview fetch or a non-template entry yields nothing.
 */
export async function loadPackComponents(): Promise<ElementTemplate[]> {
  let overview: Awaited<ReturnType<typeof getExtensions>>["data"];
  try {
    overview = (await getExtensions({ throwOnError: true })).data;
  } catch {
    return [];
  }
  const byId = new Map<string, ElementTemplate>();
  for (const ext of overview?.extensions ?? []) {
    for (const raw of ext.components ?? []) {
      if (isElementTemplate(raw)) byId.set(raw.id, raw);
    }
  }
  return [...byId.values()];
}

/**
 * Merges the built-in, pack-contributed and project-local component sets into
 * the final installed set for the modeler (ADR 0033 §4). Built-ins
 * (`builtinComponents.ts`, e.g. the MCP start event) are the base layer, so they
 * are always offered; pack components override them, and project components
 * override both on an id collision — the local project always wins, mirroring
 * how project files shadow installed defaults.
 */
export function combineComponents(
  pack: ElementTemplate[],
  project: ElementTemplate[],
): ElementTemplate[] {
  return mergeComponentsById([[...BUILTIN_COMPONENTS], pack, project]);
}

export {
  componentTaskType,
  filterEnabledPackComponents,
} from "./connectorPalette";
