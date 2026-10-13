// Map a worker file name to a Monaco language id. Kept in its own module (free
// of any `monaco-editor` import) so views can call it without dragging the
// multi-MB Monaco bundle out of its lazy-loaded chunk.
//
// Two sources feed the mapping:
// - A static table below for the runtimes we've always shipped (Node/TS, Rust)
//   and the common companions we don't want to be silent about (Java, XML,
//   YAML, Python, Go, C/C++, Shell, Dockerfile). Monaco's default bundle
//   already ships tokenisers for all of these via basic-languages; the map
//   just wires the file extension to the language id.
// - Installed lang packs' `fileTypes[]`, registered at runtime by
//   `registerFileTypes()`. Views that fetch `/console/api/extensions` should
//   call this once so `.java` etc. light up as soon as the pack is loaded —
//   without it, editing a `.java` file falls back to "typescript" and Monaco
//   paints nonsense highlighting on Java source.

const STATIC_EXT_TO_LANG: Record<string, string> = {
  ".json": "json",
  ".lock": "json",
  ".js": "javascript",
  ".mjs": "javascript",
  ".cjs": "javascript",
  ".ts": "typescript",
  ".tsx": "typescript",
  ".rs": "rust",
  ".toml": "ini",
  ".md": "markdown",
  ".java": "java",
  ".xml": "xml",
  ".yaml": "yaml",
  ".yml": "yaml",
  ".py": "python",
  ".go": "go",
  ".c": "c",
  ".h": "c",
  ".cpp": "cpp",
  ".hpp": "cpp",
  ".sh": "shell",
  ".bash": "shell",
  ".html": "html",
  ".css": "css",
  ".scss": "scss",
  ".sql": "sql",
};

// Extensionless filenames Monaco already tokenises via basic-languages. Matched
// case-insensitively against the *basename* so paths like `services/Dockerfile`
// still resolve.
const STATIC_BASENAME_TO_LANG: Record<string, string> = {
  dockerfile: "dockerfile",
  containerfile: "dockerfile",
  makefile: "shell",
};

const dynamicExtToLang = new Map<string, string>();
const dynamicBasenameToLang = new Map<string, string>();

function basename(file: string): string {
  // Handle both POSIX and Windows separators; strip any trailing slash.
  const trimmed = file.replace(/[/\\]+$/, "");
  const sep = Math.max(trimmed.lastIndexOf("/"), trimmed.lastIndexOf("\\"));
  return sep >= 0 ? trimmed.slice(sep + 1) : trimmed;
}

/// Register file-type -> Monaco language mappings from installed extension
/// packs. Later calls override earlier ones for the same extension so a pack
/// can shadow a stale static default. `ext` is either a leading-dot suffix
/// (".java") or a full basename ("Dockerfile") for extensionless files.
export function registerFileTypes(
  fileTypes: ReadonlyArray<{ ext: string; monacoLang: string }>,
): void {
  for (const ft of fileTypes) {
    if (!ft?.ext || !ft?.monacoLang) continue;
    if (ft.ext.startsWith(".")) {
      dynamicExtToLang.set(ft.ext.toLowerCase(), ft.monacoLang);
    } else if (ft.ext.includes(".")) {
      // Bare "foo.bar" — treat as an extension.
      dynamicExtToLang.set(`.${ft.ext.toLowerCase()}`, ft.monacoLang);
    } else {
      // No dot at all — a basename match (e.g. "Dockerfile").
      dynamicBasenameToLang.set(ft.ext.toLowerCase(), ft.monacoLang);
    }
  }
}

/// Absorb a fresh ExtensionsOverview into the Monaco ext->language map. Called
/// at App boot and again after any install/remove in the Extensions view so
/// newly contributed file types take effect without a full page reload.
export function registerFileTypesFromOverview(ov: {
  extensions: ReadonlyArray<{
    fileTypes?: ReadonlyArray<{ ext: string; monacoLang: string }>;
  }>;
}): void {
  for (const e of ov.extensions) {
    if (e.fileTypes?.length) registerFileTypes(e.fileTypes);
  }
}

export function languageForFile(file: string): string {
  const name = basename(file);
  const lower = name.toLowerCase();
  const dot = name.lastIndexOf(".");
  if (dot > 0) {
    // dot > 0 (not >= 0) so dotfiles like `.env` fall through to the basename
    // table rather than being treated as extension "".
    const ext = lower.slice(dot);
    const dyn = dynamicExtToLang.get(ext);
    if (dyn) return dyn;
    const stat = STATIC_EXT_TO_LANG[ext];
    if (stat) return stat;
  }
  const dynBase = dynamicBasenameToLang.get(lower);
  if (dynBase) return dynBase;
  const statBase = STATIC_BASENAME_TO_LANG[lower];
  if (statBase) return statBase;
  // Files without any known extension fall through to TypeScript because the
  // console's original worker surface was TypeScript-only and unmarked files
  // were always TS. Kept for back-compat; anything with a real extension or a
  // recognised basename is routed via the tables above.
  return "typescript";
}
