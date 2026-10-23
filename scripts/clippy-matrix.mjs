#!/usr/bin/env node
// The Clippy gate matrix runner + coverage model (#1368).
//
// scripts/clippy-matrix.json is the ONE place the per-crate / per-feature /
// per-target Clippy permutations live. This runner turns an entry into its
// `cargo clippy … -- -D warnings` invocation; `make clippy-matrix` (local) and
// every Clippy step in .github/workflows/ci.yml call it, so neither duplicates
// the permutation list. scripts/clippy-matrix.test.mjs uses the exported model
// below to prove the matrix covers every production crate, declared feature and
// #[cfg] predicate.
//
// Usage:
//   node scripts/clippy-matrix.mjs list                 # print every command
//   node scripts/clippy-matrix.mjs run                  # run every entry
//   node scripts/clippy-matrix.mjs run --job <ci-job>   # entries of one CI job
//   node scripts/clippy-matrix.mjs run --id <id> [--id <id>…]
import { execFileSync, spawnSync } from 'node:child_process';
import { existsSync, readdirSync, readFileSync, statSync } from 'node:fs';
import { dirname, join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';

export const ROOT = join(dirname(fileURLToPath(import.meta.url)), '..');

export function loadMatrix(root = ROOT) {
  return JSON.parse(readFileSync(join(root, 'scripts', 'clippy-matrix.json'), 'utf8'));
}

/** The argv (after `cargo`) for one matrix entry. */
export function clippyArgs(entry) {
  const args = ['clippy'];
  if (entry.workspace) args.push('--workspace');
  if (entry.scope === 'lib') args.push('--lib');
  else if (entry.scope === 'all-targets') args.push('--all-targets');
  else throw new Error(`clippy-matrix: entry ${entry.id}: unknown scope ${entry.scope}`);
  if (entry.noDefaultFeatures) args.push('--no-default-features');
  if (entry.features?.length) args.push('--features', entry.features.join(','));
  if (entry.target !== 'host') args.push('--target', entry.target);
  args.push('--', '-D', 'warnings');
  return args;
}

/** Env for an entry: its own `env` plus, for `wasmCc`, scripts/wasm-cc-env.sh. */
export function entryEnv(entry, root = ROOT, baseEnv = process.env) {
  const env = { ...baseEnv, ...(entry.env ?? {}) };
  if (entry.wasmCc) {
    // --json is lossless: a value such as `sccache clang` or a path with
    // spaces survives intact (a whitespace split would truncate it).
    const out = execFileSync(join(root, 'scripts', 'wasm-cc-env.sh'), ['--require', '--json'], {
      env: baseEnv,
      encoding: 'utf8',
    });
    Object.assign(env, JSON.parse(out));
  }
  return env;
}

export function describe(entry) {
  const env = [
    ...Object.entries(entry.env ?? {}).map(([k, v]) => `${k}=${v}`),
    ...(entry.wasmCc ? ['$(scripts/wasm-cc-env.sh)'] : []),
  ];
  return `(cd ${entry.dir} && ${[...env, 'cargo', ...clippyArgs(entry)].join(' ')})`;
}

// ---------------------------------------------------------------------------
// Coverage model
// ---------------------------------------------------------------------------

const SKIP_DIRS = new Set(['target', 'node_modules', '.git', 'pkg', 'dist']);

function walk(dir, pred, out = []) {
  for (const name of readdirSync(dir)) {
    if (SKIP_DIRS.has(name) || name.startsWith('.')) continue;
    const p = join(dir, name);
    const st = statSync(p);
    if (st.isDirectory()) walk(p, pred, out);
    else if (pred(name)) out.push(p);
  }
  return out;
}

/** Minimal TOML section reader: the `key = value` lines of `[section]`. */
function tomlSection(text, section) {
  const lines = text.split('\n');
  const out = {};
  let inside = false;
  let key = null;
  for (const raw of lines) {
    const line = raw.replace(/\s+#.*$/, '').trimEnd();
    if (/^\s*#/.test(raw)) continue;
    const header = line.match(/^\s*\[([^\]]+)\]\s*$/);
    if (header) {
      inside = header[1].trim() === section;
      key = null;
      continue;
    }
    if (!inside) continue;
    const kv = line.match(/^\s*("?[A-Za-z0-9_-]+"?)\s*=\s*(.*)$/);
    if (kv) {
      key = kv[1].replace(/"/g, '');
      out[key] = kv[2];
    } else if (key && line.trim()) {
      out[key] += ` ${line.trim()}`;
    }
  }
  return out;
}

/** Parse one crate's Cargo.toml into the facts the guard needs. */
export function readCrate(root, dir) {
  const text = readFileSync(join(root, dir, 'Cargo.toml'), 'utf8');
  const pkg = tomlSection(text, 'package');
  const feats = tomlSection(text, 'features');
  const features = {};
  for (const [k, v] of Object.entries(feats)) {
    features[k] = [...v.matchAll(/"([^"]+)"/g)].map((m) => m[1]);
  }
  const lints = tomlSection(text, 'lints.clippy');
  const workspace = tomlSection(text, 'workspace');
  // Path dependencies' requested features: Cargo unifies them into every build
  // that links both crates, so they are on in every run that lints the dep.
  const deps = [];
  for (const m of text.matchAll(/^\s*\[((?:target\.([^\]]*)\.)?(dev-|build-)?dependencies)\]\s*$/gm)) {
    // `[target.'cfg(P)'.dependencies]` / `[target.TRIPLE.dependencies]`: the
    // dependency only exists (and only unifies features) on matching targets.
    const t = m[2]?.trim().replace(/^['"]|['"]$/g, '');
    const target = t === undefined ? null : t.startsWith('cfg(') ? { cfg: t.slice(4, -1) } : { triple: t };
    for (const v of Object.values(tomlSection(text, m[1]))) {
      const path = v.match(/\bpath\s*=\s*"([^"]+)"/);
      if (!path || /\boptional\s*=\s*true/.test(v)) continue;
      const fl = v.match(/\bfeatures\s*=\s*\[([^\]]*)\]/);
      deps.push({
        dir: join(dir, path[1]),
        dev: m[3] === 'dev-',
        target,
        features: [
          ...(/\bdefault-features\s*=\s*false/.test(v) ? [] : ['default']),
          ...(fl ? [...fl[1].matchAll(/"([^"]+)"/g)].map((x) => x[1]) : []),
        ],
      });
    }
  }
  const members = workspace.members ? [...workspace.members.matchAll(/"([^"]+)"/g)].map((m) => m[1]) : [];
  return {
    dir,
    name: pkg.name ? pkg.name.replace(/"/g, '') : null,
    features,
    lints: Object.fromEntries(Object.entries(lints).map(([k, v]) => [k, v.replace(/"/g, '').trim()])),
    members,
    deps,
    sources: walk(join(root, dir), (n) => n.endsWith('.rs'))
      .filter((p) => !relative(join(root, dir), p).startsWith('crates/'))
      .map((p) => relative(root, p)),
  };
}

/** Every Cargo package in the repo (dirs relative to root). Tracked files only
 * (gitignored codegen output such as generated/ is not production source);
 * falls back to a directory walk outside a git checkout (test fixtures). */
export function discoverCrates(root = ROOT) {
  let manifests;
  try {
    manifests = execFileSync('git', ['ls-files', '--', 'Cargo.toml', '**/Cargo.toml'], {
      cwd: root,
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'ignore'],
    })
      .split('\n')
      .filter(Boolean)
      .map((p) => join(root, p));
  } catch {
    manifests = walk(root, (n) => n === 'Cargo.toml');
  }
  return manifests
    .map((p) => relative(root, dirname(p)))
    .filter((dir) => /^\s*\[package\]/m.test(readFileSync(join(root, dir, 'Cargo.toml'), 'utf8')))
    .sort();
}

/** Expand a workspace entry's member globs (`crates/*`) to crate dirs. */
function workspaceMembers(root, crate) {
  return crate.members.flatMap((m) => {
    if (!m.endsWith('/*')) return [join(crate.dir, m)];
    const base = join(crate.dir, m.slice(0, -2));
    return readdirSync(join(root, base))
      .map((n) => join(base, n))
      .filter((d) => existsSync(join(root, d, 'Cargo.toml')));
  });
}

/**
 * Per entry: the set of crates it lints, and for each the enabled feature set
 * (defaults + explicit, closed over `feat = ["other", "pkg/feat"]`).
 */
export function entryConfigs(entry, crates, root = ROOT) {
  const byDir = new Map(crates.map((c) => [c.dir, c]));
  const byName = new Map(crates.map((c) => [c.name, c]));
  const rootCrate = byDir.get(entry.dir) ?? readCrate(root, entry.dir);
  const linted = [rootCrate.dir];
  if (entry.workspace) linted.push(...workspaceMembers(root, rootCrate));
  const matrix = loadMatrix(root);
  const targetCfg = entry.target === 'host' ? matrix.hostCfg : matrix.targets[entry.target];
  if (!targetCfg) throw new Error(`clippy-matrix: entry ${entry.id}: unknown target ${entry.target}`);
  // Fail closed: a target-specific dependency counts only when its condition
  // provably holds for this entry's target (an unknown triple never does).
  const depActive = (t) =>
    !t ||
    (t.cfg !== undefined
      ? reachable(t.cfg, { features: new Set(), test: false, cfg: targetCfg })
      : t.triple === (entry.target === 'host' ? matrix.hostTriple : entry.target));
  const enabled = new Map(linted.map((d) => [d, new Set()]));
  const queue = [];
  const enable = (dir, feat) => {
    if (!enabled.has(dir)) return; // dependency outside the linted set: Clippy does not lint it
    const set = enabled.get(dir);
    if (set.has(feat)) return;
    set.add(feat);
    queue.push([dir, feat]);
  };
  if (!entry.noDefaultFeatures) for (const d of linted) enable(d, 'default');
  for (const f of entry.features ?? []) {
    const [pkg, feat] = f.includes('/') ? f.split('/') : [rootCrate.name, f];
    const c = byName.get(pkg);
    if (!c) throw new Error(`clippy-matrix: entry ${entry.id}: unknown package in feature ${f}`);
    enable(c.dir, feat);
  }
  // Feature unification: a linted crate's (non-optional) path dependency on
  // another linted crate forces the requested features on (dev-dependencies
  // only when test targets are built).
  for (const d of linted) {
    for (const dep of byDir.get(d)?.deps ?? []) {
      if (dep.dev && entry.scope !== 'all-targets') continue;
      if (!depActive(dep.target)) continue;
      for (const f of dep.features) enable(dep.dir, f);
    }
  }
  while (queue.length) {
    const [dir, feat] = queue.shift();
    const crate = byDir.get(dir);
    for (const imp of crate?.features[feat] ?? []) {
      if (imp.startsWith('dep:')) continue;
      if (imp.includes('/')) {
        const [pkg, f] = imp.replace('?', '').split('/');
        const c = byName.get(pkg);
        if (c) enable(c.dir, f);
      } else enable(dir, imp);
    }
  }
  return linted.map((dir) => ({
    dir,
    features: enabled.get(dir),
    cfg: targetCfg,
    test: entry.scope === 'all-targets' ? null : false, // null = both (lib + test builds)
  }));
}

// --- #[cfg] predicate parsing + evaluation --------------------------------

/** Every `#[cfg(P)]` / `#[cfg_attr(P, …)]` / `#![cfg(P)]` predicate P in `text`. */
export function cfgPredicates(source) {
  return cfgSites(source).sites.map((s) => s.pred);
}

/** Split `src` at top-level commas (outside `(…)`, `[…]` and `"…"`). */
function splitTop(src) {
  const out = [];
  let d = 0;
  let start = 0;
  for (let i = 0; i < src.length; i++) {
    const c = src[i];
    if (c === '"') {
      for (i++; i < src.length && src[i] !== '"'; i++) if (src[i] === '\\') i++;
    } else if (c === '(' || c === '[') d++;
    else if (c === ')' || c === ']') d--;
    else if (c === ',' && d === 0) {
      out.push(src.slice(start, i));
      start = i + 1;
    }
  }
  out.push(src.slice(start));
  return out.map((x) => x.trim()).filter(Boolean);
}

/**
 * The item-inclusion condition a `cfg_attr(P, attrs…)` argument list imposes:
 * each `cfg(Q)` in `attrs` (also inside nested `cfg_attr`) excludes the item
 * unless `Q` holds whenever `P` does, i.e. contributes `any(not(P), Q)`.
 * `undefined` when the payload carries no `cfg`.
 */
export function cfgAttrGate(args) {
  const [p, ...attrs] = splitTop(args);
  const parts = [];
  for (const a of attrs) {
    const m = a.match(/^(cfg|cfg_attr)\s*\(([\s\S]*)\)$/);
    if (!m) continue;
    const q = m[1] === 'cfg' ? m[2].trim() : cfgAttrGate(m[2]);
    if (q !== undefined) parts.push(`any(not(${p}), ${q})`);
  }
  return andPred(parts);
}

const MOD_DECL = /^\s*(?:pub(?:\s*\([^)]*\))?\s+)?mod\s+([A-Za-z_][A-Za-z0-9_]*)\s*;/;

const andPred = (parts) => {
  const p = parts.filter((x) => x !== undefined);
  return p.length === 0 ? undefined : p.length === 1 ? p[0] : `all(${p.join(', ')})`;
};

/**
 * Every `#[cfg]` / `#[cfg_attr]` site in `source`, each with `gate`: the
 * COMBINED condition under which that site is compiled — its own predicate
 * AND every sibling `#[cfg]` stacked on the same item AND every enclosing
 * gated scope (`#[cfg(x)] mod m { … }`, `#![cfg(x)]`). Coverage is decided on
 * `gate`, so `#[cfg(a)] #[cfg(b)]` on one item is `all(a, b)`, not two
 * independently-reachable atoms. `mods` lists out-of-line `mod name;`
 * declarations with their gate, so the caller can propagate it to the child
 * file. `#[cfg_attr(p, …)]` normally gates only the attribute, so its own
 * predicate is never added to the item's gate — but a `cfg(q)` in its payload
 * (directly or via nested `cfg_attr`) adds `any(not(p), q)` (see cfgAttrGate).
 */
export function cfgSites(source) {
  const text = stripComments(source);
  // Attribute starts / item structure are scanned with strings + char
  // literals blanked; predicates are read from `text`, which keeps `"…"`.
  const code = stripComments(source, { strings: true });
  const sites = [];
  const mods = [];
  // brace/paren/bracket stack; `list` scopes (struct/enum/union bodies, `(…)`,
  // `[…]`) are the only ones where a `,` separates items.
  const scopes = [{ close: null, gate: undefined, list: false }];
  let angle = 0; // `<…>` generics depth at the current item's own level
  let pending = []; // { pred, site? } cfgs on the not-yet-started item
  let pendingAttrOnly = []; // cfg_attr sites awaiting the item's gate
  let pendingDepth = 0;
  const scopeGate = () => andPred(scopes.map((s) => s.gate));
  const flush = (item) => {
    const own = andPred(pending.map((p) => p.pred));
    const gate = andPred([scopeGate(), own]);
    for (const p of pending) if (p.site) p.site.gate = gate;
    for (const s of pendingAttrOnly) s.gate = andPred([gate, s.pred]);
    if (item) {
      const m = item.match(MOD_DECL);
      if (m) mods.push({ name: m[1], gate });
    }
    pending = [];
    pendingAttrOnly = [];
    return own;
  };
  const OPEN = { '{': '}', '(': ')', '[': ']' };
  let itemStart = 0;
  let i = 0;
  while (i < code.length) {
    const rest = code.slice(i, i + 64);
    const attr = rest.match(/^#(!?)\[/);
    if (attr) {
      // Find the attribute's closing bracket.
      let depth = 1;
      let j = i + attr[0].length;
      for (; j < code.length && depth > 0; j++) {
        if (code[j] === '[') depth++;
        else if (code[j] === ']') depth--;
      }
      const inner = code.slice(i + attr[0].length, j - 1);
      const cm = inner.match(/^\s*cfg(_attr)?\s*\(/);
      if (cm) {
        const from = i + attr[0].length + cm[0].length;
        let d = 1;
        let k = from;
        let comma = -1;
        for (; k < j && d > 0; k++) {
          if (text[k] === '(') d++;
          else if (text[k] === ')') d--;
          else if (text[k] === ',' && d === 1 && cm[1] && comma < 0) comma = k;
        }
        if (d === 0) {
          const pred = text.slice(from, cm[1] ? (comma < 0 ? k - 1 : comma) : k - 1).replace(/\s+/g, ' ').trim();
          const site = { pred, gate: undefined };
          sites.push(site);
          // A `cfg_attr` whose payload is (or nests) a `cfg` gates the item too.
          const attrGate = cm[1] ? cfgAttrGate(text.slice(from, k - 1)) : undefined;
          if (attr[1]) {
            // Inner attribute: gates the enclosing scope (or the whole file).
            const top = scopes[scopes.length - 1];
            top.gate = andPred([top.gate, cm[1] ? attrGate : pred]);
            site.gate = andPred([scopeGate(), cm[1] ? pred : undefined]);
          } else {
            if (pending.length + pendingAttrOnly.length === 0) pendingDepth = scopes.length;
            if (cm[1]) {
              pendingAttrOnly.push(site);
              if (attrGate !== undefined) pending.push({ pred: attrGate });
            } else pending.push({ pred, site });
          }
        }
      }
      i = j;
      itemStart = i; // the item text (e.g. `mod name;`) starts after its attributes
      continue;
    }
    const ch = code[i];
    const has = pending.length + pendingAttrOnly.length > 0;
    // Only punctuation at the item's own nesting level ends it: `fn f(a, b)`
    // or `[u8; 4]` inside the item must not flush its pending gate.
    const atItem = has && scopes.length === pendingDepth;
    if (atItem && ch === '<') angle++;
    else if (atItem && ch === '>' && code[i - 1] !== '-' && code[i - 1] !== '=' && angle > 0) angle--;
    if (OPEN[ch]) {
      const list = ch !== '{' || /\b(?:struct|enum|union)\b/.test(code.slice(itemStart, i));
      const own = ch === '{' && atItem ? flush(code.slice(itemStart, i)) : undefined;
      if (ch === '{' && atItem) angle = 0;
      scopes.push({ close: OPEN[ch], gate: own, list });
      if (!has || ch === '{') itemStart = i + 1;
    } else if (ch === '}' || ch === ')' || ch === ']') {
      if (has && scopes.length <= pendingDepth) {
        flush(code.slice(itemStart, i));
        angle = 0;
      }
      if (scopes.length > 1 && scopes[scopes.length - 1].close === ch) scopes.pop();
      if (!(pending.length + pendingAttrOnly.length)) itemStart = i + 1;
    } else if (ch === ';' || ch === ',') {
      // A `,` inside generics (`impl<A, B>`, `Foo<A, B>`) or a where-clause
      // (outside a list scope) does not end the item.
      const ends = ch === ';' || (scopes[scopes.length - 1].list && angle === 0);
      if (atItem && ends) {
        flush(code.slice(itemStart, i + 1));
        angle = 0;
      } else if (!has && ch === ';') {
        // An ungated `mod child;` still links child into the module tree, so
        // the enclosing scope's gate (and the file's) propagates through it.
        const m = code.slice(itemStart, i + 1).match(MOD_DECL);
        if (m) mods.push({ name: m[1], gate: scopeGate() });
      }
      if (!(pending.length + pendingAttrOnly.length)) itemStart = i + 1;
    }
    i++;
  }
  if (pending.length + pendingAttrOnly.length) flush(code.slice(itemStart));
  for (const s of sites) if (s.gate === undefined) s.gate = s.pred;
  return { sites, mods };
}

/**
 * The cfg gate each source file inherits from the `#[cfg(x)] mod name;`
 * declarations that include it (transitively), keyed by repo-relative path.
 * Files no declaration reaches (crate roots, bins, tests) get no gate.
 */
export function fileGates(files) {
  const byPath = new Map(files.map(([p, t]) => [p, cfgSites(t)]));
  const parent = new Map(); // child path -> { from, gate }
  for (const [p, { mods }] of byPath) {
    const base = p.replace(/\.rs$/, '');
    const dir = /\/(mod|lib|main)\.rs$/.test(p) ? p.replace(/\/[^/]+$/, '') : base;
    for (const m of mods) {
      for (const c of [`${dir}/${m.name}.rs`, `${dir}/${m.name}/mod.rs`]) {
        if (byPath.has(c)) parent.set(c, { from: p, gate: m.gate });
      }
    }
  }
  const memo = new Map();
  const gateOf = (p, seen = new Set()) => {
    if (memo.has(p)) return memo.get(p);
    const up = parent.get(p);
    let g;
    if (up && !seen.has(p)) {
      seen.add(p);
      g = andPred([gateOf(up.from, seen), up.gate]);
    }
    memo.set(p, g);
    return g;
  };
  return new Map([...byPath].map(([p, s]) => [p, { gate: gateOf(p), sites: s.sites }]));
}

/**
 * Blank out `//` and (nested) `/* … *\/` comments, preserving newlines, so
 * cfg attributes in doc/plain comments are ignored. String and raw-string
 * literals are skipped intact so `"//"` inside one does not start a comment
 * (or, with `strings: true`, blanked too). Offsets are preserved.
 */
export function stripComments(src, { strings = false } = {}) {
  let out = '';
  let i = 0;
  const n = src.length;
  const blank = (s) => s.replace(/[^\n]/g, ' ');
  while (i < n) {
    const ch = src[i];
    const raw = src.slice(i).match(/^b?r(#*)"/);
    if (raw && !/[A-Za-z0-9_]/.test(src[i - 1] ?? '')) {
      const close = `"${raw[1]}`;
      const end = src.indexOf(close, i + raw[0].length);
      const stop = end < 0 ? n : end + close.length;
      out += strings ? blank(src.slice(i, stop)) : src.slice(i, stop);
      i = stop;
    } else if (
      (ch === "'" || (ch === 'b' && src[i + 1] === "'" && !/[A-Za-z0-9_]/.test(src[i - 1] ?? ''))) &&
      /^b?'(?:\\(?:u\{[0-9A-Fa-f_]{1,8}\}|x[0-9A-Fa-f]{2}|.)|[^\\'\n])'/.test(src.slice(i, i + 16))
    ) {
      // A char / byte-char literal (`'"'`, `b'\''`) — skipped whole so its
      // quote never opens a phantom string. A lifetime (`'a`) has no closing
      // quote, so it falls through as plain code.
      const lit = src.slice(i, i + 16).match(/^b?'(?:\\(?:u\{[0-9A-Fa-f_]{1,8}\}|x[0-9A-Fa-f]{2}|.)|[^\\'\n])'/)[0];
      out += strings ? blank(lit) : lit;
      i += lit.length;
    } else if (ch === '"') {
      let j = i + 1;
      while (j < n && src[j] !== '"') j += src[j] === '\\' ? 2 : 1;
      out += strings ? blank(src.slice(i, j + 1)) : src.slice(i, j + 1);
      i = j + 1;
    } else if (ch === '/' && src[i + 1] === '/') {
      const end = src.indexOf('\n', i);
      const stop = end < 0 ? n : end;
      out += blank(src.slice(i, stop));
      i = stop;
    } else if (ch === '/' && src[i + 1] === '*') {
      let depth = 1;
      let j = i + 2;
      while (j < n && depth > 0) {
        if (src[j] === '/' && src[j + 1] === '*') (depth++, (j += 2));
        else if (src[j] === '*' && src[j + 1] === '/') (depth--, (j += 2));
        else j++;
      }
      out += blank(src.slice(i, j));
      i = j;
    } else {
      out += ch;
      i++;
    }
  }
  return out;
}

function parsePred(src) {
  let i = 0;
  const ws = () => {
    while (src[i] === ' ' || src[i] === ',') i++;
  };
  const node = () => {
    ws();
    const id = src.slice(i).match(/^[A-Za-z_][A-Za-z0-9_]*/);
    if (!id) throw new Error(`cfg parse error at ${i}: ${src}`);
    i += id[0].length;
    ws();
    if (src[i] === '(') {
      i++;
      const args = [];
      ws();
      while (src[i] !== ')') {
        args.push(node());
        ws();
        if (i >= src.length) throw new Error(`cfg parse error: ${src}`);
      }
      i++;
      return { op: id[0], args };
    }
    if (src[i] === '=') {
      i++;
      ws();
      const s = src.slice(i).match(/^"([^"]*)"/);
      if (!s) throw new Error(`cfg parse error (value) at ${i}: ${src}`);
      i += s[0].length;
      return { key: id[0], value: s[1] };
    }
    return { key: id[0] };
  };
  const n = node();
  ws();
  if (i !== src.length) throw new Error(`cfg parse error (trailing) : ${src}`);
  return n;
}

/**
 * Can predicate `pred` be TRUE under lint config `conf`? Only `test` on an
 * all-targets run is free (clippy --all-targets compiles both the lib and its
 * test harness): the predicate is reachable if SOME value of it makes it true.
 * Every other key takes the value the matrix's actual builds give it —
 * `debug_assertions` true (every entry is a dev-profile build), `doc` false (no
 * entry runs rustdoc) — unless the target config overrides it; any key the
 * config does not define (an unmodelled target key, a custom `--cfg`) is
 * FALSE. Fail closed: release-only, rustdoc-only and unknown-cfg code
 * surfaces as a gap instead of being counted as linted.
 */
export const DEFAULT_CFG = Object.freeze({ debug_assertions: true, doc: false });

export function reachable(pred, conf) {
  const ast = typeof pred === 'string' ? parsePred(pred) : pred;
  const free = new Set();
  const decide = (a) => {
    if (a.key === 'feature') return conf.features.has(a.value);
    if (a.key === 'test') return a.value === undefined ? conf.test : false;
    const cfg = { ...DEFAULT_CFG, ...conf.cfg };
    if (a.key in cfg) {
      const v = cfg[a.key];
      return a.value === undefined ? Boolean(v) : v === a.value;
    }
    return false;
  };
  const collect = (n) => {
    if (n.op) n.args.forEach(collect);
    else if (decide(n) === null) free.add(JSON.stringify(n));
  };
  collect(ast);
  const freeList = [...free];
  const ev = (n, assign) => {
    if (n.op === 'all') return n.args.every((a) => ev(a, assign));
    if (n.op === 'any') return n.args.some((a) => ev(a, assign));
    if (n.op === 'not') return !ev(n.args[0], assign);
    if (n.op) throw new Error(`unknown cfg operator ${n.op}`);
    const d = decide(n);
    return d === null ? assign.get(JSON.stringify(n)) : d;
  };
  for (let mask = 0; mask < 1 << freeList.length; mask++) {
    const assign = new Map(freeList.map((k, b) => [k, Boolean(mask & (1 << b))]));
    if (ev(ast, assign)) return true;
  }
  return false;
}

/**
 * The coverage guard. Returns a list of human-readable gaps (empty = covered).
 * `opts.workflow` is the ci.yml text; `opts.requiredJobs` maps ci.yml job keys
 * to whether their `name:` is a required check.
 */
export function coverageGaps({ root = ROOT, matrix = loadMatrix(root), crates, workflow, requiredJobNames }) {
  const gaps = [];
  const allDirs = crates ?? discoverCrates(root);
  const production = allDirs.filter((d) => !(d in matrix.excludedCrates));
  for (const d of Object.keys(matrix.excludedCrates)) {
    if (!allDirs.includes(d)) gaps.push(`excludedCrates lists ${d}, which is not a crate (stale exclusion)`);
  }
  const facts = production.map((d) => readCrate(root, d));
  const unlinted = new Map();
  for (const u of matrix.unlintedCfg ?? []) {
    if (!u.reason) gaps.push(`unlintedCfg ${u.file} #[cfg(${u.pred})] has no reason`);
    unlinted.set(`${u.file}\0${u.pred}`, u);
  }
  const ids = new Set();
  const configs = [];
  for (const e of matrix.entries) {
    if (ids.has(e.id)) gaps.push(`duplicate entry id ${e.id}`);
    ids.add(e.id);
    for (const c of entryConfigs(e, facts, root)) configs.push({ ...c, entry: e.id });
  }
  for (const crate of facts) {
    const mine = configs.filter((c) => c.dir === crate.dir);
    if (mine.length === 0) {
      gaps.push(`crate ${crate.dir} is linted by no clippy-matrix entry`);
      continue;
    }
    for (const lint of ['unwrap_used', 'expect_used']) {
      if (crate.lints[lint] !== 'deny') gaps.push(`crate ${crate.dir}: [lints.clippy] ${lint} is not "deny"`);
    }
    const exempt = matrix.exemptFeatures?.[crate.dir] ?? {};
    const texts = crate.sources.map((p) => [p, readFileSync(join(root, p), 'utf8')]);
    const gated = fileGates(texts);
    for (const f of Object.keys(crate.features)) {
      if (f === 'default') continue;
      if (f in exempt) {
        const used = texts.find(([, t]) => t.includes(`feature = "${f}"`));
        if (used) gaps.push(`crate ${crate.dir}: feature ${f} is exempt but ${used[0]} gates code on it`);
        continue;
      }
      if (!mine.some((c) => c.features.has(f))) gaps.push(`crate ${crate.dir}: feature "${f}" is enabled by no clippy-matrix entry`);
    }
    for (const [path, text] of texts) {
      // Keyed by the site's own predicate (the allowlist's unit); a pred is
      // covered only if EVERY one of its sites — under its combined gate
      // (sibling cfgs + enclosing scopes + the file's `mod` gate) — is.
      const counts = new Map();
      const { gate: fileGate, sites } = gated.get(path);
      for (const s of sites) {
        const e = counts.get(s.pred) ?? { count: 0, gates: [] };
        e.count++;
        e.gates.push(andPred([fileGate, s.gate]));
        counts.set(s.pred, e);
      }
      for (const [pred, { count, gates }] of counts) {
        const key = `${path}\0${pred}`;
        const allowed = unlinted.get(key);
        unlinted.delete(key);
        if (gates.every((g) => mine.some((c) => reachable(g, c)))) {
          if (allowed) gaps.push(`${path}: unlintedCfg entry #[cfg(${pred})] is now covered — remove it (stale)`);
          continue;
        }
        if (!allowed) {
          gaps.push(`${path}: #[cfg(${pred})] is compiled by no clippy-matrix entry for ${crate.dir}`);
        } else if (allowed.count !== count) {
          gaps.push(
            `${path}: #[cfg(${pred})] occurs ${count}x but unlintedCfg allows ${allowed.count}x — ` +
              (count > allowed.count ? 'new unlinted code: cover it with a matrix entry or cfg!()' : 'lower the count (ratchet)'),
          );
        }
      }
    }
  }
  for (const key of unlinted.keys()) {
    const [path, pred] = key.split('\0');
    gaps.push(`${path}: unlintedCfg entry #[cfg(${pred})] no longer exists — remove it (stale)`);
  }
  if (workflow !== undefined) {
    if (/cargo clippy/.test(workflow.replace(/^\s*#.*$/gm, ''))) {
      gaps.push('ci.yml runs `cargo clippy` directly — route it through scripts/clippy-matrix.mjs');
    }
    const jobs = parseJobs(workflow);
    for (const job of new Set(matrix.entries.map((e) => e.job))) {
      const j = jobs.get(job);
      if (!j) {
        gaps.push(`clippy-matrix job ${job} is not a ci.yml job`);
        continue;
      }
      if (!requiredJobNames.has(j.name)) gaps.push(`clippy-matrix job ${job} ("${j.name}") is not a required check`);
      if (!j.body.includes(`scripts/clippy-matrix.mjs run --job ${job}`)) {
        gaps.push(`ci.yml job ${job} does not run \`node scripts/clippy-matrix.mjs run --job ${job}\``);
      }
    }
    for (const [key, j] of jobs) {
      for (const m of j.body.matchAll(/clippy-matrix\.mjs run --job ([\w-]+)/g)) {
        if (m[1] !== key) gaps.push(`ci.yml job ${key} runs the clippy-matrix entries of job ${m[1]}`);
      }
    }
  }
  return gaps;
}

/** ci.yml `jobs:` → Map(key → { name, body }). Indentation-based, no YAML dep. */
export function parseJobs(workflow) {
  const jobs = new Map();
  const lines = workflow.split('\n');
  const start = lines.findIndex((l) => /^jobs:\s*$/.test(l));
  let cur = null;
  for (const line of lines.slice(start + 1)) {
    const k = line.match(/^ {2}([A-Za-z0-9_-]+):\s*$/);
    if (k) {
      cur = { name: k[1], body: '' };
      jobs.set(k[1], cur);
      continue;
    }
    if (/^\S/.test(line)) break;
    if (!cur) continue;
    if (/^\s*#/.test(line)) continue; // a commented-out step does not run
    const n = line.match(/^ {4}name:\s*['"]?(.*?)['"]?\s*$/);
    if (n) cur.name = n[1];
    cur.body += `${line}\n`;
  }
  return jobs;
}

/** Required check names from the AGENTS.md merge-protocol block. */
export function requiredCheckNames(root = ROOT) {
  const agents = readFileSync(join(root, 'AGENTS.md'), 'utf8');
  const block = agents.match(/```merge-protocol\s*\n([\s\S]*?)\n```/);
  return new Set(JSON.parse(block[1]).requiredChecks.map((c) => c.name));
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

function main(argv) {
  const [cmd, ...rest] = argv;
  const matrix = loadMatrix();
  const jobs = [];
  const idsWanted = [];
  for (let i = 0; i < rest.length; i++) {
    if (rest[i] === '--job') jobs.push(rest[++i]);
    else if (rest[i] === '--id') idsWanted.push(rest[++i]);
    else throw new Error(`clippy-matrix: unknown argument ${rest[i]}`);
  }
  const selected = matrix.entries.filter(
    (e) => (jobs.length === 0 || jobs.includes(e.job)) && (idsWanted.length === 0 || idsWanted.includes(e.id)),
  );
  if (selected.length === 0) throw new Error(`clippy-matrix: no entries match ${rest.join(' ')}`);
  if (cmd === 'list') {
    for (const e of selected) console.log(`${e.id}\t[${e.job}]\t${describe(e)}`);
    return 0;
  }
  if (cmd !== 'run') throw new Error('usage: clippy-matrix.mjs list|run [--job J] [--id I]');
  for (const e of selected) {
    console.log(`::group::clippy-matrix ${e.id}: ${describe(e)}`);
    const r = spawnSync('cargo', clippyArgs(e), { cwd: join(ROOT, e.dir), env: entryEnv(e), stdio: 'inherit' });
    console.log('::endgroup::');
    if (r.status !== 0) {
      console.error(`clippy-matrix: entry ${e.id} failed (exit ${r.status ?? r.signal})`);
      return r.status || 1;
    }
  }
  return 0;
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  try {
    process.exit(main(process.argv.slice(2)));
  } catch (err) {
    console.error(err.message);
    process.exit(2);
  }
}
