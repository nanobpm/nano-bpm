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
    const out = execFileSync(join(root, 'scripts', 'wasm-cc-env.sh'), ['--require'], {
      env: baseEnv,
      encoding: 'utf8',
    }).trim();
    for (const pair of out.split(/\s+/)) {
      const i = pair.indexOf('=');
      env[pair.slice(0, i)] = pair.slice(i + 1);
    }
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
  const members = workspace.members ? [...workspace.members.matchAll(/"([^"]+)"/g)].map((m) => m[1]) : [];
  return {
    dir,
    name: pkg.name ? pkg.name.replace(/"/g, '') : null,
    features,
    lints: Object.fromEntries(Object.entries(lints).map(([k, v]) => [k, v.replace(/"/g, '').trim()])),
    members,
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
  const matrix = loadMatrix(root);
  const targetCfg = entry.target === 'host' ? matrix.hostCfg : matrix.targets[entry.target];
  if (!targetCfg) throw new Error(`clippy-matrix: entry ${entry.id}: unknown target ${entry.target}`);
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
  const out = [];
  const text = stripComments(source);
  // Attribute STARTS are located with strings blanked too, so `#[cfg(…)]`
  // text inside a string literal (test fixtures, codegen) is not an attribute;
  // the predicate is then read from `text`, which keeps its own `"…"` values.
  const code = stripComments(source, { strings: true });
  // Unanchored: attributes may follow other attributes or code on the same
  // line (`#[inline] #[cfg(windows)] fn f() {}`, `struct S { #[cfg(x)] a: u8 }`).
  const re = /#!?\[\s*cfg(_attr)?\s*\(/g;
  let m;
  while ((m = re.exec(code))) {
    let depth = 1;
    let i = m.index + m[0].length;
    const start = i;
    let end = -1;
    for (; i < text.length && depth > 0; i++) {
      const ch = text[i];
      if (ch === '(') depth++;
      else if (ch === ')') depth--;
      else if (ch === ',' && depth === 1 && m[1] && end < 0) end = i;
    }
    if (depth !== 0) continue;
    out.push(text.slice(start, m[1] ? (end < 0 ? i - 1 : end) : i - 1).replace(/\s+/g, ' ').trim());
  }
  return out;
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
 * Can predicate `pred` be TRUE under lint config `conf`? Only the atoms in
 * FREE_CFG_KEYS (plus `test` on an all-targets run) are free: the predicate is
 * reachable if SOME assignment of them makes it true. Every OTHER key the
 * target config does not define (an unmodelled target key, a custom
 * `--cfg`) is FALSE — fail closed, so unknown cfgs surface as gaps instead of
 * being silently counted as linted.
 */
/** cfg atoms that genuinely vary across the lint runs' builds (debug vs release, rustdoc). */
export const FREE_CFG_KEYS = new Set(['debug_assertions', 'doc']);

export function reachable(pred, conf) {
  const ast = typeof pred === 'string' ? parsePred(pred) : pred;
  const free = new Set();
  const decide = (a) => {
    if (a.key === 'feature') return conf.features.has(a.value);
    if (a.key === 'test') return conf.test;
    if (a.key in conf.cfg) {
      const v = conf.cfg[a.key];
      return a.value === undefined ? Boolean(v) : v === a.value;
    }
    if (FREE_CFG_KEYS.has(a.key) && a.value === undefined) return null;
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
      const counts = new Map();
      for (const pred of cfgPredicates(text)) counts.set(pred, (counts.get(pred) ?? 0) + 1);
      for (const [pred, count] of counts) {
        const key = `${path}\0${pred}`;
        const allowed = unlinted.get(key);
        unlinted.delete(key);
        if (mine.some((c) => reachable(pred, c))) {
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
