// Shared helpers for the previous-release determinism divergence allowlist
// (engine-core/examples/determinism/divergence-allowlist.json, #1698) — the
// root-cause fix for #1801.
//
// Two facts about an entry's `introduced_in` are DERIVED, never hand-guessed:
//
//   * The only valid value for a NEW entry is the NEXT gateway release — the
//     version the in-flight release PR will cut. #1528's entries were
//     hand-labelled `v0.0.29` but shipped in v0.0.28, and the append-only
//     upgrade-boundaries.json ledger recorded the spurious 0.0.29 boundary
//     (it forbids edits, so the wrong boundary is permanent). Deriving the
//     value from the latest tag + the pending version bump makes a mislabel
//     a CI failure instead of a ledger entry.
//   * An entry EXPIRES once the previous release reaches `introduced_in`
//     (compare.rs `check_expiry` fails the gate on stale entries). The set
//     to prune at tag `vX` is exactly `introduced_in <= vX` — the same
//     comparison the gate runs — so the release itself can compute it.

/**
 * `vMAJOR.MINOR.PATCH` (leading `v` optional) as a comparable triple, or null.
 * Mirrors compare.rs `parse_release`.
 */
import { execFileSync } from "node:child_process";

export function parseRelease(tag) {
  const m = /^v?(\d+)\.(\d+)\.(\d+)$/.exec(String(tag).trim());
  if (!m) return null;
  return [Number(m[1]), Number(m[2]), Number(m[3])];
}

/** -1 / 0 / +1 by SemVer precedence over the numeric triple. */
export function cmpRelease(a, b) {
  for (let i = 0; i < 3; i++) {
    if (a[i] !== b[i]) return a[i] < b[i] ? -1 : 1;
  }
  return 0;
}

/** The highest `vX.Y.Z` tag in `tags` (an array of tag names), or null. */
export function latestGatewayTag(tags) {
  let best = null;
  for (const t of tags) {
    const v = parseRelease(t);
    if (v && /^v/.test(t) && (best === null || cmpRelease(v, best.v) > 0)) {
      best = { tag: t, v };
    }
  }
  return best;
}

/**
 * The gateway version a release PR will cut next: the workspace version when
 * it is ahead of the latest tag (a bump is pending), else the next patch.
 * `workspaceVersion` is server/Cargo.toml's [package] version; `tags` are the
 * remote tag names.
 */
export function nextGatewayRelease({ workspaceVersion, tags }) {
  const ws = parseRelease(workspaceVersion);
  if (!ws) throw new Error(`cannot parse gateway workspace version: ${workspaceVersion}`);
  const latest = latestGatewayTag(tags);
  if (latest && cmpRelease(ws, latest.v) <= 0) {
    return `v${latest.v[0]}.${latest.v[1]}.${latest.v[2] + 1}`;
  }
  return `v${ws[0]}.${ws[1]}.${ws[2]}`;
}

/** Every `introduced_in` in the allowlist, as `{what, introduced_in}`. */
export function allowlistEntries(allowlist) {
  const obs = (allowlist.observational_variants ?? []).map((o) => ({
    what: `observational variant ${o.variant}`,
    introduced_in: o.introduced_in,
  }));
  const div = (allowlist.divergences ?? []).map((d) => ({
    what: `scenario ${d.scenario} divergence`,
    introduced_in: d.introduced_in,
  }));
  return [...obs, ...div];
}

/**
 * Fails (returns an error string) when any entry's `introduced_in` is not
 * exactly `expected` — the next gateway release. Returns null when clean.
 * Entries with an unparseable `introduced_in` are reported too (compare.rs
 * would fail on them at gate time; catch them earlier).
 */
export function checkIntroducedIn(allowlist, expected) {
  const errors = [];
  for (const { what, introduced_in } of allowlistEntries(allowlist)) {
    if (!parseRelease(introduced_in)) {
      errors.push(`allowlist ${what}: introduced_in ${JSON.stringify(introduced_in)} is not a release tag`);
    } else if (introduced_in !== expected) {
      errors.push(
        `allowlist ${what}: introduced_in is ${introduced_in}, but the next gateway release is ${expected} — ` +
          `derive it, don't guess it (a mislabel becomes a spurious append-only upgrade-boundaries.json entry, #1801)`,
      );
    }
  }
  return errors.length ? errors.join("\n") : null;
}

/**
 * The allowlist with every entry whose `introduced_in <= tag` dropped — the
 * exact set the determinism gate's `check_expiry` declares STALE once `tag`
 * is the previous release. Returns `{pruned, removed}` where `removed` lists
 * the dropped `{what, introduced_in}` entries (for the PR body). Entries with
 * an unparseable `introduced_in` are KEPT (fail closed: a human must look).
 */
export function pruneAllowlist(allowlist, tag) {
  const cut = parseRelease(tag);
  if (!cut) throw new Error(`not a release tag: ${tag}`);
  const keep = (entry, what) => {
    const v = parseRelease(entry.introduced_in);
    if (v && cmpRelease(v, cut) <= 0) {
      removed.push({ what, introduced_in: entry.introduced_in });
      return false;
    }
    return true;
  };
  const removed = [];
  const pruned = {
    observational_variants: (allowlist.observational_variants ?? []).filter((o) =>
      keep(o, `observational variant ${o.variant}`),
    ),
    divergences: (allowlist.divergences ?? []).filter((d) => keep(d, `scenario ${d.scenario} divergence`)),
  };
  return { pruned, removed };
}

/**
 * The repository's release tag names — the single tag source for every
 * `nextGatewayRelease` caller (the CI gate AND its repo-state test), so a
 * shallow checkout without local tags can't make them disagree. Asks the
 * remote first (authoritative, independent of checkout depth); falls back to
 * local tags only when the remote is unreachable.
 */
export function releaseTagNames(root) {
  try {
    return execFileSync("git", ["ls-remote", "--tags", "origin"], { encoding: "utf8", cwd: root, stdio: ["ignore", "pipe", "pipe"] })
      .split("\n")
      .map((l) => l.match(/refs\/tags\/(.+?)(\^\{\})?$/)?.[1])
      .filter(Boolean);
  } catch {
    return execFileSync("git", ["tag", "--list", "v*"], { encoding: "utf8", cwd: root })
      .split("\n")
      .filter(Boolean);
  }
}
