//! Dependency-direction guard: freezes `engine-core`'s single-direction module
//! layering so the big splits that follow (epic #1201, slices 5–8) cannot
//! silently reintroduce a cycle or a backward import.
//!
//! Rust happily allows intra-crate cycles — nothing in the compiler stops
//! `model` from importing `engine`, or `event` from importing `state::apply`
//! again. The layering is therefore held by a checked-in table plus this test,
//! exactly like the repo's other structural guards (golden serde-drift,
//! `console/scripts/merge-gates.test.mjs`, the `processos` spec-parity test):
//! the table below is the **single source of truth**, and CI fails on any edge
//! that does not respect it.
//!
//! ## The table (mirrors `docs/engine-architecture-analysis.md` §3)
//!
//! ```text
//! L0  model  xml  json  read_query  temporal      ← zero internal deps
//! L1  feel → model         dmn → feel, model, xml
//! L2  state → model        (state::apply additionally → event)
//! L3  event → model, state::types
//! L4  command · agent · lease → L0..L3
//! L5  validate → model      bpmn → model, xml, validate
//! L6  engine → L0..L5       ffi → engine, bpmn, json
//! ```
//!
//! A `use crate::<module>` import from a module at level *Ls* to one at level
//! *Lt* is allowed when `Lt < Ls` (strictly lower), plus three intra-level
//! edges the layering deliberately permits (`dmn → feel`, `bpmn → validate`,
//! `ffi → engine`) and the two cycle-breaking special cases that slice 3
//! established:
//!
//! * `state::apply` (and *only* `apply`) may import `event`; and
//! * `event` may import `state::types` (and *only* `types`, never bare `state`
//!   or `state::apply`).
//!
//! ## What it inspects, and what it does not
//!
//! It walks `src/**.rs`, ignores `#[cfg(test)]` modules and test-only files
//! (including feature-gated `#[cfg(all(test, feature = "…"))]` modules), strips
//! comments and literals, and extracts
//! module-level `use crate::<module>[::<sub>]` edges. The extractor parses the
//! full use-tree after each `use crate::`, so brace use-trees
//! (`use crate::{event::Event, state::apply}`, expanded entry-by-entry) and
//! paths wrapped across newlines are handled — no common import form can slip a
//! backward edge past the guard. Inline fully-qualified paths
//! (`crate::feel::eval_bool` used *without* a `use`) are out of scope: that is
//! the idiom modules use to reach a helper *without* declaring a layering
//! dependency (see `model.rs`). A leaf whose first segment is *not* a top-level
//! module — a crate-root re-export such as `use crate::Engine` — names the
//! crate's public surface rather than a module and is ignored. That
//! module-membership filter is only safe because [`MODULES`] is held in
//! lock-step with the modules on disk by [`modules_table_matches_src_tree`], so
//! a newly added module can never be an unlisted first segment that silently
//! escapes the guard before its layer is assigned.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "non-production target (integration tests / examples); the production deny-unwrap gate (#1357) applies to the library crate only"
)]
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

/// Every top-level module in `engine-core/src`. Anything not in this set that
/// follows `use crate::` (e.g. a crate-root re-export like `crate::Engine`) is
/// not a module edge and is ignored.
const MODULES: &[&str] = &[
    "model",
    "xml",
    "json",
    "read_query",
    "temporal",
    "feel",
    "dmn",
    "cluster_vars",
    "state",
    "event",
    "command",
    "agent",
    "lease",
    "validate",
    "bpmn",
    "engine",
    "ffi",
];

/// The layer of each module (the allowed-edges table's spine). `None` is
/// returned for a non-module, which the caller never looks up.
fn module_level(module: &str) -> Option<u32> {
    Some(match module {
        "model" | "xml" | "json" | "read_query" | "temporal" => 0,
        "feel" | "dmn" | "cluster_vars" => 1,
        "state" => 2,
        "event" => 3,
        "command" | "agent" | "lease" => 4,
        "validate" | "bpmn" => 5,
        "engine" | "ffi" => 6,
        _ => return None,
    })
}

/// The only intra-level (`Lt == Ls`) edges the layering permits. Every other
/// same-level or upward edge is a violation unless it is one of the two special
/// cases handled in [`edge_allowed`].
const SAME_LEVEL_ALLOWED: &[(&str, &str)] =
    &[("dmn", "feel"), ("bpmn", "validate"), ("ffi", "engine")];

/// A single `use crate::<tgt_module>[::<tgt_sub>]` edge declared in a source
/// file belonging to `src_module` (with `src_sub` set for sub-moduled files
/// such as `state/apply.rs`).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Edge {
    src_module: String,
    src_sub: Option<String>,
    tgt_module: String,
    tgt_sub: Option<String>,
    file: String,
    line: usize,
}

/// The matcher — the guarded core. Returns `Ok(())` for an allowed edge and
/// `Err(reason)` for a backward/forbidden one. Pure over its inputs so the
/// negative self-tests can exercise it directly.
fn edge_allowed(
    src_module: &str,
    src_sub: Option<&str>,
    tgt_module: &str,
    tgt_sub: Option<&str>,
) -> Result<(), String> {
    // Cycle-breaking special case 1: only `state::apply` may reach `event`.
    if src_module == "state" && tgt_module == "event" {
        return if src_sub == Some("apply") {
            Ok(())
        } else {
            Err(format!(
                "only `state::apply` may import `event`; found in `state::{}`",
                src_sub.unwrap_or("<root>")
            ))
        };
    }
    // Cycle-breaking special case 2: `event` may reach `state::types` only.
    if src_module == "event" && tgt_module == "state" {
        return if tgt_sub == Some("types") {
            Ok(())
        } else {
            Err(format!(
                "`event` may import `state::types` only; found `state::{}`",
                tgt_sub.unwrap_or("<root>")
            ))
        };
    }

    let (ls, lt) = match (module_level(src_module), module_level(tgt_module)) {
        (Some(ls), Some(lt)) => (ls, lt),
        _ => {
            return Err(format!(
                "unknown module in edge {src_module} -> {tgt_module}"
            ))
        }
    };

    if lt < ls {
        return Ok(());
    }
    if SAME_LEVEL_ALLOWED.contains(&(src_module, tgt_module)) {
        return Ok(());
    }
    Err(format!(
        "backward/lateral edge {src_module} (L{ls}) -> {tgt_module} (L{lt}) \
         is not in the allowed-edges table"
    ))
}

/// Recursively collect every `.rs` file under `dir`.
fn collect_rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries = fs::read_dir(dir).unwrap_or_else(|e| panic!("read_dir {}: {e}", dir.display()));
    for entry in entries {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            collect_rs_files(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

/// Blank every comment and the *interior* bytes of every string/char literal,
/// replacing them with spaces (newlines preserved so line numbers stay stable)
/// while leaving real code — the literal delimiters, braces, and `use crate::`
/// paths — intact.
///
/// This is deliberately a single small Rust-literal-aware lexer rather than a
/// set of independent textual scanners. The earlier approach stripped block
/// comments, line comments, and `#[cfg(test)]` braces in separate passes that
/// each modelled only a slice of Rust's lexical grammar, so a comment marker or
/// an unbalanced brace *inside a literal* corrupted detection — e.g. a
/// `const M = "/*";` opened a spurious block comment that erased following code
/// (including a forbidden import), a fixture string containing a lone `"{"` /
/// `"}"` shifted the test-module brace balance, and a lifetime (`'a`) on a line
/// with a trailing `//` comment left the line-comment scanner stuck "in a
/// string" so the comment was never stripped. Modelling normal strings, raw
/// strings (`r#"…"#`), byte strings/chars (`b"…"`, `b'…'`), char literals vs
/// lifetimes (`'a`), and nestable block comments in one pass closes all of
/// those gaps so the layering guard stays reliable across the epic #1201 moves,
/// whose fixtures routinely embed such markers.
fn sanitize(src: &str) -> String {
    let b = src.as_bytes();
    let mut out = String::with_capacity(src.len());
    let mut i = 0;
    // Was the previous emitted *code* byte part of an identifier/number? Needed
    // to tell a raw/byte-string prefix (`r"`, `b"`), which follows a non-ident
    // byte, from an `r`/`b` that is merely inside an identifier such as `for` or
    // `sub`.
    let mut prev_ident = false;
    fn blank(out: &mut String, byte: u8) {
        out.push(if byte == b'\n' { '\n' } else { ' ' });
    }
    while i < b.len() {
        let c = b[i];
        // Line comment: blank to end of line.
        if c == b'/' && i + 1 < b.len() && b[i + 1] == b'/' {
            while i < b.len() && b[i] != b'\n' {
                blank(&mut out, b[i]);
                i += 1;
            }
            prev_ident = false;
            continue;
        }
        // Block comment (nestable): blank the whole span, markers included.
        if c == b'/' && i + 1 < b.len() && b[i + 1] == b'*' {
            let mut depth = 0usize;
            while i < b.len() {
                if i + 1 < b.len() && b[i] == b'/' && b[i + 1] == b'*' {
                    depth += 1;
                    out.push_str("  ");
                    i += 2;
                    continue;
                }
                if i + 1 < b.len() && b[i] == b'*' && b[i + 1] == b'/' {
                    depth -= 1;
                    out.push_str("  ");
                    i += 2;
                    if depth == 0 {
                        break;
                    }
                    continue;
                }
                blank(&mut out, b[i]);
                i += 1;
            }
            prev_ident = false;
            continue;
        }
        // Raw string: `r"…"`, `r#"…"#`, or byte-raw `br"…"` — only when the
        // `r`/`br` is not part of a wider identifier.
        if !prev_ident && (c == b'r' || (c == b'b' && i + 1 < b.len() && b[i + 1] == b'r')) {
            let r_pos = if c == b'b' { i + 1 } else { i };
            let mut j = r_pos + 1;
            let mut hashes = 0usize;
            while j < b.len() && b[j] == b'#' {
                hashes += 1;
                j += 1;
            }
            if j < b.len() && b[j] == b'"' {
                // Emit the prefix and the opening quote verbatim.
                for &byte in &b[i..=j] {
                    out.push(byte as char);
                }
                i = j + 1;
                // Blank the body until the matching `"` + `hashes` `#`.
                while i < b.len() {
                    if b[i] == b'"' {
                        let mut m = 0;
                        while m < hashes && i + 1 + m < b.len() && b[i + 1 + m] == b'#' {
                            m += 1;
                        }
                        if m == hashes {
                            out.push('"');
                            for _ in 0..hashes {
                                out.push('#');
                            }
                            i += 1 + hashes;
                            break;
                        }
                    }
                    blank(&mut out, b[i]);
                    i += 1;
                }
                prev_ident = false;
                continue;
            }
            // Not actually a raw string — fall through and treat `r`/`b` as code.
        }
        // Normal or byte string: `"…"` / `b"…"`.
        if c == b'"' || (c == b'b' && i + 1 < b.len() && b[i + 1] == b'"') {
            let q_pos = if c == b'b' { i + 1 } else { i };
            for &byte in &b[i..=q_pos] {
                out.push(byte as char);
            }
            i = q_pos + 1;
            while i < b.len() {
                if b[i] == b'\\' && i + 1 < b.len() {
                    blank(&mut out, b[i]);
                    blank(&mut out, b[i + 1]);
                    i += 2;
                    continue;
                }
                if b[i] == b'"' {
                    out.push('"');
                    i += 1;
                    break;
                }
                blank(&mut out, b[i]);
                i += 1;
            }
            prev_ident = false;
            continue;
        }
        // Char literal vs lifetime/label. `'\x'…'` and `'x'` are char literals
        // (blank the interior); anything else beginning with `'` is a lifetime
        // or loop label and stays as code.
        if c == b'\'' {
            if i + 1 < b.len() && b[i + 1] == b'\\' {
                // Escaped char literal: `'\n'`, `'\''`, `'\\'`, `'\xFF'`, …
                out.push('\'');
                i += 1;
                while i < b.len() {
                    if b[i] == b'\\' && i + 1 < b.len() {
                        blank(&mut out, b[i]);
                        blank(&mut out, b[i + 1]);
                        i += 2;
                        continue;
                    }
                    if b[i] == b'\'' {
                        out.push('\'');
                        i += 1;
                        break;
                    }
                    blank(&mut out, b[i]);
                    i += 1;
                }
                prev_ident = false;
                continue;
            }
            if i + 2 < b.len() && b[i + 2] == b'\'' {
                // Simple char literal `'x'`.
                out.push('\'');
                blank(&mut out, b[i + 1]);
                out.push('\'');
                i += 3;
                prev_ident = false;
                continue;
            }
            // Lifetime / label: emit the quote, let the identifier follow.
            out.push('\'');
            i += 1;
            prev_ident = false;
            continue;
        }
        // Ordinary code byte.
        out.push(c as char);
        prev_ident = c == b'_' || c.is_ascii_alphanumeric();
        i += 1;
    }
    out
}

/// Remove inline `#[cfg(test)] mod name { ... }` blocks (brace-balanced),
/// replacing them with blank lines. Any *test-only* cfg predicate counts
/// (e.g. `#[cfg(all(test, feature = "serde"))]`), matched by
/// [`match_cfg_test_mod`]; production-enabling predicates like `#[cfg(not(test))]`
/// are left in place. External `#[cfg(test)] mod name;` declarations are
/// handled separately (they gate a whole file).
fn strip_inline_cfg_test_modules(src: &str) -> String {
    let bytes = src.as_bytes();
    let mut out = String::with_capacity(src.len());
    let mut i = 0;
    while i < bytes.len() {
        if let Some(after) = match_cfg_test_mod(bytes, i) {
            // `after` points just past `mod <name>`. Find the next `{` or `;`.
            let mut j = after;
            while j < bytes.len() && bytes[j] != b'{' && bytes[j] != b';' {
                j += 1;
            }
            if j < bytes.len() && bytes[j] == b'{' {
                // Balance braces and drop the whole block.
                let mut depth = 0usize;
                let mut k = j;
                while k < bytes.len() {
                    match bytes[k] {
                        b'{' => depth += 1,
                        b'}' => {
                            depth -= 1;
                            if depth == 0 {
                                k += 1;
                                break;
                            }
                        }
                        _ => {}
                    }
                    k += 1;
                }
                for b in &bytes[i..k] {
                    out.push(if *b == b'\n' { '\n' } else { ' ' });
                }
                i = k;
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

/// If a `#[cfg(<pred>)]` attribute whose predicate is *test-only* — it compiles
/// its item only when the `test` configuration is enabled (`#[cfg(test)]`, but
/// also compound spellings such as `#[cfg(all(test, feature = "serde"))]`) — is
/// followed by `mod <name>` (optionally behind intervening outer attributes
/// such as `#[allow(dead_code)]` and/or a visibility modifier such as
/// `pub(crate)`), return the byte offset just past `<name>`; otherwise `None`.
/// The `#[cfg(...)]` attribute is tokenized rather than matched literally, so
/// whitespace variants like `#[cfg (test)]` are recognized too.
///
/// Matching *any* test-only predicate (not just the exact `#[cfg(test)]`
/// spelling) is required so test-only modules gated behind a feature — e.g. the
/// `#[cfg(all(test, feature = "serde"))]` replay-compat modules in `event.rs`
/// and `state/apply.rs` — are stripped too; otherwise their `use crate::…`
/// statements would be scanned as production layering edges. Test-only-ness is
/// decided by [`cfg_predicate_is_test_only`], so production-enabling predicates
/// such as `#[cfg(not(test))]` or `#[cfg(any(test, feature = "…"))]` are NOT
/// stripped and keep their imports visible. Callers pass sanitized source, so a
/// `test` substring inside a string literal (e.g. `feature = "test-utils"`) has
/// already been blanked and cannot false-match.
fn match_cfg_test_mod(bytes: &[u8], start: usize) -> Option<usize> {
    // Match `#[`, then optional whitespace, `cfg`, optional whitespace, `(`.
    // Rust permits whitespace inside the attribute (`#[cfg (test)]`), so we
    // tokenize rather than requiring the exact `#[cfg(` spelling.
    let hash_bracket = b"#[";
    if start + hash_bracket.len() > bytes.len()
        || &bytes[start..start + hash_bracket.len()] != hash_bracket
    {
        return None;
    }
    let mut i = skip_ws(bytes, start + hash_bracket.len());
    let cfg = b"cfg";
    if i + cfg.len() > bytes.len() || &bytes[i..i + cfg.len()] != cfg {
        return None;
    }
    i += cfg.len();
    // Require a word boundary so `#[cfgfoo(...)]` cannot false-match.
    if i < bytes.len() && (bytes[i] == b'_' || bytes[i].is_ascii_alphanumeric()) {
        return None;
    }
    i = skip_ws(bytes, i);
    if i >= bytes.len() || bytes[i] != b'(' {
        return None;
    }
    // Balance parens across the cfg predicate; `i` is at the opening `(`.
    let pred_start = i + 1;
    let mut depth = 0usize;
    let pred_end;
    loop {
        if i >= bytes.len() {
            return None;
        }
        match bytes[i] {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    pred_end = i;
                    i += 1;
                    break;
                }
            }
            _ => {}
        }
        i += 1;
    }
    // Require the attribute's closing `]` (optional whitespace before it).
    i = skip_ws(bytes, i);
    if i >= bytes.len() || bytes[i] != b']' {
        return None;
    }
    i += 1;
    if !cfg_predicate_is_test_only(&bytes[pred_start..pred_end]) {
        return None;
    }
    i = skip_ws(bytes, i);
    // Skip any intervening outer attributes between the cfg and the module,
    // e.g. `#[cfg(test)] #[allow(dead_code)] mod tests { … }`; without this the
    // test-only module and its `use crate::…` imports would leak into the
    // layering scan as production dependencies.
    while let Some(after_attr) = skip_outer_attr(bytes, i) {
        i = skip_ws(bytes, after_attr);
    }
    // Skip an optional visibility modifier between the attribute and `mod`.
    // Valid Rust permits `#[cfg(test)] pub(crate) mod tests { … }` (and the
    // external `#[cfg(test)] pub mod tests;` form); without this the test-only
    // module — and its `use crate::…` imports — would leak into the layering
    // scan as production dependencies.
    let vis = b"pub";
    if i + vis.len() <= bytes.len()
        && &bytes[i..i + vis.len()] == vis
        && (i + vis.len() >= bytes.len()
            || !(bytes[i + vis.len()] == b'_' || bytes[i + vis.len()].is_ascii_alphanumeric()))
    {
        i += vis.len();
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        // Optional restriction `(crate)`, `(super)`, `(in path)`, …
        if i < bytes.len() && bytes[i] == b'(' {
            let mut depth = 0usize;
            while i < bytes.len() {
                match bytes[i] {
                    b'(' => depth += 1,
                    b')' => {
                        depth -= 1;
                        if depth == 0 {
                            i += 1;
                            break;
                        }
                    }
                    _ => {}
                }
                i += 1;
            }
        }
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
    }
    let kw = b"mod";
    if i + kw.len() > bytes.len() || &bytes[i..i + kw.len()] != kw {
        return None;
    }
    i += kw.len();
    // require a word boundary
    if i < bytes.len() && (bytes[i] == b'_' || bytes[i].is_ascii_alphanumeric()) {
        return None;
    }
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    let name_start = i;
    while i < bytes.len() && (bytes[i] == b'_' || bytes[i].is_ascii_alphanumeric()) {
        i += 1;
    }
    if i == name_start {
        return None;
    }
    Some(i)
}

/// If `bytes[start..]` begins an outer attribute `#[ … ]`, return the offset
/// just past its closing `]` (brackets balanced); otherwise `None`. Used to
/// step over attributes such as `#[allow(dead_code)]` that may sit between a
/// `#[cfg(test)]` and its `mod` declaration. Callers pass sanitized source, so
/// a `]` inside a string literal has already been blanked and cannot unbalance
/// the scan.
fn skip_outer_attr(bytes: &[u8], start: usize) -> Option<usize> {
    if start + 2 > bytes.len() || bytes[start] != b'#' || bytes[start + 1] != b'[' {
        return None;
    }
    let mut i = start + 1; // at the opening `[`
    let mut depth = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'[' => depth += 1,
            b']' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i + 1);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Does a `cfg` predicate compile its item *only* when the `test`
/// configuration is enabled? Returns true iff the predicate is unsatisfiable
/// whenever `test` is off — i.e. it *implies* `test`, so the module/file is
/// genuinely test-only and safe to strip before layering analysis. `test`,
/// `all(test, …)`, and nestings thereof qualify. Production-enabling predicates
/// that compile even without `test` — `not(test)`, `any(test, feature = "…")`,
/// or a plain non-test atom — do NOT, and are deliberately left in place so
/// their imports are still scanned as real layering edges. Non-`test` atoms
/// (`feature = "…"`, `unix`, unknown functions) are treated as independently
/// free, which keeps the decision conservative: an item is stripped only when
/// *no* non-test configuration can enable it.
fn cfg_predicate_is_test_only(pred: &[u8]) -> bool {
    let (node, _) = parse_cfg_pred(pred, 0);
    !node.can_be_true_without_test()
}

/// A parsed `cfg(...)` predicate over the boolean atoms `test`, `feature = "…"`,
/// `unix`, etc., combined with `all`/`any`/`not`.
enum CfgPred {
    /// A leaf config atom; `is_test` marks the `test` configuration specifically.
    Atom {
        is_test: bool,
    },
    Not(Box<CfgPred>),
    All(Vec<CfgPred>),
    Any(Vec<CfgPred>),
}

impl CfgPred {
    /// Can this predicate be satisfied in some configuration where `test` is
    /// off? The `test` atom is pinned false; every other atom is treated as an
    /// independent free variable (over-approximating satisfiability, so we err
    /// toward *not* stripping).
    fn can_be_true_without_test(&self) -> bool {
        match self {
            CfgPred::Atom { is_test } => !is_test,
            CfgPred::Not(p) => p.can_be_false_without_test(),
            CfgPred::All(ps) => ps.iter().all(CfgPred::can_be_true_without_test),
            CfgPred::Any(ps) => ps.iter().any(CfgPred::can_be_true_without_test),
        }
    }

    /// Dual of [`can_be_true_without_test`]: can the predicate evaluate false in
    /// some configuration where `test` is off?
    fn can_be_false_without_test(&self) -> bool {
        match self {
            CfgPred::Atom { .. } => true,
            CfgPred::Not(p) => p.can_be_true_without_test(),
            CfgPred::All(ps) => ps.iter().any(CfgPred::can_be_false_without_test),
            CfgPred::Any(ps) => ps.iter().all(CfgPred::can_be_false_without_test),
        }
    }
}

/// Recursive-descent parse of a `cfg` predicate body (the text inside the outer
/// `cfg(...)`). `all`/`any`/`not` become the matching combinators; anything
/// else is an atom — its identifier decides `is_test`, and a trailing
/// `= "value"` (already blanked by `sanitize`) is skipped. Malformed input
/// degrades to a free non-test atom rather than panicking.
fn parse_cfg_pred(b: &[u8], mut i: usize) -> (CfgPred, usize) {
    i = skip_ws(b, i);
    let (ident, j) = read_ident(b, i);
    i = skip_ws(b, j);
    if i < b.len() && b[i] == b'(' {
        i += 1; // consume '('
        let mut children = Vec::new();
        loop {
            i = skip_ws(b, i);
            if i >= b.len() || b[i] == b')' {
                if i < b.len() {
                    i += 1; // consume ')'
                }
                break;
            }
            let (child, k) = parse_cfg_pred(b, i);
            children.push(child);
            i = skip_ws(b, k);
            if i < b.len() && b[i] == b',' {
                i += 1;
                continue;
            }
            if i < b.len() && b[i] == b')' {
                i += 1;
                break;
            }
            break; // malformed
        }
        let node = match ident.as_str() {
            "not" => CfgPred::Not(Box::new(
                children
                    .into_iter()
                    .next()
                    .unwrap_or(CfgPred::Atom { is_test: false }),
            )),
            "all" => CfgPred::All(children),
            "any" => CfgPred::Any(children),
            // Unknown predicate function (e.g. `target_has_atomic("ptr")`): a
            // non-test config, so treat the whole thing as a free atom.
            _ => CfgPred::Atom { is_test: false },
        };
        (node, i)
    } else {
        // Plain atom, optionally `ident = "value"`; only the identifier matters.
        if i < b.len() && b[i] == b'=' {
            i += 1;
            while i < b.len() && b[i] != b',' && b[i] != b')' {
                i += 1;
            }
        }
        (
            CfgPred::Atom {
                is_test: ident == "test",
            },
            i,
        )
    }
}

/// Read an identifier `[A-Za-z_][A-Za-z0-9_]*` starting at `i`; return the
/// identifier and the offset just past it. A leading raw-identifier prefix
/// `r#` (e.g. `r#type`) is consumed and stripped from the returned name, so a
/// raw alias like `use crate::{state::apply as r#type, engine::Engine}` no
/// longer leaves `#type` unconsumed — which would stall the enclosing brace
/// scan and silently drop every sibling after it (hiding a forbidden edge). The
/// `r#"…"#` raw *string* form is excluded by requiring an identifier-start byte
/// after `#` (sanitized source has already blanked string literals regardless).
fn read_ident(bytes: &[u8], mut i: usize) -> (String, usize) {
    if i + 2 < bytes.len()
        && bytes[i] == b'r'
        && bytes[i + 1] == b'#'
        && (bytes[i + 2] == b'_' || bytes[i + 2].is_ascii_alphabetic())
    {
        i += 2;
    }
    let start = i;
    if i < bytes.len() && (bytes[i] == b'_' || bytes[i].is_ascii_alphabetic()) {
        i += 1;
        while i < bytes.len() && (bytes[i] == b'_' || bytes[i].is_ascii_alphanumeric()) {
            i += 1;
        }
    }
    (String::from_utf8_lossy(&bytes[start..i]).into_owned(), i)
}

/// Map a source file path (relative to `src/`) to its top-level module and, for
/// directory modules, its sub-module. The sub-module is the *first* child
/// component of the Rust module path — `state/apply.rs`, `state/apply/mod.rs`,
/// and `state/apply/<family>.rs` all map to `state::apply` — while `state/mod.rs`
/// maps to the module root (`None`).
fn module_of(rel: &Path) -> (String, Option<String>) {
    let comps: Vec<String> = rel
        .components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect();
    if comps.len() == 1 {
        // e.g. `event.rs` -> module `event`
        let stem = comps[0].trim_end_matches(".rs").to_string();
        (stem, None)
    } else {
        let module = comps[0].clone();
        // Derive the sub-module from the FIRST child component so the Rust
        // module path — not the on-disk filename — drives classification. A
        // directory sub-module `state/apply/…` (whether it lands as
        // `state/apply/mod.rs` or a `state/apply/<family>.rs` split) then still
        // classifies as `state::apply`, keeping its `state::apply -> event`
        // exception; and `state/mod.rs` collapses to the module root (`None`)
        // rather than a spurious `state::mod`.
        let child = comps[1].trim_end_matches(".rs");
        let sub = if child == "mod" {
            None
        } else {
            Some(child.to_string())
        };
        (module, sub)
    }
}

/// The full module path of a source file relative to the crate root:
/// `state/apply.rs` -> `[state, apply]`, `state/mod.rs` -> `[state]`,
/// `event.rs` -> `[event]`. `mod.rs` names the directory module itself, so it
/// contributes no extra segment. Used to resolve `self`/`super` relative
/// imports back to an absolute `crate::…` path.
fn module_path_of(rel: &Path) -> Vec<String> {
    let comps: Vec<String> = rel
        .components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect();
    let n = comps.len();
    let mut segs = Vec::new();
    for (idx, comp) in comps.iter().enumerate() {
        let name = comp.trim_end_matches(".rs");
        if idx == n - 1 && name == "mod" {
            continue; // `mod.rs` is the directory module itself
        }
        segs.push(name.to_string());
    }
    segs
}

/// Skip ASCII whitespace (including newlines) starting at `i`.
fn skip_ws(b: &[u8], mut i: usize) -> usize {
    while i < b.len() && b[i].is_ascii_whitespace() {
        i += 1;
    }
    i
}

/// Parse a single Rust *use-tree* — the grammar that follows `use crate::` —
/// expanding brace groups so every leaf becomes its own path. Returns the list
/// of leaf paths (each a vector of `::`-separated segments) and the offset just
/// past the tree. Whitespace *including newlines* between tokens is skipped, so
/// a declaration wrapped across several physical lines is parsed as one unit.
///
/// Examples (the `crate::` prefix is already consumed by the caller):
/// * `state::types::X`              -> `[[state, types, X]]`
/// * `{event::Event, state::apply}` -> `[[event, Event], [state, apply]]`
/// * `state::{types::X, apply}`     -> `[[state, types, X], [state, apply]]`
/// * `model::Thing as Alias`        -> `[[model, Thing]]` (the `as` rename is
///   irrelevant to the module edge and is consumed so it cannot stall an
///   enclosing brace group)
fn parse_use_tree(b: &[u8], start: usize) -> (Vec<Vec<String>>, usize) {
    let mut i = skip_ws(b, start);
    // Brace group: expand each comma-separated sub-tree.
    if i < b.len() && b[i] == b'{' {
        i += 1;
        let mut out = Vec::new();
        loop {
            i = skip_ws(b, i);
            if i >= b.len() {
                break;
            }
            if b[i] == b'}' {
                i += 1;
                break;
            }
            let before = i;
            let (paths, ni) = parse_use_tree(b, i);
            i = ni;
            out.extend(paths);
            i = skip_ws(b, i);
            if i < b.len() && b[i] == b',' {
                i += 1;
                continue;
            }
            if i < b.len() && b[i] == b'}' {
                i += 1;
                break;
            }
            // Malformed input or no forward progress — stop to guarantee
            // termination rather than spin.
            if i == before {
                i += 1;
            }
            break;
        }
        return (out, i);
    }
    // Path: ident (`::` (ident | `*` | `{…}`))*
    let mut prefix: Vec<String> = Vec::new();
    loop {
        i = skip_ws(b, i);
        if i < b.len() && b[i] == b'{' {
            // `prefix::{…}` — distribute the shared prefix over each leaf.
            let (subs, ni) = parse_use_tree(b, i);
            i = ni;
            let out = subs
                .into_iter()
                .map(|s| {
                    let mut full = prefix.clone();
                    full.extend(s);
                    full
                })
                .collect();
            return (out, i);
        }
        if i < b.len() && b[i] == b'*' {
            i += 1;
            let mut full = prefix.clone();
            full.push("*".to_string());
            return (vec![full], i);
        }
        let (ident, ni) = read_ident(b, i);
        if ident.is_empty() {
            break;
        }
        prefix.push(ident);
        i = ni;
        i = skip_ws(b, i);
        if i + 1 < b.len() && b[i] == b':' && b[i + 1] == b':' {
            i += 2;
            continue;
        }
        break;
    }
    if prefix.is_empty() {
        (Vec::new(), i)
    } else {
        // Consume an optional `as <alias>` rename. Leaving it unconsumed would
        // stall an enclosing brace group's `,`/`}` scan and silently drop every
        // sibling after an aliased entry (e.g. `{state::apply as run, engine}`).
        i = skip_ws(b, i);
        let (kw, after_kw) = read_ident(b, i);
        if kw == "as" {
            let (_alias, after_alias) = read_ident(b, skip_ws(b, after_kw));
            i = skip_ws(b, after_alias);
        }
        (vec![prefix], i)
    }
}

/// Extract every crate-internal module edge from already-cleaned source
/// (comments + string/char literals blanked, inline test modules stripped).
///
/// Each `use` declaration is tokenized rather than matched as one fixed
/// substring, so all of these resolve to the same absolute edge and are checked:
/// * `use crate::state::apply;` — absolute.
/// * `use\ncrate::state::apply;` / `use /* c */ crate::state::apply;` —
///   whitespace or a (blanked) comment between `use` and `crate`.
/// * `use self::foo;` / `use super::super::event::Event;` — `self`/`super`
///   relative imports, resolved against the file's own [`module_path_of`] path
///   so a backward edge cannot hide behind relative syntax.
///
/// The use-tree after the root is parsed via [`parse_use_tree`], so brace
/// use-trees (`use crate::{a::b, c}`) are expanded entry-by-entry and
/// declarations wrapped across newlines are read as one unit. A *root* brace
/// group (`use {crate::a, self::b};`) is likewise expanded, each element rooted
/// independently (via [`resolve_use_forest`]) so a backward edge cannot hide
/// behind the leading brace. Every leaf whose
/// resolved first segment is a top-level module (and is not the source module
/// itself) becomes an edge. A leaf whose first segment is not a module (a
/// crate-root re-export such as `use crate::Engine`) is not a module edge and
/// is skipped. External-crate imports (`use serde::…`, `use std::…`) have a
/// non-`crate`/`self`/`super` root and are ignored.
fn edges_in_source(
    cleaned: &str,
    module_path: &[String],
    src_module: &str,
    src_sub: Option<&str>,
    file_label: &str,
    edges: &mut Vec<Edge>,
) {
    let bytes = cleaned.as_bytes();
    let mut search = 0;
    while let Some(rel) = cleaned[search..].find("use") {
        let kw_start = search + rel;
        let kw_end = kw_start + 3;
        // Require word boundaries so `reuse`, `used`, `cause` never match.
        let before_ok = kw_start == 0
            || !(bytes[kw_start - 1] == b'_' || bytes[kw_start - 1].is_ascii_alphanumeric());
        let after_ok = kw_end >= bytes.len()
            || !(bytes[kw_end] == b'_' || bytes[kw_end].is_ascii_alphanumeric());
        if !(before_ok && after_ok) {
            search = kw_end;
            continue;
        }
        let Some((paths, end)) = resolve_use_forest(bytes, kw_end, module_path) else {
            search = kw_end;
            continue;
        };
        let lineno = 1 + cleaned[..kw_start]
            .bytes()
            .filter(|&byte| byte == b'\n')
            .count();
        for resolved in paths {
            let Some(module) = resolved.first() else {
                continue;
            };
            if MODULES.contains(&module.as_str()) && module != src_module {
                let sub = resolved.get(1).filter(|s| s.as_str() != "*").cloned();
                edges.push(Edge {
                    src_module: src_module.to_string(),
                    src_sub: src_sub.map(str::to_string),
                    tgt_module: module.clone(),
                    tgt_sub: sub,
                    file: file_label.to_string(),
                    line: lineno,
                });
            }
        }
        search = end.max(kw_end);
    }
}

/// Consume `::` (with surrounding whitespace already handled by the caller),
/// returning the offset just past it, or `None` if `::` is not next.
fn expect_path_sep(b: &[u8], i: usize) -> Option<usize> {
    let i = skip_ws(b, i);
    if i + 2 <= b.len() && b[i] == b':' && b[i + 1] == b':' {
        Some(i + 2)
    } else {
        None
    }
}

/// Parse the root of a `use` path starting just after the `use` keyword and
/// return `(base, tail)`: the absolute module-path segments the tail is rooted
/// at, and the offset of the first tail segment. `crate` roots at the crate
/// root (`[]`), `self` at the file's own module, and each leading `super` pops
/// one segment off the file's module path. Returns `None` for an external root
/// (`serde`, `std`, …) or a malformed/empty root, so such imports are skipped.
fn resolve_use_root(b: &[u8], i: usize, module_path: &[String]) -> Option<(Vec<String>, usize)> {
    let i = skip_ws(b, i);
    let (first, ni) = read_ident(b, i);
    match first.as_str() {
        "crate" => Some((Vec::new(), expect_path_sep(b, ni)?)),
        "self" => Some((module_path.to_vec(), expect_path_sep(b, ni)?)),
        "super" => {
            let mut base = module_path.to_vec();
            base.pop();
            let mut after = expect_path_sep(b, ni)?;
            loop {
                let (kw, nj) = read_ident(b, skip_ws(b, after));
                if kw != "super" {
                    break;
                }
                base.pop();
                after = expect_path_sep(b, nj)?;
            }
            Some((base, after))
        }
        _ => None,
    }
}

/// Parse a full use-tree *including its root(s)* starting just after the `use`
/// keyword, returning every leaf resolved to an absolute crate path plus the
/// offset past the declaration. A bare root (`use crate::…`, `use self::…`) is
/// resolved via [`resolve_use_root`] and its tail expanded by [`parse_use_tree`].
/// A *root* brace group (`use {crate::a, self::b};`) is expanded element by
/// element, each element re-entered through this function so its own root is
/// resolved — otherwise the leading `{` reads as an empty identifier, the whole
/// declaration is dropped, and a backward dependency hides inside the root
/// group. An element with an external/malformed root contributes no paths but
/// does not discard its siblings. Returns `None` only for a bare external or
/// malformed root, so the caller skips just that declaration.
fn resolve_use_forest(
    b: &[u8],
    i: usize,
    module_path: &[String],
) -> Option<(Vec<Vec<String>>, usize)> {
    let i = skip_ws(b, i);
    if i < b.len() && b[i] == b'{' {
        let mut j = i + 1;
        let mut out = Vec::new();
        loop {
            j = skip_ws(b, j);
            if j >= b.len() {
                break;
            }
            if b[j] == b'}' {
                j += 1;
                break;
            }
            let before = j;
            match resolve_use_forest(b, j, module_path) {
                Some((paths, nj)) => {
                    out.extend(paths);
                    j = nj;
                }
                // External/malformed element: skip it, keeping its siblings.
                None => j = skip_use_element(b, j),
            }
            j = skip_ws(b, j);
            if j < b.len() && b[j] == b',' {
                j += 1;
                continue;
            }
            if j < b.len() && b[j] == b'}' {
                j += 1;
                break;
            }
            // Malformed input or no forward progress — stop to guarantee
            // termination rather than spin.
            if j == before {
                j += 1;
            }
            break;
        }
        return Some((out, j));
    }
    let (base, tail) = resolve_use_root(b, i, module_path)?;
    let (paths, end) = parse_use_tree(b, tail);
    let out = paths
        .into_iter()
        .map(|path| {
            let mut full = base.clone();
            full.extend(path);
            full
        })
        .collect();
    Some((out, end))
}

/// Advance past one element of a root brace group whose root is external or
/// otherwise yields no edges, stopping just before the element's terminating
/// top-level `,` or the group's closing `}` (nested `{…}` are skipped as a
/// unit). Guarantees forward progress so the enclosing group scan cannot spin.
fn skip_use_element(b: &[u8], mut i: usize) -> usize {
    let mut depth = 0usize;
    while i < b.len() {
        match b[i] {
            b'{' => depth += 1,
            b'}' => {
                if depth == 0 {
                    break;
                }
                depth -= 1;
            }
            b',' if depth == 0 => break,
            _ => {}
        }
        i += 1;
    }
    i
}

/// Collect the set of files gated by an external `#[cfg(test)] mod name;`
/// declaration (test-only files that must be ignored).
fn collect_test_gated_files(src_root: &Path, files: &[PathBuf]) -> BTreeSet<PathBuf> {
    let mut gated = BTreeSet::new();
    for file in files {
        let raw = fs::read_to_string(file).expect("read source file");
        let src = sanitize(&raw);
        let bytes = src.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            if let Some(after) = match_cfg_test_mod(bytes, i) {
                // find `{` or `;`
                let mut j = after;
                // the module name we just consumed:
                let (name, _) = {
                    // re-read the name ending at `after`
                    let mut k = after;
                    while k > 0 && (bytes[k - 1] == b'_' || bytes[k - 1].is_ascii_alphanumeric()) {
                        k -= 1;
                    }
                    (
                        String::from_utf8_lossy(&bytes[k..after]).into_owned(),
                        after,
                    )
                };
                while j < bytes.len() && bytes[j] != b'{' && bytes[j] != b';' {
                    j += 1;
                }
                if j < bytes.len() && bytes[j] == b';' {
                    // external module: the flat `<dir>/<name>.rs`, the directory
                    // module `<dir>/<name>/mod.rs`, and — because child modules
                    // inherit the parent's `cfg(test)` — every descendant of
                    // `<dir>/<name>/`. `<dir>` is the *module source directory*
                    // of the declaring file, not simply its parent: a flat file
                    // `src/bpmn.rs` resolves `mod name;` to `src/bpmn/name.rs`.
                    let dir = module_source_dir(file, src_root);
                    for f in gated_files_for_external_mod(&dir, &name, files) {
                        gated.insert(f.clone());
                    }
                }
                i = after;
                continue;
            }
            i += 1;
        }
    }
    gated
}

/// Resolve the *module source directory* that an external `mod <name>;`
/// declared inside `file` resolves its children against, per Rust 2018 module
/// resolution.
///
/// A directory owner — `mod.rs`, `lib.rs`, or `main.rs` — resolves `mod name;`
/// to `<parent>/name.rs`, so its module source directory is its own parent. A
/// flat file `<parent>/foo.rs` instead owns the subdirectory `<parent>/foo/`,
/// resolving `mod name;` to `<parent>/foo/name.rs`. Resolving relative to
/// `file.parent()` unconditionally (the old behaviour) misplaced children
/// declared in flat files like `src/bpmn.rs` — looking for `src/name.rs` rather
/// than `src/bpmn/name.rs` — which would leave a genuine test file in the
/// production scan and produce a false layering failure.
fn module_source_dir(file: &Path, src_root: &Path) -> PathBuf {
    let parent = file.parent().unwrap_or(src_root);
    match file.file_stem().and_then(|s| s.to_str()) {
        Some("mod") | Some("lib") | Some("main") => parent.to_path_buf(),
        Some(stem) => parent.join(stem),
        None => parent.to_path_buf(),
    }
}

/// Resolve an external `#[cfg(test)] mod <name>;` declared in `dir` to every
/// source file it gates, given the full set of `files` under `src`.
///
/// A `#[cfg(test)]` module gates not only its own file — the flat
/// `<dir>/<name>.rs` or the directory module `<dir>/<name>/mod.rs` — but its
/// *entire* module subtree: child modules inherit the parent's `cfg(test)`, so
/// once the planned test-file split lands, files like `<dir>/<name>/foo.rs`
/// (module `<name>::foo`) are equally test-only. Without gating the whole
/// subtree such a descendant would be scanned as production and could reject a
/// valid test-only import (e.g. `crate::ffi`), producing a false layering
/// failure. Membership in `files` (the complete `src` walk) stands in for a
/// disk `exists()` check, and `starts_with` captures `mod.rs` and every
/// descendant of the directory in one predicate.
fn gated_files_for_external_mod<'a>(
    dir: &Path,
    name: &str,
    files: &'a [PathBuf],
) -> Vec<&'a PathBuf> {
    let flat = dir.join(format!("{name}.rs"));
    let subtree = dir.join(name);
    files
        .iter()
        .filter(|f| **f == flat || f.starts_with(&subtree))
        .collect()
}

/// Walk `engine-core/src`, apply the exclusions, and return every module edge.
fn collect_edges() -> Vec<Edge> {
    let src_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    collect_rs_files(&src_root, &mut files);
    files.sort();

    let gated = collect_test_gated_files(&src_root, &files);

    let mut edges = Vec::new();
    for file in &files {
        if gated.contains(file) {
            continue;
        }
        let rel = file.strip_prefix(&src_root).expect("under src");
        let (src_module, src_sub) = module_of(rel);
        if src_module == "lib" {
            continue; // crate root: declares modules, has no layering edges of its own
        }
        let module_path = module_path_of(rel);
        let raw = fs::read_to_string(file).expect("read source file");
        let cleaned = strip_inline_cfg_test_modules(&sanitize(&raw));
        edges_in_source(
            &cleaned,
            &module_path,
            &src_module,
            src_sub.as_deref(),
            &rel.to_string_lossy(),
            &mut edges,
        );
    }
    edges
}

#[test]
fn dependency_direction_holds() {
    let edges = collect_edges();
    assert!(
        !edges.is_empty(),
        "no `use crate::` edges found — the scanner or src path is wrong"
    );

    let mut violations = Vec::new();
    for e in &edges {
        if let Err(reason) = edge_allowed(
            &e.src_module,
            e.src_sub.as_deref(),
            &e.tgt_module,
            e.tgt_sub.as_deref(),
        ) {
            violations.push(format!(
                "  {}:{}  {} -> {}{}   ({reason})",
                e.file,
                e.line,
                e.src_module,
                e.tgt_module,
                e.tgt_sub
                    .as_deref()
                    .map(|s| format!("::{s}"))
                    .unwrap_or_default(),
            ));
        }
    }

    assert!(
        violations.is_empty(),
        "engine-core module-layering violations (see docs/engine-architecture-analysis.md §3 \
         and the table in this test):\n{}",
        violations.join("\n")
    );
}

/// [`MODULES`] (and therefore [`module_level`] and the allowed-edges table) must
/// list *exactly* the top-level modules that exist under `src/`. This is the
/// single source of truth that keeps the extractor honest: [`edges_in_source`]
/// only records an edge when the imported first segment is in `MODULES`, so a
/// module added to `src/` but *not* added here would make its imports invisible
/// to the guard — a backward edge to (or from) it could land unchecked, exactly
/// the "import of an unlisted top-level module is silently dropped" gap. Failing
/// the moment the table drifts from disk forces the new/removed module's layer
/// to be assigned *before* any of its edges can bypass evaluation.
#[test]
fn modules_table_matches_src_tree() {
    let src_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut on_disk: BTreeSet<String> = BTreeSet::new();
    for entry in fs::read_dir(&src_root).expect("read src") {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            // A directory is a module only if it has a `mod.rs`.
            if path.join("mod.rs").exists() {
                if let Some(name) = path.file_name() {
                    on_disk.insert(name.to_string_lossy().into_owned());
                }
            }
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            if let Some(stem) = path.file_stem() {
                let stem = stem.to_string_lossy().into_owned();
                if stem != "lib" {
                    on_disk.insert(stem);
                }
            }
        }
    }

    let listed: BTreeSet<String> = MODULES.iter().map(|m| (*m).to_string()).collect();
    assert_eq!(
        listed,
        on_disk,
        "MODULES has drifted from engine-core/src — update MODULES, `module_level`, and the \
         layering table so the module's edges are checked (on disk but not in MODULES: {:?}; in \
         MODULES but not on disk: {:?})",
        on_disk.difference(&listed).collect::<Vec<_>>(),
        listed.difference(&on_disk).collect::<Vec<_>>(),
    );

    // Every listed module must have a layer, or `edge_allowed` cannot classify
    // its edges (it would report "unknown module" for a real one).
    for m in MODULES {
        assert!(
            module_level(m).is_some(),
            "module `{m}` is listed in MODULES but has no layer in `module_level`"
        );
    }
}

/// Guard-the-guard: the matcher must *reject* deliberately-wrong edges. If any
/// of these silently pass, the guard above is worthless.
#[test]
fn guard_rejects_backward_and_forbidden_edges() {
    let cases: &[(&str, Option<&str>, &str, Option<&str>)] = &[
        // pure backward level edges
        ("model", None, "engine", None),
        ("model", None, "feel", None),
        ("event", None, "engine", None),
        ("state", Some("types"), "engine", None),
        // lateral edges not in the allow-list
        ("json", None, "model", None),    // L0 -> L0
        ("agent", None, "lease", None),   // L4 -> L4
        ("command", None, "agent", None), // L4 -> L4
        ("validate", None, "bpmn", None), // L5 -> L5
        // the cycle we broke must stay broken:
        ("state", Some("types"), "event", None), // only apply may import event
        ("state", None, "event", None),          // bare state may not import event
        ("event", None, "state", Some("apply")), // event may not reach apply
        ("event", None, "state", None),          // event may not reach bare state
    ];
    for (sm, ss, tm, ts) in cases {
        assert!(
            edge_allowed(sm, *ss, tm, *ts).is_err(),
            "matcher wrongly ALLOWED forbidden edge {sm}{} -> {tm}{}",
            ss.map(|s| format!("::{s}")).unwrap_or_default(),
            ts.map(|s| format!("::{s}")).unwrap_or_default(),
        );
    }
}

/// The matcher must *accept* the edges the layering deliberately permits,
/// including the intra-level and cycle-breaking special cases.
#[test]
fn guard_accepts_known_good_edges() {
    let cases: &[(&str, Option<&str>, &str, Option<&str>)] = &[
        ("engine", None, "model", None),
        ("engine", None, "state", None),
        ("bpmn", None, "agent", None), // L5 -> L4
        ("feel", None, "model", None),
        ("dmn", None, "feel", None),             // permitted intra-level
        ("bpmn", None, "validate", None),        // permitted intra-level
        ("ffi", None, "engine", None),           // permitted intra-level
        ("state", Some("apply"), "event", None), // cycle-breaking exception
        ("event", None, "state", Some("types")), // cycle-breaking exception
    ];
    for (sm, ss, tm, ts) in cases {
        assert!(
            edge_allowed(sm, *ss, tm, *ts).is_ok(),
            "matcher wrongly REJECTED allowed edge {sm}{} -> {tm}{}",
            ss.map(|s| format!("::{s}")).unwrap_or_default(),
            ts.map(|s| format!("::{s}")).unwrap_or_default(),
        );
    }
}

/// Run the real cleaning pipeline (`sanitize` → strip inline test modules →
/// use-declaration edge extraction) over a synthetic single-file source and
/// return the module edges it yields, so the lexer's literal-awareness can be
/// exercised directly. The file is treated as the top-level module `src_module`
/// (module path `[src_module]`); use [`edges_of_source_at`] for a deeper file
/// path when exercising `self`/`super` relative-import resolution.
#[cfg(test)]
fn edges_of_source(src_module: &str, src: &str) -> Vec<Edge> {
    edges_of_source_at(&[src_module], src)
}

/// Like [`edges_of_source`] but for a file at an explicit module path (e.g.
/// `["state", "apply"]` for `state/apply.rs`), so `self`/`super` relative
/// imports resolve exactly as they would on disk.
#[cfg(test)]
fn edges_of_source_at(module_path: &[&str], src: &str) -> Vec<Edge> {
    let cleaned = strip_inline_cfg_test_modules(&sanitize(src));
    let owned: Vec<String> = module_path.iter().map(|s| s.to_string()).collect();
    let src_module = owned[0].clone();
    let src_sub = owned.get(1).cloned();
    let mut edges = Vec::new();
    edges_in_source(
        &cleaned,
        &owned,
        &src_module,
        src_sub.as_deref(),
        "synthetic.rs",
        &mut edges,
    );
    edges
}

#[cfg(test)]
fn has_edge(edges: &[Edge], tgt_module: &str) -> bool {
    edges.iter().any(|e| e.tgt_module == tgt_module)
}

/// A comment marker inside a string literal must not open a spurious block
/// comment that erases the following real code (issue: the guard could silently
/// miss a forbidden import after a `const M = "/*";` line).
#[test]
fn sanitize_ignores_comment_markers_inside_strings() {
    let src = "const MARKER: &str = \"/*\";\nuse crate::model::Thing;\n";
    let edges = edges_of_source("engine", src);
    assert!(
        has_edge(&edges, "model"),
        "block-comment marker inside a string literal erased a real `use crate::` edge: {edges:?}"
    );
}

/// A lifetime (`'a`) on a line that also carries a trailing `//` comment must
/// not wedge the scanner "inside a string" and leak the comment's text as code
/// (which would spuriously report a backward edge named in the comment).
#[test]
fn sanitize_strips_line_comment_after_lifetime() {
    let src = "fn f<'a>() {} // use crate::engine::Foo\nuse crate::model::T;\n";
    let edges = edges_of_source("feel", src);
    assert!(
        !has_edge(&edges, "engine"),
        "text inside a `//` comment after a lifetime leaked as a real edge: {edges:?}"
    );
    assert!(
        has_edge(&edges, "model"),
        "the genuine post-comment `use crate::` edge was lost: {edges:?}"
    );
}

/// An unbalanced brace inside a string literal must not shift the
/// `#[cfg(test)] mod { … }` brace balance, which would either leak the test
/// module's `use crate::` edges or eat real code after it.
#[test]
fn sanitize_ignores_braces_inside_test_module_strings() {
    let src = "#[cfg(test)]\nmod tests {\n    let s = \"}\";\n    use crate::engine::X;\n}\nuse crate::model::Y;\n";
    let edges = edges_of_source("feel", src);
    assert!(
        !has_edge(&edges, "engine"),
        "a `\"}}\"` string closed the test module early, leaking a test-only edge: {edges:?}"
    );
    assert!(
        has_edge(&edges, "model"),
        "an unbalanced brace inside a string swallowed real code after the test module: {edges:?}"
    );
}

/// Raw-string contents (including embedded `//`, `/*`, and `use crate::` text)
/// must be blanked, never mistaken for comments or real import edges.
#[test]
fn sanitize_blanks_raw_string_contents() {
    let src = "let q = r#\"use crate::engine::Z and /* not a comment\"#;\nuse crate::model::W;\n";
    let edges = edges_of_source("feel", src);
    assert!(
        !has_edge(&edges, "engine"),
        "a `use crate::` inside a raw string was reported as a real edge: {edges:?}"
    );
    assert!(
        has_edge(&edges, "model"),
        "a `/*` inside a raw string opened a spurious block comment: {edges:?}"
    );
}

/// A `use crate::` declaration wrapped across newlines (the module segment on a
/// separate physical line from `use crate::`) must still be extracted. Scanning
/// one line at a time missed this form, letting a backward edge land unchecked.
#[test]
fn extracts_multiline_use_declaration() {
    let src = "use crate::\n    engine::Engine;\nuse crate::model::T;\n";
    let edges = edges_of_source("feel", src);
    assert!(
        has_edge(&edges, "engine"),
        "a `use crate::` path wrapped across newlines was not extracted: {edges:?}"
    );
    assert!(
        has_edge(&edges, "model"),
        "the single-line edge on the following line was lost: {edges:?}"
    );
}

/// A brace use-tree (`use crate::{a::b, c::d}`) must be expanded entry-by-entry
/// so every leaf module is checked. The old single-path scanner saw the `{`,
/// read an empty identifier, and dropped the whole declaration — letting a
/// forbidden edge hide inside braces (`use crate::{engine::Engine}` from a lower
/// layer would bypass the guard entirely).
#[test]
fn extracts_brace_use_tree_entries() {
    let src = "use crate::{event::Event, model::Thing};\n";
    let edges = edges_of_source("feel", src);
    assert!(
        has_edge(&edges, "event"),
        "a module edge inside a brace use-tree was ignored: {edges:?}"
    );
    assert!(
        has_edge(&edges, "model"),
        "a second brace use-tree entry was ignored: {edges:?}"
    );
}

/// A nested brace use-tree with a shared prefix (`use crate::state::{types::X,
/// apply::Y}`) must distribute the prefix over each leaf, so the cycle-breaking
/// `state::types` vs `state::apply` distinction survives the braced form.
#[test]
fn extracts_nested_brace_use_tree_with_shared_prefix() {
    let src = "use crate::state::{types::T, apply::A};\n";
    let edges = edges_of_source("event", src);
    assert!(
        edges
            .iter()
            .any(|e| e.tgt_module == "state" && e.tgt_sub.as_deref() == Some("types")),
        "the `state::types` leaf of a prefixed brace use-tree was lost: {edges:?}"
    );
    assert!(
        edges
            .iter()
            .any(|e| e.tgt_module == "state" && e.tgt_sub.as_deref() == Some("apply")),
        "the `state::apply` leaf of a prefixed brace use-tree was lost: {edges:?}"
    );
}

/// A backward edge written inside a brace use-tree must be caught by the real
/// matcher — the whole point of expanding braces. `event` importing `engine`
/// via `use crate::{engine::Engine}` is a forbidden L3 -> L6 edge and must be
/// rejected once extracted.
#[test]
fn brace_use_tree_backward_edge_is_rejected() {
    let src = "use crate::{engine::Engine};\n";
    let edges = edges_of_source("event", src);
    let engine = edges
        .iter()
        .find(|e| e.tgt_module == "engine")
        .expect("brace-wrapped backward edge should be extracted");
    assert!(
        edge_allowed(&engine.src_module, None, &engine.tgt_module, None).is_err(),
        "a backward edge hidden in a brace use-tree slipped past the matcher"
    );
}

/// A crate-root re-export (`use crate::Engine`, `use crate::{Command, Engine}`)
/// names the crate's public surface, not a module, so its first segment is not
/// in `MODULES` and it must be ignored — only real module first segments count.
#[test]
fn ignores_crate_root_reexports() {
    let src = "use crate::{Engine, Command};\nuse crate::model::T;\n";
    let edges = edges_of_source("feel", src);
    assert_eq!(
        edges.len(),
        1,
        "a crate-root re-export was mistaken for a module edge: {edges:?}"
    );
    assert!(has_edge(&edges, "model"));
}

/// An `as <alias>` rename on a brace-group entry must not stall the group and
/// drop the entries after it. `event` importing `state` (renamed) *and*
/// `engine` via `use crate::{state::apply as run, engine::Engine}` must still
/// surface the backward `engine` edge — the sibling hidden after the alias.
#[test]
fn aliased_brace_entry_does_not_drop_later_leaves() {
    let src = "use crate::{state::apply as run, engine::Engine};\n";
    let edges = edges_of_source("event", src);
    assert!(
        edges.iter().any(|e| e.tgt_module == "state"),
        "the aliased brace entry itself was lost: {edges:?}"
    );
    let engine = edges
        .iter()
        .find(|e| e.tgt_module == "engine")
        .expect("the entry after an aliased brace sibling was silently dropped");
    assert!(
        edge_allowed(&engine.src_module, None, &engine.tgt_module, None).is_err(),
        "a backward edge hidden after an aliased brace sibling slipped past"
    );
}

/// Whitespace or a (blanked) comment between `use` and `crate` must not hide a
/// declaration: `sanitize` turns comments into spaces, so `use /* c */ crate::`
/// and `use\ncrate::` are valid Rust the guard must still tokenize.
#[test]
fn tolerates_whitespace_and_comments_before_crate() {
    for src in [
        "use\ncrate::engine::Engine;\n",
        "use /* reach around */ crate::engine::Engine;\n",
        "use    crate  ::  engine :: Engine ;\n",
    ] {
        let edges = edges_of_source("event", src);
        assert!(
            edges.iter().any(|e| e.tgt_module == "engine"),
            "a `use crate::` split by whitespace/comment slipped past the scanner: {src:?} -> {edges:?}"
        );
    }
}

/// A `super`-relative import that climbs to the crate root and dips into another
/// top-level module is the same layering edge as its `crate::` spelling and must
/// be caught. From `state/apply.rs` (== `crate::state::apply`),
/// `super::super::event` resolves to `crate::event` (an allowed cycle-break);
/// from `state/mod.rs` (== `crate::state`), `super::event` also resolves to
/// `crate::event` but from the module root, where it is forbidden.
#[test]
fn resolves_super_relative_imports() {
    // `state/apply.rs`: two `super`s climb `apply` -> `state` -> crate root,
    // so `super::super::event` == `crate::event`, the allowed cycle-break edge.
    let edges = edges_of_source_at(&["state", "apply"], "use super::super::event::Event;\n");
    let e = edges
        .iter()
        .find(|e| e.tgt_module == "event")
        .expect("a super-relative import crossing into another module produced no edge");
    assert!(
        edge_allowed(&e.src_module, Some("apply"), &e.tgt_module, None).is_ok(),
        "state::apply -> event is an allowed cycle-break edge"
    );

    // `state/mod.rs`: one `super` reaches the crate root, so `super::event` ==
    // `crate::event` — a backward edge from the module root that must be rejected.
    let back = edges_of_source_at(&["state"], "use super::event::Event;\n");
    let b = back
        .iter()
        .find(|e| e.tgt_module == "event")
        .expect("a super-relative backward import produced no edge");
    assert!(
        edge_allowed(&b.src_module, None, &b.tgt_module, None).is_err(),
        "a backward edge written with `super::` relative syntax slipped past the matcher"
    );
}

/// A `super` that stays within the file's own top-level module is not a layering
/// edge: from `state/apply.rs`, `super::types` == `crate::state::types`, whose
/// first segment is the source module itself, so it must not be reported.
#[test]
fn intra_module_super_import_is_not_an_edge() {
    let edges = edges_of_source_at(&["state", "apply"], "use super::types::T;\n");
    assert!(
        edges.is_empty(),
        "an intra-module `super::` import was mistaken for a layering edge: {edges:?}"
    );
}

/// A `self`-relative import stays inside the file's own module subtree, so it is
/// never a cross-module layering edge and must not be reported.
#[test]
fn self_relative_import_is_intra_module() {
    let edges = edges_of_source_at(&["state", "apply"], "use self::helpers::run;\n");
    assert!(
        edges.is_empty(),
        "a `self::` intra-module import was mistaken for a layering edge: {edges:?}"
    );
}

/// A test-only module gated by a compound `#[cfg(all(test, feature = "…"))]`
/// predicate — the spelling the `serde` replay-compat modules in `event.rs` and
/// `state/apply.rs` use — must be stripped like a plain `#[cfg(test)]` module,
/// so its `use crate::…` imports are never scanned as production layering edges.
#[test]
fn strips_feature_gated_test_modules() {
    let src = "#[cfg(all(test, feature = \"serde\"))]\nmod compat {\n    use crate::engine::X;\n}\nuse crate::model::Y;\n";
    let edges = edges_of_source("feel", src);
    assert!(
        !has_edge(&edges, "engine"),
        "a `#[cfg(all(test, feature = ...))]` module leaked a test-only edge: {edges:?}"
    );
    assert!(
        has_edge(&edges, "model"),
        "stripping the feature-gated test module swallowed real code after it: {edges:?}"
    );
}

/// The `test`-in-predicate check must be whole-word: a `cfg` key that merely
/// *contains* the substring `test` (e.g. `latest`) is not the `test`
/// configuration, so its module must NOT be stripped and its edges stay visible.
#[test]
fn cfg_containing_test_substring_is_not_stripped() {
    let src = "#[cfg(feature = latest)]\nmod real {\n    use crate::engine::X;\n}\nuse crate::model::Y;\n";
    let edges = edges_of_source("feel", src);
    assert!(
        has_edge(&edges, "engine") && has_edge(&edges, "model"),
        "a `latest` cfg key was mis-read as `test` and its module stripped: {edges:?}"
    );
}

/// A production `cfg` predicate that merely *mentions* `test` — `not(test)` or
/// `any(test, …)` compiles the item WITHOUT the test config — must NOT be
/// stripped, or a forbidden layering edge inside it would silently bypass the
/// guard. Only predicates that *imply* `test` are test-only.
#[test]
fn production_cfg_predicates_mentioning_test_are_not_stripped() {
    for pred in ["not(test)", "any(test, feature = \"serde\")"] {
        let src = format!(
            "#[cfg({pred})]\nmod prod {{\n    use crate::engine::X;\n}}\nuse crate::model::Y;\n"
        );
        let edges = edges_of_source("feel", &src);
        assert!(
            has_edge(&edges, "engine"),
            "production `#[cfg({pred})]` module was wrongly stripped, hiding its edge: {edges:?}"
        );
        assert!(
            has_edge(&edges, "model"),
            "stripping the `#[cfg({pred})]` module swallowed real code after it: {edges:?}"
        );
    }
}

/// `cfg_predicate_is_test_only` classifies predicates by whether they imply the
/// `test` configuration, not by a bare substring match: `test` / `all(test, …)`
/// (any nesting) are test-only; `not(test)`, `any(test, …)`, a plain feature
/// atom, and `latest` are not.
#[test]
fn cfg_predicate_test_only_classification() {
    let test_only = [
        "test",
        "all(test, feature = \"serde\")",
        "all(feature = \"x\", test)",
        "any(all(test, unix), all(test, windows))",
        "all(test, not(feature = \"x\"))",
    ];
    for p in test_only {
        assert!(
            cfg_predicate_is_test_only(p.as_bytes()),
            "`{p}` should be classified test-only"
        );
    }
    let not_test_only = [
        "not(test)",
        "any(test, feature = \"serde\")",
        "feature = \"serde\"",
        "unix",
        "feature = latest",
        "all(feature = \"a\", feature = \"b\")",
    ];
    for p in not_test_only {
        assert!(
            !cfg_predicate_is_test_only(p.as_bytes()),
            "`{p}` should NOT be classified test-only"
        );
    }
}

/// `module_of` derives the sub-module from the Rust module path's first child
/// component, so the anticipated `state` split into directory sub-modules
/// (`state/apply/mod.rs`, `state/apply/<family>.rs`) still classifies as
/// `state::apply` and keeps its `state::apply -> event` exception — while
/// `state/mod.rs` collapses to the module root.
#[test]
fn module_of_derives_submodule_from_module_path() {
    let cases: &[(&str, &str, Option<&str>)] = &[
        ("event.rs", "event", None),
        ("state/mod.rs", "state", None),
        ("state/apply.rs", "state", Some("apply")),
        ("state/apply/mod.rs", "state", Some("apply")),
        ("state/apply/transitions.rs", "state", Some("apply")),
        ("state/types.rs", "state", Some("types")),
    ];
    for (path, module, sub) in cases {
        let (m, s) = module_of(Path::new(path));
        assert_eq!(
            (m.as_str(), s.as_deref()),
            (*module, *sub),
            "module_of({path:?}) misclassified"
        );
    }
}

/// A raw-identifier alias (`as r#type`) must be fully consumed so it cannot
/// stall the enclosing brace group and drop the siblings after it. `event`
/// importing `state` (aliased to the raw keyword `r#type`) *and* `engine` via
/// `use crate::{state::apply as r#type, engine::Engine}` must still surface the
/// backward `engine` edge hidden after the raw alias.
#[test]
fn raw_identifier_alias_does_not_drop_later_leaves() {
    let src = "use crate::{state::apply as r#type, engine::Engine};\n";
    let edges = edges_of_source("event", src);
    assert!(
        edges.iter().any(|e| e.tgt_module == "state"),
        "the raw-aliased brace entry itself was lost: {edges:?}"
    );
    let engine = edges
        .iter()
        .find(|e| e.tgt_module == "engine")
        .expect("the entry after a raw-identifier alias was silently dropped");
    assert!(
        edge_allowed(&engine.src_module, None, &engine.tgt_module, None).is_err(),
        "a backward edge hidden after a raw-identifier alias slipped past"
    );
}

/// A raw-identifier path segment (`crate::r#type::Thing`) names the module
/// `type`; `read_ident` must strip the `r#` prefix so the edge resolves to the
/// real module name rather than a spurious `r#type`.
#[test]
fn raw_identifier_path_segment_strips_prefix() {
    let (segs, _) = parse_use_tree(b"r#state::apply::Thing;", 0);
    assert_eq!(
        segs,
        vec![vec![
            "state".to_string(),
            "apply".to_string(),
            "Thing".to_string()
        ]],
        "a raw-identifier path segment kept its `r#` prefix"
    );
}

/// A test-only module carrying a visibility modifier — `#[cfg(test)] pub(crate)
/// mod tests { … }` — is still test-only and must be stripped; without parsing
/// the optional visibility prefix its `use crate::…` imports would leak into
/// the layering scan as production dependencies.
#[test]
fn strips_visibility_qualified_test_modules() {
    for vis in [
        "pub ",
        "pub(crate) ",
        "pub(super) ",
        "pub(in crate::state) ",
    ] {
        let src = format!(
            "#[cfg(test)]\n{vis}mod tests {{\n    use crate::engine::X;\n}}\nuse crate::model::Y;\n"
        );
        let edges = edges_of_source("feel", &src);
        assert!(
            !has_edge(&edges, "engine"),
            "a `#[cfg(test)] {vis}mod` leaked a test-only edge: {edges:?}"
        );
        assert!(
            has_edge(&edges, "model"),
            "stripping the `{vis}mod` test module swallowed real code after it: {edges:?}"
        );
    }
}

/// The external, visibility-qualified form `#[cfg(test)] pub mod name;` gates a
/// whole file; `match_cfg_test_mod` must see past the `pub` so the declaration
/// is recognised and its file collected as test-only.
#[test]
fn matches_visibility_qualified_external_test_mod() {
    let src = "#[cfg(test)] pub mod tests;\n";
    let after = match_cfg_test_mod(src.as_bytes(), 0)
        .expect("a `#[cfg(test)] pub mod name;` declaration was not recognised");
    assert!(
        src[..after].ends_with("tests"),
        "the external module name was misparsed: {:?}",
        &src[..after]
    );
}

/// A *root* brace group (`use {crate::a, crate::b};`) — with no path before the
/// `{` — must be expanded so every leaf is checked. The old scanner read an
/// empty identifier at the leading `{`, resolved no root, and dropped the whole
/// declaration, letting a backward dependency bypass the guard entirely.
#[test]
fn extracts_root_brace_use_tree_entries() {
    let src = "use {crate::event::Event, crate::model::Thing};\n";
    let edges = edges_of_source("feel", src);
    assert!(
        has_edge(&edges, "event"),
        "a leaf of a root brace use-tree was ignored: {edges:?}"
    );
    assert!(
        has_edge(&edges, "model"),
        "a second root brace use-tree leaf was ignored: {edges:?}"
    );
}

/// A backward edge hidden in a root brace group must reach the real matcher —
/// the whole point of expanding the root group. `event` importing `engine` via
/// `use {crate::engine::Engine};` is a forbidden L3 -> L6 edge.
#[test]
fn root_brace_use_tree_backward_edge_is_rejected() {
    let src = "use {crate::engine::Engine};\n";
    let edges = edges_of_source("event", src);
    let engine = edges
        .iter()
        .find(|e| e.tgt_module == "engine")
        .expect("root-brace-wrapped backward edge should be extracted");
    assert!(
        edge_allowed(&engine.src_module, None, &engine.tgt_module, None).is_err(),
        "a backward edge hidden in a root brace use-tree slipped past the matcher"
    );
}

/// Each element of a root brace group is rooted independently, so `self`- and
/// `super`-relative elements resolve against the file's own module path just as
/// a bare relative import would — a backward edge cannot hide behind a mixed
/// root group.
#[test]
fn root_brace_use_tree_resolves_relative_elements() {
    // From `state/apply.rs`: `super::super::engine` climbs apply -> state ->
    // crate root, so the second leaf is the forbidden `engine` edge.
    let src = "use {self::helper::H, super::super::engine::Engine};\n";
    let edges = edges_of_source_at(&["state", "apply"], src);
    assert!(
        edges.iter().any(|e| e.tgt_module == "engine"),
        "a `super`-relative leaf of a root brace use-tree was lost: {edges:?}"
    );
}

/// An external or crate-root-re-export element in a root brace group must be
/// skipped without discarding its siblings — the group scan stays aligned past
/// a `serde::…` element and still finds the following `crate::…` module leaf.
#[test]
fn root_brace_use_tree_skips_external_elements() {
    let src = "use {serde::Serialize, crate::model::Thing};\n";
    let edges = edges_of_source("feel", src);
    assert!(
        has_edge(&edges, "model"),
        "a `crate::` leaf after an external root-group element was dropped: {edges:?}"
    );
    assert!(
        !has_edge(&edges, "serde"),
        "an external root-group element was mistaken for a module edge: {edges:?}"
    );
}

/// A test-only module whose `#[cfg(test)]` carries whitespace inside the
/// attribute (`#[cfg (test)]`) is still test-only and must be stripped; the
/// matcher tokenizes the attribute rather than requiring the exact `#[cfg(`
/// spelling, so its `use crate::…` imports do not leak into the layering scan.
#[test]
fn strips_spaced_cfg_test_modules() {
    let src = "#[cfg (test)]\nmod tests {\n    use crate::engine::X;\n}\nuse crate::model::Y;\n";
    let edges = edges_of_source("feel", src);
    assert!(
        !has_edge(&edges, "engine"),
        "a `#[cfg (test)]` module leaked a test-only edge: {edges:?}"
    );
    assert!(
        has_edge(&edges, "model"),
        "stripping the `#[cfg (test)]` module swallowed real code after it: {edges:?}"
    );
}

/// An intervening outer attribute between the `#[cfg(test)]` and the `mod`
/// keyword — `#[cfg(test)] #[allow(dead_code)] mod tests { … }` — must not stop
/// the module being recognised as test-only; otherwise its imports would be
/// scanned as production layering edges.
#[test]
fn strips_test_modules_with_intervening_attributes() {
    let src = "#[cfg(test)]\n#[allow(dead_code)]\nmod tests {\n    use crate::engine::X;\n}\nuse crate::model::Y;\n";
    let edges = edges_of_source("feel", src);
    assert!(
        !has_edge(&edges, "engine"),
        "a test module behind an intervening attribute leaked an edge: {edges:?}"
    );
    assert!(
        has_edge(&edges, "model"),
        "stripping the attributed test module swallowed real code after it: {edges:?}"
    );
}

/// The spaced-and-attributed external form
/// `#[cfg (test)] #[allow(dead_code)] pub mod name;` must also be recognised by
/// `match_cfg_test_mod` so the whole file it gates is collected as test-only.
#[test]
fn matches_spaced_and_attributed_external_test_mod() {
    let src = "#[cfg (test)] #[allow(dead_code)] pub mod tests;\n";
    let after = match_cfg_test_mod(src.as_bytes(), 0)
        .expect("a spaced/attributed external test mod was not recognised");
    assert!(
        src[..after].ends_with("tests"),
        "the external module name was misparsed: {:?}",
        &src[..after]
    );
}

/// A production module must NOT be stripped just because an intervening
/// attribute follows a non-test `#[cfg(...)]`: `#[cfg(feature = "x")]` keeps its
/// imports visible even with an `#[allow(...)]` before `mod`.
#[test]
fn keeps_production_module_with_intervening_attribute() {
    let src = "#[cfg(feature = \"x\")]\n#[allow(dead_code)]\nmod real {\n    use crate::engine::X;\n}\nuse crate::model::Y;\n";
    let edges = edges_of_source("feel", src);
    assert!(
        has_edge(&edges, "engine"),
        "a production module behind an attribute was wrongly stripped: {edges:?}"
    );
}

/// An external `#[cfg(test)] mod tests;` that resolves to a *directory* module
/// (`<dir>/tests/mod.rs`) gates not just `mod.rs` but the whole subtree: child
/// modules inherit the parent's `cfg(test)`, so a later test-file split such as
/// `<dir>/tests/activation.rs` must also be excluded. Otherwise that descendant
/// would be scanned as production and could reject a valid test-only import,
/// producing a false layering failure.
#[test]
fn external_test_mod_gates_directory_subtree() {
    let files = vec![
        PathBuf::from("engine/mod.rs"),
        PathBuf::from("engine/tests/mod.rs"),
        PathBuf::from("engine/tests/activation.rs"),
        PathBuf::from("engine/tests/nested/deep.rs"),
        PathBuf::from("engine/other.rs"),
    ];
    let dir = Path::new("engine");
    let gated: BTreeSet<&PathBuf> = gated_files_for_external_mod(dir, "tests", &files)
        .into_iter()
        .collect();
    assert!(
        gated.contains(&PathBuf::from("engine/tests/mod.rs")),
        "the directory module's `mod.rs` was not gated: {gated:?}"
    );
    assert!(
        gated.contains(&PathBuf::from("engine/tests/activation.rs")),
        "a descendant of the gated test subtree was left to be scanned: {gated:?}"
    );
    assert!(
        gated.contains(&PathBuf::from("engine/tests/nested/deep.rs")),
        "a deeply nested descendant of the gated test subtree was not gated: {gated:?}"
    );
    assert!(
        !gated.contains(&PathBuf::from("engine/other.rs")),
        "an unrelated sibling file was wrongly gated: {gated:?}"
    );
    assert!(
        !gated.contains(&PathBuf::from("engine/mod.rs")),
        "the declaring parent module was wrongly gated: {gated:?}"
    );
}

/// The flat external form `#[cfg(test)] mod tests;` resolving to `<dir>/tests.rs`
/// gates that file and, under Rust 2018, any sibling `<dir>/tests/` submodule
/// directory it owns — the whole subtree, not merely the flat file.
#[test]
fn external_test_mod_gates_flat_file_and_its_submodules() {
    let files = vec![
        PathBuf::from("engine/lib.rs"),
        PathBuf::from("engine/tests.rs"),
        PathBuf::from("engine/tests/helpers.rs"),
        PathBuf::from("engine/testsuite.rs"),
    ];
    let dir = Path::new("engine");
    let gated: BTreeSet<&PathBuf> = gated_files_for_external_mod(dir, "tests", &files)
        .into_iter()
        .collect();
    assert!(
        gated.contains(&PathBuf::from("engine/tests.rs")),
        "the flat `<dir>/tests.rs` file was not gated: {gated:?}"
    );
    assert!(
        gated.contains(&PathBuf::from("engine/tests/helpers.rs")),
        "a 2018-style submodule of the flat test file was not gated: {gated:?}"
    );
    assert!(
        !gated.contains(&PathBuf::from("engine/testsuite.rs")),
        "a same-prefix but distinct sibling (`testsuite.rs`) was wrongly gated: {gated:?}"
    );
}

/// An external `#[cfg(test)] mod name;` declared inside a *flat* module file
/// such as `src/bpmn.rs` resolves its child to `src/bpmn/name.rs` — the
/// subdirectory the flat file owns — not `src/name.rs`. Resolving against the
/// declaring file's bare parent would look in the wrong directory, leaving the
/// real test file scanned as production.
#[test]
fn module_source_dir_flat_file_owns_named_subdirectory() {
    let src_root = Path::new("src");
    let dir = module_source_dir(Path::new("src/bpmn.rs"), src_root);
    assert_eq!(
        dir,
        PathBuf::from("src/bpmn"),
        "a flat module file must resolve children under `<stem>/`: {dir:?}"
    );
    assert_eq!(
        module_source_dir(Path::new("src/engine/state.rs"), src_root),
        PathBuf::from("src/engine/state"),
        "a nested flat module file must resolve children under its own `<stem>/`"
    );
}

/// A directory owner — `mod.rs`, `lib.rs`, or `main.rs` — resolves `mod name;`
/// to `<parent>/name.rs`, so its module source directory is its own parent.
#[test]
fn module_source_dir_directory_owner_uses_parent() {
    let src_root = Path::new("src");
    assert_eq!(
        module_source_dir(Path::new("src/engine/mod.rs"), src_root),
        PathBuf::from("src/engine"),
        "`mod.rs` must resolve children in its own directory"
    );
    assert_eq!(
        module_source_dir(Path::new("src/lib.rs"), src_root),
        PathBuf::from("src"),
        "`lib.rs` must resolve children in the crate root"
    );
    assert_eq!(
        module_source_dir(Path::new("src/main.rs"), src_root),
        PathBuf::from("src"),
        "`main.rs` must resolve children in the crate root"
    );
}
