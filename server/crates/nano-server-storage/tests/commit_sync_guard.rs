//! Defect-class guard for #1477 / #1462: **observing on-disk journal state
//! before awaiting the write that produced it.**
//!
//! The group-commit writer thread appends asynchronously: `apply_command*`
//! returns a `Commit` that resolves only once the events are written and
//! fsynced. A test that discards that commit (`let _ = journal.apply_command(..)`)
//! and then samples the disk (`uncompacted_usage`, `fs::metadata`, a segment
//! read, …) races the writer. It passes on an idle machine and fails under
//! parallel-test load — exactly how
//! `seglog::tests::uncompacted_usage_does_not_double_count_a_seal_mid_sample`
//! failed intermittently (it saw an empty active segment).
//!
//! The fix was to order the observation after a real sync point
//! (`Commit::blocking_wait`, or a writer-ordered message such as the rotation
//! inside `snapshot_and_rotate`, which queues FIFO behind every pending append).
//! This test enforces that contract statically across every source file of the
//! crate, so the same class cannot be reintroduced in any other test: after a
//! discarded commit, a disk observation must be preceded by a sync point.
//!

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "non-production target (integration tests / examples)"
)]

use std::path::Path;

/// Calls whose returned `Commit` is the durability handle of the write.
const APPLY: &str = ".apply_command";

/// Reads of on-disk state that a still-pending write can make stale.
const DISK_OBSERVATIONS: &[&str] = &[
    "uncompacted_usage(",
    "usage_with_active(",
    "fs::metadata(",
    "fs::read(",
    "fs::read_to_string(",
    "fs::read_dir(",
    "read_segment_events",
    ".metadata()",
];

/// Method calls on the *affected journal* that order everything before them on
/// its writer: a writer-queued (FIFO) message, or closing/reopening that
/// journal (the writer drains on drop). A sync point must be tied to the same
/// receiver as the discarded commit — `barrier.wait()`, or opening *another*
/// journal, says nothing about this write.
const RECEIVER_SYNC_METHODS: &[&str] = &[
    "snapshot_and_rotate",
    "streaming_snapshot_and_rotate",
    "rotate",
    "flush",
    "shutdown",
];

/// Calls that await a commit regardless of the receiver they are invoked on:
/// the commit's own `blocking_wait`, or `drop(journal…` (the writer drains on
/// drop). Matched as *calls* — an identifier-bounded name followed by `(` — so
/// identifiers that merely contain a name (`rotated`, `flushed`) are never
/// mistaken for a sync point.
const FREE_SYNC_CALLS: &[&str] = &["blocking_wait", "drop"];

fn is_ident(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_'
}

/// Offset of the first call to any of `names` in `hay`: the name must not be
/// preceded by an identifier byte and must be followed (after optional
/// whitespace) by `(`.
fn first_call(hay: &str, names: &[&str]) -> Option<usize> {
    let b = hay.as_bytes();
    names
        .iter()
        .filter_map(|name| {
            let mut from = 0;
            while let Some(off) = hay[from..].find(name) {
                let at = from + off;
                from = at + name.len();
                if at > 0 && is_ident(b[at - 1]) {
                    continue;
                }
                let rest = hay[from..].trim_start();
                if !rest.starts_with('(') {
                    continue;
                }
                return Some(at);
            }
            None
        })
        .min()
}

/// The identifier a `.method(` call at `dot_at` is invoked on (the text
/// immediately before the dot), or `None` when there is no receiver.
fn receiver_before(hay: &str, dot_at: usize) -> Option<String> {
    if dot_at == 0 || hay.as_bytes()[dot_at - 1] == b'.' {
        return None; // a path (`Journal::open`) or a float, not a receiver
    }
    let recv: String = hay[..dot_at]
        .trim_end()
        .chars()
        .rev()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    (!recv.is_empty()).then_some(recv)
}

/// Whether `region` contains a sync point for the journal `recv` was applied
/// on, returning its offset: a writer-ordered method called *on that same
/// receiver*, or a receiver-independent await (`blocking_wait`, `drop(journal…`).
fn sync_point_at(region: &str, recv: &str) -> Option<usize> {
    let b = region.as_bytes();
    let tied = RECEIVER_SYNC_METHODS.iter().filter_map(|name| {
        let mut from = 0;
        while let Some(off) = region[from..].find(name) {
            let at = from + off;
            from = at + name.len();
            if at > 0 && is_ident(b[at - 1]) {
                continue;
            }
            if !region[from..].trim_start().starts_with('(') {
                continue;
            }
            // `.name(` — the receiver is the identifier before the dot.
            if b[at - 1] == b'.' && receiver_before(region, at - 1).as_deref() == Some(recv) {
                return Some(at);
            }
        }
        None
    });
    tied.chain(
        FREE_SYNC_CALLS
            .iter()
            .filter_map(|name| first_call(region, std::slice::from_ref(name))),
    )
    .min()
}

/// The index of the first byte of `region` that is NOT reachable on every
/// control-flow path: the start of the smallest enclosing `{…}` that follows a
/// conditional keyword (`if`/`else`/`match`/`while`/`for`/`loop`) or a `?` /
/// `=>` (an early-returning call or a match arm) — `None` when the whole
/// region is unconditional. A sync point at or after this offset may never
/// execute, so it cannot order the observation; one *before* it still can.
fn conditional_from(region: &str) -> Option<usize> {
    let b = region.as_bytes();
    let mut best = region.len();
    for kw in ["if", "else", "match", "while", "for", "loop"] {
        let mut from = 0;
        while let Some(off) = region[from..].find(kw) {
            let at = from + off;
            from = at + kw.len();
            let prev_ok = at == 0 || !is_ident(b[at - 1]);
            let next_ok = from >= region.len() || !is_ident(b[from]);
            if !(prev_ok && next_ok) {
                continue;
            }
            if let Some(rel) = region[from..].find('{') {
                best = best.min(from + rel);
            }
        }
    }
    for (i, &c) in b.iter().enumerate() {
        if c == b'?' {
            best = best.min(i);
        }
        if c == b'=' && b.get(i + 1) == Some(&b'>') {
            best = best.min(i);
        }
    }
    (best < region.len()).then_some(best)
}

/// Replaces string/char literals and comments with spaces (keeping byte
/// offsets and newlines), so braces and keywords inside them are ignored.
fn strip_literals(src: &str) -> String {
    let b = src.as_bytes();
    let mut out = b.to_vec();
    let mut i = 0;
    let blank = |out: &mut Vec<u8>, from: usize, to: usize| {
        for c in &mut out[from..to] {
            if *c != b'\n' {
                *c = b' ';
            }
        }
    };
    while i < b.len() {
        match b[i] {
            b'/' if b.get(i + 1) == Some(&b'/') => {
                let end = src[i..].find('\n').map_or(b.len(), |n| i + n);
                blank(&mut out, i, end);
                i = end;
            }
            b'/' if b.get(i + 1) == Some(&b'*') => {
                let end = src[i + 2..].find("*/").map_or(b.len(), |n| i + 2 + n + 2);
                blank(&mut out, i, end);
                i = end;
            }
            b'r' if matches!(b.get(i + 1), Some(b'#' | b'"'))
                && (i == 0 || !(b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_')) =>
            {
                let hashes = b[i + 1..].iter().take_while(|&&c| c == b'#').count();
                if b.get(i + 1 + hashes) != Some(&b'"') {
                    i += 1;
                    continue;
                }
                let close = format!("\"{}", "#".repeat(hashes));
                let body = i + 2 + hashes;
                let end = src[body..]
                    .find(&close)
                    .map_or(b.len(), |n| body + n + close.len());
                blank(&mut out, i, end);
                i = end;
            }
            b'"' => {
                let mut j = i + 1;
                while j < b.len() && b[j] != b'"' {
                    j += if b[j] == b'\\' { 2 } else { 1 };
                }
                let end = (j + 1).min(b.len());
                blank(&mut out, i, end);
                i = end;
            }
            b'\'' => {
                // A char literal ('x', '\n', '\''); a lifetime has no closing quote.
                let end = if b.get(i + 1) == Some(&b'\\') {
                    src[i + 2..].find('\'').map(|n| i + 2 + n + 1)
                } else if b.get(i + 2) == Some(&b'\'') {
                    Some(i + 3)
                } else {
                    None
                };
                match end {
                    Some(end) => {
                        blank(&mut out, i, end);
                        i = end;
                    }
                    None => i += 1,
                }
            }
            _ => i += 1,
        }
    }
    String::from_utf8(out).expect("blanking keeps UTF-8 boundaries of ASCII")
}

/// Every `fn` body in `src` (literal-stripped) as `(name, body)`.
fn fn_bodies(src: &str) -> Vec<(String, String)> {
    let s = strip_literals(src);
    let bytes = s.as_bytes();
    let mut out = Vec::new();
    let mut from = 0;
    while let Some(off) = s[from..].find("fn ") {
        let at = from + off;
        from = at + 3;
        if at > 0 && (bytes[at - 1].is_ascii_alphanumeric() || bytes[at - 1] == b'_') {
            continue;
        }
        let name: String = s[at + 3..]
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        // The body opens at the first `{` not preceded by a `;` (a bodyless
        // trait method declaration ends at `;`).
        let Some(rel) = s[at..].find(['{', ';']) else {
            break;
        };
        let open = at + rel;
        if bytes[open] == b';' {
            continue;
        }
        let mut depth = 0usize;
        let mut close = open;
        for (k, &c) in bytes[open..].iter().enumerate() {
            match c {
                b'{' => depth += 1,
                b'}' => {
                    depth -= 1;
                    if depth == 0 {
                        close = open + k;
                        break;
                    }
                }
                _ => {}
            }
        }
        out.push((name, s[open..=close].to_string()));
        // Nested fns are scanned as part of their parent; resume after it.
        from = close.max(from);
    }
    out
}

/// Whether the statement ending at `apply_at` drops the returned commit
/// without awaiting it: a bare expression statement (nothing bound), a
/// `_`-prefixed binding (`let _ =`, `let _r =`), or a tuple whose commit (last)
/// slot is `_`-prefixed (`let (e, _) =`, `let (_e, _commit) =`).
fn discards_commit(body: &str, apply_at: usize) -> bool {
    let stmt_start = body[..apply_at].rfind([';', '{', '}']).map_or(0, |n| n + 1);
    let stmt: String = body[stmt_start..apply_at]
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    let Some(pat) = stmt.strip_prefix("let") else {
        // No binding at all: an expression statement discards the commit,
        // unless the value is assigned/returned/passed on.
        return !stmt.contains('=')
            && !stmt.starts_with("return")
            && stmt.matches('(').count() == stmt.matches(')').count();
    };
    let pat = pat.strip_prefix("mut").unwrap_or(pat);
    let Some(eq) = pat.find('=') else {
        return false;
    };
    let pat = pat[..eq].split(':').next().unwrap_or("");
    let last = match pat.strip_prefix('(').and_then(|p| p.strip_suffix(')')) {
        // The commit is the last tuple slot; a trailing comma leaves an empty
        // final component, so skip empty slots.
        Some(inner) => inner.rsplit(',').find(|s| !s.is_empty()).unwrap_or(""),
        None => pat,
    };
    last.trim_start_matches("mut").starts_with('_')
}

/// Whether the journal `apply_at` is called on was built in memory
/// (`Journal::in_memory*`) and never reassigned: it has no writer thread, so
/// its commit is always already resolved and nothing can race it. A later
/// reassignment (`j = Journal::open(&path)`) invalidates the exemption — from
/// then on `j` has an asynchronous disk writer.
fn is_in_memory_receiver(body: &str, apply_at: usize) -> bool {
    let Some(recv) = receiver_before(body, apply_at) else {
        return false;
    };
    ["let mut ", "let "].iter().any(|kw| {
        let binding = format!("{kw}{recv} =");
        body[..apply_at].rfind(&binding).is_some_and(|at| {
            let stmt_end = body[at..].find(';').map_or(body.len(), |n| at + n);
            // No reassignment of the receiver between its binding and the apply.
            let reassigned = body[stmt_end..apply_at].split([';', '{', '}']).any(|stmt| {
                let stmt: String = stmt.chars().filter(|c| !c.is_whitespace()).collect();
                stmt.strip_prefix(&recv)
                    .is_some_and(|rest| rest.starts_with('=') && !rest.starts_with("=="))
            });
            !reassigned && body[at..stmt_end].contains("in_memory")
        })
    })
}

/// The `(fn name, offending observation)` pairs in `src`: a disk observation
/// that follows a discarded commit with no sync point in between. A sync point
/// must be tied to the *affected journal* (or await the commit itself) and must
/// execute on every path to the observation — an unrelated `barrier.wait()`, a
/// different journal's rotation, or a rotation inside `if …` orders nothing.
fn violations(src: &str) -> Vec<(String, String)> {
    let mut found = Vec::new();
    for (name, body) in fn_bodies(src) {
        let mut search = 0;
        while let Some(off) = body[search..].find(APPLY) {
            let apply_at = search + off;
            search = apply_at + APPLY.len();
            if !discards_commit(&body, apply_at) || is_in_memory_receiver(&body, apply_at) {
                continue;
            }
            let tail = &body[search..];
            let first = |needles: &[&'static str]| {
                needles
                    .iter()
                    .filter_map(|n| tail.find(n).map(|p| (p, *n)))
                    .min()
            };
            if let Some((obs_at, obs)) = first(DISK_OBSERVATIONS) {
                let recv = receiver_before(&body, apply_at).unwrap_or_default();
                // A sync point orders the observation only when it is tied to
                // the affected journal (or awaits the commit itself) AND
                // executes on every path to it: nothing conditional (a `?`, an
                // `if`/`match`/loop body, a match arm) may sit between the sync
                // point and the observation. Conditionals *before* the sync
                // point do not weaken it — every path still reaches it.
                let region = &tail[..obs_at];
                let synced = sync_point_at(region, &recv).is_some_and(|sync_at| {
                    conditional_from(region).is_none_or(|cut| sync_at < cut)
                });
                if !synced {
                    found.push((name.clone(), obs.to_string()));
                    break;
                }
            }
        }
    }
    found
}

#[test]
fn no_disk_observation_races_a_discarded_commit() {
    let src_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut offenders = Vec::new();
    let mut scanned = 0;
    for entry in std::fs::read_dir(&src_dir).expect("read src dir") {
        let path = entry.expect("dir entry").path();
        if path.extension().is_some_and(|e| e == "rs") {
            let src = std::fs::read_to_string(&path).expect("read source");
            scanned += 1;
            for (func, obs) in violations(&src) {
                offenders.push(format!("{}::{func} reads `{obs}`", path.display()));
            }
        }
    }
    assert!(
        scanned > 0,
        "no sources scanned under {}",
        src_dir.display()
    );
    assert!(
        offenders.is_empty(),
        "on-disk state observed after a DISCARDED commit with no sync point in between \
         (the group-commit writer may not have appended yet — #1477). Bind the commit and \
         call `blocking_wait()` before observing the disk:\n  {}",
        offenders.join("\n  ")
    );
}

/// The guard must flag the exact pre-fix shape of the #1477 test (RED) and
/// accept the fixed shape (GREEN), or it guards nothing.
#[test]
fn guard_flags_the_1477_shape_and_accepts_the_fix() {
    let racy = r#"
        fn uncompacted_usage_does_not_double_count_a_seal_mid_sample() {
            let (mut journal, recovery) = Journal::open_segmented(&dir).expect("open");
            let shared = Arc::clone(&recovery.shared);
            let _ = journal
                .apply_command(Command::DeployProcess(demo()))
                .unwrap();
            let expected = shared.uncompacted_usage();
            assert!(expected.1 > 0);
        }
    "#;
    assert_eq!(
        violations(racy),
        vec![(
            "uncompacted_usage_does_not_double_count_a_seal_mid_sample".to_string(),
            "uncompacted_usage(".to_string()
        )]
    );

    let tuple_discard = r#"
        fn t() {
            let (_events, _) = journal.apply_command_ref_at(cmd, 0).unwrap();
            let len = fs::metadata(&path).unwrap().len();
        }
    "#;
    assert_eq!(violations(tuple_discard).len(), 1);

    let fixed = r#"
        fn uncompacted_usage_does_not_double_count_a_seal_mid_sample() {
            let (_, commit) = journal
                .apply_command(Command::DeployProcess(demo()))
                .unwrap();
            commit.blocking_wait();
            let expected = shared.uncompacted_usage();
        }
    "#;
    assert!(violations(fixed).is_empty());

    let rotated = r#"
        fn t() {
            let _ = journal.apply_command(Command::DeployProcess(demo())).unwrap();
            let (snap, covered) = journal.snapshot_and_rotate().expect("rotate");
            let bytes = fs::metadata(&p).unwrap().len();
        }
    "#;
    assert!(violations(rotated).is_empty());

    // An identifier merely containing a sync name is not a sync point.
    let lookalike = r#"
        fn t() {
            let _ = journal.apply_command(c).unwrap();
            let rotated = flushed; let u = recovery.shared.uncompacted_usage();
        }
    "#;
    assert_eq!(violations(lookalike).len(), 1);

    // A bare expression statement and a `_`-prefixed commit slot both discard.
    let bare = r#"
        fn t() {
            journal.apply_command(c).unwrap();
            let u = shared.uncompacted_usage();
        }
    "#;
    assert_eq!(violations(bare).len(), 1);
    let named_unawaited = r#"
        fn t() {
            let (_e, _commit) = journal.apply_command(c).unwrap();
            let u = shared.uncompacted_usage();
        }
    "#;
    assert_eq!(violations(named_unawaited).len(), 1);
    let awaited_named = r#"
        fn t() {
            let (_e, commit) = journal.apply_command(c).unwrap();
            commit.blocking_wait();
            let u = shared.uncompacted_usage();
        }
    "#;
    assert!(violations(awaited_named).is_empty());

    // An in-memory journal has no writer thread: its commit is always Ready.
    let in_memory = r#"
        fn t() {
            let mut j = crate::journal::Journal::in_memory_partition(0);
            let (deploy, _) = j.apply_command(Command::DeployProcess(demo())).unwrap();
            let got = read_segment_events(&path).unwrap();
        }
    "#;
    assert!(violations(in_memory).is_empty());

    // …but reassigning it to a persistent journal revives the race.
    let reassigned = r#"
        fn t() {
            let mut j = Journal::in_memory();
            j = Journal::open(&path).unwrap();
            let _ = j.apply_command(c).unwrap();
            let u = shared.uncompacted_usage();
        }
    "#;
    assert_eq!(violations(reassigned).len(), 1);

    // A trailing comma still discards the commit slot (one line and multiline).
    let trailing_comma = r#"
        fn t() {
            let (_events, _,) = journal.apply_command(c).unwrap();
            let len = fs::metadata(&path).unwrap().len();
        }
    "#;
    assert_eq!(violations(trailing_comma).len(), 1);
    let trailing_comma_multiline = r#"
        fn t() {
            let (
                _events,
                _,
            ) = journal.apply_command(c).unwrap();
            let len = fs::metadata(&path).unwrap().len();
        }
    "#;
    assert_eq!(violations(trailing_comma_multiline).len(), 1);

    // A sync point must be tied to the affected journal: an unrelated
    // `barrier.wait()`, another journal's rotation, or reopening a *different*
    // journal orders nothing for this write.
    let unrelated_barrier = r#"
        fn t() {
            let _ = journal.apply_command(c).unwrap();
            barrier.wait();
            let u = shared.uncompacted_usage();
        }
    "#;
    assert_eq!(violations(unrelated_barrier).len(), 1);
    let other_journal = r#"
        fn t() {
            let _ = journal.apply_command(c).unwrap();
            other.snapshot_and_rotate();
            let other2 = Journal::open(&path).unwrap();
            let u = shared.uncompacted_usage();
        }
    "#;
    assert_eq!(violations(other_journal).len(), 1);

    // A sync point that may never execute orders nothing either.
    let conditional_barrier = r#"
        fn t() {
            let _ = journal.apply_command(c).unwrap();
            if false {
                journal.snapshot_and_rotate();
            }
            let u = shared.uncompacted_usage();
        }
    "#;
    assert_eq!(violations(conditional_barrier).len(), 1);
    let conditional_wait = r#"
        fn t() {
            let _ = journal.apply_command(c).unwrap();
            if flag { commit.blocking_wait(); }
            let u = shared.uncompacted_usage();
        }
    "#;
    assert_eq!(violations(conditional_wait).len(), 1);

    // …while the same calls, tied to the affected journal and unconditional,
    // are real sync points.
    let tied = r#"
        fn t() {
            let _ = journal.apply_command(c).unwrap();
            journal.snapshot_and_rotate();
            let u = shared.uncompacted_usage();
        }
    "#;
    assert!(violations(tied).is_empty());
    let dropped = r#"
        fn t() {
            let _ = journal.apply_command(c).unwrap();
            drop(journal);
            let u = shared.uncompacted_usage();
        }
    "#;
    assert!(violations(dropped).is_empty());

    // A sync point *before* a conditional still executes on every path: the
    // `?` only makes the suffix after it conditional.
    let synced_then_fallible = r#"
        fn t() {
            let _ = journal.apply_command(c).unwrap();
            journal.snapshot_and_rotate();
            let x = fallible()?;
            let u = shared.uncompacted_usage();
        }
    "#;
    assert!(violations(synced_then_fallible).is_empty());

    // Text inside string literals and comments is not code.
    let literal = r#"
        fn t() {
            // let _ = journal.apply_command(x); shared.uncompacted_usage();
            let s = "let _ = j.apply_command(x); fs::metadata(p)";
        }
    "#;
    assert!(violations(literal).is_empty());
}
