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

/// Points that order everything before them on the writer: awaiting a commit,
/// a writer-queued (FIFO) message, or closing/reopening the journal (the writer
/// drains on drop).
const SYNC_POINTS: &[&str] = &[
    "blocking_wait(",
    ".wait(",
    "snapshot_and_rotate",
    "rotate",
    "drop(journal",
    "flush",
    "shutdown",
    "open_segmented",
    "Journal::open",
    "recover",
];

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

/// Whether the statement ending at `apply_at` binds the returned commit to `_`:
/// `let _ = <expr>.apply_command…` or `let (<pat>, _) = <expr>.apply_command…`.
fn discards_commit(body: &str, apply_at: usize) -> bool {
    let stmt_start = body[..apply_at].rfind([';', '{', '}']).map_or(0, |n| n + 1);
    let stmt: String = body[stmt_start..apply_at]
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    stmt.starts_with("let_=") || (stmt.starts_with("let(") && stmt.contains(",_)="))
}

/// Whether the journal `apply_at` is called on was built in memory
/// (`Journal::in_memory*`): it has no writer thread, so its commit is always
/// already resolved and nothing can race it.
fn is_in_memory_receiver(body: &str, apply_at: usize) -> bool {
    let recv: String = body[..apply_at]
        .trim_end()
        .chars()
        .rev()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    if recv.is_empty() {
        return false;
    }
    ["let mut ", "let "].iter().any(|kw| {
        let binding = format!("{kw}{recv} =");
        body[..apply_at].rfind(&binding).is_some_and(|at| {
            let stmt_end = body[at..].find(';').map_or(body.len(), |n| at + n);
            body[at..stmt_end].contains("in_memory")
        })
    })
}

/// The `(fn name, offending observation)` pairs in `src`: a disk observation
/// that follows a discarded commit with no sync point in between.
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
                let synced = first(SYNC_POINTS).is_some_and(|(sync_at, _)| sync_at < obs_at);
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

    // An in-memory journal has no writer thread: its commit is always Ready.
    let in_memory = r#"
        fn t() {
            let mut j = crate::journal::Journal::in_memory_partition(0);
            let (deploy, _) = j.apply_command(Command::DeployProcess(demo())).unwrap();
            let got = read_segment_events(&path).unwrap();
        }
    "#;
    assert!(violations(in_memory).is_empty());

    // Text inside string literals and comments is not code.
    let literal = r#"
        fn t() {
            // let _ = journal.apply_command(x); shared.uncompacted_usage();
            let s = "let _ = j.apply_command(x); fs::metadata(p)";
        }
    "#;
    assert!(violations(literal).is_empty());
}
