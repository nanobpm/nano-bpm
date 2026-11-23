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
    "barrier",
];

/// Receiver methods that sync only when they actually rotated: on a
/// non-segmented journal they return `None` without queuing anything, so the
/// result must be unwrapped (`.expect`/`.unwrap`/`?`) to prove a rotation ran.
const CHECKED_SYNC_METHODS: &[&str] = &["snapshot_and_rotate", "streaming_snapshot_and_rotate"];

fn is_ident(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_'
}

/// The full receiver expression a `.method(` call at `dot_at` is invoked on
/// (the field path immediately before the dot, e.g. `a.journal`), or `None`
/// when there is no plain receiver. The whole path is kept so `a.journal` and
/// `b.journal` are never conflated.
fn receiver_before(hay: &str, dot_at: usize) -> Option<String> {
    if dot_at == 0 || hay.as_bytes()[dot_at - 1] == b'.' {
        return None; // a path (`Journal::open`) or a float, not a receiver
    }
    let recv: String = hay[..dot_at]
        .trim_end()
        .chars()
        .rev()
        .take_while(|c| c.is_alphanumeric() || *c == '_' || *c == '.')
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    let recv = recv.trim_start_matches('.').to_string();
    (!recv.is_empty()).then_some(recv)
}

/// The statement containing byte `at` of `region` (between the nearest
/// `;`/`{`/`}` delimiters).
fn stmt_around(region: &str, at: usize) -> &str {
    let start = region[..at].rfind([';', '{', '}']).map_or(0, |n| n + 1);
    let end = region[at..]
        .find([';', '{', '}'])
        .map_or(region.len(), |n| at + n);
    &region[start..end]
}

/// Whether a sync call at `at` executes on every path that reaches the end of
/// `region`: it is not inside a block opened within `region` (an `if`/`match`/
/// loop body, a match arm, a closure body), not in a deferred closure
/// (`|| journal.rotate()`), and not short-circuited (`c && journal.flush()`).
/// Conditionals that *complete* before it, or a `?` before it, do not weaken
/// it: a path that skips it also skips the observation.
fn unconditional_at(region: &str, at: usize) -> bool {
    let mut depth = 0usize;
    for &c in &region.as_bytes()[..at] {
        match c {
            b'{' => depth += 1,
            // Closing a block opened before the region leaves depth at 0.
            b'}' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    let stmt = stmt_around(region, at);
    depth == 0 && !(stmt.contains('|') || stmt.contains("=>") || stmt.contains("&&"))
}

/// Whether `region` contains an unconditional sync point for the journal
/// `recv` was applied on, returning its offset: a writer-ordered method called
/// *on that same receiver* (a rotation must be unwrapped to prove it ran), or
/// `drop(<recv>)` when `recv` is bound *by value* in this function (dropping
/// the owned journal closes it, draining the writer; dropping a `&mut Journal`
/// is a no-op that orders nothing). Calls are matched identifier-bounded and
/// followed by `(`, so `rotated`/`flushed` never count. Unrelated drops and
/// waits on stale or foreign commits order nothing. A wait on a *later*
/// apply's commit is not a sync point: non-emptiness of that apply's events
/// cannot be proven statically, and an eventless later apply returns a ready
/// commit covering nothing — so only the discarded apply's own handle (checked
/// in `violations`), a writer-queued message, or a close/reopen proves the
/// write landed. `body` is the whole function text: the by-value binding may
/// precede the apply, which `region` starts after.
fn sync_point_at(region: &str, recv: &str, body: &str) -> Option<usize> {
    if recv.is_empty() {
        return None;
    }
    let b = region.as_bytes();
    let mut hits = Vec::new();
    for name in RECEIVER_SYNC_METHODS.iter().chain(["drop"].iter()) {
        let mut from = 0;
        while let Some(off) = region[from..].find(name) {
            let at = from + off;
            from = at + name.len();
            if (at > 0 && is_ident(b[at - 1])) || (from < b.len() && is_ident(b[from])) {
                continue;
            }
            let rest = region[from..].trim_start();
            if !rest.starts_with('(') {
                continue;
            }
            let dotted = at > 0 && b[at - 1] == b'.';
            let ok = match *name {
                "drop" => {
                    let arg: String = rest[1..]
                        .chars()
                        .take_while(|c| *c != ')')
                        .filter(|c| !c.is_whitespace())
                        .collect();
                    !dotted && arg == recv && bound_by_value(body, recv)
                }
                "barrier" => {
                    dotted
                        && receiver_before(region, at - 1).as_deref() == Some(recv)
                        && barrier_awaited(region, at)
                }
                _ => {
                    dotted
                        && receiver_before(region, at - 1).as_deref() == Some(recv)
                        && (!CHECKED_SYNC_METHODS.contains(name) || {
                            let chain = call_result_chain(region, at + name.len());
                            chain.contains(".expect(")
                                || chain.contains(".unwrap()")
                                || chain.contains('?')
                        })
                }
            };
            // A sync on a *shadowing* `recv` (`let journal = <other>;`) orders
            // the replacement journal's writer, not the discarded write's.
            if ok && unconditional_at(region, at) && !shadows(&region[..at], recv) {
                hits.push(at);
            }
        }
    }
    hits.into_iter().min()
}

/// Whether the `barrier()` call at `at` has its returned `Commit` awaited: an
/// unawaited barrier queues a message but orders nothing for the caller. The
/// wait is either chained (`.barrier().blocking_wait()`, `.wait()`,
/// `.wait_durable()`) or on its `let` binding, unconditionally, later in
/// `region`.
fn barrier_awaited(region: &str, at: usize) -> bool {
    const WAITS: &[&str] = &[".blocking_wait()", ".wait()", ".wait_durable()"];
    let chain = call_result_chain(region, at + "barrier".len());
    if WAITS.iter().any(|w| chain.starts_with(w)) {
        return true;
    }
    let stmt_start = region[..at].rfind([';', '{', '}']).map_or(0, |n| n + 1);
    let head: String = region[stmt_start..at]
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    let Some(name) = head
        .strip_prefix("let")
        .map(|p| p.strip_prefix("mut").unwrap_or(p))
        .and_then(|p| p.split('=').next())
        .filter(|n| !n.is_empty() && n.bytes().all(is_ident))
    else {
        return false;
    };
    let after = call_args_end(region, at + "barrier".len());
    let rest = &region[after..];
    WAITS.iter().any(|w| {
        rest.match_indices(&format!("{name}{w}")).any(|(k, _)| {
            (k == 0 || !is_ident(rest.as_bytes()[k - 1]))
                && !rebound_after(&rest[..k], name, "")
                && unconditional_at(region, after + k)
        })
    })
}

/// Whether `after` redeclares `name` (`let name =` / `let mut name =`, or a
/// typed `let name: T =`).
fn shadows(after: &str, name: &str) -> bool {
    !name.is_empty()
        && after.split([';', '{', '}']).any(|stmt| {
            let stmt: String = stmt.chars().filter(|c| !c.is_whitespace()).collect();
            let rest = stmt
                .strip_prefix("letmut")
                .or_else(|| stmt.strip_prefix("let"))
                .and_then(|r| r.strip_prefix(name));
            rest.is_some_and(|r| r.starts_with('=') || r.starts_with(':'))
        })
}

/// Whether `recv` is bound *by value* anywhere in `region`'s function
/// (`let recv = …` / `let mut recv = …`), so `drop(recv)` moves and closes the
/// owned journal. A receiver that is only ever a `&mut Journal` reference
/// (`let journal = &mut *real;`, a `&mut Journal` parameter) makes
/// `drop(recv)` a no-op: the reference dies, the write stays pending.
fn bound_by_value(region: &str, recv: &str) -> bool {
    ["let mut ", "let "].iter().any(|kw| {
        let binding = format!("{kw}{recv} =");
        let at = region
            .find(&binding)
            .or_else(|| region.find(&binding.replace(" =", "=")));
        at.is_some_and(|at| {
            let stmt_end = region[at..].find(';').map_or(region.len(), |n| at + n);
            let stmt: String = region[at..stmt_end]
                .chars()
                .filter(|c| !c.is_whitespace())
                .collect();
            // An initializer that *is* a reference (`= &…` / `= &mut …`) binds
            // a reference: dropping it is a no-op, not a close. A `&` deeper
            // in the expression (a `&dir` argument) is just an argument.
            let init = stmt.split('=').nth(1).unwrap_or("");
            !init.starts_with('&')
        })
    })
}

/// Whether `handle` is rebound or `recv` shadowed in `after` (the text from
/// the apply call to the wait). Either breaks the tie between the wait's
/// handle name and the apply's commit: `handle = Commit::ready()` makes the
/// wait resolve immediately, and `let recv = <other journal>` means the apply
/// ran on a different journal than the discarded write did.
fn rebound_after(after: &str, handle: &str, recv: &str) -> bool {
    after.split([';', '{', '}']).any(|stmt| {
        let stmt: String = stmt.chars().filter(|c| !c.is_whitespace()).collect();
        stmt.strip_prefix(handle)
            .or_else(|| stmt.strip_prefix(&format!("mut {handle}")))
            .is_some_and(|rest| rest.starts_with('=') && !rest.starts_with("=="))
    }) || shadows(after, handle)
        || shadows(after, recv)
}

/// The binding of the commit (last tuple slot, or the whole pattern) in the
/// `let` statement ending at `apply_at`; `None` when nothing is bound.
fn commit_slot(body: &str, apply_at: usize) -> Option<String> {
    let stmt_start = body[..apply_at].rfind([';', '{', '}']).map_or(0, |n| n + 1);
    let stmt: String = body[stmt_start..apply_at]
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    let pat = stmt.strip_prefix("let")?;
    let pat = pat.strip_prefix("mut").unwrap_or(pat);
    let pat = &pat[..pat.find('=')?];
    let pat = pat.split(':').next().unwrap_or("");
    let last = match pat.strip_prefix('(').and_then(|p| p.strip_suffix(')')) {
        // A trailing comma leaves an empty final component; skip empty slots.
        Some(inner) => inner.rsplit(',').find(|s| !s.is_empty()).unwrap_or(""),
        None => pat,
    };
    Some(last.trim_start_matches("mut").to_string())
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
                // Rust block comments nest: scan to the `*/` that closes the
                // outermost one, or an inner close leaves its tail (quotes,
                // braces) exposed to the string scanner.
                let mut depth = 1usize;
                let mut j = i + 2;
                while j + 1 < b.len() && depth > 0 {
                    match (b[j], b[j + 1]) {
                        (b'/', b'*') => {
                            depth += 1;
                            j += 2;
                        }
                        (b'*', b'/') => {
                            depth -= 1;
                            j += 2;
                        }
                        _ => j += 1,
                    }
                }
                let end = j.min(b.len());
                blank(&mut out, i, end);
                i = end;
            }
            // `r"…"`/`r#"…"#`, and the raw byte string `br#"…"#`.
            b'r' if matches!(b.get(i + 1), Some(b'#' | b'"'))
                && (i == 0
                    || !is_ident(b[i - 1])
                    || (b[i - 1] == b'b' && (i == 1 || !is_ident(b[i - 2])))) =>
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
        // trait method declaration ends at `;`) — ignoring delimiters nested in
        // the parameter list or a type (`key: &[u8; 32]`).
        let mut nest = 0i64;
        let Some(rel) = bytes[at..].iter().position(|&c| {
            match c {
                b'(' | b'[' => nest += 1,
                b')' | b']' => nest -= 1,
                b'{' | b';' if nest <= 0 => return true,
                _ => {}
            }
            false
        }) else {
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

/// The offset just past the balanced argument list of the call whose method
/// name ends at `name_end` (`body[name_end]` is its `(`), or `body.len()`
/// when it never closes.
fn call_args_end(body: &str, name_end: usize) -> usize {
    let b = body.as_bytes();
    let mut depth = 0usize;
    let mut close = body.len();
    for (k, &c) in b[name_end..].iter().enumerate() {
        match c {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    close = name_end + k;
                    break;
                }
            }
            _ => {}
        }
    }
    close.saturating_add(1).min(body.len())
}

/// The chain consuming the result of the call whose method name ends at
/// `name_end` (its argument list through the end of the statement),
/// whitespace-stripped — the call's *own* result chain, so an unrelated
/// unwrap elsewhere in the statement (`let _ = (j.rotate(), Some(()).unwrap())`)
/// does not pass for the call's. A `,` ends the chain too: in a tuple or
/// argument list the call's value stops there.
fn call_result_chain(body: &str, name_end: usize) -> String {
    let after = call_args_end(body, name_end);
    let stmt_end = body[after..]
        .find([';', '{', '}', ','])
        .map_or(body.len(), |n| after + n);
    body[after..stmt_end]
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect()
}

/// The statement text following the apply call whose method name ends at
/// `apply_at` (the `)` of its argument list through the end of the statement),
/// whitespace-stripped. This is the chain that consumes the returned
/// `Result<(events, Commit), _>`: `.unwrap()`, `?`, a `.0`/`.1` projection, or
/// the rest of a larger expression the apply is nested in.
fn apply_result_suffix(body: &str, apply_at: usize) -> String {
    // The argument list opens at the first `(` at/after the method name.
    let Some(open) = body[apply_at..].find('(').map(|n| apply_at + n) else {
        return String::new();
    };
    call_result_chain(body, open)
}

/// Whether the statement ending at `apply_at` drops the returned commit
/// without awaiting it: a bare expression statement (nothing bound), a
/// `_`-prefixed binding (`let _ =`, `let _r =`), or a tuple whose commit (last)
/// slot is `_`-prefixed (`let (e, _) =`, `let (_e, _commit) =`).
///
/// The commit is also dropped when the apply's `Result<(events, Commit), _>`
/// is consumed rather than bound whole: projecting the events half
/// (`….unwrap().0`, `….0`, `let n = …?.0.len()`) discards the `.1` commit, and
/// nesting the apply in a larger expression (`assert!(….is_ok())`,
/// `let n = ….0.len()`) binds no commit at all. Only a statement whose value
/// *is* the bare apply-chain (`….unwrap()` / `….expect(…)` / `…?`, no
/// projection) lets the binding decide.
fn discards_commit(body: &str, apply_at: usize) -> bool {
    let stmt_start = body[..apply_at].rfind([';', '{', '}']).map_or(0, |n| n + 1);
    let stmt: String = body[stmt_start..apply_at]
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    // The chain after the apply's argument list: an unwrapping chain
    // (`.unwrap()` / `.expect(…)` / `?`) keeps the whole `(events, Commit)`
    // tuple for the binding to split; anything beyond it consumes the result.
    let suffix = apply_result_suffix(body, apply_at);
    let rest = strip_unwrap_chain(&suffix);
    // A `.0` projection (or further use of it, `.0.len()`) keeps the events and
    // drops the `.1` commit. Anything else left over (`assert!(…)`'s `.is_ok()`,
    // a method on the projection, arithmetic) likewise consumes the result
    // without binding the commit. Only an empty remainder — the bare
    // apply-chain — or a `.1` projection (which *keeps* the commit) leaves the
    // commit for the binding below.
    if !rest.is_empty() && !rest.starts_with(".1") {
        return true;
    }
    if !stmt.starts_with("let") {
        // No binding at all: an expression statement discards the commit,
        // unless the value is assigned/returned/passed on.
        return !stmt.contains('=')
            && !stmt.starts_with("return")
            && stmt.matches('(').count() == stmt.matches(')').count();
    }
    let slot = commit_slot(body, apply_at);
    // A named handle that is rebound before its wait (`late = Commit::ready()`)
    // no longer names the apply's commit: the wait resolves immediately and the
    // pending write races the observation — the commit is effectively dropped.
    //
    // A *named* handle is never trusted by name alone either: whether it is
    // awaited before the observation, unconditionally, and without being
    // rebound is position-dependent, so every bound apply is handed to
    // `violations`, which accepts it only on such a proven wait.
    slot.is_some()
}

/// The wait expression that awaits the apply's own commit through its binding:
/// `<slot>.1.blocking_wait()` when the binding holds the whole
/// `(events, Commit)` tuple, else `<slot>.blocking_wait()`.
fn own_wait(body: &str, apply_at: usize, slot: &str) -> String {
    let stmt_start = body[..apply_at].rfind([';', '{', '}']).map_or(0, |n| n + 1);
    let pat: String = body[stmt_start..apply_at]
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    let tuple_pattern = pat.contains('(') && pat.find('(') < pat.find('=');
    let suffix = apply_result_suffix(body, apply_at);
    if !tuple_pattern && strip_unwrap_chain(&suffix).is_empty() {
        format!("{slot}.1.blocking_wait()")
    } else {
        format!("{slot}.blocking_wait()")
    }
}

/// The `suffix` of an apply statement with its unwrapping chain removed:
/// leading `?`, `.unwrap()`, and `.expect(…)` (balanced-args) segments. What
/// remains is how the `(events, Commit)` tuple itself is used.
fn strip_unwrap_chain(suffix: &str) -> &str {
    let mut s = suffix;
    loop {
        if let Some(r) = s.strip_prefix('?') {
            s = r;
        } else if let Some(r) = s.strip_prefix(".unwrap()") {
            s = r;
        } else if let Some(r) = s.strip_prefix(".expect(") {
            // Skip the balanced argument list of `.expect("…")`.
            let mut depth = 1usize;
            let mut end = r.len();
            for (k, c) in r.char_indices() {
                match c {
                    '(' => depth += 1,
                    ')' => {
                        depth -= 1;
                        if depth == 0 {
                            end = k + 1;
                            break;
                        }
                    }
                    _ => {}
                }
            }
            s = &r[end..];
        } else {
            return s;
        }
    }
}

/// Whether the journal `apply_at` is called on was built in memory
/// (`Journal::in_memory*` — matched as the initializer's *constructor*, so a
/// persistent `Journal::open(&in_memory_path)` does not pass on an argument
/// name) and never reassigned: it has no writer thread, so its commit is
/// always already resolved and nothing can race it. A later reassignment
/// (`j = Journal::open(&path)`) invalidates the exemption — from then on `j`
/// has an asynchronous disk writer.
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
            let init: String = body[at..stmt_end]
                .chars()
                .filter(|c| !c.is_whitespace())
                .collect();
            !reassigned && init.contains("::in_memory")
        })
    })
}

/// The `(fn name, offending observation)` pairs in `src`: a disk observation
/// that follows a discarded commit with no sync point in between. A sync point
/// must be tied to the *affected journal* (or await the commit itself — the
/// apply's own named handle included, `_commit` or not) and must execute on
/// every path to the observation — an unrelated `barrier.wait()`, a different
/// journal's rotation, or a rotation inside `if …` orders nothing.
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
            // Observations race only once the write is queued: scanning starts
            // past the apply's argument list, so a disk read *constructing* the
            // command (`apply_command(make(fs::read(&p).unwrap()))`) — over
            // before the write is queued — is not flagged.
            let tail = &body[call_args_end(&body, apply_at)..];
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
                let synced = sync_point_at(region, &recv, &body).is_some()
                    || commit_slot(&body, apply_at).is_some_and(|slot| {
                        !slot.is_empty()
                            && region.match_indices(&own_wait(&body, apply_at, &slot)).any(
                                |(at, _)| {
                                    (at == 0 || !is_ident(region.as_bytes()[at - 1]))
                                        && !rebound_after(&region[..at], &slot, &recv)
                                        && unconditional_at(region, at)
                                },
                            )
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
            journal.snapshot_and_rotate().expect("rotate");
            let u = shared.uncompacted_usage();
        }
    "#;
    assert!(violations(tied).is_empty());
    let dropped = r#"
        fn t() {
            let journal = Journal::open_segmented(&dir).expect("open").0;
            let _ = journal.apply_command(c).unwrap();
            drop(journal);
            let u = shared.uncompacted_usage();
        }
    "#;
    assert_eq!(violations(dropped), Vec::<(String, String)>::new());

    // A sync point *before* a conditional still executes on every path: the
    // `?` only makes the suffix after it conditional.
    let synced_then_fallible = r#"
        fn t() {
            let _ = journal.apply_command(c).unwrap();
            journal.snapshot_and_rotate().expect("rotate");
            let x = fallible()?;
            let u = shared.uncompacted_usage();
        }
    "#;
    assert!(violations(synced_then_fallible).is_empty());

    // An unrelated drop, or a wait on a commit from *before* the discarded
    // write (or from another journal), does not cover the pending append.
    let unrelated_drop = r#"
        fn t() {
            let _ = journal.apply_command(c).unwrap();
            drop(events);
            let u = shared.uncompacted_usage();
        }
    "#;
    assert_eq!(violations(unrelated_drop).len(), 1);
    let stale_wait = r#"
        fn t() {
            let (_e, early) = journal.apply_command(a).unwrap();
            let _ = journal.apply_command(c).unwrap();
            early.blocking_wait();
            let u = shared.uncompacted_usage();
        }
    "#;
    assert_eq!(violations(stale_wait).len(), 1);
    let later_wait = r#"
        fn t() {
            let _ = journal.apply_command(c).unwrap();
            let (_e, late) = journal.apply_command(d).unwrap();
            late.blocking_wait();
            let u = shared.uncompacted_usage();
        }
    "#;
    // A later apply's wait is NOT a sync point (see the eventless-later block
    // below): the guard cannot prove the later apply queued a write.
    assert_eq!(violations(later_wait).len(), 1);

    // A deferred closure or short-circuited call may never run.
    let deferred = r#"
        fn t() {
            let _ = journal.apply_command(c).unwrap();
            let later = || journal.snapshot_and_rotate().expect("r");
            let u = shared.uncompacted_usage();
        }
    "#;
    assert_eq!(violations(deferred).len(), 1);
    let short_circuit = r#"
        fn t() {
            let _ = journal.apply_command(c).unwrap();
            let ok = flag && journal.flush().is_ok();
            let u = shared.uncompacted_usage();
        }
    "#;
    assert_eq!(violations(short_circuit).len(), 1);

    // …but an unconditional sync after a `?` or a completed conditional holds.
    let after_fallible = r#"
        fn t() -> Result<()> {
            let _ = journal.apply_command(c)?;
            if x { y(); }
            journal.snapshot_and_rotate()?;
            let u = shared.uncompacted_usage();
        }
    "#;
    assert!(violations(after_fallible).is_empty());

    // An unchecked rotation may be a no-op (non-segmented journal → `None`).
    let unchecked_rotation = r#"
        fn t() {
            let _ = journal.apply_command(c).unwrap();
            journal.snapshot_and_rotate();
            let b = fs::read(&p).unwrap();
        }
    "#;
    assert_eq!(violations(unchecked_rotation).len(), 1);

    // Field-qualified receivers are compared as whole paths.
    let other_field = r#"
        fn t() {
            let _ = a.journal.apply_command(c).unwrap();
            b.journal.snapshot_and_rotate().expect("r");
            let u = shared.uncompacted_usage();
        }
    "#;
    assert_eq!(violations(other_field).len(), 1);

    // A raw byte string with embedded quotes/braces must not hide later fns.
    let raw_bytes = r##"
        fn a() { let x = br#"{"k":"v"#; }
        fn racy() {
            let _ = journal.apply_command(c).unwrap();
            let u = shared.uncompacted_usage();
        }
    "##;
    assert_eq!(violations(raw_bytes).len(), 1);

    // An array type's `;` in the signature is not a bodyless declaration.
    let array_param = r#"
        fn t(key: &[u8; 32]) {
            let _ = journal.apply_command(c).unwrap();
            let u = shared.uncompacted_usage();
        }
    "#;
    assert_eq!(violations(array_param).len(), 1);

    // Text inside string literals and comments is not code.
    let literal = r#"
        fn t() {
            // let _ = journal.apply_command(x); shared.uncompacted_usage();
            let s = "let _ = j.apply_command(x); fs::metadata(p)";
        }
    "#;
    assert!(violations(literal).is_empty());

    // Projecting the events half (`.0`) drops the `.1` commit, however the
    // result is bound: a named binding does not make the commit awaited.
    let project_unwrap = r#"
        fn t() {
            let events = journal.apply_command(c).unwrap().0;
            let u = shared.uncompacted_usage();
        }
    "#;
    assert_eq!(violations(project_unwrap).len(), 1);
    let project_question = r#"
        fn t() -> Result<()> {
            let n = journal.apply_command(c)?.0.len();
            let u = shared.uncompacted_usage();
        }
    "#;
    assert_eq!(violations(project_question).len(), 1);

    // The apply nested inside a larger expression (a macro, a call) binds no
    // commit at all: the result is consumed by the surrounding expression.
    let nested_assert = r#"
        fn t() {
            assert!(journal.apply_command(c).is_ok());
            let u = shared.uncompacted_usage();
        }
    "#;
    assert_eq!(violations(nested_assert).len(), 1);

    // …while a `.1` projection *keeps* the commit for the binding to await.
    let project_commit = r#"
        fn t() {
            let commit = journal.apply_command(c).unwrap().1;
            commit.blocking_wait();
            let u = shared.uncompacted_usage();
        }
    "#;
    assert!(violations(project_commit).is_empty());

    // A later apply's commit can never prove it covers the discarded write:
    // non-emptiness of its events is not statically provable, and an eventless
    // apply returns `Commit::ready()` without enqueueing anything — so a wait
    // on it orders nothing, even when the result is unwrapped (an Ok result
    // says nothing about a queued write: `persist` returns a ready commit for
    // an empty batch). Only the discarded apply's own handle, a writer-queued
    // message (rotation/barrier), or a close/reopen syncs.
    let eventless_later = r#"
        fn t() {
            let _ = journal.apply_command(Command::DeployProcess(demo())).unwrap();
            let (_, late) = journal.apply_command(Command::ExpireJobs);
            late.blocking_wait();
            let u = shared.uncompacted_usage();
        }
    "#;
    assert_eq!(violations(eventless_later).len(), 1);
    let queued_later = r#"
        fn t() {
            let _ = journal.apply_command(c).unwrap();
            let (_, late) = journal.apply_command(d).unwrap();
            late.blocking_wait();
            let u = shared.uncompacted_usage();
        }
    "#;
    assert_eq!(violations(queued_later).len(), 1);
    let projected_later = r#"
        fn t() {
            let _ = journal.apply_command(c).unwrap();
            let late = journal.apply_command(d).unwrap().1;
            late.blocking_wait();
            let u = shared.uncompacted_usage();
        }
    "#;
    assert_eq!(violations(projected_later).len(), 1);

    // `barrier()` is the canonical writer-FIFO sync: it queues an empty write
    // behind every pending append, so waiting on it covers the discarded write.
    let barrier_wait = r#"
        fn t() {
            let _ = journal.apply_command(c).unwrap();
            journal.barrier().blocking_wait();
            let u = shared.uncompacted_usage();
        }
    "#;
    assert!(violations(barrier_wait).is_empty());
    let barrier_bound = r#"
        fn t() {
            let _ = journal.apply_command(c).unwrap();
            let b = journal.barrier();
            b.blocking_wait();
            let u = shared.uncompacted_usage();
        }
    "#;
    assert!(violations(barrier_bound).is_empty());

    // Dropping a `&mut Journal` reference is a no-op — the write stays pending.
    // Only dropping a journal bound by value closes it and drains the writer.
    let drop_ref = r#"
        fn t(journal: &mut Journal) {
            let _ = journal.apply_command(c).unwrap();
            drop(journal);
            let b = fs::read(&p).unwrap();
        }
    "#;
    assert_eq!(violations(drop_ref).len(), 1);
    let drop_ref_alias = r#"
        fn t() {
            let journal = &mut *real;
            let _ = journal.apply_command(c).unwrap();
            drop(journal);
            let b = fs::read(&p).unwrap();
        }
    "#;
    assert_eq!(violations(drop_ref_alias).len(), 1);

    // A wait's handle must still name the apply's commit at the wait: rebinding
    // the handle, or shadowing the receiver to a different journal, breaks it.
    let rebound_handle = r#"
        fn t() {
            let (_, mut late) = journal.apply_command(c).unwrap();
            late = Commit::ready();
            late.blocking_wait();
            let u = shared.uncompacted_usage();
        }
    "#;
    assert_eq!(
        violations(rebound_handle),
        vec![("t".to_string(), "uncompacted_usage(".to_string())]
    );
    let shadowed_receiver = r#"
        fn t() {
            let _ = journal.apply_command(c).unwrap();
            let journal = Journal::open_segmented(&dir2).expect("open").0;
            let (_, late) = journal.apply_command(d).unwrap();
            late.blocking_wait();
            let u = shared.uncompacted_usage();
        }
    "#;
    assert_eq!(violations(shadowed_receiver).len(), 1);

    // A rotation must be unwrapped on its *own* result chain: an unrelated
    // unwrap elsewhere in the statement does not prove the rotation ran.
    let independent_unwrap = r#"
        fn t() {
            let _ = journal.apply_command(c).unwrap();
            let _ = (journal.snapshot_and_rotate(), Some(()).unwrap());
            let u = shared.uncompacted_usage();
        }
    "#;
    assert_eq!(violations(independent_unwrap).len(), 1);

    // Rust block comments nest: an inner `*/` does not end the comment, so a
    // quote inside the outer comment never reaches the string scanner.
    let nested_comment = r#"
        fn a() { /* outer /* inner */ " */ }
        fn racy() {
            let _ = journal.apply_command(c).unwrap();
            let u = shared.uncompacted_usage();
        }
    "#;
    assert_eq!(violations(nested_comment).len(), 1);

    // A wait on the apply's *own* named handle synchronizes, even when the
    // binding is `_`-prefixed to silence the unused-events lint.
    let own_handle = r#"
        fn t() {
            let (_, _commit) = journal.apply_command(c).unwrap();
            _commit.blocking_wait();
            let u = shared.uncompacted_usage();
        }
    "#;
    assert!(violations(own_handle).is_empty());

    // The in-memory exemption matches the constructor, not arbitrary argument
    // text: `Journal::open(&in_memory_path)` is a persistent journal.
    let persistent_initializer = r#"
        fn t() {
            let mut j = Journal::open(&in_memory_path).unwrap();
            let _ = j.apply_command(c).unwrap();
            let u = shared.uncompacted_usage();
        }
    "#;
    assert_eq!(violations(persistent_initializer).len(), 1);

    // A disk read *constructing* the command finishes before the write is
    // queued, so it cannot race it.
    let read_builds_command = r#"
        fn t() {
            let _ = journal.apply_command(make_command(fs::read(&path).unwrap())).unwrap();
        }
    "#;
    assert!(violations(read_builds_command).is_empty());

    // A wait on the candidate's own handle must be unconditional: inside a
    // conditional it may never run.
    let own_handle_conditional = r#"
        fn t() {
            let (_, _commit) = journal.apply_command(c).unwrap();
            if flag { _commit.blocking_wait(); }
            let u = shared.uncompacted_usage();
        }
    "#;
    assert_eq!(violations(own_handle_conditional).len(), 1);
}

/// Round-5 review (#1822): each shape that used to slip past the guard is
/// flagged (RED), and the valid whole-tuple wait is accepted (GREEN).
#[test]
fn guard_requires_a_proven_ordered_wait_or_sync() {
    let wait_after_read = r#"
        fn t() {
            let (_, commit) = journal.apply_command(c).unwrap();
            shared.uncompacted_usage();
            commit.blocking_wait();
        }
    "#;
    assert_eq!(violations(wait_after_read).len(), 1);
    let conditional_wait = r#"
        fn t() {
            let (_, commit) = journal.apply_command(c).unwrap();
            if flag { commit.blocking_wait(); }
            shared.uncompacted_usage();
        }
    "#;
    assert_eq!(violations(conditional_wait).len(), 1);
    let let_shadowed_handle = r#"
        fn t() {
            let (_, _commit) = journal.apply_command(c).unwrap();
            let _commit = Commit::ready();
            _commit.blocking_wait();
            shared.uncompacted_usage();
        }
    "#;
    assert_eq!(violations(let_shadowed_handle).len(), 1);
    let shadowed_receiver_rotated = r#"
        fn t() {
            let _ = journal.apply_command(c).unwrap();
            let journal = Journal::open_segmented(&dir2).unwrap();
            journal.snapshot_and_rotate().unwrap();
            shared.uncompacted_usage();
        }
    "#;
    assert_eq!(violations(shadowed_receiver_rotated).len(), 1);
    let unawaited_barrier = r#"
        fn t() {
            let _ = journal.apply_command(c).unwrap();
            journal.barrier();
            shared.uncompacted_usage();
        }
    "#;
    assert_eq!(violations(unawaited_barrier).len(), 1);
    let bound_barrier_waited_late = r#"
        fn t() {
            let _ = journal.apply_command(c).unwrap();
            let b = journal.barrier();
            shared.uncompacted_usage();
            b.blocking_wait();
        }
    "#;
    assert_eq!(violations(bound_barrier_waited_late).len(), 1);
    let whole_tuple = r#"
        fn t() {
            let pair = journal.apply_command(c).unwrap();
            pair.1.blocking_wait();
            shared.uncompacted_usage();
        }
    "#;
    assert!(violations(whole_tuple).is_empty());
    let whole_tuple_projection_only = r#"
        fn t() {
            let pair = journal.apply_command(c).unwrap();
            pair.0.len();
            shared.uncompacted_usage();
        }
    "#;
    assert_eq!(violations(whole_tuple_projection_only).len(), 1);
}
