//! Lint: no silent FEEL fallbacks in the engine (#1390, #1391 step 4).
//!
//! #1390 was a defect *class*, not a one-off: an engine behaviour evaluated a
//! FEEL expression and, on failure, quietly substituted something (the literal
//! expression text, a default, `None`) instead of raising the incident Zeebe
//! raises. This test scans `engine-core/src/engine/**` (production code only —
//! the `tests/` directory, inline `#[cfg(test)]` tails and comments are skipped)
//! and fails on two patterns:
//!
//! - **`direct-feel`** — a direct `feel::eval*(..)` call. Engine behaviours
//!   must evaluate through the single entry point
//!   `engine/expression.rs::evaluate_or_failure` (and its typed checks), which
//!   returns `Result<T, Failure>` like Zeebe's `ExpressionProcessor`.
//! - **`fallback`** — `.ok()` / `.unwrap_or(..)` / `.unwrap_or_else(..)` /
//!   `.unwrap_or_default()` applied to an evaluation result (a `feel::eval*`,
//!   `expression::evaluate*` / `evaluate_or_failure`, `resolve_*_checked`,
//!   `resolve_job_type` or `resolve_job_retries` call in the same statement).
//!
//! A site may only remain if it is on [`ALLOW`] with an exact match count and a
//! justification: either the **Zeebe** behaviour it mirrors, or a **Tracked**
//! known divergence with its follow-up issue. A stale entry (its site moved,
//! was fixed, or its count changed) also fails, so the list cannot rot.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "non-production target (integration tests / examples); the production deny-unwrap gate (#1357) applies to the library crate only"
)]

use std::fs;
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Rule {
    DirectFeel,
    Fallback,
}

impl Rule {
    fn name(self) -> &'static str {
        match self {
            Rule::DirectFeel => "direct-feel",
            Rule::Fallback => "fallback",
        }
    }
}

/// Why an allow-listed site may stay.
#[derive(Clone, Copy, Debug)]
#[allow(dead_code, reason = "the payloads are documentation read by humans")]
enum Why {
    /// The site matches Zeebe's behaviour (cite the Zeebe source).
    Zeebe(&'static str),
    /// A known Zeebe divergence, tracked in the given nano-bpm issue.
    Tracked(u32, &'static str),
}

struct Allow {
    /// Path relative to `src/engine`.
    file: &'static str,
    /// A substring of the offending (trimmed) source line.
    contains: &'static str,
    rule: Rule,
    /// Exactly how many violating lines this entry covers.
    count: usize,
    why: Why,
}

const T: u32 = 1398;

/// Every remaining site. Keep sorted by file.
const ALLOW: &[Allow] = &[
    Allow {
        file: "call_activity.rs",
        contains: "match crate::feel::eval_string(trimmed, eval_view)",
        rule: Rule::DirectFeel,
        count: 1,
        why: Why::Zeebe(
            "calledElement: a failure propagates as an ExpressionEvaluation incident \
             (Zeebe CallActivityProcessor -> evaluateStringExpression EXTRACT_VALUE_ERROR)",
        ),
    },
    Allow {
        file: "mod.rs",
        contains: "crate::feel::eval_bool(&condition, &",
        rule: Rule::DirectFeel,
        count: 2,
        why: Why::Tracked(
            T,
            "conditional-event condition error treated as not-yet-satisfied",
        ),
    },
    Allow {
        file: "mod.rs",
        contains: "match crate::feel::eval(&mi.input_collection, &vars)",
        rule: Rule::DirectFeel,
        count: 1,
        why: Why::Tracked(T, "MI inputCollection non-list -> empty"),
    },
    Allow {
        file: "mod.rs",
        contains: "crate::feel::eval(expr, &vars).ok()",
        rule: Rule::DirectFeel,
        count: 1,
        why: Why::Tracked(T, "MI / ad-hoc outputElement error -> null"),
    },
    Allow {
        file: "mod.rs",
        contains: "crate::feel::eval(expr, &vars).ok()",
        rule: Rule::Fallback,
        count: 1,
        why: Why::Tracked(T, "MI / ad-hoc outputElement error -> null"),
    },
    Allow {
        file: "mod.rs",
        contains: "crate::feel::eval(expr, view).ok()",
        rule: Rule::DirectFeel,
        count: 1,
        why: Why::Tracked(T, "MI outputElement error -> null"),
    },
    Allow {
        file: "mod.rs",
        contains: "crate::feel::eval(expr, view).ok()",
        rule: Rule::Fallback,
        count: 1,
        why: Why::Tracked(T, "MI outputElement error -> null"),
    },
    Allow {
        file: "mod.rs",
        contains: "crate::feel::eval_bool(c, view)",
        rule: Rule::DirectFeel,
        count: 1,
        why: Why::Tracked(T, "MI completionCondition error -> false"),
    },
    Allow {
        file: "mod.rs",
        contains: "match crate::feel::eval(expr, vars)",
        rule: Rule::DirectFeel,
        count: 1,
        why: Why::Tracked(T, "ad-hoc activeElementsCollection error -> no elements"),
    },
    Allow {
        file: "mod.rs",
        contains: "matches!(crate::feel::eval_bool(cond, &ctx), Ok(true))",
        rule: Rule::DirectFeel,
        count: 1,
        why: Why::Tracked(T, "ad-hoc completionCondition error -> false"),
    },
    Allow {
        file: "mod.rs",
        contains: "match crate::feel::eval(&expression, &vars)",
        rule: Rule::DirectFeel,
        count: 1,
        why: Why::Zeebe("script task: a failure raises an EXTRACT_VALUE_ERROR incident"),
    },
    Allow {
        file: "resolve.rs",
        contains: "crate::feel::eval_string(correlation_key, vars).map_err",
        rule: Rule::DirectFeel,
        count: 1,
        why: Why::Zeebe("correlation key: the error is propagated (caller raises the incident)"),
    },
    Allow {
        file: "resolve.rs",
        contains: "crate::feel::eval(source.trim(), vars)",
        rule: Rule::DirectFeel,
        count: 1,
        why: Why::Zeebe(
            "ioMapping source: the error is propagated and raised as IO_MAPPING_ERROR \
             (Zeebe BpmnVariableMappingBehavior)",
        ),
    },
    Allow {
        file: "resolve.rs",
        contains: "crate::feel::eval_string(trimmed, vars).ok()",
        rule: Rule::DirectFeel,
        count: 1,
        why: Why::Tracked(
            T,
            "user-task assignee/dates error -> unset (Zeebe: only null)",
        ),
    },
    Allow {
        file: "resolve.rs",
        contains: "crate::feel::eval_string(trimmed, vars).ok()",
        rule: Rule::Fallback,
        count: 1,
        why: Why::Tracked(
            T,
            "user-task assignee/dates error -> unset (Zeebe: only null)",
        ),
    },
    Allow {
        file: "resolve.rs",
        contains: "match crate::feel::eval(trimmed, vars)",
        rule: Rule::DirectFeel,
        count: 1,
        why: Why::Tracked(T, "user-task candidate list error -> empty"),
    },
    Allow {
        file: "resolve.rs",
        contains: "match crate::feel::eval(expr, vars)",
        rule: Rule::DirectFeel,
        count: 1,
        why: Why::Tracked(T, "job / user-task priority error -> default 50"),
    },
    Allow {
        file: "resolve.rs",
        contains: "crate::feel::eval_string(&def.expr, vars).ok()",
        rule: Rule::DirectFeel,
        count: 1,
        why: Why::Tracked(T, "timer definition error"),
    },
    Allow {
        file: "resolve.rs",
        contains: "crate::feel::eval_string(&def.expr, vars).ok()",
        rule: Rule::Fallback,
        count: 1,
        why: Why::Tracked(T, "timer definition error"),
    },
    Allow {
        file: "resolve.rs",
        contains: "self.resolve_correlation_value_checked(vars, correlation_key)",
        rule: Rule::Fallback,
        count: 1,
        why: Why::Tracked(T, "boundary correlation key error -> empty string"),
    },
    Allow {
        file: "resolve.rs",
        contains: "self.resolve_job_type(vars, job_type)",
        rule: Rule::Fallback,
        count: 1,
        why: Why::Tracked(
            T,
            "listener / ad-hoc job type error -> literal (resolve_job_type_lenient)",
        ),
    },
    Allow {
        file: "resolve.rs",
        contains: "self.resolve_job_retries(vars, raw)",
        rule: Rule::Fallback,
        count: 1,
        why: Why::Tracked(
            T,
            "listener / ad-hoc job retries error -> default (resolve_retries)",
        ),
    },
    Allow {
        file: "resolve.rs",
        contains: "self.resolve_event_name_checked(vars, raw)",
        rule: Rule::Fallback,
        count: 1,
        why: Why::Tracked(
            T,
            "boundary / start / signal name error -> literal (resolve_event_name)",
        ),
    },
];

#[derive(Debug, Clone, PartialEq, Eq)]
struct Violation {
    file: String,
    line: usize,
    text: String,
    rule: Rule,
}

const EVAL_CALLS: &[&str] = &[
    "feel::eval(",
    "feel::eval_string(",
    "feel::eval_bool(",
    "expression::evaluate",
    "evaluate_or_failure(",
    "_checked(",
    ".resolve_job_type(",
    ".resolve_job_retries(",
];
const FALLBACKS: &[&str] = &[
    ".ok()",
    ".unwrap_or(",
    ".unwrap_or_else(",
    ".unwrap_or_default()",
];

fn has_direct_feel(line: &str) -> bool {
    line.match_indices("feel::eval").any(|(i, m)| {
        let rest = &line[i + m.len()..];
        let ident_len = rest
            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
            .unwrap_or(rest.len());
        rest[ident_len..].starts_with('(')
    })
}

/// The full expression starting at `lines[first][col..]`: delimiter nesting is
/// tracked across lines (string / char literals skipped), so a multiline call
/// whose arguments end in commas, followed by a `.unwrap_or_else(..)` chain on
/// later lines, is collected whole (#1399 review). The statement ends at a
/// depth-0 `;` / `,` / block-opening `{`, when an enclosing delimiter closes,
/// or at a line end at depth 0 unless the next line continues the method chain
/// (leading `.` or `?`).
fn statement_from(lines: &[&str], first: usize, col: usize) -> String {
    let mut stmt = String::new();
    let mut depth: i64 = 0;
    for (n, full) in lines.iter().enumerate().skip(first) {
        let text = if n == first {
            &full[col..]
        } else {
            full.trim()
        };
        let mut chars = text.chars().peekable();
        while let Some(c) = chars.next() {
            stmt.push(c);
            match c {
                '"' => {
                    while let Some(d) = chars.next() {
                        stmt.push(d);
                        if d == '\\' {
                            if let Some(e) = chars.next() {
                                stmt.push(e);
                            }
                        } else if d == '"' {
                            break;
                        }
                    }
                }
                '(' | '[' => depth += 1,
                '{' if depth == 0 => return stmt,
                '{' => depth += 1,
                ')' | ']' | '}' => {
                    depth -= 1;
                    if depth < 0 {
                        return stmt;
                    }
                }
                ';' | ',' if depth == 0 => return stmt,
                _ => {}
            }
        }
        if depth == 0 {
            let next = lines.get(n + 1).map(|l| l.trim_start()).unwrap_or("");
            if !(next.starts_with('.') || next.starts_with('?')) {
                return stmt;
            }
        }
    }
    stmt
}

/// Scans one file's source. `file` is the path reported in violations.
fn scan(file: &str, src: &str) -> Vec<Violation> {
    let lines: Vec<&str> = src.lines().collect();
    let mut out = Vec::new();
    for (i, raw) in lines.iter().enumerate() {
        // Inline unit tests live in a trailing `#[cfg(test)]` module.
        if raw.trim_end() == "#[cfg(test)]" && !raw.starts_with(' ') {
            break;
        }
        let line = raw.trim();
        if line.starts_with("//") {
            continue;
        }
        let violation = |rule| Violation {
            file: file.to_string(),
            line: i + 1,
            text: line.to_string(),
            rule,
        };
        if has_direct_feel(line) {
            out.push(violation(Rule::DirectFeel));
        }
        if let Some(start) = EVAL_CALLS.iter().filter_map(|c| line.find(c)).min() {
            let stmt = statement_from(&lines, i, raw.len() - raw.trim_start().len() + start);
            if FALLBACKS.iter().any(|f| stmt.contains(f)) {
                out.push(violation(Rule::Fallback));
            }
        }
    }
    out
}

/// Applies the allow-list: returns every problem (unlisted violation, stale or
/// miscounted entry). Empty means clean.
fn check(violations: &[Violation], allow: &[Allow]) -> Vec<String> {
    let mut problems = Vec::new();
    let mut covered = vec![false; violations.len()];
    for entry in allow {
        let hits: Vec<usize> = violations
            .iter()
            .enumerate()
            .filter(|(_, v)| {
                v.file == entry.file && v.rule == entry.rule && v.text.contains(entry.contains)
            })
            .map(|(i, _)| i)
            .collect();
        if hits.len() != entry.count {
            problems.push(format!(
                "stale allow-list entry ({} in {} containing `{}`, {:?}): expected {} site(s), found {} — \
                 update or remove it",
                entry.rule.name(),
                entry.file,
                entry.contains,
                entry.why,
                entry.count,
                hits.len()
            ));
        }
        for i in hits {
            covered[i] = true;
        }
    }
    for (v, ok) in violations.iter().zip(covered) {
        if !ok {
            problems.push(format!(
                "{}:{}: [{}] silent FEEL fallback / evaluation bypass: `{}` — evaluate through \
                 engine/expression.rs (evaluate_or_failure) and raise the Zeebe incident, or \
                 allow-list it in tests/feel_fallback_lint.rs citing Zeebe",
                v.file,
                v.line,
                v.rule.name(),
                v.text
            ));
        }
    }
    problems
}

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let mut entries: Vec<_> = fs::read_dir(dir)
        .expect("read engine dir")
        .map(|e| e.expect("dir entry").path())
        .collect();
    entries.sort();
    for p in entries {
        if p.is_dir() {
            // The engine's unit-test tree is not production code.
            if p.file_name().is_some_and(|n| n == "tests") {
                continue;
            }
            rust_files(&p, out);
        } else if p.extension().is_some_and(|e| e == "rs") {
            out.push(p);
        }
    }
}

fn engine_violations() -> Vec<Violation> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/engine");
    let mut files = Vec::new();
    rust_files(&root, &mut files);
    let mut out = Vec::new();
    for path in files {
        let rel = path
            .strip_prefix(&root)
            .expect("under engine root")
            .to_string_lossy()
            .replace('\\', "/");
        // The single evaluation entry point itself.
        if rel == "expression.rs" {
            continue;
        }
        let src = fs::read_to_string(&path).expect("read source");
        out.extend(scan(&rel, &src));
    }
    out
}

#[test]
fn engine_has_no_unlisted_silent_feel_fallbacks() {
    let problems = check(&engine_violations(), ALLOW);
    assert!(
        problems.is_empty(),
        "feel_fallback_lint:\n{}",
        problems.join("\n")
    );
}

#[test]
fn seeded_violation_fails_the_lint() {
    // The #1390 shape: a failed job-type expression replaced by its own text.
    let seeded = r#"
fn resolve(vars: &HashMap<String, Value>, job_type: &str) -> String {
    crate::feel::eval_string(job_type, vars).unwrap_or_else(|_| job_type.to_string())
}
fn assignee(vars: &HashMap<String, Value>) -> Option<String> {
    let a = expression::evaluate_string("=assignee", vars)
        .ok();
    a
}
fn multiline(vars: &HashMap<String, Value>, raw: &str) -> String {
    expression::evaluate_string(
        raw,
        vars,
    )
    .unwrap_or_else(|_| raw.to_string())
}
fn clean(vars: &HashMap<String, Value>, raw: &str) -> Result<String, Failure> {
    let s = expression::evaluate_string(raw, vars)?;
    let fallback = other.unwrap_or_default();
    Ok(s)
}
"#;
    let violations = scan("seeded.rs", seeded);
    let rules: Vec<(usize, Rule)> = violations.iter().map(|v| (v.line, v.rule)).collect();
    assert_eq!(
        rules,
        vec![
            (3, Rule::DirectFeel),
            (3, Rule::Fallback),
            (6, Rule::Fallback),
            (11, Rule::Fallback)
        ],
        "{violations:?}"
    );
    let problems = check(&violations, &[]);
    assert_eq!(problems.len(), 4, "{problems:?}");
    assert!(problems[0].contains("seeded.rs:3"), "{problems:?}");
}

#[test]
fn comments_and_inline_tests_are_ignored() {
    let src = "// crate::feel::eval(x, v).ok()\nfn f() {}\n#[cfg(test)]\nmod tests { fn t() { crate::feel::eval(x, v).ok(); } }\n";
    assert!(scan("c.rs", src).is_empty());
}

#[test]
fn stale_allow_list_entry_fails_the_lint() {
    let stale = [Allow {
        file: "gone.rs",
        contains: "crate::feel::eval(",
        rule: Rule::DirectFeel,
        count: 1,
        why: Why::Zeebe("seeded"),
    }];
    let problems = check(&[], &stale);
    assert_eq!(problems.len(), 1);
    assert!(
        problems[0].starts_with("stale allow-list entry"),
        "{problems:?}"
    );
}

#[test]
fn allow_list_entry_covering_more_sites_than_declared_fails() {
    let v = scan(
        "x.rs",
        "let a = crate::feel::eval(e, v);\nlet b = crate::feel::eval(e, v);\n",
    );
    let entry = [Allow {
        file: "x.rs",
        contains: "crate::feel::eval(e, v)",
        rule: Rule::DirectFeel,
        count: 1,
        why: Why::Zeebe("seeded"),
    }];
    let problems = check(&v, &entry);
    assert_eq!(
        problems.len(),
        1,
        "a new duplicate site must not hide behind an entry: {problems:?}"
    );
}
