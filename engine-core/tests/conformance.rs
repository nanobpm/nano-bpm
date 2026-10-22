//! Differential conformance harness — Nano's parser vs Zeebe's deploy verdicts
//! (epic #850, deploy-validation parity; final slice #857).
//!
//! A checked-in corpus of `.bpmn` fixtures (in `tests/conformance/corpus/`) is
//! run through Nano's [`parse_bpmn`] and each model's expected verdict — ACCEPT,
//! or REJECT with a category — is asserted to MATCH Zeebe. Zeebe cannot run
//! in-process, so each fixture carries its Zeebe-derived verdict **declaratively**
//! (a leading `<!-- verdict: … -->` comment), captured once and committed
//! alongside the model. New parity gaps then fail CI instead of shipping
//! silently: this converts deploy-validation parity from per-bug whack-a-mole
//! into a permanently-guarded invariant.
//!
//! This harness aggregates the fixtures/error-categories introduced by every
//! prior slice of the epic (#849/#851 sub-class A, #853 sub-class B,
//! #854/#855/#856 sub-class C), so it can only go green once all those fixes are
//! on `main`. Reverting any one slice's fix flips one of its REJECT fixtures to
//! ACCEPT and turns the harness RED — the regression guarantee.
//!
//! See `tests/conformance/README.md` for: how to add a corpus entry, how the
//! verdicts were captured from Zeebe, and the Nano↔Zeebe reject-category mapping
//! table (whose single source of truth is [`NANO_ZEEBE_MAPPING`] below).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "non-production target (integration tests / examples); the production deny-unwrap gate (#1357) applies to the library crate only"
)]
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use nanobpmn_engine_core::bpmn::{parse_bpmn, ParseError};
use nanobpmn_engine_core::ElementKind;

// ───────────────────────────── mapping table ──────────────────────────────

/// One row of the single-source-of-truth mapping from a Nano [`ParseError`]
/// category to the Zeebe rejection class it corresponds to. The harness proves,
/// per corpus entry, that Nano rejects iff Zeebe rejects and that Nano's
/// category maps here to the expected Zeebe class — the text need not be
/// identical, only the class.
struct Mapping {
    /// Stable Nano category key — the [`ParseError`] variant name, as returned
    /// by [`nano_category`]. This is what a REJECT fixture tags itself with.
    nano: &'static str,
    /// The Zeebe validator family / rejection class this maps to. `INVALID_ARGUMENT`
    /// is Zeebe's deploy-rejection gRPC status; the parenthetical names the
    /// concrete Zeebe validator (see the README for source pointers).
    zeebe: &'static str,
    /// The wave-N slice that established this parity (for traceability), or the
    /// pre-epic baseline that already enforced it.
    origin: &'static str,
}

/// The Nano↔Zeebe reject-category mapping table — the single source of truth,
/// mirrored in `tests/conformance/README.md`. It covers every [`ParseError`]
/// variant that is a genuine Zeebe-parity rejection. Together with
/// [`NANO_ONLY_DIVERGENCES`] (the intentional Nano-only rejections Zeebe accepts)
/// the two tables **partition** the full [`ParseError`] surface — every category
/// is in exactly one of them (enforced by [`nano_category`]'s exhaustive match
/// plus [`mapping_covers_every_parse_error_category`]). A new Zeebe-parity
/// variant belongs here; a new Nano-only divergence belongs in
/// [`NANO_ONLY_DIVERGENCES`].
const NANO_ZEEBE_MAPPING: &[Mapping] = &[
    Mapping {
        nano: "MalformedXml",
        zeebe: "INVALID_ARGUMENT (not well-formed XML / SAXParseException)",
        origin: "baseline",
    },
    Mapping {
        nano: "ProcessWithoutId",
        zeebe: "INVALID_ARGUMENT (Process must have an id / bpmnProcessId)",
        origin: "baseline",
    },
    Mapping {
        nano: "IncompleteSequenceFlow",
        zeebe: "INVALID_ARGUMENT (SequenceFlow source/target QName unresolved)",
        origin: "baseline",
    },
    Mapping {
        nano: "NoProcess",
        zeebe: "INVALID_ARGUMENT (resource contains no executable process)",
        origin: "baseline",
    },
    Mapping {
        nano: "InvalidProcess",
        zeebe: "INVALID_ARGUMENT (ProcessValidator: no start event / unresolved flow target)",
        origin: "baseline + #855",
    },
    Mapping {
        nano: "InvalidBoundaryEvent",
        zeebe: "INVALID_ARGUMENT (BoundaryEvent attachedToRef unresolved)",
        origin: "baseline",
    },
    Mapping {
        nano: "InvalidMessageEvent",
        zeebe: "INVALID_ARGUMENT (unresolved messageRef / missing correlationKey)",
        origin: "baseline",
    },
    Mapping {
        nano: "InvalidLinkedResource",
        zeebe: "INVALID_ARGUMENT (zeebe:linkedResource missing required attribute)",
        origin: "baseline",
    },
    Mapping {
        nano: "UnresolvedReference",
        zeebe: "INVALID_ARGUMENT (camunda-xml-model eager QName resolution failure)",
        origin: "#849/#851",
    },
    Mapping {
        nano: "UnsupportedElement",
        zeebe: "INVALID_ARGUMENT (element type has no Zeebe transformer)",
        origin: "#853",
    },
    Mapping {
        nano: "InvalidGateway",
        zeebe: "INVALID_ARGUMENT (SequenceFlowValidator: condition-or-default)",
        origin: "#854",
    },
    Mapping {
        nano: "InvalidStartEvents",
        zeebe: "INVALID_ARGUMENT (ProcessValidator: multiple none start events)",
        origin: "#855",
    },
    Mapping {
        nano: "InvalidEndEvent",
        zeebe: "INVALID_ARGUMENT (EndEventValidator: end event has outgoing flow)",
        origin: "#856",
    },
    Mapping {
        nano: "DuplicateStartEvent",
        zeebe: "INVALID_ARGUMENT (ModelUtil.verifyNoDuplicate{Message,Signal}StartEvents)",
        origin: "#856",
    },
    Mapping {
        nano: "InvalidTaskDefinition",
        zeebe: "INVALID_ARGUMENT (ZeebeElementValidator hasNonEmptyAttribute)",
        origin: "#856",
    },
    Mapping {
        nano: "InvalidAgentDefinition",
        zeebe: "INVALID_ARGUMENT (AgentDefinitionValidator agentType placement)",
        origin: "agent-instance-parity S1",
    },
];

/// Maps a [`ParseError`] to its stable category key. The match is **exhaustive
/// with no wildcard**: adding a new `ParseError` variant to the shared enum
/// forces a new arm here, which in turn forces a row in **exactly one** of
/// [`NANO_ZEEBE_MAPPING`] (a Zeebe-parity rejection) or [`NANO_ONLY_DIVERGENCES`]
/// (an intentional Nano-only divergence) and (via the coverage ratchets) a
/// corpus entry — closing the drift surface.
fn nano_category(err: &ParseError) -> &'static str {
    match err {
        ParseError::MalformedXml(_) => "MalformedXml",
        ParseError::ProcessWithoutId => "ProcessWithoutId",
        ParseError::IncompleteSequenceFlow { .. } => "IncompleteSequenceFlow",
        ParseError::NoProcess => "NoProcess",
        ParseError::InvalidProcess { .. } => "InvalidProcess",
        ParseError::InvalidBoundaryEvent { .. } => "InvalidBoundaryEvent",
        ParseError::InvalidMessageEvent { .. } => "InvalidMessageEvent",
        ParseError::InvalidLinkedResource { .. } => "InvalidLinkedResource",
        ParseError::UnsupportedUserTaskFormBinding { .. } => "UnsupportedUserTaskFormBinding",
        ParseError::UnresolvedReference { .. } => "UnresolvedReference",
        ParseError::UnsupportedElement { .. } => "UnsupportedElement",
        ParseError::InvalidGateway { .. } => "InvalidGateway",
        ParseError::InvalidStartEvents { .. } => "InvalidStartEvents",
        ParseError::InvalidEndEvent { .. } => "InvalidEndEvent",
        ParseError::DuplicateStartEvent { .. } => "DuplicateStartEvent",
        ParseError::InvalidTaskDefinition { .. } => "InvalidTaskDefinition",
        ParseError::InvalidAgentDefinition { .. } => "InvalidAgentDefinition",
        ParseError::UnsupportedExecutionListener { .. } => "UnsupportedExecutionListener",
        ParseError::UnsupportedTaskListener { .. } => "UnsupportedTaskListener",
    }
}

fn mapping_for(nano: &str) -> Option<&'static Mapping> {
    NANO_ZEEBE_MAPPING.iter().find(|m| m.nano == nano)
}

// ───────────────────────── Nano-only divergences ──────────────────────────

/// One row describing an **intentional Nano/Zeebe divergence**: a model Zeebe
/// *accepts* but Nano deliberately *rejects*, because Nano does not implement the
/// feature and refuses to silently degrade it.
///
/// This is categorically distinct from [`NANO_ZEEBE_MAPPING`], whose invariant is
/// genuine parity — Nano rejects **iff** Zeebe rejects. A divergence must **not**
/// be shoehorned into that table: doing so would fabricate a Zeebe rejection
/// class for a model Zeebe actually accepts, making the conformance oracle assert
/// a falsehood and blinding it to the very divergence it is meant to record. A
/// divergence carries no Zeebe rejection class — only the rationale for why Nano
/// intentionally differs.
struct Divergence {
    /// Stable Nano category key — the [`ParseError`] variant name, as returned by
    /// [`nano_category`]. This is what a `diverge` fixture tags itself with.
    nano: &'static str,
    /// Why Nano rejects a model Zeebe accepts (the deliberate non-implementation).
    rationale: &'static str,
    /// The slice that established this divergence (for traceability).
    origin: &'static str,
}

/// The Nano-only divergence registry — models Zeebe **accepts** but Nano
/// intentionally **rejects**. Together with [`NANO_ZEEBE_MAPPING`] it partitions
/// the full [`ParseError`] surface (every category is in exactly one of the two,
/// enforced by [`mapping_covers_every_parse_error_category`]).
const NANO_ONLY_DIVERGENCES: &[Divergence] = &[
    Divergence {
        nano: "UnsupportedUserTaskFormBinding",
        rationale: "Zeebe implements the `deployment` and `versionTag` user-task form \
                    bindings and ACCEPTS such a model; Nano implements only `latest` and \
                    rejects the deploy loudly rather than silently degrading the binding \
                    to `latest` (#1190).",
        origin: "#1190",
    },
    Divergence {
        nano: "UnsupportedExecutionListener",
        rationale: "Zeebe ACCEPTS `zeebe:executionListener`s on multi-incoming \
                    parallel/inclusive joins, on compensation boundary events, on \
                    terminate end events, on surplus signal start events, on \
                    ad-hoc sub-process tools, and \
                    on sequence flows; Nano rejects those it cannot enact: a parallel \
                    join completes without running the activation body or the \
                    end-listener chain (both phases dead), an inclusive join's \
                    `start` listener never fires (the join short-circuits the \
                    activation body — but its `end` listener IS supported and is \
                    accepted, since an accepted join runs the end-listener chain \
                    before routing), a compensation boundary is a passive marker \
                    never entered by token flow, a terminate end event's `end` \
                    listener never fires (its scope-wide teardown emits completion \
                    directly, bypassing the end-listener chain — its `start` \
                    listener IS supported and accepted), a surplus signal start is \
                    demoted to an inert throw event that is never activated, an \
                    ad-hoc tool is pruned or \
                    activated/completed with direct lifecycle events (bypassing the \
                    listener gate), and a sequence flow is an edge with \
                    no lifecycle. Nano rejects the unsupported deploy loudly rather \
                    than silently store (or drop) a dead listener (#1197).",
        origin: "#1197",
    },
    Divergence {
        nano: "UnsupportedTaskListener",
        rationale: "Zeebe ACCEPTS a `zeebe:taskListener` only on a user task, but \
                    its lenient parser does not reject one misplaced on another \
                    element; Nano rejects a task listener on any non-user-task \
                    element (e.g. a `receiveTask`, which rides the `io_stack` for \
                    its execution listeners): task-listener jobs are created only on \
                    the user-task runtime path, so such a listener could never fire. \
                    Nano rejects it loudly rather than store a dead task listener \
                    (#1197).",
        origin: "#1197",
    },
];

fn divergence_for(nano: &str) -> Option<&'static Divergence> {
    NANO_ONLY_DIVERGENCES.iter().find(|d| d.nano == nano)
}

// ─────────────────────────── corpus loading ───────────────────────────────

/// A parsed corpus entry: the fixture path, its declarative expectation, and
/// the raw model XML.
struct CorpusEntry {
    name: String,
    xml: String,
    expected: Expectation,
}

#[derive(Debug, PartialEq)]
enum Expectation {
    /// Zeebe accepts this model; Nano's `parse_bpmn` must return `Ok`.
    Accept,
    /// Zeebe rejects this model; Nano's `parse_bpmn` must return `Err` whose
    /// [`nano_category`] equals this category.
    Reject { category: String },
    /// An **intentional Nano/Zeebe divergence**: Zeebe *accepts* this model but
    /// Nano deliberately *rejects* it (it does not implement the feature). Nano's
    /// `parse_bpmn` must return `Err` whose [`nano_category`] equals this category
    /// and which is registered in [`NANO_ONLY_DIVERGENCES`]. Recorded so the
    /// oracle never falsely claims Zeebe rejected a model it accepts.
    NanoReject { category: String },
}

fn corpus_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/conformance/corpus")
}

/// Extracts the leading `<!-- verdict: … -->` directive from a fixture.
///
/// Scans only the fixture's *prolog* — the run of leading blank lines, XML
/// declarations, and `<!-- … -->` comment lines before the first element
/// content — for the comment line carrying the `verdict:` keyword. Stopping at
/// the first content line means an in-model BPMN comment later in the document
/// (e.g. modeller-exported documentation) cannot be mistaken for the directive,
/// enforcing the leading-comment contract documented in
/// `tests/conformance/README.md`.
///
/// Grammar (case-insensitive on the keywords):
///   * `<!-- verdict: accept -->`
///   * `<!-- verdict: reject | category: <NanoCategory> -->`
///   * `<!-- verdict: diverge | category: <NanoCategory> -->`
///     (an intentional Nano-only rejection of a model Zeebe accepts)
fn parse_directive(name: &str, xml: &str) -> Expectation {
    let mut in_comment = false;
    let comment = xml
        .lines()
        .take_while(|l| is_prolog_line(l, &mut in_comment))
        .find(|l| l.contains("<!--") && contains_keyword(l, "verdict:"))
        .unwrap_or_else(|| panic!("{name}: missing leading `<!-- verdict: … -->` directive"));
    let after_verdict = after_keyword(comment, "verdict:")
        .expect("verdict token")
        .trim();
    let verdict = after_verdict
        .split(|c: char| c.is_whitespace() || c == '|' || c == '-')
        .find(|t| !t.is_empty())
        .unwrap_or("")
        .to_ascii_lowercase();
    // Both `reject` and `diverge` carry a `category:` naming the Nano ParseError;
    // extract it once so the two arms cannot drift.
    let category = |kind: &str| -> String {
        after_keyword(comment, "category:")
            .unwrap_or_else(|| panic!("{name}: {kind} directive missing `category:`"))
            .split(|c: char| c.is_whitespace() || c == '|' || c == '-' || c == '>')
            .find(|t| !t.is_empty())
            .unwrap_or_else(|| panic!("{name}: empty {kind} category"))
            .to_string()
    };
    match verdict.as_str() {
        "accept" => Expectation::Accept,
        "reject" => Expectation::Reject {
            category: category("reject"),
        },
        "diverge" => Expectation::NanoReject {
            category: category("diverge"),
        },
        other => panic!("{name}: unknown verdict `{other}` (expected accept|reject|diverge)"),
    }
}

/// Case-insensitive check for an ASCII `keyword` within `haystack`.
fn contains_keyword(haystack: &str, keyword: &str) -> bool {
    after_keyword(haystack, keyword).is_some()
}

/// Whether `line` belongs to a fixture's leading prolog: a blank line, an XML
/// declaration (`<?xml … ?>`), or an XML comment — **including the continuation
/// and closing lines of a *wrapped* multi-line `<!-- … -->` comment**. The
/// `in_comment` cursor carries that open-comment state across lines so a comment
/// that spans several lines (e.g. a wrapped `oracle:` note) does not prematurely
/// end the prolog scan on a continuation line that lacks a leading `<!--`. The
/// first line that is none of these marks the start of element content, bounding
/// the directive search in [`parse_directive`] to the leading comment block.
fn is_prolog_line(line: &str, in_comment: &mut bool) -> bool {
    let trimmed = line.trim_start();
    if *in_comment {
        // Inside a wrapped comment: this line is still prolog; clear the cursor
        // once the comment closes.
        if trimmed.contains("-->") {
            *in_comment = false;
        }
        return true;
    }
    if trimmed.is_empty() || trimmed.starts_with("<?") {
        return true;
    }
    if trimmed.starts_with("<!--") {
        // A comment that does not close on this line opens a wrapped comment.
        if !trimmed.contains("-->") {
            *in_comment = true;
        }
        return true;
    }
    false
}

/// Returns the slice of `haystack` following the first case-insensitive match of
/// the ASCII `keyword`. Both sides are ASCII-lowercased, so the match is
/// case-insensitive regardless of how `keyword` is cased. ASCII-lowercasing
/// preserves byte length, so indices from the lowercased copy align with the
/// original string.
fn after_keyword<'a>(haystack: &'a str, keyword: &str) -> Option<&'a str> {
    let needle = keyword.to_ascii_lowercase();
    haystack
        .to_ascii_lowercase()
        .find(&needle)
        .map(|i| &haystack[i + needle.len()..])
}

fn load_corpus() -> Vec<CorpusEntry> {
    let dir = corpus_dir();
    let mut paths: Vec<PathBuf> = fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("cannot read corpus dir {}: {e}", dir.display()))
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("bpmn"))
        .collect();
    paths.sort();
    assert!(
        !paths.is_empty(),
        "conformance corpus is empty at {}",
        dir.display()
    );
    paths
        .into_iter()
        .map(|p| {
            let name = p.file_name().unwrap().to_str().unwrap().to_string();
            let xml = fs::read_to_string(&p).unwrap();
            let expected = parse_directive(&name, &xml);
            CorpusEntry {
                name,
                xml,
                expected,
            }
        })
        .collect()
}

// ─────────────────────────── the assertions ───────────────────────────────

/// The core differential assertion: for every corpus entry, `parse_bpmn` accepts
/// iff Zeebe accepts, and on a reject Nano's category matches the fixture's
/// captured Zeebe-derived category (via the [`NANO_ZEEBE_MAPPING`] table).
#[test]
fn parse_bpmn_matches_zeebe_verdict_for_every_corpus_entry() {
    let mut failures = Vec::new();
    for entry in load_corpus() {
        let result = parse_bpmn(&entry.xml);
        match (&entry.expected, &result) {
            (Expectation::Accept, Ok(_)) => {}
            (Expectation::Accept, Err(e)) => failures.push(format!(
                "{}: expected ACCEPT (Zeebe accepts) but Nano REJECTED with {:?}",
                entry.name, e
            )),
            (Expectation::Reject { category }, Ok(_)) => failures.push(format!(
                "{}: expected REJECT ({category}, Zeebe rejects) but Nano ACCEPTED — \
                 a parity fix has regressed",
                entry.name
            )),
            (Expectation::Reject { category }, Err(e)) => {
                let actual = nano_category(e);
                if actual != category {
                    failures.push(format!(
                        "{}: expected REJECT category `{category}` but Nano rejected with `{actual}` ({e:?})",
                        entry.name
                    ));
                } else if mapping_for(actual).is_none() {
                    failures.push(format!(
                        "{}: category `{actual}` is not in the Nano↔Zeebe mapping table",
                        entry.name
                    ));
                }
            }
            (Expectation::NanoReject { category }, Ok(_)) => failures.push(format!(
                "{}: expected DIVERGE ({category}: Zeebe accepts, Nano intentionally rejects) \
                 but Nano ACCEPTED — the intentional divergence has regressed",
                entry.name
            )),
            (Expectation::NanoReject { category }, Err(e)) => {
                let actual = nano_category(e);
                if actual != category {
                    failures.push(format!(
                        "{}: expected DIVERGE category `{category}` but Nano rejected with `{actual}` ({e:?})",
                        entry.name
                    ));
                } else if divergence_for(actual).is_none() {
                    failures.push(format!(
                        "{}: category `{actual}` is not in the Nano-only divergence table",
                        entry.name
                    ));
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "conformance parity mismatches ({}):\n  {}",
        failures.len(),
        failures.join("\n  ")
    );
}

/// Every fixture's declared category must be a real Nano category that exists in
/// the appropriate registry: a `reject` fixture maps to the Zeebe-parity table,
/// a `diverge` fixture to the Nano-only divergence table (guards against a typo'd
/// tag silently passing, and against a divergence masquerading as parity).
#[test]
fn every_reject_fixture_tags_a_mapped_category() {
    for entry in load_corpus() {
        match &entry.expected {
            Expectation::Reject { category } => assert!(
                mapping_for(category).is_some(),
                "{}: reject category `{category}` is not a known Nano↔Zeebe mapping key",
                entry.name
            ),
            Expectation::NanoReject { category } => assert!(
                divergence_for(category).is_some(),
                "{}: diverge category `{category}` is not a known Nano-only divergence key",
                entry.name
            ),
            Expectation::Accept => {}
        }
    }
}

/// Coverage ratchet #1 — every Nano [`ParseError`] category in the mapping table
/// is exercised by at least one REJECT corpus entry. Because the mapping table
/// must cover the full enum (below), this transitively guarantees the whole
/// shared `ParseError` surface is guarded, and reports which validator families
/// are represented.
#[test]
fn every_mapped_category_has_a_reject_corpus_entry() {
    let covered: BTreeSet<String> = load_corpus()
        .into_iter()
        .filter_map(|e| match e.expected {
            Expectation::Reject { category } => Some(category),
            Expectation::NanoReject { .. } | Expectation::Accept => None,
        })
        .collect();

    let missing: Vec<&str> = NANO_ZEEBE_MAPPING
        .iter()
        .map(|m| m.nano)
        .filter(|n| !covered.contains(*n))
        .collect();

    // Report the represented families for visibility in `cargo test -- --nocapture`.
    eprintln!(
        "[conformance] Zeebe validator families represented ({}/{}):",
        covered.len(),
        NANO_ZEEBE_MAPPING.len()
    );
    for m in NANO_ZEEBE_MAPPING {
        eprintln!(
            "  [{}] {} -> {} ({})",
            if covered.contains(m.nano) { "x" } else { " " },
            m.nano,
            m.zeebe,
            m.origin
        );
    }

    assert!(
        missing.is_empty(),
        "these mapped ParseError categories have no REJECT corpus entry (add one — see README): {missing:?}"
    );
}

/// Coverage ratchet #1b — every Nano-only divergence category is exercised by at
/// least one DIVERGE corpus entry, mirroring the reject-category ratchet for the
/// [`NANO_ONLY_DIVERGENCES`] registry so an intentional divergence cannot lose
/// its regression fixture unnoticed.
#[test]
fn every_divergence_category_has_a_diverge_corpus_entry() {
    let covered: BTreeSet<String> = load_corpus()
        .into_iter()
        .filter_map(|e| match e.expected {
            Expectation::NanoReject { category } => Some(category),
            Expectation::Reject { .. } | Expectation::Accept => None,
        })
        .collect();

    let missing: Vec<&str> = NANO_ONLY_DIVERGENCES
        .iter()
        .map(|d| d.nano)
        .filter(|n| !covered.contains(*n))
        .collect();

    // Report the registered divergences for visibility in `-- --nocapture`.
    eprintln!(
        "[conformance] intentional Nano-only divergences ({}):",
        NANO_ONLY_DIVERGENCES.len()
    );
    for d in NANO_ONLY_DIVERGENCES {
        eprintln!(
            "  [{}] {} — {} ({})",
            if covered.contains(d.nano) { "x" } else { " " },
            d.nano,
            d.rationale,
            d.origin
        );
    }

    assert!(
        missing.is_empty(),
        "these Nano-only divergence categories have no DIVERGE corpus entry (add one — see README): {missing:?}"
    );
}

/// One witness value per [`ParseError`] variant. This is the single source for
/// "every category that can exist": the category strings are **derived** by
/// running each witness through [`nano_category`] rather than re-typed into a
/// parallel list that could drift.
///
/// The trailing `match` is exhaustive **with no wildcard**, so adding a
/// `ParseError` variant fails to compile here until a witness is added above —
/// which then flows automatically into the coverage assertions in
/// [`mapping_covers_every_parse_error_category`].
fn parse_error_witnesses() -> Vec<ParseError> {
    let witnesses = vec![
        ParseError::MalformedXml(String::new()),
        ParseError::ProcessWithoutId,
        ParseError::IncompleteSequenceFlow {
            process_id: String::new(),
        },
        ParseError::NoProcess,
        ParseError::InvalidProcess {
            process_id: String::new(),
            reason: String::new(),
        },
        ParseError::InvalidBoundaryEvent {
            process_id: String::new(),
            reason: String::new(),
        },
        ParseError::InvalidMessageEvent {
            process_id: String::new(),
            reason: String::new(),
        },
        ParseError::InvalidLinkedResource {
            task_id: String::new(),
            attribute: String::new(),
        },
        ParseError::UnsupportedUserTaskFormBinding {
            task_id: String::new(),
            binding_type: String::new(),
        },
        ParseError::UnresolvedReference {
            kind: String::new(),
            id: String::new(),
            process_id: String::new(),
            from_node: String::new(),
        },
        ParseError::UnsupportedElement {
            tag: String::new(),
            element_id: String::new(),
        },
        ParseError::InvalidGateway {
            process_id: String::new(),
            gateway_id: String::new(),
            reason: String::new(),
        },
        ParseError::InvalidStartEvents {
            process_id: String::new(),
            reason: String::new(),
        },
        ParseError::InvalidEndEvent {
            process_id: String::new(),
            element_id: String::new(),
            reason: String::new(),
        },
        ParseError::DuplicateStartEvent {
            process_id: String::new(),
            correlation_kind: String::new(),
            reference: String::new(),
            reason: String::new(),
        },
        ParseError::InvalidTaskDefinition {
            process_id: String::new(),
            task_id: String::new(),
            attribute: String::new(),
            reason: String::new(),
        },
        ParseError::InvalidAgentDefinition {
            process_id: String::new(),
            element_id: String::new(),
            reason: String::new(),
        },
        ParseError::UnsupportedExecutionListener {
            process_id: String::new(),
            element_id: String::new(),
            reason: String::new(),
        },
        ParseError::UnsupportedTaskListener {
            process_id: String::new(),
            element_id: String::new(),
            reason: String::new(),
        },
    ];
    // Compile-time completeness ratchet: this exhaustive, wildcard-free match
    // will not compile if a `ParseError` variant is added without a witness
    // above. It never changes behaviour — it purely forces the list to stay
    // complete.
    for w in &witnesses {
        match w {
            ParseError::MalformedXml(_)
            | ParseError::ProcessWithoutId
            | ParseError::IncompleteSequenceFlow { .. }
            | ParseError::NoProcess
            | ParseError::InvalidProcess { .. }
            | ParseError::InvalidBoundaryEvent { .. }
            | ParseError::InvalidMessageEvent { .. }
            | ParseError::InvalidLinkedResource { .. }
            | ParseError::UnsupportedUserTaskFormBinding { .. }
            | ParseError::UnresolvedReference { .. }
            | ParseError::UnsupportedElement { .. }
            | ParseError::InvalidGateway { .. }
            | ParseError::InvalidStartEvents { .. }
            | ParseError::InvalidEndEvent { .. }
            | ParseError::DuplicateStartEvent { .. }
            | ParseError::InvalidTaskDefinition { .. } => {}
            ParseError::InvalidAgentDefinition { .. } => {}
            ParseError::UnsupportedExecutionListener { .. } => {}
            ParseError::UnsupportedTaskListener { .. } => {}
        }
    }
    witnesses
}

/// The two registries together must cover every [`ParseError`] category, and be
/// **disjoint**. The category set is derived from [`parse_error_witnesses`] (one
/// witness per enum variant, kept complete by a compile-time exhaustiveness
/// ratchet) rather than a hand-maintained string list, so a new `ParseError`
/// variant forces both a witness and a row in exactly one of the two registries
/// ([`NANO_ZEEBE_MAPPING`] for a genuine Zeebe-parity rejection, or
/// [`NANO_ONLY_DIVERGENCES`] for an intentional Nano-only divergence).
#[test]
fn mapping_covers_every_parse_error_category() {
    let all_categories: BTreeSet<&'static str> =
        parse_error_witnesses().iter().map(nano_category).collect();
    for cat in &all_categories {
        assert!(
            mapping_for(cat).is_some() || divergence_for(cat).is_some(),
            "ParseError category `{cat}` is in neither the Nano↔Zeebe mapping nor the \
             Nano-only divergence table"
        );
        assert!(
            !(mapping_for(cat).is_some() && divergence_for(cat).is_some()),
            "ParseError category `{cat}` is in BOTH the Nano↔Zeebe mapping and the \
             Nano-only divergence table — a category is either genuine parity or a \
             divergence, never both"
        );
    }
    // And no stale rows in either table for categories that no longer exist.
    for m in NANO_ZEEBE_MAPPING {
        assert!(
            all_categories.contains(&m.nano),
            "mapping row `{}` names an unknown ParseError category",
            m.nano
        );
    }
    for d in NANO_ONLY_DIVERGENCES {
        assert!(
            all_categories.contains(&d.nano),
            "divergence row `{}` names an unknown ParseError category",
            d.nano
        );
    }
}

// ───────────────── coverage ratchet #2: element-kind registry ──────────────

/// A coarse family of modelled BPMN element kinds, used by the element-kind
/// coverage ratchet.
///
/// The variant list **and** [`ElementFamily::ALL`] are generated from a single
/// `element_families!` invocation, so a new family lands in the enum and in
/// `ALL` in the same edit — there is no hand-maintained second list to drift out
/// of sync, and the coverage ratchet that iterates `ALL` therefore sees every
/// family by construction.
macro_rules! element_families {
    ($($variant:ident),+ $(,)?) => {
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
        enum ElementFamily {
            $($variant),+
        }

        impl ElementFamily {
            const ALL: &'static [ElementFamily] = &[$(ElementFamily::$variant),+];
        }
    };
}

element_families! {
    StartEvent,
    EndEvent,
    Task,
    Gateway,
    BoundaryEvent,
    IntermediateCatchEvent,
    IntermediateThrowEvent,
    MessageStartEvent,
    TimerStartEvent,
    SubProcess,
    CallActivity,
}

/// Maps every modelled [`ElementKind`] to a coarse [`ElementFamily`]. The match
/// is **exhaustive with no wildcard**: teaching the engine a new `ElementKind`
/// (the canonical registry mirrored by `processos`' `ELEMENT_KIND_SPECS`) forces
/// a new arm here. The coverage ratchet then operates at *family* granularity —
/// a new kind that maps to an already-covered family is not separately flagged;
/// only a kind that introduces an *uncovered* family trips the ratchet (ties
/// into slice #853's derive-from-registry guard).
fn element_kind_family(kind: &ElementKind) -> ElementFamily {
    match kind {
        ElementKind::StartEvent => ElementFamily::StartEvent,
        ElementKind::EndEvent | ElementKind::TerminateEndEvent => ElementFamily::EndEvent,
        ElementKind::ServiceTask { .. }
        | ElementKind::BusinessRuleTask { .. }
        | ElementKind::UserTask(_)
        | ElementKind::ScriptTask { .. }
        | ElementKind::AgentTask { .. }
        | ElementKind::Task => ElementFamily::Task,
        ElementKind::ExclusiveGateway
        | ElementKind::ParallelGateway
        | ElementKind::InclusiveGateway
        | ElementKind::EventBasedGateway => ElementFamily::Gateway,
        ElementKind::ErrorBoundaryEvent { .. }
        | ElementKind::TimerBoundaryEvent { .. }
        | ElementKind::MessageBoundaryEvent { .. }
        | ElementKind::SignalBoundaryEvent { .. }
        | ElementKind::CompensationBoundaryEvent { .. }
        | ElementKind::EscalationBoundaryEvent { .. }
        | ElementKind::ConditionalBoundaryEvent { .. } => ElementFamily::BoundaryEvent,
        ElementKind::TimerIntermediateCatchEvent { .. }
        | ElementKind::MessageIntermediateCatchEvent { .. }
        | ElementKind::SignalIntermediateCatchEvent { .. }
        | ElementKind::LinkIntermediateCatchEvent { .. }
        | ElementKind::ConditionalIntermediateCatchEvent { .. } => {
            ElementFamily::IntermediateCatchEvent
        }
        ElementKind::IntermediateThrowEvent
        | ElementKind::LinkIntermediateThrowEvent { .. }
        | ElementKind::EscalationThrowEvent { .. }
        | ElementKind::CompensationThrowEvent => ElementFamily::IntermediateThrowEvent,
        ElementKind::MessageStartEvent { .. } => ElementFamily::MessageStartEvent,
        ElementKind::TimerStartEvent { .. } => ElementFamily::TimerStartEvent,
        ElementKind::SubProcess { .. } => ElementFamily::SubProcess,
        ElementKind::CallActivity { .. } => ElementFamily::CallActivity,
    }
}

/// Families the current corpus deliberately does not yet exercise. This is the
/// ratchet's baseline: a NEW modelled element family must either be exercised by
/// an ACCEPT corpus entry or be explicitly parked here (with a follow-up), so it
/// can never slip into the registry untracked. Shrinking this set (adding real
/// coverage) is always welcome; growing it requires a deliberate, reviewed edit.
const BASELINE_UNCOVERED_FAMILIES: &[ElementFamily] = &[
    ElementFamily::BoundaryEvent,
    ElementFamily::IntermediateThrowEvent,
    ElementFamily::TimerStartEvent,
    ElementFamily::SubProcess,
    ElementFamily::CallActivity,
];

/// Coverage ratchet #2 — every modelled element *family* is either exercised by
/// an ACCEPT corpus entry or explicitly listed in [`BASELINE_UNCOVERED_FAMILIES`].
/// Adding a new `ElementKind` that introduces a new, uncovered family is
/// therefore flagged here (a new kind mapped into an already-covered family is
/// not), mirroring #853's registry-derived guard for the accept side of parity.
#[test]
fn every_element_family_is_covered_or_explicitly_baselined() {
    // Families actually produced by parsing the ACCEPT corpus.
    let mut represented: BTreeSet<ElementFamily> = BTreeSet::new();
    for entry in load_corpus() {
        if entry.expected != Expectation::Accept {
            continue;
        }
        let defs = parse_bpmn(&entry.xml)
            .unwrap_or_else(|e| panic!("{}: accept fixture failed to parse: {e:?}", entry.name));
        for def in &defs {
            for el in def.elements.values() {
                represented.insert(element_kind_family(&el.kind));
            }
        }
    }

    let baseline: BTreeSet<ElementFamily> = BASELINE_UNCOVERED_FAMILIES.iter().copied().collect();

    // A family cannot be both represented and baselined — shrink the baseline
    // when real coverage lands.
    let redundant: Vec<ElementFamily> = represented.intersection(&baseline).copied().collect();
    assert!(
        redundant.is_empty(),
        "these families are represented by the corpus and should be removed from \
         BASELINE_UNCOVERED_FAMILIES: {redundant:?}"
    );

    // Every known family must be accounted for: covered, or deliberately parked.
    let accounted: BTreeSet<ElementFamily> = represented.union(&baseline).copied().collect();
    let unaccounted: Vec<ElementFamily> = ElementFamily::ALL
        .iter()
        .copied()
        .filter(|f| !accounted.contains(f))
        .collect();
    assert!(
        unaccounted.is_empty(),
        "new modelled element families are neither exercised by an ACCEPT corpus entry \
         nor baselined (add a corpus fixture, or park them in BASELINE_UNCOVERED_FAMILIES): \
         {unaccounted:?}"
    );

    eprintln!(
        "[conformance] element families: {} represented, {} baselined (of {} total)",
        represented.len(),
        baseline.len(),
        ElementFamily::ALL.len()
    );
}

// ─────────────────────────── directive-parsing unit tests ─────────────────

/// A `verdict:` directive placed *after* a wrapped, multi-line leading comment
/// must still be found: the prolog scan has to treat the comment's continuation
/// and closing lines as prolog rather than stopping at the first line that does
/// not itself open with `<!--`.
#[test]
fn parse_directive_finds_verdict_after_wrapped_comment() {
    let xml = "\
<!-- oracle: this note wraps across
     several physical lines before the
     directive is reached -->
<!-- verdict: reject | category: InvalidProcess -->
<bpmn:definitions/>
";
    match parse_directive("wrapped", xml) {
        Expectation::Reject { category } => assert_eq!(category, "InvalidProcess"),
        other => panic!("expected reject, got {other:?}"),
    }
}

/// The prolog scan must still stop at the first element-content line, so an
/// in-model comment that only appears *after* content is never mistaken for the
/// directive.
#[test]
#[should_panic(expected = "missing leading")]
fn parse_directive_ignores_directive_after_content() {
    let xml = "\
<bpmn:definitions>
  <!-- verdict: accept -->
</bpmn:definitions>
";
    parse_directive("after-content", xml);
}
