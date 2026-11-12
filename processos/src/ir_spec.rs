//! The **declarative notation table** for the reversible semantic IR (ADR 0001) — one spec, many
//! consumers.
//!
//! The engine's possibility space is a closed algebraic data type ([`nanobpmn_engine_core::ElementKind`],
//! [`nanobpmn_engine_core::SequenceFlow`], and the shared element-level extras). This module holds
//! that possibility space in a single declarative table [`ELEMENT_KIND_SPECS`] and derives all
//! surfaces from it:
//!
//! 1. **GBNF for llama.cpp constrained decoding** — [`emit_gbnf`] renders the table to a per-kind
//!    grammar so a local model literally cannot emit invalid IR at sampling time.
//! 2. **Scoped `describe_ir_grammar` tool** — [`describe`] returns the productions for one element
//!    kind (or the compact one-page overview for none), so an LLM can look up "what can I say at an
//!    exclusive gateway?" on demand without dumping the whole grammar every turn.
//! 3. **Parity harness** — [`sample_instances`] materialises one placeholder of every variant so
//!    the tests can round-trip through the real pretty-printer and catch drift the moment a new
//!    `ElementKind` variant lands without a matching spec.
//!
//! The pretty-printer ([`crate::model_ir::render_kind_attrs`]) and the parser
//! ([`crate::model_ir::build_kind`]) remain exhaustive matches over `ElementKind` — that gives
//! compile-time coverage when a new engine variant lands. This table is kept honest against them
//! by [`tests::specs_match_pretty_printer`], which builds a placeholder instance of every variant
//! (via [`sample_instances`]), runs the real pretty-printer, and asserts the emitted attribute keys
//! match what the spec table declares. Add a new `ElementKind` → the compiler fails on the
//! pretty-printer/parser matches; forget to add it here → the parity test fails. Either way the
//! grammar cannot silently fall behind the engine.

use serde_json::{json, Value};

/// The type of an IR attribute value — drives both the GBNF rendering and the JSON-Schema mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttrType {
    /// A double-quoted string with `\` and `"` escaped (`"hello"`).
    Str,
    /// A bare identifier ([A-Za-z_][A-Za-z0-9_]*), used for cross-element references (`attachedTo
    /// review_task`, `parent inner_sp`).
    Id,
    /// A non-negative integer followed by the literal `ms` suffix (`5000ms`). Millisecond
    /// durations and intervals.
    Duration,
    /// The literal `true` or `false`.
    Bool,
}

/// One attribute an element kind may (or must) carry in the IR.
#[derive(Debug, Clone, Copy)]
pub struct AttrSpec {
    /// The IR keyword for this attribute (`jobType`, `attachedTo`, …).
    pub key: &'static str,
    /// True when the attribute must be present. Required attributes correspond to non-`Option`
    /// fields on the `ElementKind` variant.
    pub required: bool,
    /// The value type — see [`AttrType`].
    pub ty: AttrType,
    /// One-line prose used by [`describe`] and the GBNF comment header.
    pub doc: &'static str,
}

/// One `ElementKind` variant, its IR keyword, and its attribute list.
#[derive(Debug, Clone, Copy)]
pub struct KindSpec {
    /// The IR keyword (`serviceTask`, `exclusiveGateway`, …). Must match
    /// [`crate::model_ir::kind_keyword`] for the corresponding variant — enforced by
    /// [`tests::specs_match_pretty_printer`].
    pub keyword: &'static str,
    /// One-line prose describing the kind.
    pub doc: &'static str,
    /// The kind-specific attributes (the ones emitted by
    /// [`crate::model_ir::render_kind_attrs`]).
    pub attrs: &'static [AttrSpec],
}

/// The whole possibility space — every `ElementKind` variant, in the order they appear on the enum.
/// Extend this when adding a new engine element kind.
pub const ELEMENT_KIND_SPECS: &[KindSpec] = &[
    KindSpec {
        keyword: "startEvent",
        doc: "None start event. Pass-through; entry point of a process.",
        attrs: &[],
    },
    KindSpec {
        keyword: "endEvent",
        doc: "None end event. Consuming its last token completes the instance.",
        attrs: &[],
    },
    KindSpec {
        keyword: "terminateEndEvent",
        doc: "Terminate end event. Kills every other token in its enclosing scope, then completes that scope.",
        attrs: &[],
    },
    KindSpec {
        keyword: "serviceTask",
        doc: "Job-based task. Creates a job of `jobType` and rests until completion. \
              Optional agent metadata does not change the job-worker lifecycle.",
        attrs: &[
            AttrSpec {
                key: "jobType",
                required: true,
                ty: AttrType::Str,
                doc: "The `zeebe:taskDefinition type` a worker subscribes to.",
            },
            AttrSpec {
                key: "priority",
                required: false,
                ty: AttrType::Str,
                doc: "Raw priority expression (literal or FEEL); higher activates first.",
            },
            AttrSpec {
                key: "agentType",
                required: false,
                ty: AttrType::Str,
                doc: "Optional `zeebe:agentDefinition` metadata: `aiAgentTask`, `external`, \
                      or `aiAgentSubProcess`. All create ordinary jobs. The compact IR retains \
                      this classification but does not represent an adHocSubProcess tool catalog.",
            },
        ],
    },
    KindSpec {
        keyword: "businessRuleTask",
        doc: "DMN-bound task. Evaluates `decisionId` natively in-engine (no job) and merges the output.",
        attrs: &[
            AttrSpec {
                key: "decisionId",
                required: true,
                ty: AttrType::Str,
                doc: "Raw `zeebe:calledDecision decisionId` (literal id or FEEL).",
            },
            AttrSpec {
                key: "resultVariable",
                required: false,
                ty: AttrType::Str,
                doc: "Variable to bind the decision output to; if absent a map output is spread.",
            },
        ],
    },
    KindSpec {
        keyword: "userTask",
        doc: "Native user task. A human claims and completes it via the user-task API.",
        attrs: &[
            AttrSpec {
                key: "assignee",
                required: false,
                ty: AttrType::Str,
                doc: "Raw assignee expression (literal or FEEL).",
            },
            AttrSpec {
                key: "candidateGroups",
                required: false,
                ty: AttrType::Str,
                doc: "Comma-separated list or FEEL expression yielding a list.",
            },
            AttrSpec {
                key: "candidateUsers",
                required: false,
                ty: AttrType::Str,
                doc: "Comma-separated list or FEEL expression yielding a list.",
            },
            AttrSpec {
                key: "dueDate",
                required: false,
                ty: AttrType::Str,
                doc: "Raw due-date expression (literal or FEEL).",
            },
            AttrSpec {
                key: "followUpDate",
                required: false,
                ty: AttrType::Str,
                doc: "Raw follow-up-date expression (literal or FEEL).",
            },
            AttrSpec {
                key: "priority",
                required: false,
                ty: AttrType::Str,
                doc: "Raw priority expression (literal or FEEL); default 50.",
            },
            AttrSpec {
                key: "formId",
                required: false,
                ty: AttrType::Str,
                doc: "Embedded/deployment form id; resolved to a numeric form key at \
                      task creation.",
            },
            AttrSpec {
                key: "externalFormReference",
                required: false,
                ty: AttrType::Str,
                doc: "External form reference, surfaced verbatim.",
            },
        ],
    },
    KindSpec {
        keyword: "exclusiveGateway",
        doc: "XOR gateway. Takes exactly one outgoing flow (first matching condition; default is \
              fallback).",
        attrs: &[],
    },
    KindSpec {
        keyword: "parallelGateway",
        doc: "AND gateway. Splits to all outgoing; joins by waiting on every incoming flow.",
        attrs: &[],
    },
    KindSpec {
        keyword: "inclusiveGateway",
        doc: "OR gateway. Splits to every outgoing flow whose condition holds (default is fallback); \
              joins by waiting until no in-flight token could still reach it.",
        attrs: &[],
    },
    KindSpec {
        keyword: "eventBasedGateway",
        doc: "Deferred choice. Arms all downstream intermediate catch events at once; the first \
              to fire wins and the losing siblings are withdrawn.",
        attrs: &[],
    },
    KindSpec {
        keyword: "errorBoundaryEvent",
        doc: "Error boundary on an activity. Fires when a job throws the matching `errorCode`.",
        attrs: &[
            AttrSpec {
                key: "attachedTo",
                required: true,
                ty: AttrType::Id,
                doc: "Id of the activity this boundary is attached to.",
            },
            AttrSpec {
                key: "errorCode",
                required: true,
                ty: AttrType::Str,
                doc: "The BPMN error code this boundary catches.",
            },
        ],
    },
    KindSpec {
        keyword: "timerIntermediateCatchEvent",
        doc: "Timer catch. Rests until `duration` after activation elapses.",
        attrs: &[AttrSpec {
            key: "duration",
            required: true,
            ty: AttrType::Duration,
            doc: "Delay after activation before the timer becomes due.",
        }],
    },
    KindSpec {
        keyword: "timerBoundaryEvent",
        doc: "Timer boundary on an activity. Interrupting or non-interrupting; may repeat.",
        attrs: &[
            AttrSpec {
                key: "attachedTo",
                required: true,
                ty: AttrType::Id,
                doc: "Id of the activity this boundary is attached to.",
            },
            AttrSpec {
                key: "duration",
                required: true,
                ty: AttrType::Duration,
                doc: "Delay after activation before firing (also the period when `repeating`).",
            },
            AttrSpec {
                key: "interrupting",
                required: true,
                ty: AttrType::Bool,
                doc: "`true` cancels the activity on fire; `false` spawns a parallel token.",
            },
            AttrSpec {
                key: "repeating",
                required: true,
                ty: AttrType::Bool,
                doc: "`true` re-arms after firing (a `timeCycle`); only meaningful when \
                      non-interrupting.",
            },
        ],
    },
    KindSpec {
        keyword: "messageIntermediateCatchEvent",
        doc: "Message catch. Opens a subscription and rests until a correlated message arrives.",
        attrs: &[
            AttrSpec {
                key: "message",
                required: true,
                ty: AttrType::Str,
                doc: "The BPMN message name this event subscribes to.",
            },
            AttrSpec {
                key: "correlationKey",
                required: true,
                ty: AttrType::Str,
                doc: "Name of the instance variable whose value identifies the target instance.",
            },
        ],
    },
    KindSpec {
        keyword: "messageBoundaryEvent",
        doc: "Message boundary on an activity. Interrupting or non-interrupting.",
        attrs: &[
            AttrSpec {
                key: "attachedTo",
                required: true,
                ty: AttrType::Id,
                doc: "Id of the activity this boundary is attached to.",
            },
            AttrSpec {
                key: "message",
                required: true,
                ty: AttrType::Str,
                doc: "The BPMN message name this event subscribes to.",
            },
            AttrSpec {
                key: "correlationKey",
                required: true,
                ty: AttrType::Str,
                doc: "Name of the instance variable whose value identifies the target instance.",
            },
            AttrSpec {
                key: "interrupting",
                required: true,
                ty: AttrType::Bool,
                doc: "`true` cancels the activity on fire; `false` spawns a parallel token.",
            },
        ],
    },
    KindSpec {
        keyword: "messageStartEvent",
        doc: "Message start. Correlating a matching message creates a new instance.",
        attrs: &[AttrSpec {
            key: "message",
            required: true,
            ty: AttrType::Str,
            doc: "The BPMN message name that triggers a new instance.",
        }],
    },
    KindSpec {
        keyword: "timerStartEvent",
        doc: "Timer start. Creates a new instance when the process-level timer fires.",
        attrs: &[
            AttrSpec {
                key: "interval",
                required: true,
                ty: AttrType::Duration,
                doc: "Delay from deployment to first fire (also the period when `repeating`).",
            },
            AttrSpec {
                key: "repeating",
                required: true,
                ty: AttrType::Bool,
                doc: "`true` re-arms after firing (a cycle); `false` fires exactly once.",
            },
        ],
    },
    KindSpec {
        keyword: "subProcess",
        doc: "Embedded sub-process. Container holding its own flow; child elements set `parent` \
              to its id.",
        attrs: &[AttrSpec {
            key: "startEvent",
            required: true,
            ty: AttrType::Id,
            doc: "Id of the sub-process's inner (none) start event.",
        }],
    },
    KindSpec {
        keyword: "eventSubProcess",
        doc: "Event sub-process (`triggeredByEvent`). Never entered by token flow; activated \
              when its start event's trigger fires while the enclosing scope is active.",
        attrs: &[
            AttrSpec {
                key: "startEvent",
                required: true,
                ty: AttrType::Id,
                doc: "Id of the event sub-process's inner (typed) start event.",
            },
            AttrSpec {
                key: "trigger",
                required: true,
                ty: AttrType::Str,
                doc: "The start event's trigger, e.g. `message \"name\"`, `timer duration \"PT5M\"`, \
                      `signal \"name\"`, `error \"CODE\"` / `error` (catch-all), \
                      `escalation \"CODE\"` / `escalation`, `conditional \"=expr\"`.",
            },
            AttrSpec {
                key: "interrupting",
                required: true,
                ty: AttrType::Bool,
                doc: "`true` terminates the scope's other work on trigger (the default); \
                      `false` runs the event sub-process concurrently. Always `true` for an \
                      error trigger.",
            },
        ],
    },
    KindSpec {
        keyword: "escalationThrowEvent",
        doc: "Escalation throw (intermediate throw or end event). Raises `escalationCode` \
              up the enclosing scopes; non-critical, so the token always continues / drains \
              (an uncaught escalation is ignored).",
        attrs: &[AttrSpec {
            key: "escalationCode",
            required: false,
            ty: AttrType::Str,
            doc: "The BPMN escalation code raised (empty raises an unnamed escalation).",
        }],
    },
    KindSpec {
        keyword: "escalationBoundaryEvent",
        doc: "Escalation boundary on an activity. Catches an escalation raised inside it \
              (exact `escalationCode`, or empty for catch-all); interrupting or non-interrupting.",
        attrs: &[
            AttrSpec {
                key: "attachedTo",
                required: true,
                ty: AttrType::Id,
                doc: "Id of the activity this boundary is attached to.",
            },
            AttrSpec {
                key: "escalationCode",
                required: false,
                ty: AttrType::Str,
                doc: "The BPMN escalation code caught (empty is a catch-all).",
            },
            AttrSpec {
                key: "interrupting",
                required: true,
                ty: AttrType::Bool,
                doc: "`true` cancels the activity on fire; `false` spawns a parallel token.",
            },
        ],
    },
    KindSpec {
        keyword: "intermediateThrowEvent",
        doc: "None intermediate throw. Pure pass-through.",
        attrs: &[],
    },
    KindSpec {
        keyword: "linkIntermediateThrowEvent",
        doc: "Link throw. Has no outgoing flow; hands its token to the matching link catch \
              (same `link`) in the same scope.",
        attrs: &[AttrSpec {
            key: "link",
            required: true,
            ty: AttrType::Str,
            doc: "The `linkEventDefinition name` this throw hands its token to.",
        }],
    },
    KindSpec {
        keyword: "linkIntermediateCatchEvent",
        doc: "Link catch. Has no incoming flow; activated directly by the matching link throw \
              (same `link`), then a pass-through along its outgoing flow.",
        attrs: &[AttrSpec {
            key: "link",
            required: true,
            ty: AttrType::Str,
            doc: "The `linkEventDefinition name` a matching throw hands its token to.",
        }],
    },
    KindSpec {
        keyword: "task",
        doc: "Abstract task (also manualTask). No execution semantics — pure pass-through.",
        attrs: &[],
    },
    KindSpec {
        keyword: "scriptTask",
        doc: "Inline FEEL script. Evaluates `expression`, stores in `resultVariable`, completes.",
        attrs: &[
            AttrSpec {
                key: "expression",
                required: true,
                ty: AttrType::Str,
                doc: "Inline FEEL expression (leading `=` optional).",
            },
            AttrSpec {
                key: "resultVariable",
                required: true,
                ty: AttrType::Str,
                doc: "Variable name the expression result is stored under.",
            },
        ],
    },
    KindSpec {
        keyword: "callActivity",
        doc: "Invokes another process definition and waits for it to complete.",
        attrs: &[
            AttrSpec {
                key: "calledElement",
                required: true,
                ty: AttrType::Str,
                doc: "The `calledElement` / `zeebe:calledElement processId`.",
            },
            AttrSpec {
                key: "propagateAllParentVariables",
                required: true,
                ty: AttrType::Bool,
                doc: "`true` (Zeebe default) copies all visible parent variables into the child; \
                      `false` copies only the input-mapping results.",
            },
            AttrSpec {
                key: "propagateAllChildVariables",
                required: true,
                ty: AttrType::Bool,
                doc: "`true` (Zeebe default) merges the child's variables back into the parent \
                      (child wins); `false` crosses back only the output mappings.",
            },
        ],
    },
    KindSpec {
        keyword: "signalIntermediateCatchEvent",
        doc: "Signal catch. Correlates by name only.",
        attrs: &[AttrSpec {
            key: "signal",
            required: true,
            ty: AttrType::Str,
            doc: "The BPMN signal name this event subscribes to.",
        }],
    },
    KindSpec {
        keyword: "signalBoundaryEvent",
        doc: "Signal boundary on an activity. Interrupting or non-interrupting.",
        attrs: &[
            AttrSpec {
                key: "attachedTo",
                required: true,
                ty: AttrType::Id,
                doc: "Id of the activity this boundary is attached to.",
            },
            AttrSpec {
                key: "signal",
                required: true,
                ty: AttrType::Str,
                doc: "The BPMN signal name this event subscribes to.",
            },
            AttrSpec {
                key: "interrupting",
                required: true,
                ty: AttrType::Bool,
                doc: "`true` cancels the activity on fire; `false` spawns a parallel token.",
            },
        ],
    },
    KindSpec {
        keyword: "conditionalIntermediateCatchEvent",
        doc: "Conditional catch. Rests until a FEEL boolean becomes true.",
        attrs: &[AttrSpec {
            key: "condition",
            required: true,
            ty: AttrType::Str,
            doc: "FEEL boolean condition (leading `=` optional).",
        }],
    },
    KindSpec {
        keyword: "conditionalBoundaryEvent",
        doc: "Conditional boundary on an activity. Interrupting or non-interrupting.",
        attrs: &[
            AttrSpec {
                key: "attachedTo",
                required: true,
                ty: AttrType::Id,
                doc: "Id of the activity this boundary is attached to.",
            },
            AttrSpec {
                key: "condition",
                required: true,
                ty: AttrType::Str,
                doc: "FEEL boolean condition (leading `=` optional).",
            },
            AttrSpec {
                key: "interrupting",
                required: true,
                ty: AttrType::Bool,
                doc: "`true` cancels the activity on fire; `false` spawns a parallel token.",
            },
        ],
    },
    KindSpec {
        keyword: "compensationBoundaryEvent",
        doc: "Compensation boundary on an activity. Marks the activity compensable and \
              names the handler activity run to undo it.",
        attrs: &[
            AttrSpec {
                key: "attachedTo",
                required: true,
                ty: AttrType::Id,
                doc: "Id of the activity this boundary is attached to (made compensable).",
            },
            AttrSpec {
                key: "handler",
                required: true,
                ty: AttrType::Id,
                doc: "Id of the `isForCompensation` handler activity run to compensate it.",
            },
        ],
    },
    KindSpec {
        keyword: "compensationThrowEvent",
        doc: "Compensation throw. Triggers the compensation handlers of completed \
              compensable activities in scope (reverse completion order).",
        attrs: &[],
    },
];

/// Element-level attributes shared across every kind (they are emitted by
/// [`crate::model_ir::render_element_attrs`], not by [`crate::model_ir::render_kind_attrs`]).
/// All optional.
pub const SHARED_ATTRS: &[AttrSpec] = &[
    AttrSpec {
        key: "parent",
        required: false,
        ty: AttrType::Id,
        doc: "Id of the enclosing sub-process (for nested elements).",
    },
    AttrSpec {
        key: "retries",
        required: false,
        ty: AttrType::Str,
        doc: "Raw retries expression (literal or FEEL) for job-based tasks.",
    },
    // `timer`, `input`, `output`, `multiInstance` have their own shapes and are not simple
    // key/value pairs — they get bespoke productions in the GBNF and are described inline in
    // `describe`. Listed here so callers know they exist.
];

/// Sequence-flow annotations (post `->`). Both optional.
/// Flow-level annotations (`when` and `default`). Kept in table form for future use by
/// `emit_gbnf` (currently hard-coded there) and the describe payload — surfaced as a compact
/// reference for anyone extending the grammar without hunting for the inline literals.
#[cfg(test)]
#[allow(dead_code)]
pub const FLOW_ANNOTATIONS: &[AttrSpec] = &[
    AttrSpec {
        key: "when",
        required: false,
        ty: AttrType::Str,
        doc: "FEEL boolean condition guarding the flow.",
    },
    // `default` is a bare keyword, not a keyword+value pair, so it's not modelled as an AttrSpec.
    // The GBNF renders it as an optional literal (`"default"?`).
];

/// Look up the spec for an element-kind IR keyword.
pub fn spec_for_keyword(kw: &str) -> Option<&'static KindSpec> {
    ELEMENT_KIND_SPECS.iter().find(|s| s.keyword == kw)
}

/// Every known element-kind IR keyword, in table order.
pub fn keywords() -> Vec<&'static str> {
    ELEMENT_KIND_SPECS.iter().map(|s| s.keyword).collect()
}

// -------------------------------------------------------------------------------------------------
// GBNF emitter — llama.cpp `--grammar-file` format.
// -------------------------------------------------------------------------------------------------

/// Render the whole IR as a llama.cpp GBNF grammar. Layout mirrors what
/// [`crate::model_ir::definition_to_ir`] emits: nodes and flows are line-oriented, attribute
/// blocks are brace-wrapped one-per-line lists.
///
/// The output is a single self-contained grammar file — no external includes. Save it and load
/// via `llama-server --grammar-file` or POST as `grammar` on `/completion` to constrain the
/// sampler so the model literally cannot emit a token sequence that is not valid IR.
pub fn emit_gbnf() -> String {
    let mut out = String::new();
    out.push_str(GBNF_HEADER);

    // Root: `process "<id>" { start <id> <element>* <flow>* }`
    out.push_str(
        r#"root      ::= ws "process" sp string sp "{" nl indent "start" sp id nl elements flows "}" ws
elements  ::= element*
flows     ::= flow*
"#,
    );

    // Every element is one of the per-kind productions.
    out.push_str("element   ::= ");
    let mut first = true;
    for s in ELEMENT_KIND_SPECS {
        if !first {
            out.push_str(" | ");
        }
        out.push_str(&format!("elem-{}", s.keyword));
        first = false;
    }
    out.push('\n');

    // Per-kind productions.
    for s in ELEMENT_KIND_SPECS {
        out.push('\n');
        out.push_str(&format!("# {} — {}\n", s.keyword, s.doc));
        out.push_str(&format!(
            "elem-{kw}  ::= indent \"{kw}\" sp id (sp string)? ",
            kw = s.keyword,
        ));
        if s.attrs.is_empty() && SHARED_ATTRS.is_empty() {
            // Never happens today (SHARED_ATTRS is non-empty), but future-proofs the emitter for
            // a purely-attributeless variant.
            out.push_str("nl\n");
        } else {
            out.push_str(&format!("(sp attrs-{})? nl\n", s.keyword));
            emit_attr_block(&mut out, s);
        }
    }

    // Attribute-value primitives (one production per AttrType, so per-kind attrs can share).
    out.push('\n');
    out.push_str(
        r#"# --- Attribute value primitives ---
attr-str      ::= string
attr-id       ::= id
attr-duration ::= [0-9]+ "ms"
attr-bool     ::= "true" | "false"
"#,
    );

    // Shared element-level attributes (parent, retries, timer, input, output, multiInstance).
    out.push('\n');
    out.push_str(EXTRAS_BLOCK);

    // Flow production.
    out.push('\n');
    out.push_str(
        r#"# --- Sequence flow ---
flow      ::= indent id sp "->" sp id (sp "when" sp string)? (sp "default")? nl
"#,
    );

    // Lexical primitives.
    out.push('\n');
    out.push_str(LEXICAL_BLOCK);

    out
}

/// Emit `attrs-<keyword> ::= "{" nl (indent2 attr nl)+ indent "}"` and the kind's attr alternation.
///
/// Semantics we want the sampler to enforce:
/// - Each required attr must appear at least once.
/// - Each optional attr may appear at most once.
/// - Shared element-level attrs may each appear at most once, at any position.
///
/// GBNF is context-free, so exact multiset ordering is over-approximated: we allow any permutation
/// of the declared attrs (via a permissive `attr-line-<kw> ::= <kind-attr> | <shared-attr>`
/// alternation), and rely on the parser to enforce single-occurrence and required-presence. This
/// is a deliberate trade-off — a strict GBNF for permutations of k attrs is O(k!) productions;
/// the parser is O(n) and gives better error messages.
fn emit_attr_block(out: &mut String, s: &KindSpec) {
    out.push_str(&format!(
        "attrs-{kw}   ::= \"{{\" nl (indent2 attr-line-{kw} nl)+ indent \"}}\"\n",
        kw = s.keyword,
    ));

    // Per-kind attr alternation: this kind's attrs plus every shared/element-level extra.
    out.push_str(&format!("attr-line-{}  ::= ", s.keyword));
    let mut alts: Vec<String> = Vec::new();
    for a in s.attrs {
        alts.push(format!("attr-{}-{}", s.keyword, a.key));
    }
    // Shared extras (element-level annotations from render_element_attrs).
    alts.push("attr-parent".into());
    alts.push("attr-retries".into());
    alts.push("attr-timer".into());
    alts.push("attr-input".into());
    alts.push("attr-output".into());
    alts.push("attr-multiInstance".into());
    out.push_str(&alts.join(" | "));
    out.push('\n');

    // Individual attr productions for this kind.
    for a in s.attrs {
        out.push_str(&format!(
            "attr-{kw}-{key}  ::= \"{key}\" sp attr-{ty}   # {doc}\n",
            kw = s.keyword,
            key = a.key,
            ty = attr_type_rule(a.ty),
            doc = a.doc,
        ));
    }
}

fn attr_type_rule(t: AttrType) -> &'static str {
    match t {
        AttrType::Str => "str",
        AttrType::Id => "id",
        AttrType::Duration => "duration",
        AttrType::Bool => "bool",
    }
}

const GBNF_HEADER: &str = r#"# Reversible semantic IR grammar (ADR 0001).
# Generated by processos::ir_spec::emit_gbnf — DO NOT EDIT by hand.
# Add or remove element kinds by editing ELEMENT_KIND_SPECS in ir_spec.rs;
# the parity test in that module fails if the spec drifts from the engine's
# ElementKind enum.
#
# Load into llama.cpp: `llama-server --grammar-file <this.gbnf>`
#                  or: POST /completion with `{"grammar": "<file contents>"}`.
# GBNF enforces FORM (well-typed IR); the parser (crate::model_ir::ir_to_definition)
# still validates cross-element references and required-attr presence.

"#;

const EXTRAS_BLOCK: &str = r#"# --- Shared element-level extras (all optional) ---
attr-parent          ::= "parent" sp id
attr-retries         ::= "retries" sp string
attr-timer           ::= "timer" sp ("duration" | "cycle" | "date") sp string
attr-input           ::= "input" sp id sp "<-" sp string
attr-output          ::= "output" sp id sp "<-" sp string
attr-multiInstance   ::= "multiInstance" sp "{" sp mi-attr (sp "," sp mi-attr)* sp "}"
mi-attr              ::= "collection" sp string
                       | "inputElement" sp string
                       | "outputCollection" sp string
                       | "outputElement" sp string
                       | "completionCondition" sp string
                       | "cancelRemainingInstances" sp attr-bool
                       | "sequential" sp attr-bool
"#;

const LEXICAL_BLOCK: &str = r#"# --- Lexical primitives ---
id        ::= [A-Za-z_] [A-Za-z0-9_]*
string    ::= "\"" ( [^"\\\n] | "\\" ["\\/bfnrt] )* "\""
sp        ::= [ \t]+
ws        ::= [ \t\r\n]*
nl        ::= [ \t]* "\n"
indent    ::= "  "
indent2   ::= "    "
"#;

// -------------------------------------------------------------------------------------------------
// Human/tool description — the `describe_ir_grammar` tool payload.
// -------------------------------------------------------------------------------------------------

/// The `describe_ir_grammar(kind?)` tool payload — a scoped grammar reference for the LLM.
///
/// - `kind: None` → the compact one-page overview: every keyword, top-level syntax skeleton,
///   flow annotations, and the shared element extras.
/// - `kind: Some("exclusiveGateway")` → just that kind's productions plus the flow annotations
///   and shared extras that apply. Small enough to send per turn.
pub fn describe(kind: Option<&str>) -> Value {
    match kind {
        None => json!({
            "scope": "overview",
            "syntax": {
                "process": "process \"<id>\" { start <id>  <element>*  <flow>* }",
                "element": "<keyword> <id> [\"<name>\"] [{ <attr>* }]",
                "flow":    "<from> -> <to> [when \"<feel>\"] [default]",
            },
            "elementKinds": ELEMENT_KIND_SPECS.iter().map(|s| json!({
                "keyword": s.keyword,
                "doc": s.doc,
                "attrs": s.attrs.iter().map(attr_json).collect::<Vec<_>>(),
            })).collect::<Vec<_>>(),
            "sharedElementAttrs": [
                { "key": "parent",       "doc": "Id of the enclosing sub-process." },
                { "key": "retries",      "doc": "Raw retries expression (literal or FEEL)." },
                { "key": "timer",        "doc": "`timer duration|cycle|date \"<feel>\"` on a timer event." },
                { "key": "input",        "doc": "`input <target-id> <- \"<feel-source>\"` — repeat for each mapping." },
                { "key": "output",       "doc": "`output <target-id> <- \"<feel-source>\"` — repeat for each mapping." },
                { "key": "multiInstance", "doc": "`multiInstance { collection \"<feel>\", ... }` on the activity." },
            ],
            "flowAnnotations": [
                { "key": "when",    "doc": "FEEL boolean guarding the flow." },
                { "key": "default", "doc": "Bare keyword marking a condition-routed gateway's fallback flow (an exclusive or inclusive gateway)." },
            ],
            "note": "Call describe_ir_grammar(kind: \"<keyword>\") for a scoped reference for one element kind.",
        }),
        Some(k) => match spec_for_keyword(k) {
            None => json!({
                "error": format!("unknown element kind `{k}`"),
                "knownKinds": keywords(),
            }),
            Some(s) => json!({
                "scope": "kind",
                "keyword": s.keyword,
                "doc": s.doc,
                "syntax": format!(
                    "{kw} <id> [\"<name>\"] [{{ <attr>* }}]",
                    kw = s.keyword,
                ),
                "attrs": s.attrs.iter().map(attr_json).collect::<Vec<_>>(),
                "flowAnnotations": [
                    { "key": "when",    "doc": "FEEL boolean guarding an outgoing flow." },
                    { "key": "default", "doc": "Bare keyword; meaningful on a condition-routed gateway's outgoing flow (an exclusiveGateway or inclusiveGateway)." },
                ],
                "sharedAttrs": ["parent", "retries", "timer", "input", "output", "multiInstance"],
                "note": "All attributes go inside a `{ ... }` block, one per line. \
                         Required attributes must be present; optional attributes may be omitted.",
            }),
        },
    }
}

fn attr_json(a: &AttrSpec) -> Value {
    json!({
        "key": a.key,
        "required": a.required,
        "type": attr_type_label(a.ty),
        "doc": a.doc,
    })
}

fn attr_type_label(t: AttrType) -> &'static str {
    match t {
        AttrType::Str => "string",
        AttrType::Id => "id",
        AttrType::Duration => "duration (Nms)",
        AttrType::Bool => "bool",
    }
}

/// The checked-in grammar file — the on-disk artifact that `emit_gbnf()` produced at build time
/// of the last committed source. Consumers that want to hand a `--grammar-file` to `llama-server`
/// or inline the grammar into a request body should use this rather than re-running
/// [`emit_gbnf`] every call — it's a `&'static str` baked into the binary at compile time.
///
/// A drift test ([`tests::checked_in_grammar_matches_emitter`]) fails CI if this file falls
/// behind the emitter. The fix is `processos emit-gbnf --out processos/assets/ir.gbnf`.
pub fn ir_gbnf() -> &'static str {
    include_str!("../assets/ir.gbnf")
}

// -------------------------------------------------------------------------------------------------
// Parity harness — exhaustive sample instances of every ElementKind for the parity test and for
// programmatic consumers (e.g. golden-fixture generators). Adding a new variant → the exhaustive
// match here fails to compile; forgetting to update ELEMENT_KIND_SPECS → the parity test fails.
// -------------------------------------------------------------------------------------------------

/// **Exhaustive compile-time witness that every `ElementKind` variant is represented in this
/// module.**
///
/// The purpose of this function is *not* to be called with useful data — it's to be a `match`
/// over `&ElementKind` that the compiler must prove exhaustive. When someone adds a variant to
/// `nanobpmn_engine_core::ElementKind`, this match fails to compile until they add an arm here
/// — and the compiler drops them directly into `ir_spec.rs`, right next to
/// [`ELEMENT_KIND_SPECS`], [`sample_instances`], and the parity test.
///
/// The doc-comment at the arm site is the checklist for a new variant:
///
/// 1. Add the arm below returning the IR keyword.
/// 2. Add a `KindSpec { keyword, doc, attrs }` entry to [`ELEMENT_KIND_SPECS`].
/// 3. Add a placeholder to [`sample_instances`] tagged with the same keyword.
/// 4. Handle the variant in the three exhaustive matches in `model_ir.rs`
///    (`kind_keyword`, `render_kind_attrs`, `build_kind`) — the Rust compiler forces this
///    independently in that file.
///
/// If (2) or (3) is skipped, [`tests::specs_match_pretty_printer`] fails on a keyword-set
/// mismatch. If (4) is skipped, the crate does not compile.
#[cfg(test)]
fn variant_witness(k: &nanobpmn_engine_core::ElementKind) -> &'static str {
    use nanobpmn_engine_core::ElementKind::*;
    // NEW VARIANT? Read the doc-comment above. Add an arm here returning the IR keyword,
    // then update ELEMENT_KIND_SPECS + sample_instances (below) to match.
    match k {
        StartEvent => "startEvent",
        EndEvent => "endEvent",
        TerminateEndEvent => "terminateEndEvent",
        ServiceTask { .. } => "serviceTask",
        BusinessRuleTask { .. } => "businessRuleTask",
        UserTask(_) => "userTask",
        ExclusiveGateway => "exclusiveGateway",
        ParallelGateway => "parallelGateway",
        InclusiveGateway => "inclusiveGateway",
        EventBasedGateway => "eventBasedGateway",
        ErrorBoundaryEvent { .. } => "errorBoundaryEvent",
        TimerIntermediateCatchEvent { .. } => "timerIntermediateCatchEvent",
        TimerBoundaryEvent { .. } => "timerBoundaryEvent",
        MessageIntermediateCatchEvent { .. } => "messageIntermediateCatchEvent",
        MessageBoundaryEvent { .. } => "messageBoundaryEvent",
        MessageStartEvent { .. } => "messageStartEvent",
        TimerStartEvent { .. } => "timerStartEvent",
        SubProcess { .. } => "subProcess",
        EventSubProcess { .. } => "eventSubProcess",
        IntermediateThrowEvent => "intermediateThrowEvent",
        EscalationThrowEvent { .. } => "escalationThrowEvent",
        EscalationBoundaryEvent { .. } => "escalationBoundaryEvent",
        LinkIntermediateThrowEvent { .. } => "linkIntermediateThrowEvent",
        LinkIntermediateCatchEvent { .. } => "linkIntermediateCatchEvent",
        Task => "task",
        ScriptTask { .. } => "scriptTask",
        CallActivity { .. } => "callActivity",
        SignalIntermediateCatchEvent { .. } => "signalIntermediateCatchEvent",
        SignalBoundaryEvent { .. } => "signalBoundaryEvent",
        ConditionalIntermediateCatchEvent { .. } => "conditionalIntermediateCatchEvent",
        ConditionalBoundaryEvent { .. } => "conditionalBoundaryEvent",
        CompensationBoundaryEvent { .. } => "compensationBoundaryEvent",
        CompensationThrowEvent => "compensationThrowEvent",
        AgentTask { .. } => "serviceTask",
    }
}

/// One dummy instance per canonical `ElementKind` variant, tagged with the IR keyword we expect the
/// pretty-printer to render for it. Field values are placeholders — enough to survive round-trip
/// and produce every declared attribute line. Historical `AgentTask` values normalize to ServiceTask.
#[cfg(test)]
pub fn sample_instances() -> Vec<(&'static str, nanobpmn_engine_core::ElementKind)> {
    use nanobpmn_engine_core::{ElementKind, UserTaskProps};
    vec![
        ("startEvent", ElementKind::StartEvent),
        ("endEvent", ElementKind::EndEvent),
        ("terminateEndEvent", ElementKind::TerminateEndEvent),
        (
            "serviceTask",
            ElementKind::ServiceTask {
                agent_type: Some(nanobpmn_engine_core::AgentType::External),
                job_type: "worker".into(),
                priority: Some("50".into()),
                custom_headers: std::collections::BTreeMap::new(),
                linked_resources: Vec::new(),
            },
        ),
        (
            "businessRuleTask",
            ElementKind::BusinessRuleTask {
                decision_id: "greeting".into(),
                result_variable: Some("greeting".into()),
            },
        ),
        (
            "userTask",
            ElementKind::UserTask(UserTaskProps {
                assignee: Some("alice".into()),
                candidate_groups: Some("reviewers".into()),
                candidate_users: Some("bob,carol".into()),
                due_date: Some("=today() + duration(\"P1D\")".into()),
                follow_up_date: Some("=today()".into()),
                priority: Some("50".into()),
                form_id: Some("feature-escalation".into()),
                external_form_reference: Some("https://forms.example/x".into()),
            }),
        ),
        ("exclusiveGateway", ElementKind::ExclusiveGateway),
        ("parallelGateway", ElementKind::ParallelGateway),
        ("inclusiveGateway", ElementKind::InclusiveGateway),
        ("eventBasedGateway", ElementKind::EventBasedGateway),
        (
            "errorBoundaryEvent",
            ElementKind::ErrorBoundaryEvent {
                attached_to: "svc_1".into(),
                error_code: "OUT_OF_STOCK".into(),
            },
        ),
        (
            "timerIntermediateCatchEvent",
            ElementKind::TimerIntermediateCatchEvent {
                duration_millis: 60_000,
            },
        ),
        (
            "timerBoundaryEvent",
            ElementKind::TimerBoundaryEvent {
                attached_to: "svc_1".into(),
                duration_millis: 60_000,
                interrupting: true,
                repeating: false,
            },
        ),
        (
            "messageIntermediateCatchEvent",
            ElementKind::MessageIntermediateCatchEvent {
                message_name: "order_ready".into(),
                correlation_key: "orderId".into(),
            },
        ),
        (
            "messageBoundaryEvent",
            ElementKind::MessageBoundaryEvent {
                attached_to: "svc_1".into(),
                message_name: "cancel".into(),
                correlation_key: "orderId".into(),
                interrupting: true,
            },
        ),
        (
            "messageStartEvent",
            ElementKind::MessageStartEvent {
                message_name: "order_placed".into(),
            },
        ),
        (
            "timerStartEvent",
            ElementKind::TimerStartEvent {
                interval_millis: 3_600_000,
                repeating: true,
            },
        ),
        (
            "subProcess",
            ElementKind::SubProcess {
                start_event: "inner_start".into(),
            },
        ),
        (
            "eventSubProcess",
            ElementKind::EventSubProcess {
                start_event: "esp_start".into(),
                trigger: nanobpmn_engine_core::EventSubProcessTrigger::Message {
                    message_name: "orderMsg".into(),
                    correlation_key: "orderId".into(),
                },
                interrupting: true,
            },
        ),
        (
            "intermediateThrowEvent",
            ElementKind::IntermediateThrowEvent,
        ),
        (
            "escalationThrowEvent",
            ElementKind::EscalationThrowEvent {
                escalation_code: "OVERLOAD".into(),
            },
        ),
        (
            "escalationBoundaryEvent",
            ElementKind::EscalationBoundaryEvent {
                attached_to: "sub_1".into(),
                escalation_code: "OVERLOAD".into(),
                interrupting: true,
            },
        ),
        (
            "linkIntermediateThrowEvent",
            ElementKind::LinkIntermediateThrowEvent {
                link_name: "hop".into(),
            },
        ),
        (
            "linkIntermediateCatchEvent",
            ElementKind::LinkIntermediateCatchEvent {
                link_name: "hop".into(),
            },
        ),
        ("task", ElementKind::Task),
        (
            "scriptTask",
            ElementKind::ScriptTask {
                expression: "=total * 1.1".into(),
                result_variable: "totalWithTax".into(),
            },
        ),
        (
            "callActivity",
            ElementKind::CallActivity {
                called_process_id: "phase_review".into(),
                propagate_all_parent_variables: true,
                propagate_all_child_variables: true,
            },
        ),
        (
            "signalIntermediateCatchEvent",
            ElementKind::SignalIntermediateCatchEvent {
                signal_name: "shutdown".into(),
            },
        ),
        (
            "signalBoundaryEvent",
            ElementKind::SignalBoundaryEvent {
                attached_to: "svc_1".into(),
                signal_name: "shutdown".into(),
                interrupting: true,
            },
        ),
        (
            "conditionalIntermediateCatchEvent",
            ElementKind::ConditionalIntermediateCatchEvent {
                condition: "=orderApproved".into(),
            },
        ),
        (
            "conditionalBoundaryEvent",
            ElementKind::ConditionalBoundaryEvent {
                attached_to: "svc_1".into(),
                condition: "=orderCancelled".into(),
                interrupting: true,
            },
        ),
        (
            "compensationBoundaryEvent",
            ElementKind::CompensationBoundaryEvent {
                attached_to: "svc_1".into(),
                handler: "undo_1".into(),
            },
        ),
        (
            "compensationThrowEvent",
            ElementKind::CompensationThrowEvent,
        ),
    ]
}

/// Placeholder instances of the shared, element-level extras — exercised by the parity test to
/// keep the `SHARED_ATTRS` list honest against
/// [`crate::model_ir::render_element_attrs`].
#[allow(dead_code)]
#[cfg(test)]
pub fn sample_shared_extras() -> (
    Option<String>,
    Option<String>,
    Option<nanobpmn_engine_core::TimerDef>,
    nanobpmn_engine_core::IoMapping,
    Option<nanobpmn_engine_core::MultiInstance>,
) {
    use nanobpmn_engine_core::{IoMapping, Mapping, MultiInstance, TimerDef, TimerDefKind};
    (
        Some("parent_sp".into()),
        Some("3".into()),
        Some(TimerDef {
            kind: TimerDefKind::Duration,
            expr: "PT1M".into(),
        }),
        IoMapping {
            inputs: vec![Mapping {
                source: "=order.total".into(),
                target: "total".into(),
            }],
            outputs: vec![Mapping {
                source: "=result.status".into(),
                target: "status".into(),
            }],
        },
        Some(MultiInstance {
            input_collection: "=items".into(),
            input_element: Some("item".into()),
            output_collection: Some("results".into()),
            output_element: Some("=result".into()),
            completion_condition: Some("=count(results) >= 5".into()),
            cancel_remaining_instances: true,
            sequential: false,
        }),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use nanobpmn_engine_core::Element;

    use super::*;
    use crate::model_ir::{
        // Re-exported via pub(crate) so the parity test can drive the private renderers without
        // going through a synthetic ProcessDefinition. Keeps the test's failure mode narrow —
        // it exercises exactly the same code paths the pretty-printer uses.
        render_element_attrs_for_test,
        render_kind_attrs_for_test,
    };

    /// For every declared spec, build the matching sample instance, run the real pretty-printer,
    /// and assert the emitted attribute keys match what the spec table declares. Any drift
    /// (spec has an attr the printer doesn't emit, or vice versa) fails this test with a diff.
    #[test]
    fn specs_match_pretty_printer() {
        let samples = sample_instances();
        // Every sample's tag must match its ElementKind's actual IR keyword — protects against
        // the sample table being copy-pasted with the wrong tag. Cross-checked two ways:
        //   * `kind_keyword` (the pretty-printer's own function) — what the printer will emit.
        //   * `variant_witness` (this module's exhaustive match) — what this module asserts as
        //     the canonical keyword. Divergence here means someone changed one without the other.
        for (expected_kw, kind) in &samples {
            let printed = crate::model_ir::kind_keyword_for_test(kind);
            let witnessed = variant_witness(kind);
            assert_eq!(
                printed, *expected_kw,
                "sample_instances tag `{expected_kw}` does not match kind_keyword(`{printed}`)",
            );
            assert_eq!(
                witnessed, *expected_kw,
                "variant_witness disagrees with sample_instances tag `{expected_kw}`",
            );
        }

        // Every ElementKind variant appears in samples.
        let sample_kws: BTreeSet<&str> = samples.iter().map(|(k, _)| *k).collect();
        let spec_kws: BTreeSet<&str> = ELEMENT_KIND_SPECS.iter().map(|s| s.keyword).collect();
        assert_eq!(
            sample_kws, spec_kws,
            "sample_instances and ELEMENT_KIND_SPECS must cover the same keywords",
        );

        // Attribute-key parity.
        for (kw, kind) in &samples {
            let spec = spec_for_keyword(kw).unwrap_or_else(|| panic!("no spec for `{kw}`"));
            let mut emitted = Vec::new();
            render_kind_attrs_for_test(kind, &mut emitted);
            // The first whitespace-separated token of each line is the attr key.
            let emitted_keys: BTreeSet<String> = emitted
                .iter()
                .filter_map(|line| line.split_whitespace().next().map(str::to_string))
                .collect();
            let spec_keys: BTreeSet<String> =
                spec.attrs.iter().map(|a| a.key.to_string()).collect();
            assert_eq!(
                emitted_keys, spec_keys,
                "keys for `{kw}` — spec vs pretty-printer diverged",
            );
        }
    }

    /// Every keyword in SHARED_ATTRS is emitted by render_element_attrs when the corresponding
    /// field is present. Keeps the SHARED_ATTRS listing honest against the printer.
    #[test]
    fn shared_attrs_match_element_renderer() {
        use nanobpmn_engine_core::ElementKind;
        let (parent, retries, timer, io, mi) = sample_shared_extras();
        let el = Element {
            id: "svc_1".into(),
            kind: ElementKind::ServiceTask {
                agent_type: None,
                job_type: "worker".into(),
                priority: None,
                custom_headers: std::collections::BTreeMap::new(),
                linked_resources: Vec::new(),
            },
            name: None,
            outgoing: Vec::new(),
            parent,
            retries,
            timer,
            io,
            multi_instance: mi,
            start_listeners: Vec::new(),
            end_listeners: Vec::new(),
            task_listeners: Vec::new(),
        };
        let mut emitted = Vec::new();
        render_element_attrs_for_test(&el, &mut emitted);
        let emitted_keys: BTreeSet<String> = emitted
            .iter()
            .filter_map(|line| line.split_whitespace().next().map(str::to_string))
            .collect();
        // The SHARED_ATTRS list only holds the simple K/V shared attrs (parent, retries). The
        // shaped ones (timer, input, output, multiInstance) are described inline in `describe`
        // and get their own GBNF productions; assert they appear too by string-match.
        for key in [
            "parent",
            "retries",
            "timer",
            "input",
            "output",
            "multiInstance",
        ] {
            assert!(
                emitted_keys.contains(key),
                "render_element_attrs did not emit `{key}`; check that SHARED_ATTRS / EXTRAS_BLOCK still match",
            );
        }
    }

    /// The generated GBNF contains a per-kind alternation covering every element-kind keyword.
    /// Catches trivial regressions in `emit_gbnf` without shelling out to llama.cpp's parser.
    #[test]
    fn gbnf_covers_every_kind() {
        let g = emit_gbnf();
        for s in ELEMENT_KIND_SPECS {
            assert!(
                g.contains(&format!("elem-{}", s.keyword)),
                "GBNF missing production for `{}`",
                s.keyword,
            );
            assert!(
                g.contains(&format!("\"{}\"", s.keyword)),
                "GBNF missing keyword literal `\"{}\"`",
                s.keyword,
            );
        }
        // Structural spot-checks.
        for needle in [
            "root",
            "element",
            "flow",
            "attr-str",
            "attr-id",
            "attr-duration",
            "attr-bool",
        ] {
            assert!(g.contains(needle), "GBNF missing rule `{needle}`");
        }
    }

    /// `describe(None)` returns the full catalog; `describe(Some(kw))` returns the scoped payload.
    #[test]
    fn describe_returns_scoped_payloads() {
        let overview = describe(None);
        assert_eq!(overview["scope"], "overview");
        let kinds = overview["elementKinds"].as_array().unwrap();
        assert_eq!(kinds.len(), ELEMENT_KIND_SPECS.len());

        let ex = describe(Some("exclusiveGateway"));
        assert_eq!(ex["scope"], "kind");
        assert_eq!(ex["keyword"], "exclusiveGateway");

        let bogus = describe(Some("no_such_kind"));
        assert!(bogus.get("error").is_some());
    }

    /// The checked-in `processos/assets/ir.gbnf` MUST equal what `emit_gbnf()` produces from the
    /// current source — otherwise runtime consumers (`ir_gbnf()`, hosted-model callers) would
    /// hand out a stale grammar that no longer matches the engine surface.
    ///
    /// If this fails in CI, regenerate:
    ///
    /// ```sh
    /// cargo run --bin processos -- emit-gbnf --out processos/assets/ir.gbnf
    /// ```
    ///
    /// and commit the result.
    #[test]
    fn checked_in_grammar_matches_emitter() {
        let generated = emit_gbnf();
        let checked_in = ir_gbnf();
        assert_eq!(
            generated, checked_in,
            "processos/assets/ir.gbnf is out of date — run `cargo run --bin processos -- \
             emit-gbnf --out processos/assets/ir.gbnf` and commit the regenerated file.",
        );
    }

    /// The Lean formal round-trip proof (`formal/lean/Roundtrip/`, issue #1232) models the
    /// supported element-kind set and proves the IR ⇄ BPMN conversion is identity on it. That
    /// model is only trustworthy if its element registry is the SAME registry the engine actually
    /// speaks — otherwise the proof is about a fiction. `formal/lean/Roundtrip/Registry.lean`
    /// mirrors [`ELEMENT_KIND_SPECS`] as plain Lean data; this test is the drift guard that pins
    /// the two together, keyword-for-keyword and attribute-for-attribute (key, `required`, type).
    ///
    /// It runs in the already-gated `processos (clippy + test)` job, so a change to the Rust
    /// registry that forgets the Lean mirror fails here; the Lean side has its own `rfl` guards
    /// (`keywords_match_registry`, `specs_match_registry`) that fail `lake build` if the mirror
    /// and the executable Lean model diverge. If this fails, update
    /// `formal/lean/Roundtrip/Registry.lean` to match `ELEMENT_KIND_SPECS`.
    #[test]
    fn lean_registry_matches_specs() {
        // Embedded at compile time — no runtime path/CWD dependency. Relative to this source file
        // (`processos/src/ir_spec.rs`): `../../` is the repository root.
        const LEAN_REGISTRY: &str = include_str!("../../formal/lean/Roundtrip/Registry.lean");

        fn attr_ty_lean(ty: AttrType) -> &'static str {
            match ty {
                AttrType::Str => "str",
                AttrType::Id => "id",
                AttrType::Duration => "duration",
                AttrType::Bool => "bool",
            }
        }

        // What the Rust source-of-truth declares.
        let expected: Vec<LeanKindGroup> = ELEMENT_KIND_SPECS
            .iter()
            .map(|s| {
                let attrs = s
                    .attrs
                    .iter()
                    .map(|a| {
                        (
                            a.key.to_string(),
                            a.required,
                            attr_ty_lean(a.ty).to_string(),
                        )
                    })
                    .collect();
                (s.keyword.to_string(), attrs)
            })
            .collect();

        let actual = parse_lean_registry(LEAN_REGISTRY);

        assert_eq!(
            actual, expected,
            "formal/lean/Roundtrip/Registry.lean has drifted from ELEMENT_KIND_SPECS — update the \
             Lean mirror to match (keyword, and each attr's key/required/type, in order).",
        );
    }

    /// Read the trailing quoted string literal that follows position `from` in `src`.
    /// Returns the unescaped-enough content (the registry uses no escapes) and the index just
    /// past the closing quote.
    fn read_quoted(src: &str, from: usize) -> (String, usize) {
        let open = from + src[from..].find('"').expect("opening quote");
        let rest = &src[open + 1..];
        let len = rest.find('"').expect("closing quote");
        (rest[..len].to_string(), open + 1 + len)
    }

    /// Read the bare identifier that follows the `.` after position `from` (`ty := .str`).
    fn read_dotted_ident(src: &str, from: usize) -> String {
        let dot = from + src[from..].find('.').expect("dotted ctor");
        let rest = &src[dot + 1..];
        let end = rest
            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
            .unwrap_or(rest.len());
        rest[..end].to_string()
    }

    /// Read the `true`/`false` literal that follows position `from`.
    fn read_bool(src: &str, from: usize) -> bool {
        let rest = &src[from..];
        let t = rest.find("true").map(|i| (i, true));
        let f = rest.find("false").map(|i| (i, false));
        match (t, f) {
            (Some((it, vt)), Some((ifa, vf))) => {
                if it <= ifa {
                    vt
                } else {
                    vf
                }
            }
            (Some((_, v)), None) | (None, Some((_, v))) => v,
            (None, None) => panic!("expected a bool literal"),
        }
    }

    /// One element kind projected to `(keyword, [(attr-key, required, type)])` — the shape both
    /// `ELEMENT_KIND_SPECS` and the Lean `registry` mirror flatten to for the drift comparison.
    type LeanKindGroup = (String, Vec<(String, bool, String)>);

    /// Parse `formal/lean/Roundtrip/Registry.lean`'s `registry` definition into the same shape
    /// [`ELEMENT_KIND_SPECS`] projects to: an ordered list of `(keyword, [(key, required, ty)])`.
    /// A tiny document-order scanner — the file is machine-authored plain data, so a full Lean
    /// parser is overkill; a `keyword :=` opens a new group and each following `key :=` (with its
    /// `required :=`/`ty := .`) appends an attribute to it.
    fn parse_lean_registry(src: &str) -> Vec<LeanKindGroup> {
        let start = src
            .find("def registry : List KindSpec :=")
            .expect("`def registry` not found in Registry.lean");
        let after = &src[start..];
        let end = after.find("\ndef registryKeywords").unwrap_or(after.len());
        let body = &after[..end];

        enum Event {
            Keyword(String),
            Attr(String, bool, String),
        }
        let mut events: Vec<(usize, Event)> = Vec::new();

        for (pos, _) in body.match_indices("keyword :=") {
            let (kw, _) = read_quoted(body, pos);
            events.push((pos, Event::Keyword(kw)));
        }
        for (pos, _) in body.match_indices("key :=") {
            // Skip the `keyword :=` matches, which also contain `key :=` — but "keyword :=" is a
            // distinct string not matched by "key :=" (it is "keyword :="), so match_indices on
            // "key :=" only hits attribute keys. Still, guard defensively.
            let (key, after_key) = read_quoted(body, pos);
            let required = read_bool(body, after_key);
            let ty = read_dotted_ident(body, after_key);
            events.push((pos, Event::Attr(key, required, ty)));
        }
        events.sort_by_key(|(pos, _)| *pos);

        let mut groups: Vec<LeanKindGroup> = Vec::new();
        for (_, ev) in events {
            match ev {
                Event::Keyword(kw) => groups.push((kw, Vec::new())),
                Event::Attr(k, r, t) => {
                    groups
                        .last_mut()
                        .expect("attribute before any keyword")
                        .1
                        .push((k, r, t));
                }
            }
        }
        groups
    }
}
