//! Model scan for data-envelope worker & message I/O (ADR 0040 slices 1–2 /
//! ADR 0033 §6 increment 12).
//!
//! The BPMN model is the authoritative source of the data envelope
//! (`io.nanobpm.dataEnvelope.in`/`.out`, carried as a `zeebe:property`, written by
//! the modeler's *Data envelope* panel): on a **service task** it types the
//! worker's job I/O, and on a shared **`bpmn:message`** it types the message's
//! payload. The typed-worker codegen historically read the worker I/O map from the
//! manifest's hand-maintained `workers[]` projection, so the two could drift; the
//! message payload was not consumed by codegen at all.
//!
//! This scan closes both gaps: it parses the project's process models and derives
//! `taskType -> {in,out}` (slice 1) and `messageName -> {in,out}` (slice 2)
//! **from the model**, so the reifier rebuilds the worker-IO map and the typed
//! `publishMessage` registry from the source of truth. It is the model-scan half
//! of the Fused Domain Model pipeline (ADR 0040 §7/§8): the models contribute
//! their motion-shape bindings, and the derived registries are recomputed from
//! them.
//!
//! The scan is intentionally Urban-specific and lives in the console, not in
//! `engine-core`: the envelope is an Urban concept and the engine stays
//! Zeebe-pure. It reuses only the engine's generic XML tokenizer
//! ([`nanobpmn_engine_core::xml`]).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use nanobpmn_engine_core::xml::{Token, attr, local_name, tokenize};
use serde::Serialize;

pub mod json_schema;

/// Reserved `zeebe:property` name carrying the input data-envelope type ref.
/// Must match `console/src/lib/dataEnvelope.ts` `ENVELOPE_KEY.inputType`.
const ENVELOPE_IN: &str = "io.nanobpm.dataEnvelope.in";
/// Reserved `zeebe:property` name carrying the output data-envelope type ref.
/// Must match `console/src/lib/dataEnvelope.ts` `ENVELOPE_KEY.outputType`.
const ENVELOPE_OUT: &str = "io.nanobpm.dataEnvelope.out";

/// The service-ish task local names whose `zeebe:taskDefinition:type` keys a
/// `workers[]` entry. Mirrors `SERVICE_TASK_TYPES` in `dataEnvelope.ts`.
const SERVICE_TASK_LOCALS: [&str; 4] =
    ["serviceTask", "businessRuleTask", "scriptTask", "sendTask"];

/// A model-derived worker I/O binding: a job type and the envelope type refs it
/// carries. Serializes to the shape the reifier's `WorkerDecl` reads
/// (`{ taskType, inputType?, outputType? }`), so it can be injected straight into
/// the `domaintypes` op request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct WorkerIo {
    #[serde(rename = "taskType")]
    pub task_type: String,
    #[serde(rename = "inputType", skip_serializing_if = "Option::is_none")]
    pub input_type: Option<String>,
    #[serde(rename = "outputType", skip_serializing_if = "Option::is_none")]
    pub output_type: Option<String>,
    /// The `zeebe:header` keys declared on the task (in author order, deduped).
    /// The reifier turns these into a typed `job.customHeaders` shape keyed by
    /// name with `string` values (Zeebe headers are strings on the wire), so a
    /// worker gets autocomplete + typo rejection on its custom headers. Empty =
    /// no declared headers (`job.customHeaders` stays the untyped fallback).
    #[serde(rename = "headerKeys", skip_serializing_if = "Vec::is_empty")]
    pub header_keys: Vec<String>,
}

/// A model-derived message payload binding: a message name and the envelope type
/// refs it carries. Serializes to `{ messageName, inputType?, outputType? }` — the
/// shape the reifier's message-IO codegen reads — so it can be injected straight
/// into the `domaintypes` op request (ADR 0040 slice 2).
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct MessageIo {
    #[serde(rename = "messageName")]
    pub message_name: String,
    #[serde(rename = "inputType", skip_serializing_if = "Option::is_none")]
    pub input_type: Option<String>,
    #[serde(rename = "outputType", skip_serializing_if = "Option::is_none")]
    pub output_type: Option<String>,
}

/// A model-derived start-envelope binding: the input type a **none start event**
/// (a `bpmn:startEvent` with no event definition — how a process started over the
/// API or from a call activity begins) declares for the process's initial
/// variables. Serializes to `{ process, inputType? }` — the shape the reifier's
/// start-IO codegen reads — so it can be injected straight into the `domaintypes`
/// op request. Input only: a start event produces the process's initial
/// variables, so there is no `.out`. A message/timer/… start is not a carrier —
/// a message start's envelope stays on the referenced `bpmn:message`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct StartIo {
    /// The enclosing `bpmn:process` id (the start event's process is what a
    /// start-instance call keys on).
    pub process: String,
    #[serde(rename = "inputType", skip_serializing_if = "Option::is_none")]
    pub input_type: Option<String>,
}

/// A model-level metadata entry (`nano:meta`), lifted from a `nano:meta` key/value
/// sibling of the `nano:shapes` container on a `bpmn:process` (ADR 0040 §5).
/// Serializes to `{ process?, key, value }` — the `MetaDecl` the reifier folds into
/// the typed `meta.ts` accessor and the structured fuse (`domain.json`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct MetaDecl {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub process: Option<Arc<str>>,
    pub key: String,
    pub value: String,
}

/// Everything one model scan lifts from a BPMN document: the service-task worker
/// I/O map (slice 1), the message payload map (slice 2), the composed motion
/// shapes (§9/§10), the model-level metadata (§5), and the none-start-event
/// input envelopes (the process start-input declarations).
#[derive(Default)]
pub struct BpmnScan {
    pub workers: Vec<WorkerIo>,
    pub messages: Vec<MessageIo>,
    pub shapes: Vec<ShapeDecl>,
    pub meta: Vec<MetaDecl>,
    pub starts: Vec<StartIo>,
    /// Every `bpmn:message` by its **id** (the identity a start event's
    /// `messageRef` points at), with its own input envelope — unlike
    /// [`messages`](Self::messages), never merged by name (issue #1748).
    pub message_defs: Vec<MessageDef>,
    /// Every process-level message start event with its `messageRef`, so a
    /// subscription's envelope is resolved by message id (issue #1748).
    pub message_starts: Vec<MessageStart>,
}

/// A `bpmn:message` keyed by its document-unique `id` and the input envelope
/// it carries (`io.nanobpm.dataEnvelope.in`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageDef {
    pub id: String,
    pub input_type: Option<String>,
}

/// A process-level message start event: its element `id` and the `messageRef`
/// of its `messageEventDefinition`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageStart {
    /// The owning process's id: element ids are unique only per process, so
    /// two processes in one resource may reuse a start id.
    pub process: Option<String>,
    pub element_id: String,
    pub message_ref: String,
}

impl BpmnScan {
    /// The input envelope ref of the message start event `start_element_id`:
    /// follows the start event's `messageRef` to the `bpmn:message` **id**
    /// (never the — possibly evaluated, fallen-back or shared — subscription
    /// message name). `None` when the start event, its message, or the
    /// message's envelope is absent.
    ///
    /// `process_id` scopes the lookup to the owning process (element ids are
    /// unique only per process, so a resource with several processes may
    /// reuse a start id); with `Some`, a start owned by another process never
    /// matches. `None` takes the first start with that id.
    pub fn start_message_envelope(
        &self,
        start_element_id: &str,
        process_id: Option<&str>,
    ) -> Option<&str> {
        let start = self.message_starts.iter().find(|s| {
            s.element_id == start_element_id
                && process_id.is_none_or(|p| s.process.as_deref() == Some(p))
        })?;
        self.message_defs
            .iter()
            .find(|m| m.id == start.message_ref)?
            .input_type
            .as_deref()
    }
}

/// One composition operation of a `nano:shape`, in author (XML) order. Serializes
/// to the `{ op, .. }` tagged union the reifier's `ShapeOp` (`domain_types.ts`)
/// reads, so the ordered fold semantics survive the scan → resolve boundary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum ShapeOp {
    /// Spread every field of the fused entity `ref`.
    Carry { r#ref: String },
    /// Spread only the named `fields` of `ref`, optionally reached via an FK path.
    Project {
        r#ref: String,
        fields: Vec<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        via: Option<String>,
    },
    /// Add a process-authored field (scalar keyword or a fused entity id).
    Extend {
        name: String,
        r#type: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        optional: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        list: Option<bool>,
        /// Human-readable field description (surfaced in a derived JSON
        /// Schema, e.g. an MCP tool `inputSchema`, issue #1738).
        #[serde(skip_serializing_if = "Option::is_none")]
        description: Option<String>,
    },
    /// Pull in another motion shape: `spread` inlines its fields, else nests it.
    Reference {
        name: String,
        r#ref: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        spread: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        list: Option<bool>,
        /// Human-readable field description (surfaced in a derived JSON
        /// Schema, e.g. an MCP tool `inputSchema`, issue #1738).
        #[serde(skip_serializing_if = "Option::is_none")]
        description: Option<String>,
    },
}

/// A composed motion-shape declaration lifted from a `nano:shape` element (ADR
/// 0040 §9). Serializes to the `ShapeDecl` shape the reifier resolves through the
/// fuse into a flat `DomainTypeDef`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ShapeDecl {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub process: Option<Arc<str>>,
    pub ops: Vec<ShapeOp>,
}

/// Which motion-shape carrier the scanner is accumulating inside an open element.
enum Kind {
    /// A service-ish task; `key` is its `zeebe:taskDefinition` type.
    Worker,
    /// A `bpmn:message`; `key` is its `name`.
    Message,
    /// A `bpmn:startEvent`; `key` is the enclosing `bpmn:process` id. The entry
    /// is kept only when the start event turns out to be a **none** start (no
    /// event-definition child) — decided when the element closes.
    Start,
}

/// An event-definition child element (`messageEventDefinition`,
/// `timerEventDefinition`, `signalEventDefinition`, …) disqualifies a start
/// event from being a *none* start. Matched by the `EventDefinition` suffix,
/// mirroring the engine parser's start-event trigger count (`bpmn/walk.rs`).
const EVENT_DEFINITION_SUFFIX: &str = "EventDefinition";

struct Accum {
    kind: Kind,
    /// The map key: a worker's `taskDefinition` type (set while open), a
    /// message's `name` (set at open), or a start event's process id (set at
    /// open from the enclosing process).
    key: Option<String>,
    input_type: Option<String>,
    output_type: Option<String>,
    /// A worker's declared `zeebe:header` keys, in author order (deduped).
    header_keys: Vec<String>,
    /// Hash index over `header_keys`, so deduplication is linear.
    header_seen: HashSet<String>,
    /// A start event's event-definition child count: zero at close = a none
    /// start (an envelope carrier); any definition = a typed start whose
    /// envelope lives elsewhere (a message start's on the `bpmn:message`).
    event_defs: usize,
    /// The carrier element's own `id` (messages and start events).
    elem_id: Option<String>,
    /// A start event's `messageEventDefinition` `messageRef`.
    message_ref: Option<String>,
}

impl Accum {
    fn new(kind: Kind, key: Option<String>) -> Self {
        Self {
            kind,
            key,
            input_type: None,
            output_type: None,
            header_keys: Vec::new(),
            header_seen: HashSet::new(),
            event_defs: 0,
            elem_id: None,
            message_ref: None,
        }
    }
}

/// A literal (non-FEEL, non-empty) job type keys a worker; a FEEL `=expr` type or
/// an empty type does not. Mirrors the modeler's `envelopeContext` guard.
fn literal_task_type(t: &str) -> Option<&str> {
    let t = t.trim();
    if t.is_empty() || t.starts_with('=') {
        None
    } else {
        Some(t)
    }
}

/// Normalise an envelope type ref read from the model: trim surrounding
/// whitespace and treat an empty (or all-whitespace) value as "no envelope", so a
/// hand-edited `.bpmn` with a blank or padded value doesn't yield a ref that
/// silently fails to resolve against any declared type id.
fn envelope_ref(value: Option<&str>) -> Option<String> {
    let v = value?.trim();
    if v.is_empty() {
        None
    } else {
        Some(v.to_string())
    }
}

/// A message name keys a message-IO entry when non-empty. Unlike a worker's job
/// type, a message name is a correlation identity (matched verbatim by the
/// engine), so it is *not* trimmed — only an absent/empty attribute is dropped.
fn message_name(value: Option<&str>) -> Option<String> {
    value.filter(|v| !v.is_empty()).map(str::to_string)
}

/// Scan a single BPMN document into its worker-, message- and start-IO maps.
/// Every service-ish task carrying a literal job type contributes a worker entry
/// (even with no envelope, so a stale manifest projection is *cleared* on
/// rebuild), every named `bpmn:message` contributes a message entry (envelope
/// optional, so the derived `MessageName` union is complete), and every **none
/// start event** (a `bpmn:startEvent` with no event definition) contributes a
/// start entry keyed by its process (envelope optional, so a cleared start
/// envelope clears the derived binding on rebuild). A malformed document yields
/// an empty scan.
pub fn scan_bpmn(xml: &str) -> BpmnScan {
    let tokens = match tokenize(xml) {
        Ok(t) => t,
        Err(_) => return BpmnScan::default(),
    };

    // Entries keyed by job type / message name / process id so a rebuild is
    // deterministic and duplicates fold to one entry (the first non-empty
    // envelope on each side wins; the fuse-conflict diagnostic of ADR 0040 §6 is
    // a later slice).
    let mut workers: BTreeMap<String, WorkerIo> = BTreeMap::new();
    let mut messages: BTreeMap<String, MessageIo> = BTreeMap::new();
    let mut starts: BTreeMap<String, StartIo> = BTreeMap::new();
    let mut message_defs: Vec<MessageDef> = Vec::new();
    let mut message_starts: Vec<MessageStart> = Vec::new();

    // Depth of the currently-open carrier's start tag; `None` when not inside one.
    // Sub-elements (`extensionElements`, `zeebe:taskDefinition`, `zeebe:properties`,
    // `zeebe:property`) are matched only while a carrier is open.
    let mut depth: usize = 0;
    let mut cur: Option<(usize, Accum)> = None;

    // Composed-shape scan (ADR 0040 §9), independent of the worker/message carrier
    // above: `nano:shape` declarations live under `bpmn:process` extension elements,
    // never inside a service task, so the two states never overlap. `process_id`
    // tags each shape's provenance; `cur_shape` accumulates the open shape's ops.
    // `process_id` also keys a start event's entry (the process is the start's
    // identity for a start-instance call).
    // Shared (`Arc`) so every shape/meta entry on a process points at ONE copy
    // of its id: cloning a long id per shape would amplify a small document
    // into an unbounded provenance copy (a wide catalog x a 64 KiB id).
    let mut process_id: Option<Arc<str>> = None;
    // Open-element stack: (in Zeebe namespace, local name). An envelope
    // binding / task definition / header is honoured only as a Zeebe element
    // under its Zeebe container, so vendor metadata with the same local name
    // can never override the real binding.
    let mut elems: Vec<(bool, String)> = Vec::new();
    // Depth of the open `<process>` start tag: only a start event that is a
    // *direct* child (depth == process_depth + 1) is a process-level start. A
    // start nested in a `subProcess` shares `process_id` but is not the
    // process's entry point (mirrors engine-core's model).
    let mut process_depth: Option<usize> = None;
    let mut shapes: Vec<ShapeDecl> = Vec::new();
    let mut cur_shape: Option<(usize, ShapeDecl)> = None;
    // Model-level metadata (ADR 0040 §5): `nano:meta` key/value elements that sit
    // as siblings of the `nano:shapes` container under a process's extension
    // elements (never inside a `nano:shape`), tagged with the enclosing process.
    let mut meta: Vec<MetaDecl> = Vec::new();
    // In-scope `xmlns[:prefix]` declarations, one frame per open element, so a
    // shape-vocabulary element is recognised only in the Nano shapes namespace
    // (a foreign `vendor:shape` with the same local name is ignored).
    // Kept as an active prefix -> URI-stack index (each frame records only the
    // prefixes it declared, popped on close), so resolution is O(1) per element
    // rather than a walk over every ancestor frame.
    let mut ns_frames: Vec<Vec<String>> = Vec::new();
    let mut ns_active: HashMap<String, Vec<String>> = HashMap::new();
    // Per-job-type header membership, kept for the whole scan so merging
    // duplicate tasks never reindexes earlier headers.
    let mut worker_seen: HashMap<String, HashSet<String>> = HashMap::new();

    for tok in &tokens {
        match tok {
            Token::Start {
                name,
                attrs,
                self_closing,
            } => {
                let ln = local_name(name);
                let mut frame: Vec<String> = Vec::new();
                for (k, v) in attrs {
                    let prefix = if k == "xmlns" {
                        ""
                    } else if let Some(p) = k.strip_prefix("xmlns:") {
                        p
                    } else {
                        continue;
                    };
                    ns_active
                        .entry(prefix.to_string())
                        .or_default()
                        .push(v.clone());
                    frame.push(prefix.to_string());
                }
                ns_frames.push(frame);
                let in_nano_ns = namespace_of(name, &ns_active) == Some(NANO_SHAPES_NS);
                let in_zeebe_ns = namespace_of(name, &ns_active) == Some(ZEEBE_NS);
                let parent_is = |local: &str| elems.last().is_some_and(|(z, l)| *z && l == local);
                if cur.is_none() {
                    if SERVICE_TASK_LOCALS.contains(&ln) {
                        // A self-closing service task carries no `taskDefinition`
                        // child, so it has no job type and contributes nothing;
                        // only an open task can key a worker entry.
                        if !self_closing {
                            cur = Some((depth, Accum::new(Kind::Worker, None)));
                        }
                    } else if ln == "message" {
                        // A `bpmn:message` keys by its own `name` (read now). A
                        // self-closing `<bpmn:message name=.. />` carries no
                        // envelope, but its name still completes the `MessageName`
                        // union (ADR 0040 slice 2), so flush it straight away.
                        let mut acc = Accum::new(Kind::Message, message_name(attr(attrs, "name")));
                        acc.elem_id = envelope_ref(attr(attrs, "id"));
                        if *self_closing {
                            record_ids(&acc, &mut message_defs, &mut message_starts);
                            flush(
                                &mut workers,
                                &mut worker_seen,
                                &mut messages,
                                &mut starts,
                                acc,
                            );
                        } else {
                            cur = Some((depth, acc));
                        }
                    } else if ln == "startEvent" && process_depth.is_some_and(|d| depth == d + 1) {
                        // A `bpmn:startEvent` keys by its enclosing process (read
                        // from the most recent open `<process>`). Whether it is a
                        // *none* start (an envelope carrier) is only decidable at
                        // the close tag — an event-definition child disqualifies
                        // it — so the accumulator stays open and `flush` decides.
                        // A self-closing `<startEvent/>` has no children, so it is
                        // a none start by construction: flush it straight away.
                        let mut acc =
                            Accum::new(Kind::Start, process_id.as_deref().map(str::to_string));
                        acc.elem_id = envelope_ref(attr(attrs, "id"));
                        if *self_closing {
                            record_ids(&acc, &mut message_defs, &mut message_starts);
                            flush(
                                &mut workers,
                                &mut worker_seen,
                                &mut messages,
                                &mut starts,
                                acc,
                            );
                        } else {
                            cur = Some((depth, acc));
                        }
                    }
                } else if let Some((_, acc)) = cur.as_mut() {
                    match ln {
                        "taskDefinition" if in_zeebe_ns && matches!(acc.kind, Kind::Worker) => {
                            if let Some(t) = attr(attrs, "type") {
                                acc.key = Some(t.to_string());
                            }
                        }
                        // An event-definition child of a start event makes it a
                        // typed (non-none) start — not an envelope carrier.
                        _ if matches!(acc.kind, Kind::Start)
                            && ln.ends_with(EVENT_DEFINITION_SUFFIX) =>
                        {
                            acc.event_defs += 1;
                            if ln == "messageEventDefinition" {
                                acc.message_ref = envelope_ref(attr(attrs, "messageRef"));
                            }
                        }
                        "property" if in_zeebe_ns && parent_is("properties") => {
                            match attr(attrs, "name") {
                                Some(ENVELOPE_IN) => {
                                    acc.input_type = envelope_ref(attr(attrs, "value"));
                                }
                                Some(ENVELOPE_OUT) => {
                                    acc.output_type = envelope_ref(attr(attrs, "value"));
                                }
                                _ => {}
                            }
                        }
                        // A `zeebe:header` (child of `zeebe:taskHeaders`): collect
                        // its literal `key` so the reifier can type `customHeaders`.
                        // Keys are strings on the wire; a blank/duplicate key is
                        // dropped so the derived shape stays clean and deterministic.
                        "header"
                            if in_zeebe_ns
                                && matches!(acc.kind, Kind::Worker)
                                && parent_is("taskHeaders") =>
                        {
                            if let Some(k) = attr(attrs, "key") {
                                let k = k.trim();
                                if !k.is_empty() && acc.header_seen.insert(k.to_string()) {
                                    acc.header_keys.push(k.to_string());
                                }
                            }
                        }
                        _ => {}
                    }
                }
                // Composed-shape carrier (independent of `cur`): track the enclosing
                // process for provenance, open a `nano:shape`, and fold its op
                // children (`nano:carry`/`project`/`extend`/`reference`) in order.
                if ln == "process"
                    && let Some(id) = attr(attrs, "id")
                {
                    process_id = Some(Arc::from(id));
                    process_depth = Some(depth);
                }
                if cur_shape.is_none() {
                    if ln == "shape" && in_nano_ns {
                        let decl = ShapeDecl {
                            id: shape_attr(attrs, "id").unwrap_or_default(),
                            name: shape_attr(attrs, "name"),
                            process: process_id.clone(),
                            ops: Vec::new(),
                        };
                        if *self_closing {
                            if !decl.id.is_empty() {
                                shapes.push(decl);
                            }
                        } else {
                            cur_shape = Some((depth, decl));
                        }
                    } else if ln == "meta"
                        && in_nano_ns
                        && let Some(key) = shape_attr(attrs, "key")
                    {
                        // A model-level `nano:meta` sibling of `nano:shapes`. Only
                        // recognised outside a shape carrier (an `extend` op is a
                        // different vocabulary), keyed to the enclosing process.
                        meta.push(MetaDecl {
                            process: process_id.clone(),
                            key,
                            value: attr(attrs, "value").unwrap_or_default().to_string(),
                        });
                    }
                } else if let Some((_, decl)) = cur_shape.as_mut()
                    && in_nano_ns
                    && let Some(op) = parse_shape_op(ln, attrs)
                {
                    decl.ops.push(op);
                }
                if *self_closing {
                    pop_ns_frame(&mut ns_frames, &mut ns_active);
                } else {
                    elems.push((in_zeebe_ns, ln.to_string()));
                    depth += 1;
                }
            }
            Token::End { .. } => {
                elems.pop();
                pop_ns_frame(&mut ns_frames, &mut ns_active);
                depth = depth.saturating_sub(1);
                if let Some((open_depth, acc)) = cur.take() {
                    if depth == open_depth {
                        record_ids(&acc, &mut message_defs, &mut message_starts);
                        flush(
                            &mut workers,
                            &mut worker_seen,
                            &mut messages,
                            &mut starts,
                            acc,
                        );
                    } else {
                        cur = Some((open_depth, acc));
                    }
                }
                if let Some((open_depth, decl)) = cur_shape.take() {
                    if depth == open_depth {
                        if !decl.id.is_empty() {
                            shapes.push(decl);
                        }
                    } else {
                        cur_shape = Some((open_depth, decl));
                    }
                }
            }
            Token::Text(_) => {}
        }
    }

    BpmnScan {
        workers: workers.into_values().collect(),
        messages: messages.into_values().collect(),
        shapes,
        meta,
        starts: starts.into_values().collect(),
        message_defs,
        message_starts,
    }
}

/// Record a closed message / message-start carrier by element id (issue
/// #1748): a message by its `id` with its own envelope, a start event with
/// a `messageEventDefinition` by its `id` and `messageRef`.
fn record_ids(acc: &Accum, defs: &mut Vec<MessageDef>, starts: &mut Vec<MessageStart>) {
    let Some(id) = acc.elem_id.clone() else {
        return;
    };
    match acc.kind {
        Kind::Message => defs.push(MessageDef {
            id,
            input_type: acc.input_type.clone(),
        }),
        Kind::Start => {
            // Keyed on `messageRef` alone, as the engine selects a message
            // start: extra (vendor / inert) event definitions don't hide it.
            if let Some(message_ref) = acc.message_ref.clone() {
                starts.push(MessageStart {
                    process: acc.key.clone(),
                    element_id: id,
                    message_ref,
                });
            }
        }
        Kind::Worker => {}
    }
}

/// The Nano shapes XML namespace (`xmlns:nano`); shape-vocabulary elements
/// (`shape`, its ops, `meta`) are recognised only in this namespace.
pub const NANO_SHAPES_NS: &str = "https://nanobpm.io/schema/shapes/1.0";

/// The Zeebe extension XML namespace (`xmlns:zeebe`); task definitions,
/// headers and envelope-binding properties are recognised only in it.
pub const ZEEBE_NS: &str = "http://camunda.org/schema/zeebe/1.0";

/// Resolve the namespace URI of a qualified element name against the active
/// prefix index (innermost declaration wins); `None` when undeclared.
fn namespace_of<'a>(name: &str, active: &'a HashMap<String, Vec<String>>) -> Option<&'a str> {
    let prefix = name.split_once(':').map_or("", |(p, _)| p);
    active
        .get(prefix)
        .and_then(|stack| stack.last())
        .map(String::as_str)
}

/// Close one element's namespace scope: restore every prefix it declared.
fn pop_ns_frame(frames: &mut Vec<Vec<String>>, active: &mut HashMap<String, Vec<String>>) {
    for prefix in frames.pop().unwrap_or_default() {
        if let Some(stack) = active.get_mut(&prefix) {
            stack.pop();
            if stack.is_empty() {
                active.remove(&prefix);
            }
        }
    }
}

/// Append `keys` to a worker's header list in first-seen order, using the
/// scan-lifetime membership set for that job type (linear in new keys only).
fn merge_header_keys(
    entry: &mut WorkerIo,
    seen: &mut HashMap<String, HashSet<String>>,
    keys: Vec<String>,
) {
    let set = seen.entry(entry.task_type.clone()).or_default();
    for k in keys {
        if set.insert(k.clone()) {
            entry.header_keys.push(k);
        }
    }
}

/// Parse one `nano:shape` composition child into a `ShapeOp`. An op missing its
/// identifying attribute (a `ref` for carry/project/reference; a `name`+`type` for
/// extend) is malformed and dropped, so a broken child does not synthesise a
/// spurious binding. Non-op local names (e.g. the `nano:shapes` container) yield
/// `None`.
fn parse_shape_op(ln: &str, attrs: &[(String, String)]) -> Option<ShapeOp> {
    match ln {
        "carry" => Some(ShapeOp::Carry {
            r#ref: shape_attr(attrs, "ref")?,
        }),
        "project" => {
            // A project with no field names is a silent no-op (`carry` already
            // spreads all fields), so treat an empty list as malformed and drop it.
            let fields = attr(attrs, "fields").map(split_fields).unwrap_or_default();
            if fields.is_empty() {
                return None;
            }
            Some(ShapeOp::Project {
                r#ref: shape_attr(attrs, "ref")?,
                fields,
                via: shape_attr(attrs, "via"),
            })
        }
        "extend" => Some(ShapeOp::Extend {
            name: shape_attr(attrs, "name")?,
            r#type: shape_attr(attrs, "type")?,
            optional: bool_attr(attrs, "optional"),
            list: bool_attr(attrs, "list"),
            description: shape_attr(attrs, "description"),
        }),
        "reference" => Some(ShapeOp::Reference {
            name: shape_attr(attrs, "name")?,
            r#ref: shape_attr(attrs, "ref")?,
            spread: bool_attr(attrs, "spread"),
            list: bool_attr(attrs, "list"),
            description: shape_attr(attrs, "description"),
        }),
        _ => None,
    }
}

/// Read a shape attribute, trimming whitespace and dropping an empty value (so a
/// blank `ref=""` is treated as absent rather than a ref to `""`).
fn shape_attr(attrs: &[(String, String)], local: &str) -> Option<String> {
    let v = attr(attrs, local)?.trim();
    if v.is_empty() {
        None
    } else {
        Some(v.to_string())
    }
}

/// Split a `project fields="a, b ,c"` attribute into trimmed, non-empty names.
fn split_fields(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

/// Read a boolean shape attribute; `Some(true)`/`Some(false)` only when present,
/// so an absent flag serializes as omitted (matching the TS optional field).
fn bool_attr(attrs: &[(String, String)], local: &str) -> Option<bool> {
    attr(attrs, local).map(|v| v.trim().eq_ignore_ascii_case("true"))
}

/// Fold one closed carrier's accumulator into the right map. Workers are keyed by
/// their literal job type (non-literal or missing types are dropped); messages by
/// their name; start events by their process — and only when the start event
/// closed with no event-definition child (a *none* start). On a duplicate key,
/// fill any still-empty envelope side so a binding is not lost.
fn flush(
    workers: &mut BTreeMap<String, WorkerIo>,
    worker_seen: &mut HashMap<String, HashSet<String>>,
    messages: &mut BTreeMap<String, MessageIo>,
    starts: &mut BTreeMap<String, StartIo>,
    acc: Accum,
) {
    let Some(raw) = acc.key.as_deref() else {
        return;
    };
    match acc.kind {
        Kind::Worker => {
            let Some(task_type) = literal_task_type(raw) else {
                return;
            };
            let entry = workers
                .entry(task_type.to_string())
                .or_insert_with(|| WorkerIo {
                    task_type: task_type.to_string(),
                    input_type: None,
                    output_type: None,
                    header_keys: Vec::new(),
                });
            fill(&mut entry.input_type, acc.input_type);
            fill(&mut entry.output_type, acc.output_type);
            // Union header keys across duplicate task entries, first-seen order
            // (hash-indexed, so the union is linear in the header count).
            merge_header_keys(entry, worker_seen, acc.header_keys);
        }
        Kind::Message => {
            let entry = messages
                .entry(raw.to_string())
                .or_insert_with(|| MessageIo {
                    message_name: raw.to_string(),
                    input_type: None,
                    output_type: None,
                });
            fill(&mut entry.input_type, acc.input_type);
            fill(&mut entry.output_type, acc.output_type);
        }
        Kind::Start => {
            // Only a none start carries an envelope on the event itself; a typed
            // start's envelope lives elsewhere (a message start's on the
            // referenced `bpmn:message`, scanned as a `Message` carrier).
            if acc.event_defs > 0 {
                return;
            }
            let entry = starts.entry(raw.to_string()).or_insert_with(|| StartIo {
                process: raw.to_string(),
                input_type: None,
            });
            fill(&mut entry.input_type, acc.input_type);
        }
    }
}

/// Set `slot` from `incoming` only when `slot` is still empty (first-wins merge).
fn fill(slot: &mut Option<String>, incoming: Option<String>) {
    if slot.is_none() {
        *slot = incoming;
    }
}

/// The manifest file whose `models.processes` globs override the convention.
pub const MANIFEST_FILE: &str = "nano.app.json";

/// The convention-default BPMN process glob (ADR 0062, `spec-app/nano-app.schema.json`
/// `models.processes`), used when the manifest declares no override.
pub const DEFAULT_PROCESS_GLOBS: &[&str] = &["resources/**/*.bpmn"];

/// The BPMN process globs for the project at `project_dir` — the SINGLE source of
/// truth for "which files are process models", shared by [`scan_project`] and the
/// save-time regeneration trigger ([`is_process_model_path`]). Reads
/// `nano.app.json` `models.processes` when declared as an array — an explicitly
/// empty array is honoured (no process models). An absent key, a missing or
/// malformed manifest, or a non-array value falls back to the convention default
/// [`DEFAULT_PROCESS_GLOBS`]. Globs that could escape the project (absolute,
/// drive/platform-prefixed, backslashed, or containing a `..` segment) are
/// dropped — fail closed.
pub fn process_globs(project_dir: &Path) -> Vec<String> {
    let declared = std::fs::read_to_string(project_dir.join(MANIFEST_FILE))
        .ok()
        .and_then(|raw| serde_json::from_str::<serde_json::Value>(&raw).ok())
        .and_then(|v| {
            v.get("models")?.get("processes")?.as_array().map(|a| {
                a.iter()
                    .filter_map(|g| g.as_str())
                    .filter(|g| is_safe_glob(g))
                    .map(|g| g.trim_start_matches("./").to_string())
                    .filter(|g| !g.is_empty())
                    .collect::<Vec<_>>()
            })
        });
    match declared {
        Some(globs) => globs,
        None => DEFAULT_PROCESS_GLOBS
            .iter()
            .map(|g| (*g).to_string())
            .collect(),
    }
}

/// A manifest glob is safe when it is project-relative and cannot escape the
/// project: no leading `/`, no `\`, no `:` (drive / URI / platform prefix) and no
/// `.`/`..` segment beyond a single leading `./`.
fn is_safe_glob(g: &str) -> bool {
    let g = g.strip_prefix("./").unwrap_or(g);
    !g.starts_with('/')
        && !g.contains('\\')
        && !g.contains(':')
        && g.split('/').all(|s| s != ".." && s != ".")
}

/// Traversal eligibility of the project-relative path `rel` — the SINGLE rule set
/// shared by the scan ([`resolve_process_models`]) and the trigger
/// ([`is_process_model_path`]): no `.`/`..`/empty-escaping segment, no directory
/// component in [`SKIP_DIRS`], no directory component that is a symlink, and a
/// symlinked file must resolve inside the project.
fn is_eligible(project_dir: &Path, rel: &str) -> bool {
    let segs: Vec<&str> = rel.split('/').filter(|s| !s.is_empty()).collect();
    let Some((_, dirs)) = segs.split_last() else {
        return false;
    };
    if segs
        .iter()
        .any(|s| *s == ".." || *s == "." || s.contains(':'))
    {
        return false;
    }
    let mut cur = project_dir.to_path_buf();
    for d in dirs {
        if SKIP_DIRS.contains(d) {
            return false;
        }
        cur.push(d);
        if std::fs::symlink_metadata(&cur).is_ok_and(|m| m.file_type().is_symlink()) {
            return false;
        }
    }
    let file = project_dir.join(rel);
    if std::fs::symlink_metadata(&file).is_ok_and(|m| m.file_type().is_symlink()) {
        let (Ok(target), Ok(root)) = (file.canonicalize(), project_dir.canonicalize()) else {
            return false;
        };
        if !target.starts_with(root) {
            return false;
        }
    }
    true
}

/// Whether the project-relative path `rel` (`/`-separated, a leading `/` or `./`
/// tolerated) is a process model of the project at `project_dir`, i.e. matches
/// one of its [`process_globs`] and passes the shared traversal-eligibility rules.
/// This is exactly the set [`resolve_process_models`] reads.
pub fn is_process_model_path(project_dir: &Path, rel: &str) -> bool {
    let rel = rel.replace('\\', "/");
    let rel = rel.trim_start_matches('/').trim_start_matches("./");
    process_globs(project_dir)
        .iter()
        .any(|g| glob_match(g, rel))
        && is_eligible(project_dir, rel)
}

/// Match a `/`-separated relative path against a glob supporting `*` and `?`
/// (within one segment) and `**` (any number of segments, including zero).
/// Directory segments match case-sensitively (the scan opens them by their exact
/// spelling); only the final (file-name) segment is ASCII case-insensitive, so a
/// `.BPMN` model matches a `*.bpmn` glob.
pub fn glob_match(glob: &str, path: &str) -> bool {
    let g: Vec<&str> = glob.split('/').filter(|s| !s.is_empty()).collect();
    let p: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    match_segments(&g, &p)
}

fn match_segments(g: &[&str], p: &[&str]) -> bool {
    let gn = g.len();
    let pn = p.len();
    // dp[j] = whether g[i..] matches p[j..] (filled bottom-up from i = gn down to 0).
    let mut dp = vec![false; pn + 1];
    dp[pn] = true;
    for i in (0..gn).rev() {
        let mut next = vec![false; pn + 1];
        if g[i] == "**" {
            // `**` matches 0 or more segments: g[i..] matches p[j..] iff
            // g[i+1..] matches p[j..] (0 segments) OR g[i..] matches p[j+1..] (1+ segments).
            let mut suffix_any = false;
            for j in (0..=pn).rev() {
                suffix_any = suffix_any || dp[j];
                next[j] = suffix_any;
            }
        } else {
            for j in 0..pn {
                // Case folding only applies when both are the final segment.
                let fold = i + 1 == gn && j + 1 == pn;
                if dp[j + 1] && match_segment(g[i].as_bytes(), p[j].as_bytes(), fold) {
                    next[j] = true;
                }
            }
        }
        dp = next;
    }
    dp[0]
}

fn match_segment(g: &[u8], s: &[u8], fold: bool) -> bool {
    let sn = s.len();
    // dp[j] = whether g[i..] matches s[j..] (O(|g| * |s|) time, O(|s|) space).
    let mut dp = vec![false; sn + 1];
    dp[sn] = true;
    for &gc in g.iter().rev() {
        let mut next = vec![false; sn + 1];
        match gc {
            b'*' => {
                let mut suffix_any = false;
                for j in (0..=sn).rev() {
                    suffix_any = suffix_any || dp[j];
                    next[j] = suffix_any;
                }
            }
            b'?' => {
                next[..sn].copy_from_slice(&dp[1..=sn]);
            }
            c => {
                for j in 0..sn {
                    let eq = if fold {
                        s[j].eq_ignore_ascii_case(&c)
                    } else {
                        s[j] == c
                    };
                    next[j] = eq && dp[j + 1];
                }
            }
        }
        dp = next;
    }
    dp[0]
}

/// Directories never descended into while resolving model globs.
const SKIP_DIRS: &[&str] = &["node_modules", ".git", "target", "nano-generated"];

/// Resolve the project's [`process_globs`] to the sorted, de-duplicated set of
/// existing model files. Each glob is walked from its literal (wildcard-free)
/// prefix (or, when the glob has no wildcards at all, a single non-recursive
/// listing of its parent directory so literal filenames do not scan unrelated
/// subtrees); every candidate must also pass the shared eligibility rules (so a
/// literal prefix cannot smuggle in an excluded or symlinked directory), and
/// symlinked directories are not followed.
pub fn resolve_process_models(project_dir: &Path) -> Vec<std::path::PathBuf> {
    let mut out: std::collections::BTreeSet<std::path::PathBuf> = Default::default();
    for glob in process_globs(project_dir) {
        let segs: Vec<&str> = glob.split('/').filter(|s| !s.is_empty()).collect();
        if segs.is_empty() {
            continue;
        }
        let lit = segs.iter().take_while(|s| !s.contains(['*', '?'])).count();
        let is_literal = lit == segs.len();
        let single_dir = is_literal || (lit + 1 == segs.len() && segs[lit] != "**");
        let base_rel = segs[..lit.min(segs.len().saturating_sub(1))].join("/");
        if !base_rel.is_empty() && !is_eligible(project_dir, &format!("{base_rel}/_")) {
            continue;
        }
        let base = if base_rel.is_empty() {
            project_dir.to_path_buf()
        } else {
            project_dir.join(&base_rel)
        };
        let mut accept = |rel: &str, path: &Path| {
            if glob_match(&glob, rel) && is_eligible(project_dir, rel) {
                out.insert(path.to_path_buf());
            }
        };
        if single_dir {
            list_dir_files(&base, &base_rel, &mut accept);
        } else {
            walk(&base, &base_rel, &mut accept);
        }
    }
    out.into_iter().collect()
}

/// Non-recursive single-directory file listing (used for literal globs so we
/// preserve case-insensitive filename matching without walking subtrees).
fn list_dir_files(dir: &Path, rel: &str, f: &mut dyn FnMut(&str, &Path)) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for e in entries.flatten() {
        let Ok(ft) = e.file_type() else { continue };
        let path = e.path();
        if !(ft.is_file() || (ft.is_symlink() && path.is_file())) {
            continue;
        }
        let name = e.file_name().to_string_lossy().into_owned();
        let child_rel = if rel.is_empty() {
            name
        } else {
            format!("{rel}/{name}")
        };
        f(&child_rel, &path);
    }
}

fn walk(dir: &Path, rel: &str, f: &mut dyn FnMut(&str, &Path)) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for e in entries.flatten() {
        let Ok(ft) = e.file_type() else { continue };
        let name = e.file_name().to_string_lossy().into_owned();
        let child_rel = if rel.is_empty() {
            name.clone()
        } else {
            format!("{rel}/{name}")
        };
        let path = e.path();
        if ft.is_dir() {
            if !SKIP_DIRS.contains(&name.as_str()) {
                walk(&path, &child_rel, f);
            }
        } else if ft.is_file() || (ft.is_symlink() && path.is_file()) {
            f(&child_rel, &path);
        }
    }
}

/// Scan every process model of the project at `project_dir` — the files matching
/// its [`process_globs`] (manifest `models.processes`, else the convention default
/// `resources/**/*.bpmn`) — into the merged worker-, message- and start-IO maps.
/// Unreadable or malformed files are skipped; a project with no processes yields
/// an empty scan.
pub fn scan_project(project_dir: &Path) -> BpmnScan {
    let files = resolve_process_models(project_dir);
    let mut workers: BTreeMap<String, WorkerIo> = BTreeMap::new();
    let mut worker_seen: HashMap<String, HashSet<String>> = HashMap::new();
    let mut messages: BTreeMap<String, MessageIo> = BTreeMap::new();
    let mut starts: BTreeMap<String, StartIo> = BTreeMap::new();
    let mut shapes: Vec<ShapeDecl> = Vec::new();
    let mut meta: Vec<MetaDecl> = Vec::new();
    for path in files {
        let Ok(xml) = std::fs::read_to_string(&path) else {
            continue;
        };
        let scan = scan_bpmn(&xml);
        for io in scan.workers {
            let entry = workers
                .entry(io.task_type.clone())
                .or_insert_with(|| WorkerIo {
                    task_type: io.task_type.clone(),
                    input_type: None,
                    output_type: None,
                    header_keys: Vec::new(),
                });
            fill(&mut entry.input_type, io.input_type);
            fill(&mut entry.output_type, io.output_type);
            merge_header_keys(entry, &mut worker_seen, io.header_keys);
        }
        for io in scan.messages {
            let entry = messages
                .entry(io.message_name.clone())
                .or_insert_with(|| MessageIo {
                    message_name: io.message_name.clone(),
                    input_type: None,
                    output_type: None,
                });
            fill(&mut entry.input_type, io.input_type);
            fill(&mut entry.output_type, io.output_type);
        }
        // Start envelopes are keyed by process id; a process spanning files folds
        // to one entry (first non-empty envelope wins), so a cleared envelope in
        // a re-saved model clears the derived binding on rebuild.
        for io in scan.starts {
            let entry = starts.entry(io.process.clone()).or_insert_with(|| StartIo {
                process: io.process.clone(),
                input_type: None,
            });
            fill(&mut entry.input_type, io.input_type);
        }
        // Shapes are model-scoped declarations; gather them across every model so
        // the reifier resolves references (including cross-model carries) against
        // the combined set (ADR 0040 §10 second pass). Deterministic file order
        // (files are sorted) keeps the merged list stable.
        shapes.extend(scan.shapes);
        // Model-level metadata is likewise model-scoped; gather it across every
        // model, tagged with its process, in deterministic file order.
        meta.extend(scan.meta);
    }
    BpmnScan {
        workers: workers.into_values().collect(),
        messages: messages.into_values().collect(),
        shapes,
        meta,
        starts: starts.into_values().collect(),
        // Element ids are document-scoped, so the id-keyed message maps of
        // single-document resolution are not merged across files.
        message_defs: Vec::new(),
        message_starts: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test helper: the worker-IO map derived from a single document.
    fn workers(xml: &str) -> Vec<WorkerIo> {
        scan_bpmn(xml).workers
    }

    /// Test helper: the message-IO map derived from a single document.
    fn messages(xml: &str) -> Vec<MessageIo> {
        scan_bpmn(xml).messages
    }

    fn task(id: &str, kind: &str, job: &str, env: &str) -> String {
        format!(
            r#"<bpmn:{kind} id="{id}" name="{id}">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="{job}" />
                <zeebe:ioMapping />
                <zeebe:properties>{env}</zeebe:properties>
              </bpmn:extensionElements>
            </bpmn:{kind}>"#
        )
    }

    fn doc(body: &str) -> String {
        format!(
            r#"<?xml version="1.0"?>
            <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                              xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
              <bpmn:process id="p" isExecutable="true">{body}</bpmn:process>
            </bpmn:definitions>"#
        )
    }

    const IN_ORDER: &str = r#"<zeebe:property name="io.nanobpm.dataEnvelope.in" value="Order" />"#;
    const OUT_RECEIPT: &str =
        r#"<zeebe:property name="io.nanobpm.dataEnvelope.out" value="Receipt" />"#;

    #[test]
    fn derives_in_and_out_from_a_service_task() {
        let xml = doc(&task(
            "t1",
            "serviceTask",
            "charge",
            &format!("{IN_ORDER}{OUT_RECEIPT}"),
        ));
        assert_eq!(
            workers(&xml),
            vec![WorkerIo {
                task_type: "charge".into(),
                input_type: Some("Order".into()),
                output_type: Some("Receipt".into()),
                header_keys: vec![],
            }]
        );
    }

    #[test]
    fn emits_a_typed_task_with_no_envelope_so_stale_io_is_cleared() {
        let xml = doc(&task("t1", "serviceTask", "noop", ""));
        assert_eq!(
            workers(&xml),
            vec![WorkerIo {
                task_type: "noop".into(),
                input_type: None,
                output_type: None,
                header_keys: vec![],
            }]
        );
    }

    #[test]
    fn covers_all_service_ish_task_kinds() {
        let body = format!(
            "{}{}{}{}",
            task("a", "serviceTask", "svc", IN_ORDER),
            task("b", "businessRuleTask", "rule", IN_ORDER),
            task("c", "scriptTask", "script", IN_ORDER),
            task("d", "sendTask", "send", IN_ORDER),
        );
        let got: Vec<String> = workers(&doc(&body))
            .into_iter()
            .map(|w| w.task_type)
            .collect();
        assert_eq!(got, vec!["rule", "script", "send", "svc"]); // BTree-sorted
    }

    #[test]
    fn skips_feel_and_empty_job_types() {
        let body = format!(
            "{}{}",
            task("a", "serviceTask", "=worker + 1", IN_ORDER),
            task("b", "serviceTask", "", IN_ORDER),
        );
        assert_eq!(workers(&doc(&body)), vec![]);
    }

    #[test]
    fn ignores_user_tasks_and_non_envelope_properties() {
        let body = format!(
            r#"<bpmn:userTask id="u" name="u">
                 <bpmn:extensionElements>
                   <zeebe:properties>
                     <zeebe:property name="something.else" value="X" />
                   </zeebe:properties>
                 </bpmn:extensionElements>
               </bpmn:userTask>
               {}"#,
            task(
                "s",
                "serviceTask",
                "svc",
                &format!(r#"<zeebe:property name="unrelated" value="Y" />{IN_ORDER}"#)
            ),
        );
        assert_eq!(
            workers(&doc(&body)),
            vec![WorkerIo {
                task_type: "svc".into(),
                input_type: Some("Order".into()),
                output_type: None,
                header_keys: vec![],
            }]
        );
    }

    #[test]
    fn malformed_xml_yields_no_bindings() {
        assert_eq!(workers("<bpmn:definitions <<>"), vec![]);
    }

    #[test]
    fn trims_whitespace_and_drops_blank_envelope_refs() {
        let env = concat!(
            r#"<zeebe:property name="io.nanobpm.dataEnvelope.in" value="  Order  " />"#,
            r#"<zeebe:property name="io.nanobpm.dataEnvelope.out" value="   " />"#,
        );
        let xml = doc(&task("t", "serviceTask", "charge", env));
        assert_eq!(
            workers(&xml),
            vec![WorkerIo {
                task_type: "charge".into(),
                input_type: Some("Order".into()),
                output_type: None,
                header_keys: vec![],
            }]
        );
    }

    #[test]
    fn serializes_to_worker_decl_shape() {
        let io = WorkerIo {
            task_type: "charge".into(),
            input_type: Some("Order".into()),
            output_type: None,
            header_keys: vec![],
        };
        let v = serde_json::to_value(&io).unwrap();
        assert_eq!(v["taskType"], "charge");
        assert_eq!(v["inputType"], "Order");
        assert!(v.get("outputType").is_none());
        assert!(v.get("headerKeys").is_none());
    }

    // --- custom-header keys (typed `job.customHeaders`, ADR 0033 §3) --------

    /// Build a service task carrying `zeebe:taskHeaders` with the given headers,
    /// each a `(key, value)` pair, plus an optional envelope body.
    fn task_with_headers(id: &str, job: &str, headers: &[(&str, &str)], env: &str) -> String {
        let hs: String = headers
            .iter()
            .map(|(k, v)| format!(r#"<zeebe:header key="{k}" value="{v}" />"#))
            .collect();
        format!(
            r#"<bpmn:serviceTask id="{id}" name="{id}">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="{job}" />
                <zeebe:taskHeaders>{hs}</zeebe:taskHeaders>
                <zeebe:properties>{env}</zeebe:properties>
              </bpmn:extensionElements>
            </bpmn:serviceTask>"#
        )
    }

    #[test]
    fn many_distinct_header_keys_dedup_linearly() {
        let headers: Vec<(String, String)> = (0..9_000)
            .map(|i| (format!("h{i}"), "v".to_string()))
            .collect();
        let refs: Vec<(&str, &str)> = headers
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let body = format!(
            "{}{}",
            task_with_headers("t1", "charge", &refs, IN_ORDER),
            task_with_headers("t2", "charge", &refs, IN_ORDER)
        );
        let ws = workers(&doc(&body));
        assert_eq!(ws[0].header_keys.len(), 9_000);
        assert_eq!(ws[0].header_keys[8_999], "h8999");
    }

    #[test]
    fn many_same_type_tasks_merge_headers_linearly() {
        // 9,000 single-header tasks sharing one job type: the cross-task union
        // must keep one membership set per type rather than reindexing.
        let body: String = (0..9_000)
            .map(|i| {
                let k = format!("h{}", i % 100);
                task_with_headers(&format!("t{i}"), "charge", &[(k.as_str(), "v")], IN_ORDER)
            })
            .collect();
        let ws = workers(&doc(&body));
        assert_eq!(ws.len(), 1);
        assert_eq!(ws[0].header_keys.len(), 100);
        assert_eq!(ws[0].header_keys[99], "h99");
    }

    #[test]
    fn deeply_nested_extensions_resolve_namespaces_with_scoped_restore() {
        // 9,000 nested foreign elements (one re-declaring `nano` to a foreign URI
        // mid-way): resolution must stay O(1) per element and restore on close.
        let depth = 9_000;
        let open: String = (0..depth)
            .map(|i| {
                if i == depth / 2 {
                    r#"<vendor:x xmlns:nano="https://example.com/not-nano">"#.to_string()
                } else {
                    "<vendor:x>".to_string()
                }
            })
            .collect();
        let close = "</vendor:x>".repeat(depth);
        let xml = format!(
            r#"<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                xmlns:vendor="https://example.com/vendor"
                xmlns:nano="https://nanobpm.io/schema/shapes/1.0">
              <bpmn:process id="orders"><bpmn:extensionElements>
                {open}<nano:shape id="Shadowed"><nano:carry ref="X" /></nano:shape>{close}
                <nano:shape id="S"><nano:carry ref="Order" /></nano:shape>
              </bpmn:extensionElements></bpmn:process>
            </bpmn:definitions>"#
        );
        let scan = scan_bpmn(&xml);
        let ids: Vec<&str> = scan.shapes.iter().map(|s| s.id.as_str()).collect();
        assert_eq!(ids, vec!["S"]);
    }

    #[test]
    fn vendor_metadata_cannot_override_zeebe_envelope_bindings() {
        let xml = r#"<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                xmlns:zeebe="http://camunda.org/schema/zeebe/1.0"
                xmlns:vendor="https://example.com/vendor">
              <bpmn:message id="m" name="placeOrder"><bpmn:extensionElements>
                <zeebe:properties><zeebe:property name="io.nanobpm.dataEnvelope.in" value="Order" /></zeebe:properties>
                <vendor:property name="io.nanobpm.dataEnvelope.in" value="Other" />
                <vendor:bag><zeebe:property name="io.nanobpm.dataEnvelope.in" value="Other2" /></vendor:bag>
              </bpmn:extensionElements></bpmn:message>
              <bpmn:process id="p"><bpmn:serviceTask id="t"><bpmn:extensionElements>
                <zeebe:taskDefinition type="charge" />
                <vendor:taskDefinition type="evil" />
                <zeebe:taskHeaders><zeebe:header key="region" value="eu" /></zeebe:taskHeaders>
                <vendor:header key="x" value="y" />
                <zeebe:properties><zeebe:property name="io.nanobpm.dataEnvelope.in" value="Order" /></zeebe:properties>
                <vendor:property name="io.nanobpm.dataEnvelope.in" value="Other" />
              </bpmn:extensionElements></bpmn:serviceTask></bpmn:process>
            </bpmn:definitions>"#;
        let scan = scan_bpmn(xml);
        assert_eq!(scan.messages.len(), 1);
        assert_eq!(scan.messages[0].input_type.as_deref(), Some("Order"));
        assert_eq!(scan.workers.len(), 1);
        let w = &scan.workers[0];
        assert_eq!(w.task_type, "charge");
        assert_eq!(w.input_type.as_deref(), Some("Order"));
        assert_eq!(w.header_keys, vec!["region".to_string()]);
    }

    #[test]
    fn long_process_id_is_shared_not_copied_per_shape() {
        let id = "p".repeat(64 * 1024);
        let shapes: String = (0..200)
            .map(|i| format!("<nano:shape id=\"S{i}\" />"))
            .collect();
        let xml = format!(
            r#"<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                xmlns:nano="https://nanobpm.io/schema/shapes/1.0">
              <bpmn:process id="{id}"><bpmn:extensionElements><nano:shapes>{shapes}</nano:shapes>
              </bpmn:extensionElements></bpmn:process></bpmn:definitions>"#
        );
        let scan = scan_bpmn(&xml);
        assert_eq!(scan.shapes.len(), 200);
        let first = scan.shapes[0].process.as_ref().unwrap();
        assert!(
            scan.shapes
                .iter()
                .all(|s| Arc::ptr_eq(s.process.as_ref().unwrap(), first))
        );
    }

    #[test]
    fn foreign_namespace_shape_vocabulary_is_ignored() {
        let xml = r#"<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                xmlns:vendor="https://example.com/vendor"
                xmlns:nano="https://nanobpm.io/schema/shapes/1.0">
              <bpmn:process id="orders"><bpmn:extensionElements>
                <vendor:shape id="S"><vendor:carry ref="X" /></vendor:shape>
                <vendor:meta key="k" value="v" />
                <nano:shapes><nano:shape id="S"><vendor:carry ref="Y" /><nano:carry ref="Order" /></nano:shape></nano:shapes>
              </bpmn:extensionElements></bpmn:process>
            </bpmn:definitions>"#;
        let scan = scan_bpmn(xml);
        assert_eq!(scan.shapes.len(), 1);
        assert_eq!(
            scan.shapes[0].ops,
            vec![ShapeOp::Carry {
                r#ref: "Order".into()
            }]
        );
        assert!(scan.meta.is_empty());
    }

    #[test]
    fn derives_header_keys_in_author_order() {
        let xml = doc(&task_with_headers(
            "t1",
            "charge",
            &[("region", "eu"), ("priority", "high")],
            IN_ORDER,
        ));
        assert_eq!(
            workers(&xml),
            vec![WorkerIo {
                task_type: "charge".into(),
                input_type: Some("Order".into()),
                output_type: None,
                header_keys: vec!["region".into(), "priority".into()],
            }]
        );
    }

    #[test]
    fn dedups_and_drops_blank_header_keys() {
        let xml = doc(&task_with_headers(
            "t1",
            "charge",
            &[("region", "eu"), ("  ", "x"), ("region", "us")],
            "",
        ));
        assert_eq!(
            workers(&xml),
            vec![WorkerIo {
                task_type: "charge".into(),
                input_type: None,
                output_type: None,
                header_keys: vec!["region".into()],
            }]
        );
    }

    #[test]
    fn serializes_header_keys_when_present() {
        let io = WorkerIo {
            task_type: "charge".into(),
            input_type: None,
            output_type: None,
            header_keys: vec!["region".into(), "priority".into()],
        };
        let v = serde_json::to_value(&io).unwrap();
        assert_eq!(v["headerKeys"], serde_json::json!(["region", "priority"]));
    }

    // --- message-carried envelopes (ADR 0040 slice 2) ----------------------

    fn message(name: &str, env: &str) -> String {
        format!(
            r#"<bpmn:message id="Msg_{name}" name="{name}">
              <bpmn:extensionElements>
                <zeebe:properties>{env}</zeebe:properties>
              </bpmn:extensionElements>
            </bpmn:message>"#
        )
    }

    /// A `bpmn:message` is a top-level definitions child, so wrap without a process.
    fn defs(body: &str) -> String {
        format!(
            r#"<?xml version="1.0"?>
            <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                              xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">{body}</bpmn:definitions>"#
        )
    }

    #[test]
    fn derives_message_payload_from_the_envelope() {
        let xml = defs(&message("orderPlaced", IN_ORDER));
        assert_eq!(
            messages(&xml),
            vec![MessageIo {
                message_name: "orderPlaced".into(),
                input_type: Some("Order".into()),
                output_type: None,
            }]
        );
    }

    #[test]
    fn emits_named_message_with_no_envelope_so_the_name_union_is_complete() {
        let xml = defs(&message("ping", ""));
        assert_eq!(
            messages(&xml),
            vec![MessageIo {
                message_name: "ping".into(),
                input_type: None,
                output_type: None,
            }]
        );
    }

    #[test]
    fn emits_a_self_closing_named_message() {
        // A hand-edited `<bpmn:message name=.. />` carries no envelope children,
        // but its name must still complete the `MessageName` union.
        let xml = defs(r#"<bpmn:message id="m" name="ping" />"#);
        assert_eq!(
            messages(&xml),
            vec![MessageIo {
                message_name: "ping".into(),
                input_type: None,
                output_type: None,
            }]
        );
    }

    #[test]
    fn skips_unnamed_messages_but_keeps_message_names_verbatim() {
        // An anonymous message (no `name`) is dropped; a named one is kept exactly
        // (message names are correlation identities, not trimmed).
        let body = format!(
            "{}{}",
            r#"<bpmn:message id="anon"><bpmn:extensionElements><zeebe:properties>"#.to_string()
                + IN_ORDER
                + r#"</zeebe:properties></bpmn:extensionElements></bpmn:message>"#,
            message(" spaced ", IN_ORDER),
        );
        let got: Vec<String> = messages(&defs(&body))
            .into_iter()
            .map(|m| m.message_name)
            .collect();
        assert_eq!(got, vec![" spaced "]);
    }

    #[test]
    fn scans_workers_and_messages_from_one_document() {
        let body = format!(
            "{}{}",
            message("orderPlaced", IN_ORDER),
            "<bpmn:process id=\"p\">".to_string()
                + &task("t", "serviceTask", "charge", OUT_RECEIPT)
                + "</bpmn:process>",
        );
        let scan = scan_bpmn(&defs(&body));
        assert_eq!(
            scan.workers,
            vec![WorkerIo {
                task_type: "charge".into(),
                input_type: None,
                output_type: Some("Receipt".into()),
                header_keys: vec![],
            }]
        );
        assert_eq!(
            scan.messages,
            vec![MessageIo {
                message_name: "orderPlaced".into(),
                input_type: Some("Order".into()),
                output_type: None,
            }]
        );
    }

    #[test]
    fn serializes_to_message_decl_shape() {
        let io = MessageIo {
            message_name: "orderPlaced".into(),
            input_type: Some("Order".into()),
            output_type: None,
        };
        let v = serde_json::to_value(&io).unwrap();
        assert_eq!(v["messageName"], "orderPlaced");
        assert_eq!(v["inputType"], "Order");
        assert!(v.get("outputType").is_none());
    }

    // --- none-start-event envelopes (process start inputs) ------------------

    /// Test helper: the start-IO map derived from a single document.
    fn starts(xml: &str) -> Vec<StartIo> {
        scan_bpmn(xml).starts
    }

    /// A `bpmn:startEvent` carrying the given extension-children body (event
    /// definitions and/or a `zeebe:properties` envelope), inside `doc`'s process.
    fn start_event(id: &str, body: &str) -> String {
        format!(r#"<bpmn:startEvent id="{id}" name="{id}">{body}</bpmn:startEvent>"#)
    }

    fn nested_start_doc(outer: &str, nested_first: bool) -> String {
        let sub = format!(
            r#"<bpmn:subProcess id="sub">{}</bpmn:subProcess>"#,
            start_event(
                "inner",
                "<bpmn:extensionElements><zeebe:properties>\
                 <zeebe:property name=\"io.nanobpm.dataEnvelope.in\" value=\"Inner\" />\
                 </zeebe:properties></bpmn:extensionElements>"
            )
        );
        if nested_first {
            doc(&format!("{sub}{outer}"))
        } else {
            doc(&format!("{outer}{sub}"))
        }
    }

    #[test]
    fn a_subprocess_start_never_supplies_the_process_start_envelope() {
        let outer_typed = start_event(
            "s",
            &format!(
                "<bpmn:extensionElements><zeebe:properties>{IN_ORDER}</zeebe:properties>\
                 </bpmn:extensionElements>"
            ),
        );
        for nested_first in [true, false] {
            let got = starts(&nested_start_doc(&outer_typed, nested_first));
            assert_eq!(
                got,
                vec![StartIo {
                    process: "p".into(),
                    input_type: Some("Order".into()),
                }],
                "nested_first={nested_first}"
            );
            // Outer start without an envelope: the nested one must not leak in.
            let got = starts(&nested_start_doc(&start_event("s", ""), nested_first));
            assert!(
                got.iter().all(|s| s.input_type.is_none()),
                "nested_first={nested_first}: {got:?}"
            );
        }
    }

    #[test]
    fn lifts_a_none_start_events_input_envelope_keyed_by_process() {
        let xml = doc(&start_event(
            "s",
            &format!(
                "<bpmn:extensionElements><zeebe:properties>{IN_ORDER}</zeebe:properties>\
                 </bpmn:extensionElements><bpmn:outgoing>f1</bpmn:outgoing>"
            ),
        ));
        assert_eq!(
            starts(&xml),
            vec![StartIo {
                process: "p".into(),
                input_type: Some("Order".into()),
            }]
        );
    }

    #[test]
    fn a_self_closing_start_event_is_a_none_start_with_no_envelope() {
        // `<startEvent/>` has no children, so it is a none start by construction;
        // it still contributes an entry (envelope-less) so a cleared envelope
        // clears the derived binding on rebuild.
        let xml = doc(r#"<bpmn:startEvent id="s" />"#);
        assert_eq!(
            starts(&xml),
            vec![StartIo {
                process: "p".into(),
                input_type: None,
            }]
        );
    }

    #[test]
    fn a_message_start_event_is_not_a_none_start() {
        // The message start's envelope stays on the referenced `bpmn:message`;
        // an `.in` property on the start event itself is not lifted.
        let body = format!(
            "{}{}",
            message("orderPlaced", IN_ORDER),
            "<bpmn:process id=\"p\">".to_string()
                + &start_event(
                    "s",
                    &format!(
                        "<bpmn:extensionElements><zeebe:properties>{OUT_RECEIPT}</zeebe:properties>\
                         </bpmn:extensionElements>\
                         <bpmn:messageEventDefinition messageRef=\"Msg_orderPlaced\" />"
                    ),
                )
                + "</bpmn:process>",
        );
        let scan = scan_bpmn(&defs(&body));
        assert!(scan.starts.is_empty());
        // …while the message itself is still scanned unchanged.
        assert_eq!(
            scan.messages,
            vec![MessageIo {
                message_name: "orderPlaced".into(),
                input_type: Some("Order".into()),
                output_type: None,
            }]
        );
    }

    /// Review of #1842: extra (vendor or inert) event definitions beside a
    /// `messageEventDefinition` do not hide the message start's envelope.
    #[test]
    fn message_start_with_extra_event_definitions_keeps_its_envelope() {
        for extra in [
            "<bpmn:terminateEventDefinition />",
            "<zeebe:customEventDefinition />",
        ] {
            let body = format!(
                "{}<bpmn:process id=\"p\">{}</bpmn:process>",
                message("orderPlaced", IN_ORDER),
                start_event(
                    "s",
                    &format!(
                        "{extra}<bpmn:messageEventDefinition messageRef=\"Msg_orderPlaced\" />"
                    ),
                ),
            );
            let scan = scan_bpmn(&defs(&body));
            assert_eq!(
                scan.start_message_envelope("s", Some("p")),
                Some("Order"),
                "{extra}"
            );
        }
    }

    #[test]
    fn a_timer_start_event_is_not_a_none_start() {
        let xml = doc(&start_event(
            "s",
            "<bpmn:timerEventDefinition><bpmn:timeCycle>R/PT1H</bpmn:timeCycle>\
             </bpmn:timerEventDefinition>",
        ));
        assert!(starts(&xml).is_empty());
    }

    #[test]
    fn a_none_start_keeps_its_envelope_alongside_documentation_and_flow_refs() {
        // `documentation`/`outgoing` children are not event definitions.
        let xml = doc(&start_event(
            "s",
            &format!(
                "<bpmn:documentation>starts it</bpmn:documentation>\
                 <bpmn:extensionElements><zeebe:properties>{IN_ORDER}</zeebe:properties>\
                 </bpmn:extensionElements><bpmn:outgoing>f1</bpmn:outgoing>"
            ),
        ));
        assert_eq!(
            starts(&xml),
            vec![StartIo {
                process: "p".into(),
                input_type: Some("Order".into()),
            }]
        );
    }

    #[test]
    fn a_start_event_outside_a_process_has_no_key_and_is_dropped() {
        let xml = defs(&start_event("s", ""));
        assert!(starts(&xml).is_empty());
    }

    #[test]
    fn multiple_none_starts_in_one_process_fold_to_one_entry() {
        let body = format!(
            "{}{}",
            start_event(
                "s1",
                &format!(
                    "<bpmn:extensionElements><zeebe:properties>{IN_ORDER}</zeebe:properties>\
                     </bpmn:extensionElements>"
                ),
            ),
            start_event("s2", ""),
        );
        assert_eq!(
            starts(&doc(&body)),
            vec![StartIo {
                process: "p".into(),
                input_type: Some("Order".into()),
            }]
        );
    }

    #[test]
    fn serializes_to_start_decl_shape() {
        let io = StartIo {
            process: "orders".into(),
            input_type: Some("Order".into()),
        };
        let v = serde_json::to_value(&io).unwrap();
        assert_eq!(v["process"], "orders");
        assert_eq!(v["inputType"], "Order");
        let bare = serde_json::to_value(&StartIo {
            process: "orders".into(),
            input_type: None,
        })
        .unwrap();
        assert!(bare.get("inputType").is_none());
    }

    // --- composed motion shapes (ADR 0040 §9/§10) --------------------------

    /// Test helper: the composed shapes derived from a single document.
    fn shapes(xml: &str) -> Vec<ShapeDecl> {
        scan_bpmn(xml).shapes
    }

    /// Wrap shape declarations in a `nano:shapes` container on the process's
    /// extension elements, in a process document with the nano namespace declared.
    fn shape_doc(shapes_body: &str) -> String {
        format!(
            r#"<?xml version="1.0"?>
            <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                              xmlns:nano="https://nanobpm.io/schema/shapes/1.0">
              <bpmn:process id="orders" isExecutable="true">
                <bpmn:extensionElements>
                  <nano:shapes>{shapes_body}</nano:shapes>
                </bpmn:extensionElements>
              </bpmn:process>
            </bpmn:definitions>"#
        )
    }

    #[test]
    fn scans_the_four_op_algebra_in_author_order() {
        let xml = shape_doc(
            r#"<nano:shape id="ApprovedOrder" name="Approved order">
                 <nano:carry ref="Order" />
                 <nano:project ref="Customer" fields="tier, region" via="Order.customerId" />
                 <nano:extend name="approved" type="boolean" />
                 <nano:extend name="reviewedBy" type="string" optional="true" description=" Who approved. " />
                 <nano:reference name="lines" ref="OrderLine" spread="false" list="true" />
               </nano:shape>"#,
        );
        assert_eq!(
            shapes(&xml),
            vec![ShapeDecl {
                id: "ApprovedOrder".into(),
                name: Some("Approved order".into()),
                process: Some("orders".into()),
                ops: vec![
                    ShapeOp::Carry {
                        r#ref: "Order".into()
                    },
                    ShapeOp::Project {
                        r#ref: "Customer".into(),
                        fields: vec!["tier".into(), "region".into()],
                        via: Some("Order.customerId".into()),
                    },
                    ShapeOp::Extend {
                        name: "approved".into(),
                        r#type: "boolean".into(),
                        optional: None,
                        list: None,
                        description: None,
                    },
                    ShapeOp::Extend {
                        name: "reviewedBy".into(),
                        r#type: "string".into(),
                        optional: Some(true),
                        list: None,
                        description: Some("Who approved.".into()),
                    },
                    ShapeOp::Reference {
                        name: "lines".into(),
                        r#ref: "OrderLine".into(),
                        spread: Some(false),
                        list: Some(true),
                        description: None,
                    },
                ],
            }]
        );
    }

    #[test]
    fn scans_multiple_shapes_and_tags_their_process() {
        let xml = shape_doc(
            r#"<nano:shape id="A"><nano:carry ref="Order" /></nano:shape>
               <nano:shape id="B"><nano:carry ref="A" /></nano:shape>"#,
        );
        let got = shapes(&xml);
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].id, "A");
        assert_eq!(got[1].id, "B");
        assert!(got.iter().all(|s| s.process.as_deref() == Some("orders")));
    }

    #[test]
    fn drops_malformed_ops_and_unidentified_shapes() {
        // A carry with no `ref` and an extend missing its `type` are malformed and
        // dropped; a shape with no `id` cannot be referenced, so it is dropped too.
        let xml = shape_doc(
            r#"<nano:shape id="Ok">
                 <nano:carry />
                 <nano:extend name="approved" />
                 <nano:carry ref="Order" />
               </nano:shape>
               <nano:shape><nano:carry ref="Order" /></nano:shape>"#,
        );
        assert_eq!(
            shapes(&xml),
            vec![ShapeDecl {
                id: "Ok".into(),
                name: None,
                process: Some("orders".into()),
                ops: vec![ShapeOp::Carry {
                    r#ref: "Order".into()
                }],
            }]
        );
    }

    #[test]
    fn drops_a_project_with_no_field_names_and_trims_ids() {
        // A project with an empty/whitespace `fields` list is a silent no-op, so it
        // is dropped; a shape id is trimmed and a whitespace-only id is unusable.
        let xml = shape_doc(
            r#"<nano:shape id="  Trimmed  ">
                 <nano:project ref="Customer" fields="  " />
                 <nano:project ref="Customer" fields="tier" via="Order.customerId" />
               </nano:shape>
               <nano:shape id="   "><nano:carry ref="Order" /></nano:shape>"#,
        );
        assert_eq!(
            shapes(&xml),
            vec![ShapeDecl {
                id: "Trimmed".into(),
                name: None,
                process: Some("orders".into()),
                ops: vec![ShapeOp::Project {
                    r#ref: "Customer".into(),
                    fields: vec!["tier".into()],
                    via: Some("Order.customerId".into()),
                }],
            }]
        );
    }

    #[test]
    fn scans_a_self_closing_empty_shape() {
        let xml = shape_doc(r#"<nano:shape id="Empty" />"#);
        assert_eq!(
            shapes(&xml),
            vec![ShapeDecl {
                id: "Empty".into(),
                name: None,
                process: Some("orders".into()),
                ops: vec![],
            }]
        );
    }

    #[test]
    fn serializes_ops_to_the_tagged_union_shape() {
        let decl = ShapeDecl {
            id: "ApprovedOrder".into(),
            name: None,
            process: Some("orders".into()),
            ops: vec![
                ShapeOp::Carry {
                    r#ref: "Order".into(),
                },
                ShapeOp::Extend {
                    name: "approved".into(),
                    r#type: "boolean".into(),
                    optional: None,
                    list: None,
                    description: None,
                },
            ],
        };
        let v = serde_json::to_value(&decl).unwrap();
        assert_eq!(v["id"], "ApprovedOrder");
        assert_eq!(v["ops"][0]["op"], "carry");
        assert_eq!(v["ops"][0]["ref"], "Order");
        assert_eq!(v["ops"][1]["op"], "extend");
        assert_eq!(v["ops"][1]["name"], "approved");
        assert_eq!(v["ops"][1]["type"], "boolean");
        // Absent optional/list flags are omitted, not serialized as null/false.
        assert!(v["ops"][1].get("optional").is_none());
        assert!(v["ops"][1].get("list").is_none());
    }

    #[test]
    fn shapes_and_workers_coexist_in_one_document() {
        // A process carrying both a service task and a shapes container yields both.
        let xml = format!(
            r#"<?xml version="1.0"?>
            <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                              xmlns:zeebe="http://camunda.org/schema/zeebe/1.0"
                              xmlns:nano="https://nanobpm.io/schema/shapes/1.0">
              <bpmn:process id="orders" isExecutable="true">
                <bpmn:extensionElements>
                  <nano:shapes><nano:shape id="S"><nano:carry ref="Order" /></nano:shape></nano:shapes>
                </bpmn:extensionElements>
                {}
              </bpmn:process>
            </bpmn:definitions>"#,
            task("t", "serviceTask", "charge", IN_ORDER),
        );
        let scan = scan_bpmn(&xml);
        assert_eq!(scan.workers.len(), 1);
        assert_eq!(scan.workers[0].task_type, "charge");
        assert_eq!(scan.shapes.len(), 1);
        assert_eq!(scan.shapes[0].id, "S");
    }

    #[test]
    fn scans_model_level_meta_siblings_tagged_with_the_process() {
        // Two `nano:meta` siblings of the shapes container are lifted, in document
        // order, each tagged with the enclosing process id (ADR 0040 §5).
        let xml = r#"<?xml version="1.0"?>
            <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                              xmlns:nano="https://nanobpm.io/schema/shapes/1.0">
              <bpmn:process id="orders" isExecutable="true">
                <bpmn:extensionElements>
                  <nano:shapes><nano:shape id="S"><nano:carry ref="Order" /></nano:shape></nano:shapes>
                  <nano:meta key="classification" value="internal" />
                  <nano:meta key="owner" value="ops" />
                </bpmn:extensionElements>
              </bpmn:process>
            </bpmn:definitions>"#;
        let scan = scan_bpmn(xml);
        assert_eq!(
            scan.meta,
            vec![
                MetaDecl {
                    process: Some("orders".into()),
                    key: "classification".to_string(),
                    value: "internal".to_string(),
                },
                MetaDecl {
                    process: Some("orders".into()),
                    key: "owner".to_string(),
                    value: "ops".to_string(),
                },
            ]
        );
        // The shape still resolves, and its `extend` op is not mistaken for meta.
        assert_eq!(scan.shapes.len(), 1);
    }

    #[test]
    fn an_extend_op_inside_a_shape_is_not_scanned_as_model_meta() {
        // `nano:extend` shares no vocabulary with `nano:meta`; an extend inside a
        // shape must never leak into the model-level meta list.
        let xml = shape_doc(
            r#"<nano:shape id="S">
                 <nano:carry ref="Order" />
                 <nano:extend name="approved" type="boolean" />
               </nano:shape>"#,
        );
        let scan = scan_bpmn(&xml);
        assert!(scan.meta.is_empty());
        assert_eq!(scan.shapes.len(), 1);
    }

    #[test]
    fn scan_project_matches_bpmn_extension_case_insensitively() {
        // `is_model_resource` triggers regeneration on any `.bpmn`/`.BPMN` save
        // (case-insensitive), so the project scanner must accept the same set of
        // files — otherwise an uppercase `.BPMN` model would regenerate against an
        // empty scan and silently drift the derived worker/header types.
        static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "nano-scan-case-test-{}-{}",
            std::process::id(),
            N.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let processes = root.join("resources").join("processes");
        std::fs::create_dir_all(&processes).unwrap();
        let xml = doc(&task(
            "t1",
            "serviceTask",
            "charge",
            &format!("{IN_ORDER}{OUT_RECEIPT}"),
        ));
        std::fs::write(processes.join("order.BPMN"), &xml).unwrap();

        let scan = scan_project(&root);
        let _ = std::fs::remove_dir_all(&root);

        assert_eq!(
            scan.workers.len(),
            1,
            "uppercase .BPMN model must be scanned"
        );
        assert_eq!(scan.workers[0].task_type, "charge");
    }

    fn temp_project(tag: &str) -> std::path::PathBuf {
        static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "nano-scan-{tag}-{}-{}",
            std::process::id(),
            N.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).unwrap();
        root
    }

    fn write_model(root: &Path, rel: &str, task_type: &str) {
        let path = root.join(rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let xml = doc(&task("t1", "serviceTask", task_type, IN_ORDER));
        std::fs::write(path, xml).unwrap();
    }

    fn task_types(scan: &BpmnScan) -> Vec<String> {
        scan.workers.iter().map(|w| w.task_type.clone()).collect()
    }

    #[test]
    fn scan_project_default_globs_cover_root_level_and_nested_resources() {
        // Regression #1751: the default convention is `resources/**/*.bpmn`, not
        // only immediate `resources/processes/*.bpmn`.
        let root = temp_project("default");
        write_model(&root, "resources/root.bpmn", "root");
        write_model(&root, "resources/processes/flat.bpmn", "flat");
        write_model(&root, "resources/processes/deep/nested.bpmn", "nested");
        write_model(&root, "outside.bpmn", "outside");
        write_model(&root, "resources/node_modules/dep.bpmn", "dep");
        let scan = scan_project(&root);
        let _ = std::fs::remove_dir_all(&root);
        assert_eq!(task_types(&scan), vec!["flat", "nested", "root"]);
    }

    #[test]
    fn scan_project_honours_custom_manifest_globs() {
        let root = temp_project("custom");
        std::fs::write(
            root.join(MANIFEST_FILE),
            r#"{"models":{"processes":["models/**/*.bpmn","extra/one.bpmn"]}}"#,
        )
        .unwrap();
        write_model(&root, "models/a.bpmn", "a");
        write_model(&root, "models/x/b.bpmn", "b");
        write_model(&root, "extra/one.bpmn", "one");
        write_model(&root, "extra/two.bpmn", "two");
        write_model(&root, "resources/processes/ignored.bpmn", "ignored");
        let scan = scan_project(&root);
        let _ = std::fs::remove_dir_all(&root);
        assert_eq!(task_types(&scan), vec!["a", "b", "one"]);
    }

    #[test]
    fn trigger_and_scan_share_one_resolver() {
        // Defect class: the save trigger and the scan drifting apart. Every file
        // the scan resolves must satisfy the trigger, and vice versa.
        let root = temp_project("parity");
        for rel in [
            "resources/a.bpmn",
            "resources/processes/b.BPMN",
            "resources/x/y/c.bpmn",
            "other/d.bpmn",
            "e.bpmn",
        ] {
            write_model(&root, rel, "t");
        }
        let resolved: Vec<String> = resolve_process_models(&root)
            .iter()
            .map(|p| {
                p.strip_prefix(&root)
                    .unwrap()
                    .to_string_lossy()
                    .replace('\\', "/")
            })
            .collect();
        for rel in [
            "resources/a.bpmn",
            "resources/processes/b.BPMN",
            "resources/x/y/c.bpmn",
            "other/d.bpmn",
            "e.bpmn",
        ] {
            assert_eq!(
                resolved.contains(&rel.to_string()),
                is_process_model_path(&root, rel),
                "{rel}"
            );
        }
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn manifest_globs_cannot_escape_the_project() {
        let parent = temp_project("escape");
        let root = parent.join("proj");
        write_model(&parent, "sibling/s.bpmn", "sibling");
        write_model(&root, "models/in.bpmn", "in");
        std::fs::write(
            root.join(MANIFEST_FILE),
            r#"{"models":{"processes":["../sibling/**/*.bpmn","/etc/*.bpmn","C:/x/*.bpmn","models/../../sibling/*.bpmn","models/**/*.bpmn"]}}"#,
        )
        .unwrap();
        assert_eq!(process_globs(&root), vec!["models/**/*.bpmn"]);
        let scan = scan_project(&root);
        assert!(!is_process_model_path(&root, "../sibling/s.bpmn"));
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(
                parent.join("sibling/s.bpmn"),
                root.join("models/link.bpmn"),
            )
            .unwrap();
            assert!(!is_process_model_path(&root, "models/link.bpmn"));
            assert_eq!(task_types(&scan_project(&root)), vec!["in"]);
        }
        let _ = std::fs::remove_dir_all(&parent);
        assert_eq!(task_types(&scan), vec!["in"]);
    }

    #[test]
    fn explicit_empty_processes_is_honoured() {
        let root = temp_project("empty");
        std::fs::write(root.join(MANIFEST_FILE), r#"{"models":{"processes":[]}}"#).unwrap();
        write_model(&root, "resources/a.bpmn", "a");
        assert!(process_globs(&root).is_empty());
        assert!(!is_process_model_path(&root, "resources/a.bpmn"));
        let scan = scan_project(&root);
        let _ = std::fs::remove_dir_all(&root);
        assert!(scan.workers.is_empty());
    }

    #[test]
    fn literal_prefix_respects_exclusions_case_and_symlinks() {
        let root = temp_project("prefix");
        std::fs::write(
            root.join(MANIFEST_FILE),
            r#"{"models":{"processes":["models/**/*.bpmn","models/target/**/*.bpmn","models/target/one.bpmn","models/link/**/*.bpmn"]}}"#,
        )
        .unwrap();
        write_model(&root, "models/a.bpmn", "a");
        write_model(&root, "models/target/t.bpmn", "t");
        write_model(&root, "models/target/one.bpmn", "one");
        write_model(&root, "real/r.bpmn", "r");
        #[cfg(unix)]
        std::os::unix::fs::symlink(root.join("real"), root.join("models/link")).unwrap();
        let resolved = rels(&root);
        let mut cases = vec![
            "models/a.bpmn",
            "models/target/t.bpmn",
            "models/target/one.bpmn",
            "Models/upper.bpmn",
        ];
        #[cfg(unix)]
        cases.push("models/link/r.bpmn");
        for rel in cases {
            assert_eq!(
                resolved.contains(&rel.to_string()),
                is_process_model_path(&root, rel),
                "{rel}"
            );
        }
        let scan = scan_project(&root);
        let _ = std::fs::remove_dir_all(&root);
        assert_eq!(task_types(&scan), vec!["a"]);
    }

    #[test]
    fn trigger_ignores_excluded_dirs_like_the_scan() {
        let root = temp_project("excluded");
        write_model(&root, "resources/node_modules/dep.bpmn", "dep");
        write_model(&root, "resources/x/.git/g.bpmn", "g");
        assert!(rels(&root).is_empty());
        assert!(!is_process_model_path(
            &root,
            "resources/node_modules/dep.bpmn"
        ));
        assert!(!is_process_model_path(&root, "resources/x/.git/g.bpmn"));
        let _ = std::fs::remove_dir_all(&root);
    }

    fn rels(root: &Path) -> Vec<String> {
        resolve_process_models(root)
            .iter()
            .map(|p| {
                p.strip_prefix(root)
                    .unwrap()
                    .to_string_lossy()
                    .replace('\\', "/")
            })
            .collect()
    }

    #[test]
    fn glob_match_semantics() {
        assert!(glob_match("resources/**/*.bpmn", "resources/a.bpmn"));
        assert!(glob_match("resources/**/*.bpmn", "resources/x/y/a.bpmn"));
        assert!(!glob_match("resources/**/*.bpmn", "res/a.bpmn"));
        assert!(glob_match("*.bpmn", "a.BPMN"));
        assert!(!glob_match("*.bpmn", "x/a.bpmn"));
        assert!(glob_match("m/?.bpmn", "m/a.bpmn"));
        assert!(glob_match("**", "any/thing"));
        assert!(!glob_match("models/*.bpmn", "Models/a.bpmn"));
        assert!(glob_match("models/*.bpmn", "models/A.BPMN"));
        assert!(glob_match("**/A.bpmn", "a.BPMN"));
        assert!(glob_match("**/a/b.bpmn", "x/a/B.BPMN"));
        assert!(!glob_match("**/a/b.bpmn", "x/A/b.bpmn"));
    }

    #[test]
    fn glob_match_polynomial_on_repeated_wildcards() {
        // Adversarial non-matches that trigger exponential backtracking in a
        // recursive `*` / `**` matcher.
        let start = std::time::Instant::now();
        let seg_glob = "*a*a*a*a*a*a*a*a*a*aZ.bpmn";
        let seg_name = format!("{}.bpmn", "a".repeat(32));
        assert!(!glob_match(seg_glob, &seg_name));

        let path_glob = "**/a/**/a/**/a/**/a/**/a/**/a/**/a/**/a/**/z.bpmn";
        let path_name = format!("{}/x.bpmn", vec!["a"; 24].join("/"));
        assert!(!glob_match(path_glob, &path_name));
        assert!(
            start.elapsed() < std::time::Duration::from_secs(1),
            "glob matching took {:?} (expected polynomial)",
            start.elapsed()
        );
    }

    #[test]
    fn literal_glob_lists_parent_without_recursing_subtrees() {
        let root = temp_project("literal-nonrec");
        std::fs::write(
            root.join(MANIFEST_FILE),
            r#"{"models":{"processes":["order.bpmn","sub/nested.bpmn","sub/target/skip.bpmn"]}}"#,
        )
        .unwrap();
        // Case-insensitive filename match at project root and in a subdir.
        write_model(&root, "Order.BPMN", "root-order");
        write_model(&root, "sub/NESTED.bpmn", "sub-nested");
        write_model(&root, "sub/target/skip.bpmn", "skip");
        // Unrelated nested file that a non-recursive listing should never touch.
        write_model(&root, "unrelated/deep/other.bpmn", "other");
        let resolved = rels(&root);
        assert_eq!(resolved, vec!["Order.BPMN", "sub/NESTED.bpmn"]);
        for rel in [
            "Order.BPMN",
            "sub/NESTED.bpmn",
            "sub/target/skip.bpmn",
            "unrelated/deep/other.bpmn",
        ] {
            assert_eq!(
                resolved.contains(&rel.to_string()),
                is_process_model_path(&root, rel),
                "{rel}"
            );
        }
        // Verify a literal glob does not even require scanning subdirectories.
        let _ = std::fs::remove_dir_all(&root);
    }
}
