//! Derive a JSON Schema from a composed motion shape (`nano:shape`, ADR 0040
//! §9) — the single canonical shape → JSON-Schema mapping (issue #1738).
//!
//! The `/mcp/processes` server uses it to advertise (and validate) a process
//! tool's `inputSchema` from the data envelope its start `bpmn:message` carries
//! (`io.nanobpm.dataEnvelope.in` → a `nano:shape` id), so a tool author declares
//! the contract once, in the model, instead of again as flat
//! `io.camunda.tool:input_N_*` properties.
//!
//! The mapping folds the shape's ops in author order (a later op re-declaring a
//! field name wins, as in the reifier):
//!
//! A ref that names no declared shape is looked up in the caller's
//! [`TypeRegistry`] (the fused domain types); a ref neither resolves is
//! reported in [`EnvelopeSchema::unresolved`] (issue #1748) — never silently
//! treated as complete — and an unresolved **root** envelope is the hard
//! error [`SchemaError::UnresolvedEnvelope`].
//!
//! - `extend name type [optional] [list] [description]` — a scalar keyword maps
//!   to its JSON-Schema type (see [`scalar_schema`]); a type naming another
//!   shape (or registry type) nests its object schema; an unresolved type is
//!   left unconstrained (and reported). Required unless `optional="true"`.
//! - `reference name ref [spread] [list] [description]` — nests the referenced
//!   type as an object (an array of them with `list`), or inlines its fields
//!   with `spread`. Required.
//! - `carry ref` — spreads a referenced type's fields; an unresolved type
//!   contributes nothing (and is reported).
//! - `project ref fields` — picks the named fields of a referenced type (a
//!   field the resolved type lacks is an invalid projection and fails the
//!   derivation), or declares them unconstrained (and optional, reported) for
//!   an unresolved type.
//!
//! A reference cycle resolves to an unconstrained object at the point it
//! closes, so derivation always terminates.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};

use serde_json::{Map, Value, json};

use crate::{BpmnScan, ShapeDecl, ShapeOp};

/// The JSON Schema for a scalar `extend` type keyword (the console's
/// `SCALAR_KEYWORDS`, `console/src/lib/shapeComposer.ts`), or `None` when the
/// keyword is not a scalar (a nominal reference to another type).
pub fn scalar_schema(keyword: &str) -> Option<Value> {
    Some(match keyword {
        "string" => json!({ "type": "string" }),
        "number" => json!({ "type": "number" }),
        "integer" => json!({ "type": "integer" }),
        "boolean" => json!({ "type": "boolean" }),
        "date" => json!({ "type": "string", "format": "date" }),
        "datetime" => json!({ "type": "string", "format": "date-time" }),
        // Arbitrary JSON: unconstrained.
        "json" => json!({}),
        _ => return None,
    })
}

/// Why a resolvable data envelope's schema could not be derived. A caller must
/// treat this as a hard failure for the envelope (never as "no envelope"):
/// falling back to a weaker contract would let calls that omit envelope fields
/// through.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaError {
    /// The expansion exceeded [`MAX_EXPANDED_OPS`] or [`MAX_EXPANDED_BYTES`].
    TooLarge,
    /// The reference nesting exceeded [`MAX_DEPTH`].
    TooDeep,
    /// A `project` names a field its resolved (locally declared) shape does
    /// not have: an invalid projection, rejected rather than weakened to an
    /// unconstrained optional field.
    UnknownProjectedField,
    /// A `project` targets a locally declared shape that is already being
    /// expanded (a reference cycle): its fields cannot be resolved, so the
    /// projection is rejected rather than weakened to unconstrained fields.
    CyclicProjection,
    /// The envelope ref names neither a `nano:shape` declared in the model nor
    /// a type of the registry: the contract is unknown, so calls are rejected
    /// rather than validated against a weaker (legacy) schema.
    UnresolvedEnvelope,
    /// A registry type's schema lies outside the supported subset (an object
    /// schema of only `type`/`properties`/`required` plus annotations, with no
    /// `$ref` anywhere): copying only its properties would drop definitions
    /// (`$defs`, leaving dangling `$ref`s) or object-level constraints
    /// (`additionalProperties`, …), silently weakening the contract.
    UnsupportedRegistrySchema,
}

impl std::fmt::Display for SchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooLarge => write!(
                f,
                "its expanded schema exceeds {MAX_EXPANDED_OPS} shape ops or \
                 {MAX_EXPANDED_BYTES} bytes"
            ),
            Self::TooDeep => write!(f, "its shape references nest deeper than {MAX_DEPTH}"),
            Self::UnknownProjectedField => write!(
                f,
                "it projects a field that the referenced shape does not declare"
            ),
            Self::CyclicProjection => write!(
                f,
                "it projects fields from a shape that is part of a reference cycle"
            ),
            Self::UnsupportedRegistrySchema => write!(
                f,
                "a registry type's schema uses keywords outside the supported subset \
                 (`type: object`, `properties`, `required`, annotations; no `$ref`)"
            ),
            Self::UnresolvedEnvelope => write!(
                f,
                "it is declared neither as a shape in the model nor in the type registry"
            ),
        }
    }
}

impl std::error::Error for SchemaError {}

/// A type registry the derivation consults for refs the model does not
/// declare as a `nano:shape` — the fused domain types (`carry`, `project`,
/// `extend`/`reference` to a domain entity). Returns the type's object JSON
/// Schema (`{type: "object", properties, required?}`), or `None` when the
/// registry does not know it (the ref is then reported as unresolved).
pub trait TypeRegistry {
    fn object_schema(&self, type_id: &str) -> Option<Value>;
}

/// No registry: only model-declared shapes resolve (every external ref is
/// reported in [`EnvelopeSchema::unresolved`]).
impl TypeRegistry for () {
    fn object_schema(&self, _: &str) -> Option<Value> {
        None
    }
}

impl TypeRegistry for HashMap<String, Value> {
    fn object_schema(&self, type_id: &str) -> Option<Value> {
        self.get(type_id).cloned()
    }
}

/// A derived data-envelope contract.
#[derive(Debug, Clone, PartialEq)]
pub struct EnvelopeSchema {
    /// The envelope's shape id.
    pub shape: String,
    /// The object JSON Schema.
    pub schema: Value,
    /// Unresolved-contract diagnostics: every ref neither the model nor the
    /// registry resolves, whose part of the contract is therefore NOT
    /// enforced by `schema`. Empty = the schema is the complete contract.
    pub unresolved: Vec<String>,
}

/// The data envelope a **message start event** is typed by (issue #1748),
/// resolved from one scanned BPMN document: the start event's `messageRef` is
/// followed to the `bpmn:message` **id** (never matched by subscription
/// message name, which may be evaluated, fall back to the message id, or be
/// shared by several messages), and that message's
/// `io.nanobpm.dataEnvelope.in` ref is derived with [`envelope_schema`].
///
/// `Ok(None)` when the start event's message declares no envelope;
/// `Err((shape_id, e))` when it declares one whose schema cannot be derived —
/// including [`SchemaError::UnresolvedEnvelope`] for a ref neither the model
/// nor `registry` declares (never a silent fallback to a weaker contract).
pub fn start_event_input_schema(
    scan: &BpmnScan,
    start_element_id: &str,
    process_id: Option<&str>,
    registry: &dyn TypeRegistry,
) -> Result<Option<EnvelopeSchema>, (String, SchemaError)> {
    let Some(shape_id) = scan.start_message_envelope(start_element_id, process_id) else {
        return Ok(None);
    };
    envelope_schema(&scan.shapes, shape_id, process_id, registry)
        .map(Some)
        .map_err(|e| (shape_id.to_string(), e))
}

/// The object JSON Schema of envelope `shape_id` with its unresolved-contract
/// diagnostics, or an error when the expansion exceeds the derivation limits
/// or the root ref resolves to neither a declared shape nor a registry type
/// (an incomplete schema is never returned). `process_id` prefers a shape
/// declared on that process when several processes declare the same id.
pub fn envelope_schema(
    shapes: &[ShapeDecl],
    shape_id: &str,
    process_id: Option<&str>,
    registry: &dyn TypeRegistry,
) -> Result<EnvelopeSchema, SchemaError> {
    // Preselect one declaration per id, once: the first declared on
    // `process_id`, else the first declared at all.
    let mut index: HashMap<&str, &ShapeDecl> = HashMap::new();
    for s in shapes {
        let on_process = s.process.as_deref() == process_id;
        index
            .entry(s.id.as_str())
            .and_modify(|cur| {
                if on_process && cur.process.as_deref() != process_id {
                    *cur = s;
                }
            })
            .or_insert(s);
    }
    let resolver = Resolver {
        index,
        registry,
        ops: Cell::new(MAX_EXPANDED_OPS),
        bytes: Cell::new(MAX_EXPANDED_BYTES),
        error: Cell::new(None),
        unresolved: RefCell::new(Vec::new()),
    };
    let mut stack = Vec::new();
    let obj = match resolver.find(shape_id) {
        Some(shape) => resolver.object(shape, &mut stack),
        None => match resolver.external(shape_id, 0) {
            Some(obj) => obj,
            // A registry root that hit a limit is not "unresolved".
            None => {
                return Err(resolver
                    .error
                    .get()
                    .unwrap_or(SchemaError::UnresolvedEnvelope));
            }
        },
    };
    if let Some(e) = resolver.error.get() {
        return Err(e);
    }
    Ok(EnvelopeSchema {
        shape: shape_id.to_string(),
        schema: obj.into_schema(),
        unresolved: resolver.unresolved.into_inner(),
    })
}

/// An object schema under construction: ordered properties plus the
/// (unique, declaration-ordered) required names. Required membership is a
/// hash index (`required`) over an append-only order log (`required_order`),
/// so every insertion and membership check is O(1) rather than a linear scan.
#[derive(Default)]
struct Obj {
    properties: Map<String, Value>,
    required: HashSet<String>,
    /// Every time a name became required, in order; stale/duplicate entries
    /// are dropped in [`Obj::required_names`] (the last occurrence wins).
    required_order: Vec<String>,
}

impl Obj {
    fn set(&mut self, name: &str, schema: Value, required: bool) {
        self.properties.insert(name.to_string(), schema);
        if required {
            self.required.insert(name.to_string());
            self.required_order.push(name.to_string());
        } else {
            self.required.remove(name);
        }
    }

    fn is_required(&self, name: &str) -> bool {
        self.required.contains(name)
    }

    fn spread(&mut self, other: Obj) {
        for (name, schema) in other.properties {
            let required = other.required.contains(&name);
            self.set(&name, schema, required);
        }
    }

    /// The unique required names, ordered by when each last became required.
    fn required_names(&self) -> Vec<&str> {
        let mut seen = HashSet::new();
        let mut names: Vec<&str> = self
            .required_order
            .iter()
            .rev()
            .filter(|n| self.required.contains(*n) && seen.insert(n.as_str()))
            .map(String::as_str)
            .collect();
        names.reverse();
        names
    }

    /// An object from an object JSON Schema (`properties` + `required`); a
    /// non-object schema yields an empty object.
    fn from_schema(schema: Value) -> Self {
        let mut obj = Obj::default();
        let required: HashSet<String> = schema
            .get("required")
            .and_then(Value::as_array)
            .map(|r| {
                r.iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        let props = schema.get("properties").and_then(Value::as_object);
        if let Some(props) = props {
            for (name, s) in props {
                obj.set(name, s.clone(), required.contains(name));
            }
        }
        // A required name absent from `properties` is still required: keep it
        // as an unconstrained (`{}`) property rather than silently dropping it.
        if let Some(Value::Array(names)) = schema.get("required") {
            for name in names.iter().filter_map(Value::as_str) {
                if !props.is_some_and(|p| p.contains_key(name)) {
                    obj.set(name, json!({}), true);
                }
            }
        }
        obj
    }

    fn into_schema(self) -> Value {
        let required = self.required_names();
        let mut v = json!({ "type": "object", "properties": self.properties });
        if !required.is_empty() {
            v["required"] = json!(required);
        }
        v
    }
}

/// The most shape ops a single derivation may expand (counting every op of
/// every nested occurrence and every projected field), bounding the work for
/// acyclic graphs whose shared references expand exponentially.
pub const MAX_EXPANDED_OPS: usize = 10_000;

/// The most bytes (field names, descriptions and type keywords of every
/// expanded occurrence, plus the size of every projected copy) a single
/// derivation may produce, bounding the schema's memory independently of the
/// op count (a few ops can carry very long descriptions).
pub const MAX_EXPANDED_BYTES: usize = 1 << 20;

/// The deepest chain of nested shape references a derivation may follow,
/// bounding recursion here and in later cloning / serialization / validation
/// of the schema.
pub const MAX_DEPTH: usize = 32;

struct Resolver<'a> {
    /// The preferred declaration per shape id, selected once per derivation
    /// so a reference lookup is O(1) in the catalog size (duplicates too).
    index: HashMap<&'a str, &'a ShapeDecl>,
    /// Fallback for refs the model does not declare.
    registry: &'a dyn TypeRegistry,
    /// Remaining op-expansion budget.
    ops: Cell<usize>,
    /// Remaining byte budget.
    bytes: Cell<usize>,
    /// The first limit hit; once set, expansion stops.
    error: Cell<Option<SchemaError>>,
    /// Unresolved-contract diagnostics, in encounter order (deduplicated).
    unresolved: RefCell<Vec<String>>,
}

impl<'a> Resolver<'a> {
    fn find(&self, id: &str) -> Option<&'a ShapeDecl> {
        self.index.get(id).copied()
    }

    /// A registry type's object, charged against the byte budget; `None`
    /// when the registry does not know it (or a limit is hit).
    /// `depth` is the enclosing shape-reference nesting: a registry schema's
    /// own nesting is checked (iteratively) against what is left of
    /// [`MAX_DEPTH`] before any recursive sizing or copying.
    fn external(&self, id: &str, depth: usize) -> Option<Obj> {
        let schema = self.registry.object_schema(id)?;
        if schema_too_deep(&schema, MAX_DEPTH.saturating_sub(depth)) {
            self.fail(SchemaError::TooDeep);
            return None;
        }
        if !registry_schema_supported(&schema) {
            self.fail(SchemaError::UnsupportedRegistrySchema);
            return None;
        }
        if !self.charge(value_size(&schema)) {
            return None;
        }
        Some(Obj::from_schema(schema))
    }

    /// Record that `op` on `id` is not enforced because `id` resolves to
    /// neither a declared shape nor a registry type.
    fn report(&self, op: &str, id: &str) {
        let msg = format!(
            "`{op}` of `{id}`: not declared as a shape in the model nor in the type \
             registry, so its part of the contract is not enforced"
        );
        let mut u = self.unresolved.borrow_mut();
        if !u.contains(&msg) {
            u.push(msg);
        }
    }

    /// Resolve `id` as a declared shape, else as a registry type, else
    /// report it as unresolved (`None`). A declared shape that does not
    /// resolve (cycle / limit) is not reported: it is not an external ref.
    fn resolve_any(&self, op: &str, id: &str, stack: &mut Vec<String>) -> Option<Obj> {
        if self.find(id).is_some() {
            return self.resolve(id, stack);
        }
        if self.error.get().is_some() {
            return None;
        }
        let found = self.external(id, stack.len());
        if found.is_none() && self.error.get().is_none() {
            self.report(op, id);
        }
        found
    }

    fn fail(&self, e: SchemaError) {
        if self.error.get().is_none() {
            self.error.set(Some(e));
        }
    }

    /// Charge one op of `bytes` bytes, before allocating it; `false` (and
    /// the error recorded) once a budget is exhausted.
    fn charge(&self, bytes: usize) -> bool {
        if self.error.get().is_some() {
            return false;
        }
        let (ops, left) = (self.ops.get(), self.bytes.get());
        if ops == 0 || left < bytes {
            self.fail(SchemaError::TooLarge);
            return false;
        }
        self.ops.set(ops - 1);
        self.bytes.set(left - bytes);
        true
    }

    /// Resolve a shape's object, or `None` when it is unknown, would close a
    /// reference cycle, or exceeds a limit (recorded in `error`).
    fn resolve(&self, id: &str, stack: &mut Vec<String>) -> Option<Obj> {
        if self.error.get().is_some() || stack.iter().any(|s| s == id) {
            return None;
        }
        let shape = self.find(id)?;
        if stack.len() >= MAX_DEPTH {
            self.fail(SchemaError::TooDeep);
            return None;
        }
        Some(self.object(shape, stack))
    }

    fn object(&self, shape: &ShapeDecl, stack: &mut Vec<String>) -> Obj {
        stack.push(shape.id.clone());
        let mut obj = Obj::default();
        for op in &shape.ops {
            let cost = match op {
                ShapeOp::Carry { r#ref } => r#ref.len(),
                ShapeOp::Project { r#ref, .. } => r#ref.len(),
                ShapeOp::Extend {
                    name,
                    r#type,
                    description,
                    ..
                } => name.len() + r#type.len() + description.as_ref().map_or(0, String::len),
                ShapeOp::Reference {
                    name,
                    r#ref,
                    description,
                    ..
                } => name.len() + r#ref.len() + description.as_ref().map_or(0, String::len),
            };
            if !self.charge(cost) {
                break;
            }
            match op {
                ShapeOp::Carry { r#ref } => {
                    if let Some(inner) = self.resolve_any("carry", r#ref, stack) {
                        obj.spread(inner);
                    }
                }
                ShapeOp::Project { r#ref, fields, .. } => {
                    let inner = self.resolve_any("project", r#ref, stack);
                    // A local shape that did not resolve without a recorded
                    // limit error closes a cycle: reject, never pass through.
                    if inner.is_none()
                        && self.error.get().is_none()
                        && stack.iter().any(|s| s == r#ref)
                    {
                        self.fail(SchemaError::CyclicProjection);
                        break;
                    }
                    for field in fields {
                        let found = inner.as_ref().and_then(|o| {
                            o.properties.get(field).map(|s| (s, o.is_required(field)))
                        });
                        // Every projected copy is charged (op + its size)
                        // before it is cloned, so repeated projections of a
                        // large field cannot amplify memory.
                        let size = field.len() + found.map_or(0, |(s, _)| value_size(s));
                        if !self.charge(size) {
                            break;
                        }
                        match (found, inner.is_some()) {
                            (Some((schema, required)), _) => {
                                obj.set(field, schema.clone(), required)
                            }
                            // A resolved local shape without the field: the
                            // projection is invalid; never weaken the contract.
                            (None, true) => {
                                self.fail(SchemaError::UnknownProjectedField);
                                break;
                            }
                            // An unresolved type (reported): documented
                            // pass-through.
                            (None, false) => obj.set(field, json!({}), false),
                        }
                    }
                }
                ShapeOp::Extend {
                    name,
                    r#type,
                    optional,
                    list,
                    description,
                } => {
                    // A type naming a declared shape is an object even where
                    // it closes a cycle (as for `reference`); only a
                    // unresolved type is left unconstrained (and reported).
                    let item = scalar_schema(r#type).unwrap_or_else(|| {
                        let fallback = if self.find(r#type).is_some() {
                            json!({ "type": "object" })
                        } else {
                            json!({})
                        };
                        self.resolve_any("extend", r#type, stack)
                            .map_or(fallback, Obj::into_schema)
                    });
                    let schema = field(item, *list, description.as_deref());
                    obj.set(name, schema, *optional != Some(true));
                }
                ShapeOp::Reference {
                    name,
                    r#ref,
                    spread,
                    list,
                    description,
                } => {
                    let inner = self.resolve_any("reference", r#ref, stack);
                    // `spread` wins over `list`, as in the reifier (`domain_types.ts`).
                    if *spread == Some(true) {
                        if let Some(inner) = inner {
                            obj.spread(inner);
                        }
                        continue;
                    }
                    let item = inner.map_or_else(|| json!({ "type": "object" }), Obj::into_schema);
                    obj.set(name, field(item, *list, description.as_deref()), true);
                }
            }
        }
        stack.pop();
        obj
    }
}

/// Whether `v` nests deeper than `levels` shape levels. One shape level of
/// an object schema is two JSON levels (`properties` → property schema),
/// plus one for the root. Iterative, so it is safe on arbitrarily deep input.
fn schema_too_deep(v: &Value, levels: usize) -> bool {
    let limit = levels.saturating_mul(2).saturating_add(1);
    let mut todo: Vec<(&Value, usize)> = vec![(v, 1)];
    while let Some((v, d)) = todo.pop() {
        let children: Box<dyn Iterator<Item = &Value>> = match v {
            Value::Array(a) => Box::new(a.iter()),
            Value::Object(m) => Box::new(m.values()),
            _ => continue,
        };
        if d > limit {
            return true;
        }
        todo.extend(children.map(|c| (c, d + 1)));
    }
    false
}

/// The top-level keywords of a registry object schema [`Obj::from_schema`]
/// preserves (or that carry no constraint).
const REGISTRY_ROOT_KEYWORDS: &[&str] = &[
    "type",
    "properties",
    "required",
    "title",
    "description",
    "$schema",
    "$id",
    "$comment",
];

/// Data-carrying keywords whose values are literal data, not schemas: a
/// `"$ref"` key inside an object here (e.g. a `const` of `{"$ref": 1}`) is a
/// field name, never a schema reference.
const DATA_KEYWORDS: &[&str] = &["enum", "const", "default", "examples"];

/// Whether a registry schema lies within the subset the derivation copies
/// faithfully: an object (`type`, if present, is `"object"`) whose root uses
/// only [`REGISTRY_ROOT_KEYWORDS`] and which contains no `$ref` in any schema
/// position (its target would not be copied, leaving it dangling). A `$ref`
/// that is data — a property literally named `"$ref"`, or a key of an object
/// value under [`DATA_KEYWORDS`] — is not a reference and stays supported.
/// Iterative, so a deep schema cannot overflow the stack.
fn registry_schema_supported(schema: &Value) -> bool {
    let Some(root) = schema.as_object() else {
        return false;
    };
    if root
        .keys()
        .any(|k| !REGISTRY_ROOT_KEYWORDS.contains(&k.as_str()))
    {
        return false;
    }
    if root.get("type").is_some_and(|t| t != "object") {
        return false;
    }
    // Every entry is a schema position: the root, or a value nested under
    // one (a property's schema, an `items` schema, …).
    let mut pending: Vec<&Value> = vec![schema];
    while let Some(v) = pending.pop() {
        let Value::Object(m) = v else { continue };
        if m.contains_key("$ref") {
            return false;
        }
        for (k, child) in m {
            // The keys of `properties` are property names (data), so a
            // property literally named `"$ref"` is fine; its values are
            // schemas and stay on the walk. Data-carrying keywords hold
            // literal data — a `"$ref"` key inside one is a field name,
            // never a reference. Everything else (annotations, `type`,
            // nested schema keywords) is descended into.
            if k == "properties" {
                if let Value::Object(props) = child {
                    pending.extend(props.values());
                }
            } else if !DATA_KEYWORDS.contains(&k.as_str()) {
                pending.extend(child.as_array().into_iter().flatten());
                pending.push(child);
            }
        }
    }
    true
}

/// An estimate of a schema value's size in bytes (string and key lengths plus
/// one per node). Recursion is bounded by [`MAX_DEPTH`]-limited nesting.
fn value_size(v: &Value) -> usize {
    match v {
        Value::String(s) => s.len() + 1,
        Value::Array(a) => 1 + a.iter().map(value_size).sum::<usize>(),
        Value::Object(m) => {
            1 + m
                .iter()
                .map(|(k, v)| k.len() + value_size(v))
                .sum::<usize>()
        }
        _ => 1,
    }
}

/// Wrap an item schema as a field: an array of it with `list`, plus the
/// field's description.
fn field(item: Value, list: Option<bool>, description: Option<&str>) -> Value {
    let mut schema = if list == Some(true) {
        json!({ "type": "array", "items": item })
    } else {
        item
    };
    if let (Some(d), Some(map)) = (description, schema.as_object_mut()) {
        map.insert("description".into(), json!(d));
    }
    schema
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The envelope of the doc's message start event `start` (no registry),
    /// diagnostics dropped.
    fn derive(
        xml: &str,
        process_id: Option<&str>,
    ) -> Result<Option<(String, Value)>, (String, SchemaError)> {
        start_event_input_schema(&crate::scan_bpmn(xml), "start", process_id, &())
            .map(|e| e.map(|e| (e.shape, e.schema)))
    }

    fn shapes_doc(shapes: &str) -> String {
        doc(shapes, ENVELOPE)
    }

    #[test]
    fn projecting_a_missing_field_of_a_local_shape_is_rejected() {
        let xml = shapes_doc(
            r#"<nano:shape id="Order">
                 <nano:extend name="qty" type="integer" />
                 <nano:project ref="Base" fields="qty" />
               </nano:shape>
               <nano:shape id="Base"><nano:extend name="other" type="string" /></nano:shape>"#,
        );
        assert_eq!(
            derive(&xml, None),
            Err(("Order".into(), SchemaError::UnknownProjectedField))
        );
    }

    #[test]
    fn projecting_from_a_local_cycle_is_rejected_not_weakened() {
        let xml = shapes_doc(
            r#"<nano:shape id="Order">
                 <nano:extend name="qty" type="integer" />
                 <nano:project ref="Order" fields="qty" />
               </nano:shape>"#,
        );
        assert_eq!(
            derive(&xml, None),
            Err(("Order".into(), SchemaError::CyclicProjection))
        );
    }

    #[test]
    fn projecting_from_an_unresolved_domain_type_passes_through() {
        let xml = shapes_doc(
            r#"<nano:shape id="Order"><nano:project ref="Customer" fields="tier" /></nano:shape>"#,
        );
        let (_, schema) = derive(&xml, None).unwrap().unwrap();
        assert_eq!(
            schema,
            json!({ "type": "object", "properties": { "tier": {} } })
        );
    }

    #[test]
    fn foreign_namespace_shape_is_not_selected() {
        let xml = format!(
            r#"<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                xmlns:zeebe="http://camunda.org/schema/zeebe/1.0"
                xmlns:vendor="https://example.com/vendor"
                xmlns:nano="https://nanobpm.io/schema/shapes/1.0">
              <bpmn:process id="orders">
                <bpmn:extensionElements>
                  <vendor:shape id="Order"><vendor:extend name="bogus" type="string" /></vendor:shape>
                  <nano:shapes><nano:shape id="Order">
                    <vendor:extend name="alsoBogus" type="string" />
                    <nano:extend name="orderId" type="string" />
                  </nano:shape></nano:shapes>
                </bpmn:extensionElements>
                <bpmn:startEvent id="start"><bpmn:messageEventDefinition messageRef="m" /></bpmn:startEvent>
              </bpmn:process>
              <bpmn:message id="m" name="placeOrder">
                <bpmn:extensionElements><zeebe:properties>{ENVELOPE}</zeebe:properties></bpmn:extensionElements>
              </bpmn:message>
            </bpmn:definitions>"#
        );
        let (_, schema) = derive(&xml, Some("orders")).unwrap().unwrap();
        assert_eq!(
            schema,
            json!({
                "type": "object",
                "properties": { "orderId": { "type": "string" } },
                "required": ["orderId"]
            })
        );
    }

    #[test]
    fn many_duplicate_ids_off_process_resolve_to_the_first() {
        // 20,000 same-id declarations on other processes plus 4,000 references:
        // the preferred declaration is preselected once, keeping the first.
        let mut shapes = String::from(r#"<nano:shape id="Order">"#);
        for i in 0..4_000 {
            shapes.push_str(&format!(r#"<nano:reference name="f{i}" ref="T" />"#));
        }
        shapes.push_str("</nano:shape>");
        let mut xml = shapes_doc(&shapes);
        let mut others = String::new();
        others.push_str(r#"<bpmn:process id="p0"><bpmn:extensionElements><nano:shapes><nano:shape id="T"><nano:extend name="first" type="string" /></nano:shape></nano:shapes></bpmn:extensionElements></bpmn:process>"#);
        for i in 1..20_000 {
            others.push_str(&format!(r#"<bpmn:process id="p{i}"><bpmn:extensionElements><nano:shapes><nano:shape id="T" /></nano:shapes></bpmn:extensionElements></bpmn:process>"#));
        }
        xml = xml.replace(
            "</bpmn:definitions>",
            &format!("{others}</bpmn:definitions>"),
        );
        let (_, schema) = derive(&xml, Some("orders")).unwrap().unwrap();
        assert_eq!(
            schema["properties"]["f3999"]["properties"],
            json!({ "first": { "type": "string" } })
        );
    }

    #[test]
    fn repeated_projection_keeps_typed_required_field() {
        let xml = shapes_doc(
            r#"<nano:shape id="Order"><nano:project ref="Base" fields="a,a" /></nano:shape>
               <nano:shape id="Base"><nano:extend name="a" type="number" /></nano:shape>"#,
        );
        let (_, schema) = derive(&xml, None).unwrap().unwrap();
        assert_eq!(
            schema,
            json!({"type":"object","properties":{"a":{"type":"number"}},"required":["a"]})
        );
    }

    #[test]
    fn cyclic_extend_keeps_object_type() {
        let xml = shapes_doc(
            r#"<nano:shape id="Order"><nano:extend name="child" type="Order" optional="true" /></nano:shape>"#,
        );
        let (_, schema) = derive(&xml, None).unwrap().unwrap();
        assert_eq!(
            schema,
            json!({"type":"object","properties":{"child":{"type":"object"}}})
        );
    }

    #[test]
    fn required_order_is_last_declaration_and_unique() {
        let xml = shapes_doc(
            r#"<nano:shape id="Order"><nano:extend name="a" type="string" /><nano:extend name="b" type="string" /><nano:extend name="a" type="number" /><nano:extend name="b" type="string" optional="true" /></nano:shape>"#,
        );
        let (_, schema) = derive(&xml, None).unwrap().unwrap();
        assert_eq!(schema["required"], json!(["a"]));
    }

    #[test]
    fn wide_catalog_and_wide_carry_derive_correctly() {
        // 20k shapes, 9k references to the last one; and a 9k-field carry.
        // Correctness at scale only: no wall-clock assertion (scheduler-
        // dependent). Linear lookup is structural (`ShapeIndex` HashMap,
        // HashSet-backed required names), not timed.
        let mut shapes = String::from(r#"<nano:shape id="Order">"#);
        for i in 0..9_000 {
            shapes.push_str(&format!(r#"<nano:reference name="r{i}" ref="E19999" />"#));
        }
        shapes.push_str("</nano:shape>");
        for i in 0..20_000 {
            shapes.push_str(&format!(r#"<nano:shape id="E{i}"></nano:shape>"#));
        }
        let (_, schema) = derive(&shapes_doc(&shapes), None).unwrap().unwrap();
        assert_eq!(schema["required"].as_array().unwrap().len(), 9_000);

        let mut wide = String::from(
            r#"<nano:shape id="Order"><nano:carry ref="W" /></nano:shape><nano:shape id="W">"#,
        );
        for i in 0..9_000 {
            wide.push_str(&format!(r#"<nano:extend name="f{i}" type="string" />"#));
        }
        wide.push_str("</nano:shape>");
        let (_, schema) = derive(&shapes_doc(&wide), None).unwrap().unwrap();
        assert_eq!(schema["required"].as_array().unwrap().len(), 9_000);
    }

    #[test]
    fn spread_wins_over_list() {
        let xml = shapes_doc(
            r#"<nano:shape id="Order"><nano:reference name="b" ref="Base" spread="true" list="true" /></nano:shape>
               <nano:shape id="Base"><nano:extend name="a" type="number" /></nano:shape>"#,
        );
        let (_, schema) = derive(&xml, None).unwrap().unwrap();
        assert_eq!(
            schema,
            json!({"type":"object","properties":{"a":{"type":"number"}},"required":["a"]})
        );
    }

    #[test]
    fn exponential_acyclic_expansion_is_bounded() {
        let mut shapes = String::from(
            r#"<nano:shape id="Order"><nano:reference name="l" ref="S0" /><nano:reference name="r" ref="S0" /></nano:shape>"#,
        );
        for i in 0..25 {
            shapes.push_str(&format!(
                r#"<nano:shape id="S{i}"><nano:reference name="l" ref="S{n}" /><nano:reference name="r" ref="S{n}" /></nano:shape>"#,
                n = i + 1
            ));
        }
        shapes.push_str(
            r#"<nano:shape id="S25"><nano:extend name="x" type="string" /></nano:shape>"#,
        );
        assert_eq!(
            derive(&shapes_doc(&shapes), None),
            Err(("Order".into(), SchemaError::TooLarge))
        );
    }

    #[test]
    fn deep_acyclic_chain_is_rejected_not_recursed() {
        let mut shapes = String::from(
            r#"<nano:shape id="Order"><nano:reference name="n" ref="S0" /></nano:shape>"#,
        );
        for i in 0..5_000 {
            shapes.push_str(&format!(
                r#"<nano:shape id="S{i}"><nano:reference name="n" ref="S{n}" /></nano:shape>"#,
                n = i + 1
            ));
        }
        assert_eq!(
            derive(&shapes_doc(&shapes), None),
            Err(("Order".into(), SchemaError::TooDeep))
        );
    }

    #[test]
    fn long_descriptions_exhaust_the_byte_budget() {
        let big = "x".repeat(64 * 1024);
        let mut order = String::from(r#"<nano:shape id="Order">"#);
        for i in 0..40 {
            order.push_str(&format!(r#"<nano:reference name="f{i}" ref="Big" />"#));
        }
        order.push_str("</nano:shape>");
        let shapes = format!(
            r#"{order}<nano:shape id="Big"><nano:extend name="a" type="string" description="{big}" /></nano:shape>"#
        );
        assert_eq!(
            derive(&shapes_doc(&shapes), None),
            Err(("Order".into(), SchemaError::TooLarge))
        );
    }

    #[test]
    fn repeated_projection_of_a_large_field_is_charged() {
        let big = "x".repeat(64 * 1024);
        let fields = vec!["a"; 40].join(",");
        let shapes = format!(
            r#"<nano:shape id="Order"><nano:project ref="Base" fields="{fields}" /></nano:shape>
               <nano:shape id="Base"><nano:extend name="a" type="string" description="{big}" /></nano:shape>"#
        );
        assert_eq!(
            derive(&shapes_doc(&shapes), None),
            Err(("Order".into(), SchemaError::TooLarge))
        );
    }

    fn doc(shapes: &str, message_props: &str) -> String {
        format!(
            r#"<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                xmlns:zeebe="http://camunda.org/schema/zeebe/1.0"
                xmlns:nano="https://nanobpm.io/schema/shapes/1.0">
              <bpmn:process id="orders">
                <bpmn:extensionElements><nano:shapes>{shapes}</nano:shapes></bpmn:extensionElements>
                <bpmn:startEvent id="start"><bpmn:messageEventDefinition messageRef="m" /></bpmn:startEvent>
              </bpmn:process>
              <bpmn:message id="m" name="placeOrder">
                <bpmn:extensionElements><zeebe:properties>{message_props}</zeebe:properties></bpmn:extensionElements>
              </bpmn:message>
            </bpmn:definitions>"#
        )
    }

    const ENVELOPE: &str = r#"<zeebe:property name="io.nanobpm.dataEnvelope.in" value="Order" />"#;

    #[test]
    fn nested_list_required_and_descriptions() {
        let xml = doc(
            r#"<nano:shape id="Order">
                 <nano:extend name="orderId" type="string" description="The order id." />
                 <nano:extend name="note" type="string" optional="true" />
                 <nano:extend name="placed" type="datetime" />
                 <nano:extend name="tags" type="string" list="true" optional="true" />
                 <nano:reference name="customer" ref="Customer" description="Who ordered." />
                 <nano:reference name="lines" ref="Line" list="true" />
               </nano:shape>
               <nano:shape id="Customer"><nano:extend name="email" type="string" /></nano:shape>
               <nano:shape id="Line">
                 <nano:extend name="sku" type="string" />
                 <nano:extend name="qty" type="integer" />
               </nano:shape>"#,
            ENVELOPE,
        );
        let (id, schema) = derive(&xml, Some("orders")).unwrap().unwrap();
        assert_eq!(id, "Order");
        assert_eq!(
            schema,
            json!({
                "type": "object",
                "properties": {
                    "orderId": { "type": "string", "description": "The order id." },
                    "note": { "type": "string" },
                    "placed": { "type": "string", "format": "date-time" },
                    "tags": { "type": "array", "items": { "type": "string" } },
                    "customer": {
                        "type": "object",
                        "properties": { "email": { "type": "string" } },
                        "required": ["email"],
                        "description": "Who ordered.",
                    },
                    "lines": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "sku": { "type": "string" },
                                "qty": { "type": "integer" },
                            },
                            "required": ["sku", "qty"],
                        },
                    },
                },
                "required": ["orderId", "placed", "customer", "lines"],
            })
        );
    }

    #[test]
    fn spread_carry_project_and_redeclaration_fold_in_order() {
        let xml = doc(
            r#"<nano:shape id="Order">
                 <nano:carry ref="Base" />
                 <nano:carry ref="DomainEntity" />
                 <nano:project ref="Base" fields="a" />
                 <nano:project ref="DomainEntity" fields="x" />
                 <nano:reference name="ignored" ref="Base" spread="true" />
                 <nano:extend name="b" type="boolean" optional="true" />
               </nano:shape>
               <nano:shape id="Base">
                 <nano:extend name="a" type="number" />
                 <nano:extend name="b" type="string" />
               </nano:shape>"#,
            ENVELOPE,
        );
        let (_, schema) = derive(&xml, None).unwrap().unwrap();
        assert_eq!(
            schema,
            json!({
                "type": "object",
                "properties": {
                    "a": { "type": "number" },
                    "b": { "type": "boolean" },
                    "x": {},
                },
                "required": ["a"],
            })
        );
    }

    #[test]
    fn cycles_terminate_and_missing_envelopes_are_none() {
        let xml = doc(
            r#"<nano:shape id="Order"><nano:reference name="self" ref="Order" /></nano:shape>"#,
            ENVELOPE,
        );
        let (_, schema) = derive(&xml, None).unwrap().unwrap();
        assert_eq!(
            schema["properties"]["self"],
            json!({ "type": "object" }),
            "{schema}"
        );
        // No envelope, or an unknown start event.
        let xml = doc(r#"<nano:shape id="Order" />"#, "");
        assert_eq!(derive(&xml, None), Ok(None));
        let scan = crate::scan_bpmn(&doc(r#"<nano:shape id="Order" />"#, ENVELOPE));
        assert_eq!(
            start_event_input_schema(&scan, "other", None, &()),
            Ok(None)
        );
    }

    /// Review of #1842: a registry schema deeper than MAX_DEPTH (as a root
    /// or behind a shape ref) is TooDeep; an oversized registry root is
    /// TooLarge, never misreported as UnresolvedEnvelope.
    #[test]
    fn registry_roots_and_refs_honour_depth_and_size_limits() {
        let mut deep = json!({ "type": "string" });
        for _ in 0..(MAX_DEPTH + 2) {
            deep = json!({ "type": "object", "properties": { "x": deep } });
        }
        let root = |reg: &HashMap<String, Value>| {
            start_event_input_schema(&crate::scan_bpmn(&doc("", ENVELOPE)), "start", None, reg)
                .map(|o| o.map(|e| e.schema))
        };
        let reg = HashMap::from([("Order".to_string(), deep.clone())]);
        assert_eq!(root(&reg), Err(("Order".into(), SchemaError::TooDeep)));
        let xml = doc(
            r#"<nano:shape id="Order"><nano:carry ref="Deep" /></nano:shape>"#,
            ENVELOPE,
        );
        let reg = HashMap::from([("Deep".to_string(), deep)]);
        assert_eq!(
            start_event_input_schema(&crate::scan_bpmn(&xml), "start", None, &reg).map(|_| ()),
            Err(("Order".into(), SchemaError::TooDeep))
        );
        let big = "x".repeat(MAX_EXPANDED_BYTES + 1);
        let reg = HashMap::from([(
            "Order".to_string(),
            json!({ "type": "object", "properties": { big: { "type": "string" } } }),
        )]);
        assert_eq!(root(&reg), Err(("Order".into(), SchemaError::TooLarge)));
    }

    /// Issue #1748: an envelope ref the model does not declare (and no
    /// registry resolves) is a hard error, never the legacy fallback.
    #[test]
    fn unresolved_root_envelope_is_an_error_not_a_fallback() {
        let xml = doc("", ENVELOPE);
        assert_eq!(
            derive(&xml, None),
            Err(("Order".into(), SchemaError::UnresolvedEnvelope))
        );
        // ...but resolves from the registry when it knows the type.
        let registry: HashMap<String, Value> = HashMap::from([(
            "Order".to_string(),
            json!({ "type": "object", "properties": { "id": { "type": "string" } }, "required": ["id"] }),
        )]);
        let e = start_event_input_schema(&crate::scan_bpmn(&xml), "start", None, &registry)
            .unwrap()
            .unwrap();
        assert_eq!(e.schema["required"], json!(["id"]));
        assert!(e.unresolved.is_empty());
    }

    /// Issue #1748: external refs resolve from the type registry; those it
    /// does not know are surfaced as unresolved-contract diagnostics.
    #[test]
    fn external_refs_resolve_from_registry_or_are_reported() {
        let xml = doc(
            r#"<nano:shape id="Order">
                 <nano:carry ref="Customer" />
                 <nano:project ref="Product" fields="sku" />
                 <nano:extend name="addr" type="Address" />
                 <nano:reference name="pay" ref="Payment" />
                 <nano:carry ref="Unknown" />
               </nano:shape>"#,
            ENVELOPE,
        );
        let obj = |p: Value, r: Value| json!({ "type": "object", "properties": p, "required": r });
        let registry: HashMap<String, Value> = HashMap::from([
            (
                "Customer".to_string(),
                obj(json!({ "email": { "type": "string" } }), json!(["email"])),
            ),
            (
                "Product".to_string(),
                obj(json!({ "sku": { "type": "string" } }), json!(["sku"])),
            ),
            (
                "Address".to_string(),
                obj(json!({ "city": { "type": "string" } }), json!(["city"])),
            ),
        ]);
        let e = start_event_input_schema(&crate::scan_bpmn(&xml), "start", None, &registry)
            .unwrap()
            .unwrap();
        assert_eq!(
            e.schema["required"],
            json!(["email", "sku", "addr", "pay"]),
            "{}",
            e.schema
        );
        assert_eq!(e.schema["properties"]["addr"]["required"], json!(["city"]));
        assert_eq!(e.unresolved.len(), 2, "{:?}", e.unresolved);
        assert!(e.unresolved[0].contains("`reference` of `Payment`"));
        assert!(e.unresolved[1].contains("`carry` of `Unknown`"));
        // A registry type without a projected field is an invalid projection.
        let xml = doc(
            r#"<nano:shape id="Order"><nano:project ref="Product" fields="nope" /></nano:shape>"#,
            ENVELOPE,
        );
        assert_eq!(
            start_event_input_schema(&crate::scan_bpmn(&xml), "start", None, &registry),
            Err(("Order".into(), SchemaError::UnknownProjectedField))
        );
    }

    /// Issue #1748: the envelope is selected through the start event's
    /// `messageRef` (message id), not the message name — two messages may
    /// share a name, or a message may have no name at all.
    #[test]
    fn envelope_is_resolved_by_message_ref_not_name() {
        let xml = r#"<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                xmlns:zeebe="http://camunda.org/schema/zeebe/1.0"
                xmlns:nano="https://nanobpm.io/schema/shapes/1.0">
              <bpmn:process id="orders">
                <bpmn:extensionElements><nano:shapes>
                  <nano:shape id="A"><nano:extend name="a" type="string" /></nano:shape>
                  <nano:shape id="B"><nano:extend name="b" type="string" /></nano:shape>
                </nano:shapes></bpmn:extensionElements>
                <bpmn:startEvent id="startA"><bpmn:messageEventDefinition messageRef="mA" /></bpmn:startEvent>
                <bpmn:startEvent id="startB"><bpmn:messageEventDefinition messageRef="mB" /></bpmn:startEvent>
                <bpmn:startEvent id="startC"><bpmn:messageEventDefinition messageRef="mC" /></bpmn:startEvent>
              </bpmn:process>
              <bpmn:message id="mA" name="shared">
                <bpmn:extensionElements><zeebe:properties><zeebe:property name="io.nanobpm.dataEnvelope.in" value="A" /></zeebe:properties></bpmn:extensionElements>
              </bpmn:message>
              <bpmn:message id="mB" name="shared">
                <bpmn:extensionElements><zeebe:properties><zeebe:property name="io.nanobpm.dataEnvelope.in" value="B" /></zeebe:properties></bpmn:extensionElements>
              </bpmn:message>
              <bpmn:message id="mC">
                <bpmn:extensionElements><zeebe:properties><zeebe:property name="io.nanobpm.dataEnvelope.in" value="A" /></zeebe:properties></bpmn:extensionElements>
              </bpmn:message>
            </bpmn:definitions>"#;
        let scan = crate::scan_bpmn(xml);
        let shape = |start: &str| {
            start_event_input_schema(&scan, start, Some("orders"), &())
                .unwrap()
                .map(|e| e.shape)
        };
        assert_eq!(shape("startA").as_deref(), Some("A"));
        assert_eq!(shape("startB").as_deref(), Some("B"));
        // An unnamed message (the engine falls back to its id as the name).
        assert_eq!(shape("startC").as_deref(), Some("A"));
    }

    /// Two processes in one resource may reuse a start id; each process's
    /// tool must resolve ITS start's message envelope, never the first
    /// process's (element ids are unique only per process).
    #[test]
    fn reused_start_id_resolves_per_owning_process() {
        let xml = r#"<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                xmlns:zeebe="http://camunda.org/schema/zeebe/1.0"
                xmlns:nano="https://nanobpm.io/schema/shapes/1.0">
              <bpmn:process id="p1">
                <bpmn:extensionElements><nano:shapes>
                  <nano:shape id="A"><nano:extend name="a" type="string" /></nano:shape>
                </nano:shapes></bpmn:extensionElements>
                <bpmn:startEvent id="start"><bpmn:messageEventDefinition messageRef="mA" /></bpmn:startEvent>
              </bpmn:process>
              <bpmn:process id="p2">
                <bpmn:extensionElements><nano:shapes>
                  <nano:shape id="B"><nano:extend name="b" type="string" /></nano:shape>
                </nano:shapes></bpmn:extensionElements>
                <bpmn:startEvent id="start"><bpmn:messageEventDefinition messageRef="mB" /></bpmn:startEvent>
              </bpmn:process>
              <bpmn:message id="mA" name="a">
                <bpmn:extensionElements><zeebe:properties><zeebe:property name="io.nanobpm.dataEnvelope.in" value="A" /></zeebe:properties></bpmn:extensionElements>
              </bpmn:message>
              <bpmn:message id="mB" name="b">
                <bpmn:extensionElements><zeebe:properties><zeebe:property name="io.nanobpm.dataEnvelope.in" value="B" /></zeebe:properties></bpmn:extensionElements>
              </bpmn:message>
            </bpmn:definitions>"#;
        let scan = crate::scan_bpmn(xml);
        let shape = |p: &str| {
            start_event_input_schema(&scan, "start", Some(p), &())
                .unwrap()
                .map(|e| e.shape)
        };
        assert_eq!(shape("p1").as_deref(), Some("A"));
        assert_eq!(shape("p2").as_deref(), Some("B"));
        assert_eq!(shape("p3"), None);
    }

    /// A registry schema outside the supported subset (definitions, `$ref`s,
    /// object-level constraints, a non-object root) is rejected — as the root
    /// envelope and as a nested ref — never copied with its constraints or
    /// definitions silently dropped.
    #[test]
    fn unsupported_registry_schemas_are_rejected() {
        let local = |inner: &str| {
            doc(
                &format!(r#"<nano:shape id="Order"><nano:carry ref="{inner}" /></nano:shape>"#),
                ENVELOPE,
            )
        };
        let cases = [
            json!({"type": "object", "$defs": {"A": {"type": "string"}},
                   "properties": {"a": {"$ref": "#/$defs/A"}}}),
            json!({"type": "object", "properties": {"a": {"$ref": "#/x"}}}),
            json!({"type": "object", "additionalProperties": false,
                   "properties": {"a": {"type": "string"}}}),
            json!({"type": "object", "minProperties": 1}),
            json!({"type": "string"}),
            json!(["not", "an", "object"]),
        ];
        for schema in cases {
            // As the root envelope.
            let mut reg = HashMap::new();
            reg.insert("Order".to_string(), schema.clone());
            let e = start_event_input_schema(
                &crate::scan_bpmn(&doc("", ENVELOPE)),
                "start",
                None,
                &reg,
            )
            .map(|_| ());
            assert_eq!(
                e,
                Err(("Order".into(), SchemaError::UnsupportedRegistrySchema)),
                "root {schema}"
            );
            // As a nested ref of a local shape.
            let mut reg = HashMap::new();
            reg.insert("Ext".to_string(), schema.clone());
            let e = start_event_input_schema(&crate::scan_bpmn(&local("Ext")), "start", None, &reg)
                .map(|_| ());
            assert_eq!(
                e,
                Err(("Order".into(), SchemaError::UnsupportedRegistrySchema)),
                "nested {schema}"
            );
        }
        // The supported subset (with annotations) still resolves.
        let mut reg = HashMap::new();
        reg.insert(
            "Order".to_string(),
            json!({"type": "object", "title": "O", "description": "d",
                   "properties": {"a": {"type": "string", "description": "x"}},
                   "required": ["a"]}),
        );
        let ok =
            start_event_input_schema(&crate::scan_bpmn(&doc("", ENVELOPE)), "start", None, &reg)
                .unwrap()
                .unwrap();
        assert_eq!(ok.schema["required"], json!(["a"]));
    }

    /// A registry schema may require a name it does not declare in
    /// `properties`; that requirement must survive conversion (as an
    /// unconstrained required property), for root and carried types alike.
    #[test]
    fn required_only_registry_fields_are_preserved() {
        let reg: HashMap<String, Value> = HashMap::from([
            (
                "Order".to_string(),
                json!({"type": "object", "properties": {}, "required": ["id"]}),
            ),
            (
                "Customer".to_string(),
                json!({"type": "object", "required": ["email"]}),
            ),
        ]);
        let root =
            start_event_input_schema(&crate::scan_bpmn(&doc("", ENVELOPE)), "start", None, &reg)
                .unwrap()
                .unwrap();
        assert_eq!(root.schema["required"], json!(["id"]), "{}", root.schema);
        assert_eq!(root.schema["properties"]["id"], json!({}));
        let xml = doc(
            r#"<nano:shape id="Order"><nano:carry ref="Customer" /></nano:shape>"#,
            ENVELOPE,
        );
        let carried = start_event_input_schema(&crate::scan_bpmn(&xml), "start", None, &reg)
            .unwrap()
            .unwrap();
        assert_eq!(
            carried.schema["required"],
            json!(["email"]),
            "{}",
            carried.schema
        );
        assert_eq!(carried.schema["properties"]["email"], json!({}));
        assert!(carried.unresolved.is_empty(), "{:?}", carried.unresolved);
    }

    /// A `$ref` that is data, not a schema keyword — a property literally
    /// named `"$ref"`, or a `"$ref"` key inside an object value under
    /// `enum` / `const` / `default` / `examples` — is not a reference, so it
    /// stays within the supported subset.
    #[test]
    fn a_data_ref_is_not_an_unsupported_registry_schema() {
        let cases = [
            // A property literally named `$ref`.
            json!({"type": "object",
                   "properties": {"$ref": {"type": "string"}}}),
            // `"$ref"` keys inside data values (enum/const/default/examples).
            json!({"type": "object",
                   "properties": {"a": {"type": "object",
                                        "const": {"$ref": "#/x"},
                                        "default": {"$ref": "#/y"},
                                        "enum": [{"$ref": "#/z"}],
                                        "examples": [{"$ref": "#/w"}]}}}),
        ];
        for schema in cases {
            let mut reg = HashMap::new();
            reg.insert("Order".to_string(), schema.clone());
            let e = start_event_input_schema(
                &crate::scan_bpmn(&doc("", ENVELOPE)),
                "start",
                None,
                &reg,
            );
            assert!(e.is_ok(), "data $ref rejected: {schema} -> {e:?}");
        }
        // A real `$ref` in a schema position is still rejected, even nested
        // under a property or a non-`properties` keyword like `items`.
        for schema in [
            json!({"type": "object", "properties": {"a": {"$ref": "#/x"}}}),
            json!({"type": "object",
                   "properties": {"a": {"type": "array", "items": {"$ref": "#/x"}}}}),
            // A property named `$ref` whose SCHEMA holds a real `$ref`.
            json!({"type": "object", "properties": {"$ref": {"$ref": "#/x"}}}),
        ] {
            let mut reg = HashMap::new();
            reg.insert("Order".to_string(), schema.clone());
            let e = start_event_input_schema(
                &crate::scan_bpmn(&doc("", ENVELOPE)),
                "start",
                None,
                &reg,
            )
            .map(|_| ());
            assert_eq!(
                e,
                Err(("Order".into(), SchemaError::UnsupportedRegistrySchema)),
                "schema-position $ref accepted: {schema}"
            );
        }
    }
}
