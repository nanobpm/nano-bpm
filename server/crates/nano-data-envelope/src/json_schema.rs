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
//! - `extend name type [optional] [list] [description]` — a scalar keyword maps
//!   to its JSON-Schema type (see [`scalar_schema`]); a type naming another
//!   shape nests that shape's object schema; any other (fused-domain) type is
//!   left unconstrained. Required unless `optional="true"`.
//! - `reference name ref [spread] [list] [description]` — nests the referenced
//!   shape as an object (an array of them with `list`), or inlines its fields
//!   with `spread`. Required.
//! - `carry ref` — spreads a referenced *shape*'s fields; a fused domain
//!   entity is not resolvable from the model alone, so it contributes nothing
//!   (undeclared arguments still pass through as process variables).
//! - `project ref fields` — picks the named fields of a referenced shape, or
//!   declares them unconstrained (and optional) for a domain entity.
//!
//! A reference cycle resolves to an unconstrained object at the point it
//! closes, so derivation always terminates.

use std::cell::Cell;

use serde_json::{Map, Value, json};

use crate::{ShapeDecl, ShapeOp, scan_bpmn};

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
        }
    }
}

impl std::error::Error for SchemaError {}

/// The data envelope a message's payload is typed by, resolved from one BPMN
/// document: `Ok(Some((shape_id, object_schema)))` when the named
/// `bpmn:message` carries an `io.nanobpm.dataEnvelope.in` ref that resolves to
/// a `nano:shape` declared in the same document; `Ok(None)` when there is no
/// envelope (or a ref the model alone cannot resolve); `Err((shape_id, e))`
/// when the envelope resolves but its schema exceeds the derivation limits.
/// `process_id` prefers a shape declared on that process when several
/// processes declare the same id.
pub fn message_input_schema(
    xml: &str,
    message_name: &str,
    process_id: Option<&str>,
) -> Result<Option<(String, Value)>, (String, SchemaError)> {
    let scan = scan_bpmn(xml);
    let Some(shape_id) = scan
        .messages
        .iter()
        .find(|m| m.message_name == message_name)
        .and_then(|m| m.input_type.clone())
    else {
        return Ok(None);
    };
    match shape_json_schema(&scan.shapes, &shape_id, process_id) {
        Ok(Some(schema)) => Ok(Some((shape_id, schema))),
        Ok(None) => Ok(None),
        Err(e) => Err((shape_id, e)),
    }
}

/// The object JSON Schema of shape `shape_id` (`{type, properties,
/// required?}`), `Ok(None)` if no shape with that id is declared, or an error
/// when the expansion exceeds the derivation limits (an incomplete schema is
/// never returned).
pub fn shape_json_schema(
    shapes: &[ShapeDecl],
    shape_id: &str,
    process_id: Option<&str>,
) -> Result<Option<Value>, SchemaError> {
    let resolver = Resolver {
        shapes,
        process_id,
        ops: Cell::new(MAX_EXPANDED_OPS),
        bytes: Cell::new(MAX_EXPANDED_BYTES),
        error: Cell::new(None),
    };
    let Some(shape) = resolver.find(shape_id) else {
        return Ok(None);
    };
    let mut stack = Vec::new();
    let obj = resolver.object(shape, &mut stack);
    match resolver.error.get() {
        Some(e) => Err(e),
        None => Ok(Some(obj.into_schema())),
    }
}

/// An object schema under construction: ordered properties plus the
/// (unique, declaration-ordered) required names.
#[derive(Default)]
struct Obj {
    properties: Map<String, Value>,
    required: Vec<String>,
}

impl Obj {
    fn set(&mut self, name: &str, schema: Value, required: bool) {
        self.properties.insert(name.to_string(), schema);
        self.required.retain(|r| r != name);
        if required {
            self.required.push(name.to_string());
        }
    }

    fn spread(&mut self, other: Obj) {
        for (name, schema) in other.properties {
            let required = other.required.contains(&name);
            self.set(&name, schema, required);
        }
    }

    fn into_schema(self) -> Value {
        let mut v = json!({ "type": "object", "properties": self.properties });
        if !self.required.is_empty() {
            v["required"] = json!(self.required);
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
    shapes: &'a [ShapeDecl],
    process_id: Option<&'a str>,
    /// Remaining op-expansion budget.
    ops: Cell<usize>,
    /// Remaining byte budget.
    bytes: Cell<usize>,
    /// The first limit hit; once set, expansion stops.
    error: Cell<Option<SchemaError>>,
}

impl<'a> Resolver<'a> {
    fn find(&self, id: &str) -> Option<&'a ShapeDecl> {
        let mut candidates = self.shapes.iter().filter(|s| s.id == id);
        let first = candidates.next()?;
        if first.process.as_deref() == self.process_id {
            return Some(first);
        }
        Some(
            candidates
                .find(|s| s.process.as_deref() == self.process_id)
                .unwrap_or(first),
        )
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
                    if let Some(inner) = self.resolve(r#ref, stack) {
                        obj.spread(inner);
                    }
                }
                ShapeOp::Project { r#ref, fields, .. } => {
                    let inner = self.resolve(r#ref, stack);
                    for field in fields {
                        let found = inner.as_ref().and_then(|o| {
                            o.properties
                                .get(field)
                                .map(|s| (s, o.required.contains(field)))
                        });
                        // Every projected copy is charged (op + its size)
                        // before it is cloned, so repeated projections of a
                        // large field cannot amplify memory.
                        let size = field.len() + found.map_or(0, |(s, _)| value_size(s));
                        if !self.charge(size) {
                            break;
                        }
                        match found {
                            Some((schema, required)) => obj.set(field, schema.clone(), required),
                            None => obj.set(field, json!({}), false),
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
                    let item = scalar_schema(r#type).unwrap_or_else(|| {
                        self.resolve(r#type, stack)
                            .map_or_else(|| json!({}), Obj::into_schema)
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
                    let inner = self.resolve(r#ref, stack);
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

    fn shapes_doc(shapes: &str) -> String {
        doc(shapes, ENVELOPE)
    }

    #[test]
    fn repeated_projection_keeps_typed_required_field() {
        let xml = shapes_doc(
            r#"<nano:shape id="Order"><nano:project ref="Base" fields="a,a" /></nano:shape>
               <nano:shape id="Base"><nano:extend name="a" type="number" /></nano:shape>"#,
        );
        let (_, schema) = message_input_schema(&xml, "placeOrder", None)
            .unwrap()
            .unwrap();
        assert_eq!(
            schema,
            json!({"type":"object","properties":{"a":{"type":"number"}},"required":["a"]})
        );
    }

    #[test]
    fn spread_wins_over_list() {
        let xml = shapes_doc(
            r#"<nano:shape id="Order"><nano:reference name="b" ref="Base" spread="true" list="true" /></nano:shape>
               <nano:shape id="Base"><nano:extend name="a" type="number" /></nano:shape>"#,
        );
        let (_, schema) = message_input_schema(&xml, "placeOrder", None)
            .unwrap()
            .unwrap();
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
            message_input_schema(&shapes_doc(&shapes), "placeOrder", None),
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
            message_input_schema(&shapes_doc(&shapes), "placeOrder", None),
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
            message_input_schema(&shapes_doc(&shapes), "placeOrder", None),
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
            message_input_schema(&shapes_doc(&shapes), "placeOrder", None),
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
        let (id, schema) = message_input_schema(&xml, "placeOrder", Some("orders"))
            .unwrap()
            .unwrap();
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
        let (_, schema) = message_input_schema(&xml, "placeOrder", None)
            .unwrap()
            .unwrap();
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
    fn cycles_terminate_and_unresolvable_envelopes_are_none() {
        let xml = doc(
            r#"<nano:shape id="Order"><nano:reference name="self" ref="Order" /></nano:shape>"#,
            ENVELOPE,
        );
        let (_, schema) = message_input_schema(&xml, "placeOrder", None)
            .unwrap()
            .unwrap();
        assert_eq!(
            schema["properties"]["self"],
            json!({ "type": "object" }),
            "{schema}"
        );
        // A ref with no declared shape, no envelope, or an unknown message.
        let xml = doc("", ENVELOPE);
        assert_eq!(message_input_schema(&xml, "placeOrder", None), Ok(None));
        let xml = doc(r#"<nano:shape id="Order" />"#, "");
        assert_eq!(message_input_schema(&xml, "placeOrder", None), Ok(None));
        assert_eq!(message_input_schema(&xml, "other", None), Ok(None));
    }
}
