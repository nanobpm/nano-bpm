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

/// The data envelope a message's payload is typed by, resolved from one BPMN
/// document: `Some((shape_id, object_schema))` when the named `bpmn:message`
/// carries an `io.nanobpm.dataEnvelope.in` ref that resolves to a `nano:shape`
/// declared in the same document; `None` otherwise (no envelope, or a ref the
/// model alone cannot resolve). `process_id` prefers a shape declared on that
/// process when several processes declare the same id.
pub fn message_input_schema(
    xml: &str,
    message_name: &str,
    process_id: Option<&str>,
) -> Option<(String, Value)> {
    let scan = scan_bpmn(xml);
    let shape_id = scan
        .messages
        .iter()
        .find(|m| m.message_name == message_name)?
        .input_type
        .clone()?;
    let schema = shape_json_schema(&scan.shapes, &shape_id, process_id)?;
    Some((shape_id, schema))
}

/// The object JSON Schema of shape `shape_id` (`{type, properties,
/// required?}`), or `None` if no shape with that id is declared.
pub fn shape_json_schema(
    shapes: &[ShapeDecl],
    shape_id: &str,
    process_id: Option<&str>,
) -> Option<Value> {
    let resolver = Resolver { shapes, process_id };
    let shape = resolver.find(shape_id)?;
    let mut stack = Vec::new();
    Some(resolver.object(shape, &mut stack).into_schema())
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

struct Resolver<'a> {
    shapes: &'a [ShapeDecl],
    process_id: Option<&'a str>,
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

    /// Resolve a shape's object, or `None` when it is unknown or would close a
    /// reference cycle.
    fn resolve(&self, id: &str, stack: &mut Vec<String>) -> Option<Obj> {
        if stack.iter().any(|s| s == id) {
            return None;
        }
        let shape = self.find(id)?;
        Some(self.object(shape, stack))
    }

    fn object(&self, shape: &ShapeDecl, stack: &mut Vec<String>) -> Obj {
        stack.push(shape.id.clone());
        let mut obj = Obj::default();
        for op in &shape.ops {
            match op {
                ShapeOp::Carry { r#ref } => {
                    if let Some(inner) = self.resolve(r#ref, stack) {
                        obj.spread(inner);
                    }
                }
                ShapeOp::Project { r#ref, fields, .. } => {
                    let mut inner = self.resolve(r#ref, stack);
                    for field in fields {
                        match inner.as_mut().and_then(|o| {
                            let required = o.required.contains(field);
                            o.properties.remove(field).map(|s| (s, required))
                        }) {
                            Some((schema, required)) => obj.set(field, schema, required),
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
                    if *spread == Some(true) && *list != Some(true) {
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
        let (id, schema) = message_input_schema(&xml, "placeOrder", Some("orders")).unwrap();
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
        let (_, schema) = message_input_schema(&xml, "placeOrder", None).unwrap();
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
        let (_, schema) = message_input_schema(&xml, "placeOrder", None).unwrap();
        assert_eq!(
            schema["properties"]["self"],
            json!({ "type": "object" }),
            "{schema}"
        );
        // A ref with no declared shape, no envelope, or an unknown message.
        let xml = doc("", ENVELOPE);
        assert_eq!(message_input_schema(&xml, "placeOrder", None), None);
        let xml = doc(r#"<nano:shape id="Order" />"#, "");
        assert_eq!(message_input_schema(&xml, "placeOrder", None), None);
        assert_eq!(message_input_schema(&xml, "other", None), None);
    }
}
