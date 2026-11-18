//! **Cluster variables** — replicated engine state read by FEEL at runtime.
//!
//! Cluster variables are cluster-wide configuration values (a set of *global*
//! variables plus per-*tenant* overlays) that BPMN FEEL expressions can read at
//! runtime. They are **engine state** (`State::cluster_variables`), changed only
//! by the replicated commands
//! [`Command::CreateClusterVariable`](crate::Command::CreateClusterVariable) /
//! [`UpdateClusterVariable`](crate::Command::UpdateClusterVariable) /
//! [`DeleteClusterVariable`](crate::Command::DeleteClusterVariable), which emit
//! the additive events `ClusterVariableCreated` / `Updated` / `Deleted`
//! (Zeebe parity: `ClusterVariableRecord` `CREATED` / `UPDATED` / `DELETED`).
//!
//! Because the state is log-derived it is snapshotted, rebuilt by replay, and
//! read at the log position being evaluated — every replica (and every replay)
//! observes the same values and kinds (#1657, ADR 0066 option C). There is
//! deliberately **no** host-injected handle: the engine's apply input has no
//! place for node-local cluster-variable state.

use std::collections::{BTreeMap, HashMap};

use crate::model::Value;

/// The tenant id every single-tenant process instance runs under. Cluster
/// variables scoped to this tenant (plus all `global` ones) are visible to the
/// engine's FEEL evaluation; a variable scoped to any other tenant is not.
pub const DEFAULT_TENANT: &str = "<default>";

/// A cluster variable's kind — Camunda 8.10 `ClusterVariableKindEnum`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "SCREAMING_SNAKE_CASE"))]
pub enum ClusterVariableKind {
    /// An ordinary JSON value (the default).
    #[default]
    Json,
    /// A host-trusted carrier of `camunda.secrets.<name>` reference text
    /// (#1296 / #1640). Stored verbatim; never resolved by the engine.
    SecretReference,
}

/// One stored cluster variable.
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ClusterVariable {
    /// A stable, per-partition monotonic id (the REST search pagination
    /// cursor). Assigned from [`ClusterVariableState::next_id`] when the
    /// variable is created, preserved across updates.
    pub id: u64,
    /// The variable's value — what FEEL resolves.
    pub value: Value,
    /// The variable's kind (fixed at creation; an update preserves it).
    #[cfg_attr(feature = "serde", serde(default))]
    pub kind: ClusterVariableKind,
    /// Camunda 8.10 metadata bag (string / number values). Opaque to the
    /// engine; never overlaid into FEEL.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "BTreeMap::is_empty")
    )]
    pub metadata: BTreeMap<String, Value>,
}

// `Value` is reflexive for every stored value (see its docs), so `Eq` holds.
impl Eq for ClusterVariable {}

/// The cluster variables visible to FEEL evaluation: global variables plus
/// per-tenant overlays. Global variables are visible to every process
/// instance; a tenant's variables are visible only to instances running under
/// that tenant. Instance/local variables always shadow cluster variables.
///
/// Ordered maps (`BTreeMap`) so snapshot encoding and iteration are canonical.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ClusterVariableState {
    /// Global-scoped cluster variables, keyed by name.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "BTreeMap::is_empty")
    )]
    pub global: BTreeMap<String, ClusterVariable>,
    /// Tenant-scoped cluster variables, keyed by tenant id then name.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "BTreeMap::is_empty")
    )]
    pub tenants: BTreeMap<String, BTreeMap<String, ClusterVariable>>,
    /// The last id handed to a created variable.
    #[cfg_attr(feature = "serde", serde(default))]
    pub next_id: u64,
}

impl ClusterVariableState {
    /// `true` when no cluster variable is defined in any scope and no id was
    /// ever handed out — lets a snapshot omit the field entirely.
    pub fn is_unused(&self) -> bool {
        self.is_empty() && self.next_id == 0
    }

    /// `true` when no cluster variable is defined in any scope — the common
    /// case, which lets the engine keep its zero-copy variable-resolution fast
    /// path.
    pub fn is_empty(&self) -> bool {
        self.global.is_empty() && self.tenants.values().all(BTreeMap::is_empty)
    }

    /// The variable stored for `(tenant, name)` (`None` tenant = global).
    pub fn get(&self, tenant: Option<&str>, name: &str) -> Option<&ClusterVariable> {
        match tenant {
            None => self.global.get(name),
            Some(t) => self.tenants.get(t)?.get(name),
        }
    }

    /// Every stored variable as `(tenant, name, variable)`, globals first, in
    /// canonical (ordered) sequence.
    pub fn iter(&self) -> impl Iterator<Item = (Option<&str>, &str, &ClusterVariable)> {
        self.global
            .iter()
            .map(|(n, v)| (None, n.as_str(), v))
            .chain(self.tenants.iter().flat_map(|(t, vars)| {
                vars.iter()
                    .map(move |(n, v)| (Some(t.as_str()), n.as_str(), v))
            }))
    }

    /// Inserts a new variable under a freshly assigned id, or replaces an
    /// existing one's value, kind and metadata while preserving its id (a
    /// distributed upsert). The sole create path, used by the event applier.
    pub fn insert(
        &mut self,
        tenant: Option<&str>,
        name: &str,
        value: Value,
        kind: ClusterVariableKind,
        metadata: BTreeMap<String, Value>,
    ) {
        let existing = self.get(tenant, name).map(|v| v.id);
        let id = existing.unwrap_or_else(|| {
            self.next_id += 1;
            self.next_id
        });
        let var = ClusterVariable {
            id,
            value,
            kind,
            metadata,
        };
        self.scope_mut(tenant).insert(name.to_string(), var);
    }

    /// Replaces an existing variable's value and metadata, preserving its id
    /// and kind. A missing variable is a no-op (the processor validates
    /// existence before emitting the event).
    pub fn update(
        &mut self,
        tenant: Option<&str>,
        name: &str,
        value: Value,
        metadata: BTreeMap<String, Value>,
    ) {
        let scope = match tenant {
            None => Some(&mut self.global),
            Some(t) => self.tenants.get_mut(t),
        };
        if let Some(var) = scope.and_then(|s| s.get_mut(name)) {
            var.value = value;
            var.metadata = metadata;
        }
    }

    /// Removes a variable (no-op when absent), dropping an emptied tenant map.
    pub fn remove(&mut self, tenant: Option<&str>, name: &str) {
        match tenant {
            None => {
                self.global.remove(name);
            }
            Some(t) => {
                if let Some(vars) = self.tenants.get_mut(t) {
                    vars.remove(name);
                    if vars.is_empty() {
                        self.tenants.remove(t);
                    }
                }
            }
        }
    }

    fn scope_mut(&mut self, tenant: Option<&str>) -> &mut BTreeMap<String, ClusterVariable> {
        match tenant {
            None => &mut self.global,
            Some(t) => self.tenants.entry(t.to_string()).or_default(),
        }
    }

    /// The cluster variables an instance running under `tenant` resolves, as
    /// `(name, value)` pairs: every global variable first, then the tenant's own
    /// variables (which shadow a same-named global). The caller layers the
    /// instance's own variables on top so they shadow both.
    pub fn resolved_for<'a>(
        &'a self,
        tenant: &str,
    ) -> impl Iterator<Item = (&'a String, &'a Value)> {
        self.global
            .iter()
            .chain(self.tenants.get(tenant).into_iter().flatten())
            .map(|(n, v)| (n, &v.value))
    }

    /// The `SECRET_REFERENCE`-kind cluster variables an instance running under
    /// `tenant` resolves, with their values: a name is trusted only when the
    /// binding that WINS the overlay (a tenant variable shadows a same-named
    /// global) is itself of kind `SECRET_REFERENCE` — a JSON-kind tenant
    /// variable shadowing a trusted global is untrusted (fail closed).
    pub fn trusted_secret_roots_for(&self, tenant: &str) -> HashMap<String, Value> {
        let mut out: HashMap<String, Value> = HashMap::new();
        for (name, var) in &self.global {
            if var.kind == ClusterVariableKind::SecretReference {
                out.insert(name.clone(), var.value.clone());
            }
        }
        if let Some(scoped) = self.tenants.get(tenant) {
            for (name, var) in scoped {
                if var.kind == ClusterVariableKind::SecretReference {
                    out.insert(name.clone(), var.value.clone());
                } else {
                    out.remove(name);
                }
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_update_remove_preserve_id_and_kind() {
        let mut s = ClusterVariableState::default();
        s.insert(
            None,
            "a",
            Value::Int(1),
            ClusterVariableKind::SecretReference,
            BTreeMap::new(),
        );
        s.insert(
            Some("t"),
            "a",
            Value::Int(2),
            ClusterVariableKind::Json,
            BTreeMap::new(),
        );
        assert_eq!(s.get(None, "a").map(|v| v.id), Some(1));
        assert_eq!(s.get(Some("t"), "a").map(|v| v.id), Some(2));
        // An upsert of an existing variable keeps its id.
        s.insert(
            Some("t"),
            "a",
            Value::Int(3),
            ClusterVariableKind::SecretReference,
            BTreeMap::new(),
        );
        assert_eq!(s.get(Some("t"), "a").map(|v| v.id), Some(2));
        assert_eq!(s.next_id, 2);
        s.update(None, "a", Value::Int(9), BTreeMap::new());
        let a = s.get(None, "a").cloned();
        assert_eq!(a.as_ref().map(|v| v.id), Some(1));
        assert_eq!(
            a.as_ref().map(|v| v.kind),
            Some(ClusterVariableKind::SecretReference)
        );
        assert_eq!(a.map(|v| v.value), Some(Value::Int(9)));
        s.update(Some("ghost"), "a", Value::Int(1), BTreeMap::new());
        assert!(!s.tenants.contains_key("ghost"), "update never creates");
        s.remove(Some("t"), "a");
        assert!(s.tenants.is_empty(), "an emptied tenant map is dropped");
        s.remove(None, "a");
        assert!(s.is_empty());
        assert!(!s.is_unused(), "the id counter survives deletion");
    }

    #[test]
    fn tenant_json_shadowing_a_trusted_global_is_untrusted() {
        let mut s = ClusterVariableState::default();
        s.insert(
            None,
            "cfg",
            Value::Int(1),
            ClusterVariableKind::SecretReference,
            BTreeMap::new(),
        );
        assert!(s.trusted_secret_roots_for("t").contains_key("cfg"));
        s.insert(
            Some("t"),
            "cfg",
            Value::Int(2),
            ClusterVariableKind::Json,
            BTreeMap::new(),
        );
        assert!(!s.trusted_secret_roots_for("t").contains_key("cfg"));
        assert!(s.trusted_secret_roots_for("other").contains_key("cfg"));
    }
}
