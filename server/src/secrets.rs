//! Activation-time resolution of trusted `camunda.secrets.<name>` references
//! (#1640, follow-up to #1296).
//!
//! A `SECRET_REFERENCE`-kind cluster variable (or a literal
//! `camunda.secrets.<name>` path in an input-mapping expression) carries
//! *reference text*; the engine passes it through verbatim, so the journal,
//! snapshots, events and instance variables only ever hold the reference. The
//! real value is substituted here, at **job activation**, into the
//! worker-facing activated-job payload only — mirroring Zeebe's
//! `ClusterVariableJobSecretResolver`.
//!
//! **Trust.** Only references a job's element input mappings *structurally*
//! deliver from a trusted source are resolved, and only inside that mapping's
//! target variable. Provenance reuses the fail-closed
//! [`eval_with_provenance`](nanobpmn_engine_core::feel::eval_with_provenance)
//! attribution of `POST /v2/expression/evaluation` (`referencedSecrets`): a
//! mapping source is evaluated against the cluster variables alone, with the
//! `SECRET_REFERENCE`-kind ones as the trusted roots. References arriving via
//! request-body / process variables, JSON-kind cluster variables, static
//! (non-`=`) mapping literals, or the bare cluster-variable overlay are never
//! resolved — false negatives (the reference text stays visible) over false
//! positives (an attacker-chosen reference steering resolution).
//!
//! **Store.** The secret provider follows the ADR 0027 §5 env-pointer
//! convention: secret `<name>` is the environment variable
//! `<prefix><name>`, where the prefix is `NANOBPMN_SECRET_ENV_PREFIX`
//! (default [`DEFAULT_SECRET_ENV_PREFIX`]). The store id is `default`, the same
//! id `referencedSecrets` reports.
//!
//! **Missing secret.** Matching Zeebe's
//! `SecretReferenceBatchCreateIncidentsProcessor`, an unresolvable secret
//! raises an incident on the element (the job is failed with no retries and
//! the upstream message) instead of passing the literal through.

use std::collections::{BTreeMap, HashMap, HashSet};

use nanobpmn_engine_core::{Mapping, Value};

/// The store id of the env-pointer secret store (also the `storeId` the
/// expression endpoint's `referencedSecrets` reports).
pub(crate) const DEFAULT_SECRET_STORE_ID: &str = "default";

/// The env var that overrides the secret env-var prefix.
pub(crate) const SECRET_ENV_PREFIX_ENV: &str = "NANOBPMN_SECRET_ENV_PREFIX";

/// The default prefix: secret `TOKEN` is read from `NANOBPMN_SECRET_TOKEN`. A
/// prefix keeps a trusted reference from reading arbitrary process
/// environment (e.g. cloud credentials) that was never meant as a secret.
pub(crate) const DEFAULT_SECRET_ENV_PREFIX: &str = "NANOBPMN_SECRET_";

/// A secret provider consulted at job activation.
pub(crate) trait SecretStore: Send + Sync {
    /// The store id surfaced in incident messages.
    fn store_id(&self) -> &str;
    /// The secret's plaintext, or `None` when it does not exist.
    fn get(&self, name: &str) -> Option<String>;
}

/// The ADR 0027 §5 env-pointer secret store.
pub(crate) struct EnvSecretStore {
    prefix: String,
}

impl EnvSecretStore {
    /// Builds the store from `NANOBPMN_SECRET_ENV_PREFIX` (default
    /// [`DEFAULT_SECRET_ENV_PREFIX`]).
    pub(crate) fn from_env() -> Self {
        Self {
            prefix: std::env::var(SECRET_ENV_PREFIX_ENV)
                .unwrap_or_else(|_| DEFAULT_SECRET_ENV_PREFIX.to_string()),
        }
    }
}

impl SecretStore for EnvSecretStore {
    fn store_id(&self) -> &str {
        DEFAULT_SECRET_STORE_ID
    }

    fn get(&self, name: &str) -> Option<String> {
        std::env::var(format!("{}{name}", self.prefix)).ok()
    }
}

/// An unresolvable secret: the incident message mirrors upstream Zeebe's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MissingSecret {
    pub(crate) name: String,
    pub(crate) store_id: String,
}

impl MissingSecret {
    /// The incident error message (upstream
    /// `SecretReferenceBatchCreateIncidentsProcessor` wording).
    pub(crate) fn incident_message(&self) -> String {
        format!(
            "Failed to resolve secret '{}' from {}. Ensure the secret exists and the store is available, then resolve the incident to retry.",
            self.name, self.store_id
        )
    }
}

/// The trusted secret names per input-mapping **target root variable**: for
/// each FEEL (`=`-prefixed) input mapping, the names
/// [`eval_with_provenance`](nanobpmn_engine_core::feel::eval_with_provenance)
/// structurally attributes to a trusted source when the source is evaluated
/// against `cluster_ctx` (the cluster variables visible to the job) with
/// `trusted_roots` (the `SECRET_REFERENCE`-kind ones) as trusted carriers.
/// Static literals and unevaluable sources contribute nothing (fail-closed).
pub(crate) fn trusted_secrets_by_target(
    inputs: &[Mapping],
    cluster_ctx: &HashMap<String, Value>,
    trusted_roots: &[String],
) -> HashMap<String, HashSet<String>> {
    let mut out: HashMap<String, HashSet<String>> = HashMap::new();
    for mapping in inputs {
        if !mapping.source.trim_start().starts_with('=') {
            continue;
        }
        let Ok(evaluated) = nanobpmn_engine_core::feel::eval_with_provenance(
            mapping.source.trim(),
            cluster_ctx,
            trusted_roots.iter().cloned(),
        ) else {
            continue;
        };
        if evaluated.referenced_secrets.is_empty() {
            continue;
        }
        let root = mapping
            .target
            .split('.')
            .next()
            .unwrap_or(&mapping.target)
            .to_string();
        out.entry(root)
            .or_default()
            .extend(evaluated.referenced_secrets);
    }
    out
}

/// Substitutes every whole-string `camunda.secrets.<name>` reference inside the
/// trusted target variables whose `<name>` is trusted for that target. Returns
/// `Ok(None)` when nothing was substituted (the caller keeps its shared
/// variable snapshot), `Ok(Some(vars))` with the resolved copy otherwise, or
/// the first [`MissingSecret`] (targets visited in name order, so the incident
/// is deterministic).
pub(crate) fn resolve_job_variables(
    vars: &HashMap<String, Value>,
    trusted: &HashMap<String, HashSet<String>>,
    store: &dyn SecretStore,
) -> Result<Option<HashMap<String, Value>>, MissingSecret> {
    let mut targets: Vec<&String> = trusted.keys().collect();
    targets.sort();
    let mut resolved: BTreeMap<String, Value> = BTreeMap::new();
    for target in targets {
        let (Some(value), Some(names)) = (vars.get(target), trusted.get(target)) else {
            continue;
        };
        let mut value = value.clone();
        if substitute(&mut value, names, store)? {
            resolved.insert(target.clone(), value);
        }
    }
    if resolved.is_empty() {
        return Ok(None);
    }
    let mut out = vars.clone();
    out.extend(resolved);
    Ok(Some(out))
}

fn substitute(
    value: &mut Value,
    names: &HashSet<String>,
    store: &dyn SecretStore,
) -> Result<bool, MissingSecret> {
    match value {
        Value::Str(s) => {
            let Some(name) = nanobpmn_engine_core::feel::secret_reference_name(s)
                .filter(|n| names.contains(*n))
                .map(str::to_owned)
            else {
                return Ok(false);
            };
            match store.get(&name) {
                Some(secret) => {
                    *s = secret;
                    Ok(true)
                }
                None => Err(MissingSecret {
                    name,
                    store_id: store.store_id().to_string(),
                }),
            }
        }
        Value::List(items) => {
            let mut changed = false;
            for item in items {
                changed |= substitute(item, names, store)?;
            }
            Ok(changed)
        }
        Value::Map(entries) => {
            let mut changed = false;
            for item in entries.values_mut() {
                changed |= substitute(item, names, store)?;
            }
            Ok(changed)
        }
        _ => Ok(false),
    }
}

/// A fixed in-memory store for tests (env vars are process-global, so tests
/// never mutate them).
#[cfg(test)]
pub(crate) struct MapSecretStore(pub(crate) HashMap<String, String>);

#[cfg(test)]
impl SecretStore for MapSecretStore {
    fn store_id(&self) -> &str {
        DEFAULT_SECRET_STORE_ID
    }

    fn get(&self, name: &str) -> Option<String> {
        self.0.get(name).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &str) -> Value {
        Value::Str(v.to_string())
    }

    fn mapping(source: &str, target: &str) -> Mapping {
        Mapping {
            source: source.to_string(),
            target: target.to_string(),
        }
    }

    fn store() -> MapSecretStore {
        MapSecretStore(HashMap::from([
            ("TOKEN".to_string(), "s3cr3t".to_string()),
            ("DIRECT".to_string(), "direct-value".to_string()),
        ]))
    }

    #[test]
    fn trust_matrix_of_input_mappings() {
        let cluster = HashMap::from([
            ("secretCfg".to_string(), s("camunda.secrets.TOKEN")),
            ("jsonCfg".to_string(), s("camunda.secrets.JSONREF")),
        ]);
        let roots = vec!["secretCfg".to_string()];
        let trusted = trusted_secrets_by_target(
            &[
                mapping("=secretCfg", "a"),
                mapping("=camunda.secrets.DIRECT", "b.inner"),
                mapping("=jsonCfg", "c"),
                mapping("=bodyVar", "d"),
                mapping("camunda.secrets.STATIC", "e"),
                mapping("=if true then secretCfg else jsonCfg", "f"),
            ],
            &cluster,
            &roots,
        );
        assert_eq!(
            trusted.get("a"),
            Some(&HashSet::from(["TOKEN".to_string()]))
        );
        assert_eq!(
            trusted.get("b"),
            Some(&HashSet::from(["DIRECT".to_string()])),
            "a dotted target keys on its root variable"
        );
        for untrusted in ["c", "d", "e", "f"] {
            assert!(
                !trusted.contains_key(untrusted),
                "{untrusted} must not be trusted"
            );
        }
    }

    #[test]
    fn resolves_only_trusted_names_in_trusted_targets() {
        let vars = HashMap::from([
            ("a".to_string(), s("camunda.secrets.TOKEN")),
            (
                "b".to_string(),
                Value::Map(BTreeMap::from([
                    ("inner".to_string(), s("camunda.secrets.DIRECT")),
                    ("other".to_string(), s("camunda.secrets.TOKEN")),
                ])),
            ),
            ("c".to_string(), s("camunda.secrets.TOKEN")),
        ]);
        let trusted = HashMap::from([
            ("a".to_string(), HashSet::from(["TOKEN".to_string()])),
            ("b".to_string(), HashSet::from(["DIRECT".to_string()])),
        ]);
        let out = resolve_job_variables(&vars, &trusted, &store())
            .expect("resolvable")
            .expect("substituted");
        assert_eq!(out.get("a"), Some(&s("s3cr3t")));
        assert_eq!(
            out.get("b"),
            Some(&Value::Map(BTreeMap::from([
                ("inner".to_string(), s("direct-value")),
                ("other".to_string(), s("camunda.secrets.TOKEN")),
            ]))),
            "only the name trusted for this target is resolved"
        );
        assert_eq!(
            out.get("c"),
            Some(&s("camunda.secrets.TOKEN")),
            "an untrusted variable keeps its reference text"
        );
        assert_eq!(
            vars.get("a"),
            Some(&s("camunda.secrets.TOKEN")),
            "the source snapshot is never mutated"
        );
    }

    #[test]
    fn nothing_trusted_is_a_no_op() {
        let vars = HashMap::from([("a".to_string(), s("camunda.secrets.TOKEN"))]);
        assert_eq!(
            resolve_job_variables(&vars, &HashMap::new(), &store()),
            Ok(None)
        );
    }

    #[test]
    fn missing_secret_reports_the_upstream_incident_message() {
        let vars = HashMap::from([("a".to_string(), s("camunda.secrets.NOPE"))]);
        let trusted = HashMap::from([("a".to_string(), HashSet::from(["NOPE".to_string()]))]);
        let err = resolve_job_variables(&vars, &trusted, &store()).expect_err("missing");
        assert_eq!(
            err.incident_message(),
            "Failed to resolve secret 'NOPE' from default. Ensure the secret exists and the store is available, then resolve the incident to retry."
        );
    }
}
