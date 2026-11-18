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
//! deliver from a trusted source are resolved, and only at the exact target
//! path the mapping wrote. The engine captures this **mapping-time provenance**
//! (`SecretProvenance`: target path + secret names, never plaintext) when the
//! input mappings run, with the fail-closed
//! [`eval_with_provenance`](nanobpmn_engine_core::feel::eval_with_provenance)
//! attribution of #1636 against the exact view the mappings saw — trusted
//! roots are the `SECRET_REFERENCE`-kind cluster variables not shadowed by any
//! instance/scope variable, and a later mapping overwriting a path drops that
//! path's provenance — and journals it on `JobCreated`. Activation never
//! re-derives trust from the live cluster variables, so editing or deleting
//! one after the mapping ran changes nothing. References arriving via
//! request-body / process variables, JSON-kind cluster variables, static
//! (non-`=`) mapping literals, the bare cluster-variable overlay, or a sibling
//! path holding equal text are never resolved; nor are jobs created on a
//! deferred path (start listeners, incident re-drive, multi-instance / ad-hoc
//! children), which carry no provenance — false negatives (the reference text
//! stays visible) over false positives.
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

use std::collections::{HashMap, HashSet};

use nanobpmn_engine_core::{SecretProvenance, Value};

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

/// Substitutes every whole-string `camunda.secrets.<name>` reference at each
/// provenance site — only inside that site's dotted target `path`, and only for
/// the names trusted for that site (#1640). Provenance was captured when the
/// input mappings ran and journaled on `JobCreated`, so nothing here consults
/// the live cluster variables. Returns `Ok(None)` when nothing was substituted
/// (the caller keeps its shared variable snapshot), `Ok(Some(vars))` with the
/// resolved copy otherwise, or the first [`MissingSecret`] (sites visited in
/// path order, so the incident is deterministic).
pub(crate) fn resolve_job_variables(
    vars: &HashMap<String, Value>,
    provenance: &[SecretProvenance],
    store: &dyn SecretStore,
) -> Result<Option<HashMap<String, Value>>, MissingSecret> {
    let mut sites: Vec<&SecretProvenance> = provenance.iter().collect();
    sites.sort_by(|a, b| a.path.cmp(&b.path));
    let mut out: Option<HashMap<String, Value>> = None;
    for site in sites {
        let names: HashSet<&str> = site.secrets.iter().map(String::as_str).collect();
        let mut segments = site.path.split('.');
        let Some(root) = segments.next() else {
            continue;
        };
        let rest: Vec<&str> = segments.collect();
        let current = out.as_ref().unwrap_or(vars);
        let Some(mut value) = current.get(root).cloned() else {
            continue;
        };
        let Some(target) = select_mut(&mut value, &rest) else {
            continue;
        };
        if substitute(target, &names, store)? {
            out.get_or_insert_with(|| vars.clone())
                .insert(root.to_string(), value);
        }
    }
    Ok(out)
}

/// The value at a dotted member path below a variable; `None` when a segment
/// is absent or the value is not a context there.
fn select_mut<'v>(value: &'v mut Value, path: &[&str]) -> Option<&'v mut Value> {
    let mut v = value;
    for key in path {
        v = match v {
            Value::Map(entries) => entries.get_mut(*key)?,
            _ => return None,
        };
    }
    Some(v)
}

fn substitute(
    value: &mut Value,
    names: &HashSet<&str>,
    store: &dyn SecretStore,
) -> Result<bool, MissingSecret> {
    match value {
        Value::Str(s) => {
            let Some(name) = nanobpmn_engine_core::feel::secret_reference_name(s)
                .filter(|n| names.contains(n))
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
    use std::collections::BTreeMap;

    use super::*;

    fn s(v: &str) -> Value {
        Value::Str(v.to_string())
    }

    fn store() -> MapSecretStore {
        MapSecretStore(HashMap::from([
            ("TOKEN".to_string(), "s3cr3t".to_string()),
            ("DIRECT".to_string(), "direct-value".to_string()),
        ]))
    }

    fn site(path: &str, names: &[&str]) -> SecretProvenance {
        SecretProvenance {
            path: path.to_string(),
            secrets: names.iter().map(|n| n.to_string()).collect(),
        }
    }

    #[test]
    fn resolves_only_trusted_names_at_trusted_paths() {
        let vars = HashMap::from([
            ("a".to_string(), s("camunda.secrets.TOKEN")),
            (
                "b".to_string(),
                Value::Map(BTreeMap::from([
                    ("inner".to_string(), s("camunda.secrets.DIRECT")),
                    ("other".to_string(), s("camunda.secrets.DIRECT")),
                ])),
            ),
            ("c".to_string(), s("camunda.secrets.TOKEN")),
        ]);
        let provenance = [site("a", &["TOKEN"]), site("b.inner", &["DIRECT"])];
        let out = resolve_job_variables(&vars, &provenance, &store())
            .expect("resolvable")
            .expect("substituted");
        assert_eq!(out.get("a"), Some(&s("s3cr3t")));
        assert_eq!(
            out.get("b"),
            Some(&Value::Map(BTreeMap::from([
                ("inner".to_string(), s("direct-value")),
                ("other".to_string(), s("camunda.secrets.DIRECT")),
            ]))),
            "a sibling path holding equal reference text is never resolved"
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
    fn only_the_name_trusted_for_the_site_resolves() {
        let vars = HashMap::from([("a".to_string(), s("camunda.secrets.DIRECT"))]);
        assert_eq!(
            resolve_job_variables(&vars, &[site("a", &["TOKEN"])], &store()),
            Ok(None)
        );
    }

    #[test]
    fn nothing_trusted_is_a_no_op() {
        let vars = HashMap::from([("a".to_string(), s("camunda.secrets.TOKEN"))]);
        assert_eq!(resolve_job_variables(&vars, &[], &store()), Ok(None));
        assert_eq!(
            resolve_job_variables(&vars, &[site("a.missing", &["TOKEN"])], &store()),
            Ok(None),
            "a path that no longer exists resolves nothing"
        );
    }

    #[test]
    fn missing_secret_reports_the_upstream_incident_message() {
        let vars = HashMap::from([("a".to_string(), s("camunda.secrets.NOPE"))]);
        let err =
            resolve_job_variables(&vars, &[site("a", &["NOPE"])], &store()).expect_err("missing");
        assert_eq!(
            err.incident_message(),
            "Failed to resolve secret 'NOPE' from default. Ensure the secret exists and the store is available, then resolve the incident to retry."
        );
    }
}
