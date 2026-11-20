//! Derivation of the determinism upgrade-boundary table (#1702), shared by
//! `build.rs` (which generates `UPGRADE_BOUNDARIES` from it) and this crate's
//! unit tests (which pin its expiry-proof semantics). Pure: JSON text in,
//! sorted boundaries out.
//!
//! The table is derived from the append-only ledger
//! (`engine-core/examples/determinism/upgrade-boundaries.json`), NOT from the
//! divergence allowlist: allowlist entries expire (`compare.rs`
//! `check_expiry`), but the join restriction a boundary implies never does.
//! The allowlist is only cross-checked — every live `upgrade-boundary` entry
//! must already be recorded in the ledger under its `introduced_in` release.

use std::collections::BTreeMap;

use serde::Deserialize;

#[derive(Deserialize)]
struct Allowlist {
    #[serde(default)]
    observational_variants: Vec<Entry>,
    #[serde(default)]
    divergences: Vec<Entry>,
}

#[derive(Deserialize)]
struct Entry {
    /// `scenario` (divergences) or `variant` (observational variants).
    #[serde(alias = "variant")]
    scenario: String,
    introduced_in: String,
    issue: String,
    upgrade: String,
}

#[derive(Deserialize)]
struct Ledger {
    boundaries: Vec<LedgerBoundary>,
}

#[derive(Deserialize)]
struct LedgerBoundary {
    version: String,
    entries: Vec<String>,
}

/// One derived boundary: `(version without leading v, entry labels)`.
pub type DerivedBoundary = (String, Vec<String>);

/// Derives the boundary table, ascending by release, from the ledger, failing
/// when the ledger is malformed or misses a live allowlist boundary entry.
pub fn derive(ledger_json: &str, allowlist_json: &str) -> Result<Vec<DerivedBoundary>, String> {
    let ledger: Ledger =
        serde_json::from_str(ledger_json).map_err(|e| format!("parse ledger: {e}"))?;
    let allowlist: Allowlist =
        serde_json::from_str(allowlist_json).map_err(|e| format!("parse allowlist: {e}"))?;

    let mut by_release: BTreeMap<(u64, u64, u64), DerivedBoundary> = BTreeMap::new();
    for boundary in &ledger.boundaries {
        let version = boundary.version.trim_start_matches('v').to_string();
        let triple = parse_release(&version).ok_or_else(|| {
            format!(
                "ledger boundary {:?} is not a release tag",
                boundary.version
            )
        })?;
        if by_release
            .insert(triple, (version, boundary.entries.clone()))
            .is_some()
        {
            return Err(format!(
                "ledger records boundary {:?} twice",
                boundary.version
            ));
        }
    }

    for entry in allowlist
        .observational_variants
        .iter()
        .chain(allowlist.divergences.iter())
        .filter(|e| e.upgrade == "upgrade-boundary")
    {
        let triple =
            parse_release(entry.introduced_in.trim_start_matches('v')).ok_or_else(|| {
                format!(
                    "allowlist introduced_in {:?} is not a release tag",
                    entry.introduced_in
                )
            })?;
        let label = format!("{} ({})", entry.scenario, entry.issue);
        let recorded = by_release
            .get(&triple)
            .is_some_and(|(_, entries)| entries.contains(&label));
        if !recorded {
            return Err(format!(
                "allowlist upgrade-boundary entry {label:?} (introduced_in {}) is not recorded \
                 in upgrade-boundaries.json: append it to that release's boundary (the ledger \
                 is append-only)",
                entry.introduced_in
            ));
        }
    }
    Ok(by_release.into_values().collect())
}

/// `MAJOR.MINOR.PATCH` (leading `v` already stripped) as a sortable triple.
fn parse_release(version: &str) -> Option<(u64, u64, u64)> {
    let mut parts = version.split('.').map(|p| p.parse::<u64>().ok());
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some(Some(a)), Some(Some(b)), Some(Some(c)), None) => Some((a, b, c)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LEDGER: &str =
        r##"{"boundaries":[{"version":"0.0.28","entries":["fork-join (#1244)"]}]}"##;

    /// #1702 review: allowlist expiry removes the v0.0.28 entries once the
    /// previous release reaches v0.0.28 — the join restriction must survive.
    #[test]
    fn expiring_the_allowlist_entry_keeps_the_boundary() {
        let expired = r##"{"observational_variants":[],"divergences":[]}"##;
        assert_eq!(
            derive(LEDGER, expired),
            Ok(vec![(
                "0.0.28".to_string(),
                vec!["fork-join (#1244)".to_string()]
            )])
        );
    }

    /// A ratified allowlist boundary that the ledger does not record fails
    /// the derivation (and therefore the build): no boundary is ever only in
    /// the expiring allowlist.
    #[test]
    fn an_unrecorded_allowlist_boundary_fails() {
        let allowlist = r##"{"divergences":[{"scenario":"x","introduced_in":"v0.0.30","issue":"#9","upgrade":"upgrade-boundary"}]}"##;
        let err = derive(LEDGER, allowlist).expect_err("unrecorded boundary");
        assert!(err.contains("x (#9)"), "{err}");
        // Recorded under the WRONG release also fails.
        let allowlist = r##"{"divergences":[{"scenario":"fork-join","introduced_in":"v0.0.29","issue":"#1244","upgrade":"upgrade-boundary"}]}"##;
        assert!(derive(LEDGER, allowlist).is_err());
        // Non-boundary allowlist entries are not cross-checked.
        let allowlist = r##"{"divergences":[{"scenario":"x","introduced_in":"v0.0.30","issue":"#9","upgrade":"rolling"}]}"##;
        assert!(derive(LEDGER, allowlist).is_ok());
    }

    #[test]
    fn malformed_or_duplicate_ledger_versions_fail() {
        let empty = "{}";
        assert!(
            derive(
                r##"{"boundaries":[{"version":"dev","entries":[]}]}"##,
                empty
            )
            .is_err()
        );
        assert!(
            derive(
                r##"{"boundaries":[{"version":"0.0.28","entries":[]},{"version":"v0.0.28","entries":[]}]}"##,
                empty
            )
            .is_err()
        );
    }

    #[test]
    fn boundaries_sort_ascending() {
        let ledger = r##"{"boundaries":[{"version":"0.1.0","entries":[]},{"version":"0.0.28","entries":[]}]}"##;
        let derived = derive(ledger, "{}").expect("derive");
        assert_eq!(
            derived.iter().map(|(v, _)| v.as_str()).collect::<Vec<_>>(),
            ["0.0.28", "0.1.0"]
        );
    }
}
