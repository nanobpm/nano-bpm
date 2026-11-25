//! Intra-cluster **join** handshake checks (#1661, ADR 0066 slice 2).
//!
//! Every `/cluster` WebSocket upgrade (both lanes of a `PeerLink`-style
//! connection) carries two headers in each direction:
//!
//! - [`CLUSTER_PARTITIONS_HEADER`]: the sender's `NANOBPMN_PARTITIONS`. The
//!   partition count decides key → partition placement and message-subscription
//!   routing, so it must be identical on every member for the life of the
//!   cluster. It is static deployment config (never changed at runtime), so it
//!   is **verified at join** rather than carried in replicated state: a member
//!   whose count differs is refused before any frame is exchanged, instead of
//!   silently routing to partitions the rest of the cluster places elsewhere.
//! - [`CLUSTER_FEATURES_HEADER`]: the replicated-command features this binary
//!   can decode. A leader emits a new command variant only once every member
//!   advertises the feature that introduced it (an older replica cannot decode
//!   it — the ADR 0066 "Rolling upgrade" row), and a node whose replicated
//!   state already depends on a feature refuses later joins that lack it.
//! - [`CLUSTER_VERSION_HEADER`]: the sender's release version
//!   (`NANOBPM_VERSION`, #1702). Raft members **re-execute** committed
//!   commands (ADR 0066, "Rolling upgrade", option C), so a determinism-affecting
//!   release the previous-release gate ratifies as an `upgrade-boundary` entry
//!   in `engine-core/examples/determinism/divergence-allowlist.json` (recorded in
//!   the append-only `upgrade-boundaries.json` ledger, which outlives allowlist
//!   expiry and is derived into [`UPGRADE_BOUNDARIES`] at build time)
//!   forbids mixed-version membership across it: a join whose two versions
//!   straddle a declared boundary is refused with
//!   [`JoinRejection::UpgradeBoundary`].
//!
//! A peer that sends no partition/feature headers is a pre-#1661 binary: it
//! is admitted (rolling upgrade) unless a feature is already required. A peer
//! that sends no version header is refused by any release-tagged node once a
//! boundary is declared — its side of the boundary is unknown (see
//! [`check_join`]).
//!
//!
/// Header carrying the sender's cluster-wide partition count.
pub const CLUSTER_PARTITIONS_HEADER: &str = "x-nano-cluster-partitions";

/// Header carrying the sender's supported replicated-command features
/// (comma-separated tokens).
pub const CLUSTER_FEATURES_HEADER: &str = "x-nano-cluster-features";

/// Header carrying the sender's release version (`NANOBPM_VERSION`, #1702).
pub const CLUSTER_VERSION_HEADER: &str = "x-nano-cluster-version";

/// Feature token: this binary decodes and applies
/// `Command::SetLenientCompletion` / `Event::LenientCompletionSet` (#1661).
pub const FEATURE_REPLICATED_LENIENT_COMPLETION: &str = "replicated-lenient-completion";

/// Replicated cluster-variable commands and events (#1657). Those Raft
/// entries are not decodable by an older binary, so a gateway emits them only
/// when every voter and learner advertises this (ADR 0066 rolling-upgrade gate).
pub const FEATURE_CLUSTER_VARIABLES: &str = "cluster-variables-v1";

/// Replica divergence digests (ADR 0066 slice 5, #1664): this binary records a
/// chain digest per applied Raft entry and compares the leader's claim. A
/// leader stamps claims only while EVERY other member advertises this, so a
/// mixed-version cluster never compares (and never false-positives).
pub const FEATURE_REPLICA_DIGEST: &str = "replica-digest-v1";

/// Replicated digest **cutover** (#1692, #1715 review): this binary decodes
/// and applies the `digest_cutover` marker on a replicated batch, re-basing
/// its chain at the cutover entry's index. Gated separately from
/// [`FEATURE_REPLICA_DIGEST`] because the pre-cutover #1688 binary already
/// advertises that token yet IGNORES the marker — a cutover gated on it would
/// leave those followers on their old chain base while the leader re-bases,
/// permanently disabling divergence comparison between them.
pub const FEATURE_REPLICA_DIGEST_CUTOVER: &str = "replica-digest-cutover-v1";

/// Envelope soft-job terminal deadlines (#1570): this binary applies
/// `ReplicatedCommand::terminal_deadlines`. A pre-#1570 binary decodes but
/// IGNORES the map (recording `deadline: None`), so a leader emits stamps only
/// while every member advertises this, and never ships a stamped entry to a
/// peer that does not.
pub const FEATURE_TERMINAL_DEADLINES: &str = "terminal-deadlines-v1";

/// Cluster-wide publication gate (#1845): this binary answers the
/// `AwaitPendingOpens` probe. A node answers a create before routing its
/// cross-partition subscription opens only while EVERY peer advertises this,
/// because a publish entering through a peer that cannot be asked would not be
/// ordered behind the pending open.
pub const FEATURE_CLUSTER_PUBLICATION_GATE: &str = "cluster-publication-gate-v1";

/// Message buffering (#1874): this binary honours `CorrelateMessage`'s
/// `time_to_live` / `message_id`. A pre-#1874 binary decodes but IGNORES them
/// (never buffering, and seeding the command differently when a `message_id`
/// is present), so an entry carrying either never ships to a peer that does
/// not advertise this.
pub const FEATURE_MESSAGE_BUFFERING: &str = "message-buffering-v1";

/// Cancel of a SUSPENDED root instance (#1863): this binary applies
/// `Command::CancelInstance { terminate_suspended: true }` by terminating a
/// SUSPENDED instance. An older binary ignores the flag and REJECTS the
/// cancel (leaving the instance SUSPENDED), so a leader clears the flag — the
/// old behaviour everywhere — unless every voter and learner advertises this,
/// and never ships a flagged entry to a peer that does not.
pub const FEATURE_CANCEL_SUSPENDED_INSTANCE: &str = "cancel-suspended-instance-v1";

/// Ack-paced buffered-message delivery (#1949, Zeebe `CORRELATING`): this
/// binary applies `Command::AcknowledgeMessageCorrelation` and honours
/// `CorrelateMessageSubscription::ack_partition`. A pre-#1949 binary would drop
/// the field (never acking, stalling the drain) and reject the ack, so an
/// entry carrying either never ships to a peer that does not advertise this.
pub const FEATURE_MESSAGE_CORRELATION_ACK: &str = "message-correlation-ack-v1";

/// Every replicated-command feature this binary supports — the single source
/// of truth for what the handshake advertises.
pub const LOCAL_CLUSTER_FEATURES: &[&str] = &[
    FEATURE_REPLICATED_LENIENT_COMPLETION,
    FEATURE_CLUSTER_VARIABLES,
    FEATURE_REPLICA_DIGEST,
    FEATURE_REPLICA_DIGEST_CUTOVER,
    FEATURE_TERMINAL_DEADLINES,
    FEATURE_CLUSTER_PUBLICATION_GATE,
    FEATURE_MESSAGE_BUFFERING,
    FEATURE_CANCEL_SUSPENDED_INSTANCE,
    FEATURE_MESSAGE_CORRELATION_ACK,
];

/// The [`CLUSTER_FEATURES_HEADER`] value this binary advertises.
pub fn local_features_header() -> String {
    LOCAL_CLUSTER_FEATURES.join(",")
}

/// The [`CLUSTER_VERSION_HEADER`] value this binary advertises: the release
/// this binary was built from (`NANOBPM_VERSION`, stamped by
/// `nano-version-stamp` from the pushed tag in CI, `git describe` locally).
pub fn local_version_header() -> String {
    advertised_version().to_string()
}

fn advertised_version() -> &'static str {
    advertised_version_override().unwrap_or(env!("NANOBPM_VERSION"))
}

#[cfg(feature = "test-hooks")]
fn advertised_version_override() -> Option<&'static str> {
    ADVERTISED_VERSION_OVERRIDE
        .get()
        .and_then(|f| *f.lock().unwrap_or_else(|e| e.into_inner()))
}

#[cfg(not(feature = "test-hooks"))]
fn advertised_version_override() -> Option<&'static str> {
    None
}

/// Test hook (`test-hooks` feature only): the version this binary advertises
/// on the join handshake, replacing `NANOBPM_VERSION`. The gateway binary's
/// mixed-version join tests (#1702) set this to model a member running a
/// different release. Process-global; tests that set it must not run in
/// parallel with each other.
///
/// Accepts ANY version: tests derive versions from the upgrade-boundary
/// ledger, which grows with every release, so a fixed list of accepted
/// strings broke the v0.12.0 release PR.
#[cfg(feature = "test-hooks")]
pub fn set_advertised_version_override(version: Option<&'static str>) {
    let cell = ADVERTISED_VERSION_OVERRIDE.get_or_init(Default::default);
    *cell.lock().unwrap_or_else(|e| e.into_inner()) = version;
}

#[cfg(feature = "test-hooks")]
type VersionOverride = std::sync::Mutex<Option<&'static str>>;

#[cfg(feature = "test-hooks")]
static ADVERTISED_VERSION_OVERRIDE: std::sync::OnceLock<VersionOverride> =
    std::sync::OnceLock::new();

/// A determinism **upgrade boundary** (#1702): the release `version` is the
/// first carrying the ratified divergences `entries`, so no rolling upgrade
/// crosses it — every member must run a version on the SAME side of it.
/// Derived at build time from the previous-release divergence allowlist (see
/// `build.rs`); never hand-maintained.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UpgradeBoundary {
    /// The boundary release (`MAJOR.MINOR.PATCH`, no leading `v`): versions
    /// below it and at/above it must not share cluster membership.
    pub version: &'static str,
    /// The allowlist entries that make this release a boundary (operator
    /// context for the rejection message / release notes).
    pub entries: &'static [&'static str],
}

include!(concat!(env!("OUT_DIR"), "/upgrade_boundaries.rs"));

/// `vMAJOR.MINOR.PATCH` (leading `v` optional) as a comparable triple. A
/// version that is not a plain release tag (a local `git describe` build such
/// as `0.0.27-12-gdeadbee`, or `dev`) parses as `None` and never refuses a
/// join: only stamped release binaries enforce boundaries.
pub fn parse_release_triple(tag: &str) -> Option<(u64, u64, u64)> {
    let core = tag.trim().trim_start_matches('v');
    let mut parts = core.split('.').map(str::parse::<u64>);
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some(Ok(a)), Some(Ok(b)), Some(Ok(c)), None) => Some((a, b, c)),
        _ => None,
    }
}

/// The boundary `local_version` and `peer_version` straddle, if any: the
/// first declared boundary at or above the HIGHER of the two versions that
/// the LOWER sits below. `None` when either version is not a release tag.
fn straddled_boundary(local_version: &str, peer_version: &str) -> Option<&'static UpgradeBoundary> {
    let local = parse_release_triple(local_version)?;
    let peer = parse_release_triple(peer_version)?;
    if local == peer {
        return None;
    }
    let (low, high) = if local < peer {
        (local, peer)
    } else {
        (peer, local)
    };
    UPGRADE_BOUNDARIES.iter().find(|b| {
        parse_release_triple(b.version).is_some_and(|boundary| low < boundary && boundary <= high)
    })
}

/// Parses a [`CLUSTER_FEATURES_HEADER`] value into its feature tokens. An
/// absent header (a pre-#1661 binary) supports nothing.
pub fn parse_features(header: Option<&str>) -> Vec<String> {
    header
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|f| !f.is_empty())
        .map(str::to_string)
        .collect()
}

/// Why a cluster join was refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JoinRejection {
    /// The peer runs with a different `NANOBPMN_PARTITIONS`.
    PartitionMismatch { local: u64, remote: u64 },
    /// The peer's partition header is not a positive integer.
    MalformedPartitions(String),
    /// The cluster's replicated state already depends on a feature the peer's
    /// binary does not advertise (an incompatible, older binary).
    MissingFeature(&'static str),
    /// The two versions straddle a determinism **upgrade boundary** (#1702):
    /// the boundary release carries a ratified `upgrade-boundary` divergence,
    /// and Raft members re-execute committed commands (ADR 0066, "Rolling
    /// upgrade", option C), so mixed-version members would journal different
    /// events for the same command. No rolling upgrade across it — quiesce
    /// and upgrade all nodes together.
    UpgradeBoundary {
        /// This node's release version.
        local_version: String,
        /// The peer's release version (`unknown` when it sent no version
        /// header — a pre-#1702 binary, see [`check_join`]).
        peer_version: String,
        /// The boundary release the two versions straddle.
        boundary: &'static str,
    },
}

impl std::fmt::Display for JoinRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinRejection::PartitionMismatch { local, remote } => write!(
                f,
                "cluster partition count mismatch: this node runs NANOBPMN_PARTITIONS={local}, \
                 the peer runs {remote}; every member must use the same value"
            ),
            JoinRejection::MalformedPartitions(raw) => {
                write!(f, "malformed {CLUSTER_PARTITIONS_HEADER} header {raw:?}")
            }
            JoinRejection::MissingFeature(feature) => write!(
                f,
                "incompatible peer binary: the cluster's replicated state requires feature \
                 {feature:?}, which the peer does not advertise (upgrade the peer)"
            ),
            JoinRejection::UpgradeBoundary {
                local_version,
                peer_version,
                boundary,
            } => write!(
                f,
                "no rolling upgrade across determinism boundary v{boundary}: this node runs \
                 v{local_version}, the peer runs v{peer_version}; release v{boundary} carries a \
                 ratified determinism divergence (see the release notes), so a mixed-version \
                 cluster would disagree — quiesce the cluster and upgrade all nodes together"
            ),
        }
    }
}

impl std::error::Error for JoinRejection {}

/// Decides whether a peer presenting `partitions` / `features` / `version`
/// headers may join a node running `local_partitions` and `local_version`
/// whose replicated state requires `required_features`. A missing partition
/// header (pre-#1661 binary) is not itself a rejection; every required
/// feature must be advertised.
///
/// Upgrade boundary (#1702): when both versions are release tags and straddle
/// a declared [`UPGRADE_BOUNDARIES`] entry the join is refused. A MISSING
/// peer version (a pre-#1702 binary, which sends no version header) fails
/// CLOSED whenever the local version is a release tag and any boundary is
/// declared: nothing about the local release proves the versionless peer sits
/// on the same side of every boundary (a v0.0.27 peer is versionless whether
/// it dials v0.0.28 or v0.0.29), so compatibility cannot be established.
pub fn check_join(
    local_partitions: u64,
    partitions: Option<&str>,
    features: Option<&str>,
    required_features: &[&'static str],
    local_version: &str,
    peer_version: Option<&str>,
) -> Result<(), JoinRejection> {
    if let Some(raw) = partitions {
        let remote = raw
            .trim()
            .parse::<u64>()
            .ok()
            .filter(|n| *n >= 1)
            .ok_or_else(|| JoinRejection::MalformedPartitions(raw.to_string()))?;
        if remote != local_partitions {
            return Err(JoinRejection::PartitionMismatch {
                local: local_partitions,
                remote,
            });
        }
    }
    let advertised = parse_features(features);
    for required in required_features {
        if !advertised.iter().any(|f| f == required) {
            return Err(JoinRejection::MissingFeature(required));
        }
    }
    let boundary = match peer_version {
        Some(peer) => straddled_boundary(local_version, peer),
        // Fail closed: a versionless peer may sit below any declared
        // boundary. Report the newest boundary at/above the local release,
        // else the newest one overall.
        None => parse_release_triple(local_version).and_then(|local| {
            UPGRADE_BOUNDARIES
                .iter()
                .find(|b| parse_release_triple(b.version).is_some_and(|boundary| local <= boundary))
                .or_else(|| UPGRADE_BOUNDARIES.last())
        }),
    };
    if let Some(boundary) = boundary {
        return Err(JoinRejection::UpgradeBoundary {
            local_version: local_version.to_string(),
            peer_version: peer_version.unwrap_or("unknown").to_string(),
            boundary: boundary.version,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matching_partitions_and_features_join() {
        let features = local_features_header();
        assert_eq!(
            check_join(
                4,
                Some("4"),
                Some(&features),
                &[FEATURE_REPLICATED_LENIENT_COMPLETION],
                "0.0.29",
                Some("0.0.29")
            ),
            Ok(())
        );
    }

    #[test]
    fn a_partition_count_mismatch_is_refused() {
        assert_eq!(
            check_join(
                4,
                Some("8"),
                Some(&local_features_header()),
                &[],
                "0.0.29",
                Some("0.0.29")
            ),
            Err(JoinRejection::PartitionMismatch {
                local: 4,
                remote: 8
            })
        );
        assert!(matches!(
            check_join(4, Some("zero"), None, &[], "0.0.29", Some("0.0.29")),
            Err(JoinRejection::MalformedPartitions(_))
        ));
        assert!(matches!(
            check_join(4, Some("0"), None, &[], "0.0.29", Some("0.0.29")),
            Err(JoinRejection::MalformedPartitions(_))
        ));
    }

    #[test]
    fn a_legacy_peer_joins_until_a_feature_is_required() {
        // A pre-#1661 binary sends no headers at all: admitted during a
        // rolling upgrade ...
        // (Version-compatible: the version rule is exercised separately.)
        assert_eq!(
            check_join(4, None, None, &[], "0.0.29", Some("0.0.29")),
            Ok(())
        );
        // ... but refused once the cluster's replicated state depends on a
        // command variant it cannot decode.
        assert_eq!(
            check_join(
                4,
                None,
                None,
                &[FEATURE_REPLICATED_LENIENT_COMPLETION],
                "0.0.29",
                Some("0.0.29")
            ),
            Err(JoinRejection::MissingFeature(
                FEATURE_REPLICATED_LENIENT_COMPLETION
            ))
        );
    }

    #[test]
    fn features_parse_tolerantly() {
        assert_eq!(parse_features(None), Vec::<String>::new());
        assert_eq!(parse_features(Some(" a, ,b ")), vec!["a", "b"]);
        assert_eq!(
            parse_features(Some(&local_features_header())),
            LOCAL_CLUSTER_FEATURES
                .iter()
                .map(|f| f.to_string())
                .collect::<Vec<_>>()
        );
    }

    // ---- #1702: determinism upgrade-boundary enforcement -----------------

    /// The checked-in allowlist declares `upgrade-boundary` entries introduced
    /// in `v0.0.28` (#1244 fork/join order, #1402 `GatewayRouteSelected`), so
    /// the derived boundary list names that release (the build fails if the
    /// append-only `upgrade-boundaries.json` ledger misses one of them).
    #[test]
    fn the_upgrade_boundaries_derive_from_the_allowlist() {
        assert!(
            !UPGRADE_BOUNDARIES.is_empty(),
            "the checked-in allowlist has upgrade-boundary entries; the derived table must not be empty"
        );
        let v0028 = UPGRADE_BOUNDARIES
            .iter()
            .find(|b| b.version == "0.0.28")
            .expect("the allowlist's v0.0.28 upgrade-boundary entries derive a boundary");
        assert!(
            v0028.entries.iter().any(|e| e.contains("#1244")),
            "the v0.0.28 boundary names its #1244 divergence entries: {v0028:?}"
        );
        assert!(
            v0028.entries.iter().any(|e| e.contains("#1402")),
            "the v0.0.28 boundary names its #1402 observational variant: {v0028:?}"
        );
        // Sorted ascending, so the FIRST boundary at/above a version is the
        // decisive one.
        let mut sorted = UPGRADE_BOUNDARIES.to_vec();
        sorted.sort_by(|a, b| {
            parse_release_triple(a.version)
                .zip(parse_release_triple(b.version))
                .map(|(a, b)| a.cmp(&b))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        assert_eq!(sorted, UPGRADE_BOUNDARIES.to_vec());
    }

    /// #1702: a mixed-version join ACROSS a declared boundary is refused with
    /// a typed, operator-actionable error naming both versions and the
    /// boundary; a join within the same boundary region still succeeds.
    #[test]
    fn a_mixed_version_join_across_an_upgrade_boundary_is_refused() {
        let features = local_features_header();
        // New member (0.0.28, carries the #1244/#1402 behaviour) dials an
        // old member (0.0.27): refused, naming both versions and v0.0.28.
        let Err(rejection) =
            check_join(4, Some("4"), Some(&features), &[], "0.0.28", Some("0.0.27"))
        else {
            panic!("a join across the v0.0.28 boundary must be refused");
        };
        let JoinRejection::UpgradeBoundary {
            local_version,
            peer_version,
            boundary,
        } = &rejection
        else {
            panic!("the rejection is the typed upgrade-boundary error, got {rejection:?}");
        };
        assert_eq!(*boundary, "0.0.28");
        assert_eq!(
            (local_version.as_str(), peer_version.as_str()),
            ("0.0.28", "0.0.27")
        );
        // Operator-actionable: the message names both versions, the boundary,
        // and what to do (quiesce and upgrade all nodes).
        let msg = rejection.to_string();
        for needle in ["0.0.28", "0.0.27", "quiesce", "upgrade"] {
            assert!(msg.contains(needle), "the error names {needle:?}: {msg}");
        }
        // The symmetric direction (old dials new) is refused identically.
        assert!(matches!(
            check_join(4, Some("4"), Some(&features), &[], "0.0.27", Some("0.0.28")),
            Err(JoinRejection::UpgradeBoundary { boundary, .. }) if boundary == "0.0.28"
        ));
        // A join WITHIN the same boundary region still succeeds.
        assert_eq!(
            check_join(4, Some("4"), Some(&features), &[], "0.0.28", Some("0.0.28")),
            Ok(())
        );
        // Past the NEWEST declared boundary, two adjacent releases share a
        // region. Derived from the ledger (not a hard-coded pair) so a later
        // boundary — e.g. one declared at v0.0.29 — cannot stale this check.
        let newest = UPGRADE_BOUNDARIES
            .last()
            .and_then(|b| parse_release_triple(b.version))
            .expect("the ledger declares at least the v0.0.28 boundary");
        let past = format!("{}.{}.{}", newest.0, newest.1, newest.2 + 1);
        let past_next = format!("{}.{}.{}", newest.0, newest.1, newest.2 + 2);
        assert_eq!(
            check_join(4, Some("4"), Some(&features), &[], &past_next, Some(&past)),
            Ok(())
        );
        assert_eq!(
            check_join(4, Some("4"), Some(&features), &[], "0.0.26", Some("0.0.27")),
            Ok(())
        );
    }

    /// A pre-#1702 peer sends no version header, so its side of every
    /// boundary is unknown: a release-tagged node refuses it whether it is AT
    /// the boundary release or PAST it (a v0.0.27 peer is just as versionless
    /// when it dials v0.0.29). Only a non-release (dev) local build admits it.
    #[test]
    fn a_versionless_peer_is_refused_by_every_release() {
        let features = local_features_header();
        for local in ["0.0.27", "0.0.28", "0.0.29", "1.0.0"] {
            assert!(
                matches!(
                    check_join(4, Some("4"), Some(&features), &[], local, None),
                    Err(JoinRejection::UpgradeBoundary { ref peer_version, .. })
                        if peer_version == "unknown"
                ),
                "local {local} must refuse a versionless peer"
            );
        }
        assert_eq!(
            check_join(4, Some("4"), Some(&features), &[], "dev", None),
            Ok(())
        );
    }

    /// #1702 review: a boundary restricts joins across it FOREVER, not only
    /// between adjacent releases — v0.0.27 and v0.0.29 both advertise and
    /// still straddle v0.0.28.
    #[test]
    fn a_join_across_a_non_adjacent_boundary_is_refused() {
        let features = local_features_header();
        for (local, peer) in [
            ("0.0.29", "0.0.27"),
            ("0.0.27", "0.0.29"),
            ("1.2.3", "0.0.1"),
        ] {
            assert!(
                matches!(
                    check_join(4, Some("4"), Some(&features), &[], local, Some(peer)),
                    Err(JoinRejection::UpgradeBoundary { boundary, .. }) if boundary == "0.0.28"
                ),
                "{local} vs {peer}"
            );
        }
    }

    /// A version that is not a release tag (a local `git describe` build,
    /// e.g. `0.0.27-12-gdeadbee`) never refuses: dev builds are not
    /// deployable release members, and refusing them would break every local
    /// multi-node run.
    #[test]
    fn unparsable_versions_never_refuse() {
        let features = local_features_header();
        for (local, peer) in [
            ("0.0.27-12-gdeadbee", "0.0.28"),
            ("0.0.28", "0.0.27-12-gdeadbee"),
            ("dev", "0.0.28"),
        ] {
            assert_eq!(
                check_join(4, Some("4"), Some(&features), &[], local, Some(peer)),
                Ok(()),
                "{local} vs {peer}"
            );
        }
    }
}
