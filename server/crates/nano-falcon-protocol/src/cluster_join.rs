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
//!
//! A peer that sends neither header is a pre-#1661 binary: it is admitted
//! (rolling upgrade) unless a feature is already required.
//!
//!
/// Header carrying the sender's cluster-wide partition count.
pub const CLUSTER_PARTITIONS_HEADER: &str = "x-nano-cluster-partitions";

/// Header carrying the sender's supported replicated-command features
/// (comma-separated tokens).
pub const CLUSTER_FEATURES_HEADER: &str = "x-nano-cluster-features";

/// Feature token: this binary decodes and applies
/// `Command::SetLenientCompletion` / `Event::LenientCompletionSet` (#1661).
pub const FEATURE_REPLICATED_LENIENT_COMPLETION: &str = "replicated-lenient-completion";

/// Replicated cluster-variable commands and events (#1657). Those Raft
/// entries are not decodable by an older binary, so a gateway emits them only
/// when every voter and learner advertises this (ADR 0066 rolling-upgrade gate).
pub const FEATURE_CLUSTER_VARIABLES: &str = "cluster-variables-v1";

/// Every replicated-command feature this binary supports — the single source
/// of truth for what the handshake advertises.
pub const LOCAL_CLUSTER_FEATURES: &[&str] = &[
    FEATURE_REPLICATED_LENIENT_COMPLETION,
    FEATURE_CLUSTER_VARIABLES,
];

/// The [`CLUSTER_FEATURES_HEADER`] value this binary advertises.
pub fn local_features_header() -> String {
    LOCAL_CLUSTER_FEATURES.join(",")
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
        }
    }
}

impl std::error::Error for JoinRejection {}

/// Decides whether a peer presenting `partitions` / `features` headers may
/// join a node running `local_partitions` whose replicated state requires
/// `required_features`. A missing partition header (pre-#1661 binary) is not
/// itself a rejection; every required feature must be advertised.
pub fn check_join(
    local_partitions: u64,
    partitions: Option<&str>,
    features: Option<&str>,
    required_features: &[&'static str],
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
                &[FEATURE_REPLICATED_LENIENT_COMPLETION]
            ),
            Ok(())
        );
    }

    #[test]
    fn a_partition_count_mismatch_is_refused() {
        assert_eq!(
            check_join(4, Some("8"), Some(&local_features_header()), &[]),
            Err(JoinRejection::PartitionMismatch {
                local: 4,
                remote: 8
            })
        );
        assert!(matches!(
            check_join(4, Some("zero"), None, &[]),
            Err(JoinRejection::MalformedPartitions(_))
        ));
        assert!(matches!(
            check_join(4, Some("0"), None, &[]),
            Err(JoinRejection::MalformedPartitions(_))
        ));
    }

    #[test]
    fn a_legacy_peer_joins_until_a_feature_is_required() {
        // A pre-#1661 binary sends no headers at all: admitted during a
        // rolling upgrade ...
        assert_eq!(check_join(4, None, None, &[]), Ok(()));
        // ... but refused once the cluster's replicated state depends on a
        // command variant it cannot decode.
        assert_eq!(
            check_join(4, None, None, &[FEATURE_REPLICATED_LENIENT_COMPLETION]),
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
}
