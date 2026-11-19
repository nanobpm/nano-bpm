//! Replica divergence digest (ADR 0066 slice 5, "Divergence digest (option C)",
//! issue #1664).
//!
//! Every replica folds a **chain digest** over the records each applied Raft
//! entry journals:
//!
//! ```text
//! h_i = SHA-256( DOMAIN_V1 || h_{i-1} || index_i (u64 LE) || n_i (u64 LE)
//!                || json(event_1) || '\n' || … || json(event_n) || '\n' )
//! ```
//!
//! where `event_1..n` are exactly the command events entry `i` journals in its
//! atomic frame (the trailing `RaftEntryApplied` record — which *carries* the
//! digest — is excluded, so there is no self-reference). Because engine state
//! is the fold of its events (#1071) and, since slice 3 (#1662), the event
//! encoding is canonical (ordered collections), two replicas with equal chains
//! journaled byte-identical records and hold equal state.
//!
//! The chain is persisted inside the replication layer's opaque `raft_state`
//! string of every `RaftEntryApplied` record as an additive `digest` field
//! ([`EntryDigest`]), so it travels with the journal, with every snapshot (the
//! engine snapshot carries the last applied record) and with the
//! migration-by-replay path (#1071), which re-derives it with
//! [`verify_journal_chain`] and fails closed on a mismatch.
//!
//! `H` is SHA-256 (`sha2`, already in the dependency tree): a stable,
//! platform-independent hash pinned here and versioned by [`CHAIN_VERSION`] —
//! never std's `DefaultHasher`, whose output is unspecified across releases.
//!
//! **Bases.** A chain starts (`base`) at the first entry a replica folds with
//! no predecessor digest — index `0`/`1` for a cluster that ran this code from
//! genesis, or the first post-upgrade entry for a replica upgraded in place.
//! Two digests are only comparable when their `version` and `base` agree
//! ([`ChainDigest::comparable`]); an incomparable pair is never reported as a
//! divergence (no false positives across a rolling upgrade). A snapshot install
//! adopts the leader's chain (base included), so every replica converges on one
//! comparable chain after its next install.

use nanobpmn_engine_core::Event;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

/// The pinned chain-hash version. Bump (and re-base every chain) only together
/// with a deliberate change of the fold or of the canonical event encoding
/// (a `SNAPSHOT_FORMAT_VERSION` bump — ADR 0066).
pub const CHAIN_VERSION: u32 = 1;

/// Domain separator for [`CHAIN_VERSION`] 1.
const DOMAIN_V1: &[u8] = b"nanobpm/raft-chain-digest/v1\0";

/// A replica's chain digest after some applied Raft entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChainDigest {
    /// Hash version ([`CHAIN_VERSION`]).
    #[serde(default = "default_version", rename = "v")]
    pub version: u32,
    /// The Raft log index the chain was (re)based at.
    pub base: u64,
    /// The chain value, hex-encoded on the wire.
    #[serde(with = "hex32")]
    pub hash: [u8; 32],
}

fn default_version() -> u32 {
    CHAIN_VERSION
}

impl ChainDigest {
    /// The empty chain a replica starts from when entry `base` is the first it
    /// folds with no predecessor digest.
    pub fn genesis(base: u64) -> Self {
        Self {
            version: CHAIN_VERSION,
            base,
            hash: [0; 32],
        }
    }

    /// Whether `self` and `other` hash the same chain definition from the same
    /// starting point, so an inequality of their hashes is a real divergence.
    pub fn comparable(&self, other: &Self) -> bool {
        self.version == other.version && self.base == other.base
    }

    /// Folds Raft entry `index`, whose journaled command events are `events`
    /// and whose journaled start-dispatch cursor (`RaftEntryApplied.
    /// start_dispatch_rr`, replayed state that routes future starts) is
    /// `start_dispatch_rr`, into this chain.
    pub fn fold<'a, I>(&self, index: u64, start_dispatch_rr: u64, events: I) -> Self
    where
        I: IntoIterator<Item = &'a Event>,
        I::IntoIter: ExactSizeIterator,
    {
        let events = events.into_iter();
        let mut hasher = Sha256::new();
        hasher.update(DOMAIN_V1);
        hasher.update(self.hash);
        hasher.update(index.to_le_bytes());
        hasher.update(start_dispatch_rr.to_le_bytes());
        hasher.update((events.len() as u64).to_le_bytes());
        let mut sink = HashSink(&mut hasher);
        for event in events {
            // Streams the canonical (slice 3) JSON straight into the hasher:
            // no intermediate buffer. Compact JSON never contains a raw '\n',
            // so the newline terminator is an unambiguous record separator.
            // Serializing an in-memory `Event` into an infallible sink cannot
            // fail; a hypothetical error still folds a distinct marker so it
            // can never silently alias another encoding.
            if serde_json::to_writer(&mut sink, event).is_err() {
                sink.0.update(b"\xffunencodable");
            }
            sink.0.update(b"\n");
        }
        Self {
            version: self.version,
            base: self.base,
            hash: hasher.finalize().into(),
        }
    }

    /// The hex form of [`hash`](Self::hash) (operator-facing logs).
    pub fn hex(&self) -> String {
        hex32::encode(&self.hash)
    }
}

impl std::fmt::Display for ChainDigest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "v{}@{}:{}", self.version, self.base, self.hex())
    }
}

/// The digest record an applied Raft entry journals (inside its opaque
/// `raft_state`): the chain AFTER the entry, plus how many command events the
/// entry folded — so migration-by-replay can re-derive the chain from the
/// flattened journal without knowing frame boundaries.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntryDigest {
    #[serde(flatten)]
    pub chain: ChainDigest,
    /// Number of command events (excluding the `RaftEntryApplied` record) the
    /// entry journaled and folded.
    pub events: u64,
}

/// Strictly extracts the [`EntryDigest`] from a `RaftEntryApplied` record's
/// opaque `raft_state` JSON. `Ok(None)` only when the record genuinely carries
/// no digest (a pre-#1664 record, or a non-JSON opaque state); a `digest`
/// field that is present but malformed is an `Err` — never conflated with an
/// absent one, which would silently re-base a verifier's chain (#1664 review).
pub fn try_entry_digest_of(raft_state: &str) -> Result<Option<EntryDigest>, String> {
    let Ok(serde_json::Value::Object(mut obj)) = serde_json::from_str(raft_state) else {
        return Ok(None);
    };
    match obj.remove("digest") {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(v) => serde_json::from_value::<EntryDigest>(v)
            .map(Some)
            .map_err(|e| format!("malformed chain digest: {e}")),
    }
}

/// Lenient form of [`try_entry_digest_of`] for the live (non-verifying)
/// paths: a malformed digest reads as absent.
pub fn entry_digest_of(raft_state: &str) -> Option<EntryDigest> {
    try_entry_digest_of(raft_state).ok().flatten()
}

/// The chain value a replica continues from after its last applied record.
/// `None` when that record predates the digest (or there is none), in which
/// case the next folded entry re-bases the chain.
pub fn chain_of_raft_state(raft_state: &str) -> Option<ChainDigest> {
    entry_digest_of(raft_state).map(|d| d.chain)
}

/// Strict form of [`chain_of_raft_state`] for replay verification seeds.
pub fn try_chain_of_raft_state(raft_state: &str) -> Result<Option<ChainDigest>, String> {
    try_entry_digest_of(raft_state).map(|d| d.map(|d| d.chain))
}

/// The chain after folding entry `index` (with `events`) onto the replica's
/// last applied record `prev_raft_state` (`None` when there is none).
pub fn next_entry_digest(
    prev_raft_state: Option<&str>,
    index: u64,
    start_dispatch_rr: u64,
    events: &[Event],
) -> EntryDigest {
    let prev = prev_raft_state
        .and_then(chain_of_raft_state)
        .filter(|c| c.version == CHAIN_VERSION)
        .unwrap_or_else(|| ChainDigest::genesis(index));
    EntryDigest {
        chain: prev.fold(index, start_dispatch_rr, events),
        events: events.len() as u64,
    }
}

/// A journal whose recorded chain digests do not re-derive (#1664): either the
/// event bytes were altered after they were journaled, or the replay fold no
/// longer reproduces them. Replay must fail closed on it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainVerifyError {
    /// The Raft log index of the first record whose digest did not re-derive.
    pub index: u64,
    pub recorded: String,
    pub derived: String,
}

impl std::fmt::Display for ChainVerifyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "journal chain digest mismatch at raft index {}: recorded {}, re-derived {} — the \
             journal was altered or replay no longer reproduces it (ADR 0066 #1664); refusing \
             to recover from it",
            self.index, self.recorded, self.derived
        )
    }
}

impl std::error::Error for ChainVerifyError {}

/// How much of a partition's history a replayed event window holds (#1664
/// review) — it decides how strictly [`verify_journal_chain`] may anchor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Coverage {
    /// The window is the partition's COMPLETE history (from event 0, or the
    /// exact continuation of a complete window whose chain is the seed), so
    /// every frame lies wholly inside it and no record may be adopted as an
    /// unchecked anchor: a chain starts only at genesis (the first record, or
    /// the first after a pre-digest / other-version record, based at its own
    /// index) and never changes base under a known same-version predecessor.
    Complete,
    /// The window may start after a snapshot base (possibly mid-frame) whose
    /// chain is unknown: the first record with an unknown predecessor is
    /// adopted as the anchor.
    Window,
}

/// Re-derives every recorded chain digest in a replayed event stream and fails
/// closed on the first mismatch (migration-by-replay, #1071 / #1664).
///
/// `events` is the flattened journal in log order. Each `RaftEntryApplied`
/// record carrying a digest names how many command events precede it in its
/// frame; the verifier folds exactly those, plus the record's journaled
/// `start_dispatch_rr`. Under [`Coverage::Window`] the chain is anchored at the
/// first digest-bearing record whose predecessor is unknown to this window: a
/// record based at its own index is verified from genesis; one based earlier
/// is adopted as the anchor. Under [`Coverage::Complete`] nothing is adopted
/// unchecked (see the variant). Returns how many records were verified and the
/// chain after the window's last digest-bearing record (the seed for a
/// following window).
///
/// `seed` is the chain of the record the replay window continues from (the
/// snapshot base's last applied record), when known.
pub fn verify_journal_chain(
    seed: Option<ChainDigest>,
    events: &[Event],
    coverage: Coverage,
) -> Result<(u64, Option<ChainDigest>), ChainVerifyError> {
    let complete = coverage == Coverage::Complete;
    let mut prev: Option<ChainDigest> = seed;
    let mut verified = 0u64;
    for (pos, event) in events.iter().enumerate() {
        let Event::RaftEntryApplied {
            index,
            raft_state,
            start_dispatch_rr,
        } = event
        else {
            continue;
        };
        let rec = try_entry_digest_of(raft_state).map_err(|e| ChainVerifyError {
            index: *index,
            recorded: format!("<{e}>"),
            derived: "<not derived>".into(),
        })?;
        let Some(rec) = rec else {
            // A pre-digest record: the chain re-bases at the next entry.
            prev = None;
            continue;
        };
        if rec.chain.version != CHAIN_VERSION {
            prev = None;
            continue;
        }
        let n = rec.events as usize;
        let reject = |why: String| ChainVerifyError {
            index: *index,
            recorded: rec.chain.to_string(),
            derived: why,
        };
        let start = match prev {
            Some(p) if p.comparable(&rec.chain) => Some(p),
            // A same-version record never re-bases under a known
            // predecessor: the live fold always continues it.
            Some(p) if complete => {
                return Err(reject(format!("<unexpected re-base from {p}>")));
            }
            // Unknown predecessor: verifiable only when the record is the
            // chain's own base (it folded from genesis) AND its whole frame
            // lies inside this window (a window can open mid-frame).
            _ if rec.chain.base == *index && n <= pos => Some(ChainDigest::genesis(*index)),
            // A complete history has no unknown predecessor to adopt: an
            // anchor that is not a whole genesis frame is altered history.
            _ if complete => {
                return Err(reject(format!(
                    "<no verifiable anchor: {n} events claimed, {pos} present>"
                )));
            }
            _ => None,
        };
        if let Some(start) = start {
            let derived = if n <= pos {
                Some(start.fold(*index, *start_dispatch_rr, events[pos - n..pos].iter()))
            } else {
                None
            };
            if derived.map(|d| d.hash) != Some(rec.chain.hash) {
                return Err(reject(derived.map(|d| d.to_string()).unwrap_or_else(
                    || format!("<{n} events missing before the record>"),
                )));
            }
            verified += 1;
        }
        prev = Some(rec.chain);
    }
    Ok((verified, prev))
}

/// An infallible `io::Write` adapter feeding a SHA-256 hasher.
struct HashSink<'a>(&'a mut Sha256);

impl std::io::Write for HashSink<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.update(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Lower-case hex (de)serialization of a 32-byte hash.
mod hex32 {
    use std::fmt::Write as _;

    pub fn encode(bytes: &[u8; 32]) -> String {
        let mut s = String::with_capacity(64);
        for b in bytes {
            let _ = write!(s, "{b:02x}");
        }
        s
    }

    pub fn serialize<S: serde::Serializer>(bytes: &[u8; 32], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&encode(bytes))
    }

    pub fn deserialize<'de, D: serde::Deserializer<'de>>(d: D) -> Result<[u8; 32], D::Error> {
        use serde::de::Error as _;
        let s = <std::borrow::Cow<'de, str> as serde::Deserialize>::deserialize(d)?;
        // Byte-indexed slicing below is only sound on ASCII input.
        if s.len() != 64 || !s.is_ascii() {
            return Err(D::Error::custom("chain digest must be 64 ASCII hex chars"));
        }
        let mut out = [0u8; 32];
        for (i, slot) in out.iter_mut().enumerate() {
            *slot = u8::from_str_radix(&s[2 * i..2 * i + 2], 16).map_err(D::Error::custom)?;
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(signal_key: u64) -> Event {
        Event::SignalBroadcast {
            signal_key,
            signal_name: "s".into(),
        }
    }

    /// The start-dispatch cursor the fixture journals for entry `index`
    /// (non-zero, so the fold's cursor input is exercised).
    fn rr(index: u64) -> u64 {
        index * 3
    }

    fn record(index: u64, digest: Option<EntryDigest>) -> Event {
        let mut state = serde_json::json!({ "log_id": index });
        if let Some(d) = digest {
            state["digest"] = serde_json::to_value(d).unwrap();
        }
        Event::RaftEntryApplied {
            index,
            raft_state: state.to_string(),
            start_dispatch_rr: rr(index),
        }
    }

    /// Builds a journal of `entries` (each a list of event job keys) with
    /// recorded digests, exactly as the live apply path journals it.
    fn journal(first_index: u64, entries: &[&[u64]]) -> Vec<Event> {
        let mut out = Vec::new();
        let mut prev: Option<String> = None;
        for (i, keys) in entries.iter().enumerate() {
            let index = first_index + i as u64;
            let events: Vec<Event> = keys.iter().map(|k| ev(*k)).collect();
            let d = next_entry_digest(prev.as_deref(), index, rr(index), &events);
            out.extend(events);
            let rec = record(index, Some(d));
            if let Event::RaftEntryApplied { raft_state, .. } = &rec {
                prev = Some(raft_state.clone());
            }
            out.push(rec);
        }
        out
    }

    #[test]
    fn digest_round_trips_through_raft_state_additively() {
        let d = next_entry_digest(None, 3, 0, &[ev(1)]);
        let state = serde_json::json!({"log_id": 3, "membership": null, "digest": d}).to_string();
        assert_eq!(entry_digest_of(&state), Some(d));
        // A pre-#1664 record has none.
        assert_eq!(entry_digest_of(r#"{"log_id":3}"#), None);
        assert_eq!(d.chain.base, 3);
        assert_eq!(d.events, 1);
    }

    #[test]
    fn fold_is_stable_and_sensitive_to_every_input() {
        let g = ChainDigest::genesis(1);
        let a = g.fold(1, 0, [ev(1), ev(2)].iter());
        // Pinned: the hash is a stable function of its inputs (never
        // DefaultHasher) — the same value on every platform and release.
        assert_eq!(a, g.fold(1, 0, [ev(1), ev(2)].iter()));
        assert_ne!(a.hash, g.fold(2, 0, [ev(1), ev(2)].iter()).hash, "index");
        assert_ne!(a.hash, g.fold(1, 0, [ev(2), ev(1)].iter()).hash, "order");
        assert_ne!(a.hash, g.fold(1, 0, [ev(1)].iter()).hash, "count");
        assert_ne!(a.hash, a.fold(1, 0, [ev(1), ev(2)].iter()).hash, "chained");
        assert!(a.comparable(&g));
        assert!(!a.comparable(&ChainDigest::genesis(2)));
    }

    #[test]
    fn replay_of_a_journal_with_recorded_digests_passes() {
        let j = journal(1, &[&[1, 2], &[], &[3]]);
        assert_eq!(
            verify_journal_chain(None, &j, Coverage::Complete).map(|v| v.0),
            Ok(3)
        );
    }

    #[test]
    fn replay_from_a_mid_chain_window_anchors_and_verifies_the_rest() {
        let j = journal(1, &[&[1, 2], &[3], &[4]]);
        // Drop the first entry (as if covered by a snapshot base): the first
        // surviving record anchors, the next one is verified against it.
        let tail = &j[3..];
        assert_eq!(
            verify_journal_chain(None, tail, Coverage::Window).map(|v| v.0),
            Ok(1)
        );
        // Seeded with the snapshot base's chain, every record verifies.
        let seed = match &j[2] {
            Event::RaftEntryApplied { raft_state, .. } => chain_of_raft_state(raft_state),
            _ => None,
        };
        assert_eq!(
            verify_journal_chain(seed, tail, Coverage::Complete).map(|v| v.0),
            Ok(2)
        );
        // A wrong seed fails closed.
        assert!(
            verify_journal_chain(Some(ChainDigest::genesis(1)), tail, Coverage::Window).is_err()
        );
    }

    #[test]
    fn a_tampered_journal_fails_closed() {
        let mut j = journal(1, &[&[1, 2], &[3], &[4]]);
        // Alter one journaled event's bytes after its digest was recorded.
        j[3] = ev(99);
        let err = verify_journal_chain(None, &j, Coverage::Complete).unwrap_err();
        assert_eq!(err.index, 2);
        // Dropping a whole event also fails closed.
        let mut j = journal(1, &[&[1, 2], &[3, 4]]);
        j.remove(3);
        assert!(verify_journal_chain(None, &j, Coverage::Complete).is_err());
    }

    fn malformed(hash: &str) -> String {
        serde_json::json!({"log_id": 2, "digest": {"v": 1, "base": 1, "hash": hash, "events": 1}})
            .to_string()
    }

    #[test]
    fn malformed_digests_are_errors_not_absent() {
        let bad_hex = malformed(&"z".repeat(64));
        // 64 BYTES but not ASCII: must not panic on a byte-indexed slice.
        let non_ascii = malformed(&format!("a\u{20ac}{}", "0".repeat(60)));
        for state in [bad_hex, non_ascii, r#"{"digest":{"base":1}}"#.to_string()] {
            assert!(try_entry_digest_of(&state).is_err(), "{state}");
            assert!(try_chain_of_raft_state(&state).is_err(), "{state}");
            assert_eq!(entry_digest_of(&state), None);
        }
        assert_eq!(try_entry_digest_of(r#"{"log_id":1}"#), Ok(None));
        assert_eq!(try_entry_digest_of(r#"{"digest":null}"#), Ok(None));
    }

    #[test]
    fn a_malformed_mid_chain_digest_fails_closed_instead_of_rebasing() {
        let mut j = journal(1, &[&[1], &[2], &[3]]);
        // Corrupt entry 2's recorded digest hash.
        j[3] = Event::RaftEntryApplied {
            index: 2,
            raft_state: malformed(&"z".repeat(64)),
            start_dispatch_rr: rr(2),
        };
        let err = verify_journal_chain(None, &j, Coverage::Complete).unwrap_err();
        assert_eq!(err.index, 2);
    }

    #[test]
    fn pre_digest_records_rebase_instead_of_failing() {
        let mut j = vec![ev(7), record(1, None)];
        // Post-upgrade entries re-base at index 2.
        let d = next_entry_digest(Some(r#"{"log_id":1}"#), 2, rr(2), &[ev(8)]);
        assert_eq!(d.chain.base, 2);
        j.push(ev(8));
        j.push(record(2, Some(d)));
        assert_eq!(
            verify_journal_chain(None, &j, Coverage::Complete).map(|v| v.0),
            Ok(1)
        );
    }

    /// #1664 review: the v1 hash is pinned to a LITERAL, so an accidental
    /// change to the domain, integer encoding or event serialization cannot
    /// stay green by re-deriving its own expectation — it would otherwise make
    /// every existing v1 journal fail recovery.
    #[test]
    fn v1_fold_is_pinned_to_a_literal() {
        let d = next_entry_digest(None, 3, 7, &[ev(1), ev(2)]);
        assert_eq!(
            d.chain.to_string(),
            "v1@3:89f55a23e7edcde77955e5cd0824e48c3c6ada2e9c9b90e9b60ca5adabd9d06b",
            "the v1 chain definition changed; existing journals would no longer verify"
        );
        // A fixed digest-bearing record (as journaled on disk), verified
        // without re-generating its expected digest.
        let fixture = vec![
            ev(1),
            ev(2),
            Event::RaftEntryApplied {
                index: 3,
                raft_state: r#"{"log_id":3,"digest":{"v":1,"base":3,"hash":"89f55a23e7edcde77955e5cd0824e48c3c6ada2e9c9b90e9b60ca5adabd9d06b","events":2}}"#
                    .into(),
                start_dispatch_rr: 7,
            },
        ];
        assert_eq!(
            verify_journal_chain(None, &fixture, Coverage::Complete).map(|v| v.0),
            Ok(1)
        );
    }

    #[test]
    fn the_start_dispatch_cursor_is_part_of_the_chain() {
        let g = ChainDigest::genesis(1);
        assert_ne!(
            g.fold(1, 0, [ev(1)].iter()).hash,
            g.fold(1, 1, [ev(1)].iter()).hash
        );
        // Altering ONLY a journaled cursor fails replay closed.
        let mut j = journal(1, &[&[1], &[2]]);
        if let Event::RaftEntryApplied {
            start_dispatch_rr, ..
        } = &mut j[1]
        {
            *start_dispatch_rr += 1;
        }
        assert_eq!(
            verify_journal_chain(None, &j, Coverage::Complete).map_err(|e| e.index),
            Err(1)
        );
    }

    #[test]
    fn complete_history_rejects_an_event_missing_from_the_first_entry() {
        let mut j = journal(1, &[&[1, 2], &[3]]);
        j.remove(0);
        assert_eq!(
            verify_journal_chain(None, &j, Coverage::Complete).map_err(|e| e.index),
            Err(1)
        );
        // A window (which may open mid-frame) still anchors there.
        assert!(verify_journal_chain(None, &j, Coverage::Window).is_ok());
    }

    #[test]
    fn complete_history_rejects_a_rebase_under_a_known_predecessor() {
        // Entry 2 re-based at its own index (with its event re-folded from
        // genesis) although entry 1 recorded a same-version chain.
        let mut j = journal(1, &[&[1], &[2]]);
        let forged = next_entry_digest(None, 2, rr(2), &[ev(2)]);
        j[3] = record(2, Some(forged));
        assert_eq!(
            verify_journal_chain(None, &j, Coverage::Complete).map_err(|e| e.index),
            Err(2)
        );
        // An earlier-based forged chain (an unchecked-anchor candidate) too.
        let mut j = journal(1, &[&[1], &[2], &[3]]);
        let mut forged = next_entry_digest(None, 2, rr(2), &[ev(2)]);
        forged.chain.base = 0;
        j[3] = record(2, Some(forged));
        assert!(verify_journal_chain(None, &j, Coverage::Complete).is_err());
    }
}
