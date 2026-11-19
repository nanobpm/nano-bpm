---- MODULE SnapshotReplay ----
(***************************************************************************)
(* Snapshot / compaction / cold-archive + replay-migration recovery       *)
(* protocol (incidents #1065-#1071), the durable state behind             *)
(* `nano-server-storage`'s segmented journal (server/crates/              *)
(* nano-server-storage/src/seglog.rs: `recover`, `compact`,               *)
(* `migrate_by_replay`, `read_cold_prefix`, `prune_cold_archive`, and     *)
(* engine-core's `SNAPSHOT_FORMAT_VERSION` / `check_format_version`).      *)
(*                                                                         *)
(* The engine's whole durable history is the contiguous event window       *)
(* `[0, total)`. Recovery must rebuild it from three overlapping sources:  *)
(*                                                                         *)
(*   - a persisted engine SNAPSHOT covering `[0, snapCovered)`, at some    *)
(*     on-disk `format_version` this build may or may not be able to read; *)
(*   - a bounded rolling COLD ARCHIVE holding a validated contiguous       *)
(*     prefix `[0, coldEnd)` (compaction archives the covered prefix here  *)
(*     before deleting it from the hot log; pruning rolls it forward);     *)
(*   - the surviving HOT TAIL `[firstIndex, total)`.                       *)
(*                                                                         *)
(* We model-check the two safety properties that failed historically:     *)
(*                                                                         *)
(*   NoSilentRewind (#1065) - whenever recovery SUCCEEDS it reconstructs   *)
(*     the FULL `[0, total)` history; it never silently rebuilds a         *)
(*     strictly-older (shorter) prefix and calls it done.                  *)
(*                                                                         *)
(*   FailClosed (#1066) - when the full history genuinely cannot be        *)
(*     reconstructed (a pruned gap, or an unreadable / UnknownVariant      *)
(*     frame in a source it must read), recovery REJECTS rather than       *)
(*     silently proceeding on a partial history.                          *)
(*                                                                         *)
(* Recovery is a pure function of the durable on-disk world               *)
(* (`RecoverOutcome`), so we do not model it as an action: the invariants  *)
(* below must hold at EVERY durable world the protocol can produce. The    *)
(* `FailClosedRecovery` constant selects the shipped fail-closed recovery  *)
(* (TRUE -> the invariants hold: `MCSnapReplayCorrect` passes) or the      *)
(* historical pre-#1066 naive recovery (FALSE -> it silently rewinds /     *)
(* proceeds: `MCSnapReplayNaive` reproduces the #1065/#1066 defect).       *)
(*                                                                         *)
(* Anti-drift anchor: the real seglog recovery+migration path is asserted  *)
(* against these same two invariants by the Rust conformance test          *)
(* `mod snapshot_replay_conformance` in                                    *)
(* server/crates/nano-server-storage/src/seglog.rs. A change to the spec's *)
(* recovery model that is not mirrored by the implementation (or vice      *)
(* versa) breaks one side or the other.                                    *)
(***************************************************************************)
EXTENDS Naturals

CONSTANTS
  MaxEvents,           \* state-space bound on the durable history length
  CurVersion,          \* SNAPSHOT_FORMAT_VERSION this build supports (>= 1)
  FailClosedRecovery   \* TRUE: shipped recovery; FALSE: historical naive recovery

VARIABLES
  total,        \* count of events ever durably appended: history is [0, total)
  firstIndex,   \* compaction floor; the surviving hot tail is [firstIndex, total)
  snapPresent,  \* a snapshot file exists on disk
  snapCovered,  \* the snapshot covers [0, snapCovered)
  snapVersion,  \* the snapshot's on-disk format_version
  snapCorrupt,  \* the snapshot payload no longer deserializes under this build
  coldEnd,      \* the validated contiguous cold-archive prefix is [0, coldEnd)
  coldOk,       \* the cold prefix decodes (no UnknownVariant / mis-sized frame)
  hotOk         \* the surviving hot tail decodes (no UnknownVariant frame)

vars == <<total, firstIndex, snapPresent, snapCovered, snapVersion,
          snapCorrupt, coldEnd, coldOk, hotOk>>

TypeOK ==
  /\ total \in 0..MaxEvents
  /\ firstIndex \in 0..total
  /\ snapPresent \in BOOLEAN
  /\ snapCovered \in 0..total
  /\ snapVersion \in 1..(CurVersion + 1)
  /\ snapCorrupt \in BOOLEAN
  /\ coldEnd \in 0..firstIndex
  /\ coldOk \in BOOLEAN
  /\ hotOk \in BOOLEAN

Init ==
  /\ total = 0
  /\ firstIndex = 0
  /\ snapPresent = FALSE
  /\ snapCovered = 0
  /\ snapVersion = CurVersion
  /\ snapCorrupt = FALSE
  /\ coldEnd = 0
  /\ coldOk = TRUE
  /\ hotOk = TRUE

(***************************************************************************)
(* Derived predicates over the durable world.                              *)
(***************************************************************************)

\* A present snapshot this build can actually load: its format_version is not
\* newer than we support and its payload still deserializes (load_latest_snapshot
\* + check_format_version). A newer version or a corrupt payload is the typed
\* SnapshotLoadError that routes to replay-migration.
SnapReadable == snapPresent /\ (snapVersion <= CurVersion) /\ (~snapCorrupt)

\* Readability is meaningful only for a NON-empty region: an empty hot tail
\* (total = firstIndex) or empty cold archive (coldEnd = 0) trivially "decodes",
\* so a stray `hotOk`/`coldOk` = FALSE on an empty region is ignored. (This lets
\* a damaged event be compacted out of the hot tail into the cold store without
\* the model tracking which physical region each byte lives in.)
HotReadable == (total = firstIndex) \/ hotOk
ColdReadable == (coldEnd = 0) \/ coldOk

\* A readable snapshot that sits BELOW the compaction floor: it covers less than
\* the surviving journal starts at, so trusting it drops the compacted window
\* [snapCovered, firstIndex) and rewinds the key generator. `recover` rejects it
\* loud rather than migrating (the distinct #1065 stale-snapshot signal).
SnapStale == SnapReadable /\ (firstIndex > 0) /\ (snapCovered < firstIndex)

\* The contiguous-from-0 history length produced by stitching a base covering
\* [0, base) with the hot tail [firstIndex, total). They join into the full
\* [0, total) only when the base reaches the floor; otherwise the gap at `base`
\* truncates the reconstructed prefix there.
Contig(base) == IF base >= firstIndex THEN total ELSE base

\* A recovery outcome is a typed record: `reject` = TRUE means recovery refused
\* (failed closed); otherwise `len` is the contiguous-from-0 history length it
\* reconstructed. (A record keeps the two safety invariants fully typed — TLC
\* refuses to compare a string sentinel with an integer length.)
Reject == [reject |-> TRUE, len |-> 0]
Rebuilt(k) == [reject |-> FALSE, len |-> k]

\* The shipped, fail-closed recovery (seglog::recover). Mirrors its branches:
\*   - an unreadable hot tail errors up front (#1070);
\*   - a stale-below-floor readable snapshot fails loud (#1065);
\*   - a readable, non-stale snapshot: snapshot base + replayed tail;
\*   - no snapshot with the hot log still rooted at 0: replay the whole tail;
\*   - otherwise migrate-by-replay: the cold prefix + hot tail, but ONLY when
\*     the validated cold prefix meets the floor (coldEnd = firstIndex) and
\*     decodes; any gap or undecodable frame fails closed (#1066).
CorrectOutcome ==
  IF ~HotReadable THEN Reject
  ELSE IF SnapStale THEN Reject
  ELSE IF SnapReadable THEN Rebuilt(Contig(snapCovered))
  ELSE IF (~snapPresent /\ firstIndex = 0) THEN Rebuilt(Contig(0))
  ELSE IF (coldEnd = firstIndex /\ ColdReadable) THEN Rebuilt(Contig(coldEnd))
  ELSE Reject

\* The historical pre-#1066 recovery: it silently stitched whatever base was
\* present (a stale snapshot, or the surviving tail atop nothing) and never
\* failed closed. `.ok()?`-swallowing an unreadable snapshot dropped it to the
\* cold/tail base; no floor check let a stale snapshot through; no gap check let
\* a compacted-away prefix rewind.
NaiveBase == IF SnapReadable THEN snapCovered ELSE coldEnd
NaiveOutcome == IF ~HotReadable THEN Reject ELSE Rebuilt(Contig(NaiveBase))

RecoverOutcome == IF FailClosedRecovery THEN CorrectOutcome ELSE NaiveOutcome

\* Ground truth (independent of the recovery under test): the furthest a usable,
\* readable base reaches from 0. A stale/inconsistent snapshot is NOT a usable
\* base (recovery must not trust it); the cold prefix is usable only when it
\* decodes.
UsableBase ==
  IF SnapReadable /\ ~SnapStale THEN snapCovered
  ELSE IF ColdReadable THEN coldEnd
  ELSE 0

\* The full [0, total) history is genuinely reconstructable iff the hot tail
\* decodes AND a readable base reaches the floor (or the hot log still starts
\* at 0, so the tail alone is the whole history).
CanReconstructFull == HotReadable /\ (firstIndex = 0 \/ UsableBase >= firstIndex)

(***************************************************************************)
(* Safety invariants.                                                      *)
(***************************************************************************)

\* #1065: recovery either rejects, or reconstructs the FULL history. It never
\* silently rebuilds a strictly-older (shorter) prefix.
NoSilentRewind == RecoverOutcome.reject \/ (RecoverOutcome.len = total)

\* #1066: when the full history cannot be reconstructed, recovery rejects rather
\* than proceeding on a partial history.
FailClosed == (~CanReconstructFull) => RecoverOutcome.reject

(***************************************************************************)
(* Durable-world transitions (the protocol that produces on-disk states).  *)
(***************************************************************************)

\* A new event is durably appended, extending the hot tail.
AppendEvent ==
  /\ total < MaxEvents
  /\ total' = total + 1
  /\ UNCHANGED <<firstIndex, snapPresent, snapCovered, snapVersion,
                 snapCorrupt, coldEnd, coldOk, hotOk>>

\* A fresh in-format snapshot is written at the current boundary.
TakeSnapshot ==
  /\ snapPresent' = TRUE
  /\ snapCovered' = total
  /\ snapVersion' = CurVersion
  /\ snapCorrupt' = FALSE
  /\ UNCHANGED <<total, firstIndex, coldEnd, coldOk, hotOk>>

\* Compaction archives the covered prefix [firstIndex, snapCovered) into the
\* cold store and advances the floor to the snapshot watermark.
Compact ==
  /\ snapPresent
  /\ SnapReadable
  /\ snapCovered > firstIndex
  /\ firstIndex' = snapCovered
  /\ coldEnd' = snapCovered
  /\ UNCHANGED <<total, snapPresent, snapCovered, snapVersion, snapCorrupt,
                 coldOk, hotOk>>

\* Roll the bounded cold archive forward, dropping older generations. Over-
\* pruning below the floor opens a gap the migrator must detect (fail-closed).
PruneCold ==
  /\ coldEnd > 0
  /\ \E k \in 0..(coldEnd - 1):
       /\ coldEnd' = k
       /\ coldOk' = (IF k = 0 THEN TRUE ELSE coldOk)
  /\ UNCHANGED <<total, firstIndex, snapPresent, snapCovered, snapVersion,
                 snapCorrupt, hotOk>>

\* A format-version bump leaves the on-disk snapshot newer than this build can
\* read (#1068) - the trigger for replay-migration.
BumpSnapshotVersion ==
  /\ snapPresent
  /\ SnapReadable
  /\ snapVersion' = CurVersion + 1
  /\ UNCHANGED <<total, firstIndex, snapPresent, snapCovered, snapCorrupt,
                 coldEnd, coldOk, hotOk>>

\* A breaking payload change leaves the snapshot present but undeserializable.
CorruptSnapshotPayload ==
  /\ snapPresent
  /\ SnapReadable
  /\ snapCorrupt' = TRUE
  /\ UNCHANGED <<total, firstIndex, snapPresent, snapCovered, snapVersion,
                 coldEnd, coldOk, hotOk>>

\* The snapshot is lost / was never flushed, while the journal stayed compacted.
LoseSnapshot ==
  /\ snapPresent
  /\ snapPresent' = FALSE
  /\ UNCHANGED <<total, firstIndex, snapCovered, snapVersion, snapCorrupt,
                 coldEnd, coldOk, hotOk>>

\* An older readable snapshot resurfaces below the compaction floor (#1065).
ResurfaceStaleSnapshot ==
  /\ firstIndex > 0
  /\ \E c \in 0..(firstIndex - 1):
       /\ snapPresent' = TRUE
       /\ snapCovered' = c
       /\ snapVersion' = CurVersion
       /\ snapCorrupt' = FALSE
  /\ UNCHANGED <<total, firstIndex, coldEnd, coldOk, hotOk>>

\* An UnknownVariant / mis-sized frame corrupts the cold prefix (#1066/#1070).
DamageColdFrame ==
  /\ coldEnd > 0
  /\ coldOk
  /\ coldOk' = FALSE
  /\ UNCHANGED <<total, firstIndex, snapPresent, snapCovered, snapVersion,
                 snapCorrupt, coldEnd, hotOk>>

\* An UnknownVariant frame corrupts the surviving hot tail (#1070).
DamageHotFrame ==
  /\ total > firstIndex
  /\ hotOk
  /\ hotOk' = FALSE
  /\ UNCHANGED <<total, firstIndex, snapPresent, snapCovered, snapVersion,
                 snapCorrupt, coldEnd, coldOk>>

\* A durable world may be recovered as-is (no-deadlock stutter). Named (rather
\* than an inline `UNCHANGED vars` disjunct in `Next`) so the TLC
\* action-coverage gate (#1248) tracks it: the gate derives obligations from
\* `Next`'s NAMED disjuncts and silently drops an inline one.
RecoverAsIs == UNCHANGED vars

Next ==
  \/ AppendEvent
  \/ TakeSnapshot
  \/ Compact
  \/ PruneCold
  \/ BumpSnapshotVersion
  \/ CorruptSnapshotPayload
  \/ LoseSnapshot
  \/ ResurfaceStaleSnapshot
  \/ DamageColdFrame
  \/ DamageHotFrame
  \/ RecoverAsIs

Spec == Init /\ [][Next]_vars
====
