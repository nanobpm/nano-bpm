//! #1556 — generic `operationReference` propagation via the [`Record`]
//! envelope. Zeebe stamps a command's `operationReference` on EVERY record the
//! command produces; Nano does the same at one seam
//! (`Engine::apply_command_records_at`), and the journal persists the envelope
//! additively (a legacy bare-`Event` line is still a valid record).
#![cfg(feature = "serde")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "non-production target (integration tests / examples)"
)]

use nanobpmn_engine_core::{
    decode_record_json, Command, Engine, Event,
    EventDecodeError, ProcessBuilder, Record,
};

fn engine_with_waiting_instance() -> (Engine, u64) {
    let def = ProcessBuilder::new("wait")
        .start_event("s")
        .service_task("t", "work")
        .end_event("e")
        .connect("s", "t")
        .connect("t", "e")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("wait"))
        .unwrap();
    let key = created.iter().find_map(Event::instance_key).unwrap();
    (engine, key)
}

/// Red-first (issue #1556): cancelling with an `operationReference` stamps it on
/// every resulting record — the termination events included, not only the
/// handful of audit variants that used to carry a per-variant copy.
#[test]
fn cancel_with_operation_reference_stamps_every_resulting_record() {
    let (mut engine, key) = engine_with_waiting_instance();
    let records = engine
        .apply_command_records_at(Command::cancel_instance(key), 5, Some(4242))
        .unwrap();
    assert!(
        records
            .iter()
            .any(|r| matches!(r.event, Event::ProcessInstanceTerminated { .. })),
        "cancel terminates the instance: {records:?}"
    );
    assert!(records.len() > 1, "cancel produces a cascade: {records:?}");
    for record in &records {
        assert_eq!(record.operation_reference, Some(4242), "{record:?}");
    }
}

/// The reference is per-command scratch: it never leaks into a later command,
/// neither after success nor after a rejected command.
#[test]
fn operation_reference_never_leaks_into_a_later_command() {
    let (mut engine, key) = engine_with_waiting_instance();
    assert!(engine
        .apply_command_records_at(Command::cancel_instance(key + 1_000), 1, Some(7))
        .is_err());
    let records = engine
        .apply_command_records_at(Command::cancel_instance(key), 2, None)
        .unwrap();
    assert!(records.iter().all(|r| r.operation_reference.is_none()));
}

/// The legacy per-event audit fields derive from the same single reference.
#[test]
fn legacy_audit_fields_derive_from_the_generic_reference() {
    let (mut engine, _) = engine_with_waiting_instance();
    let job_key = engine.activate_jobs("work", "w", 1, 60_000, 0)[0].key;
    let records = engine
        .apply_command_records_at(Command::update_job_retries(job_key, 3), 1, Some(11))
        .unwrap();
    assert!(records.iter().any(|r| matches!(
        r.event,
        Event::JobRetriesUpdated {
            operation_reference: Some(11),
            ..
        }
    )));
}

/// Wire form: a referenced record is the `{"event":…,"operation_reference":N}`
/// envelope; an unreferenced one is the bare legacy `Event` — byte-identical
/// to every pre-#1556 journal line.
#[test]
fn record_wire_form_round_trips_and_stays_legacy_without_a_reference() {
    let (mut engine, key) = engine_with_waiting_instance();
    let records = engine
        .apply_command_records_at(Command::cancel_instance(key), 5, Some(-3))
        .unwrap();
    for record in records {
        let line = serde_json::to_string(&record).unwrap();
        assert!(line.starts_with("{\"event\":"), "{line}");
        assert_eq!(decode_record_json(&line).unwrap(), record);

        let bare = Record::bare(record.event.clone());
        let line = serde_json::to_string(&bare).unwrap();
        assert_eq!(line, serde_json::to_string(&record.event).unwrap());
        assert_eq!(decode_record_json(&line).unwrap(), bare);
    }
}

/// Legacy fallback: a pre-#1556 journal line (bare `Event`) decodes as a
/// record with no reference and replays.
#[test]
fn legacy_bare_event_lines_decode_as_unreferenced_records() {
    let (mut engine, key) = engine_with_waiting_instance();
    let events = engine.apply_command(Command::cancel_instance(key)).unwrap();
    for event in events {
        let line = serde_json::to_string(&event).unwrap();
        let record = decode_record_json(&line).unwrap();
        assert_eq!(record.event, event);
        assert_eq!(record.operation_reference, None);
    }
}

/// An unknown variant inside the envelope is still the typed rejection —
/// never a silent drop (#1065 class).
#[test]
fn unknown_variant_inside_an_envelope_is_rejected_not_dropped() {
    let line = r#"{"event":{"NoSuchEvent":{"x":1}},"operation_reference":9}"#;
    match decode_record_json(line) {
        Err(EventDecodeError::UnknownVariant { variant, .. }) => {
            assert_eq!(variant, "NoSuchEvent");
        }
        other => panic!("expected UnknownVariant, got {other:?}"),
    }
    let line = r#"{"event":{"NoSuchEvent":{"x":1}}}"#;
    assert!(matches!(
        decode_record_json(line),
        Err(EventDecodeError::UnknownVariant { .. })
    ));
    assert!(matches!(
        decode_record_json(r#"{"event":{"ProcessInstanceTerminated":"#),
        Err(EventDecodeError::Malformed { .. })
    ));
}
