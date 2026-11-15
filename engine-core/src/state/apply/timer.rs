//! Timer-family appliers: timers and process-start timers.

use crate::event::Event;
use crate::state::types::*;

pub(super) fn apply_timer(state: &mut State, event: &Event) {
    match event {
        Event::TimerCreated {
            timer_key,
            instance_key,
            element_instance_key,
            element_id,
            due_at,
            kind,
            repetitions,
        } => {
            state.timers.insert(
                *timer_key,
                Timer {
                    key: *timer_key,
                    instance_key: *instance_key,
                    element_instance_key: *element_instance_key,
                    element_id: element_id.clone(),
                    due_at: *due_at,
                    state: TimerState::Created,
                    kind: kind.clone(),
                    repetitions: *repetitions,
                },
            );
        }

        Event::TimerTriggered { timer_key, .. } => {
            if let Some(timer) = state.timers.get_mut(timer_key) {
                timer.state = TimerState::Triggered;
            }
        }

        Event::TimerCanceled { timer_key, .. } => {
            if let Some(timer) = state.timers.get_mut(timer_key) {
                timer.state = TimerState::Canceled;
            }
        }

        Event::ProcessStartTimerArmed {
            timer_key,
            process_definition_key,
            process_id,
            start_element_id,
            due_at,
            interval_millis,
            repeating,
            repetitions,
        } => {
            state.start_timers.insert(
                *timer_key,
                StartTimer {
                    timer_key: *timer_key,
                    process_definition_key: *process_definition_key,
                    process_id: process_id.clone(),
                    start_element_id: start_element_id.clone(),
                    due_at: Some(*due_at),
                    interval_millis: *interval_millis,
                    repeating: *repeating,
                    repetitions: *repetitions,
                },
            );
        }

        Event::ProcessStartTimerFired {
            timer_key,
            next_due_at,
        } => {
            if let Some(start_timer) = state.start_timers.get_mut(timer_key) {
                // A cycle re-arms (next_due_at is Some); a one-shot is retained
                // with due_at = None so a later tick never re-fires it.
                start_timer.due_at = *next_due_at;
                // A bounded cycle consumes one of its remaining fires (#1596).
                start_timer.repetitions = start_timer.repetitions.map(|n| n.saturating_sub(1));
            }
        }
        _ => unreachable!(),
    }
}
