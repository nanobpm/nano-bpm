//! Stepping / breakpoint debugger for the engine (prototype — see issue #646).
//!
//! The debugger reuses the engine's production step semantics unchanged. The only
//! difference between a normal `apply_command` and a debug run is the *driver*:
//! [`super::run_with`] consults a [`super::StepDriver`] after every processed
//! [`super::Step`], and the debug drivers here return [`Drive::Pause`] on a
//! breakpoint (or after a single step) where [`super::RunToCompletion`] always
//! continues. Because the loop, the step processor ([`Engine::process_step`]) and
//! the event applier ([`Engine::emit`]) are the *same* in both modes, a paused
//! session that is resumed to quiescence produces byte-identical output to a plain
//! `apply_command` (the RTC-parity contract, asserted in the tests).
//!
//! Determinism makes this a *time-travel* debugger: the append-only event log plus
//! [`Engine::snapshot`]/[`Engine::from_snapshot`] let a host rewind to any prior
//! point and replay forward, since [`Engine::emit`] performs no I/O.

use super::{Drive, Engine, EngineError, Paused, StepDriver};
use crate::command::Command;
use crate::event::Event;

/// A condition that pauses a debug run when it is satisfied by the events a step
/// emits. Element ids are BPMN element ids (e.g. `"Task_Charge"`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BreakCondition {
    /// Pause when the named element is activated (enters `ACTIVE`).
    ElementActivated(String),
    /// Pause when the named element completes (enters `COMPLETED`).
    ElementCompleted(String),
    /// Pause when a process instance completes.
    ProcessCompleted,
    /// Pause after every step (equivalent to stepping through the whole command).
    EveryStep,
}

impl BreakCondition {
    /// Whether this breakpoint is satisfied by the slice of events a single step
    /// emitted.
    fn matches(&self, events: &[Event]) -> bool {
        match self {
            BreakCondition::EveryStep => true,
            BreakCondition::ElementActivated(id) => events.iter().any(
                |e| matches!(e, Event::ElementActivated { element_id, .. } if element_id == id),
            ),
            BreakCondition::ElementCompleted(id) => events.iter().any(
                |e| matches!(e, Event::ElementCompleted { element_id, .. } if element_id == id),
            ),
            BreakCondition::ProcessCompleted => events
                .iter()
                .any(|e| matches!(e, Event::ProcessInstanceCompleted { .. })),
        }
    }
}

/// A live debug run over a single command: the events produced so far, the
/// still-pending fixpoint state while paused, and the active breakpoints. Owned by
/// the caller and threaded back into [`Engine::debug_resume`] /
/// [`Engine::debug_step`]. When [`is_paused`](Self::is_paused) is false the command
/// has run to completion and the session's [`log`](Self::log) is the same event
/// list a plain `apply_command` would have returned.
///
/// # Exclusive access while paused
///
/// A paused session holds a snapshot of the engine's mid-drain work queue, and
/// resuming replays it against the *live* engine. It therefore assumes the engine
/// has **not** been mutated by anything else since the pause: while a session is
/// paused you must not apply other commands (or drive another session) on the same
/// [`Engine`], or the resumed run will operate on inconsistent state and the
/// RTC-parity contract no longer holds. Treat the engine as exclusively borrowed by
/// the session until it finishes (`is_paused()` returns `false`). This is not
/// enforced at compile time because the session is owned by the caller rather than
/// holding a `&mut Engine`.
pub struct DebugSession {
    log: Vec<Event>,
    paused: Option<Paused>,
    breakpoints: Vec<BreakCondition>,
}

impl DebugSession {
    /// Whether the run is paused at a breakpoint (`true`) or has run to completion
    /// (`false`).
    pub fn is_paused(&self) -> bool {
        self.paused.is_some()
    }

    /// The events emitted so far. While paused this is a prefix of the full run;
    /// once finished it is the complete command output.
    pub fn log(&self) -> &[Event] {
        &self.log
    }

    /// Consumes the session, returning the accumulated event log.
    pub fn into_log(self) -> Vec<Event> {
        self.log
    }

    /// Replaces the active breakpoints; takes effect on the next
    /// [`Engine::debug_resume`].
    pub fn set_breakpoints(&mut self, breakpoints: Vec<BreakCondition>) {
        self.breakpoints = breakpoints;
    }
}

/// Pauses after processing exactly one step.
struct SingleStep;

impl StepDriver for SingleStep {
    fn after_step(&mut self, _events: &[Event]) -> Drive {
        Drive::Pause
    }
}

/// Pauses on the first step whose emitted events satisfy any active breakpoint.
struct Breakpointing<'b> {
    breakpoints: &'b [BreakCondition],
}

impl StepDriver for Breakpointing<'_> {
    fn after_step(&mut self, events: &[Event]) -> Drive {
        if self.breakpoints.iter().any(|b| b.matches(events)) {
            Drive::Pause
        } else {
            Drive::Continue
        }
    }
}

impl Engine {
    /// Begins a debug run of `command`, pausing at the first breakpoint (or running
    /// to completion if none match). The returned [`DebugSession`] is resumed with
    /// [`debug_resume`](Self::debug_resume) or advanced one step at a time with
    /// [`debug_step`](Self::debug_step).
    ///
    /// The engine state is mutated exactly as a normal `apply_command` would mutate
    /// it, but incrementally: each pause leaves the engine at a real intermediate
    /// point of the same run, not a copy.
    pub fn debug_command_at(
        &mut self,
        command: Command,
        now: u64,
        breakpoints: Vec<BreakCondition>,
    ) -> Result<DebugSession, EngineError> {
        let (log, queue) = self.plan_command_at(command, now)?;
        let mut session = DebugSession {
            log,
            paused: Some(Paused { queue, cursor: 0 }),
            breakpoints,
        };
        self.debug_resume(&mut session);
        Ok(session)
    }

    /// Resumes a paused session, running until the next breakpoint or, if none
    /// fires, to quiescence — at which point the post-drain tail
    /// ([`finish_command`](Self::finish_command)) runs and the session is no longer
    /// paused. A no-op on an already-finished session.
    pub fn debug_resume(&mut self, session: &mut DebugSession) {
        let Some(paused) = session.paused.take() else {
            return;
        };
        let mut driver = Breakpointing {
            breakpoints: &session.breakpoints,
        };
        let result = self.run_with(&mut session.log, paused.queue, paused.cursor, &mut driver);
        self.settle(session, result);
    }

    /// Advances a paused session by exactly one step, then pauses again (unless that
    /// step drained the last work, in which case the run finishes). A no-op on an
    /// already-finished session.
    pub fn debug_step(&mut self, session: &mut DebugSession) {
        let Some(paused) = session.paused.take() else {
            return;
        };
        let mut driver = SingleStep;
        let result = self.run_with(&mut session.log, paused.queue, paused.cursor, &mut driver);
        self.settle(session, result);
    }

    /// Stores the outcome of a `run_with` back into the session: either it paused
    /// (carry the state forward) or it drained, in which case the command's
    /// post-drain tail runs exactly once.
    fn settle(&mut self, session: &mut DebugSession, result: Option<Paused>) {
        match result {
            Some(paused) => session.paused = Some(paused),
            None => {
                self.finish_command(&session.log);
                session.paused = None;
            }
        }
    }
}
