//! Unit tests for the engine. The former single ~27.5k-line `tests.rs` is now a
//! dispatcher module: each `#[test]` lives in the per-concern submodule below
//! (moved verbatim), while the shared fixture/helper functions stay here so
//! every submodule reaches them via `use super::*;`.
use super::*;
use crate::model::{ProcessBuilder, ProcessDefinition};
use crate::ActivateElementInstruction;

// Per-concern test submodules (slice 7 of #1201): each test moved verbatim
// into the file matching its concern; shared helpers stay here.
mod adhoc;
mod boundary;
mod call_activity;
mod compensation_escalation;
mod incidents;
mod jobs;
mod lifecycle;
mod listeners;
mod migration;
mod modify_scopes;
mod multi_instance;
mod timers_catch;
mod user_task;

fn linear_with_task() -> ProcessDefinition {
    ProcessBuilder::new("order")
        .start_event("start")
        .service_task("charge", "payment")
        .end_event("end")
        .connect("start", "charge")
        .connect("charge", "end")
        .build()
        .unwrap()
}

/// A linear process whose single service task emits `work` jobs at the given
/// (literal) priority. Distinct `proc_id`s let several share one job type.
fn task_with_priority(proc_id: &str, priority: &str) -> ProcessDefinition {
    ProcessBuilder::new(proc_id)
        .start_event("start")
        .service_task_with_priority("do", "work", Some(priority.to_string()))
        .end_event("end")
        .connect("start", "do")
        .connect("do", "end")
        .build()
        .unwrap()
}

// --- Part C phase 3: sub-process scopes + Zeebe variable propagation --------

/// A sub-process `sub` carrying an input mapping (`scoped = seed + 1`) around an
/// inner service task `inner` (job `work`), then a normal flow out to `done`.
fn subprocess_with_input_mapping(output: Vec<crate::model::Mapping>) -> ProcessDefinition {
    ProcessBuilder::new("sub-scope")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .with_io(
            "sub",
            crate::model::IoMapping {
                inputs: vec![crate::model::Mapping {
                    source: "=seed + 1".to_string(),
                    target: "scoped".to_string(),
                }],
                outputs: output,
            },
        )
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .service_task("inner", "work")
        .contained_in("inner", "sub")
        .end_event("sub_end")
        .contained_in("sub_end", "sub")
        .end_event("done")
        .connect("start", "sub")
        .connect("sub_start", "inner")
        .connect("inner", "sub_end")
        .connect("sub", "done")
        .build()
        .unwrap()
}

/// Like [`subprocess_with_input_mapping`] but the inner service task ALSO carries
/// an input mapping (`derived = scoped * 10`) whose source reads the
/// sub-process-local `scoped`. This only resolves if the inner element's inputs
/// are evaluated against its enclosing (sub-process) scope, not the root.
fn subprocess_with_nested_input_mapping() -> ProcessDefinition {
    ProcessBuilder::new("nested-scope")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .with_io(
            "sub",
            crate::model::IoMapping {
                inputs: vec![crate::model::Mapping {
                    source: "=seed + 1".to_string(),
                    target: "scoped".to_string(),
                }],
                outputs: Vec::new(),
            },
        )
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .service_task("inner", "work")
        .with_io(
            "inner",
            crate::model::IoMapping {
                inputs: vec![crate::model::Mapping {
                    source: "=scoped * 10".to_string(),
                    target: "derived".to_string(),
                }],
                outputs: Vec::new(),
            },
        )
        .contained_in("inner", "sub")
        .end_event("sub_end")
        .contained_in("sub_end", "sub")
        .end_event("done")
        .connect("start", "sub")
        .connect("sub_start", "inner")
        .connect("inner", "sub_end")
        .connect("sub", "done")
        .build()
        .unwrap()
}

fn create_instance_key(engine: &mut Engine, proc_id: &str) -> Key {
    engine
        .apply_command(Command::create_instance(proc_id))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap()
}

/// Test helper: activate the first job of `job_type` (locking it) and
/// complete it by key, returning the events from completion.
fn complete_one(engine: &mut Engine, job_type: &str) -> Vec<Event> {
    let job = engine
        .activate_jobs(job_type, "test-worker", 10, 60_000, 0)
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("no activatable job of type {job_type}"));
    engine
        .apply_command(Command::complete_job(job.key))
        .unwrap()
}

/// start -> charge (service task, PT5S interrupting timer boundary) -> done
///                       \--(timer "timeout")--> escalated
fn process_with_timer_boundary() -> ProcessDefinition {
    ProcessBuilder::new("ship")
        .start_event("start")
        .service_task("charge", "payment")
        .timer_boundary_event("timeout", "charge", 5_000)
        .end_event("done")
        .end_event("escalated")
        .connect("start", "charge")
        .connect("charge", "done")
        .connect("timeout", "escalated")
        .build()
        .unwrap()
}

/// start -> await (message catch "payment-received", correlationKey orderId)
///       -> end
fn process_with_message_catch() -> ProcessDefinition {
    ProcessBuilder::new("await-payment")
        .start_event("start")
        .message_intermediate_catch_event("await", "payment-received", "orderId")
        .end_event("end")
        .connect("start", "await")
        .connect("await", "end")
        .build()
        .unwrap()
}

fn vars(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

fn process_with_signal_catch() -> ProcessDefinition {
    ProcessBuilder::new("await-signal")
        .start_event("start")
        .signal_intermediate_catch_event("await", "all-clear")
        .end_event("end")
        .connect("start", "await")
        .connect("await", "end")
        .build()
        .unwrap()
}

fn deploy_and_create_retryable(retries: Option<&str>, vars_in: HashMap<String, Value>) -> Engine {
    let mut builder = ProcessBuilder::new("retryable")
        .start_event("start")
        .service_task("work", "do-work")
        .end_event("end")
        .connect("start", "work")
        .connect("work", "end");
    if let Some(r) = retries {
        builder = builder.with_retries("work", r);
    }
    let def = builder.build().unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    engine
        .apply_command(Command::create_instance_with("retryable", vars_in))
        .unwrap();
    engine
}

/// start -> charge (service task, interrupting message boundary "cancel"
///          correlating on orderId) -> done
///                       \--(message)--> aborted
fn process_with_message_boundary() -> ProcessDefinition {
    ProcessBuilder::new("cancellable")
        .start_event("start")
        .service_task("charge", "payment")
        .message_boundary_event("cancel", "charge", "order-cancelled", "orderId")
        .end_event("done")
        .end_event("aborted")
        .connect("start", "charge")
        .connect("charge", "done")
        .connect("cancel", "aborted")
        .build()
        .unwrap()
}

/// start -> charge (service task, PT5S NON-interrupting timer boundary
///                   "remind") -> done
///                       \--(timer)--> reminded
fn process_with_non_interrupting_timer_boundary() -> ProcessDefinition {
    ProcessBuilder::new("ship")
        .start_event("start")
        .service_task("charge", "payment")
        .non_interrupting_timer_boundary_event("remind", "charge", 5_000)
        .end_event("done")
        .end_event("reminded")
        .connect("start", "charge")
        .connect("charge", "done")
        .connect("remind", "reminded")
        .build()
        .unwrap()
}

/// start -> charge (service task, NON-interrupting message boundary "notify"
///                   on message "reminder" correlating orderId) -> done
///                       \--(message)--> notified
fn process_with_non_interrupting_message_boundary() -> ProcessDefinition {
    ProcessBuilder::new("notifiable")
        .start_event("start")
        .service_task("charge", "payment")
        .non_interrupting_message_boundary_event("notify", "charge", "reminder", "orderId")
        .end_event("done")
        .end_event("notified")
        .connect("start", "charge")
        .connect("charge", "done")
        .connect("notify", "notified")
        .build()
        .unwrap()
}

fn approval_process() -> ProcessDefinition {
    // s -> g(xor): decision==yes -> approved ; else default -> rejected
    ProcessBuilder::new("approval")
        .start_event("s")
        .exclusive_gateway("g")
        .end_event("approved")
        .end_event("rejected")
        .connect("s", "g")
        .connect_when("g", "approved", r#"decision = "yes""#)
        .connect("g", "rejected")
        .build()
        .unwrap()
}

fn process_with_error_boundary() -> ProcessDefinition {
    // s -> charge(task) --normal--> done
    //              \--(error CARD_DECLINED)--> boundary -> declined
    ProcessBuilder::new("payment")
        .start_event("s")
        .service_task("charge", "payment")
        .error_boundary_event("boundary", "charge", "CARD_DECLINED")
        .end_event("done")
        .end_event("declined")
        .connect("s", "charge")
        .connect("charge", "done")
        .connect("boundary", "declined")
        .build()
        .unwrap()
}

/// start -> book(job) -> throw(compensation) -> done
///            \--(compensation boundary "book-comp")..association..> cancel(job)
///                                                    isForCompensation
fn process_with_compensation() -> ProcessDefinition {
    ProcessBuilder::new("trip")
        .start_event("s")
        .service_task("book", "book-job")
        .compensation_boundary_event("book-comp", "book", "cancel")
        .service_task("cancel", "cancel-job")
        .compensation_throw_event("throw")
        .end_event("done")
        .connect("s", "book")
        .connect("book", "throw")
        .connect("throw", "done")
        .build()
        .unwrap()
}

/// start -> sub[ sub_start -> inner(work) -> sub_end ] --normal--> done
///                               (sub catches BUSINESS_ERROR)
///          sub --(error boundary)--> sad(sad-flow) -> sad_end
fn process_with_subprocess_error_boundary() -> ProcessDefinition {
    ProcessBuilder::new("sub-error")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .service_task("inner", "work")
        .contained_in("inner", "sub")
        .end_event("sub_end")
        .contained_in("sub_end", "sub")
        .error_boundary_event("boundary", "sub", "BUSINESS_ERROR")
        .service_task("sad", "sad-flow")
        .end_event("done")
        .end_event("sad_end")
        .connect("start", "sub")
        .connect("sub_start", "inner")
        .connect("inner", "sub_end")
        .connect("sub", "done")
        .connect("boundary", "sad")
        .connect("sad", "sad_end")
        .build()
        .unwrap()
}

// --- Escalation events (#1173) -------------------------------------------

/// start -> sub[ sub_start -> throw(esc CODE) -> work(work) -> sub_end ] -> done
///          sub --(escalation boundary CODE, interrupting?)--> handler(handle)
///                                                              -> handler_end
fn process_with_escalation_boundary(interrupting: bool, boundary_code: &str) -> ProcessDefinition {
    let builder = ProcessBuilder::new("esc-sub")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .escalation_throw_event("throw", "OVERLOAD")
        .contained_in("throw", "sub")
        .service_task("work", "work")
        .contained_in("work", "sub")
        .end_event("sub_end")
        .contained_in("sub_end", "sub");
    let builder = if interrupting {
        builder.escalation_boundary_event("boundary", "sub", boundary_code)
    } else {
        builder.non_interrupting_escalation_boundary_event("boundary", "sub", boundary_code)
    };
    builder
        .service_task("handler", "handle")
        .end_event("done")
        .end_event("handler_end")
        .connect("start", "sub")
        .connect("sub_start", "throw")
        .connect("throw", "work")
        .connect("work", "sub_end")
        .connect("sub", "done")
        .connect("boundary", "handler")
        .connect("handler", "handler_end")
        .build()
        .unwrap()
}

/// start -> charge (service task, PT5S NON-interrupting CYCLE timer boundary
///                   "tick") -> done
///                       \--(timer, every 5s)--> ticked
fn process_with_non_interrupting_cycle_timer_boundary() -> ProcessDefinition {
    ProcessBuilder::new("ticker")
        .start_event("start")
        .service_task("charge", "payment")
        .non_interrupting_timer_cycle_boundary_event("tick", "charge", 5_000)
        .end_event("done")
        .end_event("ticked")
        .connect("start", "charge")
        .connect("charge", "done")
        .connect("tick", "ticked")
        .build()
        .unwrap()
}

/// start -> sub[ sub_start -> inner(work) -> sub_end ] --normal--> done
///          sub --(PT5S interrupting timer boundary)--> escalated
fn process_with_subprocess_timer_boundary() -> ProcessDefinition {
    ProcessBuilder::new("sub-timer")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .service_task("inner", "work")
        .contained_in("inner", "sub")
        .end_event("sub_end")
        .contained_in("sub_end", "sub")
        .timer_boundary_event("timeout", "sub", 5_000)
        .end_event("done")
        .end_event("escalated")
        .connect("start", "sub")
        .connect("sub_start", "inner")
        .connect("inner", "sub_end")
        .connect("sub", "done")
        .connect("timeout", "escalated")
        .build()
        .unwrap()
}

/// start -> sub[ sub_start -> inner(work) -> sub_end ] --normal--> done
///          sub --(interrupting message boundary "cancel" on orderId)--> aborted
fn process_with_subprocess_message_boundary() -> ProcessDefinition {
    ProcessBuilder::new("sub-msg")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .service_task("inner", "work")
        .contained_in("inner", "sub")
        .end_event("sub_end")
        .contained_in("sub_end", "sub")
        .message_boundary_event("cancel", "sub", "order-cancelled", "orderId")
        .end_event("done")
        .end_event("aborted")
        .connect("start", "sub")
        .connect("sub_start", "inner")
        .connect("inner", "sub_end")
        .connect("sub", "done")
        .connect("cancel", "aborted")
        .build()
        .unwrap()
}

/// start -> charge(payment) --normal--> done
///     charge --(error CARD_DECLINED boundary)--> recover(recovery) -> rec_done
fn process_with_error_boundary_to_task() -> ProcessDefinition {
    ProcessBuilder::new("payment-recover")
        .start_event("s")
        .service_task("charge", "payment")
        .error_boundary_event("boundary", "charge", "CARD_DECLINED")
        .service_task("recover", "recovery")
        .end_event("done")
        .end_event("rec_done")
        .connect("s", "charge")
        .connect("charge", "done")
        .connect("boundary", "recover")
        .connect("recover", "rec_done")
        .build()
        .unwrap()
}

// ---- message start events ----

/// (message "order-placed") --> start -> end
fn process_with_message_start() -> ProcessDefinition {
    ProcessBuilder::new("order-flow")
        .message_start_event("start", "order-placed")
        .end_event("end")
        .connect("start", "end")
        .build()
        .unwrap()
}

/// message-start "start"(probe-alert) -> service task "host"(agent), with an
/// interrupting message boundary "bnd"(probe-alert, correlating on customerId)
/// on the host. The SAME message name is subscribed both at the process level
/// (start event) and by the boundary on a running instance.
fn process_message_start_and_boundary() -> ProcessDefinition {
    ProcessBuilder::new("agent")
        .message_start_event("start", "probe-alert")
        .service_task("host", "agent")
        .message_boundary_event("bnd", "host", "probe-alert", "customerId")
        .end_event("running")
        .end_event("interrupted")
        .connect("start", "host")
        .connect("host", "running")
        .connect("bnd", "interrupted")
        .build()
        .unwrap()
}

// ---- timer start events ----

/// (timer, one-shot PT10S) --> start -> end
fn process_with_timer_start_once() -> ProcessDefinition {
    ProcessBuilder::new("delayed-start")
        .timer_start_event_once("start", 10_000)
        .end_event("end")
        .connect("start", "end")
        .build()
        .unwrap()
}

/// (timer, cycle every 10S) --> start -> end
fn process_with_timer_start_cycle() -> ProcessDefinition {
    ProcessBuilder::new("recurring-start")
        .timer_start_event_cycle("start", 10_000)
        .end_event("end")
        .connect("start", "end")
        .build()
        .unwrap()
}

// ---- multi-start processes (Zeebe parity, #855) ----
//
// Zeebe permits a process to declare several start events — a none start
// alongside any number of message/timer starts — and every one is *live*: the
// none start accepts CreateInstance, and each typed start opens its own
// deploy-time trigger (subscription / armed timer) that fires an independent
// instance at its own start element.

/// none-start "a_none" -> a_end ; message-start "b_msg"(order-placed) -> b_end
fn process_none_plus_message() -> ProcessDefinition {
    ProcessBuilder::new("dual-start")
        .start_event("a_none")
        .message_start_event("b_msg", "order-placed")
        .end_event("a_end")
        .end_event("b_end")
        .connect("a_none", "a_end")
        .connect("b_msg", "b_end")
        .build()
        .unwrap()
}

/// none-start "a_none" -> a_end ; timer-start "b_timer"(once PT10S) -> b_end
fn process_none_plus_timer() -> ProcessDefinition {
    ProcessBuilder::new("dual-timer")
        .start_event("a_none")
        .timer_start_event_once("b_timer", 10_000)
        .end_event("a_end")
        .end_event("b_end")
        .connect("a_none", "a_end")
        .connect("b_timer", "b_end")
        .build()
        .unwrap()
}

fn assert_job_index_consistent(engine: &Engine) {
    use std::collections::{BTreeSet, HashMap, HashSet};
    let mut expected: HashMap<String, BTreeSet<(i32, Key)>> = HashMap::new();
    let mut expected_activated: HashSet<Key> = HashSet::new();
    for job in engine.state().jobs.values() {
        if job.state == state::JobState::Created {
            expected
                .entry(job.job_type.clone())
                .or_default()
                .insert(state::activation_order(job.priority, job.key));
        }
        if job.state == state::JobState::Activated {
            expected_activated.insert(job.key);
        }
    }
    assert_eq!(
        engine.state().activatable_jobs,
        expected,
        "activatable index drifted from jobs"
    );
    assert_eq!(
        engine.state().activated_jobs,
        expected_activated,
        "activated index drifted from jobs"
    );
    let mut expected_by_instance: HashMap<Key, HashSet<Key>> = HashMap::new();
    for job in engine.state().jobs.values() {
        expected_by_instance
            .entry(job.instance_key)
            .or_default()
            .insert(job.key);
    }
    assert_eq!(
        engine.state().jobs_by_instance,
        expected_by_instance,
        "jobs_by_instance index drifted from jobs"
    );
}

// ---- cancel process instance ----

// ---- modify process instance ----

/// A linear process with two service tasks (`a` → `b`) so a modify can move a
/// token from one to the other.
fn two_service_tasks() -> ProcessDefinition {
    ProcessBuilder::new("two")
        .start_event("start")
        .service_task("a", "jobA")
        .service_task("b", "jobB")
        .end_event("end")
        .connect("start", "a")
        .connect("a", "b")
        .connect("b", "end")
        .build()
        .unwrap()
}

fn active_eik_of(engine: &Engine, instance_key: Key, element_id: &str) -> Key {
    *engine
        .instance(instance_key)
        .unwrap()
        .active
        .iter()
        .find(|(_, id)| id.as_str() == element_id)
        .map(|(k, _)| k)
        .unwrap_or_else(|| panic!("no active element instance for {element_id}"))
}

/// A single phase process: pstart -> work(job) -> pend.
fn phase_process(id: &str, job: &str) -> ProcessDefinition {
    ProcessBuilder::new(id)
        .start_event("pstart")
        .service_task("work", job)
        .end_event("pend")
        .connect("pstart", "work")
        .connect("work", "pend")
        .build()
        .unwrap()
}

/// Deploys an orchestrator with a raw (un-inlined) call activity `c1 -> phase`
/// and returns the running engine. The call activity carries the given io
/// mappings so tests can exercise cross-boundary variable propagation.
fn deploy_native_call(io: crate::model::IoMapping, child: ProcessDefinition) -> Engine {
    let mut orchestrator = ProcessBuilder::new("orch")
        .start_event("start")
        .call_activity("c1", "phase")
        .end_event("end")
        .connect("start", "c1")
        .connect("c1", "end");
    if !io.is_empty() {
        orchestrator = orchestrator.with_io("c1", io);
    }
    let orchestrator = orchestrator.build().unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(child)).unwrap();
    engine
        .apply_command(Command::DeployProcess(orchestrator))
        .unwrap();
    engine
}

/// Deploys the `orch` orchestrator whose `c1` call activity invokes `phase`
/// (child) with explicit Zeebe `propagateAllParentVariables` /
/// `propagateAllChildVariables` flags, optionally with an ioMapping.
fn deploy_native_call_with_propagation(
    io: crate::model::IoMapping,
    child: ProcessDefinition,
    propagate_all_parent: bool,
    propagate_all_child: bool,
) -> Engine {
    let mut orchestrator = ProcessBuilder::new("orch")
        .start_event("start")
        .call_activity_with_propagation("c1", "phase", propagate_all_parent, propagate_all_child)
        .end_event("end")
        .connect("start", "c1")
        .connect("c1", "end");
    if !io.is_empty() {
        orchestrator = orchestrator.with_io("c1", io);
    }
    let orchestrator = orchestrator.build().unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(child)).unwrap();
    engine
        .apply_command(Command::DeployProcess(orchestrator))
        .unwrap();
    engine
}

/// A pass-through callee that, while running, writes two of its own variables:
/// a fresh `childOnly` and an overwrite of the shared `shared` (so a merge-back
/// collision is observable — Zeebe semantics: the child's value wins).
fn propagating_child() -> ProcessDefinition {
    ProcessBuilder::new("phase")
        .start_event("pstart")
        .script_task("s1", "=99", "childOnly")
        .script_task("s2", "=\"child\"", "shared")
        .end_event("pend")
        .connect("pstart", "s1")
        .connect("s1", "s2")
        .connect("s2", "pend")
        .build()
        .unwrap()
}

/// Finds the child instance's seed variables (its `ProcessInstanceCreated`).
fn child_seed_of(events: &[Event]) -> HashMap<String, Value> {
    events
        .iter()
        .find_map(|e| match e {
            Event::ProcessInstanceCreated {
                process_id,
                variables,
                ..
            } if process_id == "phase" => Some(variables.clone()),
            _ => None,
        })
        .expect("child created")
}

/// The instance key of the `ProcessInstanceCreated` event for `process_id`.
///
/// A native call activity emits `ProcessInstanceCreated` for **both** the parent
/// and the spawned child, so selecting the first event carrying an instance key
/// is order-dependent and could latch onto the child. Match the parent's
/// `process_id` explicitly instead.
fn parent_key_of(events: &[Event], process_id: &str) -> Key {
    events
        .iter()
        .find_map(|e| match e {
            Event::ProcessInstanceCreated {
                instance_key,
                process_id: pid,
                ..
            } if pid == process_id => Some(*instance_key),
            _ => None,
        })
        .expect("parent process instance created")
}

/// The last value a variable took in the parent scope across the command's
/// `VariablesUpdated` events (`None` if it never crossed back).
fn parent_var_after<'a>(events: &'a [Event], parent_key: Key, name: &str) -> Option<&'a Value> {
    events.iter().rev().find_map(|e| match e {
        Event::VariablesUpdated {
            instance_key,
            variables,
        } if *instance_key == parent_key => variables.get(name),
        _ => None,
    })
}

// --- zeebe:ioMapping (FEEL input/output variable mappings) ------------------

fn io_var(engine: &Engine, key: Key, name: &str) -> Option<Value> {
    engine.instance(key)?.variables.get(name).cloned()
}

/// The value a `VariablesUpdated` event in `events` merged for `name` (last
/// wins), used to inspect variables that a completed instance no longer retains
/// in hot state.
fn merged_var(events: &[Event], name: &str) -> Option<Value> {
    events.iter().rev().find_map(|e| match e {
        Event::VariablesUpdated { variables, .. } => variables.get(name).cloned(),
        _ => None,
    })
}

// --- Conditional events (conditionalEventDefinition, re-evaluated on variable
// change), FEEL parity feature 6. ---

/// start -> gate (conditional catch: `= approved = true`) -> end
fn process_with_conditional_catch() -> ProcessDefinition {
    ProcessBuilder::new("await-approval")
        .start_event("start")
        .conditional_intermediate_catch_event("gate", "=approved = true")
        .end_event("end")
        .connect("start", "gate")
        .connect("gate", "end")
        .build()
        .unwrap()
}

/// start -> work (service task) with an interrupting conditional boundary
///          `= cancel = true` -> done ; boundary -> aborted
fn process_with_conditional_boundary(interrupting: bool) -> ProcessDefinition {
    let builder = ProcessBuilder::new("guarded-cond")
        .start_event("start")
        .service_task("work", "do-work");
    let builder = if interrupting {
        builder.conditional_boundary_event("bnd", "work", "=cancel = true")
    } else {
        builder.non_interrupting_conditional_boundary_event("bnd", "work", "=ping = true")
    };
    builder
        .end_event("done")
        .end_event("aborted")
        .connect("start", "work")
        .connect("work", "done")
        .connect("bnd", "aborted")
        .build()
        .unwrap()
}

// ---------------------------------------------------------------------------
// Multi-instance activities (FEEL parity 7/7)
// ---------------------------------------------------------------------------

/// A single-service-task process whose task carries multi-instance
/// characteristics driven by the `items` variable. `output_element` doubles the
/// bound `item`, collected into `results`. A trailing `sink` service task parks
/// the token after the loop so the aggregated variables remain observable in hot
/// state (ADR 0012 clears variables only on instance completion).
fn multi_instance_service_process(sequential: bool) -> ProcessDefinition {
    ProcessBuilder::new("mi")
        .start_event("start")
        .service_task("each", "handle")
        .with_multi_instance(
            "each",
            crate::model::MultiInstance {
                input_collection: "=items".to_string(),
                input_element: Some("item".to_string()),
                output_collection: Some("results".to_string()),
                output_element: Some("=item * 2".to_string()),
                completion_condition: None,
                sequential,
            },
        )
        .service_task("sink", "sink-work")
        .end_event("end")
        .connect("start", "each")
        .connect("each", "sink")
        .connect("sink", "end")
        .build()
        .unwrap()
}

/// A process whose embedded sub-process `wave` carries multi-instance
/// characteristics driven by `=tasks`, binding each item to `task`. Unlike
/// [`multi_instance_service_process`] the MI body is a whole SUB-PROCESS scope
/// (start -> inner service task `impl` (job `do-work`) -> end), so this exercises
/// MI fan-out over a multi-step body. `output_element` echoes the bound `task`
/// into `done_tasks`. A trailing `sink` service task parks the token after the
/// join so the aggregate stays observable in hot state. This is the primitive
/// under the agent-fleet "sequential MI over waves wrapping parallel MI over a
/// wave's tasks" pattern (Magikcraft/nano-bpm#547).
fn multi_instance_subprocess(sequential: bool) -> ProcessDefinition {
    ProcessBuilder::new("mi-sub")
        .start_event("start")
        .sub_process("wave", "wave_start")
        .with_multi_instance(
            "wave",
            crate::model::MultiInstance {
                input_collection: "=tasks".to_string(),
                input_element: Some("task".to_string()),
                output_collection: Some("done_tasks".to_string()),
                output_element: Some("=task".to_string()),
                completion_condition: None,
                sequential,
            },
        )
        .start_event("wave_start")
        .contained_in("wave_start", "wave")
        .service_task("impl", "do-work")
        .contained_in("impl", "wave")
        .end_event("wave_end")
        .contained_in("wave_end", "wave")
        .service_task("sink", "sink-work")
        .end_event("end")
        .connect("start", "wave")
        .connect("wave_start", "impl")
        .connect("impl", "wave_end")
        .connect("wave", "sink")
        .connect("sink", "end")
        .build()
        .unwrap()
}

/// The bound `task` string of an activated `do-work` job (fails loudly if the
/// MI-bound item never reached the inner sub-process scope).
fn bound_task(job: &ActivatedJob) -> String {
    match job.variables.get("task") {
        Some(Value::Str(s)) => s.clone(),
        other => panic!("expected a bound `task` string in the child scope, got {other:?}"),
    }
}

/// Like `multi_instance_subprocess` but the `wave` sub-process carries its own
/// `zeebe:output` mapping (`tag = n * 10`) and the loop's `outputElement` reads
/// that mapped local (`=tag`). Proves an MI-child sub-process applies its own
/// output mappings into its local scope before the `outputElement` is collected
/// (Zeebe parity: `getVariableScopeKey` writes them to the instance's own scope,
/// visible to `outputElement`, NOT propagated to the parent).
fn multi_instance_subprocess_with_output_mapping() -> ProcessDefinition {
    ProcessBuilder::new("mi-sub-out")
        .start_event("start")
        .sub_process("wave", "wave_start")
        .with_multi_instance(
            "wave",
            crate::model::MultiInstance {
                input_collection: "=nums".to_string(),
                input_element: Some("n".to_string()),
                output_collection: Some("tags".to_string()),
                output_element: Some("=tag".to_string()),
                completion_condition: None,
                sequential: false,
            },
        )
        .with_io(
            "wave",
            crate::model::IoMapping {
                inputs: vec![],
                outputs: vec![crate::model::Mapping {
                    source: "=n * 10".to_string(),
                    target: "tag".to_string(),
                }],
            },
        )
        .start_event("wave_start")
        .contained_in("wave_start", "wave")
        .service_task("impl", "do-work")
        .contained_in("impl", "wave")
        .end_event("wave_end")
        .contained_in("wave_end", "wave")
        .service_task("sink", "sink-work")
        .end_event("end")
        .connect("start", "wave")
        .connect("wave_start", "impl")
        .connect("impl", "wave_end")
        .connect("wave", "sink")
        .connect("sink", "end")
        .build()
        .unwrap()
}

/// An MI service task `each` whose own `zeebe:input` mapping references the
/// per-child `inputElement` (`item`) and `loopCounter` — so the mapping can only
/// be evaluated correctly once those child bindings exist.
fn multi_instance_service_with_input_mapping() -> ProcessDefinition {
    ProcessBuilder::new("mi-in")
        .start_event("start")
        .service_task("each", "handle")
        .with_multi_instance(
            "each",
            crate::model::MultiInstance {
                input_collection: "=items".to_string(),
                input_element: Some("item".to_string()),
                output_collection: None,
                output_element: None,
                completion_condition: None,
                sequential: false,
            },
        )
        .with_io(
            "each",
            crate::model::IoMapping {
                inputs: vec![crate::model::Mapping {
                    source: "=item * 100 + loopCounter".to_string(),
                    target: "handle_arg".to_string(),
                }],
                outputs: vec![],
            },
        )
        .end_event("end")
        .connect("start", "each")
        .connect("each", "end")
        .build()
        .unwrap()
}

// ---------------------------------------------------------------------------
// Hierarchical variable scoping — machinery (Part C phase 1)
// ---------------------------------------------------------------------------

/// Applies a raw event straight to the engine's state (test-only shortcut for
/// exercising the scope appliers/helpers without a driving command).
fn apply_raw(engine: &mut Engine, event: Event) {
    crate::state::apply(&mut engine.state, &event);
}

// ---------------------------------------------------------------------------
// Zeebe variable-scope parity matrix (Part C phase: tests)
//
// Rounds out the ported Zeebe scope semantics beyond the single-level cases
// above: deep (3-level) nesting with mid-level shadowing, parallel sibling
// isolation, and local-write shadowing over an inherited root variable.
// ---------------------------------------------------------------------------

// --- DMN business rule task (native decision evaluation) --------------------

/// A one-decision DRG: a decision table `greeting` mapping `lang` -> a greeting
/// string, single string output (scalar result).
fn greeting_dmn() -> crate::dmn::DecisionRequirementsGraph {
    let xml = r##"<definitions xmlns="https://www.omg.org/spec/DMN/20191111/MODEL/" id="drg" name="drg">
      <decision id="greeting" name="Greeting">
        <decisionTable hitPolicy="UNIQUE">
          <input id="i1"><inputExpression id="e1" typeRef="string"><text>lang</text></inputExpression></input>
          <output id="o1" name="result" typeRef="string" />
          <rule id="r1"><inputEntry id="ie1"><text>"en"</text></inputEntry>
            <outputEntry id="oe1"><text>"hello"</text></outputEntry></rule>
          <rule id="r2"><inputEntry id="ie2"><text>"de"</text></inputEntry>
            <outputEntry id="oe2"><text>"hallo"</text></outputEntry></rule>
        </decisionTable>
      </decision>
    </definitions>"##;
    crate::dmn::parse_dmn(xml).unwrap()
}

fn brt_process(decision_id: &str, result_variable: Option<String>) -> ProcessDefinition {
    ProcessBuilder::new("brt")
        .start_event("s")
        .business_rule_task("decide", decision_id, result_variable)
        .end_event("e")
        .connect("s", "decide")
        .connect("decide", "e")
        .build()
        .unwrap()
}

// ---------------------------------------------------------------------------
// Ad-hoc sub-process runtime (ADR 0023 seam 2): the agentic activate-element
// loop. A JOB_WORKER ad-hoc container emits an "agent" job; the agent returns
// `activateElements[]`; the engine activates those inner tools as real element
// instances, drains them, re-emits the agent job for the next turn, and
// completes the container (writing its `outputCollection`) when the agent
// signals it is done.
// ---------------------------------------------------------------------------

fn adhoc_agent_process() -> ProcessDefinition {
    // A top-level JOB_WORKER ad-hoc container ("agent") with two service-task
    // tools. `outputElement="=result"` captures each tool's `result` output into
    // the container's `outputCollection` ("results").
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="results" outputElement="=result" />
            </bpmn:extensionElements>
            <bpmn:serviceTask id="toolA">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="tool" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
            <bpmn:serviceTask id="toolB">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="tool" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    crate::bpmn::parse_bpmn(xml).unwrap().remove(0)
}

fn adhoc_agent_with_user_task_tool() -> ProcessDefinition {
    // A JOB_WORKER ad-hoc container whose tool catalog mixes a service-task tool
    // (`toolA`, a `tool` job) and a native user-task tool (`ask`, a
    // human-in-the-loop tool with a static assignee). ADR 0023 lists user tasks
    // as an in-scope v1 tool kind: activating one must create a real user task
    // and park the child until it is completed — not silently pass through.
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="results" outputElement="=result" />
            </bpmn:extensionElements>
            <bpmn:serviceTask id="toolA">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="tool" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
            <bpmn:userTask id="ask">
              <bpmn:extensionElements>
                <zeebe:assignmentDefinition assignee="alice" />
              </bpmn:extensionElements>
            </bpmn:userTask>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    crate::bpmn::parse_bpmn(xml).unwrap().remove(0)
}

fn adhoc_agent_named_tools_process() -> ProcessDefinition {
    // Like `adhoc_agent_process` but the two tools carry human `name`s, so the
    // advertised tool catalog can be checked for both `elementId` and
    // `elementName`.
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="results" outputElement="=result" />
            </bpmn:extensionElements>
            <bpmn:serviceTask id="toolA" name="Search the web">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="tool" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
            <bpmn:serviceTask id="toolB" name="Send an email">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="tool" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    crate::bpmn::parse_bpmn(xml).unwrap().remove(0)
}

fn adhoc_agent_with_tool_input_mapping() -> ProcessDefinition {
    // A JOB_WORKER ad-hoc container whose `toolA` carries an input
    // `zeebe:ioMapping` (`weighted = base + 1`). `base` is resolved from the
    // container scope, so a bad `base` fails the tool's input mapping on
    // activation — the #946 ad-hoc-tool input path.
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="results" outputElement="=result" />
            </bpmn:extensionElements>
            <bpmn:serviceTask id="toolA">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="tool" />
                <zeebe:ioMapping>
                  <zeebe:input source="=base + 1" target="weighted" />
                </zeebe:ioMapping>
              </bpmn:extensionElements>
            </bpmn:serviceTask>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    crate::bpmn::parse_bpmn(xml).unwrap().remove(0)
}

fn adhoc_agent_with_fromai_tool_input_mapping() -> ProcessDefinition {
    // The reproduction from nanobpm/nano-bpm#1200: a JOB_WORKER ad-hoc container
    // whose `toolA` carries a tool input `zeebe:ioMapping` written the standard
    // Camunda agentic way — `=fromAi(toolCall.foo, "desc", "string")`. The value
    // the LLM supplies arrives as `toolCall.foo` in the activation variables;
    // `fromAi` must return it unchanged (no `IO_MAPPING_ERROR` incident).
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="results" outputElement="=result" />
            </bpmn:extensionElements>
            <bpmn:serviceTask id="toolA">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="tool" />
                <zeebe:ioMapping>
                  <zeebe:input source="=fromAi(toolCall.foo, &#34;desc&#34;, &#34;string&#34;)" target="mapped" />
                </zeebe:ioMapping>
              </bpmn:extensionElements>
            </bpmn:serviceTask>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    crate::bpmn::parse_bpmn(xml).unwrap().remove(0)
}

fn activate_element(id: &str) -> crate::model::AdHocActivateElement {
    activate_element_with(id, &[])
}

fn activate_element_with(
    id: &str,
    variables: &[(&str, Value)],
) -> crate::model::AdHocActivateElement {
    crate::model::AdHocActivateElement {
        element_id: id.to_string(),
        variables: variables
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect(),
    }
}

fn adhoc_agent_with_interrupting_message_boundary() -> ProcessDefinition {
    // An `adHocSubProcess` (JOB_WORKER agent) carrying an interrupting message
    // boundary event, correlating on `customerId`. This is the executable shape
    // of Camunda's event-driven agent pattern (#1155): a newly-arrived event
    // must abandon the agent mid-investigation, including while its own
    // human-consult (`InnerTask`) task is open.
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="Host">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="probe-agent" />
              <zeebe:adHoc outputCollection="r" outputElement="=toolCallResult" />
            </bpmn:extensionElements>
            <bpmn:userTask id="InnerTask">
              <bpmn:extensionElements>
                <zeebe:userTask />
              </bpmn:extensionElements>
            </bpmn:userTask>
          </bpmn:adHocSubProcess>
          <bpmn:boundaryEvent id="Bnd" attachedToRef="Host">
            <bpmn:messageEventDefinition messageRef="M" />
          </bpmn:boundaryEvent>
          <bpmn:endEvent id="EndNormal" />
          <bpmn:endEvent id="EndInterrupted" />
          <bpmn:sequenceFlow id="F1" sourceRef="s" targetRef="Host" />
          <bpmn:sequenceFlow id="F2" sourceRef="Host" targetRef="EndNormal" />
          <bpmn:sequenceFlow id="F3" sourceRef="Bnd" targetRef="EndInterrupted" />
        </bpmn:process>
        <bpmn:message id="M" name="probe-cancel">
          <bpmn:extensionElements>
            <zeebe:subscription correlationKey="=customerId" />
          </bpmn:extensionElements>
        </bpmn:message>
      </bpmn:definitions>"#;
    crate::bpmn::parse_bpmn(xml).unwrap().remove(0)
}

fn adhoc_agent_chained_tools_process() -> ProcessDefinition {
    // A JOB_WORKER ad-hoc container whose two service-task tools are joined by a
    // plain `bpmn:sequenceFlow` BETWEEN THE CONTAINER'S OWN CHILDREN (issue
    // #1154): `toolA -> toolB`. Camunda documents this "structured sequence" —
    // activating `toolA` alone must, on its completion, take the flow and run
    // `toolB`; the container re-emits its agent job only once the whole chain
    // drains. `toolA`/`toolB` carry distinct job types so each is drained
    // independently by the test.
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="results" outputElement="=result" />
            </bpmn:extensionElements>
            <bpmn:serviceTask id="toolA">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="toolA-type" />
              </bpmn:extensionElements>
              <bpmn:outgoing>chain</bpmn:outgoing>
            </bpmn:serviceTask>
            <bpmn:sequenceFlow id="chain" sourceRef="toolA" targetRef="toolB" />
            <bpmn:serviceTask id="toolB">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="toolB-type" />
              </bpmn:extensionElements>
              <bpmn:incoming>chain</bpmn:incoming>
            </bpmn:serviceTask>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    crate::bpmn::parse_bpmn(xml).unwrap().remove(0)
}

fn adhoc_agent_chained_tools_completion_condition_process() -> ProcessDefinition {
    // Like `adhoc_agent_chained_tools_process` (the `toolA -> toolB` structured
    // sequence, issue #1154) but the container also declares a
    // `<completionCondition>=done = true`. When `toolA` completes with
    // `done = true`, the container's completion condition fires MID-CHAIN: the
    // sub-process must complete at once rather than take `toolA`'s outgoing flow
    // and activate the follow-up `toolB`.
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="results" outputElement="=result" />
            </bpmn:extensionElements>
            <bpmn:completionCondition>=done = true</bpmn:completionCondition>
            <bpmn:serviceTask id="toolA">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="toolA-type" />
              </bpmn:extensionElements>
              <bpmn:outgoing>chain</bpmn:outgoing>
            </bpmn:serviceTask>
            <bpmn:sequenceFlow id="chain" sourceRef="toolA" targetRef="toolB" />
            <bpmn:serviceTask id="toolB">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="toolB-type" />
              </bpmn:extensionElements>
              <bpmn:incoming>chain</bpmn:incoming>
            </bpmn:serviceTask>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    crate::bpmn::parse_bpmn(xml).unwrap().remove(0)
}

fn adhoc_agent_with_embedded_subprocess_tool() -> ProcessDefinition {
    // A JOB_WORKER ad-hoc container whose tool `review` is a plain embedded
    // `bpmn:subProcess` with a MULTI-ELEMENT token-flow body
    // (startEvent -> userTask `ask` -> endEvent). This is the camunda.com
    // `/orchestrate/agents/` "Loan decision review" governance construct: a
    // subprocess tool that runs a human review (and, in the full model, a
    // routing gateway) as its own inner flow before the tool completes and
    // feeds the agent loop.
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="results" outputElement="=result" />
            </bpmn:extensionElements>
            <bpmn:subProcess id="review">
              <bpmn:startEvent id="r_s" />
              <bpmn:userTask id="ask">
                <bpmn:extensionElements>
                  <zeebe:assignmentDefinition assignee="alice" />
                </bpmn:extensionElements>
              </bpmn:userTask>
              <bpmn:endEvent id="r_e" />
              <bpmn:sequenceFlow id="rf1" sourceRef="r_s" targetRef="ask" />
              <bpmn:sequenceFlow id="rf2" sourceRef="ask" targetRef="r_e" />
            </bpmn:subProcess>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    crate::bpmn::parse_bpmn(xml).unwrap().remove(0)
}

/// #872 (headline shape): the camunda.com "Loan decision review" governance
/// construct — an embedded subProcess tool whose body is a MULTI-element
/// token-flow with a routing exclusive gateway (human review → gateway → one of
/// two outcomes). Activating it must run the whole body (user task, then the
/// gateway routing) and only complete the tool when the chosen branch reaches an
/// end event — proving token flow past a single leaf, through a gateway.
fn adhoc_agent_with_loan_decision_review_tool() -> ProcessDefinition {
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="results" outputElement="=decision" />
            </bpmn:extensionElements>
            <bpmn:subProcess id="review">
              <bpmn:startEvent id="r_s" />
              <bpmn:userTask id="officer">
                <bpmn:extensionElements>
                  <zeebe:assignmentDefinition assignee="senior" />
                </bpmn:extensionElements>
              </bpmn:userTask>
              <bpmn:exclusiveGateway id="r_gw" />
              <bpmn:endEvent id="r_offer" />
              <bpmn:endEvent id="r_decline" />
              <bpmn:sequenceFlow id="rf1" sourceRef="r_s" targetRef="officer" />
              <bpmn:sequenceFlow id="rf2" sourceRef="officer" targetRef="r_gw" />
              <bpmn:sequenceFlow id="rf3" sourceRef="r_gw" targetRef="r_offer">
                <bpmn:conditionExpression xsi:type="bpmn:tFormalExpression"
                  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">=decision = "approve"</bpmn:conditionExpression>
              </bpmn:sequenceFlow>
              <bpmn:sequenceFlow id="rf4" sourceRef="r_gw" targetRef="r_decline">
                <bpmn:conditionExpression xsi:type="bpmn:tFormalExpression"
                  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">=decision != "approve"</bpmn:conditionExpression>
              </bpmn:sequenceFlow>
            </bpmn:subProcess>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    crate::bpmn::parse_bpmn(xml).unwrap().remove(0)
}

// Reads the ad-hoc container's local `outputCollection` variable (`results`)
// straight off the container scope — the live value visible mid-run, before the
// container completes and propagates it outward.
fn container_output_collection(
    engine: &Engine,
    instance_key: Key,
    container: Key,
    name: &str,
) -> Option<Value> {
    engine
        .instance(instance_key)
        .unwrap()
        .scope_variables
        .get(&container)
        .and_then(|m| m.get(name))
        .cloned()
}

fn adhoc_non_array_output_collection_process() -> ProcessDefinition {
    // A container whose own input mapping overwrites the seeded `outputCollection`
    // (`results`) with a scalar — the misconfiguration Zeebe guards against.
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="results" outputElement="=result" />
              <zeebe:ioMapping>
                <zeebe:input source="=5" target="results" />
              </zeebe:ioMapping>
            </bpmn:extensionElements>
            <bpmn:serviceTask id="toolA">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="tool" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    crate::bpmn::parse_bpmn(xml).unwrap().remove(0)
}

// --- ADR 0023 seam 4: completion condition + tool ioMapping ---------------

fn adhoc_completion_condition_process() -> ProcessDefinition {
    // An ad-hoc container whose `<completionCondition>` ends the loop once a tool
    // sets `done = true`. Two tools are activatable so the condition firing after
    // the first completes must cancel the second.
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="results" outputElement="=result" />
            </bpmn:extensionElements>
            <bpmn:completionCondition>=done = true</bpmn:completionCondition>
            <bpmn:serviceTask id="toolA">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="tool" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
            <bpmn:serviceTask id="toolB">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="tool" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    crate::bpmn::parse_bpmn(xml).unwrap().remove(0)
}

fn adhoc_completion_condition_defer_process() -> ProcessDefinition {
    // Identical to `adhoc_completion_condition_process`, but the container carries
    // `cancelRemainingInstances="false"`: a fulfilled `<completionCondition>` must
    // NOT cancel the still-running tool — the container defers its completion until
    // that tool drains, collecting its output too.
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent" cancelRemainingInstances="false">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="results" outputElement="=result" />
            </bpmn:extensionElements>
            <bpmn:completionCondition>=done = true</bpmn:completionCondition>
            <bpmn:serviceTask id="toolA">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="tool" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
            <bpmn:serviceTask id="toolB">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="tool" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    crate::bpmn::parse_bpmn(xml).unwrap().remove(0)
}

fn adhoc_tool_io_process() -> ProcessDefinition {
    // A container whose tool declares a `zeebe:ioMapping`: an input mapping
    // (`=base + 1` → `n`, local to the tool) and an output mapping
    // (`=result` → `status`, projected into the container scope). The container's
    // completionCondition reads that projected `status`.
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="results" outputElement="=result" />
            </bpmn:extensionElements>
            <bpmn:completionCondition>=status = "ok"</bpmn:completionCondition>
            <bpmn:serviceTask id="toolA">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="tool" />
                <zeebe:ioMapping>
                  <zeebe:input source="=base + 1" target="n" />
                  <zeebe:output source="=result" target="status" />
                </zeebe:ioMapping>
              </bpmn:extensionElements>
            </bpmn:serviceTask>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    crate::bpmn::parse_bpmn(xml).unwrap().remove(0)
}

// --- ADR 0023 v1.1: declarative (BPMN_TASK) ad-hoc sub-process --------------

fn adhoc_declarative_process() -> ProcessDefinition {
    // A declarative ad-hoc container: no `zeebe:taskDefinition` (so it is NOT a
    // job worker), only a `zeebe:adHoc activeElementsCollection` FEEL expression
    // naming the inner elements to activate. Each tool captures its `result` into
    // the container's `outputCollection` ("results").
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="sub">
            <bpmn:extensionElements>
              <zeebe:adHoc activeElementsCollection="=tools" outputCollection="results" outputElement="=result" />
            </bpmn:extensionElements>
            <bpmn:serviceTask id="toolA">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="tool" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
            <bpmn:serviceTask id="toolB">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="tool" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="sub" />
          <bpmn:sequenceFlow id="f2" sourceRef="sub" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    crate::bpmn::parse_bpmn(xml).unwrap().remove(0)
}

fn create_instance_with_vars(
    engine: &mut Engine,
    process_id: &str,
    variables: HashMap<String, Value>,
) -> Key {
    engine
        .apply_command(Command::create_instance_with(process_id, variables))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap()
}

// --- Execution listeners (ADR 0037) -----------------------------------------

use crate::model::{ExecutionListener, ListenerEventType};

fn el(event_type: ListenerEventType, job_type: &str) -> ExecutionListener {
    ExecutionListener {
        event_type,
        job_type: job_type.to_string(),
        retries: None,
    }
}

/// A linear process whose single service task `charge` (job `payment`) carries
/// the given start/end execution listeners.
fn task_with_listeners(
    start: Vec<ExecutionListener>,
    end: Vec<ExecutionListener>,
) -> ProcessDefinition {
    ProcessBuilder::new("order")
        .start_event("start")
        .service_task("charge", "payment")
        .end_event("end")
        .connect("start", "charge")
        .connect("charge", "end")
        .with_listeners("charge", start, end)
        .build()
        .unwrap()
}

fn kinds(events: &[Event]) -> Vec<&'static str> {
    events
        .iter()
        .map(|e| match e {
            Event::ElementActivating { .. } => "Activating",
            Event::ElementActivated { .. } => "Activated",
            Event::ElementCompleting { .. } => "Completing",
            Event::ElementCompleted { .. } => "Completed",
            Event::JobCreated { .. } => "JobCreated",
            Event::ExecutionListenerJobCreated { .. } => "ListenerJobCreated",
            Event::JobCompleted { .. } => "JobCompleted",
            Event::JobActivated { .. } => "JobActivated",
            Event::SequenceFlowTaken { .. } => "FlowTaken",
            Event::ProcessInstanceCompleted { .. } => "InstanceCompleted",
            _ => "_",
        })
        .collect()
}

/// A parallel multi-instance service task `each` (job `handle`) over `items`,
/// carrying the given start/end execution listeners on the multi-instance body.
fn mi_body_with_listeners(
    start: Vec<ExecutionListener>,
    end: Vec<ExecutionListener>,
) -> ProcessDefinition {
    ProcessBuilder::new("mi")
        .start_event("start")
        .service_task("each", "handle")
        .with_multi_instance(
            "each",
            crate::model::MultiInstance {
                input_collection: "=items".to_string(),
                input_element: Some("item".to_string()),
                output_collection: None,
                output_element: None,
                completion_condition: None,
                sequential: false,
            },
        )
        .end_event("end")
        .connect("start", "each")
        .connect("each", "end")
        .with_listeners("each", start, end)
        .build()
        .unwrap()
}

// --- Task listeners (ADR 0037 §6) -------------------------------------------

use crate::model::{
    TaskListener, TaskListenerEventType, TaskListenerJobResult, UserTaskCorrections,
};

fn tl(event_type: TaskListenerEventType, job_type: &str) -> TaskListener {
    TaskListener {
        event_type,
        job_type: job_type.to_string(),
        retries: None,
    }
}

/// `start -> review (user task, with the given task listeners) -> end`.
fn user_task_with_listeners(listeners: Vec<TaskListener>) -> ProcessDefinition {
    ProcessBuilder::new("approval")
        .start_event("start")
        .user_task("review")
        .end_event("end")
        .connect("start", "review")
        .connect("review", "end")
        .with_task_listeners("review", listeners)
        .build()
        .unwrap()
}

/// Deploys `def`, creates an instance and returns `(engine, instance_key)`.
fn deploy_and_start(def: ProcessDefinition) -> (Engine, Key) {
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("approval"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    (engine, instance_key)
}

fn only_user_task_key(engine: &Engine) -> Key {
    let keys: Vec<Key> = engine.state().user_tasks.keys().copied().collect();
    assert_eq!(keys.len(), 1, "expected exactly one user task");
    keys[0]
}

/// `start -> review (user task w/ props + listeners) -> end`.
fn user_task_with_props_and_listeners(
    props: crate::model::UserTaskProps,
    listeners: Vec<TaskListener>,
) -> ProcessDefinition {
    ProcessBuilder::new("approval")
        .start_event("start")
        .user_task_with("review", props)
        .end_event("end")
        .connect("start", "review")
        .connect("review", "end")
        .with_task_listeners("review", listeners)
        .build()
        .unwrap()
}

/// Builds the classic event-based gateway race: a gateway routing to a timer
/// intermediate catch (`onTimer`, due 5s after activation) and a message
/// intermediate catch (`onReply`, correlated on `orderId`). Whichever fires
/// first wins; the loser is withdrawn.
fn event_gateway_race() -> ProcessDefinition {
    ProcessBuilder::new("race")
        .start_event("start")
        .event_based_gateway("gw")
        .timer_intermediate_catch_event("onTimer", 5_000)
        .message_intermediate_catch_event("onReply", "reply", "orderId")
        .end_event("timedOut")
        .end_event("replied")
        .connect("start", "gw")
        .connect("gw", "onTimer")
        .connect("gw", "onReply")
        .connect("onTimer", "timedOut")
        .connect("onReply", "replied")
        .build()
        .unwrap()
}

// Two event-based gateways route into the same catch event `onShared`, making
// the owning race ambiguous. When `onShared` wins we must NOT withdraw the
// sibling of either gateway, since we cannot tell which race it belonged to.
fn ambiguous_event_gateway_race() -> ProcessDefinition {
    ProcessBuilder::new("ambig")
        .start_event("start")
        .event_based_gateway("gw1")
        .event_based_gateway("gw2")
        .message_intermediate_catch_event("onShared", "reply", "orderId")
        .timer_intermediate_catch_event("onTimer1", 5_000)
        .timer_intermediate_catch_event("onTimer2", 5_000)
        .end_event("sharedEnd")
        .end_event("end1")
        .end_event("end2")
        .connect("start", "gw1")
        .connect("gw1", "onShared")
        .connect("gw1", "onTimer1")
        .connect("gw2", "onShared")
        .connect("gw2", "onTimer2")
        .connect("onShared", "sharedEnd")
        .connect("onTimer1", "end1")
        .connect("onTimer2", "end2")
        .build()
        .unwrap()
}

// A malformed model routes an event-based gateway into a non-catch node (a
// service task) alongside a genuine catch event. When the catch event wins, the
// service-task sibling must NOT be force-completed: doing so would emit
// `ElementCompleted` without cancelling its job, orphaning the work. Only genuine
// catch siblings are ever withdrawn.
fn malformed_event_gateway_with_service_task_sibling() -> ProcessDefinition {
    ProcessBuilder::new("malformed")
        .start_event("start")
        .event_based_gateway("gw")
        .message_intermediate_catch_event("onReply", "reply", "orderId")
        .service_task("work", "do-work")
        .end_event("replied")
        .end_event("worked")
        .connect("start", "gw")
        .connect("gw", "onReply")
        .connect("gw", "work")
        .connect("onReply", "replied")
        .connect("work", "worked")
        .build()
        .unwrap()
}

// ---- Correlation-key evaluation incident (durable fix for the silent hang) ----

// A catch whose correlation key concatenates a scalar (`planKey`) with a member
// of a variable (`task.id`) that is not yet a Map when the subscription opens.
// The FEEL `+` errors ("+ not defined for string and null"), which previously
// collapsed to an empty key and parked the token forever with no error.
fn process_with_concat_correlation_key() -> ProcessDefinition {
    ProcessBuilder::new("concat-corr")
        .start_event("start")
        .message_intermediate_catch_event("await", "answered", "=planKey + \":\" + task.id")
        .end_event("end")
        .connect("start", "await")
        .connect("await", "end")
        .build()
        .unwrap()
}

// --- Process-instance migration (Zeebe-behaviour parity) --------------------

/// Deploys `def` and returns the process definition key the engine assigned it.
fn deploy_for_migration(engine: &mut Engine, def: ProcessDefinition) -> Key {
    engine
        .apply_command(Command::DeployProcess(def))
        .unwrap()
        .iter()
        .find_map(|e| match e {
            Event::ProcessDeployed {
                process_definition_key,
                ..
            } => Some(*process_definition_key),
            _ => None,
        })
        .unwrap()
}

/// A linear process `id` whose single service task `task_id` emits `job_type`
/// jobs, parked while a worker is expected to pick them up.
fn migratable_task_process(id: &str, task_id: &str, job_type: &str) -> ProcessDefinition {
    ProcessBuilder::new(id)
        .start_event("start")
        .service_task(task_id, job_type)
        .end_event("end")
        .connect("start", task_id)
        .connect(task_id, "end")
        .build()
        .unwrap()
}

fn pending_job_element(engine: &Engine, instance_key: Key) -> String {
    engine
        .state()
        .jobs
        .values()
        .find(|j| j.instance_key == instance_key)
        .map(|j| j.element_id.clone())
        .expect("instance has a pending job")
}

// ===========================================================================
// Issue #750 — process-definition version support (Zeebe parity).
//
// The engine retains *every* deployed version of a process definition (keyed
// by process-definition key), pins each running instance to the exact version
// it was created on, and honours the create-time version selector (by explicit
// definition key, or by process id + version number, defaulting to latest).
// ===========================================================================

/// A `payment`-emitting "order" definition whose model differs from
/// `linear_with_task` by adding `extra` service tasks after `charge`, so each
/// distinct `extra` count deploys as a new, non-idempotent version of "order".
fn order_with_extra_tasks(extra: usize) -> ProcessDefinition {
    let mut b = ProcessBuilder::new("order")
        .start_event("start")
        .service_task("charge", "payment");
    let mut prev = "charge".to_string();
    for i in 0..extra {
        let id = format!("extra{i}");
        b = b.service_task(&id, "work");
        b = b.connect(&prev, &id);
        prev = id;
    }
    b.end_event("end")
        .connect("start", "charge")
        .connect(&prev, "end")
        .build()
        .unwrap()
}

/// Deploy `def` and return its `(process_definition_key, version)`.
fn deploy_returning_key(engine: &mut Engine, def: ProcessDefinition) -> (Key, i32) {
    engine
        .apply_command(Command::DeployProcess(def))
        .unwrap()
        .iter()
        .find_map(|e| match e {
            Event::ProcessDeployed {
                process_definition_key,
                version,
                ..
            } => Some((*process_definition_key, *version)),
            _ => None,
        })
        .expect("a changed definition emits ProcessDeployed")
}

// =====================================================================
// Nested ad-hoc sub-process tools (agent-of-agents) — Magikcraft/nano-bpm#631
// =====================================================================

/// An outer JOB_WORKER ad-hoc container (`agent`) whose single tool `subagent`
/// is itself a nested `adHocSubProcess` (a second-level agent) carrying its own
/// `zeebe:taskDefinition`, `outputCollection`, and one leaf service-task tool
/// `leaf`. This is the Zeebe "agent-of-agents" shape: `isAdHocActivity` admits a
/// nested `AD_HOC_SUB_PROCESS` as an activatable tool (issue #631).
fn nested_adhoc_agent_process() -> ProcessDefinition {
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="results" outputElement="=result" />
            </bpmn:extensionElements>
            <bpmn:adHocSubProcess id="subagent">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="sub-agent-worker" />
                <zeebe:adHoc outputCollection="subResults" outputElement="=leafOut" />
              </bpmn:extensionElements>
              <bpmn:serviceTask id="leaf">
                <bpmn:extensionElements>
                  <zeebe:taskDefinition type="leaf-tool" />
                </bpmn:extensionElements>
              </bpmn:serviceTask>
            </bpmn:adHocSubProcess>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    crate::bpmn::parse_bpmn(xml).unwrap().remove(0)
}

/// An outer JOB_WORKER container whose nested `subagent` declares a
/// `<completionCondition>` (`=done = true`) and holds TWO leaf tools.
/// Once the first leaf drains the nested container's condition fires and — with
/// the default `cancelRemainingInstances=true` — cancels the still-running
/// second leaf, then completes and feeds the OUTER container across the nesting
/// boundary.
fn nested_adhoc_cancel_process() -> ProcessDefinition {
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="results" outputElement="=done" />
            </bpmn:extensionElements>
            <bpmn:adHocSubProcess id="subagent">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="sub-agent-worker" />
                <zeebe:adHoc outputCollection="subResults" outputElement="=leafOut" />
              </bpmn:extensionElements>
              <bpmn:completionCondition>=done = true</bpmn:completionCondition>
              <bpmn:serviceTask id="leaf1">
                <bpmn:extensionElements>
                  <zeebe:taskDefinition type="leaf-tool" />
                </bpmn:extensionElements>
              </bpmn:serviceTask>
              <bpmn:serviceTask id="leaf2">
                <bpmn:extensionElements>
                  <zeebe:taskDefinition type="leaf-tool" />
                </bpmn:extensionElements>
              </bpmn:serviceTask>
            </bpmn:adHocSubProcess>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    crate::bpmn::parse_bpmn(xml).unwrap().remove(0)
}

// --- external (job-backed) agent parity (Camunda 8.10, #1099) ---------------

/// Deploy an `external` agent (a `serviceTask` bearing
/// `zeebe:agentDefinition agentType="external"`), start an instance, and return
/// the engine, the process-instance key and the agent element's active
/// element-instance key. All agent tasks auto-mint **no** AgentInstance —
/// they are job-backed — so the element-instance key is read
/// from the `ElementActivated` event, not from a minted record.
fn external_agent_instance() -> (Engine, Key, Key) {
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="agent-proc" isExecutable="true">
          <bpmn:startEvent id="start" />
          <bpmn:serviceTask id="agent">
            <bpmn:extensionElements>
              <zeebe:agentDefinition agentType="external" />
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:endEvent id="end" />
          <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="end" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml).unwrap().remove(0);
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let events = engine
        .apply_command(Command::create_instance("agent-proc"))
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();
    let eik = events
        .iter()
        .find_map(|e| match e {
            Event::ElementActivated {
                element_instance_key,
                element_id,
                ..
            } if element_id == "agent" => Some(*element_instance_key),
            _ => None,
        })
        .expect("the agent element should activate");
    (engine, instance_key, eik)
}

// --- AgentHistory turn log (Camunda 8.10 parity, Stage 3 / slice S2) --------

/// Deploy an `aiAgentTask` service task, start an instance, register its agent,
/// and return the engine together with the owning process-instance key and the
/// `agent_instance_key` — the fixture every AgentHistory test builds on.
fn agent_instance_for_history() -> (Engine, Key, Key) {
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="agent-proc" isExecutable="true">
          <bpmn:startEvent id="start" />
          <bpmn:serviceTask id="agent">
            <bpmn:extensionElements>
              <zeebe:agentDefinition agentType="aiAgentTask" />
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:endEvent id="end" />
          <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="end" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml).unwrap().remove(0);
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let events = engine
        .apply_command(Command::create_instance("agent-proc"))
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();
    let agent_instance_key = register_job_backed_agent(&mut engine, "agent").agent_instance_key;
    (engine, instance_key, agent_instance_key)
}

fn lease_options() -> crate::JobActivationOptions {
    crate::JobActivationOptions {
        with_lease: true,
        ..Default::default()
    }
}

/// Activate the fixture's job and explicitly register its agent using that lease.
fn register_job_backed_agent(engine: &mut Engine, job_type: &str) -> crate::agent::AgentInstance {
    register_job_backed_agent_with_history(engine, job_type, Vec::new())
}

fn register_job_backed_agent_with_history(
    engine: &mut Engine,
    job_type: &str,
    history: Vec<crate::agent::AgentHistoryTurn>,
) -> crate::agent::AgentInstance {
    let job = engine
        .activate_jobs_with_options(job_type, "W", 1, 60_000, 0, lease_options())
        .pop()
        .expect("the agent job should be available for activation");
    engine
        .apply_command(Command::CreateAgentInstance {
            element_instance_key: job.element_instance_key,
            job_key: job.key,
            job_lease: job.lease_token.expect("an activated job has a lease"),
            definition: crate::agent::AgentDefinition::default(),
            limits: None,
            history,
        })
        .unwrap()
        .into_iter()
        .find_map(|event| match event {
            Event::AgentInstanceCreated { agent_instance, .. } => Some(agent_instance),
            _ => None,
        })
        .expect("worker registration should mint an AgentInstance")
}

fn worker_history_turn(
    iteration: i32,
    produced_at: u64,
    role: crate::agent::AgentHistoryRole,
) -> crate::agent::AgentHistoryTurn {
    let mut turn = history_turn(iteration, produced_at, role);
    turn.history_item_id = Some(format!("{iteration}-{produced_at}"));
    if role == crate::agent::AgentHistoryRole::Configuration {
        turn.model = Some("gpt-4o".into());
        turn.provider = Some("test-provider".into());
        turn.system_prompt = Some(vec![crate::agent::AgentHistoryContent {
            content_type: crate::agent::AgentHistoryContentType::Text,
            text: Some("test-prompt".into()),
            document_reference: None,
            object: None,
        }]);
    }
    turn
}

fn agent_with_initial_history(history: Vec<crate::agent::AgentHistoryTurn>) -> (Engine, Key, Key) {
    let (mut engine, pi, _) = external_agent_instance();
    let aik =
        register_job_backed_agent_with_history(&mut engine, "agent", history).agent_instance_key;
    (engine, pi, aik)
}

/// A minimal AgentHistory turn carrying only the ordering-relevant fields.
fn history_turn(
    loop_iteration: i32,
    produced_at: u64,
    role: crate::agent::AgentHistoryRole,
) -> crate::agent::AgentHistoryTurn {
    crate::agent::AgentHistoryTurn {
        loop_iteration,
        produced_at,
        role,
        ..Default::default()
    }
}

/// The stored, ordered history log for `agent_instance_key`.
fn stored_history(
    engine: &Engine,
    instance_key: Key,
    agent_instance_key: Key,
) -> Vec<crate::agent::AgentHistoryRecord> {
    engine
        .state
        .instances
        .get(&instance_key)
        .and_then(|pi| pi.agent_history.get(&agent_instance_key))
        .cloned()
        .unwrap_or_default()
}

/// A history turn carrying a stable `historyItemId`, used to exercise the
/// idempotent-retry dedup path.
fn history_turn_with_id(
    loop_iteration: i32,
    produced_at: u64,
    role: crate::agent::AgentHistoryRole,
    history_item_id: &str,
) -> crate::agent::AgentHistoryTurn {
    crate::agent::AgentHistoryTurn {
        loop_iteration,
        produced_at,
        role,
        history_item_id: Some(history_item_id.to_string()),
        ..Default::default()
    }
}

// --- AgentInstance lifecycle processors (Camunda 8.10 parity, slice S3) ------
//
// CREATE/UPDATE/COMPLETE processors with the stable/8.10 validation rules and
// AgentInstanceLimits enforcement. History application reuses the S2
// batch-append behavior. Parity reference: camunda/camunda stable/8.10
// (8.10.0-SNAPSHOT).

/// The owning `element_instance_key` of the worker-registered AgentInstance `aik`.
fn agent_element_instance_key(engine: &Engine, instance_key: Key, aik: Key) -> Key {
    engine
        .state
        .instances
        .get(&instance_key)
        .and_then(|pi| pi.agent_instances.get(&aik))
        .map(|ai| ai.element_instance_key)
        .expect("the agent instance should be stored")
}

/// The stored AgentInstance record for `aik`.
fn stored_agent_instance(
    engine: &Engine,
    instance_key: Key,
    aik: Key,
) -> crate::agent::AgentInstance {
    engine
        .state
        .instances
        .get(&instance_key)
        .and_then(|pi| pi.agent_instances.get(&aik))
        .cloned()
        .expect("the agent instance should be stored")
}

/// Build an UPDATE command with the given status/metrics/history, asserting the
/// stored ownership fields (`element_id` "agent").
fn update_agent(
    engine: &Engine,
    aik: Key,
    eik: Key,
    pi: Key,
    status: crate::agent::AgentInstanceStatus,
    metrics: crate::agent::AgentInstanceMetricsDelta,
    history: Vec<crate::agent::AgentHistoryTurn>,
) -> Command {
    let (job_key, job_lease) = if history.is_empty() {
        (0, String::new())
    } else {
        let job = engine
            .state
            .jobs
            .values()
            .find(|job| job.element_instance_key == eik)
            .expect("history-bearing UPDATE requires the agent's job");
        (
            job.key,
            job.lease_token
                .clone()
                .expect("the agent job must be activated"),
        )
    };
    Command::UpdateAgentInstance {
        agent_instance_key: aik,
        element_instance_key: eik,
        element_id: "agent".to_string(),
        process_instance_key: pi,
        job_key,
        job_lease,
        status: Some(status),
        metrics,
        tools: None,
        history,
    }
}

/// Deploy a fork/join process with two parallel agent tasks and start it,
/// returning the engine, process-instance key, and the two agent-instance keys.
fn two_agent_instances() -> (Engine, Key, Vec<Key>) {
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="two-agents" isExecutable="true">
          <bpmn:startEvent id="start" />
          <bpmn:parallelGateway id="fork" />
          <bpmn:serviceTask id="agentA">
            <bpmn:extensionElements>
              <zeebe:agentDefinition agentType="aiAgentTask" />
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:serviceTask id="agentB">
            <bpmn:extensionElements>
              <zeebe:agentDefinition agentType="aiAgentTask" />
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:sequenceFlow id="f0" sourceRef="start" targetRef="fork" />
          <bpmn:sequenceFlow id="fa" sourceRef="fork" targetRef="agentA" />
          <bpmn:sequenceFlow id="fb" sourceRef="fork" targetRef="agentB" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml).unwrap().remove(0);
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let events = engine
        .apply_command(Command::create_instance("two-agents"))
        .unwrap();
    let pi = events.iter().find_map(|e| e.instance_key()).unwrap();
    let aiks: Vec<Key> = ["agentA", "agentB"]
        .into_iter()
        .map(|job_type| register_job_backed_agent(&mut engine, job_type).agent_instance_key)
        .collect();
    (engine, pi, aiks)
}

/// A parent process whose ad-hoc agent's only tool is a `bpmn:callActivity`
/// delegating to a separate `child` process (issue #1159). The tool maps
/// `askedAbout -> customerRequest` inbound and `{status, summary} ->
/// toolCallResult` outbound, with both propagate flags off (the child crosses
/// the instance boundary purely through the mappings). The container's
/// `outputElement` collects `toolCallResult`.
fn adhoc_agent_with_call_activity_tool() -> Vec<ProcessDefinition> {
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="parent">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="toolCallResults" outputElement="=toolCallResult" />
            </bpmn:extensionElements>
            <bpmn:callActivity id="CallSpecialist">
              <bpmn:extensionElements>
                <zeebe:calledElement processId="child"
                    propagateAllChildVariables="false"
                    propagateAllParentVariables="false" />
                <zeebe:ioMapping>
                  <zeebe:input source="=askedAbout" target="customerRequest" />
                  <zeebe:output source="={status: status, summary: summary}" target="toolCallResult" />
                </zeebe:ioMapping>
              </bpmn:extensionElements>
            </bpmn:callActivity>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
        <bpmn:process id="child">
          <bpmn:startEvent id="cs" />
          <bpmn:serviceTask id="specialist">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="probe-child" />
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:endEvent id="ce" />
          <bpmn:sequenceFlow id="cf1" sourceRef="cs" targetRef="specialist" />
          <bpmn:sequenceFlow id="cf2" sourceRef="specialist" targetRef="ce" />
        </bpmn:process>
      </bpmn:definitions>"#;
    crate::bpmn::parse_bpmn(xml).unwrap()
}

/// A parent whose ad-hoc `bpmn:callActivity` tool declares two CHAINED output
/// mappings (issue #1159): `summary -> summaryCopy`, then a second mapping whose
/// source references `summaryCopy`. Zeebe's `eval_io_mappings_in` is a SINGLE
/// pass — every mapping reads the original child-variable view — so the second
/// mapping sees no `summaryCopy` (it is a sibling target, not a child variable)
/// and its `summary` field resolves to `null`. This guards against the tool's
/// output mapping being evaluated TWICE (once against the child variables, then
/// again against the seeded tool scope): a second pass would see `summaryCopy`
/// and manufacture a non-null field, and — because `outputElement` reads the
/// first-pass value while the container projection would read the second — the
/// collected `outputElement` and the projected `toolCallResult` would DISAGREE.
fn adhoc_agent_with_chained_output_call_activity_tool() -> Vec<ProcessDefinition> {
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="parent">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="toolCallResults" outputElement="=toolCallResult" />
            </bpmn:extensionElements>
            <bpmn:callActivity id="CallSpecialist">
              <bpmn:extensionElements>
                <zeebe:calledElement processId="child"
                    propagateAllChildVariables="false"
                    propagateAllParentVariables="false" />
                <zeebe:ioMapping>
                  <zeebe:input source="=askedAbout" target="customerRequest" />
                  <zeebe:output source="=summary" target="summaryCopy" />
                  <zeebe:output source="={status: status, summary: summaryCopy}" target="toolCallResult" />
                </zeebe:ioMapping>
              </bpmn:extensionElements>
            </bpmn:callActivity>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
        <bpmn:process id="child">
          <bpmn:startEvent id="cs" />
          <bpmn:serviceTask id="specialist">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="probe-child" />
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:endEvent id="ce" />
          <bpmn:sequenceFlow id="cf1" sourceRef="cs" targetRef="specialist" />
          <bpmn:sequenceFlow id="cf2" sourceRef="specialist" targetRef="ce" />
        </bpmn:process>
      </bpmn:definitions>"#;
    crate::bpmn::parse_bpmn(xml).unwrap()
}

/// A parent process whose ad-hoc agent has a `bpmn:callActivity` tool that is
/// ALSO the head of a chained inner flow (issue #1154 × #1159): `CallSpecialist`
/// carries the same chained output mappings as
/// `adhoc_agent_with_chained_output_call_activity_tool` AND an outgoing
/// `bpmn:sequenceFlow` to a follow-up sibling `toolB`. Completing the tool
/// therefore hands off through `continue_adhoc_inner_flow` (the mid-chain path),
/// not the leaf path — so the precomputed single-pass projection must be carried
/// through the hand-off too, or the follow-up sees a double-applied mapping.
fn adhoc_agent_with_chained_flow_call_activity_tool() -> Vec<ProcessDefinition> {
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="parent">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="toolCallResults" outputElement="=result" />
            </bpmn:extensionElements>
            <bpmn:callActivity id="CallSpecialist">
              <bpmn:extensionElements>
                <zeebe:calledElement processId="child"
                    propagateAllChildVariables="false"
                    propagateAllParentVariables="false" />
                <zeebe:ioMapping>
                  <zeebe:input source="=askedAbout" target="customerRequest" />
                  <zeebe:output source="=summary" target="summaryCopy" />
                  <zeebe:output source="={status: status, summary: summaryCopy}" target="toolCallResult" />
                </zeebe:ioMapping>
              </bpmn:extensionElements>
              <bpmn:outgoing>chain</bpmn:outgoing>
            </bpmn:callActivity>
            <bpmn:sequenceFlow id="chain" sourceRef="CallSpecialist" targetRef="toolB" />
            <bpmn:serviceTask id="toolB">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="toolB-type" />
              </bpmn:extensionElements>
              <bpmn:incoming>chain</bpmn:incoming>
            </bpmn:serviceTask>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
        <bpmn:process id="child">
          <bpmn:startEvent id="cs" />
          <bpmn:serviceTask id="specialist">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="probe-child" />
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:endEvent id="ce" />
          <bpmn:sequenceFlow id="cf1" sourceRef="cs" targetRef="specialist" />
          <bpmn:sequenceFlow id="cf2" sourceRef="specialist" targetRef="ce" />
        </bpmn:process>
      </bpmn:definitions>"#;
    crate::bpmn::parse_bpmn(xml).unwrap()
}

/// A parent process whose ad-hoc agent has an UNBOUND `bpmn:callActivity` tool —
/// a `callActivity` with no `calledElement`/`zeebe:calledElement processId`
/// (issue #1159). The ad-hoc catalog deliberately supports this shape
/// (`process_id: None`, round-tripped as `UnboundCall` by `processos`); it names
/// no callee, so it must pass straight through to completion (its pre-#1159
/// behaviour), NOT be handed an empty callee that raises a spurious
/// `CalledElementError`.
fn adhoc_agent_with_unbound_call_activity_tool() -> ProcessDefinition {
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="parent">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="results" outputElement="=toolResult" />
            </bpmn:extensionElements>
            <bpmn:callActivity id="Unbound">
              <bpmn:extensionElements>
                <zeebe:ioMapping>
                  <zeebe:output source="=42" target="toolResult" />
                </zeebe:ioMapping>
              </bpmn:extensionElements>
            </bpmn:callActivity>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    crate::bpmn::parse_bpmn(xml).unwrap().remove(0)
}

/// A parent process whose ad-hoc agent has a `bpmn:callActivity` tool with BOTH
/// propagate flags at their Zeebe default (`true`, the attributes absent): the
/// whole parent scope crosses INTO the child, and the child's whole final scope
/// crosses BACK into the container (issue #1159). The tool still input-maps
/// `askedAbout -> customerRequest` and output-maps `{status, summary} ->
/// toolCallResult`.
fn adhoc_agent_with_propagating_call_activity_tool() -> Vec<ProcessDefinition> {
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="parent">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="toolCallResults" outputElement="=toolCallResult" />
            </bpmn:extensionElements>
            <bpmn:callActivity id="CallSpecialist">
              <bpmn:extensionElements>
                <zeebe:calledElement processId="child" />
                <zeebe:ioMapping>
                  <zeebe:input source="=askedAbout" target="customerRequest" />
                  <zeebe:output source="={status: status, summary: summary}" target="toolCallResult" />
                </zeebe:ioMapping>
              </bpmn:extensionElements>
            </bpmn:callActivity>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
        <bpmn:process id="child">
          <bpmn:startEvent id="cs" />
          <bpmn:serviceTask id="specialist">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="probe-child" />
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:endEvent id="ce" />
          <bpmn:sequenceFlow id="cf1" sourceRef="cs" targetRef="specialist" />
          <bpmn:sequenceFlow id="cf2" sourceRef="specialist" targetRef="ce" />
        </bpmn:process>
      </bpmn:definitions>"#;
    crate::bpmn::parse_bpmn(xml).unwrap()
}

/// Same ad-hoc `callActivity` tool as `adhoc_agent_with_call_activity_tool`, but
/// the container sits on ONE branch of a parallel fork whose other branch parks
/// on an open `hold` user task, so cancelling the container does NOT complete the
/// parent instance (the join still waits on `hold`). This isolates the #1159
/// cancellation leak: with the parent instance alive, the generic
/// `cascade_cancel_children` sweep (which only reaps children of a *terminated*
/// instance) never reaps the call-activity tool's child — so the tool-local
/// teardown must terminate it.
fn adhoc_agent_with_call_activity_tool_and_keepalive() -> Vec<ProcessDefinition> {
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="parent">
          <bpmn:startEvent id="s" />
          <bpmn:parallelGateway id="fork" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="toolCallResults" outputElement="=toolCallResult" />
            </bpmn:extensionElements>
            <bpmn:callActivity id="CallSpecialist">
              <bpmn:extensionElements>
                <zeebe:calledElement processId="child"
                    propagateAllChildVariables="false"
                    propagateAllParentVariables="false" />
                <zeebe:ioMapping>
                  <zeebe:input source="=askedAbout" target="customerRequest" />
                  <zeebe:output source="={status: status, summary: summary}" target="toolCallResult" />
                </zeebe:ioMapping>
              </bpmn:extensionElements>
            </bpmn:callActivity>
          </bpmn:adHocSubProcess>
          <bpmn:userTask id="hold" />
          <bpmn:parallelGateway id="join" />
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="fork" />
          <bpmn:sequenceFlow id="f1" sourceRef="fork" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="fork" targetRef="hold" />
          <bpmn:sequenceFlow id="f3" sourceRef="agent" targetRef="join" />
          <bpmn:sequenceFlow id="f4" sourceRef="hold" targetRef="join" />
          <bpmn:sequenceFlow id="f5" sourceRef="join" targetRef="e" />
        </bpmn:process>
        <bpmn:process id="child">
          <bpmn:startEvent id="cs" />
          <bpmn:serviceTask id="specialist">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="probe-child" />
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:endEvent id="ce" />
          <bpmn:sequenceFlow id="cf1" sourceRef="cs" targetRef="specialist" />
          <bpmn:sequenceFlow id="cf2" sourceRef="specialist" targetRef="ce" />
        </bpmn:process>
      </bpmn:definitions>"#;
    crate::bpmn::parse_bpmn(xml).unwrap()
}

/// A `callActivity` ad-hoc tool whose OUTPUT mapping cannot evaluate against the
/// child's real result (`=status + 1`, string + int): the tool must park an
/// `IO_MAPPING_ERROR` incident that PRESERVES the child's produced variables on
/// its redrive (issue #1159), so resolution re-enters the call-activity→ad-hoc
/// bridge with the gone child's result rather than completing the tool through
/// `complete_adhoc_tool` against the pre-child scope (which would manufacture a
/// wrong answer). A genuinely unfixable mapping therefore stays parked on resolve
/// instead of silently completing wrong.
fn adhoc_agent_with_failing_output_call_activity_tool() -> Vec<ProcessDefinition> {
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="parent">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="toolCallResults" outputElement="=toolCallResult" />
            </bpmn:extensionElements>
            <bpmn:callActivity id="CallSpecialist">
              <bpmn:extensionElements>
                <zeebe:calledElement processId="child"
                    propagateAllChildVariables="false"
                    propagateAllParentVariables="false" />
                <zeebe:ioMapping>
                  <zeebe:output source="=status + 1" target="toolCallResult" />
                </zeebe:ioMapping>
              </bpmn:extensionElements>
            </bpmn:callActivity>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
        <bpmn:process id="child">
          <bpmn:startEvent id="cs" />
          <bpmn:serviceTask id="specialist">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="probe-child" />
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:endEvent id="ce" />
          <bpmn:sequenceFlow id="cf1" sourceRef="cs" targetRef="specialist" />
          <bpmn:sequenceFlow id="cf2" sourceRef="specialist" targetRef="ce" />
        </bpmn:process>
      </bpmn:definitions>"#;
    crate::bpmn::parse_bpmn(xml).unwrap()
}

/// A parent whose ad-hoc `callActivity` tool names a callee that is NOT deployed
/// at activation time — its spawn parks a recoverable `CALLED_ELEMENT_ERROR`
/// incident (issue #1159). Deploying the callee and resolving the incident must
/// RE-ATTEMPT the spawn (`RetryCallActivitySpawn` → the ad-hoc respawn path) and
/// actually create the child, instead of completing the tool with a manufactured
/// all-null result.
fn adhoc_agent_tool_calls_undeployed() -> ProcessDefinition {
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="parent">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="toolCallResults" outputElement="=toolCallResult" />
            </bpmn:extensionElements>
            <bpmn:callActivity id="CallSpecialist">
              <bpmn:extensionElements>
                <zeebe:calledElement processId="specialist-proc"
                    propagateAllChildVariables="false"
                    propagateAllParentVariables="false" />
                <zeebe:ioMapping>
                  <zeebe:input source="=askedAbout" target="customerRequest" />
                  <zeebe:output source="={status: status, summary: summary}" target="toolCallResult" />
                </zeebe:ioMapping>
              </bpmn:extensionElements>
            </bpmn:callActivity>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    crate::bpmn::parse_bpmn(xml).unwrap().pop().unwrap()
}

fn specialist_proc() -> ProcessDefinition {
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="specialist-proc">
          <bpmn:startEvent id="cs" />
          <bpmn:serviceTask id="specialist">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="probe-child" />
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:endEvent id="ce" />
          <bpmn:sequenceFlow id="cf1" sourceRef="cs" targetRef="specialist" />
          <bpmn:sequenceFlow id="cf2" sourceRef="specialist" targetRef="ce" />
        </bpmn:process>
      </bpmn:definitions>"#;
    crate::bpmn::parse_bpmn(xml).unwrap().pop().unwrap()
}

/// An ad-hoc `callActivity` tool with `propagateAllParentVariables="true"` whose
/// callee is undeployed at activation, so its first spawn parks a recoverable
/// `CALLED_ELEMENT_ERROR` incident. It carries an input mapping
/// (`=askedAbout -> customerRequest`).
fn adhoc_agent_tool_calls_undeployed_propagate_all() -> ProcessDefinition {
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="parent">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="toolCallResults" outputElement="=toolCallResult" />
            </bpmn:extensionElements>
            <bpmn:callActivity id="CallSpecialist">
              <bpmn:extensionElements>
                <zeebe:calledElement processId="specialist-proc"
                    propagateAllChildVariables="false"
                    propagateAllParentVariables="true" />
                <zeebe:ioMapping>
                  <zeebe:input source="=askedAbout" target="customerRequest" />
                  <zeebe:output source="={status: status, summary: summary}" target="toolCallResult" />
                </zeebe:ioMapping>
              </bpmn:extensionElements>
            </bpmn:callActivity>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    crate::bpmn::parse_bpmn(xml).unwrap().pop().unwrap()
}

/// A `callActivity` ad-hoc tool with **chained input mappings**
/// (`=x -> y`, `=y -> z`) and `propagateAllParentVariables=false`. On the first
/// pass the tool's inputs are folded into the tool child's local scope, so a
/// respawn that re-projects them against that already-mutated view would resolve
/// `z` non-null (`eval_io_mappings_in` is single-pass) — silently altering the
/// child seed across a recoverable spawn-incident retry (#1176). The post-resolve
/// respawn must produce the **same** child seed as a clean first spawn.
fn adhoc_agent_tool_chained_inputs(callee: &str) -> ProcessDefinition {
    let xml = format!(
        r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="parent">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="toolCallResults" outputElement="=toolCallResult" />
            </bpmn:extensionElements>
            <bpmn:callActivity id="CallChain">
              <bpmn:extensionElements>
                <zeebe:calledElement processId="{callee}"
                    propagateAllChildVariables="false"
                    propagateAllParentVariables="false" />
                <zeebe:ioMapping>
                  <zeebe:input source="=x" target="y" />
                  <zeebe:input source="=y" target="z" />
                </zeebe:ioMapping>
              </bpmn:extensionElements>
            </bpmn:callActivity>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#
    );
    crate::bpmn::parse_bpmn(&xml).unwrap().pop().unwrap()
}

/// Drives the chained-input ad-hoc call-activity tool to the point where its
/// child process instance is spawned, returning the child's seed (the spawned
/// `probe-child` job's variables). When `deploy_callee_first` is false the first
/// spawn parks a recoverable `CALLED_ELEMENT_ERROR` incident; the callee is then
/// deployed and the incident resolved, so the returned seed is the *respawn*
/// seed.
fn chained_input_child_seed(deploy_callee_first: bool) -> HashMap<String, Value> {
    let mut engine = Engine::new();
    if deploy_callee_first {
        engine
            .apply_command(Command::DeployProcess(specialist_proc()))
            .unwrap();
    }
    engine
        .apply_command(Command::DeployProcess(adhoc_agent_tool_chained_inputs(
            "specialist-proc",
        )))
        .unwrap();
    engine
        .apply_command(Command::create_instance_with(
            "parent",
            vars(&[("x", Value::Str("seed".into()))]),
        ))
        .unwrap();
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job");
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("CallChain")],
                ..Default::default()
            },
        ))
        .unwrap();

    if !deploy_callee_first {
        // The first spawn parked a recoverable incident; deploy the callee and
        // resolve so the tool respawns.
        let active = engine.active_incidents();
        assert_eq!(active.len(), 1, "the unknown callee parks one incident");
        assert_eq!(active[0].kind, state::IncidentKind::CalledElementError);
        engine
            .apply_command(Command::DeployProcess(specialist_proc()))
            .unwrap();
        let incident_key = engine.incidents()[0].key;
        engine
            .apply_command(Command::resolve_incident(incident_key))
            .unwrap();
        assert!(
            engine.active_incidents().is_empty(),
            "resolving the incident cleared it by spawning the child"
        );
    }

    engine
        .activate_jobs("probe-child", "W", 10, 1_000, 0)
        .into_iter()
        .next()
        .expect("child spawned")
        .variables
        .as_ref()
        .clone()
}

/// A `callActivity` ad-hoc tool with **chained output mappings**
/// (`=status -> intermediate`, `=intermediate -> toolCallResult`) whose
/// completion parks the output-collection *type* incident (the container
/// `outputCollection` was overwritten with a scalar). Resolving it must reuse the
/// tool's single-pass output projection — not re-evaluate the chained output
/// mappings against the seeded child scope, which would make
/// `toolCallResult = <intermediate>` non-null (#1176).
fn adhoc_agent_tool_chained_outputs(overwrite_collection: bool) -> ProcessDefinition {
    let container_input = if overwrite_collection {
        r#"<zeebe:ioMapping><zeebe:input source="=5" target="results" /></zeebe:ioMapping>"#
    } else {
        ""
    };
    let xml = format!(
        r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="parent">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="results" outputElement="=toolCallResult" />
              {container_input}
            </bpmn:extensionElements>
            <bpmn:callActivity id="CallChain">
              <bpmn:extensionElements>
                <zeebe:calledElement processId="specialist2"
                    propagateAllChildVariables="false"
                    propagateAllParentVariables="false" />
                <zeebe:ioMapping>
                  <zeebe:output source="=status" target="intermediate" />
                  <zeebe:output source="=intermediate" target="toolCallResult" />
                </zeebe:ioMapping>
              </bpmn:extensionElements>
            </bpmn:callActivity>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#
    );
    crate::bpmn::parse_bpmn(&xml).unwrap().pop().unwrap()
}

fn specialist2_proc() -> ProcessDefinition {
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="specialist2">
          <bpmn:startEvent id="cs" />
          <bpmn:serviceTask id="specialist">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="probe-child2" />
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:endEvent id="ce" />
          <bpmn:sequenceFlow id="cf1" sourceRef="cs" targetRef="specialist" />
          <bpmn:sequenceFlow id="cf2" sourceRef="specialist" targetRef="ce" />
        </bpmn:process>
      </bpmn:definitions>"#;
    crate::bpmn::parse_bpmn(xml).unwrap().pop().unwrap()
}

/// Runs the chained-output ad-hoc call-activity tool to completion of the tool,
/// returning the tool's output projection as written into the container scope
/// (`intermediate` and `toolCallResult`). With `overwrite_collection` the tool's
/// completion parks the output-collection type incident, which is then fixed
/// (`results = []`) and resolved, so the returned projection is the *redrive*
/// projection.
fn chained_output_container_projection(
    overwrite_collection: bool,
) -> (Option<Value>, Option<Value>) {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(specialist2_proc()))
        .unwrap();
    engine
        .apply_command(Command::DeployProcess(adhoc_agent_tool_chained_outputs(
            overwrite_collection,
        )))
        .unwrap();
    let inst = create_instance_key(&mut engine, "parent");
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job");
    let container = agent.element_instance_key;
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("CallChain")],
                ..Default::default()
            },
        ))
        .unwrap();
    // The tool spawned a child; complete its service task producing `status`.
    let child = engine
        .activate_jobs("probe-child2", "W", 10, 1_000, 0)
        .into_iter()
        .next()
        .expect("child job");
    engine
        .apply_command(Command::complete_job_with(
            child.key,
            vars(&[("status", Value::Str("ok".into()))]),
        ))
        .unwrap();

    if overwrite_collection {
        // The tool's completion parked the output-collection type incident.
        let active = engine.active_incidents();
        assert_eq!(
            active.len(),
            1,
            "the scalar outputCollection parks one incident"
        );
        assert_eq!(active[0].kind, state::IncidentKind::ExpressionEvaluation);
        assert_eq!(active[0].element_id, "CallChain");
        // Fix the outputCollection back to a list and resolve.
        engine
            .apply_command(Command::set_variables_scoped(
                container,
                vars(&[("results", Value::List(Vec::new()))]),
                true,
            ))
            .unwrap();
        let incident_key = engine.incidents()[0].key;
        engine
            .apply_command(Command::resolve_incident(incident_key))
            .unwrap();
        assert!(
            engine.active_incidents().is_empty(),
            "resolving the type incident cleared it by completing the tool",
        );
    }

    (
        engine.variables(inst).get("intermediate").cloned(),
        engine.variables(inst).get("toolCallResult").cloned(),
    )
}
