//! # nanobpmn-engine-core
//!
//! A minimal, embeddable BPMN execution core.
//!
//! The architecture is deliberately modelled on **Camunda 8 (Zeebe)** rather than
//! Camunda 7's Process Virtual Machine, but with all of the distributed-systems
//! machinery removed (no Raft, no partitions, no exporters, no gRPC, no RocksDB).
//! What remains is the part that makes the engine correct and testable:
//!
//! * **Single writer.** All state changes flow through one sequential loop
//!   ([`Engine::apply_command`]). There are no threads, no locks and no
//!   optimistic-locking retries — an entire class of concurrency bugs simply
//!   cannot occur.
//! * **command → event → applier.** A [`Command`] is *decided* by a pure
//!   processor that reads state and produces [`Event`]s plus follow-up work. The
//!   [`state::apply`] function is the **only** code that mutates state. This makes
//!   the engine deterministic: feed the same commands, get the same events.
//! * **Event-sourced & replayable.** Events carry enough information to rebuild
//!   state, so persistence is a caller concern — append the event log anywhere
//!   (in-memory, WAL, `redb`, SQLite) and replay to recover.
//! * **Zero dependencies, `std`-only.** The crate compiles unchanged for servers,
//!   for iOS/Android (embed via FFI, e.g. UniFFI) and for `wasm32`.
//!
//! ## The element lifecycle
//!
//! Every BPMN element instance walks the same state machine, mirroring Zeebe:
//!
//! ```text
//! ACTIVATING -> ACTIVATED -> COMPLETING -> COMPLETED --(take outgoing flow)--> ACTIVATING(next)
//! ```
//!
//! Pass-through elements (start/end events) traverse it in one burst. A
//! [`ElementKind::ServiceTask`] rests in `ACTIVATED` after creating a job and only
//! advances to `COMPLETING` when a [`Command::CompleteJob`] arrives — this is how
//! asynchronous work is modelled without any background thread.
//!
//! ## Example
//!
//! ```
//! use nanobpmn_engine_core::{Command, Engine, ProcessBuilder};
//!
//! let mut engine = Engine::new();
//!
//! let process = ProcessBuilder::new("order")
//!     .start_event("start")
//!     .service_task("charge", "payment")
//!     .end_event("end")
//!     .connect("start", "charge")
//!     .connect("charge", "end")
//!     .build()
//!     .unwrap();
//!
//! engine.apply_command(Command::DeployProcess(process)).unwrap();
//! let events = engine
//!     .apply_command(Command::create_instance("order"))
//!     .unwrap();
//!
//! // The instance is parked on the service task, waiting for its job.
//! let instance_key = events
//!     .iter()
//!     .find_map(|e| e.instance_key())
//!     .unwrap();
//! assert!(!engine.is_completed(instance_key));
//!
//! // A worker activates the job (locking it for 30s of logical time), does the
//! // work, and completes it by key; the token resumes and the instance finishes.
//! let jobs = engine.activate_jobs("payment", "worker-1", 10, 30_000, 0);
//! engine.apply_command(Command::complete_job(jobs[0].key)).unwrap();
//! assert!(engine.is_completed(instance_key));
//! ```

mod agent;
mod cluster_vars;
mod command;
mod engine;
mod event;
mod json;
mod model;
mod read_query;
mod state;
mod temporal;

pub mod bpmn;
pub mod dmn;
pub mod feel;

pub mod xml;

mod validate;

#[cfg(feature = "ffi")]
pub mod ffi;

/// The `engine-core` crate version, used as a coarse build-identity fingerprint
/// (e.g. stamped into the persisted snapshot envelope header). Distinct from
/// [`SNAPSHOT_FORMAT_VERSION`], which versions the serialized payload shape.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub use agent::{
    AgentDefinition, AgentHistoryCommitStatus, AgentHistoryContent, AgentHistoryContentType,
    AgentHistoryIntent, AgentHistoryMetrics, AgentHistoryRecord, AgentHistoryRole,
    AgentHistoryToolCall, AgentHistoryTurn, AgentInstance, AgentInstanceIntent,
    AgentInstanceLimits, AgentInstanceMetrics, AgentInstanceMetricsDelta, AgentInstanceStatus,
    AgentLimitKind, AgentTool, AgentType, AGENT_LIMIT_UNLIMITED,
};
pub use cluster_vars::{ClusterVariableSnapshot, ClusterVariables, DEFAULT_TENANT};
#[cfg(feature = "serde")]
pub use command::form_id_of;
pub use command::JobActivationOptions;
pub use command::{
    ActivateElementInstruction, Command, DecisionReference, FormResource, GenericResource,
    UserTaskChangeset,
};
#[cfg(feature = "serde")]
pub use engine::EngineSnapshot;
pub use engine::SNAPSHOT_FORMAT_VERSION;
mod lease;
pub use engine::{adhoc_inner_instance_id, ADHOC_INNER_INSTANCE_ID_POSTFIX};
pub use engine::{check_business_id, ActivatedJob, Engine, EngineError, BUSINESS_ID_MAX_CHARS};
pub use engine::{BreakCondition, DebugSession};
#[cfg(feature = "serde")]
pub use event::decode_event_json;
pub use event::Event;
pub use event::EventDecodeError;
pub use feel::FeelError;
pub use model::{
    AdHocActivateElement, AdHocImplementationType, AdHocJobResult, AdHocSubProcessDef, AdHocTool,
    AdHocToolKind, BindingType, BuildError, Condition, Element, ElementId, ElementKind,
    ExecutionListener, IncomingFlow, IoMapping, LinkedResource, ListenerEventType, Mapping,
    MultiInstance, ProcessBuilder, ProcessDefinition, SequenceFlow, TaskListener,
    TaskListenerEventType, TaskListenerJobResult, TimerDef, TimerDefKind, UserTaskCorrections,
    UserTaskProps, Value,
};
pub use read_query::ReadQuery;
pub use state::{
    compose_key, local_of, partition_of, stable_hash, subscription_partition,
    ConditionalSubscription, DeployedProcess, Incident, IncidentKind, IncidentState,
    InstanceSnapshot, IoMappingRedrive, Job, JobKind, JobState, Key, MessageStartSubscription,
    MessageSubscription, MessageSubscriptionKind, MessageSubscriptionState, ProcessInstance,
    ProcessInstanceState, SignalSubscription, StartTimer, State, Timer, TimerKind, TimerState,
    UserTask, UserTaskState, DEFAULT_JOB_PRIORITY, DEFAULT_JOB_RETRIES, LOCAL_BITS, LOCAL_MASK,
    MAX_PARTITION_ID, PARTITION_BITS,
};
