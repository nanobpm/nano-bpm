//! Small, solver-friendly stand-ins for the state kernel's join bookkeeping
//! ([`FlowArrivals`]) and job index maintainer ([`types::resync_job_index`]),
//! used by the Kani harnesses (#1994, `kani_proofs.rs`).
//!
//! The production types are heap-backed (`Vec<(IncomingFlow, usize)>`,
//! `BTreeMap`/`BTreeSet`), which CBMC cannot fit in a CI runner. The stand-ins
//! keep the same algorithms over fixed-size arrays with a bounded length. The
//! `refinement` tests below run the stand-in and the production type through
//! every operation sequence up to a bound and assert they agree, so a proof
//! over a stand-in is a proof about the production behaviour on that domain.
//! The job index stand-in shares the membership decision itself with
//! production ([`types::job_index_membership`]); only the index storage is
//! modelled.

use super::types::{self, JobState};
use super::Key;

/// [`FlowArrivals`](super::FlowArrivals) over at most `N` flows, each flow
/// identified by a `u8` whose order mirrors `IncomingFlow`'s order.
#[derive(Clone, Copy, Debug)]
pub(crate) struct BoundedArrivals<const N: usize> {
    entries: [(u8, usize); N],
    len: usize,
}

impl<const N: usize> Default for BoundedArrivals<N> {
    fn default() -> Self {
        Self {
            entries: [(0, 0); N],
            len: 0,
        }
    }
}

impl<const N: usize> BoundedArrivals<N> {
    /// `FlowArrivals::record`.
    pub(crate) fn record(&mut self, flow: u8) {
        self.add(flow, 1);
    }

    /// `FlowArrivals::add`: sorted insert (the binary search's insertion
    /// point), or bump the existing count. Panics past capacity `N`.
    pub(crate) fn add(&mut self, flow: u8, tokens: usize) {
        if tokens == 0 {
            return;
        }
        let mut i = 0;
        while i < self.len && self.entries[i].0 < flow {
            i += 1;
        }
        if i < self.len && self.entries[i].0 == flow {
            self.entries[i].1 += tokens;
            return;
        }
        assert!(self.len < N, "BoundedArrivals capacity exceeded");
        let mut j = self.len;
        while j > i {
            self.entries[j] = self.entries[j - 1];
            j -= 1;
        }
        self.entries[i] = (flow, tokens);
        self.len += 1;
    }

    /// `FlowArrivals::distinct_flows`.
    pub(crate) fn distinct_flows(&self) -> usize {
        self.len
    }

    /// `FlowArrivals::count`.
    pub(crate) fn count(&self, flow: u8) -> usize {
        self.as_slice()
            .iter()
            .find(|(f, _)| *f == flow)
            .map_or(0, |(_, c)| *c)
    }

    /// `FlowArrivals::consume_one_each`: decrement every count, then drop the
    /// zeros (`retain`, order-preserving).
    pub(crate) fn consume_one_each(&mut self) {
        let mut kept = 0;
        for i in 0..self.len {
            let (flow, count) = self.entries[i];
            let count = count - 1;
            if count > 0 {
                self.entries[kept] = (flow, count);
                kept += 1;
            }
        }
        self.len = kept;
    }

    /// `FlowArrivals::is_empty`.
    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// `FlowArrivals::iter`, as a slice.
    pub(crate) fn as_slice(&self) -> &[(u8, usize)] {
        &self.entries[..self.len]
    }
}

/// A sorted set of at most `N` elements (stand-in for a `BTreeSet`).
#[derive(Clone, Copy, Debug)]
pub(crate) struct BoundedSet<T: Copy + Ord + Default, const N: usize> {
    items: [T; N],
    len: usize,
}

impl<T: Copy + Ord + Default, const N: usize> Default for BoundedSet<T, N> {
    fn default() -> Self {
        Self {
            items: [T::default(); N],
            len: 0,
        }
    }
}

impl<T: Copy + Ord + Default, const N: usize> BoundedSet<T, N> {
    pub(crate) fn insert(&mut self, x: T) {
        if self.contains(&x) {
            return;
        }
        assert!(self.len < N, "BoundedSet capacity exceeded");
        let mut j = self.len;
        while j > 0 && self.items[j - 1] > x {
            self.items[j] = self.items[j - 1];
            j -= 1;
        }
        self.items[j] = x;
        self.len += 1;
    }

    pub(crate) fn remove(&mut self, x: &T) {
        if let Some(i) = self.as_slice().iter().position(|y| y == x) {
            for j in i..self.len - 1 {
                self.items[j] = self.items[j + 1];
            }
            self.len -= 1;
        }
    }

    pub(crate) fn contains(&self, x: &T) -> bool {
        self.as_slice().contains(x)
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn as_slice(&self) -> &[T] {
        &self.items[..self.len]
    }
}

/// The derived job indexes of a [`State`](super::State) holding one job of one
/// job type, with the production `BTreeMap`/`BTreeSet`s replaced by bounded
/// stand-ins.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct JobIndexes {
    /// `activatable_jobs[job_type]` (an absent entry is the empty set).
    pub activatable: BoundedSet<(i64, Key), 1>,
    /// `deferred_jobs`.
    pub deferred_jobs: BoundedSet<(u64, Key), 2>,
    /// `deferred_job_deadlines` entry for the job.
    pub deferred_deadline: Option<u64>,
    /// `activated_jobs`.
    pub activated: BoundedSet<Key, 1>,
}

impl JobIndexes {
    /// [`types::resync_job_index`] for job `key` at `priority`, in its new
    /// `job_state` with `retry_backoff_until`, step for step.
    pub(crate) fn resync(
        &mut self,
        key: Key,
        priority: i32,
        job_state: JobState,
        retry_backoff_until: Option<u64>,
    ) {
        let membership = types::job_index_membership(job_state, retry_backoff_until);
        let order = types::activation_order(priority, key);
        if membership.activatable {
            self.activatable.insert(order);
        } else {
            self.activatable.remove(&order);
        }
        if let Some(until) = membership.deferred_until {
            if let Some(old) = self.deferred_deadline.replace(until) {
                self.deferred_jobs.remove(&(old, key));
            }
            self.deferred_jobs.insert((until, key));
        } else if let Some(until) = self.deferred_deadline.take() {
            self.deferred_jobs.remove(&(until, key));
        }
        if membership.activated {
            self.activated.insert(key);
        } else {
            self.activated.remove(&key);
        }
    }
}

/// Refinement: the stand-ins agree with the production types on every
/// operation sequence up to a bound (exhaustively enumerated, so stronger than
/// sampling on this domain).
#[cfg(test)]
mod refinement {
    use super::*;
    use crate::model::IncomingFlow;
    use crate::state::types::{resync_job_index, Job, JobKind};
    use crate::state::{FlowArrivals, State};

    /// Flow `id` as a production identity. Ids `0..` map to strictly
    /// increasing `IncomingFlow`s (same source, ascending ordinal, plus a
    /// second source), so the stand-in's `u8` order mirrors production order.
    fn flow(id: u8) -> IncomingFlow {
        IncomingFlow {
            from: if id < 2 { "a" } else { "b" }.to_string(),
            ordinal: usize::from(id % 2),
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum ArrivalOp {
        Record(u8),
        Add(u8, usize),
        Consume,
    }

    const FLOWS: u8 = 3;

    fn arrival_ops() -> Vec<ArrivalOp> {
        let mut ops = vec![ArrivalOp::Consume];
        for f in 0..FLOWS {
            ops.push(ArrivalOp::Record(f));
            for t in 0..3 {
                ops.push(ArrivalOp::Add(f, t));
            }
        }
        ops
    }

    fn assert_arrivals_agree(model: &BoundedArrivals<3>, real: &FlowArrivals) {
        let real_entries: Vec<(IncomingFlow, usize)> =
            real.iter().map(|(f, c)| (f.clone(), c)).collect();
        let model_entries: Vec<(IncomingFlow, usize)> = model
            .as_slice()
            .iter()
            .map(|(f, c)| (flow(*f), *c))
            .collect();
        assert_eq!(model_entries, real_entries);
        assert_eq!(model.distinct_flows(), real.distinct_flows());
        assert_eq!(model.is_empty(), real.is_empty());
        for f in 0..FLOWS {
            assert_eq!(model.count(f), real.count(&flow(f)));
        }
    }

    fn walk_arrivals(model: BoundedArrivals<3>, real: FlowArrivals, depth: usize) {
        assert_arrivals_agree(&model, &real);
        if depth == 0 {
            return;
        }
        for op in arrival_ops() {
            let (mut m, mut r) = (model, real.clone());
            match op {
                ArrivalOp::Record(f) => {
                    m.record(f);
                    r.record(&flow(f));
                }
                ArrivalOp::Add(f, t) => {
                    m.add(f, t);
                    r.add(&flow(f), t);
                }
                ArrivalOp::Consume => {
                    m.consume_one_each();
                    r.consume_one_each();
                }
            }
            walk_arrivals(m, r, depth - 1);
        }
    }

    #[test]
    fn flow_order_mirrors_production_order() {
        for a in 0..FLOWS {
            for b in 0..FLOWS {
                assert_eq!(a.cmp(&b), flow(a).cmp(&flow(b)));
            }
        }
    }

    #[test]
    fn bounded_arrivals_refines_flow_arrivals() {
        walk_arrivals(BoundedArrivals::default(), FlowArrivals::default(), 4);
    }

    const KEY: Key = 7;

    const STATES: [JobState; 6] = [
        JobState::Created,
        JobState::Activated,
        JobState::Failed,
        JobState::Errored,
        JobState::Canceled,
        JobState::Completed,
    ];

    fn state_with_job(priority: i32) -> State {
        let mut state = State::default();
        state.jobs.insert(
            KEY,
            Job {
                key: KEY,
                instance_key: 1,
                element_instance_key: 2,
                element_id: "A".to_string(),
                job_type: "t".to_string(),
                state: JobState::Created,
                worker: None,
                deadline: None,
                activated_at: None,
                activation_timeout: None,
                lease_token: None,
                durable_activation: false,
                activated: false,
                retries: 3,
                priority,
                created_at: 0,
                kind: JobKind::BpmnElement,
                error_message: None,
                error_code: None,
                has_failed_with_retries_left: false,
                retry_backoff_until: None,
                settled_tokens: Vec::new(),
                terminal_end: None,
                secret_provenance: Vec::new(),
            },
        );
        state
    }

    fn assert_indexes_agree(model: &JobIndexes, real: &State) {
        let activatable: Vec<(i64, Key)> = real
            .activatable_jobs
            .get("t")
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default();
        assert!(real.activatable_jobs.values().all(|s| !s.is_empty()));
        assert_eq!(
            real.activatable_jobs.len(),
            usize::from(!activatable.is_empty())
        );
        assert_eq!(model.activatable.as_slice(), activatable.as_slice());
        let deferred: Vec<(u64, Key)> = real.deferred_jobs.iter().copied().collect();
        assert_eq!(model.deferred_jobs.as_slice(), deferred.as_slice());
        assert_eq!(model.deferred_jobs.len(), deferred.len());
        assert_eq!(
            model.deferred_deadline,
            real.deferred_job_deadlines.get(&KEY).copied()
        );
        assert_eq!(
            real.deferred_job_deadlines.len(),
            usize::from(model.deferred_deadline.is_some())
        );
        let activated: Vec<Key> = real.activated_jobs.iter().copied().collect();
        assert_eq!(model.activated.as_slice(), activated.as_slice());
    }

    fn walk_jobs(priority: i32, model: JobIndexes, real: &State, depth: usize) {
        assert_indexes_agree(&model, real);
        if depth == 0 {
            return;
        }
        for job_state in STATES {
            for backoff in [None, Some(5), Some(9)] {
                let (mut m, mut r) = (model, real.clone());
                if let Some(job) = r.jobs.get_mut(&KEY) {
                    job.state = job_state;
                    job.retry_backoff_until = backoff;
                }
                resync_job_index(&mut r, KEY);
                m.resync(KEY, priority, job_state, backoff);
                walk_jobs(priority, m, &r, depth - 1);
            }
        }
    }

    #[test]
    fn job_indexes_refine_resync_job_index() {
        for priority in [i32::MIN, -1, 0, i32::MAX] {
            let mut real = state_with_job(priority);
            resync_job_index(&mut real, KEY);
            let mut model = JobIndexes::default();
            model.resync(KEY, priority, JobState::Created, None);
            walk_jobs(priority, model, &real, 3);
        }
    }
}
