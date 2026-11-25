//! `searchProcessInstances`: the single spec-derived implementation both the
//! gateway and `engine-wasm` call, plus the process-instance filter matcher the
//! gateway's process-instance batch operations share with it.

use nanobpmn_engine_core::ProcessInstanceState;
use nanobpmn_read_model as readstore;

use crate::{convert::epoch, models, query, source::ReadSource, types};

/// `POST /v2/process-instances/search`: filter (every top-level field and the
/// `$or` clauses), sort and paginate process instances.
pub fn search_process_instances<S: ReadSource + ?Sized>(
    store: &S,
    body: Option<&models::ProcessInstanceSearchQuery>,
) -> models::ProcessInstanceSearchQueryResult {
    let filter = body.and_then(|q| q.filter.as_ref());
    let instances = store.process_instances();

    // One shared matcher (the same algebra the batch-selection path uses),
    // honouring every top-level field AND the `$or` clause list (#1293).
    let matcher = filter.map(|f| ProcessInstanceMatcher::new(f, store));
    let mut matched: Vec<&readstore::ProcessInstanceRow> = instances
        .iter()
        .filter(|inst| {
            matcher.as_ref().is_none_or(|m| {
                m.matches(
                    inst,
                    ProcessInstanceStatePolicy::HonourFilter,
                    BatchHierarchyPolicy::HonourFilters,
                )
            })
        })
        .collect();

    let sort = query::sort_keys(
        body.and_then(|q| q.sort.as_ref()),
        |r: &models::ProcessInstanceSearchQuerySortRequest| (r.field.clone(), r.order),
    );
    query::sort_items(
        &mut matched,
        &sort,
        |inst, field| process_instance_sort_val(inst, field),
        |inst| inst.key,
    );

    let sorted: Vec<(u64, &readstore::ProcessInstanceRow)> =
        matched.into_iter().map(|inst| (inst.key, inst)).collect();
    let page = query::paginate(sorted, body.and_then(|q| q.page.as_ref()));
    // Build result models only for the returned page, never the whole
    // (potentially very large) matched set. One memoised root resolver spans
    // the page so instances sharing a parent chain walk it once.
    let roots = readstore::RootResolver::new(|k| store.process_instance(k));
    let items: Vec<models::ProcessInstanceResult> = page
        .items
        .into_iter()
        .map(|inst| process_instance_result(inst, &roots))
        .collect();
    models::ProcessInstanceSearchQueryResult::new(page.response, items)
}

/// The `searchProcessInstances` sort projection: every spec sort field.
pub fn process_instance_sort_val(
    inst: &readstore::ProcessInstanceRow,
    field: &str,
) -> query::SortVal {
    match field {
        "processDefinitionId" => query::SortVal::Str(inst.process_definition_id.clone()),
        "processDefinitionKey" => query::key_sort_val(&inst.process_definition_key),
        "state" => query::SortVal::Str(process_instance_state_enum(inst.state).to_string()),
        "businessId" => query::opt_str_sort_val(inst.business_id.as_deref()),
        "startDate" => query::SortVal::Num(inst.start_date_ms as i128),
        "suspendedDate" => query::opt_num_sort_val(inst.suspended_date_ms.map(|ms| ms as i128)),
        "hasIncident" => query::SortVal::Num(i128::from(inst.has_incident)),
        "parentProcessInstanceKey" => {
            query::opt_num_sort_val(inst.parent_process_instance_key.map(|k| k as i128))
        }
        "parentElementInstanceKey" => {
            query::opt_num_sort_val(inst.parent_element_instance_key.map(|k| k as i128))
        }
        "processDefinitionVersion" => query::SortVal::Num(i128::from(inst.version)),
        // Constant in Nano: the result always projects these as null
        // (`process_instance_result`), so every row ties and the
        // entity-key tiebreak orders them.
        "endDate" | "processDefinitionName" | "processDefinitionVersionTag" => {
            query::SortVal::Num(0)
        }
        "processInstanceKey" => query::SortVal::Num(inst.key as i128),
        "tenantId" => query::SortVal::Str("<default>".to_string()),
        _ => query::SortVal::Num(inst.key as i128),
    }
}

/// How a process-instance batch matcher treats the call-activity hierarchy
/// (issue #977). Batch operations differ by contract, so the policy is passed
/// explicitly rather than inferred:
///
/// * [`HonourFilters`](BatchHierarchyPolicy::HonourFilters) — evaluate the
///   caller's `parentProcessInstanceKey` / `parentElementInstanceKey` predicates
///   verbatim (incident-resolution and the `search` surface).
/// * [`RootOnly`](BatchHierarchyPolicy::RootOnly) — ignore the caller's parent
///   filters **and** target only root instances
///   (`parent_process_instance_key.is_none()`). Cancellation: only ACTIVE root
///   instances are cancellable (`spec/process-instances.yaml:371-375`), so a
///   child-scoped request must neither scope to the child nor enqueue active
///   call-activity children.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum BatchHierarchyPolicy {
    HonourFilters,
    RootOnly,
}

/// Per-instance cross-entity index backing the inherited element / job /
/// incident filters on a process-instance query (`elementId`,
/// `elementInstanceState`, `hasElementInstanceIncident`, `errorMessage`,
/// `hasRetriesLeft`). These are **existential** predicates over an instance's
/// child rows (element instances, jobs, incidents), not columns on
/// [`readstore::ProcessInstanceRow`], so they are pre-indexed by
/// `instance_key` once per query — only when at least one such filter is
/// actually present, so the common path pays nothing for the extra scans.
///
/// Built lazily via [`InstanceCrossEntityIndex::new`]; each child-row source
/// (element instances, jobs, incidents) is scanned only when a filter that
/// reads it is supplied — at the top level or in any `$or` clause — mirroring
/// the `variables_index` construction in `search_process_instances_impl`.
#[derive(Default)]
pub struct InstanceCrossEntityIndex {
    /// `instance_key → its element instances` (for the element predicates).
    /// The whole row is kept per element — not a per-field map — so the
    /// `elementId` / `elementInstanceState` / `hasElementInstanceIncident`
    /// predicates stay correlated: they must hold of the **same** element
    /// instance, not of three independent rows (a COMPLETED `review` plus an
    /// ACTIVE `approve` must not satisfy `elementId: "review",
    /// elementInstanceState: "ACTIVE"`).
    elements: std::collections::HashMap<u64, Vec<IndexedElement>>,
    /// `instance_key → worker/incident error messages` (for `errorMessage`):
    /// the last `errorMessage` of each failed/errored job plus each incident's
    /// reason — the two places a process-level error message is projected.
    error_messages: std::collections::HashMap<u64, Vec<String>>,
    /// `instance_key → true when any job has failed with retries left` (for
    /// `hasRetriesLeft`).
    has_retries_left: std::collections::HashMap<u64, bool>,
}

/// One element instance as carried by [`InstanceCrossEntityIndex::elements`].
pub struct IndexedElement {
    element_id: String,
    /// The element-instance state wire spelling (`ACTIVE` / `COMPLETED` /
    /// `TERMINATED`).
    state: String,
    has_incident: bool,
}

/// The child-row sources an [`InstanceCrossEntityIndex`] is built from. Each
/// field is loaded by the caller **only when a supplied filter reads it** (see
/// [`InstanceCrossEntityIndex::needs_elements`] / `needs_jobs` /
/// `needs_incidents`), so an `elementId`-only query never materializes the job
/// or incident tables. `None` sources are treated as empty; construction
/// asserts the filter only reads sources that were loaded.
#[derive(Default)]
pub struct CrossEntitySources {
    element_instances: Option<Vec<readstore::ElementInstanceRow>>,
    jobs: Option<Vec<readstore::JobRow>>,
    incidents: Option<Vec<readstore::IncidentRow>>,
}

impl InstanceCrossEntityIndex {
    /// Builds the index, populating only the maps the supplied `filters`
    /// (the base filter plus every `$or` clause) actually constrain on and
    /// reading only the sources in `sources`.
    #[allow(
        clippy::expect_used,
        reason = "construction invariant: sources_for loads exactly the sources the supplied filters read, so a missing source here is a wiring bug between the two, not a runtime condition"
    )]
    pub fn new(filters: &[&models::ProcessInstanceFilter], sources: &CrossEntitySources) -> Self {
        let mut idx = InstanceCrossEntityIndex::default();
        if Self::needs_elements_any(filters) {
            let rows = sources
                .element_instances
                .as_deref()
                .expect("element predicates require the element-instance source");
            for e in rows {
                idx.elements
                    .entry(e.instance_key)
                    .or_default()
                    .push(IndexedElement {
                        element_id: e.element_id.clone(),
                        state: element_instance_state_enum(e.state).to_string(),
                        has_incident: e.has_incident,
                    });
            }
        }
        if filters.iter().any(|f| f.error_message.is_some()) {
            for j in sources
                .jobs
                .as_deref()
                .expect("errorMessage requires the job source")
            {
                if let Some(msg) = &j.error_message {
                    idx.error_messages
                        .entry(j.instance_key)
                        .or_default()
                        .push(msg.clone());
                }
            }
            for i in sources
                .incidents
                .as_deref()
                .expect("errorMessage requires the incident source")
            {
                idx.error_messages
                    .entry(i.instance_key)
                    .or_default()
                    .push(i.reason.clone());
            }
        }
        if filters.iter().any(|f| f.has_retries_left.is_some()) {
            for j in sources
                .jobs
                .as_deref()
                .expect("hasRetriesLeft requires the job source")
            {
                if j.has_failed_with_retries_left {
                    idx.has_retries_left.insert(j.instance_key, true);
                }
            }
        }
        idx
    }

    /// Whether any element predicate (`elementId` / `elementInstanceState` /
    /// `hasElementInstanceIncident`) is present, so the caller loads the
    /// element-instance source only then.
    fn needs_elements(filter: &models::ProcessInstanceFilter) -> bool {
        filter.element_id.is_some()
            || filter.element_instance_state.is_some()
            || filter.has_element_instance_incident.is_some()
    }

    /// [`Self::needs_elements`] over every group of one query (base + `$or`
    /// clauses).
    fn needs_elements_any(filters: &[&models::ProcessInstanceFilter]) -> bool {
        filters.iter().any(|f| Self::needs_elements(f))
    }

    /// Whether any job-reading predicate (`errorMessage` / `hasRetriesLeft`)
    /// is present.
    fn needs_jobs(filter: &models::ProcessInstanceFilter) -> bool {
        filter.error_message.is_some() || filter.has_retries_left.is_some()
    }

    /// [`Self::needs_jobs`] over every group of one query.
    fn needs_jobs_any(filters: &[&models::ProcessInstanceFilter]) -> bool {
        filters.iter().any(|f| Self::needs_jobs(f))
    }

    /// Whether any incident-reading predicate (`errorMessage`) is present.
    fn needs_incidents(filter: &models::ProcessInstanceFilter) -> bool {
        filter.error_message.is_some()
    }

    /// [`Self::needs_incidents`] over every group of one query.
    fn needs_incidents_any(filters: &[&models::ProcessInstanceFilter]) -> bool {
        filters.iter().any(|f| Self::needs_incidents(f))
    }

    /// Whether any cross-entity filter is present (so the caller can skip
    /// building the index entirely when none is).
    pub fn is_needed(filter: &models::ProcessInstanceFilter) -> bool {
        Self::needs_elements(filter) || Self::needs_jobs(filter) || Self::needs_incidents(filter)
    }

    /// Loads exactly the child-row sources the supplied `filters` (the base
    /// filter plus every `$or` clause) read: the element-instance table only
    /// when an element predicate is present, jobs only for `errorMessage` /
    /// `hasRetriesLeft`, incidents only for `errorMessage` — so an
    /// `elementId`-only query never scans the job or incident tables. Shared
    /// by the search and batch-selection paths so both honour the same
    /// lazy-source contract.
    pub fn sources_for<S: ReadSource + ?Sized>(
        filters: &[&models::ProcessInstanceFilter],
        store: &S,
    ) -> CrossEntitySources {
        CrossEntitySources {
            element_instances: Self::needs_elements_any(filters).then(|| store.element_instances()),
            jobs: Self::needs_jobs_any(filters).then(|| store.jobs()),
            incidents: Self::needs_incidents_any(filters).then(|| store.incidents()),
        }
    }
}

/// Evaluates the inherited element / job / incident / batch-operation filters
/// on a process-instance query against instance `instance_key`, using the
/// pre-indexed `index`. Each is an **existential** predicate over the
/// instance's child rows:
///
/// - `elementId` / `elementInstanceState` / `hasElementInstanceIncident` — the
///   instance has **one** element instance satisfying all supplied element
///   predicates at once (they are correlated: a COMPLETED `review` plus an
///   ACTIVE `approve` must not satisfy `elementId: "review",
///   elementInstanceState: "ACTIVE"`). `hasElementInstanceIncident: false`
///   inverts the whole existential: it selects instances with **no** element
///   satisfying the supplied id/state predicates that carries an incident.
/// - `errorMessage` — it has a failed/errored job or incident whose message
///   matches.
/// - `hasRetriesLeft` — it has a job that failed with retries left (`false`
///   selects instances with no such job).
///
/// `incidentErrorHashCode` and `batchOperationId` / `batchOperationKey` are
/// declared on the filter schema but Nano does not project a per-instance value
/// for them (no incident error-hash column; batch membership lives in the
/// separate `BatchOperationStore`, not the read model), so they are matched
/// against an **absent** value: equality / `$in` / `$exists: true` correctly
/// exclude (fail closed — the field is honoured, not silently broadening a
/// mutating batch) while `$exists: false` still matches. This mirrors the
/// `endDate` / `processDefinitionName` convention.
pub fn match_instance_cross_entity_filters(
    filter: &models::ProcessInstanceFilter,
    instance_key: u64,
    index: &InstanceCrossEntityIndex,
) -> bool {
    // The element predicates are evaluated together, existentially over the
    // instance's element rows, so an id/state/incident combination must hold of
    // the SAME element instance. An instance with no element instances matches
    // an `elementId` filter only against an absent value (so `$exists: false`
    // still matches) and never matches a state or `hasElementInstanceIncident:
    // true` predicate.
    let element_matches = {
        let has_element_predicates = filter.element_id.is_some()
            || filter.element_instance_state.is_some()
            || filter.has_element_instance_incident.is_some();
        if !has_element_predicates {
            true
        } else {
            let elements = index.elements.get(&instance_key);
            let row_matches = |e: &IndexedElement| {
                let id_ok = match &filter.element_id {
                    None => true,
                    Some(f) => query::match_string_property(f, Some(e.element_id.as_str())),
                };
                let state_ok = match &filter.element_instance_state {
                    None => true,
                    Some(f) => {
                        query::match_element_instance_state(&Some(f.clone()), e.state.as_str())
                    }
                };
                id_ok && state_ok
            };
            match filter.has_element_instance_incident {
                None => match elements {
                    Some(rows) => rows.iter().any(row_matches),
                    // No element rows: `elementId` still matches against an
                    // absent value (the `$exists: false` case); a state
                    // predicate can never be satisfied.
                    None => {
                        let id_ok = match &filter.element_id {
                            None => true,
                            Some(f) => query::match_string_property(f, None),
                        };
                        id_ok && filter.element_instance_state.is_none()
                    }
                },
                // `true`: some element satisfying the id/state predicates
                // carries an incident. `false`: none does (the inverted
                // existential — an instance with no element instances has no
                // incidented element, so it matches).
                Some(want) => {
                    let any_incidented = elements
                        .map(|rows| rows.iter().any(|e| row_matches(e) && e.has_incident))
                        .unwrap_or(false);
                    any_incidented == want
                }
            }
        }
    };
    // `errorMessage`: existential over the instance's job/incident messages.
    let error_message_matches = match &filter.error_message {
        None => true,
        Some(f) => match index.error_messages.get(&instance_key) {
            Some(msgs) => msgs
                .iter()
                .any(|m| query::match_string_property(f, Some(m.as_str()))),
            None => query::match_string_property(f, None),
        },
    };
    // `hasRetriesLeft`: `true` requires a job that failed with retries left.
    let retries_matches = match filter.has_retries_left {
        None => true,
        Some(want) => {
            index
                .has_retries_left
                .get(&instance_key)
                .copied()
                .unwrap_or(false)
                == want
        }
    };
    element_matches
        && error_message_matches
        && retries_matches
        // Not projected per-instance → matched against an absent value (fail
        // closed; see the doc comment).
        && query::match_integer(&filter.incident_error_hash_code, None)
        && query::match_string_opt(&filter.batch_operation_id, None)
        && query::match_string_opt(&filter.batch_operation_key, None)
}

/// Evaluates a process-instance `variables` filter against an instance's
/// process-scoped (root) variables, provided as a `name → serialized-JSON value`
/// map (absent when the instance has no root variables). Each
/// [`models::VariableValueFilterProperty`] selects a variable by `name` and
/// matches its value with the entry's `StringFilterProperty`; multiple entries
/// are ANDed (Camunda semantics). A variable absent from the instance is matched
/// against `None`, so an equality/`$like`/`$in` operator honestly excludes it
/// (rather than the field being silently ignored) while `$exists: false` still
/// matches — mirroring the `endDate` / `searchClusterVariables` convention.
pub fn match_instance_variables(
    filter: &Option<Vec<models::VariableValueFilterProperty>>,
    vars: Option<&std::collections::HashMap<String, String>>,
) -> bool {
    match filter {
        None => true,
        Some(entries) => entries.iter().all(|entry| {
            let value = vars.and_then(|m| m.get(&entry.name)).map(String::as_str);
            query::match_string_property(&entry.value, value)
        }),
    }
}

/// How a [`ProcessInstanceMatcher`] treats the filter's `state` field.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ProcessInstanceStatePolicy {
    /// Search: honour the supplied `state` predicate.
    HonourFilter,
    /// Process-instance batch operations: `state` is ignored and overridden to
    /// ACTIVE (`spec/process-instances.yaml:371-375`, `416-418`).
    ActiveOnly,
    /// Process-instance cancellation batch: `state` is ignored and overridden to
    /// ACTIVE or SUSPENDED (`spec/process-instances.yaml:443-445`).
    LiveOnly,
}

/// One AND-group of a process-instance filter (the top-level fields, or one
/// `$or` clause). Indexes are NOT per-group: they live once on the
/// [`ProcessInstanceMatcher`] so N `$or` clauses never build N copies.
pub struct ProcessInstanceFilterGroup {
    filter: models::ProcessInstanceFilter,
}

/// Query-level indexes shared by every group of one [`ProcessInstanceMatcher`].
pub struct ProcessInstanceIndexes {
    /// Root variables for the UNION of variable names referenced by the base
    /// filter and every `$or` clause — one scan, one map.
    variables: std::collections::HashMap<u64, std::collections::HashMap<String, String>>,
    /// Cross-entity (element / job / incident) index, built from the UNION of
    /// the base filter and every `$or` clause: the clause shape
    /// (`ProcessInstanceFilterFields`) carries the same cross-entity fields the
    /// top-level filter does, so a clause-only `elementId` / `hasRetriesLeft` /
    /// `errorMessage` must populate the index too — otherwise it matches
    /// against an empty index (missed matches, or overbroad `false` matches).
    cross: InstanceCrossEntityIndex,
}

/// The single process-instance filter predicate, shared by
/// `searchProcessInstances` and every process-instance batch creator so the
/// two can never drift. Honours every top-level field AND, when a non-empty
/// `$or` list is present, requires at least one clause to match (each clause an
/// AND of its own fields) — #1293.
pub struct ProcessInstanceMatcher {
    base: ProcessInstanceFilterGroup,
    or_clauses: Vec<ProcessInstanceFilterGroup>,
    indexes: ProcessInstanceIndexes,
}

impl ProcessInstanceMatcher {
    pub fn new<S: ReadSource + ?Sized>(filter: &models::ProcessInstanceFilter, store: &S) -> Self {
        let mut base = filter.clone();
        let clauses: Vec<models::ProcessInstanceFilter> = base
            .dollar_or
            .take()
            .unwrap_or_default()
            .into_iter()
            .map(process_instance_or_clause)
            .collect();
        let variable_names: std::collections::HashSet<String> = std::iter::once(&base)
            .chain(clauses.iter())
            .filter_map(|f| f.variables.as_ref())
            .flatten()
            .map(|e| e.name.clone())
            .collect();
        // One cross-entity index for the whole query, built from every group
        // that can carry element / job / incident predicates: the base filter
        // AND each `$or` clause (the clause schema carries the same fields).
        // `needs_elements` / `needs_jobs` / `needs_incidents` keep the build
        // lazy — a source table is loaded only when some group reads it.
        let index_groups: Vec<&models::ProcessInstanceFilter> =
            std::iter::once(&base).chain(clauses.iter()).collect();
        let indexes = ProcessInstanceIndexes {
            variables: root_variables_index(&variable_names, store),
            cross: if index_groups
                .iter()
                .any(|f| InstanceCrossEntityIndex::is_needed(f))
            {
                InstanceCrossEntityIndex::new(
                    &index_groups,
                    &InstanceCrossEntityIndex::sources_for(&index_groups, store),
                )
            } else {
                InstanceCrossEntityIndex::default()
            },
        };
        ProcessInstanceMatcher {
            base: ProcessInstanceFilterGroup { filter: base },
            or_clauses: clauses
                .into_iter()
                .map(|filter| ProcessInstanceFilterGroup { filter })
                .collect(),
            indexes,
        }
    }

    pub fn matches(
        &self,
        inst: &readstore::ProcessInstanceRow,
        state_policy: ProcessInstanceStatePolicy,
        hierarchy: BatchHierarchyPolicy,
    ) -> bool {
        let state_ok = match state_policy {
            ProcessInstanceStatePolicy::HonourFilter => true,
            ProcessInstanceStatePolicy::ActiveOnly => {
                matches!(inst.state, ProcessInstanceState::Active)
            }
            ProcessInstanceStatePolicy::LiveOnly => matches!(
                inst.state,
                ProcessInstanceState::Active | ProcessInstanceState::Suspended
            ),
        };
        let root_ok = match hierarchy {
            BatchHierarchyPolicy::HonourFilters => true,
            // Cancellation: only root instances, never active children.
            BatchHierarchyPolicy::RootOnly => inst.parent_process_instance_key.is_none(),
        };
        state_ok
            && root_ok
            && match_process_instance_group(
                inst,
                &self.base,
                &self.indexes,
                state_policy,
                hierarchy,
            )
            && (self.or_clauses.is_empty()
                || self.or_clauses.iter().any(|g| {
                    match_process_instance_group(inst, g, &self.indexes, state_policy, hierarchy)
                }))
    }
}

/// Lifts one `$or` clause into a `ProcessInstanceFilter` (no nested `$or`).
/// Exhaustive destructuring: a field added to the clause schema fails to
/// compile here instead of being silently dropped.
pub fn process_instance_or_clause(
    c: models::ProcessInstanceFilterFields,
) -> models::ProcessInstanceFilter {
    let models::ProcessInstanceFilterFields {
        start_date,
        end_date,
        state,
        has_incident,
        suspended_date,
        tenant_id,
        variables,
        process_instance_key,
        parent_process_instance_key,
        parent_element_instance_key,
        batch_operation_id,
        batch_operation_key,
        error_message,
        has_retries_left,
        element_instance_state,
        element_id,
        has_element_instance_incident,
        incident_error_hash_code,
        tags,
        business_id,
        process_definition_id,
        process_definition_name,
        process_definition_version,
        process_definition_version_tag,
        process_definition_key,
    } = c;
    models::ProcessInstanceFilter {
        start_date,
        end_date,
        state,
        has_incident,
        suspended_date,
        tenant_id,
        variables,
        process_instance_key,
        parent_process_instance_key,
        parent_element_instance_key,
        batch_operation_id,
        batch_operation_key,
        error_message,
        has_retries_left,
        element_instance_state,
        element_id,
        has_element_instance_incident,
        incident_error_hash_code,
        tags,
        business_id,
        process_definition_id,
        process_definition_name,
        process_definition_version,
        process_definition_version_tag,
        process_definition_key,
        dollar_or: None,
    }
}

/// Indexes process-scoped (root) variables by instance, restricted to the
/// `requested` variable names; empty (no scan) when none are requested.
pub fn root_variables_index<S: ReadSource + ?Sized>(
    requested: &std::collections::HashSet<String>,
    store: &S,
) -> std::collections::HashMap<u64, std::collections::HashMap<String, String>> {
    let mut idx: std::collections::HashMap<u64, std::collections::HashMap<String, String>> =
        std::collections::HashMap::new();
    if requested.is_empty() {
        return idx;
    }
    // Only the requested names are read, via the `name` index (#1886).
    let pushdown = readstore::VariablePushdown {
        names: Some(requested.iter().cloned().collect()),
        ..Default::default()
    };
    for v in store.variables_matching(&pushdown) {
        // Root-scope only: a nested-scope variable has `scope_key != instance_key`.
        if v.scope_key == v.instance_key && requested.contains(&v.name) {
            idx.entry(v.instance_key)
                .or_default()
                .insert(v.name, v.value);
        }
    }
    idx
}

/// Evaluates one AND-group of process-instance filter fields.
pub fn match_process_instance_group(
    inst: &readstore::ProcessInstanceRow,
    g: &ProcessInstanceFilterGroup,
    idx: &ProcessInstanceIndexes,
    state_policy: ProcessInstanceStatePolicy,
    hierarchy: BatchHierarchyPolicy,
) -> bool {
    let f = &g.filter;
    let state_ok = match state_policy {
        ProcessInstanceStatePolicy::HonourFilter => query::match_process_instance_state(
            &f.state,
            &process_instance_state_enum(inst.state).to_string(),
        ),
        // Overridden to ACTIVE by the caller; the supplied `state` is ignored.
        ProcessInstanceStatePolicy::ActiveOnly | ProcessInstanceStatePolicy::LiveOnly => true,
    };
    // Call-activity hierarchy filters (C8 parity, #977): a bare parent key
    // enumerates direct children; `$exists:false` selects top-level instances.
    // Cancellation (`RootOnly`) ignores them (the caller enforces root-only).
    let hierarchy_ok = match hierarchy {
        BatchHierarchyPolicy::HonourFilters => {
            query::match_process_instance_key_opt(
                &f.parent_process_instance_key,
                inst.parent_process_instance_key
                    .map(|k| k.to_string())
                    .as_deref(),
            ) && query::match_element_instance_key_opt(
                &f.parent_element_instance_key,
                inst.parent_element_instance_key
                    .map(|k| k.to_string())
                    .as_deref(),
            )
        }
        BatchHierarchyPolicy::RootOnly => true,
    };
    state_ok
        && hierarchy_ok
        && query::match_process_instance_key(&f.process_instance_key, &inst.key.to_string())
        && query::match_process_definition_key(
            &f.process_definition_key,
            &inst.process_definition_key,
        )
        && query::match_string(&f.process_definition_id, &inst.process_definition_id)
        && query::match_integer(&f.process_definition_version, Some(i64::from(inst.version)))
        && f.has_incident.is_none_or(|want| want == inst.has_incident)
        && query::match_date_time_ms(&f.start_date, Some(inst.start_date_ms as i64))
        // No projected completion timestamp: `endDate` matches an absent value
        // (value operators exclude, `$exists: false` matches).
        && query::match_date_time_ms(&f.end_date, None)
        && query::match_string_opt(&f.business_id, inst.business_id.as_deref())
        // `suspended_date_ms` is the single source of truth for suspension.
        && query::match_date_time_ms(
            &f.suspended_date,
            inst.suspended_date_ms.map(|ms| ms as i64),
        )
        // Single-tenant: every instance's `tenantId` is `<default>`.
        && query::match_string(&f.tenant_id, "<default>")
        // Never projected (always null in `process_instance_result`): matched
        // against an absent value.
        && query::match_string_opt(&f.process_definition_name, None)
        && query::match_string_opt(&f.process_definition_version_tag, None)
        && match_instance_variables(&f.variables, idx.variables.get(&inst.key))
        && match_instance_tags(&f.tags, &inst.tags)
        // Inherited element / job / incident / batch-operation filters.
        && match_instance_cross_entity_filters(f, inst.key, &idx.cross)
}

/// Matches the `tags` filter against an instance's carried tags. Every
/// requested tag must be present on the instance (AND semantics — a tag set
/// constrains to instances carrying the whole set, Camunda-style). An empty
/// filter set matches everything; an instance with no tags matches only an
/// absent/empty filter, never a non-empty one (so the field is honoured, not
/// silently ignored — the class that let a `tags: ["canary"]` migration select
/// untagged instances, #1551 review).
pub fn match_instance_tags(filter: &Option<Vec<models::Tag>>, instance_tags: &[String]) -> bool {
    match filter {
        None => true,
        Some(tags) => tags
            .iter()
            .all(|want| instance_tags.iter().any(|have| have == &want.0)),
    }
}

/// Projects a [`ProcessInstanceRow`] into the generated `ProcessInstanceResult`.
///
/// Surfaces the call-activity hierarchy (C8 parity): `parentProcessInstanceKey`
/// / `parentElementInstanceKey` carry the engine-tracked linkage for a child
/// spawned by a call activity (both null for a top-level instance), and
/// `rootProcessInstanceKey` is resolved by walking the parent chain to the
/// top-level ancestor (`roots`, memoised per request) — a top-level instance
/// reports its own key.
pub fn process_instance_result(
    instance: &readstore::ProcessInstanceRow,
    roots: &readstore::RootResolver,
) -> models::ProcessInstanceResult {
    let process_definition_id = instance.process_definition_id.clone();
    let version = instance.version;
    let process_definition_key = instance.process_definition_key.clone();

    let state_enum = process_instance_state_enum(instance.state);

    let start_date =
        chrono::DateTime::<chrono::Utc>::from_timestamp_millis(instance.start_date_ms as i64)
            .unwrap_or_else(epoch);

    let parent_process_instance_key = match instance.parent_process_instance_key {
        Some(k) => types::Nullable::Present(models::ProcessInstanceKey(k.to_string())),
        None => types::Nullable::Null,
    };
    let parent_element_instance_key = match instance.parent_element_instance_key {
        Some(k) => types::Nullable::Present(models::ElementInstanceKey(k.to_string())),
        None => types::Nullable::Null,
    };
    let root_process_instance_key = types::Nullable::Present(models::ProcessInstanceKey(
        roots.root_of_row(instance).to_string(),
    ));

    // Single source of truth: the read-model's `suspended_date_ms` column (also
    // what derives the `Suspended` state). Always present — a datetime while
    // suspended, null otherwise.
    let suspended_date = match instance.suspended_date_ms {
        Some(ms) => types::Nullable::Present(
            chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ms as i64).unwrap_or_else(epoch),
        ),
        None => types::Nullable::Null,
    };

    models::ProcessInstanceResult {
        process_definition_id,
        process_definition_name: types::Nullable::Null,
        process_definition_version: version,
        process_definition_version_tag: types::Nullable::Null,
        start_date,
        end_date: types::Nullable::Null,
        state: state_enum,
        suspended_date,
        has_incident: instance.has_incident,
        tenant_id: "<default>".to_string(),
        process_instance_key: models::ProcessInstanceKey(instance.key.to_string()),
        process_definition_key: models::ProcessDefinitionKey(process_definition_key),
        parent_process_instance_key,
        parent_element_instance_key,
        root_process_instance_key,
        tags: instance.tags.clone().into_iter().map(models::Tag).collect(),
        business_id: instance
            .business_id
            .clone()
            .map(types::Nullable::Present)
            .unwrap_or(types::Nullable::Null),
    }
}

/// Maps an engine [`ProcessInstanceState`] to the REST state enum.
pub fn process_instance_state_enum(
    state: ProcessInstanceState,
) -> models::ProcessInstanceStateEnum {
    match state {
        ProcessInstanceState::Active => models::ProcessInstanceStateEnum::Active,
        ProcessInstanceState::Completed => models::ProcessInstanceStateEnum::Completed,
        // The REST enum has no transient "terminating" state; an instance draining
        // its `canceling` task-listener chain (ADR 0037 §6) has already had its
        // tokens discarded and is on its way to `Terminated`, so it projects as
        // TERMINATED externally.
        ProcessInstanceState::Terminated | ProcessInstanceState::Terminating => {
            models::ProcessInstanceStateEnum::Terminated
        }
        ProcessInstanceState::Suspended => models::ProcessInstanceStateEnum::Suspended,
    }
}

/// A read-model business id (`None` when unset) as the REST `Nullable`.
pub fn nullable_business_id(id: Option<String>) -> types::Nullable<String> {
    id.map_or(types::Nullable::Null, types::Nullable::Present)
}

/// Maps a read-model [`readstore::ElementInstanceState`] to the REST element
/// instance state enum.
pub fn element_instance_state_enum(
    state: readstore::ElementInstanceState,
) -> models::ElementInstanceStateEnum {
    match state {
        readstore::ElementInstanceState::Active => models::ElementInstanceStateEnum::Active,
        readstore::ElementInstanceState::Completed => models::ElementInstanceStateEnum::Completed,
        readstore::ElementInstanceState::Terminated => models::ElementInstanceStateEnum::Terminated,
    }
}
