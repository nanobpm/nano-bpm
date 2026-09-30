//! Search-query support: the advanced filter algebra, multi-field sorting, and
//! cursor/offset pagination shared by the `search*` endpoints.
//!
//! The generated request models express filters as either a bare value (exact
//! match) or an "advanced" object carrying the `$eq`/`$neq`/`$exists`/`$in`/
//! `$notIn`/`$like` operators. Every advanced filter has the same shape, so we
//! normalise each one into a single [`Ops`] matcher that compares against a
//! string projection of the engine value. Keys project to their decimal string,
//! enums to their wire spelling (their `Display`), and string fields to
//! themselves.

use nanobpm_gateway_rest::{models, types};

/// The advanced-filter operators, normalised to string comparisons. `None`
/// fields are simply not constrained.
#[derive(Default)]
pub struct Ops {
    eq: Option<String>,
    neq: Option<String>,
    exists: Option<bool>,
    in_: Option<Vec<String>>,
    not_in: Option<Vec<String>>,
    like: Option<String>,
}

impl Ops {
    /// Whether `value` (the engine value's string projection, or `None` when the
    /// property is absent) satisfies every operator present on this filter.
    pub fn matches(&self, value: Option<&str>) -> bool {
        if matches!(self.exists, Some(e) if e != value.is_some()) {
            return false;
        }
        let v = match value {
            Some(v) => v,
            None => {
                // An absent value can only satisfy an `$exists: false` (handled
                // above); any value-based operator fails to match.
                return self.eq.is_none()
                    && self.neq.is_none()
                    && self.in_.is_none()
                    && self.not_in.is_none()
                    && self.like.is_none();
            }
        };
        if self.eq.as_deref().is_some_and(|eq| v != eq) {
            return false;
        }
        if self.neq.as_deref().is_some_and(|neq| v == neq) {
            return false;
        }
        if self
            .in_
            .as_ref()
            .is_some_and(|in_| !in_.iter().any(|x| x == v))
        {
            return false;
        }
        if self
            .not_in
            .as_ref()
            .is_some_and(|not_in| not_in.iter().any(|x| x == v))
        {
            return false;
        }
        if self
            .like
            .as_deref()
            .is_some_and(|like| !like_matches(like, v))
        {
            return false;
        }
        true
    }
}

/// The numeric advanced-filter operators (integer and date-time), normalised to
/// `i64` comparisons. Date-times are projected to epoch milliseconds so integer
/// and timestamp filters share one comparator. `None` fields are unconstrained.
#[derive(Default)]
struct NumOps {
    eq: Option<i64>,
    neq: Option<i64>,
    exists: Option<bool>,
    gt: Option<i64>,
    gte: Option<i64>,
    lt: Option<i64>,
    lte: Option<i64>,
    in_: Option<Vec<i64>>,
}

impl NumOps {
    /// Whether `value` (the engine value's `i64` projection, or `None` when the
    /// property is absent) satisfies every operator present on this filter.
    fn matches(&self, value: Option<i64>) -> bool {
        if matches!(self.exists, Some(e) if e != value.is_some()) {
            return false;
        }
        let v = match value {
            Some(v) => v,
            None => {
                // An absent value can only satisfy an `$exists: false` (handled
                // above); any value-based operator fails to match.
                return self.eq.is_none()
                    && self.neq.is_none()
                    && self.gt.is_none()
                    && self.gte.is_none()
                    && self.lt.is_none()
                    && self.lte.is_none()
                    && self.in_.is_none();
            }
        };
        if self.eq.is_some_and(|eq| v != eq) {
            return false;
        }
        if self.neq.is_some_and(|neq| v == neq) {
            return false;
        }
        if self.gt.is_some_and(|gt| v <= gt) {
            return false;
        }
        if self.gte.is_some_and(|gte| v < gte) {
            return false;
        }
        if self.lt.is_some_and(|lt| v >= lt) {
            return false;
        }
        if self.lte.is_some_and(|lte| v > lte) {
            return false;
        }
        if self.in_.as_ref().is_some_and(|in_| !in_.contains(&v)) {
            return false;
        }
        true
    }
}

/// Matches a `$like` pattern against a value. `*` matches any run of characters,
/// `?` matches a single character, and `\` escapes the next metacharacter. The
/// match is anchored to the whole string.
fn like_matches(pattern: &str, value: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let v: Vec<char> = value.chars().collect();
    // Classic backtracking wildcard matcher with a remembered `*` position.
    let (mut pi, mut vi) = (0usize, 0usize);
    let (mut star_p, mut star_v): (Option<usize>, usize) = (None, 0);
    while vi < v.len() {
        let lit = if pi < p.len() && p[pi] == '\\' && pi + 1 < p.len() {
            Some(p[pi + 1])
        } else {
            None
        };
        if let Some(c) = lit {
            if c == v[vi] {
                pi += 2;
                vi += 1;
                continue;
            }
        } else if pi < p.len() && p[pi] == '?' {
            pi += 1;
            vi += 1;
            continue;
        } else if pi < p.len() && p[pi] == '*' {
            star_p = Some(pi);
            star_v = vi;
            pi += 1;
            continue;
        } else if pi < p.len() && p[pi] == v[vi] {
            pi += 1;
            vi += 1;
            continue;
        }
        // Mismatch: backtrack to the last `*` if there was one.
        if let Some(sp) = star_p {
            pi = sp + 1;
            star_v += 1;
            vi = star_v;
        } else {
            return false;
        }
    }
    // Consume any trailing `*`s in the pattern.
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

/// Builds an [`Ops`] from an advanced filter's standard fields, projecting each
/// typed value to a string via `conv`. The optional `like` token is passed
/// through directly (only string-bearing filters carry one).
macro_rules! ops {
    ($adv:expr, $conv:expr) => {{
        let a = $adv;
        let conv = $conv;
        Ops {
            eq: a.dollar_eq.as_ref().map(&conv),
            neq: a.dollar_neq.as_ref().map(&conv),
            exists: a.dollar_exists,
            in_: a.dollar_in.as_ref().map(|v| v.iter().map(&conv).collect()),
            not_in: a
                .dollar_not_in
                .as_ref()
                .map(|v| v.iter().map(&conv).collect()),
            like: None,
        }
    }};
    ($adv:expr, $conv:expr, like) => {{
        let mut o = ops!($adv, $conv);
        o.like = $adv.dollar_like.clone();
        o
    }};
    // Variant for filters that carry `$like` but no `$notIn` (the enum-state
    // filters): omit `not_in`, include `like`.
    ($adv:expr, $conv:expr, like_no_notin) => {{
        let a = $adv;
        let conv = $conv;
        Ops {
            eq: a.dollar_eq.as_ref().map(&conv),
            neq: a.dollar_neq.as_ref().map(&conv),
            exists: a.dollar_exists,
            in_: a.dollar_in.as_ref().map(|v| v.iter().map(&conv).collect()),
            not_in: None,
            like: a.dollar_like.clone(),
        }
    }};
}

/// Matches a `StringFilterProperty` (bare string or advanced) against a value.
pub fn match_string(filter: &Option<models::StringFilterProperty>, value: &str) -> bool {
    match_string_opt(filter, Some(value))
}

/// Matches a `StringFilterProperty` against a possibly-absent string value.
/// Passing `None` models a schema field Nano does not yet project: an equality/
/// `$like`/`$in` filter then correctly yields no match (so the field is not
/// silently ignored), while `$exists: false` still matches. This keeps declared
/// filters honest instead of returning rows a client asked to exclude.
pub fn match_string_opt(
    filter: &Option<models::StringFilterProperty>,
    value: Option<&str>,
) -> bool {
    match filter {
        None => true,
        Some(f) => match_string_property(f, value),
    }
}

/// Matches a required (non-optional) `StringFilterProperty` against a
/// possibly-absent string value — the shape carried by nested filters where the
/// property itself is mandatory (e.g. `VariableValueFilterProperty.value`).
/// Shares its semantics with [`match_string_opt`] so the two cannot drift.
pub fn match_string_property(filter: &models::StringFilterProperty, value: Option<&str>) -> bool {
    match filter {
        models::StringFilterProperty::String(s) => value == Some(s.as_str()),
        models::StringFilterProperty::AdvancedStringFilter(a) => {
            ops!(a, |s: &String| s.clone(), like).matches(value)
        }
    }
}

/// Matches an `IntegerFilterProperty` (bare int or advanced `$eq`/`$neq`/
/// `$exists`/`$gt`/`$gte`/`$lt`/`$lte`/`$in`) against a possibly-absent integer.
pub fn match_integer(filter: &Option<models::IntegerFilterProperty>, value: Option<i64>) -> bool {
    match filter {
        None => true,
        Some(models::IntegerFilterProperty::I32(n)) => value == Some(*n as i64),
        Some(models::IntegerFilterProperty::AdvancedIntegerFilter(a)) => NumOps {
            eq: a.dollar_eq.map(i64::from),
            neq: a.dollar_neq.map(i64::from),
            exists: a.dollar_exists,
            gt: a.dollar_gt.map(i64::from),
            gte: a.dollar_gte.map(i64::from),
            lt: a.dollar_lt.map(i64::from),
            lte: a.dollar_lte.map(i64::from),
            in_: a
                .dollar_in
                .as_ref()
                .map(|v| v.iter().map(|&n| i64::from(n)).collect()),
        }
        .matches(value),
    }
}

/// Matches a `DateTimeFilterProperty` (bare date-time or advanced range filter)
/// against a possibly-absent value expressed as epoch milliseconds. Comparisons
/// are performed in millisecond space so `$gt`/`$lt` behave as calendar
/// comparisons; passing `None` correctly yields no match for value operators
/// while satisfying `$exists: false`.
pub fn match_date_time_ms(
    filter: &Option<models::DateTimeFilterProperty>,
    value_ms: Option<i64>,
) -> bool {
    match filter {
        None => true,
        Some(models::DateTimeFilterProperty::DateTimeUtc(dt)) => {
            value_ms == Some(dt.timestamp_millis())
        }
        Some(models::DateTimeFilterProperty::AdvancedDateTimeFilter(a)) => NumOps {
            eq: a.dollar_eq.map(|d| d.timestamp_millis()),
            neq: a.dollar_neq.map(|d| d.timestamp_millis()),
            exists: a.dollar_exists,
            gt: a.dollar_gt.map(|d| d.timestamp_millis()),
            gte: a.dollar_gte.map(|d| d.timestamp_millis()),
            lt: a.dollar_lt.map(|d| d.timestamp_millis()),
            lte: a.dollar_lte.map(|d| d.timestamp_millis()),
            in_: a
                .dollar_in
                .as_ref()
                .map(|v| v.iter().map(|d| d.timestamp_millis()).collect()),
        }
        .matches(value_ms),
    }
}

/// Matches an `ElementIdFilterProperty` (bare string or `$like` advanced
/// filter) against an element id. Its advanced filter mixes `String` (`$eq`/
/// `$neq`) and `ElementId` (`$in`/`$notIn`) field types, so the `Ops` matcher is
/// assembled by hand rather than via the single-`conv` `ops!` macro.
pub fn match_element_id(filter: &Option<models::ElementIdFilterProperty>, value: &str) -> bool {
    match filter {
        None => true,
        Some(models::ElementIdFilterProperty::String(s)) => s == value,
        Some(models::ElementIdFilterProperty::AdvancedElementIdFilter(a)) => Ops {
            eq: a.dollar_eq.clone(),
            neq: a.dollar_neq.clone(),
            exists: a.dollar_exists,
            in_: a
                .dollar_in
                .as_ref()
                .map(|v| v.iter().map(|e| e.0.clone()).collect()),
            not_in: a
                .dollar_not_in
                .as_ref()
                .map(|v| v.iter().map(|e| e.0.clone()).collect()),
            like: a.dollar_like.clone(),
        }
        .matches(Some(value)),
    }
}

/// Matches an `ElementInstanceStateFilterProperty` against a state's wire
/// spelling (`ACTIVE`/`COMPLETED`/`TERMINATED`).
pub fn match_element_instance_state(
    filter: &Option<models::ElementInstanceStateFilterProperty>,
    value: &str,
) -> bool {
    match filter {
        None => true,
        Some(models::ElementInstanceStateFilterProperty::ElementInstanceStateEnum(e)) => {
            e.to_string() == value
        }
        Some(models::ElementInstanceStateFilterProperty::AdvancedElementInstanceStateFilter(a)) => {
            ops!(
                a,
                |e: &models::ElementInstanceStateEnum| e.to_string(),
                like_no_notin
            )
            .matches(Some(value))
        }
    }
}

/// Matches a `WaitStateElementTypeFilterProperty` (bare enum or advanced) against
/// an element type's wire spelling (e.g. `SERVICE_TASK`).
pub fn match_wait_state_element_type(
    filter: &Option<models::WaitStateElementTypeFilterProperty>,
    value: &str,
) -> bool {
    match filter {
        None => true,
        Some(models::WaitStateElementTypeFilterProperty::WaitStateElementTypeEnum(e)) => {
            e.to_string() == value
        }
        Some(models::WaitStateElementTypeFilterProperty::AdvancedWaitStateElementTypeFilter(a)) => {
            ops!(
                a,
                |e: &models::WaitStateElementTypeEnum| e.to_string(),
                like_no_notin
            )
            .matches(Some(value))
        }
    }
}

/// Matches a `WaitStateTypeFilterProperty` (bare enum or advanced) against a wait
/// state type's wire spelling (`JOB`/`MESSAGE`).
pub fn match_wait_state_type(
    filter: &Option<models::WaitStateTypeFilterProperty>,
    value: &str,
) -> bool {
    match filter {
        None => true,
        Some(models::WaitStateTypeFilterProperty::WaitStateTypeEnum(e)) => e.to_string() == value,
        Some(models::WaitStateTypeFilterProperty::AdvancedWaitStateTypeFilter(a)) => ops!(
            a,
            |e: &models::WaitStateTypeEnum| e.to_string(),
            like_no_notin
        )
        .matches(Some(value)),
    }
}

/// Matches a `ClusterVariableScopeFilterProperty` (bare enum or advanced)
/// against a scope's wire spelling (`GLOBAL`/`TENANT`).
pub fn match_cluster_variable_scope(
    filter: &Option<models::ClusterVariableScopeFilterProperty>,
    value: &str,
) -> bool {
    match filter {
        None => true,
        Some(models::ClusterVariableScopeFilterProperty::ClusterVariableScopeEnum(e)) => {
            e.to_string() == value
        }
        Some(models::ClusterVariableScopeFilterProperty::AdvancedClusterVariableScopeFilter(a)) => {
            ops!(
                a,
                |e: &models::ClusterVariableScopeEnum| e.to_string(),
                like_no_notin
            )
            .matches(Some(value))
        }
    }
}

/// Matches a `ClusterVariableKindFilterProperty` (bare enum or advanced)
/// against a kind's wire spelling (`JSON`/`SECRET_REFERENCE`).
pub fn match_cluster_variable_kind(
    filter: &Option<models::ClusterVariableKindFilterProperty>,
    value: &str,
) -> bool {
    match filter {
        None => true,
        Some(models::ClusterVariableKindFilterProperty::ClusterVariableKindEnum(e)) => {
            e.to_string() == value
        }
        Some(models::ClusterVariableKindFilterProperty::AdvancedClusterVariableKindFilter(a)) => {
            ops!(
                a,
                |e: &models::ClusterVariableKindEnum| e.to_string(),
                like_no_notin
            )
            .matches(Some(value))
        }
    }
}

/// A metadata scalar, borrowed from any of the generated string-or-number
/// unions (stored value, `$eq`, `$neq`, `$in` item). Comparison is type-strict:
/// the string `"30"` is not the number `30`.
#[derive(Clone, Copy, PartialEq)]
enum MetaScalar<'a> {
    Str(&'a str),
    Num(f64),
}

impl<'a> From<&'a models::ClusterVariableResultBaseMetadataValue> for MetaScalar<'a> {
    fn from(v: &'a models::ClusterVariableResultBaseMetadataValue) -> Self {
        match v {
            models::ClusterVariableResultBaseMetadataValue::String(s) => Self::Str(s),
            models::ClusterVariableResultBaseMetadataValue::F64(n) => Self::Num(*n),
        }
    }
}

impl<'a> From<&'a models::AdvancedMetadataValueFilterEq> for MetaScalar<'a> {
    fn from(v: &'a models::AdvancedMetadataValueFilterEq) -> Self {
        match v {
            models::AdvancedMetadataValueFilterEq::String(s) => Self::Str(s),
            models::AdvancedMetadataValueFilterEq::F64(n) => Self::Num(*n),
        }
    }
}

impl<'a> From<&'a models::AdvancedMetadataValueFilterNeq> for MetaScalar<'a> {
    fn from(v: &'a models::AdvancedMetadataValueFilterNeq) -> Self {
        match v {
            models::AdvancedMetadataValueFilterNeq::String(s) => Self::Str(s),
            models::AdvancedMetadataValueFilterNeq::F64(n) => Self::Num(*n),
        }
    }
}

/// Matches one metadata entry (absent = `None`) against its
/// `AdvancedMetadataValueFilter`. Absent-value semantics mirror [`Ops`]: only
/// `$exists: false` (or no value operator at all) matches a missing key. Range
/// operators match numbers only; `$like` matches strings only.
fn metadata_value_matches(
    f: &models::AdvancedMetadataValueFilter,
    value: Option<&models::ClusterVariableResultBaseMetadataValue>,
) -> bool {
    if matches!(f.dollar_exists, Some(e) if e != value.is_some()) {
        return false;
    }
    let Some(v) = value.map(MetaScalar::from) else {
        return f.dollar_eq.is_none()
            && f.dollar_neq.is_none()
            && f.dollar_gt.is_none()
            && f.dollar_gte.is_none()
            && f.dollar_lt.is_none()
            && f.dollar_lte.is_none()
            && f.dollar_in.is_none()
            && f.dollar_like.is_none();
    };
    let num = match v {
        MetaScalar::Num(n) => Some(n),
        MetaScalar::Str(_) => None,
    };
    let range = |bound: Option<f64>, ok: fn(f64, f64) -> bool| {
        bound.is_none_or(|b| num.is_some_and(|n| ok(n, b)))
    };
    f.dollar_eq
        .as_ref()
        .is_none_or(|eq| v == MetaScalar::from(eq))
        && f.dollar_neq
            .as_ref()
            .is_none_or(|neq| v != MetaScalar::from(neq))
        && range(f.dollar_gt, |n, b| n > b)
        && range(f.dollar_gte, |n, b| n >= b)
        && range(f.dollar_lt, |n, b| n < b)
        && range(f.dollar_lte, |n, b| n <= b)
        && f.dollar_in
            .as_ref()
            .is_none_or(|in_| in_.iter().any(|x| v == MetaScalar::from(x)))
        && f.dollar_like.as_deref().is_none_or(|like| match v {
            MetaScalar::Str(s) => like_matches(like, s),
            MetaScalar::Num(_) => false,
        })
}

/// Matches a cluster variable's metadata bag against a per-key filter map:
/// every filtered key must match (AND).
pub fn match_cluster_variable_metadata(
    filter: &Option<std::collections::HashMap<String, models::AdvancedMetadataValueFilter>>,
    metadata: &std::collections::HashMap<String, models::ClusterVariableResultBaseMetadataValue>,
) -> bool {
    filter
        .iter()
        .flatten()
        .all(|(key, f)| metadata_value_matches(f, metadata.get(key)))
}

/// Matches a `BasicStringFilterProperty` (bare string or basic filter — no
/// `$like`) against a value.
pub fn match_basic_string(filter: &Option<models::BasicStringFilterProperty>, value: &str) -> bool {
    match filter {
        None => true,
        Some(models::BasicStringFilterProperty::String(s)) => s == value,
        Some(models::BasicStringFilterProperty::BasicStringFilter(a)) => {
            ops!(a, |s: &String| s.clone()).matches(Some(value))
        }
    }
}

/// Matches a `ProcessInstanceKeyFilterProperty` against a key's decimal string.
pub fn match_process_instance_key(
    filter: &Option<models::ProcessInstanceKeyFilterProperty>,
    value: &str,
) -> bool {
    match_process_instance_key_opt(filter, Some(value))
}

/// Matches a `ProcessInstanceKeyFilterProperty` against a possibly-absent key.
/// Passing `None` (no key on the record, e.g. a top-level instance's
/// `parentProcessInstanceKey`) lets `$exists: false` match while a bare key or
/// any value operator fails — the honest semantics for an absent linkage.
pub fn match_process_instance_key_opt(
    filter: &Option<models::ProcessInstanceKeyFilterProperty>,
    value: Option<&str>,
) -> bool {
    match filter {
        None => true,
        Some(models::ProcessInstanceKeyFilterProperty::ProcessInstanceKey(k)) => {
            value == Some(k.0.as_str())
        }
        Some(models::ProcessInstanceKeyFilterProperty::AdvancedProcessInstanceKeyFilter(a)) => {
            ops!(a, |k: &models::ProcessInstanceKey| k.0.clone()).matches(value)
        }
    }
}

/// Matches a `ProcessDefinitionKeyFilterProperty` against a key's decimal string.
pub fn match_process_definition_key(
    filter: &Option<models::ProcessDefinitionKeyFilterProperty>,
    value: &str,
) -> bool {
    match filter {
        None => true,
        Some(models::ProcessDefinitionKeyFilterProperty::ProcessDefinitionKey(k)) => k.0 == value,
        Some(models::ProcessDefinitionKeyFilterProperty::AdvancedProcessDefinitionKeyFilter(a)) => {
            ops!(a, |k: &models::ProcessDefinitionKey| k.0.clone()).matches(Some(value))
        }
    }
}

/// Matches an `ElementInstanceKeyFilterProperty` against a key's decimal string.
pub fn match_element_instance_key(
    filter: &Option<models::ElementInstanceKeyFilterProperty>,
    value: &str,
) -> bool {
    match_element_instance_key_opt(filter, Some(value))
}

/// Matches an `ElementInstanceKeyFilterProperty` against a possibly-absent key.
/// Passing `None` (e.g. a top-level instance's `parentElementInstanceKey`) lets
/// `$exists: false` match while a bare key or value operator fails.
pub fn match_element_instance_key_opt(
    filter: &Option<models::ElementInstanceKeyFilterProperty>,
    value: Option<&str>,
) -> bool {
    match filter {
        None => true,
        Some(models::ElementInstanceKeyFilterProperty::ElementInstanceKey(k)) => {
            value == Some(k.0.as_str())
        }
        Some(models::ElementInstanceKeyFilterProperty::AdvancedElementInstanceKeyFilter(a)) => {
            ops!(a, |k: &models::ElementInstanceKey| k.0.clone()).matches(value)
        }
    }
}

/// Matches an `AgentInstanceKeyFilterProperty` against a key's decimal string.
pub fn match_agent_instance_key(
    filter: &Option<models::AgentInstanceKeyFilterProperty>,
    value: &str,
) -> bool {
    match filter {
        None => true,
        Some(models::AgentInstanceKeyFilterProperty::AgentInstanceKey(k)) => k.0 == value,
        Some(models::AgentInstanceKeyFilterProperty::AdvancedAgentInstanceKeyFilter(a)) => {
            ops!(a, |k: &models::AgentInstanceKey| k.0.clone()).matches(Some(value))
        }
    }
}

pub fn match_agent_definition_key(
    filter: &Option<models::AgentDefinitionKeyFilterProperty>,
    value: &str,
) -> bool {
    match filter {
        None => true,
        Some(models::AgentDefinitionKeyFilterProperty::AgentDefinitionKey(key)) => key.0 == value,
        Some(models::AgentDefinitionKeyFilterProperty::AdvancedAgentDefinitionKeyFilter(
            filter,
        )) => ops!(filter, |key: &models::AgentDefinitionKey| key.0.clone()).matches(Some(value)),
    }
}

pub fn match_agent_history_item_key(
    filter: &Option<models::AgentHistoryItemKeyFilterProperty>,
    value: &str,
) -> bool {
    match filter {
        None => true,
        Some(models::AgentHistoryItemKeyFilterProperty::AgentHistoryItemKey(key)) => key.0 == value,
        Some(models::AgentHistoryItemKeyFilterProperty::AdvancedAgentHistoryItemKeyFilter(
            filter,
        )) => ops!(filter, |key: &models::AgentHistoryItemKey| key.0.clone()).matches(Some(value)),
    }
}

/// Matches an `AgentInstanceStatusFilterProperty` (exact enum or advanced
/// `$eq`/`$neq`/`$exists`/`$in`/`$like`) against a status's wire spelling
/// (`INITIALIZING`, `IDLE`, …).
pub fn match_agent_instance_status(
    filter: &Option<models::AgentInstanceStatusFilterProperty>,
    value: &str,
) -> bool {
    match filter {
        None => true,
        Some(models::AgentInstanceStatusFilterProperty::AgentInstanceStatusEnum(e)) => {
            e.to_string() == value
        }
        Some(models::AgentInstanceStatusFilterProperty::AdvancedAgentInstanceStatusFilter(a)) => {
            ops!(
                a,
                |e: &models::AgentInstanceStatusEnum| e.to_string(),
                like_no_notin
            )
            .matches(Some(value))
        }
    }
}

/// Matches a `ResourceKeyFilterProperty` against a key's decimal string.
pub fn match_resource_key(filter: &Option<models::ResourceKeyFilterProperty>, value: &str) -> bool {
    match filter {
        None => true,
        Some(models::ResourceKeyFilterProperty::ResourceKey(k)) => k.0 == value,
        Some(models::ResourceKeyFilterProperty::AdvancedResourceKeyFilter(a)) => {
            ops!(a, |k: &models::ResourceKey| k.0.clone()).matches(Some(value))
        }
    }
}

/// Matches a `JobKeyFilterProperty` against a key's decimal string.
pub fn match_job_key(filter: &Option<models::JobKeyFilterProperty>, value: &str) -> bool {
    match_job_key_opt(filter, Some(value))
}

/// Matches a `JobKeyFilterProperty` against a possibly-absent job key. Passing
/// `None` (no job key on the record) lets advanced `$exists: false` filters
/// match, and makes any value-based operator (including a bare key) fail —
/// unlike coercing absence to an empty string, which spuriously satisfies
/// `$exists: true`.
pub fn match_job_key_opt(
    filter: &Option<models::JobKeyFilterProperty>,
    value: Option<&str>,
) -> bool {
    match filter {
        None => true,
        Some(models::JobKeyFilterProperty::JobKey(k)) => value == Some(k.0.as_str()),
        Some(models::JobKeyFilterProperty::AdvancedJobKeyFilter(a)) => {
            ops!(a, |k: &models::JobKey| k.0.clone()).matches(value)
        }
    }
}

/// Matches a `VariableKeyFilterProperty` against a key's decimal string.
pub fn match_variable_key(filter: &Option<models::VariableKeyFilterProperty>, value: &str) -> bool {
    match filter {
        None => true,
        Some(models::VariableKeyFilterProperty::VariableKey(k)) => k.0 == value,
        Some(models::VariableKeyFilterProperty::AdvancedVariableKeyFilter(a)) => {
            ops!(a, |k: &models::VariableKey| k.0.clone()).matches(Some(value))
        }
    }
}

/// Matches a `MessageSubscriptionKeyFilterProperty` against a key's decimal string.
pub fn match_message_subscription_key(
    filter: &Option<models::MessageSubscriptionKeyFilterProperty>,
    value: &str,
) -> bool {
    match filter {
        None => true,
        Some(models::MessageSubscriptionKeyFilterProperty::MessageSubscriptionKey(k)) => {
            k.0 == value
        }
        Some(
            models::MessageSubscriptionKeyFilterProperty::AdvancedMessageSubscriptionKeyFilter(a),
        ) => ops!(a, |k: &models::MessageSubscriptionKey| k.0.clone()).matches(Some(value)),
    }
}

/// Matches a `MessageSubscriptionStateFilterProperty` against a state's wire spelling.
pub fn match_message_subscription_state(
    filter: &Option<models::MessageSubscriptionStateFilterProperty>,
    value: &str,
) -> bool {
    match filter {
        None => true,
        Some(models::MessageSubscriptionStateFilterProperty::MessageSubscriptionStateEnum(e)) => {
            e.to_string() == value
        }
        Some(
            models::MessageSubscriptionStateFilterProperty::AdvancedMessageSubscriptionStateFilter(
                a,
            ),
        ) => ops!(
            a,
            |e: &models::MessageSubscriptionStateEnum| e.to_string(),
            like_no_notin
        )
        .matches(Some(value)),
    }
}

/// Matches a `MessageSubscriptionTypeFilterProperty` against a type's wire spelling.
pub fn match_message_subscription_type(
    filter: &Option<models::MessageSubscriptionTypeFilterProperty>,
    value: &str,
) -> bool {
    match filter {
        None => true,
        Some(models::MessageSubscriptionTypeFilterProperty::MessageSubscriptionTypeEnum(e)) => {
            e.to_string() == value
        }
        Some(
            models::MessageSubscriptionTypeFilterProperty::AdvancedMessageSubscriptionTypeFilter(a),
        ) => ops!(
            a,
            |e: &models::MessageSubscriptionTypeEnum| e.to_string(),
            like_no_notin
        )
        .matches(Some(value)),
    }
}

/// Matches a `ScopeKeyFilterProperty` against a key's decimal string.
pub fn match_scope_key(filter: &Option<models::ScopeKeyFilterProperty>, value: &str) -> bool {
    match filter {
        None => true,
        Some(models::ScopeKeyFilterProperty::ScopeKey(k)) => k.0 == value,
        Some(models::ScopeKeyFilterProperty::AdvancedScopeKeyFilter(a)) => {
            ops!(a, |k: &models::ScopeKey| k.0.clone()).matches(Some(value))
        }
    }
}

/// Matches an `IncidentStateFilterProperty` against a state's wire spelling.
pub fn match_incident_state(
    filter: &Option<models::IncidentStateFilterProperty>,
    value: &str,
) -> bool {
    match filter {
        None => true,
        Some(models::IncidentStateFilterProperty::IncidentStateEnum(e)) => e.to_string() == value,
        Some(models::IncidentStateFilterProperty::AdvancedIncidentStateFilter(a)) => {
            ops!(a, |e: &models::IncidentStateEnum| e.to_string(), like).matches(Some(value))
        }
    }
}

/// Matches an `IncidentErrorTypeFilterProperty` against an error type's wire
/// spelling.
pub fn match_incident_error_type(
    filter: &Option<models::IncidentErrorTypeFilterProperty>,
    value: &str,
) -> bool {
    match filter {
        None => true,
        Some(models::IncidentErrorTypeFilterProperty::IncidentErrorTypeEnum(e)) => {
            e.to_string() == value
        }
        Some(models::IncidentErrorTypeFilterProperty::AdvancedIncidentErrorTypeFilter(a)) => {
            ops!(a, |e: &models::IncidentErrorTypeEnum| e.to_string(), like).matches(Some(value))
        }
    }
}

/// Matches a `DecisionDefinitionKeyFilterProperty` against a key's decimal
/// string. Shared by the decision-instance search's `decisionDefinitionKey` and
/// `rootDecisionDefinitionKey` filters.
pub fn match_decision_definition_key(
    filter: &Option<models::DecisionDefinitionKeyFilterProperty>,
    value: &str,
) -> bool {
    match filter {
        None => true,
        Some(models::DecisionDefinitionKeyFilterProperty::DecisionDefinitionKey(k)) => k.0 == value,
        Some(models::DecisionDefinitionKeyFilterProperty::AdvancedDecisionDefinitionKeyFilter(
            a,
        )) => ops!(a, |k: &models::DecisionDefinitionKey| k.0.clone()).matches(Some(value)),
    }
}

/// Matches a `DecisionRequirementsKeyFilterProperty` against a key's decimal
/// string.
pub fn match_decision_requirements_key(
    filter: &Option<models::DecisionRequirementsKeyFilterProperty>,
    value: &str,
) -> bool {
    match filter {
        None => true,
        Some(models::DecisionRequirementsKeyFilterProperty::DecisionRequirementsKey(k)) => {
            k.0 == value
        }
        Some(
            models::DecisionRequirementsKeyFilterProperty::AdvancedDecisionRequirementsKeyFilter(a),
        ) => ops!(a, |k: &models::DecisionRequirementsKey| k.0.clone()).matches(Some(value)),
    }
}

/// Matches a `DecisionInstanceStateFilterProperty` against a state's wire
/// spelling (`EVALUATED` / `FAILED`).
pub fn match_decision_instance_state(
    filter: &Option<models::DecisionInstanceStateFilterProperty>,
    value: &str,
) -> bool {
    match filter {
        None => true,
        Some(models::DecisionInstanceStateFilterProperty::DecisionInstanceStateEnum(e)) => {
            e.to_string() == value
        }
        Some(models::DecisionInstanceStateFilterProperty::AdvancedDecisionInstanceStateFilter(
            a,
        )) => ops!(
            a,
            |e: &models::DecisionInstanceStateEnum| e.to_string(),
            like
        )
        .matches(Some(value)),
    }
}

/// Matches a `DecisionEvaluationInstanceKeyFilterProperty` against a decision
/// instance's `<key>-<idx>` id. The advanced filter mixes a `String` `$eq`/`$neq`
/// with newtype `$in`/`$notIn`, so its [`Ops`] is assembled by hand.
pub fn match_decision_evaluation_instance_key(
    filter: &Option<models::DecisionEvaluationInstanceKeyFilterProperty>,
    value: &str,
) -> bool {
    match filter {
        None => true,
        Some(models::DecisionEvaluationInstanceKeyFilterProperty::String(s)) => s == value,
        Some(
            models::DecisionEvaluationInstanceKeyFilterProperty::AdvancedDecisionEvaluationInstanceKeyFilter(a),
        ) => Ops {
            eq: a.dollar_eq.clone(),
            neq: a.dollar_neq.clone(),
            exists: a.dollar_exists,
            in_: a
                .dollar_in
                .as_ref()
                .map(|v| v.iter().map(|k| k.0.clone()).collect()),
            not_in: a
                .dollar_not_in
                .as_ref()
                .map(|v| v.iter().map(|k| k.0.clone()).collect()),
            like: None,
        }
        .matches(Some(value)),
    }
}

/// Matches a `ProcessInstanceStateFilterProperty` against a state's wire
/// spelling.
pub fn match_process_instance_state(
    filter: &Option<models::ProcessInstanceStateFilterProperty>,
    value: &str,
) -> bool {
    match filter {
        None => true,
        Some(models::ProcessInstanceStateFilterProperty::ProcessInstanceStateEnum(e)) => {
            e.to_string() == value
        }
        Some(models::ProcessInstanceStateFilterProperty::AdvancedProcessInstanceStateFilter(a)) => {
            ops!(
                a,
                |e: &models::ProcessInstanceStateEnum| e.to_string(),
                like_no_notin
            )
            .matches(Some(value))
        }
    }
}

/// Matches a `JobStateFilterProperty` against a job state's wire spelling.
pub fn match_job_state(filter: &Option<models::JobStateFilterProperty>, value: &str) -> bool {
    match filter {
        None => true,
        Some(models::JobStateFilterProperty::JobStateEnum(e)) => e.to_string() == value,
        Some(models::JobStateFilterProperty::AdvancedJobStateFilter(a)) => {
            ops!(a, |e: &models::JobStateEnum| e.to_string(), like_no_notin).matches(Some(value))
        }
    }
}

/// Matches a `UserTaskStateFilterProperty` against a user-task state's wire
/// spelling.
pub fn match_user_task_state(
    filter: &Option<models::UserTaskStateFilterProperty>,
    value: &str,
) -> bool {
    match filter {
        None => true,
        Some(models::UserTaskStateFilterProperty::UserTaskStateEnum(e)) => e.to_string() == value,
        Some(models::UserTaskStateFilterProperty::AdvancedUserTaskStateFilter(a)) => ops!(
            a,
            |e: &models::UserTaskStateEnum| e.to_string(),
            like_no_notin
        )
        .matches(Some(value)),
    }
}

/// Matches a `BatchOperationStateFilterProperty` against a batch operation
/// state's wire spelling (`ACTIVE`, `SUSPENDED`, `CANCELED`, …).
pub fn match_batch_operation_state(
    filter: &Option<models::BatchOperationStateFilterProperty>,
    value: &str,
) -> bool {
    match filter {
        None => true,
        Some(models::BatchOperationStateFilterProperty::BatchOperationStateEnum(e)) => {
            e.to_string() == value
        }
        Some(models::BatchOperationStateFilterProperty::AdvancedBatchOperationStateFilter(a)) => {
            ops!(
                a,
                |e: &models::BatchOperationStateEnum| e.to_string(),
                like_no_notin
            )
            .matches(Some(value))
        }
    }
}

/// Matches a `BatchOperationTypeFilterProperty` against a batch operation type's
/// wire spelling (`CANCEL_PROCESS_INSTANCE`, `RESOLVE_INCIDENT`, …).
pub fn match_batch_operation_type(
    filter: &Option<models::BatchOperationTypeFilterProperty>,
    value: &str,
) -> bool {
    match filter {
        None => true,
        Some(models::BatchOperationTypeFilterProperty::BatchOperationTypeEnum(e)) => {
            e.to_string() == value
        }
        Some(models::BatchOperationTypeFilterProperty::AdvancedBatchOperationTypeFilter(a)) => {
            ops!(
                a,
                |e: &models::BatchOperationTypeEnum| e.to_string(),
                like_no_notin
            )
            .matches(Some(value))
        }
    }
}

/// Matches a `BatchOperationItemStateFilterProperty` against a batch-operation
/// item state's wire spelling (`ACTIVE`, `COMPLETED`, `FAILED`, `CANCELED`).
pub fn match_batch_operation_item_state(
    filter: &Option<models::BatchOperationItemStateFilterProperty>,
    value: &str,
) -> bool {
    match filter {
        None => true,
        Some(models::BatchOperationItemStateFilterProperty::BatchOperationItemStateEnum(e)) => {
            e.to_string() == value
        }
        Some(
            models::BatchOperationItemStateFilterProperty::AdvancedBatchOperationItemStateFilter(a),
        ) => ops!(
            a,
            |e: &models::BatchOperationItemStateEnum| e.to_string(),
            like_no_notin
        )
        .matches(Some(value)),
    }
}

/// One normalised sort instruction: the field name and whether it is descending.
pub struct SortKey {
    pub field: String,
    pub descending: bool,
}

/// Normalises the generated sort requests (field + optional ASC/DESC) into a
/// flat list. `extract` pulls `(field, order)` from each request so this works
/// for every endpoint's sort-request type.
pub fn sort_keys<S>(
    sort: Option<&Vec<S>>,
    extract: impl Fn(&S) -> (String, Option<models::SortOrderEnum>),
) -> Vec<SortKey> {
    sort.map(|reqs| {
        reqs.iter()
            .map(|r| {
                let (field, order) = extract(r);
                SortKey {
                    field,
                    descending: matches!(order, Some(models::SortOrderEnum::Desc)),
                }
            })
            .collect()
    })
    .unwrap_or_default()
}

/// A value to sort by: numeric keys sort numerically, everything else
/// lexicographically. Numbers always sort before strings (they never mix in
/// practice).
#[derive(PartialEq, Eq)]
pub enum SortVal {
    Num(i64),
    Str(String),
}

impl PartialOrd for SortVal {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SortVal {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        match (self, other) {
            (SortVal::Num(a), SortVal::Num(b)) => a.cmp(b),
            (SortVal::Str(a), SortVal::Str(b)) => a.cmp(b),
            (SortVal::Num(_), SortVal::Str(_)) => Ordering::Less,
            (SortVal::Str(_), SortVal::Num(_)) => Ordering::Greater,
        }
    }
}

/// Stably sorts `items` by the given sort keys, breaking ties by entity key so
/// the order is fully deterministic (and stable for cursor paging). `project`
/// returns the [`SortVal`] for a given `(item, field)`; `key` returns the
/// entity key used as the final tiebreaker.
pub fn sort_items<T, K: Ord>(
    items: &mut [T],
    keys: &[SortKey],
    project: impl Fn(&T, &str) -> SortVal,
    key: impl Fn(&T) -> K,
) {
    items.sort_by(|a, b| {
        for sk in keys {
            let ord = project(a, &sk.field).cmp(&project(b, &sk.field));
            let ord = if sk.descending { ord.reverse() } else { ord };
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        key(a).cmp(&key(b))
    });
}

/// A page of results plus the response envelope describing it.
pub struct Page<T> {
    pub items: Vec<T>,
    pub response: models::SearchQueryPageResponse,
}

/// An entity key usable as an opaque page cursor: it must round-trip through a
/// spec-conformant base64 string (no padding, length a multiple of four) and
/// support equality so a forward/backward cursor can locate its resume point.
/// Implemented for a single `u64` entity key and for a composite `(u64, u64)`
/// key (used where no single column is unique, e.g. correlated message
/// subscriptions keyed by `(message_key, subscription_key)`).
pub trait CursorKey: Copy + Eq {
    fn encode(self) -> String;
    fn decode(cursor: &str) -> Option<Self>;
}

impl CursorKey for u64 {
    fn encode(self) -> String {
        encode_cursor(self)
    }
    fn decode(cursor: &str) -> Option<Self> {
        decode_cursor(cursor)
    }
}

impl CursorKey for (u64, u64) {
    /// Eighteen big-endian bytes — two leading zero bytes then the two `u64`s,
    /// contiguously — in standard base64 without padding. Eighteen is a multiple
    /// of three, so the result is exactly 24 characters with no `=` padding, a
    /// clean multiple of four that satisfies the spec's cursor charset. The two
    /// fixed leading zero bytes are validated on decode so there is exactly one
    /// canonical encoding per key.
    fn encode(self) -> String {
        let mut bytes = [0u8; 18];
        bytes[2..10].copy_from_slice(&self.0.to_be_bytes());
        bytes[10..18].copy_from_slice(&self.1.to_be_bytes());
        base64_encode(&bytes)
    }
    fn decode(cursor: &str) -> Option<Self> {
        let bytes = base64_decode(cursor)?;
        if bytes.len() != 18 || bytes[0] != 0 || bytes[1] != 0 {
            return None;
        }
        let mut a = [0u8; 8];
        let mut b = [0u8; 8];
        a.copy_from_slice(&bytes[2..10]);
        b.copy_from_slice(&bytes[10..18]);
        Some((u64::from_be_bytes(a), u64::from_be_bytes(b)))
    }
}

/// Applies pagination (limit + offset/forward-cursor/backward-cursor) to an
/// already-sorted `sorted` list of `(entity key, item)` pairs. Cursors are
/// opaque encodings of the entity key (see [`CursorKey`]); because the sort
/// always ends in an entity-key tiebreaker, resuming from a key is unambiguous —
/// so the key MUST be unique per row (use a composite `(u64, u64)` key where no
/// single column is).
pub fn paginate<T, K: CursorKey>(
    sorted: Vec<(K, T)>,
    page: Option<&models::SearchQueryPageRequest>,
) -> Page<T> {
    let total = sorted.len() as i64;

    // Per the search spec every `limit` defaults to 100 when absent and is
    // bounded to [0, 10000]: an explicit 0 is a count-only query (empty page,
    // `totalItems` retained); clamp so an oversized limit can never
    // materialize an unbounded page. The generated `limit` fields differ in
    // width across pagination variants, so accept anything convertible to u64.
    let default_limit = 100usize;
    const MAX_LIMIT: u64 = 10_000;
    fn clamp_limit<T: Into<u64>>(limit: Option<T>, default: usize) -> usize {
        match limit {
            Some(l) => (l.into().min(MAX_LIMIT)) as usize,
            None => default,
        }
    }
    let (start, limit, backward_before) = match page {
        Some(models::SearchQueryPageRequest::LimitPagination(p)) => {
            (0usize, clamp_limit(p.limit, default_limit), None)
        }
        Some(models::SearchQueryPageRequest::OffsetPagination(p)) => (
            p.from.map(|f| f as usize).unwrap_or(0),
            clamp_limit(p.limit, default_limit),
            None,
        ),
        Some(models::SearchQueryPageRequest::CursorForwardPagination(p)) => {
            let after = p.after.as_deref().and_then(K::decode);
            let start = after
                .and_then(|k| sorted.iter().position(|(key, _)| *key == k).map(|i| i + 1))
                .unwrap_or(0);
            (start, clamp_limit(p.limit, default_limit), None)
        }
        Some(models::SearchQueryPageRequest::CursorBackwardPagination(p)) => {
            let before = p.before.as_deref().and_then(K::decode);
            (0usize, clamp_limit(p.limit, default_limit), before)
        }
        None => (0usize, default_limit, None),
    };

    let window: Vec<(K, T)> = if let Some(before_key) = backward_before {
        // Backward paging: take the `limit` items immediately preceding the
        // cursor (keeping ascending order within the page).
        let end = sorted
            .iter()
            .position(|(key, _)| *key == before_key)
            .unwrap_or(0);
        let begin = end.saturating_sub(limit);
        sorted.into_iter().take(end).skip(begin).collect()
    } else {
        sorted.into_iter().skip(start).take(limit).collect()
    };

    let start_cursor = window
        .first()
        .map(|(k, _)| types::Nullable::Present(k.encode()))
        .unwrap_or(types::Nullable::Null);
    let end_cursor = window
        .last()
        .map(|(k, _)| types::Nullable::Present(k.encode()))
        .unwrap_or(types::Nullable::Null);

    Page {
        items: window.into_iter().map(|(_, item)| item).collect(),
        response: models::SearchQueryPageResponse {
            total_items: total,
            has_more_total_items: false,
            start_cursor,
            end_cursor,
        },
    }
}

/// Encodes an entity key as an opaque page cursor: nine big-endian bytes (a
/// leading zero plus the `u64`) in standard base64 without padding. Nine bytes
/// yield exactly twelve base64 characters, which satisfies the spec's cursor
/// charset (no `=` padding, length a multiple of four).
pub fn encode_cursor(key: u64) -> String {
    let mut bytes = [0u8; 9];
    bytes[1..].copy_from_slice(&key.to_be_bytes());
    base64_encode(&bytes)
}

/// Decodes a cursor produced by [`encode_cursor`] back into an entity key,
/// returning `None` if it is malformed.
pub fn decode_cursor(cursor: &str) -> Option<u64> {
    let bytes = base64_decode(cursor)?;
    if bytes.len() != 9 {
        return None;
    }
    let mut key = [0u8; 8];
    key.copy_from_slice(&bytes[1..]);
    Some(u64::from_be_bytes(key))
}

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Standard base64 without padding. Inputs are always nine bytes, so the output
/// is a clean multiple of four characters.
fn base64_encode(input: &[u8]) -> String {
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as usize;
        let b1 = chunk.get(1).copied().unwrap_or(0) as usize;
        let b2 = chunk.get(2).copied().unwrap_or(0) as usize;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(B64[(n >> 18) & 0x3f] as char);
        out.push(B64[(n >> 12) & 0x3f] as char);
        if chunk.len() > 1 {
            out.push(B64[(n >> 6) & 0x3f] as char);
        }
        if chunk.len() > 2 {
            out.push(B64[n & 0x3f] as char);
        }
    }
    out
}

/// Standard base64 decode (no padding expected), returning `None` on any invalid
/// character or length.
fn base64_decode(input: &str) -> Option<Vec<u8>> {
    fn val(c: u8) -> Option<u32> {
        match c {
            b'A'..=b'Z' => Some((c - b'A') as u32),
            b'a'..=b'z' => Some((c - b'a' + 26) as u32),
            b'0'..=b'9' => Some((c - b'0' + 52) as u32),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    for chunk in bytes.chunks(4) {
        if chunk.len() < 2 {
            return None;
        }
        let mut n = 0u32;
        for &c in chunk {
            n = (n << 6) | val(c)?;
        }
        // Left-align when the final chunk is short.
        n <<= 6 * (4 - chunk.len());
        out.push((n >> 16) as u8);
        if chunk.len() > 2 {
            out.push((n >> 8) as u8);
        }
        if chunk.len() > 3 {
            out.push(n as u8);
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_round_trips_and_is_padding_free() {
        for key in [0u64, 1, 42, 9, u64::MAX, 1_700_000_000_000] {
            let c = encode_cursor(key);
            assert_eq!(c.len(), 12, "cursor must be 12 chars: {c}");
            assert!(!c.contains('='), "cursor must be padding-free: {c}");
            assert!(
                c.chars()
                    .all(|ch| ch.is_ascii_alphanumeric() || ch == '+' || ch == '/'),
                "cursor charset: {c}"
            );
            assert_eq!(decode_cursor(&c), Some(key));
        }
        assert_eq!(decode_cursor("not base64!!"), None);
    }

    #[test]
    fn paginate_defaults_to_100_and_clamps_limit() {
        let rows: Vec<(u64, u64)> = (0..500).map(|k| (k, k)).collect();

        // No page request -> spec default of 100, full total reported.
        let p = paginate(rows.clone(), None);
        assert_eq!(p.items.len(), 100);
        assert_eq!(p.response.total_items, 500);

        // Explicit small limit is honored.
        let req = models::SearchQueryPageRequest::LimitPagination(models::LimitPagination {
            limit: Some(10),
        });
        let p = paginate(rows.clone(), Some(&req));
        assert_eq!(p.items.len(), 10);
        assert_eq!(p.response.total_items, 500);

        // Oversized limit clamps to MAX_LIMIT (10000); only 500 rows exist.
        let big: Vec<(u64, u64)> = (0..20_000).map(|k| (k, k)).collect();
        let req = models::SearchQueryPageRequest::LimitPagination(models::LimitPagination {
            limit: Some(u16::MAX),
        });
        let p = paginate(big, Some(&req));
        assert_eq!(p.items.len(), 10_000);
        assert_eq!(p.response.total_items, 20_000);
    }

    #[test]
    fn paginate_honours_an_explicit_zero_limit_in_every_variant() {
        // The 8.10 spec lowered every `limit` minimum to 0: an explicit zero is
        // a count-only query (empty page, `totalItems` retained), not a request
        // for the default or a single item. Only an absent limit defaults.
        let rows: Vec<(u64, u64)> = (0..5).map(|k| (k, k)).collect();
        let variants = [
            models::SearchQueryPageRequest::LimitPagination(models::LimitPagination {
                limit: Some(0),
            }),
            models::SearchQueryPageRequest::OffsetPagination(models::OffsetPagination {
                from: Some(1),
                limit: Some(0),
            }),
            models::SearchQueryPageRequest::CursorForwardPagination(
                models::CursorForwardPagination {
                    after: Some(encode_cursor(1)),
                    limit: Some(0),
                },
            ),
            models::SearchQueryPageRequest::CursorBackwardPagination(
                models::CursorBackwardPagination {
                    before: Some(encode_cursor(4)),
                    limit: Some(0),
                },
            ),
        ];
        for req in &variants {
            let p = paginate(rows.clone(), Some(req));
            assert!(p.items.is_empty(), "limit 0 yields no items: {req:?}");
            assert_eq!(p.response.total_items, 5, "total retained: {req:?}");
            assert_eq!(p.response.start_cursor, types::Nullable::Null);
            assert_eq!(p.response.end_cursor, types::Nullable::Null);
        }
    }

    #[test]
    fn composite_cursor_round_trips_and_pages_unambiguously() {
        // A composite (u64, u64) cursor encodes to a spec-conformant 24-char,
        // padding-free base64 string and round-trips.
        for key in [(0u64, 0u64), (1, 2), (u64::MAX, 0), (7, u64::MAX), (42, 99)] {
            let c = <(u64, u64) as CursorKey>::encode(key);
            assert_eq!(c.len(), 24, "composite cursor must be 24 chars: {c}");
            assert!(!c.contains('='), "cursor must be padding-free: {c}");
            assert!(
                c.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'+' || b == b'/'),
                "cursor charset: {c}"
            );
            assert_eq!(<(u64, u64) as CursorKey>::decode(&c), Some(key));
        }
        assert_eq!(<(u64, u64) as CursorKey>::decode("short"), None);
        // A well-formed 18-byte cursor whose fixed leading bytes aren't zero is
        // rejected, so there is exactly one canonical encoding per key.
        let mut noncanonical = [0u8; 18];
        noncanonical[0] = 1;
        assert_eq!(
            <(u64, u64) as CursorKey>::decode(&base64_encode(&noncanonical)),
            None
        );

        // Rows that share a first key component must page without skip/dup: three
        // rows share message_key 5, distinguished only by the second component.
        let rows: Vec<((u64, u64), u64)> =
            vec![((5, 1), 10), ((5, 2), 20), ((5, 3), 30), ((6, 1), 40)];
        // Resume after the middle duplicate (5, 2): must yield exactly (5,3),(6,1).
        let after = <(u64, u64) as CursorKey>::encode((5, 2));
        let req = models::SearchQueryPageRequest::CursorForwardPagination(
            models::CursorForwardPagination {
                after: Some(after),
                limit: Some(10),
            },
        );
        let p = paginate(rows, Some(&req));
        assert_eq!(p.items, vec![30, 40], "resume must not skip or duplicate");
    }

    #[test]
    fn like_matches_wildcards() {
        assert!(like_matches("order*", "order-123"));
        assert!(like_matches("*123", "order-123"));
        assert!(like_matches("order-???", "order-123"));
        assert!(like_matches("*", "anything"));
        assert!(!like_matches("order-?", "order-12"));
        assert!(!like_matches("paid", "unpaid"));
        // Escaped metacharacters match literally.
        assert!(like_matches(r"a\*b", "a*b"));
        assert!(!like_matches(r"a\*b", "axb"));
    }

    #[test]
    fn ops_eq_in_and_exists() {
        let o = Ops {
            in_: Some(vec!["A".into(), "B".into()]),
            ..Default::default()
        };
        assert!(o.matches(Some("A")));
        assert!(!o.matches(Some("C")));

        let exists_false = Ops {
            exists: Some(false),
            ..Default::default()
        };
        assert!(exists_false.matches(None));
        assert!(!exists_false.matches(Some("x")));
    }

    #[test]
    fn match_job_key_opt_respects_absence() {
        // `$exists: false` must match a record with no job key (passed as None),
        // and reject one that has a key.
        let exists_false = Some(models::JobKeyFilterProperty::AdvancedJobKeyFilter(
            models::AdvancedJobKeyFilter {
                dollar_exists: Some(false),
                ..models::AdvancedJobKeyFilter::new()
            },
        ));
        assert!(match_job_key_opt(&exists_false, None));
        assert!(!match_job_key_opt(&exists_false, Some("42")));

        // `$exists: true` is the mirror image.
        let exists_true = Some(models::JobKeyFilterProperty::AdvancedJobKeyFilter(
            models::AdvancedJobKeyFilter {
                dollar_exists: Some(true),
                ..models::AdvancedJobKeyFilter::new()
            },
        ));
        assert!(match_job_key_opt(&exists_true, Some("42")));
        assert!(!match_job_key_opt(&exists_true, None));

        // A bare key filter never matches an absent job key.
        let bare = Some(models::JobKeyFilterProperty::JobKey(models::JobKey(
            "42".to_string(),
        )));
        assert!(match_job_key_opt(&bare, Some("42")));
        assert!(!match_job_key_opt(&bare, None));
        // No filter matches anything, present or absent.
        assert!(match_job_key_opt(&None, None));
    }
}
