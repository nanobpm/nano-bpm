//! Shared engine-row → REST-model value conversions.

/// Builds a `chrono` UTC datetime from a `_ms` epoch stamp, clamping to the epoch
/// on the (impossible) overflow so the mapper is total. A `u64` past `i64::MAX`
/// would wrap to a negative instant under an `as` cast, so the conversion is
/// checked (`i64::try_from`) and falls back to the epoch out of range.
pub fn ms_to_datetime(ms: u64) -> chrono::DateTime<chrono::Utc> {
    i64::try_from(ms)
        .ok()
        .and_then(chrono::DateTime::<chrono::Utc>::from_timestamp_millis)
        .unwrap_or_else(epoch)
}

/// A placeholder timestamp. The clock-free engine does not record wall-clock
/// times for process instances, so their read projections report the Unix
/// epoch. (Incidents do carry a real `created_at` fed in at command time.)
#[allow(
    clippy::expect_used,
    reason = "infallible by construction: from_timestamp(0, 0) is the Unix epoch, always a valid DateTime"
)]
pub fn epoch() -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::<chrono::Utc>::from_timestamp(0, 0).expect("epoch is valid")
}

/// A row's tenant id, defensively mapping an unset/empty tenant to the engine's
/// canonical default tenant ([`nanobpmn_engine_core::DEFAULT_TENANT`], the literal
/// `<default>`) the REST surface reports elsewhere. Engine rows normally already
/// carry `DEFAULT_TENANT`; this only backfills a row that somehow lacks one.
pub fn tenant_or_default(tenant_id: &str) -> String {
    if tenant_id.is_empty() {
        nanobpmn_engine_core::DEFAULT_TENANT.to_string()
    } else {
        tenant_id.to_string()
    }
}
