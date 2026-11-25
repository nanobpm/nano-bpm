//! The read-model row source a search pipeline reads from.
//!
//! The gateway answers from a sharded `nano-server-storage::ReadModel` (rows
//! concatenated across partitions); `engine-wasm` answers from its single
//! in-memory [`ReadStore`]. Both expose the same row queries, abstracted here so
//! every pipeline in this crate has ONE implementation over either store.

use nanobpmn_engine_core::Key;
use nanobpmn_read_model::{
    ElementInstanceRow, FormRow, IncidentRow, JobRow, ProcessInstanceRow, ReadStore,
    ResourceMetaRow, UserTaskRow, VariablePushdown, VariableRow,
};

/// The read-model queries the shared search/get pipelines need.
pub trait ReadSource {
    fn process_instances(&self) -> Vec<ProcessInstanceRow>;
    fn process_instance(&self, key: Key) -> Option<ProcessInstanceRow>;
    fn element_instances(&self) -> Vec<ElementInstanceRow>;
    fn jobs(&self) -> Vec<JobRow>;
    fn incidents(&self) -> Vec<IncidentRow>;
    fn user_tasks(&self) -> Vec<UserTaskRow>;
    fn variables_matching(&self, pushdown: &VariablePushdown) -> Vec<VariableRow>;
    fn form_by_key(&self, key: Key) -> Option<FormRow>;
    fn resource_by_key_meta(&self, key: Key) -> Option<ResourceMetaRow>;
}

/// Implements [`ReadSource`] for `$ty` by delegating each query to the
/// identically named inherent method of `$inner` (an expression over `$s`, the
/// `&self` binding). Exported so the gateway can implement it for a local
/// newtype over its sharded read model without restating the method list.
#[macro_export]
macro_rules! impl_read_source {
    ($ty:ty, |$s:ident| $inner:expr) => {
        impl $crate::source::ReadSource for $ty {
            fn process_instances(&self) -> Vec<$crate::readstore::ProcessInstanceRow> {
                let $s = self;
                $inner.process_instances()
            }
            fn process_instance(&self, key: u64) -> Option<$crate::readstore::ProcessInstanceRow> {
                let $s = self;
                $inner.process_instance(key)
            }
            fn element_instances(&self) -> Vec<$crate::readstore::ElementInstanceRow> {
                let $s = self;
                $inner.element_instances()
            }
            fn jobs(&self) -> Vec<$crate::readstore::JobRow> {
                let $s = self;
                $inner.jobs()
            }
            fn incidents(&self) -> Vec<$crate::readstore::IncidentRow> {
                let $s = self;
                $inner.incidents()
            }
            fn user_tasks(&self) -> Vec<$crate::readstore::UserTaskRow> {
                let $s = self;
                $inner.user_tasks()
            }
            fn variables_matching(
                &self,
                pushdown: &$crate::readstore::VariablePushdown,
            ) -> Vec<$crate::readstore::VariableRow> {
                let $s = self;
                $inner.variables_matching(pushdown)
            }
            fn form_by_key(&self, key: u64) -> Option<$crate::readstore::FormRow> {
                let $s = self;
                $inner.form_by_key(key)
            }
            fn resource_by_key_meta(&self, key: u64) -> Option<$crate::readstore::ResourceMetaRow> {
                let $s = self;
                $inner.resource_by_key_meta(key)
            }
        }
    };
}

impl_read_source!(ReadStore, |s| s);
