//! Resource-family appliers: decision/form/generic-resource deploys and decision evaluation.

use crate::event::Event;
use crate::state::types::*;

pub(super) fn apply_resource(state: &mut State, event: &Event) {
    match event {
        Event::DecisionRequirementsDeployed {
            decision_requirements_key,
            version,
            drg,
            ..
        } => {
            let deployed = DeployedDrg {
                key: *decision_requirements_key,
                version: *version,
                drg: drg.clone(),
            };
            // Retain every version by its unique key so a decision that
            // references an older DRG (see the DecisionDeployed arm) resolves.
            state
                .decision_requirements_versions
                .insert(*decision_requirements_key, deployed.clone());
            // Maintain the latest-by-id index; the `>=` guard keeps an
            // out-of-order durable copy from regressing the latest pointer.
            let is_latest = state
                .decision_requirements
                .get(&drg.id)
                .map(|existing| *version >= existing.version)
                .unwrap_or(true);
            if is_latest {
                state.decision_requirements.insert(drg.id.clone(), deployed);
            }
        }

        Event::DecisionDeployed {
            decision_requirements_key,
            decision_key,
            decision_id,
            decision_name,
            version,
            ..
        } => {
            // The DRG carrying this decision was applied by the preceding
            // DecisionRequirementsDeployed event; look it up by its exact key in
            // the version-retention map (the latest-by-id index may already point
            // at a newer DRG) to bind for eval.
            if let Some(drg) = state
                .decision_requirements_versions
                .get(decision_requirements_key)
                .map(|d| d.drg.clone())
            {
                let deployed = DeployedDecision {
                    key: *decision_key,
                    version: *version,
                    decision_requirements_key: *decision_requirements_key,
                    decision_id: decision_id.clone(),
                    decision_name: decision_name.clone(),
                    drg,
                };
                // Retain every version by its unique key so an EvaluateDecision
                // pinned to an older decision key still resolves.
                state
                    .decision_versions
                    .insert(*decision_key, deployed.clone());
                let is_latest = state
                    .decisions
                    .get(decision_id)
                    .map(|existing| *version >= existing.version)
                    .unwrap_or(true);
                if is_latest {
                    state.decisions.insert(decision_id.clone(), deployed);
                }
            }
        }

        Event::FormDeployed {
            deployment_key,
            form_key,
            version,
            form_id,
            resource_name,
            schema,
            version_tag,
        } => {
            let deployed = DeployedForm {
                key: *form_key,
                version: *version,
                form_id: form_id.clone(),
                resource_name: resource_name.clone(),
                schema: schema.clone(),
                deployment_key: *deployment_key,
                version_tag: version_tag.clone(),
            };
            // Retain every version by its unique key so a form binding pinned to
            // a specific version resolves.
            state.form_versions.insert(*form_key, deployed.clone());
            let is_latest = state
                .forms
                .get(form_id)
                .map(|existing| *version >= existing.version)
                .unwrap_or(true);
            if is_latest {
                state.forms.insert(form_id.clone(), deployed);
            }
        }

        Event::GenericResourceDeployed {
            deployment_key,
            resource_key,
            version,
            resource_id,
            resource_name,
            content,
            version_tag,
        } => {
            let deployed = DeployedResource {
                key: *resource_key,
                version: *version,
                resource_id: resource_id.clone(),
                resource_name: resource_name.clone(),
                content: content.clone(),
                deployment_key: *deployment_key,
                version_tag: version_tag.clone(),
            };
            // Retain every version by its unique key so a linked-resource binding
            // pinned to a specific version (or an older key) resolves.
            state
                .resource_versions
                .insert(*resource_key, deployed.clone());
            let is_latest = state
                .resources
                .get(resource_id)
                .map(|existing| *version >= existing.version)
                .unwrap_or(true);
            if is_latest {
                state.resources.insert(resource_id.clone(), deployed);
            }
        }

        Event::DecisionEvaluated { .. } => {
            // Informational/audit only: the decision output is propagated to the
            // instance via a separate VariablesUpdated event, and the record is
            // surfaced to the exporter. No core state to mutate.
        }

        Event::DecisionInstanceDeleted { .. } => {
            // Audit/projection-only: the read model deletes the retracted decision
            // instance rows. No core engine state to mutate.
        }

        Event::ProcessInstanceHistoryDeleted { instance_key } => {
            // Event-sourced deletion (#1948): remove the ended tree — the root
            // plus every call-activity descendant still RESIDENT on this
            // replica — from engine state, so no snapshot carries it and a
            // reseed from engine state cannot resurrect it. Residency is
            // replica-local, so the tree is resolved here at apply (a local
            // state effect), never journaled. Idempotent for absent keys.
            let victims = state.resident_instance_tree(*instance_key);
            state.purge_instance_tree(&victims);
        }

        // Cluster variables (#1657): replicated configuration state, folded
        // from the log so every replica and every replay reads the same set.
        Event::ClusterVariableCreated {
            tenant_id,
            name,
            value,
            kind,
            metadata,
        } => state.cluster_variables.insert(
            tenant_id.as_deref(),
            name,
            value.clone(),
            *kind,
            metadata.clone(),
        ),
        Event::ClusterVariableUpdated {
            tenant_id,
            name,
            value,
            metadata,
            // The kind is preserved by an update (it is fixed at creation);
            // the event carries it for the response/fan-out projection, but
            // the state fold leaves the stored kind untouched.
            kind: _,
        } => state.cluster_variables.update(
            tenant_id.as_deref(),
            name,
            value.clone(),
            metadata.clone(),
        ),
        Event::ClusterVariableDeleted { tenant_id, name } => {
            state.cluster_variables.remove(tenant_id.as_deref(), name)
        }
        _ => unreachable!(),
    }
}
