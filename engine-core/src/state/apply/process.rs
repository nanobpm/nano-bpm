//! Deploy-family appliers: deployment + process-definition records.

use crate::event::Event;
use crate::state::types::*;

pub(super) fn apply_process(state: &mut State, event: &Event) {
    match event {
        Event::DeploymentCreated { .. } => {
            // No-op today. The event exists so every deploy has a persisted
            // deployment key (issue #47, Option B) — the key counter is
            // advanced via mint_key() at emit time, and replay derives it
            // from the max key in the log, so simply having the event in
            // the journal is enough. Applier promotion to record deployment
            // metadata (resource keys, timestamp, audit hooks) is Option A.
        }

        Event::ProcessDeployed {
            deployment_key,
            process_definition_key,
            version,
            process,
            ..
        } => {
            let mut definition = process.clone();
            definition.normalize_legacy_agent_tasks();
            let deployed = DeployedProcess {
                key: *process_definition_key,
                version: *version,
                deployment_key: *deployment_key,
                definition,
            };
            // Retain every version, keyed by its unique definition key, so a
            // running instance can always resolve the version it was created on.
            state
                .process_versions
                .insert(*process_definition_key, deployed.clone());
            // Maintain the latest-by-id index. In sequential replay versions
            // strictly increase, so the last applied wins; the `>=` guard makes
            // out-of-order snapshot merge (see `install_deployment_if_newer`)
            // monotonic — an older surviving durable copy never regresses the
            // latest pointer.
            let is_latest = state
                .processes
                .get(&process.id)
                .map(|existing| *version >= existing.version)
                .unwrap_or(true);
            if is_latest {
                state.processes.insert(process.id.clone(), deployed);
            }
        }
        _ => unreachable!(),
    }
}
