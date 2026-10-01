//! Implements the generated `nanobpm-console-api` per-tag `Api` traits for
//! [`ServerImpl`] by delegating to the hand-written console handler logic in
//! [`super`]. The generated router (`nanobpm_console_api::server::new`) is
//! mounted alongside the reduced hand-written router in `main.rs`.
//!
//! ## Strategy
//! The OpenAPI spec was authored *from* the hand-written DTOs, so the generated
//! models serialize to identical JSON. Each trait method therefore delegates to
//! a `nano_server_console::` core function (which returns a DTO or a `serde_json::Value`) and
//! round-trips through `serde_json` into the generated response model.
//!
//! ## Deviations from the hand-written handlers
//! * **RunStatus `crashed` → `error`**: the project `Phase` enum serializes
//!   `crashed`, but the spec's `RunStatus` has no such variant (it uses
//!   `error`). [`fix_run_status`] remaps it on any `RunState`-bearing value
//!   (identified by the presence of a `compiling` key) before conversion.
//! * **Collapsed status codes**: the spec exposes fewer status codes than some
//!   handlers produce. Where a handler's error status has no matching response
//!   variant it is mapped to the nearest available one (e.g. `500`/`400` on
//!   `GetModel` → `404`; list endpoints that only declare `200` fall back to an
//!   empty/default `200` body).

use async_trait::async_trait;
use axum_extra::extract::CookieJar;
use headers::Host;
use http::Method;
use nanobpm_console_api::apis;
use nanobpm_console_api::models;
use nanobpm_console_api::types::Nullable;

use crate::ServerImpl;

// --- conversion helpers ---------------------------------------------------

/// Serialize a hand-written DTO and deserialize it into the generated model.
/// Safe because the spec's JSON property names/shapes match the DTOs.
fn from_dto<T, D>(dto: D) -> T
where
    D: serde::Serialize,
    T: serde::de::DeserializeOwned,
{
    serde_json::from_value(serde_json::to_value(dto).expect("dto serializes"))
        .expect("generated model matches DTO shape")
}

/// Deserialize a `serde_json::Value` (already the response body shape) into the
/// generated model.
fn from_val<T: serde::de::DeserializeOwned>(v: serde_json::Value) -> T {
    serde_json::from_value(v).expect("generated model matches value shape")
}

/// Remap the project run `status` `crashed` → `error` on every `RunState`
/// object (identified by the `compiling` discriminator key) nested in `v`.
fn fix_run_status(v: &mut serde_json::Value) {
    match v {
        serde_json::Value::Object(map) => {
            if map.contains_key("compiling")
                && let Some(serde_json::Value::String(s)) = map.get_mut("status")
                && s == "crashed"
            {
                *s = "error".to_string();
            }
            for child in map.values_mut() {
                fix_run_status(child);
            }
        }
        serde_json::Value::Array(arr) => {
            for child in arr.iter_mut() {
                fix_run_status(child);
            }
        }
        _ => {}
    }
}

/// Flatten an optional nullable request field to a plain `Option`.
fn flatten_nullable<T: Clone>(v: &Option<Nullable<T>>) -> Option<T> {
    match v {
        Some(Nullable::Present(x)) => Some(x.clone()),
        _ => None,
    }
}

// --- error handler --------------------------------------------------------

#[async_trait]
impl apis::ErrorHandler<()> for ServerImpl {}

// --- cluster --------------------------------------------------------------

#[async_trait]
impl apis::cluster::Cluster for ServerImpl {
    async fn get_cluster_health(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
    ) -> Result<apis::cluster::GetClusterHealthResponse, ()> {
        let dto = nano_server_console::cluster_health(self).await;
        Ok(apis::cluster::GetClusterHealthResponse::Status200_ClusterHealth(from_dto(dto)))
    }

    async fn get_cluster_metrics(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
    ) -> Result<apis::cluster::GetClusterMetricsResponse, ()> {
        let dto = nano_server_console::cluster_metrics(self).await;
        Ok(apis::cluster::GetClusterMetricsResponse::Status200_ClusterMetrics(from_dto(dto)))
    }

    async fn get_metrics(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
    ) -> Result<apis::cluster::GetMetricsResponse, ()> {
        let dto = nano_server_console::build_local_metrics(self);
        Ok(apis::cluster::GetMetricsResponse::Status200_MetricsSnapshot(from_dto(dto)))
    }

    async fn get_topology(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
    ) -> Result<apis::cluster::GetTopologyResponse, ()> {
        let dto = nano_server_console::topology(self);
        Ok(apis::cluster::GetTopologyResponse::Status200_ClusterTopology(from_dto(dto)))
    }
}

// --- config ---------------------------------------------------------------

#[async_trait]
impl apis::config::Config for ServerImpl {
    async fn get_ide_config(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
    ) -> Result<apis::config::GetIdeConfigResponse, ()> {
        // Only 200 is declared; the sole error path is a task-join failure, so
        // fall back to computing the config on the current thread.
        let v = nano_server_console::config_ide()
            .await
            .unwrap_or_else(|_| nano_server_console::config::ide_config_json());
        Ok(apis::config::GetIdeConfigResponse::Status200_IDEConfig(
            from_val(v),
        ))
    }

    async fn get_server_config(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
    ) -> Result<apis::config::GetServerConfigResponse, ()> {
        let v = nano_server_console::config_server(self);
        Ok(apis::config::GetServerConfigResponse::Status200_ServerConfig(from_val(v)))
    }

    async fn set_sla_mode(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        body: &models::SetSlaRequest,
    ) -> Result<apis::config::SetSlaModeResponse, ()> {
        match nano_server_console::config_server_sla(self, &body.mode).await {
            Ok(v) => {
                Ok(apis::config::SetSlaModeResponse::Status200_UpdatedServerConfig(from_val(v)))
            }
            Err((_, msg)) => Ok(apis::config::SetSlaModeResponse::Status400_InvalidRequest(
                msg,
            )),
        }
    }
}

// --- extensions -----------------------------------------------------------

#[async_trait]
impl apis::server::Server for ServerImpl {
    async fn get_server_update(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
    ) -> Result<apis::server::GetServerUpdateResponse, ()> {
        // Only 200 is declared; the handler is offline-soft and never errors.
        let v = nano_server_console::server_update()
            .await
            .unwrap_or_else(|_| serde_json::json!({}));
        Ok(apis::server::GetServerUpdateResponse::Status200_ServerUpdateStatus(from_val(v)))
    }
}

#[async_trait]
impl apis::extensions::Extensions for ServerImpl {
    async fn get_extensions(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
    ) -> Result<apis::extensions::GetExtensionsResponse, ()> {
        let v = nano_server_console::extensions_list();
        Ok(apis::extensions::GetExtensionsResponse::Status200_ExtensionsOverview(from_val(v)))
    }

    async fn get_marketplace(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
    ) -> Result<apis::extensions::GetMarketplaceResponse, ()> {
        // Only 200 is declared; on a registry error fall back to empty entries.
        let v = nano_server_console::extensions_marketplace()
            .await
            .unwrap_or_else(|_| serde_json::json!({ "entries": [] }));
        Ok(apis::extensions::GetMarketplaceResponse::Status200_MarketplaceListing(from_val(v)))
    }

    async fn get_extension_readme(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        query_params: &models::GetExtensionReadmeQueryParams,
    ) -> Result<apis::extensions::GetExtensionReadmeResponse, ()> {
        match nano_server_console::extensions_readme(query_params.pkg.clone()).await {
            Ok(v) => {
                Ok(apis::extensions::GetExtensionReadmeResponse::Status200_PackREADME(from_val(v)))
            }
            Err((_, msg)) => {
                Ok(apis::extensions::GetExtensionReadmeResponse::Status404_NotFound(msg))
            }
        }
    }

    async fn get_extension_changelog(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        query_params: &models::GetExtensionChangelogQueryParams,
    ) -> Result<apis::extensions::GetExtensionChangelogResponse, ()> {
        match nano_server_console::extensions_changelog(
            query_params.pkg.clone(),
            query_params.from.clone(),
            query_params.to.clone(),
        )
        .await
        {
            Ok(v) => Ok(
                apis::extensions::GetExtensionChangelogResponse::Status200_PackChangelog(from_val(
                    v,
                )),
            ),
            Err((status, msg)) => {
                use apis::extensions::GetExtensionChangelogResponse as Resp;
                if status == http::StatusCode::INTERNAL_SERVER_ERROR {
                    Ok(Resp::Status500_InternalError(msg))
                } else {
                    Ok(Resp::Status404_NotFound(msg))
                }
            }
        }
    }

    async fn install_extension(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        body: &models::ExtPkgRequest,
    ) -> Result<apis::extensions::InstallExtensionResponse, ()> {
        match nano_server_console::extensions_install(body.pkg.clone()).await {
            Ok(v) => Ok(
                apis::extensions::InstallExtensionResponse::Status201_InstalledExtension(from_val(
                    v,
                )),
            ),
            Err((_, msg)) => {
                Ok(apis::extensions::InstallExtensionResponse::Status400_InvalidRequest(msg))
            }
        }
    }

    async fn install_urban_toolkit(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
    ) -> Result<apis::extensions::InstallUrbanToolkitResponse, ()> {
        match nano_server_console::ensure_urban_toolkit().await {
            Ok(available) => Ok(
                apis::extensions::InstallUrbanToolkitResponse::Status200_UrbanToolkitAvailabilityAfterEnsuringInstallation(
                    models::UrbanToolkitStatus { available },
                ),
            ),
            Err((_, msg)) => Ok(
                apis::extensions::InstallUrbanToolkitResponse::Status400_InvalidRequest(msg),
            ),
        }
    }

    async fn remove_extension(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        body: &models::ExtPkgRequest,
    ) -> Result<apis::extensions::RemoveExtensionResponse, ()> {
        match nano_server_console::extensions_remove(&body.pkg) {
            Ok(_) => Ok(apis::extensions::RemoveExtensionResponse::Status204_Removed),
            Err((_, msg)) => {
                Ok(apis::extensions::RemoveExtensionResponse::Status400_InvalidRequest(msg))
            }
        }
    }

    async fn trust_extension(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        body: &models::TrustRequest,
    ) -> Result<apis::extensions::TrustExtensionResponse, ()> {
        let yolo = flatten_nullable(&body.yolo);
        let approve = flatten_nullable(&body.approve);
        let revoke = flatten_nullable(&body.revoke);
        // Only 200 is declared; on a save error fall back to the current view.
        let v = nano_server_console::extensions_trust(yolo, approve, revoke)
            .unwrap_or_else(|_| nano_server_console::extensions_list());
        Ok(
            apis::extensions::TrustExtensionResponse::Status200_UpdatedExtensionsOverview(
                from_val(v),
            ),
        )
    }
}

// --- instances ------------------------------------------------------------

#[async_trait]
impl apis::instances::Instances for ServerImpl {
    async fn get_instance(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::GetInstancePathParams,
    ) -> Result<apis::instances::GetInstanceResponse, ()> {
        match nano_server_console::instance_detail(self, &path_params.key).await {
            Some(dto) => {
                Ok(apis::instances::GetInstanceResponse::Status200_InstanceDetail(from_dto(dto)))
            }
            None => Ok(apis::instances::GetInstanceResponse::Status404_NotFound(
                "no such instance".to_string(),
            )),
        }
    }

    async fn list_instances(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        query_params: &models::ListInstancesQueryParams,
    ) -> Result<apis::instances::ListInstancesResponse, ()> {
        let page = query_params.page.map(|p| p as i64).unwrap_or(0);
        let page_size = query_params.page_size.map(|p| p as i64).unwrap_or(50);
        let filter = crate::readstore::InstanceFilter {
            state: query_params
                .state
                .as_deref()
                .and_then(nano_server_console::parse_instance_state_filter),
            has_incident: query_params.has_incident,
        };
        let dto = nano_server_console::instances(self, page, page_size, filter);
        Ok(
            apis::instances::ListInstancesResponse::Status200_OnePageOfProcessInstances(from_dto(
                dto,
            )),
        )
    }

    async fn resolve_incident(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::ResolveIncidentPathParams,
    ) -> Result<apis::instances::ResolveIncidentResponse, ()> {
        use apis::instances::ResolveIncidentResponse as Resp;

        use crate::ResolveIncidentOutcome as Out;

        let incident_key: u64 = match path_params.incident_key.parse() {
            Ok(k) => k,
            Err(_) => {
                return Ok(Resp::Status404_NotFound(format!(
                    "Incident key '{}' is not a valid key.",
                    path_params.incident_key
                )));
            }
        };

        // Console operator action: no operationReference (that is a client
        // idempotency token on the public v2 API). Operate-style one-click retry:
        // a JobNoRetries incident's parked job is granted a retry before the
        // shared resolve core runs, so the single button doesn't dead-end on the
        // engine's Zeebe-parity "update its retries first" guard.
        Ok(match self.resolve_incident_operator(incident_key).await {
            Out::Resolved => Resp::Status204_TheIncidentWasResolved,
            Out::NotFound(d) => Resp::Status404_NotFound(d),
            Out::NotResolvable(d) => Resp::Status409_AlreadyExists(d),
            // A transient Raft condition (handoff pause / leader election) is
            // retryable — surface it as 503, not a 500 internal fault.
            Out::Unavailable(d) => Resp::Status503_TheServiceIsCurrentlyUnavailable(d),
            Out::Internal(d) => Resp::Status500_InternalError(d),
        })
    }

    async fn cancel_instance(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::CancelInstancePathParams,
    ) -> Result<apis::instances::CancelInstanceResponse, ()> {
        use apis::instances::CancelInstanceResponse as Resp;

        use crate::CancelInstanceOutcome as Out;

        let instance_key: u64 = match path_params.key.parse() {
            Ok(k) => k,
            Err(_) => {
                return Ok(Resp::Status404_NotFound(format!(
                    "Instance key '{}' is not a valid key.",
                    path_params.key
                )));
            }
        };

        // Console operator action: reuses the same engine-command + leader-forward
        // core as `POST /v2/process-instances/{key}/cancellation`.
        Ok(match self.cancel_instance_core(instance_key).await {
            Out::Canceled => Resp::Status204_TheInstanceWasCancelled,
            Out::NotFound(d) => Resp::Status404_NotFound(d),
            Out::Unavailable(d) => Resp::Status503_TheServiceIsCurrentlyUnavailable(d),
            Out::Internal(d) => Resp::Status500_InternalError(d),
        })
    }

    async fn suspend_instance(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::SuspendInstancePathParams,
    ) -> Result<apis::instances::SuspendInstanceResponse, ()> {
        use apis::instances::SuspendInstanceResponse as Resp;

        use crate::TransitionInstanceOutcome as Out;

        let instance_key: u64 = match path_params.key.parse() {
            Ok(k) => k,
            Err(_) => {
                return Ok(Resp::Status400_InvalidRequest(format!(
                    "Instance key '{}' is not a valid key.",
                    path_params.key
                )));
            }
        };

        // Console operator action: reuses the same engine-command + leader-forward
        // core as `POST /v2/process-instances/{key}/suspension`.
        Ok(match self.suspend_instance_core(instance_key).await {
            Out::Ok => Resp::Status204_TheInstanceWasSuspended,
            Out::Conflict(d) => Resp::Status400_InvalidRequest(d),
            Out::NotFound(d) => Resp::Status404_NotFound(d),
            Out::Unavailable(d) => Resp::Status503_TheServiceIsCurrentlyUnavailable(d),
            Out::Internal(d) => Resp::Status500_InternalError(d),
        })
    }

    async fn resume_instance(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::ResumeInstancePathParams,
    ) -> Result<apis::instances::ResumeInstanceResponse, ()> {
        use apis::instances::ResumeInstanceResponse as Resp;

        use crate::TransitionInstanceOutcome as Out;

        let instance_key: u64 = match path_params.key.parse() {
            Ok(k) => k,
            Err(_) => {
                return Ok(Resp::Status400_InvalidRequest(format!(
                    "Instance key '{}' is not a valid key.",
                    path_params.key
                )));
            }
        };

        // Console operator action: reuses the same engine-command + leader-forward
        // core as `POST /v2/process-instances/{key}/resumption`.
        Ok(match self.resume_instance_core(instance_key).await {
            Out::Ok => Resp::Status204_TheInstanceWasResumed,
            Out::Conflict(d) => Resp::Status400_InvalidRequest(d),
            Out::NotFound(d) => Resp::Status404_NotFound(d),
            Out::Unavailable(d) => Resp::Status503_TheServiceIsCurrentlyUnavailable(d),
            Out::Internal(d) => Resp::Status500_InternalError(d),
        })
    }

    async fn set_instance_variables(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _path_params: &models::SetInstanceVariablesPathParams,
        body: &models::SetInstanceVariablesRequest,
    ) -> Result<apis::instances::SetInstanceVariablesResponse, ()> {
        use apis::instances::SetInstanceVariablesResponse as Resp;

        use crate::SetVariablesOutcome as Out;

        let scope_key: u64 = match body.scope_key.parse() {
            Ok(k) => k,
            Err(_) => {
                return Ok(Resp::Status400_InvalidRequest(format!(
                    "Scope key '{}' is not a valid key.",
                    body.scope_key
                )));
            }
        };

        // The generated `types::Object` wraps the raw JSON value in `.0`; forward
        // it verbatim so the engine (or a peer) re-derives identical values.
        let variables: serde_json::Map<String, serde_json::Value> = body
            .variables
            .iter()
            .map(|(name, obj)| (name.clone(), obj.0.clone()))
            .collect();
        let local = body.local.unwrap_or(false);

        Ok(
            match self.set_variables_core(scope_key, variables, local).await {
                Out::Updated => Resp::Status204_TheVariablesWereMerged,
                Out::ScopeNotFound(d) => Resp::Status404_NotFound(d),
                Out::Unavailable(d) => Resp::Status503_TheServiceIsCurrentlyUnavailable(d),
                Out::Internal(d) => Resp::Status500_InternalError(d),
            },
        )
    }
}

// --- traces ---------------------------------------------------------------

#[async_trait]
impl apis::traces::Traces for ServerImpl {
    async fn get_trace(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::GetTracePathParams,
    ) -> Result<apis::traces::GetTraceResponse, ()> {
        match nano_server_console::trace_detail(self, &path_params.key) {
            Some(dto) => Ok(apis::traces::GetTraceResponse::Status200_InstanceTrace(
                from_dto(dto),
            )),
            None => Ok(apis::traces::GetTraceResponse::Status404_NotFound(
                "no such trace".to_string(),
            )),
        }
    }

    async fn get_trace_otel(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::GetTraceOtelPathParams,
    ) -> Result<apis::traces::GetTraceOtelResponse, ()> {
        match nano_server_console::trace_otel(self, &path_params.key) {
            Some(v) => Ok(apis::traces::GetTraceOtelResponse::Status200_OpaqueOTLP(
                from_val(v),
            )),
            None => Ok(apis::traces::GetTraceOtelResponse::Status404_NotFound(
                "no such trace".to_string(),
            )),
        }
    }

    async fn list_traces(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        query_params: &models::ListTracesQueryParams,
    ) -> Result<apis::traces::ListTracesResponse, ()> {
        let limit = query_params.limit.map(|l| l as usize).unwrap_or(100);
        let dtos = nano_server_console::traces(self, limit);
        Ok(apis::traces::ListTracesResponse::Status200_TraceSummaries(
            from_dto(dtos),
        ))
    }

    async fn get_trace_config(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
    ) -> Result<apis::traces::GetTraceConfigResponse, ()> {
        Ok(
            apis::traces::GetTraceConfigResponse::Status200_TraceConfiguration(from_dto(
                nano_server_console::trace_config(self),
            )),
        )
    }

    async fn set_trace_config(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        body: &models::TraceConfigUpdate,
    ) -> Result<apis::traces::SetTraceConfigResponse, ()> {
        let variables = flatten_nullable(&body.capture_variables);
        let stimuli = flatten_nullable(&body.capture_stimuli);
        let dto = nano_server_console::set_trace_config(self, variables, stimuli);
        Ok(
            apis::traces::SetTraceConfigResponse::Status200_UpdatedTraceConfiguration(from_dto(
                dto,
            )),
        )
    }
}

// --- models ---------------------------------------------------------------

#[async_trait]
impl apis::models::Models for ServerImpl {
    async fn create_model(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        body: &models::CreateModelRequest,
    ) -> Result<apis::models::CreateModelResponse, ()> {
        match nano_server_console::model_create(self, body.name.clone(), body.xml.clone()) {
            Ok(v) => Ok(apis::models::CreateModelResponse::Status201_ModelCreated(
                from_val(v),
            )),
            Err((code, msg)) if code == http::StatusCode::CONFLICT => Ok(
                apis::models::CreateModelResponse::Status409_AlreadyExists(msg),
            ),
            Err((_, msg)) => Ok(apis::models::CreateModelResponse::Status400_InvalidRequest(
                msg,
            )),
        }
    }

    async fn delete_model(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::DeleteModelPathParams,
    ) -> Result<apis::models::DeleteModelResponse, ()> {
        match nano_server_console::model_delete(&path_params.name) {
            Ok(_) => Ok(apis::models::DeleteModelResponse::Status204_Deleted),
            Err((_, msg)) => Ok(apis::models::DeleteModelResponse::Status404_NotFound(msg)),
        }
    }

    async fn get_model(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::GetModelPathParams,
    ) -> Result<apis::models::GetModelResponse, ()> {
        match nano_server_console::model_get(self, &path_params.name) {
            Ok(v) => Ok(apis::models::GetModelResponse::Status200_Model(from_val(v))),
            Err((_, msg)) => Ok(apis::models::GetModelResponse::Status404_NotFound(msg)),
        }
    }

    async fn list_models(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
    ) -> Result<apis::models::ListModelsResponse, ()> {
        // Only 200 is declared; on a workspace error fall back to empty.
        let v = nano_server_console::models(self).unwrap_or_else(|_| serde_json::json!([]));
        Ok(apis::models::ListModelsResponse::Status200_ModelSummaries(
            from_val(v),
        ))
    }

    async fn save_model(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::SaveModelPathParams,
        body: &String,
    ) -> Result<apis::models::SaveModelResponse, ()> {
        match nano_server_console::model_save(self, &path_params.name, body.clone()) {
            Ok(v) => Ok(apis::models::SaveModelResponse::Status200_SavedModel(
                from_val(v),
            )),
            Err((_, msg)) => Ok(apis::models::SaveModelResponse::Status400_InvalidRequest(
                msg,
            )),
        }
    }
}

// --- lib ------------------------------------------------------------------

#[async_trait]
impl apis::lib::Lib for ServerImpl {
    async fn create_lib_file(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        body: &models::CreateFileRequest,
    ) -> Result<apis::lib::CreateLibFileResponse, ()> {
        match nano_server_console::lib_file_create(&body.path) {
            Ok(_) => Ok(apis::lib::CreateLibFileResponse::Status201_Created),
            Err((code, msg)) if code == http::StatusCode::CONFLICT => Ok(
                apis::lib::CreateLibFileResponse::Status409_AlreadyExists(msg),
            ),
            Err((_, msg)) => Ok(apis::lib::CreateLibFileResponse::Status400_InvalidRequest(
                msg,
            )),
        }
    }

    async fn delete_lib_file(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        query_params: &models::DeleteLibFileQueryParams,
    ) -> Result<apis::lib::DeleteLibFileResponse, ()> {
        match nano_server_console::lib_file_delete(&query_params.path) {
            Ok(_) => Ok(apis::lib::DeleteLibFileResponse::Status204_Deleted),
            Err((_, msg)) => Ok(apis::lib::DeleteLibFileResponse::Status400_InvalidRequest(
                msg,
            )),
        }
    }

    async fn get_lib_file(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        query_params: &models::GetLibFileQueryParams,
    ) -> Result<apis::lib::GetLibFileResponse, ()> {
        match nano_server_console::lib_file_get(&query_params.path) {
            Ok(contents) => Ok(apis::lib::GetLibFileResponse::Status200_FileContents(
                contents,
            )),
            Err((code, msg)) if code == http::StatusCode::NOT_FOUND => {
                Ok(apis::lib::GetLibFileResponse::Status404_NotFound(msg))
            }
            Err((_, msg)) => Ok(apis::lib::GetLibFileResponse::Status400_InvalidRequest(msg)),
        }
    }

    async fn list_lib_files(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
    ) -> Result<apis::lib::ListLibFilesResponse, ()> {
        // Only 200 is declared; on error fall back to empty.
        let v =
            nano_server_console::lib_list().unwrap_or_else(|_| serde_json::json!({ "files": [] }));
        Ok(apis::lib::ListLibFilesResponse::Status200_LibraryFilePaths(
            from_val(v),
        ))
    }

    async fn save_lib_file(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        query_params: &models::SaveLibFileQueryParams,
        body: &String,
    ) -> Result<apis::lib::SaveLibFileResponse, ()> {
        match nano_server_console::lib_file_save(&query_params.path, body) {
            Ok(_) => Ok(apis::lib::SaveLibFileResponse::Status204_Saved),
            Err((_, msg)) => Ok(apis::lib::SaveLibFileResponse::Status400_InvalidRequest(
                msg,
            )),
        }
    }
}

// --- workers --------------------------------------------------------------

#[async_trait]
impl apis::workers::Workers for ServerImpl {
    async fn create_worker(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        body: &models::CreateWorkerRequest,
    ) -> Result<apis::workers::CreateWorkerResponse, ()> {
        let job_type = flatten_nullable(&body.job_type);
        match nano_server_console::worker_create(body.name.clone(), job_type).await {
            Ok(v) => Ok(apis::workers::CreateWorkerResponse::Status201_WorkerCreated(from_val(v))),
            Err((code, msg)) if code == http::StatusCode::CONFLICT => {
                Ok(apis::workers::CreateWorkerResponse::Status409_AlreadyExists(msg))
            }
            Err((_, msg)) => Ok(apis::workers::CreateWorkerResponse::Status400_InvalidRequest(msg)),
        }
    }

    async fn create_worker_file(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::CreateWorkerFilePathParams,
        body: &models::CreateFileRequest,
    ) -> Result<apis::workers::CreateWorkerFileResponse, ()> {
        match nano_server_console::worker_file_create(&path_params.name, &body.path) {
            Ok(_) => Ok(apis::workers::CreateWorkerFileResponse::Status201_Created),
            Err((code, msg)) if code == http::StatusCode::CONFLICT => {
                Ok(apis::workers::CreateWorkerFileResponse::Status409_AlreadyExists(msg))
            }
            Err((code, msg)) if code == http::StatusCode::NOT_FOUND => Ok(
                apis::workers::CreateWorkerFileResponse::Status404_NotFound(msg),
            ),
            Err((_, msg)) => {
                Ok(apis::workers::CreateWorkerFileResponse::Status400_InvalidRequest(msg))
            }
        }
    }

    async fn delete_worker(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::DeleteWorkerPathParams,
    ) -> Result<apis::workers::DeleteWorkerResponse, ()> {
        match nano_server_console::worker_delete(&path_params.name).await {
            Ok(_) => Ok(apis::workers::DeleteWorkerResponse::Status204_Deleted),
            Err((_, msg)) => Ok(apis::workers::DeleteWorkerResponse::Status404_NotFound(msg)),
        }
    }

    async fn delete_worker_file(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::DeleteWorkerFilePathParams,
        query_params: &models::DeleteWorkerFileQueryParams,
    ) -> Result<apis::workers::DeleteWorkerFileResponse, ()> {
        match nano_server_console::worker_file_delete(&path_params.name, &query_params.path) {
            Ok(_) => Ok(apis::workers::DeleteWorkerFileResponse::Status204_Deleted),
            Err((_, msg)) => {
                Ok(apis::workers::DeleteWorkerFileResponse::Status400_InvalidRequest(msg))
            }
        }
    }

    async fn get_deno_types(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
    ) -> Result<apis::workers::GetDenoTypesResponse, ()> {
        Ok(
            apis::workers::GetDenoTypesResponse::Status200_DenoTypeDeclarations(
                nano_server_console::deno_types_source(),
            ),
        )
    }

    async fn get_worker(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::GetWorkerPathParams,
    ) -> Result<apis::workers::GetWorkerResponse, ()> {
        match nano_server_console::worker_get(&path_params.name).await {
            Ok(v) => Ok(apis::workers::GetWorkerResponse::Status200_WorkerSummary(
                from_val(v),
            )),
            Err((_, msg)) => Ok(apis::workers::GetWorkerResponse::Status404_NotFound(msg)),
        }
    }

    async fn get_worker_file(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::GetWorkerFilePathParams,
        query_params: &models::GetWorkerFileQueryParams,
    ) -> Result<apis::workers::GetWorkerFileResponse, ()> {
        match nano_server_console::worker_file_get(&path_params.name, &query_params.path) {
            Ok(contents) => {
                Ok(apis::workers::GetWorkerFileResponse::Status200_FileContents(contents))
            }
            Err((code, msg)) if code == http::StatusCode::NOT_FOUND => Ok(
                apis::workers::GetWorkerFileResponse::Status404_NotFound(msg),
            ),
            Err((_, msg)) => {
                Ok(apis::workers::GetWorkerFileResponse::Status400_InvalidRequest(msg))
            }
        }
    }

    async fn get_worker_sdk(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
    ) -> Result<apis::workers::GetWorkerSdkResponse, ()> {
        Ok(
            apis::workers::GetWorkerSdkResponse::Status200_WorkerSDKSource(
                nano_server_console::worker_sdk_source(),
            ),
        )
    }

    async fn list_workers(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
    ) -> Result<apis::workers::ListWorkersResponse, ()> {
        // Only 200 is declared; on a workspace error fall back to empty.
        let v = nano_server_console::workers_list()
            .await
            .unwrap_or_else(|_| serde_json::json!({ "workers": [], "denoAvailable": false }));
        Ok(
            apis::workers::ListWorkersResponse::Status200_WorkersPlusRuntimeAvailability(from_val(
                v,
            )),
        )
    }

    async fn save_worker_file(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::SaveWorkerFilePathParams,
        query_params: &models::SaveWorkerFileQueryParams,
        body: &String,
    ) -> Result<apis::workers::SaveWorkerFileResponse, ()> {
        match nano_server_console::worker_file_save(&path_params.name, &query_params.path, body) {
            Ok(_) => Ok(apis::workers::SaveWorkerFileResponse::Status204_Saved),
            Err((_, msg)) => {
                Ok(apis::workers::SaveWorkerFileResponse::Status400_InvalidRequest(msg))
            }
        }
    }

    async fn start_worker(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::StartWorkerPathParams,
    ) -> Result<apis::workers::StartWorkerResponse, ()> {
        match nano_server_console::worker_start(&path_params.name).await {
            Ok(v) => {
                Ok(apis::workers::StartWorkerResponse::Status200_WorkerRuntimeState(from_val(v)))
            }
            Err((_, msg)) => Ok(apis::workers::StartWorkerResponse::Status404_NotFound(msg)),
        }
    }

    async fn stop_worker(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::StopWorkerPathParams,
    ) -> Result<apis::workers::StopWorkerResponse, ()> {
        match nano_server_console::worker_stop(&path_params.name).await {
            Ok(v) => {
                Ok(apis::workers::StopWorkerResponse::Status200_WorkerRuntimeState(from_val(v)))
            }
            Err((_, msg)) => Ok(apis::workers::StopWorkerResponse::Status404_NotFound(msg)),
        }
    }
}

// --- projects -------------------------------------------------------------

#[async_trait]
impl apis::projects::Projects for ServerImpl {
    async fn compile_project(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::CompileProjectPathParams,
        body: &models::CompileRequest,
    ) -> Result<apis::projects::CompileProjectResponse, ()> {
        let targets = body.targets.clone().unwrap_or_default();
        match nano_server_console::project_compile(&path_params.name, targets) {
            Ok(v) => Ok(
                apis::projects::CompileProjectResponse::Status200_WhetherACompileWasStarted(
                    from_val(v),
                ),
            ),
            Err((_, msg)) => Ok(apis::projects::CompileProjectResponse::Status404_NotFound(
                msg,
            )),
        }
    }

    async fn create_project(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        body: &models::CreateProjectRequest,
    ) -> Result<apis::projects::CreateProjectResponse, ()> {
        let description = body.description.clone().unwrap_or_default();
        let template = flatten_nullable(&body.template).unwrap_or_else(|| "starter".to_string());
        let options = flatten_nullable(&body.options).unwrap_or_default();
        match nano_server_console::project_create(&body.name, &description, &template, &options) {
            Ok(v) => {
                // Code-first projects (ADR 0048): generate the initial laid-out
                // `resources/processes/*.bpmn` from the scaffolded `workflows/*.ts`
                // so a brand-new project opens with a rendered model. It needs a
                // Deno round-trip (npm fetch); both paths below are best-effort and
                // only log on failure — they never fail the create. How they run
                // differs per template: the `workflow-starter` path is fire-and-
                // forget (spawned, so the create response isn't blocked on the Deno
                // round-trip), while the pack-template path awaits (see below).
                //
                // Both post-create refresh paths key off the created config's
                // *slug* (`v["name"]`), never `body.name`: a display name like
                // "Home Heating" is scaffolded on disk under its slug
                // ("home-heating"), and `project_dir`/`generate_models` reject the
                // spaced display name with "invalid project name" — so using
                // `body.name` here would silently skip the regen for spaced names.
                let created_slug = v.get("name").and_then(|n| n.as_str()).map(str::to_string);
                if template == "workflow-starter" {
                    if let Some(project) = created_slug {
                        tokio::spawn(async move {
                            nano_server_console::regenerate_workflow_models(&project).await;
                        });
                    }
                } else if let Some(project) = created_slug {
                    // Post-create refresh (#1036): a pack-template / Urban app
                    // ships neither `node_modules/` nor the derived
                    // `nano-generated/` facade, so without this its first Run's
                    // OpenAPI request 500s with "delegate failed to load" until
                    // `npm i && urban gen` are run by hand. Run the *same* refresh
                    // the update path uses (`finalize_after_update`) so the first
                    // run is instant. Await it (mirroring the update handler) so
                    // the app is ready before the response, and use the created
                    // config's slug (not the display name). Best-effort: it guards
                    // deps/gen internally, so a non-Urban builtin starter is a
                    // cheap no-op, and a flaky install/gen only logs a warning —
                    // it never fails the create the maker already succeeded at.
                    let outcome =
                        nano_server_console::projects::finalize_after_update(&project).await;
                    for warning in &outcome.warnings {
                        tracing::warn!(project = %project, warning = %warning, "post-create refresh");
                    }
                }
                Ok(apis::projects::CreateProjectResponse::Status201_ProjectCreated(from_val(v)))
            }
            Err((code, msg)) if code == http::StatusCode::CONFLICT => {
                Ok(apis::projects::CreateProjectResponse::Status409_AlreadyExists(msg))
            }
            Err((_, msg)) => {
                Ok(apis::projects::CreateProjectResponse::Status400_InvalidRequest(msg))
            }
        }
    }

    async fn import_project(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        body: &models::ImportProjectRequest,
    ) -> Result<apis::projects::ImportProjectResponse, ()> {
        let name = body.name.trim().to_string();
        let path = body.path.trim().to_string();
        let out = tokio::task::spawn_blocking(move || {
            nano_server_console::projects::import_project_ref(&name, &path)
        })
        .await
        .unwrap_or_else(|e| Err(format!("import task panicked: {e}")));
        match out {
            Ok(r) => Ok(
                apis::projects::ImportProjectResponse::Status200_ProjectReferenceRegistered(
                    from_dto(r),
                ),
            ),
            // The only 409 `import_project_ref` produces is the workspace-name
            // conflict, whose message starts with this exact phrase — match it
            // precisely so an unrelated error can't be misclassified as 409.
            Err(e) if e.starts_with("a workspace project named") => {
                Ok(apis::projects::ImportProjectResponse::Status409_AlreadyExists(e))
            }
            Err(e) => Ok(apis::projects::ImportProjectResponse::Status400_InvalidRequest(e)),
        }
    }

    async fn create_project_path(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::CreateProjectPathPathParams,
        body: &models::CreateProjectPathRequest,
    ) -> Result<apis::projects::CreateProjectPathResponse, ()> {
        let dir = body.dir.unwrap_or(false);
        match nano_server_console::project_path_create(&path_params.name, &body.path, dir) {
            Ok(_) => Ok(apis::projects::CreateProjectPathResponse::Status201_Created),
            Err((code, msg)) if code == http::StatusCode::CONFLICT => {
                Ok(apis::projects::CreateProjectPathResponse::Status409_AlreadyExists(msg))
            }
            Err((_, msg)) => {
                Ok(apis::projects::CreateProjectPathResponse::Status400_InvalidRequest(msg))
            }
        }
    }

    async fn delete_project(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::DeleteProjectPathParams,
    ) -> Result<apis::projects::DeleteProjectResponse, ()> {
        match nano_server_console::project_delete(&path_params.name).await {
            Ok(_) => Ok(apis::projects::DeleteProjectResponse::Status204_Deleted),
            Err((_, msg)) => Ok(apis::projects::DeleteProjectResponse::Status404_NotFound(
                msg,
            )),
        }
    }

    async fn delete_project_path(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::DeleteProjectPathPathParams,
        query_params: &models::DeleteProjectPathQueryParams,
    ) -> Result<apis::projects::DeleteProjectPathResponse, ()> {
        match nano_server_console::project_path_delete(&path_params.name, &query_params.path) {
            Ok(_) => Ok(apis::projects::DeleteProjectPathResponse::Status204_Deleted),
            Err((_, msg)) => {
                Ok(apis::projects::DeleteProjectPathResponse::Status400_InvalidRequest(msg))
            }
        }
    }

    async fn get_project(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::GetProjectPathParams,
    ) -> Result<apis::projects::GetProjectResponse, ()> {
        match nano_server_console::project_detail(&path_params.name).await {
            Ok(mut v) => {
                fix_run_status(&mut v);
                Ok(apis::projects::GetProjectResponse::Status200_ProjectDetail(
                    from_val(v),
                ))
            }
            Err((_, msg)) => Ok(apis::projects::GetProjectResponse::Status404_NotFound(msg)),
        }
    }

    async fn get_project_config(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::GetProjectConfigPathParams,
    ) -> Result<apis::projects::GetProjectConfigResponse, ()> {
        match nano_server_console::project_config_get(&path_params.name) {
            Ok(v) => {
                Ok(apis::projects::GetProjectConfigResponse::Status200_ProjectConfig(from_val(v)))
            }
            Err((_, msg)) => Ok(apis::projects::GetProjectConfigResponse::Status404_NotFound(msg)),
        }
    }

    async fn get_run_configs(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::GetRunConfigsPathParams,
    ) -> Result<apis::projects::GetRunConfigsResponse, ()> {
        match nano_server_console::project_run_configs_list(&path_params.name) {
            Ok(v) => Ok(
                apis::projects::GetRunConfigsResponse::Status200_RunConfigurationsPlusTheActiveId(
                    from_val(v),
                ),
            ),
            Err((_, msg)) => Ok(apis::projects::GetRunConfigsResponse::Status404_NotFound(
                msg,
            )),
        }
    }

    async fn list_project_files(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::ListProjectFilesPathParams,
    ) -> Result<apis::projects::ListProjectFilesResponse, ()> {
        match nano_server_console::project_files(&path_params.name) {
            Ok(v) => Ok(apis::projects::ListProjectFilesResponse::Status200_FileTree(from_val(v))),
            Err((_, msg)) => Ok(apis::projects::ListProjectFilesResponse::Status404_NotFound(msg)),
        }
    }

    async fn list_projects(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
    ) -> Result<apis::projects::ListProjectsResponse, ()> {
        // Only 200 is declared; on error fall back to empty.
        let mut v = nano_server_console::projects_list()
            .await
            .unwrap_or_else(|_| {
                serde_json::json!({
                    "projects": [],
                    "denoAvailable": false,
                    "platforms": nano_server_console::projects::PLATFORMS,
                })
            });
        fix_run_status(&mut v);
        Ok(
            apis::projects::ListProjectsResponse::Status200_ProjectsPlusRuntimeAvailability(
                from_val(v),
            ),
        )
    }

    async fn rename_project(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::RenameProjectPathParams,
        body: &models::RenameProjectRequest,
    ) -> Result<apis::projects::RenameProjectResponse, ()> {
        match nano_server_console::project_rename(&path_params.name, &body.new_name).await {
            Ok(v) => Ok(
                apis::projects::RenameProjectResponse::Status200_UpdatedProjectConfig(from_val(v)),
            ),
            Err((code, msg)) if code == http::StatusCode::CONFLICT => {
                Ok(apis::projects::RenameProjectResponse::Status409_AlreadyExists(msg))
            }
            Err((_, msg)) => {
                Ok(apis::projects::RenameProjectResponse::Status400_InvalidRequest(msg))
            }
        }
    }

    async fn update_project_from_template(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::UpdateProjectFromTemplatePathParams,
        body: &Option<models::UpdateFromTemplateRequest>,
    ) -> Result<apis::projects::UpdateProjectFromTemplateResponse, ()> {
        use apis::projects::UpdateProjectFromTemplateResponse as R;
        let name = path_params.name.clone();
        let apply = body.as_ref().and_then(|b| b.apply).unwrap_or(false);
        let version = body.as_ref().and_then(|b| b.version.clone());
        // Conflict resolution: specific take-upstream paths and/or a bulk
        // "take theirs" for every conflict. `resolveConflicts: mine` is the
        // default (keep local) and needs no explicit handling.
        let resolution = nano_server_console::projects::ConflictResolution {
            take_theirs: body
                .as_ref()
                .and_then(|b| b.take_theirs.clone())
                .unwrap_or_default()
                .into_iter()
                .collect(),
            all_theirs: body
                .as_ref()
                .and_then(|b| b.resolve_conflicts.as_deref())
                .map(|r| r.eq_ignore_ascii_case("theirs"))
                .unwrap_or(false),
        };
        // npm pack + filesystem work — keep it off the async runtime.
        let update_name = name.clone();
        let res = tokio::task::spawn_blocking(move || {
            nano_server_console::projects::update_from_template(
                &update_name,
                apply,
                version.as_deref(),
                &resolution,
            )
        })
        .await;
        match res {
            Ok(Ok(mut plan)) => {
                // On a clean apply, refresh the project (npm install + `urban gen`)
                // so it runs without the maker manually re-installing deps and
                // regenerating artifacts after a pack update. Best-effort: any
                // problem is reported as a warning on the plan, not a failure.
                if apply && plan.applied && plan.conflicts.is_empty() {
                    plan.post_update =
                        Some(nano_server_console::projects::finalize_after_update(&name).await);
                }
                let v = serde_json::to_value(plan).expect("update plan serializes");
                Ok(R::Status200_TheOverlayPlan(from_val(v)))
            }
            Ok(Err(msg)) if msg == "not found" => Ok(R::Status404_NotFound(msg)),
            Ok(Err(msg)) => Ok(R::Status400_InvalidRequest(msg)),
            // A JoinError means the blocking update task itself panicked/was
            // cancelled — not a client error. Keep the 400 (no 5xx variant on
            // this op) but make the message name the failure mode explicitly so
            // operators can triage it, mirroring the `import task panicked`
            // convention on the import handler above.
            Err(e) => Ok(R::Status400_InvalidRequest(format!(
                "update task panicked: {e}"
            ))),
        }
    }

    async fn run_project(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::RunProjectPathParams,
    ) -> Result<apis::projects::RunProjectResponse, ()> {
        match nano_server_console::project_run(&path_params.name).await {
            Ok(mut v) => {
                fix_run_status(&mut v);
                Ok(apis::projects::RunProjectResponse::Status200_RunState(
                    from_val(v),
                ))
            }
            Err((_, msg)) => Ok(apis::projects::RunProjectResponse::Status404_NotFound(msg)),
        }
    }

    async fn save_project_config(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::SaveProjectConfigPathParams,
        body: &models::ProjectConfig,
    ) -> Result<apis::projects::SaveProjectConfigResponse, ()> {
        let cfg: nano_server_console::projects::ProjectConfig =
            from_val(serde_json::to_value(body).expect("config serializes"));
        match nano_server_console::project_config_put(&path_params.name, cfg) {
            Ok(v) => Ok(
                apis::projects::SaveProjectConfigResponse::Status200_SavedProjectConfig(from_val(
                    v,
                )),
            ),
            Err((_, msg)) => {
                Ok(apis::projects::SaveProjectConfigResponse::Status400_InvalidRequest(msg))
            }
        }
    }

    async fn save_project_file(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::SaveProjectFilePathParams,
        query_params: &models::SaveProjectFileQueryParams,
        body: &String,
    ) -> Result<apis::projects::SaveProjectFileResponse, ()> {
        match nano_server_console::project_file_save(&path_params.name, &query_params.path, body) {
            Ok(_) => {
                // Regenerate the typed SDK when a process model is saved: the
                // model is the source of truth for worker/message I/O + custom
                // headers (ADR 0033 §3), so a saved `.bpmn` must re-derive
                // `worker-io.d.ts` et al. or the generated types drift from the
                // model. Best-effort — the save already succeeded and the types
                // are an authoring-time contract only (mirrors the DDL/migrate
                // regen triggers on the data path).
                if nano_server_console::is_model_resource(&query_params.path) {
                    nano_server_console::regenerate_domain_types(&path_params.name).await;
                } else if nano_server_console::is_workflow_source(&query_params.path) {
                    // Code-first inverse (ADR 0048): a `workflows/*.ts` save
                    // (re)generates the laid-out `resources/processes/*.bpmn` the
                    // SDK derives, then refreshes the types from them. Fire-and-
                    // forget — it needs a Deno round-trip (npm fetch + auto-layout)
                    // we must not block the save response on; best-effort.
                    let project = path_params.name.clone();
                    tokio::spawn(async move {
                        nano_server_console::regenerate_workflow_models(&project).await;
                    });
                }
                Ok(apis::projects::SaveProjectFileResponse::Status204_Saved)
            }
            Err((_, msg)) => {
                Ok(apis::projects::SaveProjectFileResponse::Status400_InvalidRequest(msg))
            }
        }
    }

    async fn set_active_run_config(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::SetActiveRunConfigPathParams,
        body: &models::ActiveRunConfigRequest,
    ) -> Result<apis::projects::SetActiveRunConfigResponse, ()> {
        let id = flatten_nullable(&body.id);
        match nano_server_console::project_active_run_config_put(&path_params.name, id) {
            Ok(v) => {
                Ok(apis::projects::SetActiveRunConfigResponse::Status200_TheActiveRun(from_val(v)))
            }
            Err((code, msg)) if code == http::StatusCode::NOT_FOUND => {
                Ok(apis::projects::SetActiveRunConfigResponse::Status404_NotFound(msg))
            }
            Err((_, msg)) => {
                Ok(apis::projects::SetActiveRunConfigResponse::Status400_InvalidRequest(msg))
            }
        }
    }

    async fn stop_project(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::StopProjectPathParams,
    ) -> Result<apis::projects::StopProjectResponse, ()> {
        match nano_server_console::project_stop(&path_params.name).await {
            Ok(mut v) => {
                fix_run_status(&mut v);
                Ok(apis::projects::StopProjectResponse::Status200_RunState(
                    from_val(v),
                ))
            }
            Err((_, msg)) => Ok(apis::projects::StopProjectResponse::Status404_NotFound(msg)),
        }
    }
}

// --- data (datasources / DB Manager, ADR 0024) ----------------------------

/// Map a datasource `ApiResult` error to a data response's collapsed status set
/// (only 400/404 exist on these operations): `NOT_FOUND` → 404, everything else
/// (bad SQL/unknown source → 400; no-Deno → 503; gateway → 500) → 400.
macro_rules! data_ok_or {
    ($res:expr, $ok:path, $bad:path, $nf:path) => {
        match $res {
            Ok(v) => Ok($ok(from_val(v))),
            Err((code, msg)) if code == http::StatusCode::NOT_FOUND => Ok($nf(msg)),
            Err((_, msg)) => Ok($bad(msg)),
        }
    };
}

#[async_trait]
impl apis::data::Data for ServerImpl {
    async fn get_data_sources(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::GetDataSourcesPathParams,
    ) -> Result<apis::data::GetDataSourcesResponse, ()> {
        use apis::data::GetDataSourcesResponse as R;
        data_ok_or!(
            nano_server_console::project_data_sources(&path_params.name).await,
            R::Status200_DatasourcesPlusTheDefaultSourceName,
            R::Status400_InvalidRequest,
            R::Status404_NotFound
        )
    }

    async fn get_data_schema(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::GetDataSchemaPathParams,
    ) -> Result<apis::data::GetDataSchemaResponse, ()> {
        use apis::data::GetDataSchemaResponse as R;
        data_ok_or!(
            nano_server_console::project_data_schema(&path_params.name, &path_params.source).await,
            R::Status200_DatasourceSchema,
            R::Status400_InvalidRequest,
            R::Status404_NotFound
        )
    }

    async fn query_data(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::QueryDataPathParams,
        body: &models::DataQueryRequest,
    ) -> Result<apis::data::QueryDataResponse, ()> {
        use apis::data::QueryDataResponse as R;
        let params = body
            .params
            .as_ref()
            .map(|ps| ps.iter().map(|o| o.0.clone()).collect())
            .unwrap_or_default();
        data_ok_or!(
            nano_server_console::project_data_query(
                &path_params.name,
                &path_params.source,
                &body.sql,
                params
            )
            .await,
            R::Status200_QueryResult,
            R::Status400_InvalidRequest,
            R::Status404_NotFound
        )
    }

    async fn exec_data(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::ExecDataPathParams,
        body: &models::DataQueryRequest,
    ) -> Result<apis::data::ExecDataResponse, ()> {
        use apis::data::ExecDataResponse as R;
        let params = body
            .params
            .as_ref()
            .map(|ps| ps.iter().map(|o| o.0.clone()).collect())
            .unwrap_or_default();
        data_ok_or!(
            nano_server_console::project_data_exec(
                &path_params.name,
                &path_params.source,
                &body.sql,
                params
            )
            .await,
            R::Status200_ExecResult,
            R::Status400_InvalidRequest,
            R::Status404_NotFound
        )
    }

    async fn exec_data_script(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::ExecDataScriptPathParams,
        body: &models::DataScriptRequest,
    ) -> Result<apis::data::ExecDataScriptResponse, ()> {
        use apis::data::ExecDataScriptResponse as R;
        data_ok_or!(
            nano_server_console::project_data_script(
                &path_params.name,
                &path_params.source,
                body.statements.clone(),
            )
            .await,
            R::Status200_ScriptResult,
            R::Status400_InvalidRequest,
            R::Status404_NotFound
        )
    }

    async fn get_data_migrations(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::GetDataMigrationsPathParams,
    ) -> Result<apis::data::GetDataMigrationsResponse, ()> {
        use apis::data::GetDataMigrationsResponse as R;
        data_ok_or!(
            nano_server_console::project_data_migrations(&path_params.name, &path_params.source)
                .await,
            R::Status200_MigrationStatus,
            R::Status400_InvalidRequest,
            R::Status404_NotFound
        )
    }

    async fn migrate_data(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::MigrateDataPathParams,
    ) -> Result<apis::data::MigrateDataResponse, ()> {
        use apis::data::MigrateDataResponse as R;
        data_ok_or!(
            nano_server_console::project_data_migrate(&path_params.name, &path_params.source).await,
            R::Status200_MigrationsApplied,
            R::Status400_InvalidRequest,
            R::Status404_NotFound
        )
    }

    async fn regenerate_domain_types(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::RegenerateDomainTypesPathParams,
    ) -> Result<apis::data::RegenerateDomainTypesResponse, ()> {
        use apis::data::RegenerateDomainTypesResponse as R;
        data_ok_or!(
            nano_server_console::project_data_domaintypes(&path_params.name, &path_params.source)
                .await,
            R::Status200_TheEmittedDomainTypes,
            R::Status400_InvalidRequest,
            R::Status404_NotFound
        )
    }

    async fn preview_domain_types(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::PreviewDomainTypesPathParams,
        body: &models::DomainTypesPreviewRequest,
    ) -> Result<apis::data::PreviewDomainTypesResponse, ()> {
        use apis::data::PreviewDomainTypesResponse as R;
        let shapes = serde_json::to_value(&body.shapes).unwrap_or(serde_json::Value::Null);
        // Preserve "meta omitted" as `None` (vs an explicit empty `meta: []`): an
        // omitted field lets `run_data_op` fall back to the saved-model `nano:meta`
        // scan, while `meta: []` explicitly previews an emptied in-editor list.
        let meta = body
            .meta
            .as_ref()
            .map(|m| serde_json::to_value(m).unwrap_or(serde_json::Value::Null));
        data_ok_or!(
            nano_server_console::project_data_preview_domaintypes(
                &path_params.name,
                &path_params.source,
                shapes,
                meta
            )
            .await,
            R::Status200_TheResolvedDomainTextAndPer,
            R::Status400_InvalidRequest,
            R::Status404_NotFound
        )
    }
}

#[async_trait]
impl apis::triggers::Triggers for ServerImpl {
    async fn add_trigger(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::AddTriggerPathParams,
        body: &models::AddTriggerRequest,
    ) -> Result<apis::triggers::AddTriggerResponse, ()> {
        use apis::triggers::AddTriggerResponse as R;
        let config: std::collections::BTreeMap<String, String> = flatten_nullable(&body.config)
            .map(|m| m.into_iter().collect())
            .unwrap_or_default();
        let connection = flatten_nullable(&body.connection);
        let action = serde_json::Value::Object(
            body.action
                .iter()
                .map(|(k, v)| (k.clone(), v.0.clone()))
                .collect(),
        );
        data_ok_or!(
            nano_server_console::project_trigger_add(
                &path_params.name,
                &body.id,
                &body.r_type,
                &config,
                connection.as_deref(),
                &action,
            )
            .await,
            R::Status200_TriggerAdded,
            R::Status400_InvalidRequest,
            R::Status404_NotFound
        )
    }

    async fn enqueue_trigger_event(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::EnqueueTriggerEventPathParams,
        body: &models::TriggerEnqueueRequest,
    ) -> Result<apis::triggers::EnqueueTriggerEventResponse, ()> {
        use apis::triggers::EnqueueTriggerEventResponse as R;
        let event_body = serde_json::Value::Object(
            body.body
                .iter()
                .map(|(k, v)| (k.clone(), v.0.clone()))
                .collect(),
        );
        let idem = flatten_nullable(&body.idempotency_key);
        data_ok_or!(
            nano_server_console::project_trigger_enqueue(
                &path_params.name,
                &body.trigger_id,
                idem,
                event_body
            )
            .await,
            R::Status200_EnqueueOutcome,
            R::Status400_InvalidRequest,
            R::Status404_NotFound
        )
    }

    async fn get_trigger_inbox(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::GetTriggerInboxPathParams,
    ) -> Result<apis::triggers::GetTriggerInboxResponse, ()> {
        use apis::triggers::GetTriggerInboxResponse as R;
        data_ok_or!(
            nano_server_console::project_trigger_inbox(&path_params.name).await,
            R::Status200_InboxStatus,
            R::Status400_InvalidRequest,
            R::Status404_NotFound
        )
    }

    async fn get_triggers(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::GetTriggersPathParams,
    ) -> Result<apis::triggers::GetTriggersResponse, ()> {
        use apis::triggers::GetTriggersResponse as R;
        data_ok_or!(
            nano_server_console::project_triggers(&path_params.name).await,
            R::Status200_Triggers,
            R::Status400_InvalidRequest,
            R::Status404_NotFound
        )
    }
}

#[async_trait]
impl apis::connectors::Connectors for ServerImpl {
    async fn add_connector(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::AddConnectorPathParams,
        body: &models::AddConnectorRequest,
    ) -> Result<apis::connectors::AddConnectorResponse, ()> {
        use apis::connectors::AddConnectorResponse as R;
        let config: std::collections::BTreeMap<String, String> = flatten_nullable(&body.config)
            .map(|m| m.into_iter().collect())
            .unwrap_or_default();
        let connection = flatten_nullable(&body.connection);
        data_ok_or!(
            nano_server_console::project_connector_add(
                &path_params.name,
                &body.r_type,
                connection.as_deref(),
                &config,
            ),
            R::Status200_ConnectorEnabled,
            R::Status400_InvalidRequest,
            R::Status404_NotFound
        )
    }

    async fn get_connectors(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::GetConnectorsPathParams,
    ) -> Result<apis::connectors::GetConnectorsResponse, ()> {
        use apis::connectors::GetConnectorsResponse as R;
        data_ok_or!(
            nano_server_console::project_connectors(&path_params.name),
            R::Status200_Connectors,
            R::Status400_InvalidRequest,
            R::Status404_NotFound
        )
    }
}

// ---------------------------------------------------------------------------
// The console seam (ADR 0064 Phase 3, Option B).
//
// `nano-server-console` reaches this binary's `ServerImpl` exclusively through
// the object-safe `ConsoleServer` trait, so the god-object type never crosses
// the crate boundary. The console router receives an `Arc<dyn ConsoleServer>`.
// ---------------------------------------------------------------------------
#[async_trait]
impl nano_server_console::ConsoleServer for ServerImpl {
    fn store(&self) -> &std::sync::Arc<crate::readstore::ReadModel> {
        &self.store
    }

    fn trace_store(&self) -> &nano_trace_store::TraceStore {
        self.trace_store.as_ref()
    }

    fn cluster_topology(&self) -> &crate::cluster::Topology {
        self.engine.topology()
    }

    fn sla_mode(&self) -> crate::backpressure::SlaMode {
        ServerImpl::sla_mode(self)
    }

    async fn switch_sla_mode(&self, mode: crate::backpressure::SlaMode) {
        ServerImpl::switch_sla_mode(self, mode).await
    }

    fn raft_enabled(&self) -> bool {
        crate::raft_enabled()
    }

    fn recovery_counts(&self) -> crate::cluster::RecoveryCounts {
        crate::recovery_counts(self)
    }

    fn raft_partition_metrics(&self, partition: u64) -> Option<(Option<u32>, u64)> {
        let part = self.raft_registry().get(partition)?;
        let m = part.raft.metrics().borrow().clone();
        Some((m.current_leader.map(|id| id as u32), m.current_term))
    }

    async fn instance_job_overlay(
        &self,
        partition: u64,
        keys: Vec<u64>,
    ) -> std::collections::HashMap<u64, nano_server_console::LiveJob> {
        let Some(handle) = self.engine_handle_for(partition) else {
            return std::collections::HashMap::new();
        };
        handle
            .with(move |journal| {
                let mut m = std::collections::HashMap::new();
                for k in keys {
                    if let Some(job) = journal.engine().job(k) {
                        m.insert(
                            k,
                            nano_server_console::LiveJob {
                                state: format!("{:?}", job.state),
                                worker: job.worker.clone(),
                                deadline_ms: job.deadline,
                                activated_at_ms: job.activated_at,
                            },
                        );
                    }
                }
                m
            })
            .await
    }
}
