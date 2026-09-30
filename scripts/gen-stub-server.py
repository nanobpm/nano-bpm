#!/usr/bin/env python3
"""Generate stub trait implementations for the generated REST layer.

The rust-axum router (`nanobpm_gateway_rest::server::new`) requires a single type
that implements every per-tag API trait. Writing those impls by hand (hundreds of
methods) is infeasible and would rot whenever the spec changes, so this script
derives them from the generated trait definitions in
`generated/src/apis/*.rs`.

Every generated method body is `Err(())`, which routes through the server type's
`ErrorHandler` implementation. The hand-written server (server/src/main.rs) maps
that to `501 Not Implemented`, giving a fully routable stub server with no backend
wiring.

Usage:
    gen-stub-server.py <generated-apis-dir> <output-rs-file>
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

TRAIT_RE = re.compile(r"pub trait (?P<name>\w+)<E\b")
HAS_CLAIMS_RE = re.compile(r"^\s*type Claims;", re.MULTILINE)
METHOD_RE = re.compile(
    r"async fn (?P<name>\w+)\((?P<params>[\s\S]*?)\)\s*->\s*Result<\s*(?P<resp>\w+)\s*,\s*E\s*>;"
)

# (module, method) -> body expression that delegates to a hand-written method on
# `ServerImpl` (defined in server/src/main.rs) instead of returning `Err(())`.
# This is how specific operations get wired to the embedded nanobpmn engine while
# every other operation stays a 501 stub. The signatures are still generated, so
# they never drift from the spec.
OVERRIDES: dict[tuple[str, str], str] = {
    ("process_instance", "create_process_instance"): "self.create_process_instance_impl(body).await",
    ("process_instance", "cancel_process_instance"): "self.cancel_process_instance_impl(path_params).await",
    ("process_instance", "suspend_process_instance"): "self.suspend_process_instance_impl(path_params).await",
    ("process_instance", "resume_process_instance"): "self.resume_process_instance_impl(path_params).await",
    ("process_instance", "migrate_process_instance"): "self.migrate_process_instance_impl(path_params, body).await",
    ("job", "complete_job"): "self.complete_job_impl(path_params, body).await",
    ("resource", "create_deployment"): "self.create_deployment_impl(body).await",
    ("resource", "get_resource"): "self.get_resource_impl(path_params).await",
    ("resource", "get_resource_content"): "self.get_resource_content_impl(path_params).await",
    ("resource", "get_resource_content_binary"): "self.get_resource_content_binary_impl(path_params).await",
    ("resource", "search_resources"): "self.search_resources_impl(body).await",
    ("decision_definition", "evaluate_decision"): "self.evaluate_decision_impl(body).await",
    ("decision_definition", "get_decision_definition"): "self.get_decision_definition_impl(path_params).await",
    ("decision_definition", "get_decision_definition_xml"): "self.get_decision_definition_xml_impl(path_params).await",
    ("decision_definition", "search_decision_definitions"): "self.search_decision_definitions_impl(body).await",
    ("decision_requirements", "get_decision_requirements"): "self.get_decision_requirements_impl(path_params).await",
    ("decision_requirements", "get_decision_requirements_xml"): "self.get_decision_requirements_xml_impl(path_params).await",
    ("decision_requirements", "search_decision_requirements"): "self.search_decision_requirements_impl(body).await",
    ("decision_instance", "get_decision_instance"): "self.get_decision_instance_impl(path_params).await",
    ("decision_instance", "delete_decision_instance"): "self.delete_decision_instance_impl(path_params).await",
    ("decision_instance", "search_decision_instances"): "self.search_decision_instances_impl(body).await",
    ("form", "get_form_by_key"): "self.get_form_by_key_impl(path_params).await",
    ("job", "activate_jobs"): "self.activate_jobs_impl(body).await",
    ("job", "fail_job"): "self.fail_job_impl(path_params, body).await",
    ("job", "throw_job_error"): "self.throw_job_error_impl(path_params, body).await",
    ("job", "update_job"): "self.update_job_impl(path_params, body).await",
    ("incident", "resolve_incident"): "self.resolve_incident_impl(path_params, body).await",
    ("incident", "get_incident"): "self.get_incident_impl(path_params).await",
    ("incident", "search_incidents"): "self.search_incidents_impl(body).await",
    ("process_instance", "search_process_instances"): "self.search_process_instances_impl(body).await",
    ("process_definition", "search_process_definitions"): "self.search_process_definitions_impl(body).await",
    ("process_definition", "get_process_definition"): "self.get_process_definition_impl(path_params).await",
    ("process_definition", "get_process_definition_xml"): "self.get_process_definition_xml_impl(path_params).await",
    ("process_definition", "get_start_process_form"): "self.get_start_process_form_impl(path_params).await",
    ("job", "search_jobs"): "self.search_jobs_impl(body).await",
    ("process_instance", "get_process_instance"): "self.get_process_instance_impl(path_params).await",
    ("element_instance", "create_element_instance_variables"): "self.create_element_instance_variables_impl(path_params, body).await",
    ("element_instance", "get_element_instance"): "self.get_element_instance_impl(path_params).await",
    ("element_instance", "search_element_instances"): "self.search_element_instances_impl(body).await",
    ("element_instance", "search_element_instance_incidents"): "self.search_element_instance_incidents_impl(path_params, body).await",
    ("element_instance", "search_element_instance_wait_states"): "self.search_element_instance_wait_states_impl(body).await",
    ("message", "publish_message"): "self.publish_message_impl(body).await",
    ("message", "correlate_message"): "self.correlate_message_impl(body).await",
    ("message_subscription", "search_message_subscriptions"): "self.search_message_subscriptions_impl(body).await",
    ("message_subscription", "search_correlated_message_subscriptions"): "self.search_correlated_message_subscriptions_impl(body).await",
    ("variable", "search_variables"): "self.search_variables_impl(query_params, body).await",
    ("variable", "get_variable"): "self.get_variable_impl(path_params).await",
    ("cluster", "get_topology"): "self.get_topology_impl().await",
    ("user_task", "search_user_tasks"): "self.search_user_tasks_impl(body).await",
    ("user_task", "assign_user_task"): "self.assign_user_task_impl(path_params, body).await",
    ("user_task", "complete_user_task"): "self.complete_user_task_impl(path_params, body).await",
    ("user_task", "get_user_task"): "self.get_user_task_impl(path_params).await",
    ("user_task", "get_user_task_form"): "self.get_user_task_form_impl(path_params).await",
    ("user_task", "unassign_user_task"): "self.unassign_user_task_impl(path_params).await",
    ("user_task", "update_user_task"): "self.update_user_task_impl(path_params, body).await",
    # --- BEGIN issue-905 batch-operations delegations ---
    ("batch_operation", "get_batch_operation"): "self.get_batch_operation_impl(path_params).await",
    ("batch_operation", "search_batch_operations"): "self.search_batch_operations_impl(body).await",
    ("batch_operation", "search_batch_operation_items"): "self.search_batch_operation_items_impl(body).await",
    ("batch_operation", "suspend_batch_operation"): "self.suspend_batch_operation_impl(path_params).await",
    ("batch_operation", "resume_batch_operation"): "self.resume_batch_operation_impl(path_params).await",
    ("batch_operation", "cancel_batch_operation"): "self.cancel_batch_operation_impl(path_params).await",
    ("process_instance", "cancel_process_instances_batch_operation"): "self.cancel_process_instances_batch_operation_impl(body).await",
    ("process_instance", "resolve_incidents_batch_operation"): "self.resolve_incidents_batch_operation_impl(body).await",
    # --- END issue-905 batch-operations delegations ---
    # --- BEGIN issue-906 cluster-variables delegations ---
    ("cluster_variable", "create_global_cluster_variable"): "self.create_global_cluster_variable_impl(body).await",
    ("cluster_variable", "create_tenant_cluster_variable"): "self.create_tenant_cluster_variable_impl(path_params, body).await",
    ("cluster_variable", "update_global_cluster_variable"): "self.update_global_cluster_variable_impl(path_params, body).await",
    ("cluster_variable", "update_tenant_cluster_variable"): "self.update_tenant_cluster_variable_impl(path_params, body).await",
    ("cluster_variable", "get_global_cluster_variable"): "self.get_global_cluster_variable_impl(path_params).await",
    ("cluster_variable", "get_tenant_cluster_variable"): "self.get_tenant_cluster_variable_impl(path_params).await",
    ("cluster_variable", "delete_global_cluster_variable"): "self.delete_global_cluster_variable_impl(path_params).await",
    ("cluster_variable", "delete_tenant_cluster_variable"): "self.delete_tenant_cluster_variable_impl(path_params).await",
    ("cluster_variable", "search_cluster_variables"): "self.search_cluster_variables_impl(query_params, body).await",
    # --- END issue-906 cluster-variables delegations ---
    # --- BEGIN issue-984 agent-instances (S5) delegations ---
    ("agent_instance", "create_agent_instance"): "self.create_agent_instance_impl(body).await",
    ("agent_instance", "get_agent_instance"): "self.get_agent_instance_impl(path_params).await",
    ("agent_instance", "update_agent_instance"): "self.update_agent_instance_impl(path_params, body).await",
    ("agent_instance", "search_agent_instances"): "self.search_agent_instances_impl(body).await",
    ("agent_instance", "search_agent_instance_history"): "self.search_agent_instance_history_impl(path_params, body).await",
    # --- END issue-984 agent-instances (S5) delegations ---
    # --- BEGIN issue-907 jobs & job-statistics delegations ---
    ("job", "update_jobs_batch_operation"): "self.update_jobs_batch_operation_impl(body).await",
    ("job", "get_global_job_statistics"): "self.get_global_job_statistics_impl(query_params).await",
    ("job", "get_job_type_statistics"): "self.get_job_type_statistics_impl(body).await",
    ("job", "get_job_worker_statistics"): "self.get_job_worker_statistics_impl(body).await",
    ("job", "get_job_time_series_statistics"): "self.get_job_time_series_statistics_impl(body).await",
    ("job", "get_job_error_statistics"): "self.get_job_error_statistics_impl(body).await",
    # --- END issue-907 jobs & job-statistics delegations ---
    # --- BEGIN issue-908 expression & conditional delegations ---
    ("expression", "evaluate_expression"): "self.evaluate_expression_impl(body).await",
    ("conditional", "evaluate_conditionals"): "self.evaluate_conditionals_impl(body).await",
    # --- END issue-908 expression & conditional delegations ---
    # --- BEGIN issue-909 ad-hoc activities delegations ---
    ("ad_hoc_sub_process", "activate_ad_hoc_sub_process_activities"): "self.activate_ad_hoc_sub_process_activities_impl(path_params, body).await",
    # --- END issue-909 ad-hoc activities delegations ---
}

HEADER = """// @generated by gateway-rust/scripts/gen-stub-server.py - DO NOT EDIT.
//
// Stub implementations of every generated API trait for `crate::ServerImpl`.
// Each method returns `Err(())`, which the server's ErrorHandler maps to a
// 501 Not Implemented response, EXCEPT for the operations listed in OVERRIDES
// (wired to the embedded nanobpmn engine). Regenerate with `make generate`.
#![allow(unused_variables, unused_imports, clippy::all)]

use async_trait::async_trait;
use axum::extract::*;
use axum_extra::extract::CookieJar;
use bytes::Bytes;
use nanobpm_gateway_rest::{apis, models, types::*};
use headers::Host;
use http::Method;

use crate::ServerImpl;
"""


def render_trait_impl(module: str, source: str) -> str | None:
    trait_match = TRAIT_RE.search(source)
    if not trait_match:
        return None
    trait = trait_match.group("name")
    has_claims = bool(HAS_CLAIMS_RE.search(source))

    methods: list[str] = []
    for m in METHOD_RE.finditer(source):
        name = m.group("name")
        params = m.group("params").rstrip()
        resp = m.group("resp")
        body = OVERRIDES.get((module, name), "Err(())")
        methods.append(
            f"    async fn {name}({params}\n"
            f"    ) -> Result<apis::{module}::{resp}, ()> {{\n"
            f"        {body}\n"
            f"    }}\n"
        )

    if not methods:
        return None

    body = "\n".join(methods)
    claims = "    type Claims = ();\n\n" if has_claims else ""
    return (
        f"#[async_trait]\n"
        f"impl apis::{module}::{trait}<()> for ServerImpl {{\n"
        f"{claims}{body}}}\n"
    )


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print(f"usage: {argv[0]} <generated-apis-dir> <output-rs-file>", file=sys.stderr)
        return 2

    apis_dir = Path(argv[1]).resolve()
    output = Path(argv[2]).resolve()
    if not apis_dir.is_dir():
        print(f"generated apis dir not found: {apis_dir}", file=sys.stderr)
        return 1

    impls: list[str] = []
    trait_count = 0
    for src in sorted(apis_dir.glob("*.rs")):
        if src.stem == "mod":
            continue
        rendered = render_trait_impl(src.stem, src.read_text(encoding="utf-8"))
        if rendered is not None:
            impls.append(rendered)
            trait_count += 1

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(HEADER + "\n" + "\n".join(impls), encoding="utf-8")
    print(f"Generated stub impls for {trait_count} API traits -> {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
