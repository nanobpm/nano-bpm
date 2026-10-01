//! `jobs` engine tests (slice 7 of #1201), extracted verbatim.
use super::*;

#[test]
fn job_result_updates_the_nearest_defining_ancestor_scope() {
    // A job inside a sub-process completes with a variable whose name is already
    // defined in the sub-process scope: Zeebe propagation updates that scope (the
    // nearest defining ancestor), NOT the root.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(subprocess_with_input_mapping(
            Vec::new(),
        )))
        .unwrap();
    let mut vars = HashMap::new();
    vars.insert("seed".to_string(), Value::Int(4));
    let inst = engine
        .apply_command(Command::create_instance_with("sub-scope", vars))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    // The sub-process scope is the parent of the inner task instance.
    let inner_eik = engine.pending_jobs()[0].element_instance_key;
    let sub_scope = *engine
        .instance(inst)
        .unwrap()
        .scopes
        .get(&inner_eik)
        .unwrap();

    // The `work` job completes writing `scoped = 99` (a name owned by the
    // sub-process scope). It must update the sub-process scope, not root.
    let job_key = engine.activate_jobs("work", "w", 1, 60_000, 0)[0].key;
    let mut job_vars = HashMap::new();
    job_vars.insert("scoped".to_string(), Value::Int(99));
    let events = engine
        .apply_command(Command::complete_job_with(job_key, job_vars))
        .unwrap();
    assert!(
        events.iter().any(|e| matches!(
            e,
            Event::ScopedVariablesUpdated { scope_key, variables, .. }
                if *scope_key == sub_scope && variables.get("scoped") == Some(&Value::Int(99))
        )),
        "job result should update the sub-process scope; events: {events:?}"
    );
    // It never landed at the root scope.
    assert_eq!(io_var(&engine, inst, "scoped"), None);
}

#[test]
fn higher_priority_jobs_activate_before_older_lower_priority_jobs() {
    // Two processes emit the same `work` job type at different priorities.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(task_with_priority("low", "10")))
        .unwrap();
    engine
        .apply_command(Command::DeployProcess(task_with_priority("high", "90")))
        .unwrap();
    // Create the LOW-priority instance first (older, lower key), then HIGH.
    let low = create_instance_key(&mut engine, "low");
    let high = create_instance_key(&mut engine, "high");
    // Activation order is priority-first: the newer high-priority job wins.
    let jobs = engine.activate_jobs("work", "W", 10, 1_000, 0);
    assert_eq!(jobs.len(), 2);
    assert_eq!(
        jobs[0].instance_key, high,
        "higher priority activates first despite being newer"
    );
    assert_eq!(jobs[1].instance_key, low, "lower priority follows");
}

#[test]
fn equal_priority_jobs_activate_oldest_first() {
    // Same priority (default 50) => FIFO by creation (key) is preserved.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let first = create_instance_key(&mut engine, "order");
    let second = create_instance_key(&mut engine, "order");
    let jobs = engine.activate_jobs("payment", "W", 10, 1_000, 0);
    assert_eq!(jobs.len(), 2);
    assert_eq!(jobs[0].instance_key, first, "oldest first");
    assert_eq!(jobs[1].instance_key, second);
}

#[test]
fn job_created_at_and_default_priority_are_stamped() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    engine
        .apply_command_at(Command::create_instance("order"), 12_345)
        .unwrap();
    let job = engine
        .state()
        .jobs
        .values()
        .find(|j| j.job_type == "payment")
        .expect("a payment job");
    assert_eq!(
        job.created_at, 12_345,
        "created_at carries the command clock"
    );
    assert_eq!(
        job.priority,
        state::DEFAULT_JOB_PRIORITY,
        "no priorityDefinition => default priority"
    );
}

#[test]
fn variable_spill_selects_only_job_parked_instances() {
    // A plain service-task instance is a spill candidate (parked on a job,
    // carries variables); spilling drops the payload and rehydration restores
    // it, and a spilled instance is no longer a candidate.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let mut vars = HashMap::new();
    vars.insert("k".to_string(), Value::Int(7));
    let events = engine
        .apply_command(Command::create_instance_with("order", vars))
        .unwrap();
    let key = events.iter().find_map(|e| e.instance_key()).unwrap();

    assert_eq!(engine.spillable_instances(10), vec![key]);
    assert_eq!(engine.resident_spillable_count(), 1);

    let payload = engine.spill_variables(key).expect("spillable");
    assert!(engine.is_variables_spilled(key));
    assert!(engine.instance(key).unwrap().variables.is_empty());
    assert!(
        engine.spillable_instances(10).is_empty(),
        "an already-spilled instance is not a candidate"
    );

    engine.rehydrate_variables(key, payload);
    assert!(!engine.is_variables_spilled(key));
    assert_eq!(
        engine.instance(key).unwrap().variables.get("k"),
        Some(&Value::Int(7))
    );
}

#[test]
fn spilling_a_nested_scope_instance_preserves_the_merged_job_view() {
    // Regression (Part C spill): a variable spill sheds only the ROOT payload;
    // the sub-process scope-local map stays resident. After the host rehydrates
    // the root, `element_variables` must fold both back together so a job on the
    // nested scope regains its full view (root `seed` + the sub-process-local
    // `scoped`) — not the root-only payload read back from the spill store.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(subprocess_with_input_mapping(
            Vec::new(),
        )))
        .unwrap();
    let mut vars = HashMap::new();
    vars.insert("seed".to_string(), Value::Int(4));
    let inst = engine
        .apply_command(Command::create_instance_with("sub-scope", vars))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let eik = engine.pending_jobs()[0].element_instance_key;

    // The full merged view before any spill: root + sub-process local.
    let before = engine.element_variables(inst, eik);
    assert_eq!(before.get("seed"), Some(&Value::Int(4)));
    assert_eq!(before.get("scoped"), Some(&Value::Int(5)));

    // Spill sheds only the root payload; the scope-local map stays resident.
    let payload = engine.spill_variables(inst).expect("spillable");
    assert!(engine.instance(inst).unwrap().variables.is_empty());
    assert!(
        !engine.instance(inst).unwrap().scope_variables.is_empty(),
        "the sub-process scope-local map stays resident through a root spill"
    );
    // While spilled the root is gone, so a naive read is scope-only — exactly
    // why the host rehydrates before it reads the worker's snapshot.
    assert_eq!(engine.element_variables(inst, eik).get("seed"), None);

    // Rehydrate the root; the merged view is whole again.
    engine.rehydrate_variables(inst, payload);
    let after = engine.element_variables(inst, eik);
    assert_eq!(after.get("seed"), Some(&Value::Int(4)));
    assert_eq!(after.get("scoped"), Some(&Value::Int(5)));
}

#[test]
fn leased_job_instance_is_still_spillable() {
    // Regression pin (ADR 0012): activating (leasing) a job to a worker keeps
    // the job indexed in `jobs_by_instance` and only adds it to `activated_jobs`,
    // so the instance remains a spill candidate. The worker already holds a copy
    // of the variables from activation; rehydration on completion/redelivery
    // restores them. (Corrects the prior claim that `is_spillable` excludes
    // leased jobs.)
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let mut vars = HashMap::new();
    vars.insert("k".to_string(), Value::Int(7));
    let events = engine
        .apply_command(Command::create_instance_with("order", vars))
        .unwrap();
    let key = events.iter().find_map(|e| e.instance_key()).unwrap();
    assert_eq!(engine.resident_spillable_count(), 1);

    let jobs = engine.activate_jobs("payment", "w", 10, 60_000, 0);
    assert_eq!(jobs.len(), 1, "one job activated");
    assert!(
        engine.state().activated_jobs.contains(&jobs[0].key),
        "the job is leased"
    );
    assert_eq!(
        engine.resident_spillable_count(),
        1,
        "a leased-job instance is still spillable"
    );
    assert_eq!(engine.spillable_instances(10), vec![key]);
}

#[test]
fn live_job_count_excludes_completed_jobs_pending_eviction() {
    // Regression: `runnable_backlog` (the admission/governor congestion signal)
    // must count only *live* jobs — Created + Activated — never the terminal
    // jobs that linger in `jobs` after their instance completes but before the
    // exporter evicts it. A completed job is deindexed the instant it settles,
    // yet its `Job` shell (and the completed instance shell) stay resident until
    // eviction. If the exporter falls behind — or is stalled by a locked
    // read-model store — those un-evicted terminal jobs accumulate; counting
    // `jobs.len()` would fold that dead weight into the backpressure reading and
    // shed legitimate new work, a self-inflicted freeze that never clears.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let events = engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let key = events.iter().find_map(|e| e.instance_key()).unwrap();

    // Job parked at the service task: one live (Created) job.
    assert_eq!(
        engine.state().live_job_count(),
        1,
        "a created-and-waiting job is live congestion"
    );

    // Activate (lease) it: still one live (now Activated) job.
    let job = engine.activate_jobs("payment", "w", 1, 60_000, 0)[0].clone();
    assert_eq!(
        engine.state().live_job_count(),
        1,
        "a leased/in-flight job is still live congestion"
    );

    // Complete it. The instance reaches its end event and goes terminal, but its
    // shell (and the completed job) stay resident until exporter-driven eviction.
    engine
        .apply_command(Command::complete_job(job.key))
        .unwrap();
    assert_eq!(
        engine.instance(key).expect("shell still resident").state,
        crate::state::ProcessInstanceState::Completed,
        "instance is terminal but not yet evicted"
    );
    assert_eq!(
        engine.state().jobs.len(),
        1,
        "the completed job shell lingers in `jobs` until eviction"
    );
    assert_eq!(
        engine.state().live_job_count(),
        0,
        "a completed job is NOT live congestion — the admission signal must \
         ignore it, or a lagging exporter would shed new work forever"
    );
}

#[test]
fn cold_spill_round_trips_a_job_parked_instance() {
    // Snapshotting a job-parked instance lifts it (and its job) entirely out
    // of hot state; rehydrating restores it so the job is activatable and the
    // instance completes exactly as if it had never been spilled.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let mut vars = HashMap::new();
    vars.insert("k".to_string(), Value::Int(7));
    let events = engine
        .apply_command(Command::create_instance_with("order", vars))
        .unwrap();
    let key = events.iter().find_map(|e| e.instance_key()).unwrap();

    assert_eq!(engine.cold_spillable_instances(10), vec![key]);
    assert_eq!(engine.cold_spillable_count(), 1);

    let snapshot = engine.snapshot_instance(key).expect("snapshottable");
    assert_eq!(snapshot.instance.key, key);
    assert_eq!(
        snapshot.jobs.len(),
        1,
        "the parked job travels in the snapshot"
    );
    assert_eq!(snapshot.instance.variables.get("k"), Some(&Value::Int(7)));
    // Fully out of hot state: instance, job and job index all gone.
    assert!(engine.instance(key).is_none());
    assert!(!engine.state().jobs_by_instance.contains_key(&key));
    assert!(engine
        .activate_jobs("payment", "w", 10, 60_000, 0)
        .is_empty());

    engine.rehydrate_instance(snapshot);
    assert!(engine.instance(key).is_some());
    assert_eq!(
        engine.instance(key).unwrap().variables.get("k"),
        Some(&Value::Int(7))
    );
    // Job is activatable and completable again; the instance then completes.
    let completion = complete_one(&mut engine, "payment");
    assert!(completion.iter().any(
        |e| matches!(e, Event::ProcessInstanceCompleted { instance_key } if *instance_key == key)
    ));
}

#[test]
fn cold_spill_excludes_instances_with_a_locked_job() {
    // An instance whose job is currently activated (a worker holds the lock)
    // is mid-task, not dormant: it must not be a cold-spill candidate.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let events = engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let key = events.iter().find_map(|e| e.instance_key()).unwrap();
    assert_eq!(engine.cold_spillable_instances(10), vec![key]);

    // Activate (lock) the job — now the instance is busy.
    let _ = engine.activate_jobs("payment", "w", 1, 60_000, 0);
    assert!(
        engine.cold_spillable_instances(10).is_empty(),
        "an instance with a locked job is not cold-spillable"
    );
    assert!(
        engine.snapshot_instance(key).is_some(),
        "snapshot_instance itself is unconditional on lock state (host gates via the selector)"
    );
}

#[test]
fn static_retries_declaration_sets_initial_job_retries() {
    let engine = deploy_and_create_retryable(Some("5"), HashMap::new());
    let job = engine.state().jobs.values().next().unwrap();
    assert_eq!(job.retries, 5);
}

#[test]
fn feel_retries_expression_resolves_against_variables() {
    let engine =
        deploy_and_create_retryable(Some("=maxRetries"), vars(&[("maxRetries", Value::Int(7))]));
    let job = engine.state().jobs.values().next().unwrap();
    assert_eq!(job.retries, 7);
}

#[test]
fn missing_retries_declaration_defaults_to_three() {
    let engine = deploy_and_create_retryable(None, HashMap::new());
    let job = engine.state().jobs.values().next().unwrap();
    assert_eq!(job.retries, state::DEFAULT_JOB_RETRIES);
}

#[test]
fn activated_job_carries_custom_headers_and_process_identity() {
    // A service task's static zeebe:taskHeaders, the owning instance's tags and
    // business id, and the process-definition identity (bpmnProcessId, key,
    // version) must all ride on the ActivatedJob worker snapshot — the full
    // Zeebe ActivatedJob contract, not just key/type/variables.
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:serviceTask id="charge">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="payment" />
              <zeebe:taskHeaders>
                <zeebe:header key="channel" value="card" />
              </zeebe:taskHeaders>
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="charge" />
          <bpmn:sequenceFlow id="b" sourceRef="charge" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml).unwrap().pop().unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    engine
        .apply_command(Command::create_instance_full(
            "p",
            HashMap::new(),
            vec!["vip".to_string(), "eu".to_string()],
            Some("order-42".to_string()),
        ))
        .unwrap();

    let deployed = engine.state().processes.get("p").unwrap();
    let expected_key = deployed.key;
    let expected_version = deployed.version;

    let activated = engine.activate_jobs("payment", "w1", 10, 60_000, 0);
    assert_eq!(activated.len(), 1);
    let job = &activated[0];

    // Custom headers surfaced verbatim.
    assert_eq!(job.custom_headers.get("channel"), Some(&"card".to_string()));
    assert_eq!(job.custom_headers.len(), 1);
    // Process-definition identity.
    assert_eq!(job.bpmn_process_id, "p");
    assert_eq!(job.process_definition_key, expected_key);
    assert_eq!(job.process_definition_version, expected_version);
    // Instance metadata.
    assert_eq!(job.tags, vec!["vip".to_string(), "eu".to_string()]);
    assert_eq!(job.business_id, Some("order-42".to_string()));
    // Priority defaults to the standard 50 when undeclared.
    assert_eq!(job.priority, state::DEFAULT_JOB_PRIORITY);

    // The read-back projection (activated_job) must agree with activate_jobs.
    let projected = engine.activated_job(job.key).unwrap();
    assert_eq!(projected.custom_headers, job.custom_headers);
    assert_eq!(projected.bpmn_process_id, "p");
    assert_eq!(projected.tags, job.tags);
    assert_eq!(projected.business_id, job.business_id);
}

#[test]
fn activated_job_has_empty_custom_headers_when_task_declares_none() {
    // A service task without zeebe:taskHeaders yields an empty header map — the
    // conservative default, never a borrowed or synthesised set.
    let engine = deploy_and_create_retryable(None, HashMap::new());
    let mut engine = engine;
    let activated = engine.activate_jobs("do-work", "w1", 10, 60_000, 0);
    assert_eq!(activated.len(), 1);
    assert!(activated[0].custom_headers.is_empty());
}

#[test]
fn bpmn_parses_zeebe_linked_resources_onto_the_service_task() {
    // A service task's zeebe:linkedResources are parsed into the model, in
    // declaration order, with binding type / ids / link names preserved.
    use crate::model::{BindingType, ElementKind};
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:serviceTask id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="run-agent" />
              <zeebe:linkedResources>
                <zeebe:linkedResource resourceId="agent-prompt.md" bindingType="latest"
                                      resourceType="GenericScript" linkName="prompt" />
                <zeebe:linkedResource resourceId="policy.md" bindingType="deployment"
                                      resourceType="GenericScript" linkName="policy" />
              </zeebe:linkedResources>
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="b" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml).unwrap().pop().unwrap();
    let task = def.element("agent").expect("the service task is parsed");
    let ElementKind::ServiceTask {
        linked_resources, ..
    } = &task.kind
    else {
        panic!("expected a service task, got {:?}", task.kind);
    };
    assert_eq!(linked_resources.len(), 2);
    assert_eq!(linked_resources[0].resource_id, "agent-prompt.md");
    assert_eq!(linked_resources[0].binding_type, BindingType::Latest);
    assert_eq!(linked_resources[0].resource_type, "GenericScript");
    assert_eq!(linked_resources[0].link_name, "prompt");
    assert_eq!(linked_resources[1].resource_id, "policy.md");
    assert_eq!(linked_resources[1].binding_type, BindingType::Deployment);
    assert_eq!(linked_resources[1].link_name, "policy");
}

#[test]
fn activated_job_resolves_linked_resource_latest_binding_into_a_header() {
    // A serviceTask linking a generic resource by id (bindingType=latest) must,
    // at activation, resolve that id to the LATEST deployed resource key and
    // deliver it to the worker in the `linkedResources` custom header. An
    // undeployed link id is simply omitted.
    use crate::command::GenericResource;
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:serviceTask id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="run-agent" />
              <zeebe:linkedResources>
                <zeebe:linkedResource resourceId="agent-prompt.md" bindingType="latest"
                                      resourceType="GenericScript" linkName="prompt" />
                <zeebe:linkedResource resourceId="missing.md" bindingType="latest"
                                      resourceType="GenericScript" linkName="gone" />
              </zeebe:linkedResources>
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="b" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml).unwrap().pop().unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    // Deploy two versions of the linked resource; `latest` must pick version 2.
    let resource = |content: &str| GenericResource {
        resource_id: "agent-prompt.md".to_string(),
        resource_name: "agent-prompt.md".to_string(),
        content: content.to_string(),
    };
    engine
        .apply_command(Command::DeployGenericResources(vec![resource("# v1")]))
        .unwrap();
    engine
        .apply_command(Command::DeployGenericResources(vec![resource("# v2")]))
        .unwrap();
    let latest_key = engine.state().resources["agent-prompt.md"].key;
    assert_eq!(engine.state().resources["agent-prompt.md"].version, 2);

    engine.apply_command(Command::create_instance("p")).unwrap();
    let activated = engine.activate_jobs("run-agent", "w1", 10, 60_000, 0);
    assert_eq!(activated.len(), 1);
    let header = activated[0]
        .custom_headers
        .get("linkedResources")
        .expect("the linkedResources header is present");

    // The resolved header points at the LATEST resource key; the undeployed
    // `missing.md` link is omitted (so exactly one entry, keyed by its link).
    let expected = format!(
        "[{{\"resourceKey\":\"{latest_key}\",\"resourceType\":\"GenericScript\",\"linkName\":\"prompt\"}}]"
    );
    assert_eq!(header, &expected);

    // The read-back projection agrees with activate_jobs.
    let projected = engine.activated_job(activated[0].key).unwrap();
    assert_eq!(
        projected.custom_headers.get("linkedResources"),
        Some(header)
    );
}

#[test]
fn activated_job_omits_linked_resources_header_when_nothing_resolves() {
    // When a service task declares linkedResources but NONE of the ids are
    // deployed, the engine must emit no `linkedResources` header at all — not a
    // surprising empty `[]` — and must not clobber any author-supplied header.
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:serviceTask id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="run-agent" />
              <zeebe:linkedResources>
                <zeebe:linkedResource resourceId="missing.md" bindingType="latest"
                                      resourceType="GenericScript" linkName="gone" />
              </zeebe:linkedResources>
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="b" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml).unwrap().pop().unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    engine.apply_command(Command::create_instance("p")).unwrap();
    let activated = engine.activate_jobs("run-agent", "w1", 10, 60_000, 0);
    assert_eq!(activated.len(), 1);
    assert!(
        !activated[0].custom_headers.contains_key("linkedResources"),
        "no header when nothing resolves (not an empty array)"
    );
}

// zeebe-cells: element:ServiceTask
#[test]
fn should_park_on_service_task_then_complete_on_job() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();

    let events = engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    assert_eq!(engine.pending_jobs().len(), 1);
    assert!(!engine.is_completed(instance_key));

    let job_key = engine.pending_jobs()[0].key;
    engine.activate_jobs("payment", "worker-1", 10, 60_000, 0);
    let events = engine
        .apply_command(Command::complete_job(job_key))
        .unwrap();

    assert!(engine.is_completed(instance_key));
    assert!(events.contains(&Event::ProcessInstanceCompleted { instance_key }));
    assert!(engine.pending_jobs().is_empty());
}

#[test]
fn should_route_on_variables_returned_by_a_completed_job() {
    // s -> task(decide) -> g(xor): decision==yes -> approved ; else rejected
    let def = ProcessBuilder::new("review")
        .start_event("s")
        .service_task("decide", "decision")
        .exclusive_gateway("g")
        .end_event("approved")
        .end_event("rejected")
        .connect("s", "decide")
        .connect("decide", "g")
        .connect_when("g", "approved", r#"decision = "yes""#)
        .connect("g", "rejected")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("review"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // given the instance parked on the service task
    assert!(!engine.is_completed(instance_key));

    // when the worker completes the job, returning decision=yes
    let job_key = engine.activate_jobs("decision", "w", 1, 60_000, 0)[0].key;
    let vars = HashMap::from([("decision".to_string(), Value::Str("yes".into()))]);
    let events = engine
        .apply_command(Command::complete_job_with(job_key, vars))
        .unwrap();

    // then the gateway routes on the returned variable to the approved branch
    assert!(engine.is_completed(instance_key));
    assert!(events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { to, .. } if to == "approved"
    )));
    assert!(!events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { to, .. } if to == "rejected"
    )));
}

#[test]
fn should_re_activate_a_failed_job_that_still_has_retries() {
    // given an activated job
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    let job_key = engine.activate_jobs("payment", "A", 10, 60_000, 0)[0].key;

    // when the worker fails it with retries remaining
    engine
        .apply_command(Command::fail_job(job_key, 2, "transient error"))
        .unwrap();

    // then no incident is raised and the job is activatable again
    assert!(engine.instance(instance_key).unwrap().incidents.is_empty());
    assert_eq!(engine.pending_jobs().len(), 1);
    let reactivated = engine.activate_jobs("payment", "B", 10, 60_000, 1);
    assert_eq!(reactivated.len(), 1);
    assert_eq!(reactivated[0].key, job_key);
    assert_eq!(reactivated[0].retries, 2);
}

#[test]
fn should_preserve_the_activating_worker_on_a_job_that_fails_with_no_retries() {
    // #959 — a terminal, incident-bearing failure must keep the last activating
    // `worker` so the incident (joined by `jobKey`) can attribute the failure to
    // the worker/host that was running it (Zeebe parity — a failed JobRecord
    // retains its `worker`).
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let job_key = engine.activate_jobs("payment", "w1", 10, 60_000, 0)[0].key;

    // when the worker fails it with no retries left
    let events = engine
        .apply_command(Command::fail_job(job_key, 0, "boom"))
        .unwrap();

    // then the parked job still reports its activating worker
    let job = engine.job(job_key).unwrap();
    assert_eq!(job.state, state::JobState::Failed);
    assert_eq!(job.worker.as_deref(), Some("w1"));

    // and an incident is raised referencing this job's key
    assert!(events.iter().any(|e| matches!(
        e,
        Event::IncidentRaised { job_key: Some(k), .. } if *k == job_key
    )));
}

#[test]
fn should_stamp_the_activating_worker_on_a_successful_job_completion() {
    // #1191 — a *successful* completion must carry the activating `worker` on the
    // emitted `Event::JobCompleted` (stamped from the live job at the completion
    // site) so the read-model leader-local job row can attribute the completed
    // job — including a **husk** (a COMPLETED job that minted no AgentInstance) —
    // to the worker that ran it, symmetric with the `JobFailed`/`JobErrorThrown`
    // terminal events. `Job.worker` is cleared on completion, so the event is the
    // only place the identity survives.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let job_key = engine.activate_jobs("payment", "w1", 10, 60_000, 0)[0].key;

    // when the worker completes the job
    let events = engine
        .apply_command(Command::complete_job(job_key))
        .unwrap();

    // then the emitted completion event carries the activating worker
    assert!(events.iter().any(|e| matches!(
        e,
        Event::JobCompleted { job_key: k, worker: Some(w), .. } if *k == job_key && w == "w1"
    )));
}

#[test]
fn should_normalize_an_empty_activation_worker_to_no_attribution() {
    // #1191 — an *empty* activation worker string (e.g. an explicitly-supplied
    // `""` forwarded by the REST activation handler) is NOT an attribution: it
    // must be normalized to `None` at the single source (the `JobActivated`
    // reducer) rather than stamped as `Some("")`. Otherwise it flows into the
    // terminal `JobCompleted`/`JobFailed`/`JobErrorThrown` events and the
    // read-model `COALESCE(?, worker)` bindings treat `""` as a non-NULL value,
    // storing an empty attribution instead of leaving the row NULL.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();

    // when a job is activated with an empty worker string
    let job_key = engine.activate_jobs("payment", "", 10, 60_000, 0)[0].key;

    // then the live job carries no attribution (not `Some("")`)
    assert_eq!(engine.job(job_key).unwrap().worker, None);

    // and a successful completion emits no empty attribution
    let events = engine
        .apply_command(Command::complete_job(job_key))
        .unwrap();
    assert!(events.iter().any(|e| matches!(
        e,
        Event::JobCompleted { job_key: k, worker: None, .. } if *k == job_key
    )));
    assert!(!events.iter().any(|e| matches!(
        e,
        Event::JobCompleted { worker: Some(w), .. } if w.is_empty()
    )));
}

#[test]
fn should_drop_the_worker_when_a_failed_job_returns_to_the_activatable_pool() {
    // #959 — with retries remaining the job is genuinely no longer held (it goes
    // back to the activatable pool), so the activating `worker` must be cleared.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let job_key = engine.activate_jobs("payment", "w1", 10, 60_000, 0)[0].key;

    engine
        .apply_command(Command::fail_job(job_key, 2, "transient"))
        .unwrap();

    let job = engine.job(job_key).unwrap();
    assert_eq!(job.state, state::JobState::Created);
    assert_eq!(job.worker, None);
}

#[test]
fn should_preserve_the_activating_worker_on_a_job_that_throws_an_error_terminally() {
    // #959 — throwError with no catching boundary parks the job in `Errored` and
    // raises an incident; the activating `worker` must be retained for attribution
    // (Zeebe parity — throwError keeps the record incl. `worker`).
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let job_key = engine.activate_jobs("payment", "w1", 10, 60_000, 0)[0].key;

    engine
        .apply_command(Command::throw_job_error(job_key, "UNCAUGHT", "kaboom"))
        .unwrap();

    let job = engine.job(job_key).unwrap();
    assert_eq!(job.state, state::JobState::Errored);
    assert_eq!(job.worker.as_deref(), Some("w1"));
}

#[test]
fn should_recover_the_activating_worker_of_a_terminally_failed_job_via_replay() {
    // #959 — activation is a volatile lease that is *not* journaled/exported, so a
    // restart replays `JobCreated` then the terminal event with no intervening
    // `JobActivated`. The worker is therefore carried *on* the terminal event so
    // it survives replay (durable across restart), not merely held in live state.
    let mut engine = Engine::new();
    let mut log: Vec<Event> = Vec::new();
    log.extend(
        engine
            .apply_command(Command::DeployProcess(linear_with_task()))
            .unwrap(),
    );
    log.extend(
        engine
            .apply_command(Command::create_instance("order"))
            .unwrap(),
    );
    let job_key = engine.activate_jobs("payment", "w1", 10, 60_000, 0)[0].key;
    let fail_log = engine
        .apply_command(Command::fail_job(job_key, 0, "boom"))
        .unwrap();

    // The exported terminal event carries the activating worker (and, being
    // volatile, `activate_jobs` emitted no `JobActivated` into the durable log).
    assert!(fail_log.iter().any(|e| matches!(
        e,
        Event::JobFailed { worker: Some(w), .. } if w == "w1"
    )));
    log.extend(fail_log);
    assert!(!log.iter().any(|e| matches!(e, Event::JobActivated { .. })));

    // Replaying the durable stream — exactly as after a restart, with the volatile
    // activation lock gone — still attributes the parked job to `w1`.
    let mut replayed = State::new();
    for event in &log {
        state::apply(&mut replayed, event);
    }
    let job = replayed.jobs.get(&job_key).unwrap();
    assert_eq!(job.state, state::JobState::Failed);
    assert_eq!(job.worker.as_deref(), Some("w1"));
}

#[test]
fn should_recover_the_activating_worker_of_a_terminally_errored_job_via_replay() {
    // #959 — mirror of the failed-job replay guard for the `throwError` path: an
    // uncaught thrown error parks the job in `Errored`, and (like `JobFailed`) the
    // activating `worker` is carried *on* `JobErrorThrown` so it survives a restart
    // replay where the volatile, unexported `JobActivated` lock is gone. This guards
    // against a serialization/replay regression silently making errored jobs
    // anonymous.
    let mut engine = Engine::new();
    let mut log: Vec<Event> = Vec::new();
    log.extend(
        engine
            .apply_command(Command::DeployProcess(linear_with_task()))
            .unwrap(),
    );
    log.extend(
        engine
            .apply_command(Command::create_instance("order"))
            .unwrap(),
    );
    let job_key = engine.activate_jobs("payment", "w1", 10, 60_000, 0)[0].key;
    let throw_log = engine
        .apply_command(Command::throw_job_error(job_key, "UNCAUGHT", "kaboom"))
        .unwrap();

    // The exported terminal event carries the activating worker (and, being
    // volatile, `activate_jobs` emitted no `JobActivated` into the durable log).
    assert!(throw_log.iter().any(|e| matches!(
        e,
        Event::JobErrorThrown { worker: Some(w), .. } if w == "w1"
    )));
    log.extend(throw_log);
    assert!(!log.iter().any(|e| matches!(e, Event::JobActivated { .. })));

    // Replaying the durable stream — exactly as after a restart, with the volatile
    // activation lock gone — still attributes the errored job to `w1`.
    let mut replayed = State::new();
    for event in &log {
        state::apply(&mut replayed, event);
    }
    let job = replayed.jobs.get(&job_key).unwrap();
    assert_eq!(job.state, state::JobState::Errored);
    assert_eq!(job.worker.as_deref(), Some("w1"));
}

#[test]
fn should_reject_failing_a_job_that_was_never_activated() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let job_key = engine.pending_jobs()[0].key;

    let err = engine
        .apply_command(Command::fail_job(job_key, 1, "nope"))
        .unwrap_err();
    assert_eq!(err, EngineError::JobNotActivated { job_key });
}

#[test]
fn should_reject_throwing_an_error_from_a_job_that_was_never_activated() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_error_boundary()))
        .unwrap();
    engine
        .apply_command(Command::create_instance("payment"))
        .unwrap();
    let job_key = engine.pending_jobs()[0].key;

    let err = engine
        .apply_command(Command::throw_job_error(job_key, "CARD_DECLINED", "x"))
        .unwrap_err();
    assert_eq!(err, EngineError::JobNotActivated { job_key });
}

#[test]
fn should_resolve_feel_variable_reference_job_type_at_job_creation() {
    // given a process whose service task type is a FEEL variable reference
    let def = ProcessBuilder::new("dynamic")
        .start_event("start")
        .service_task("work", "=jobType")
        .end_event("end")
        .connect("start", "work")
        .connect("work", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    // when an instance is created with jobType bound to a concrete value
    let mut vars = HashMap::new();
    vars.insert("jobType".to_string(), Value::Str("payment".to_string()));
    engine
        .apply_command(Command::create_instance_with("dynamic", vars))
        .unwrap();

    // then the created job carries the resolved type, not the literal "=jobType"
    let jobs = engine.pending_jobs();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].job_type, "payment");
    // and it is activatable by the resolved type
    assert_eq!(engine.activate_jobs("payment", "w", 10, 1_000, 0).len(), 1);
}

#[test]
fn should_fall_back_to_literal_when_job_type_variable_is_missing() {
    // given the same process but no jobType variable provided
    let def = ProcessBuilder::new("dynamic")
        .start_event("start")
        .service_task("work", "=jobType")
        .end_event("end")
        .connect("start", "work")
        .connect("work", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    engine
        .apply_command(Command::create_instance("dynamic"))
        .unwrap();

    // then the unresolved expression falls back to the literal text (no panic)
    let jobs = engine.pending_jobs();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].job_type, "=jobType");
}

#[test]
fn should_reject_unknown_job() {
    let mut engine = Engine::new();
    let err = engine.apply_command(Command::complete_job(42)).unwrap_err();
    assert_eq!(err, EngineError::JobNotFound { job_key: 42 });
}

#[test]
fn should_reject_completing_a_job_that_was_never_activated() {
    // given an instance parked on a service task with a created (but
    // un-activated) job
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let job_key = engine.pending_jobs()[0].key;

    // when it is completed without being activated first
    let err = engine
        .apply_command(Command::complete_job(job_key))
        .unwrap_err();

    // then it is rejected
    assert_eq!(err, EngineError::JobNotActivated { job_key });
}

#[test]
fn lenient_completion_accepts_a_job_that_was_never_activated() {
    // given a replica engine in lenient-completion mode (leader-local
    // activation: this replica never saw the job activated)
    let mut engine = Engine::new();
    engine.set_lenient_completion(true);
    assert!(engine.lenient_completion());
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let job_key = engine.pending_jobs()[0].key;

    // when a replicated completion arrives for the un-activated job, it is
    // applied (the leader held the lock; possession of the key is the
    // capability) instead of being rejected as JobNotActivated
    engine
        .apply_command(Command::complete_job(job_key))
        .unwrap();

    // then the job is gone and the instance advanced past the service task
    assert!(engine.pending_jobs().is_empty());
}

#[test]
fn should_lock_an_activated_job_until_its_deadline() {
    // given an instance parked on a service task
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();

    // when worker A activates the job at t=0 for 1000ms
    let activated = engine.activate_jobs("payment", "A", 10, 1_000, 0);
    assert_eq!(activated.len(), 1);
    assert_eq!(activated[0].worker, "A");
    assert_eq!(activated[0].deadline, 1_000);

    // then a second activation before the deadline gets nothing
    assert!(engine
        .activate_jobs("payment", "B", 10, 1_000, 500)
        .is_empty());

    // and once the lock has expired and the periodic expiry tick reclaims it,
    // the job is activatable again
    engine.expire_jobs(1_500);
    let reactivated = engine.activate_jobs("payment", "B", 10, 1_000, 1_500);
    assert_eq!(reactivated.len(), 1);
    assert_eq!(reactivated[0].worker, "B");
}

#[test]
fn should_extend_a_job_lock_past_its_original_deadline() {
    // given worker A activated the job at t=0 for 1000ms (deadline=1000)
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let job_key = engine.activate_jobs("payment", "A", 10, 1_000, 0)[0].key;
    assert_eq!(engine.job(job_key).unwrap().deadline, Some(1_000));

    // when A extends the lock by 5000ms at t=800 (before the original deadline)
    engine
        .apply_command_at(Command::update_job_timeout(job_key, 5_000), 800)
        .unwrap();

    // then the deadline moved out to now + timeout = 5800
    assert_eq!(engine.job(job_key).unwrap().deadline, Some(5_800));

    // and the periodic expiry tick fired at the ORIGINAL deadline no longer
    // reclaims the job, so another worker still cannot activate it
    engine.expire_jobs(1_500);
    assert!(engine
        .activate_jobs("payment", "B", 10, 1_000, 1_500)
        .is_empty());

    // only once the EXTENDED deadline passes is the lock released and the job
    // redelivered
    engine.expire_jobs(5_900);
    let reactivated = engine.activate_jobs("payment", "B", 10, 1_000, 5_900);
    assert_eq!(reactivated.len(), 1);
    assert_eq!(reactivated[0].key, job_key);
}

#[test]
fn should_reject_a_lock_extension_for_an_unknown_job() {
    let mut engine = Engine::new();
    let err = engine
        .apply_command(Command::update_job_timeout(999, 5_000))
        .unwrap_err();
    assert_eq!(err, EngineError::JobNotFound { job_key: 999 });
}

/// #592 follow-up #4: `JobUpdateRequest.operationReference` must be threaded
/// onto the emitted update events (audit correlation), for BOTH the retries and
/// the timeout changeset fields — not silently dropped.
#[test]
fn should_thread_operation_reference_onto_job_update_events() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let job_key = engine.activate_jobs("payment", "A", 10, 60_000, 0)[0].key;

    // when a retries update carries an operation reference
    let events = engine
        .apply_command(Command::update_job_retries_with_ref(job_key, 5, Some(4242)))
        .unwrap();
    // then the reference lands on the JobRetriesUpdated event
    assert!(events.iter().any(|e| matches!(
        e,
        Event::JobRetriesUpdated {
            operation_reference: Some(4242),
            ..
        }
    )));

    // and likewise for a timeout (lock-extension) update
    let events = engine
        .apply_command_at(
            Command::update_job_timeout_with_ref(job_key, 5_000, Some(9001)),
            100,
        )
        .unwrap();
    assert!(events.iter().any(|e| matches!(
        e,
        Event::JobTimeoutUpdated {
            operation_reference: Some(9001),
            ..
        }
    )));

    // class-scoped: an update WITHOUT a reference leaves the field None (not a
    // defaulted zero), so the audit trail distinguishes "no ref" from "ref 0".
    let events = engine
        .apply_command(Command::update_job_retries(job_key, 7))
        .unwrap();
    assert!(events.iter().any(|e| matches!(
        e,
        Event::JobRetriesUpdated {
            operation_reference: None,
            ..
        }
    )));
}

/// #592 follow-up: `updateJob` may carry BOTH `retries` and `timeout` in one
/// changeset, applied by the server as two engine commands. The server applies
/// `timeout` first so the combined update never partially applies then fails.
/// That ordering is only sound because of an engine invariant: any job for which
/// a lock extension (`UpdateJobTimeout`) succeeds is Activated, and an Activated
/// job is never terminal, so `UpdateJobRetries` on it also succeeds. This test
/// pins that invariant — if retries were ever tightened to reject Activated
/// jobs, the partial-apply hazard would return and this guard would fail.
#[test]
fn activated_job_accepts_both_timeout_then_retries_updates() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let job_key = engine.activate_jobs("payment", "A", 10, 1_000, 0)[0].key;

    // A lock extension succeeds only while Activated...
    engine
        .apply_command_at(Command::update_job_timeout(job_key, 5_000), 100)
        .unwrap();
    // ...and because the job is still Activated (not terminal), the retries
    // update that the server applies next is guaranteed to succeed too.
    engine
        .apply_command(Command::update_job_retries(job_key, 3))
        .unwrap();
}

#[test]
fn should_let_a_previous_worker_complete_after_re_activation() {
    // given worker A activated the job, then its lock expired and worker B
    // re-activated it (e.g. A's work outran the activation window)
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let events = engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    let job_key = engine.activate_jobs("payment", "A", 10, 1_000, 0)[0].key;
    // A's lock expires; the periodic expiry tick returns the job to the pool.
    engine.expire_jobs(1_500);
    let reactivated = engine.activate_jobs("payment", "B", 10, 1_000, 1_500);
    assert_eq!(reactivated[0].key, job_key);

    // when the slow worker A finally completes the job by key
    engine
        .apply_command(Command::complete_job(job_key))
        .unwrap();

    // then completion succeeds and the instance finishes
    assert!(engine.is_completed(instance_key));

    // and B can no longer complete the already-completed job
    let err = engine
        .apply_command(Command::complete_job(job_key))
        .unwrap_err();
    assert_eq!(err, EngineError::JobNotActive { job_key });
}

#[test]
fn should_dispatch_jobs_to_a_callback_worker() {
    // given an instance parked on a service task
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let events = engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    // when a callback worker polls and handles the job
    let mut seen = Vec::new();
    let handled = engine.poll_jobs("payment", "cb", 10, 60_000, 0, |job| {
        seen.push(job.job_type.clone());
        Some(HashMap::new())
    });

    // then the job was dispatched and completed, finishing the instance
    assert_eq!(handled, 1);
    assert_eq!(seen, ["payment"]);
    assert!(engine.is_completed(instance_key));
}

#[test]
fn job_index_tracks_create_activate_complete_expire_and_evict() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();

    // Three instances → three activatable "payment" jobs, indexed in key
    // order.
    let mut instances = Vec::new();
    for _ in 0..3 {
        let k = engine
            .apply_command(Command::create_instance("order"))
            .unwrap()
            .iter()
            .find_map(|e| e.instance_key())
            .unwrap();
        instances.push(k);
    }
    assert_eq!(engine.state().activatable_jobs["payment"].len(), 3);
    assert_job_index_consistent(&engine);

    // Activating removes the job from the activatable index (it is now
    // locked); a lock that expires re-adds it via `JobLockExpired`. The index
    // iterates by key ascending and holds only `Created` jobs.
    let first = engine.activate_jobs("payment", "A", 1, 1_000, 0);
    assert_eq!(first.len(), 1);
    assert_eq!(engine.state().activatable_jobs["payment"].len(), 2);
    assert_job_index_consistent(&engine);

    // Completing the activated job leaves the index unchanged (it was already
    // de-indexed at activation).
    engine
        .apply_command(Command::complete_job(first[0].key))
        .unwrap();
    assert_eq!(engine.state().activatable_jobs["payment"].len(), 2);
    assert_job_index_consistent(&engine);

    // Expiry of another worker's lock returns the job to the index.
    let locked = engine.activate_jobs("payment", "B", 1, 1_000, 0)[0].key;
    engine.expire_jobs(5_000);
    assert!(engine.state().activatable_jobs["payment"]
        .iter()
        .any(|&(_, k)| k == locked));
    assert_job_index_consistent(&engine);

    // Evicting a completed instance drops its (already-deindexed) job and
    // leaves the index consistent.
    engine.evict_instances(&instances);
    assert_job_index_consistent(&engine);
}

#[test]
fn cancel_terminates_instance_and_cancels_its_job() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let instance_key = engine
        .apply_command(Command::create_instance("order"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    // Token parked on the service-task job.
    let job_key = engine
        .state()
        .jobs
        .values()
        .find(|j| j.instance_key == instance_key)
        .unwrap()
        .key;
    assert!(!engine.is_completed(instance_key));

    let events = engine
        .apply_command(Command::cancel_instance(instance_key))
        .unwrap();

    // The job is cancelled and the instance is terminated (not completed).
    assert!(events.contains(&Event::JobCanceled {
        job_key,
        instance_key
    }));
    assert!(events.contains(&Event::ProcessInstanceTerminated { instance_key }));
    assert!(!events
        .iter()
        .any(|e| matches!(e, Event::ProcessInstanceCompleted { .. })));
    assert_eq!(
        engine.instance(instance_key).unwrap().state,
        ProcessInstanceState::Terminated
    );
    assert!(engine.instance(instance_key).unwrap().active.is_empty());
    assert_eq!(
        engine.job(job_key).unwrap().state,
        state::JobState::Canceled
    );
}

#[test]
fn input_mapping_merges_before_job_activation() {
    // A service task with an input mapping `y = x + 1`. On activation the mapped
    // variable is merged, so a worker that activates the job sees it.
    let def = ProcessBuilder::new("io-in")
        .start_event("s")
        .service_task("t", "work")
        .with_io(
            "t",
            crate::model::IoMapping {
                inputs: vec![crate::model::Mapping {
                    source: "=x + 1".to_string(),
                    target: "y".to_string(),
                }],
                outputs: Vec::new(),
            },
        )
        .end_event("e")
        .connect("s", "t")
        .connect("t", "e")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let mut vars = HashMap::new();
    vars.insert("x".to_string(), Value::Int(1));
    let inst = engine
        .apply_command(Command::create_instance_with("io-in", vars))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    assert_eq!(
        io_var(&engine, inst, "y"),
        None,
        "an input mapping creates a variable LOCAL to the activity scope, not at the instance root"
    );
    // The activated job still sees the mapped variable (its own scope shadows root).
    let job = &engine.activate_jobs("work", "w", 1, 60_000, 0)[0];
    assert_eq!(job.variables.get("y"), Some(&Value::Int(2)));
}

#[test]
fn output_mapping_projects_job_result() {
    // A service task with an output mapping `approved = result.ok`. The job
    // completes with `result`, and the mapping projects a renamed variable.
    let def = ProcessBuilder::new("io-out")
        .start_event("s")
        .service_task("t", "work")
        .with_io(
            "t",
            crate::model::IoMapping {
                inputs: Vec::new(),
                outputs: vec![crate::model::Mapping {
                    source: "=result.ok".to_string(),
                    target: "approved".to_string(),
                }],
            },
        )
        .end_event("e")
        .connect("s", "t")
        .connect("t", "e")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let inst = create_instance_key(&mut engine, "io-out");
    let job_key = engine.activate_jobs("work", "w", 1, 60_000, 0)[0].key;
    let mut result = std::collections::BTreeMap::new();
    result.insert("ok".to_string(), Value::Bool(true));
    let mut job_vars = HashMap::new();
    job_vars.insert("result".to_string(), Value::Map(result));
    let events = engine
        .apply_command(Command::complete_job_with(job_key, job_vars))
        .unwrap();
    // The process runs to completion (clearing instance variables), so assert the
    // output mapping surfaced on a VariablesUpdated event for this instance.
    let mapped = events.iter().any(|e| {
        matches!(
            e,
            Event::VariablesUpdated { instance_key, variables }
                if *instance_key == inst
                    && variables.get("approved") == Some(&Value::Bool(true))
        )
    });
    assert!(
        mapped,
        "output mapping should set approved=true; events: {events:?}"
    );
}

#[test]
fn job_type_expression_resolves_against_the_enclosing_sub_process_scope() {
    // A service task inside a sub-process resolves its `=FEEL` job type against
    // the sub-process's local variables (an input mapping), not just the root —
    // if scoping leaked, `region` would be unresolved and the type would fall
    // back to the literal expression text.
    let model = ProcessBuilder::new("scoped-jt")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .with_io(
            "sub",
            crate::model::IoMapping {
                inputs: vec![crate::model::Mapping {
                    source: "=\"north\"".to_string(),
                    target: "region".to_string(),
                }],
                outputs: vec![],
            },
        )
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .service_task("inner", "=\"worker-\" + region")
        .contained_in("inner", "sub")
        .end_event("sub_end")
        .contained_in("sub_end", "sub")
        .end_event("done")
        .connect("start", "sub")
        .connect("sub_start", "inner")
        .connect("inner", "sub_end")
        .connect("sub", "done")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(model)).unwrap();
    create_instance_key(&mut engine, "scoped-jt");

    // The inner job was created with the scope-resolved type.
    let jobs = engine.activate_jobs("worker-north", "w", 10, 60_000, 0);
    assert_eq!(
        jobs.len(),
        1,
        "job type resolved against the sub-process scope"
    );
    // And is NOT sitting under the literal fallback type.
    assert!(engine
        .activate_jobs("=\"worker-\" + region", "w", 10, 60_000, 0)
        .is_empty());
}

// zeebe-cells: element:BusinessRuleTask
#[test]
fn business_rule_task_evaluates_decision_and_binds_result_variable() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployDecisionRequirements(vec![greeting_dmn()]))
        .unwrap();
    engine
        .apply_command(Command::DeployProcess(brt_process(
            "greeting",
            Some("greeting".to_string()),
        )))
        .unwrap();

    let mut vars = HashMap::new();
    vars.insert("lang".to_string(), Value::Str("de".into()));
    let events = engine
        .apply_command(Command::create_instance_with("brt", vars))
        .unwrap();
    let inst = events.iter().find_map(|e| e.instance_key()).unwrap();

    // The decision output was bound under the result variable...
    assert_eq!(
        merged_var(&events, "greeting"),
        Some(Value::Str("hallo".into()))
    );
    // ...a DecisionEvaluated audit record was emitted...
    assert!(events.iter().any(|e| matches!(
        e,
        Event::DecisionEvaluated { decision_id, .. } if decision_id == "greeting"
    )));
    // ...and the instance ran straight through to completion (no job, no wait).
    assert_eq!(
        engine.instance(inst).map(|i| i.state),
        Some(ProcessInstanceState::Completed)
    );
}

/// A two-decision DRG (#1292): `root` requires `base`; `root`'s UNIQUE table
/// fails (two rules match) when `lang` is `"xx"`.
fn requiring_drg(drg_id: &str) -> crate::dmn::DecisionRequirementsGraph {
    let xml = format!(
        r##"<definitions xmlns="https://www.omg.org/spec/DMN/20191111/MODEL/" id="{drg_id}" name="{drg_id}">
      <decision id="base" name="Base">
        <literalExpression id="bl"><text>"b"</text></literalExpression>
      </decision>
      <decision id="root" name="Root">
        <informationRequirement id="ir"><requiredDecision href="#base" /></informationRequirement>
        <decisionTable hitPolicy="UNIQUE">
          <input id="ri"><inputExpression id="re" typeRef="string"><text>lang</text></inputExpression></input>
          <output id="ro" name="result" typeRef="string" />
          <rule id="r1"><inputEntry id="rie1"><text>"xx"</text></inputEntry>
            <outputEntry id="roe1"><text>"a"</text></outputEntry></rule>
          <rule id="r2"><inputEntry id="rie2"><text>"xx"</text></inputEntry>
            <outputEntry id="roe2"><text>"b"</text></outputEntry></rule>
          <rule id="r3"><inputEntry id="rie3"><text>not("xx")</text></inputEntry>
            <outputEntry id="roe3"><text>base</text></outputEntry></rule>
        </decisionTable>
      </decision>
    </definitions>"##
    );
    crate::dmn::parse_dmn(&xml).unwrap()
}

/// The deployed decision key of `decision_id` from a deploy's events.
fn deployed_decision_key(events: &[Event], decision_id: &str) -> crate::state::Key {
    events
        .iter()
        .find_map(|e| match e {
            Event::DecisionDeployed {
                decision_key,
                decision_id: id,
                ..
            } if id == decision_id => Some(*decision_key),
            _ => None,
        })
        .expect("decision deployed")
}

// zeebe-cells: element:BusinessRuleTask
#[test]
fn business_rule_task_failure_records_a_failed_evaluation_before_the_incident() {
    // #1292 (Zeebe `DecisionEvaluation:FAILED`): a failed evaluation is still a
    // decision evaluation — journaled with its own key and the failure, ahead of
    // the incident — and its audit trail ends with the failed decision.
    let mut engine = Engine::new();
    let deployed = engine
        .apply_command(Command::DeployDecisionRequirements(vec![requiring_drg(
            "drg",
        )]))
        .unwrap();
    let (base_key, root_key) = (
        deployed_decision_key(&deployed, "base"),
        deployed_decision_key(&deployed, "root"),
    );
    engine
        .apply_command(Command::DeployProcess(brt_process(
            "root",
            Some("out".to_string()),
        )))
        .unwrap();
    let mut vars = HashMap::new();
    vars.insert("lang".to_string(), Value::Str("xx".into()));
    let events = engine
        .apply_command(Command::create_instance_with("brt", vars))
        .unwrap();

    let evaluated_at = events
        .iter()
        .position(|e| matches!(e, Event::DecisionEvaluated { .. }))
        .expect("the failed evaluation is journaled");
    let incident_at = events
        .iter()
        .position(|e| matches!(e, Event::IncidentRaised { .. }))
        .expect("the failure raises an incident");
    assert!(
        evaluated_at < incident_at,
        "evaluation precedes the incident"
    );
    let Event::DecisionEvaluated {
        decision_key,
        decision_evaluation_key,
        failure,
        evaluated_decisions,
        ..
    } = &events[evaluated_at]
    else {
        unreachable!()
    };
    assert_eq!(*decision_key, root_key);
    assert_ne!(*decision_evaluation_key, 0, "a key is minted");
    assert_eq!(
        failure.as_ref().map(|f| f.failed_decision_id.as_str()),
        Some("root")
    );
    let trail: Vec<_> = evaluated_decisions
        .iter()
        .map(|d| (d.decision_id.as_str(), d.decision_key, d.decision_version))
        .collect();
    assert_eq!(trail, [("base", base_key, 1), ("root", root_key, 1)]);

    // Retrying (after fixing nothing) is a NEW evaluation with its own key.
    let incident_key = events
        .iter()
        .find_map(|e| match e {
            Event::IncidentRaised { incident_key, .. } => Some(*incident_key),
            _ => None,
        })
        .unwrap();
    let retried = engine
        .apply_command(Command::ResolveIncident {
            incident_key,
            operation_reference: None,
        })
        .unwrap();
    let retry_key = retried
        .iter()
        .find_map(Event::decision_evaluation_key)
        .expect("the re-drive evaluates again");
    assert_ne!(retry_key, *decision_evaluation_key);
}

#[test]
fn evaluated_decisions_are_stamped_within_the_evaluated_drg() {
    // #1292: each evaluated decision is identified within the DRG version that
    // was evaluated, not by the latest deployment of its id — here a second DRG
    // later takes over `base`, but `root`'s evaluation still used its own.
    let mut engine = Engine::new();
    let first = engine
        .apply_command(Command::DeployDecisionRequirements(vec![requiring_drg(
            "first",
        )]))
        .unwrap();
    let own_base = deployed_decision_key(&first, "base");
    let second = engine
        .apply_command(Command::DeployDecisionRequirements(vec![requiring_drg(
            "second",
        )]))
        .unwrap();
    assert_ne!(deployed_decision_key(&second, "base"), own_base);
    let first_root = deployed_decision_key(&first, "root");

    let mut vars = HashMap::new();
    vars.insert("lang".to_string(), Value::Str("de".into()));
    let events = engine
        .apply_command(Command::EvaluateDecision {
            decision: crate::command::DecisionReference::Key(first_root),
            variables: vars,
        })
        .unwrap();
    let [Event::DecisionEvaluated {
        evaluated_decisions,
        failure: None,
        instance_key: 0,
        decision_requirements_id,
        decision_requirements_key,
        ..
    }] = events.as_slice()
    else {
        panic!("expected one successful standalone evaluation, got {events:?}")
    };
    assert_eq!(decision_requirements_id, "first");
    assert_eq!(
        *decision_requirements_key,
        engine
            .state()
            .decision_by_key(first_root)
            .unwrap()
            .decision_requirements_key
    );
    assert_eq!(evaluated_decisions[0].decision_id, "base");
    assert_eq!(evaluated_decisions[0].decision_key, own_base);
    assert_eq!(evaluated_decisions[1].decision_key, first_root);
}

#[test]
fn standalone_evaluation_records_failures_and_rejects_unknown_decisions() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployDecisionRequirements(vec![requiring_drg(
            "drg",
        )]))
        .unwrap();
    let mut vars = HashMap::new();
    vars.insert("lang".to_string(), Value::Str("xx".into()));
    // A failed evaluation is recorded (with its failure), not rejected.
    let events = engine
        .apply_command(Command::EvaluateDecision {
            decision: crate::command::DecisionReference::Id("root".to_string()),
            variables: vars.clone(),
        })
        .unwrap();
    assert!(
        matches!(
            events.as_slice(),
            [Event::DecisionEvaluated {
                failure: Some(_),
                ..
            }]
        ),
        "{events:?}"
    );
    // An unknown decision is a typed rejection with nothing journaled.
    let err = engine
        .apply_command(Command::EvaluateDecision {
            decision: crate::command::DecisionReference::Id("nope".to_string()),
            variables: vars,
        })
        .unwrap_err();
    assert_eq!(
        err,
        crate::EngineError::DecisionNotFound {
            reference: "id 'nope'".to_string()
        }
    );
}

#[test]
fn business_rule_task_resolves_decision_id_via_feel_expression() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployDecisionRequirements(vec![greeting_dmn()]))
        .unwrap();
    engine
        .apply_command(Command::DeployProcess(brt_process(
            "=decisionToCall",
            Some("out".to_string()),
        )))
        .unwrap();

    let mut vars = HashMap::new();
    vars.insert("lang".to_string(), Value::Str("en".into()));
    vars.insert("decisionToCall".to_string(), Value::Str("greeting".into()));
    let events = engine
        .apply_command(Command::create_instance_with("brt", vars))
        .unwrap();
    let inst = events.iter().find_map(|e| e.instance_key()).unwrap();
    assert_eq!(merged_var(&events, "out"), Some(Value::Str("hello".into())));
    let _ = inst;
}

#[test]
fn deploy_decision_requirements_indexes_decisions_and_is_idempotent() {
    let mut engine = Engine::new();
    let events = engine
        .apply_command(Command::DeployDecisionRequirements(vec![greeting_dmn()]))
        .unwrap();
    assert!(events
        .iter()
        .any(|e| matches!(e, Event::DecisionRequirementsDeployed { .. })));
    assert!(events.iter().any(|e| matches!(
        e,
        Event::DecisionDeployed { decision_id, version, .. } if decision_id == "greeting" && *version == 1
    )));
    assert!(engine.state().decisions.contains_key("greeting"));

    // Redeploying the identical DRG is a no-op (only DeploymentCreated).
    let again = engine
        .apply_command(Command::DeployDecisionRequirements(vec![greeting_dmn()]))
        .unwrap();
    assert!(!again
        .iter()
        .any(|e| matches!(e, Event::DecisionRequirementsDeployed { .. })));
    assert_eq!(engine.state().decision_requirements["drg"].version, 1);
}

#[test]
fn business_rule_task_spreads_map_output_without_result_variable() {
    // A two-output decision table yields a map output; with no result variable
    // its entries are spread into the instance scope.
    let xml = r##"<definitions xmlns="https://www.omg.org/spec/DMN/20191111/MODEL/" id="drg2" name="drg2">
      <decision id="scores" name="Scores">
        <decisionTable hitPolicy="UNIQUE">
          <input id="i1"><inputExpression id="e1" typeRef="string"><text>tier</text></inputExpression></input>
          <output id="o1" name="discount" typeRef="number" />
          <output id="o2" name="priority" typeRef="number" />
          <rule id="r1"><inputEntry id="ie1"><text>"gold"</text></inputEntry>
            <outputEntry id="oe1"><text>20</text></outputEntry>
            <outputEntry id="oe2"><text>1</text></outputEntry></rule>
        </decisionTable>
      </decision>
    </definitions>"##;
    let drg = crate::dmn::parse_dmn(xml).unwrap();
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployDecisionRequirements(vec![drg]))
        .unwrap();
    engine
        .apply_command(Command::DeployProcess(brt_process("scores", None)))
        .unwrap();

    let mut vars = HashMap::new();
    vars.insert("tier".to_string(), Value::Str("gold".into()));
    let events = engine
        .apply_command(Command::create_instance_with("brt", vars))
        .unwrap();
    let inst = events.iter().find_map(|e| e.instance_key()).unwrap();
    assert_eq!(merged_var(&events, "discount"), Some(Value::Int(20)));
    assert_eq!(merged_var(&events, "priority"), Some(Value::Int(1)));
    let _ = inst;
}

#[test]
fn agent_task_activation_creates_a_job_then_worker_registers_agent_instance() {
    // Deploying a serviceTask bearing zeebe:agentDefinition agentType="aiAgentTask"
    // creates an ordinary service-task job. Only explicit worker registration
    // mints an AgentInstance linked to the active elementInstanceKey.
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="agent-proc" isExecutable="true">
          <bpmn:startEvent id="start" />
          <bpmn:serviceTask id="agent">
            <bpmn:extensionElements>
              <zeebe:agentDefinition agentType="aiAgentTask" />
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:endEvent id="end" />
          <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="end" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml).unwrap().remove(0);

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    let events = engine
        .apply_command(Command::create_instance("agent-proc"))
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    // The agent element activated (its token parks, like a service task).
    let agent_eik = events
        .iter()
        .find_map(|e| match e {
            Event::ElementActivated {
                element_instance_key,
                element_id,
                ..
            } if element_id == "agent" => Some(*element_instance_key),
            _ => None,
        })
        .expect("the agent element should activate");

    assert!(events
        .iter()
        .any(|e| matches!(e, Event::JobCreated { element_id, .. } if element_id == "agent")));
    assert!(!events
        .iter()
        .any(|e| matches!(e, Event::AgentInstanceCreated { .. })));
    assert!(engine.state.instances[&instance_key]
        .agent_instances
        .is_empty());

    let agent_instance = register_job_backed_agent(&mut engine, "agent");
    assert_eq!(
        agent_instance.status,
        crate::agent::AgentInstanceStatus::Initializing
    );
    assert_eq!(agent_instance.element_instance_key, agent_eik);
    assert_eq!(agent_instance.process_instance_key, instance_key);
    assert_eq!(
        agent_instance.agent_type,
        crate::agent::AgentType::AiAgentTask
    );
    assert_ne!(
        agent_instance.agent_instance_key, agent_eik,
        "the AgentInstance must have its own dedicated key, distinct from the element instance"
    );
    assert_ne!(agent_instance.agent_instance_key, 0);

    // The instance is the system-of-record: it is held in engine state.
    let stored = engine
        .state
        .instances
        .get(&instance_key)
        .and_then(|pi| pi.agent_instances.get(&agent_instance.agent_instance_key))
        .expect("the AgentInstance should be stored on the process instance");
    assert_eq!(
        stored.status,
        crate::agent::AgentInstanceStatus::Initializing
    );

    let job = &engine.state.jobs[&agent_instance.job_key];
    assert_eq!(job.element_instance_key, agent_eik);
    assert_eq!(job.lease_token, Some(agent_instance.job_lease.clone()));

    let events = engine
        .apply_command(Command::CompleteAgentInstance {
            agent_instance_key: agent_instance.agent_instance_key,
        })
        .unwrap();
    assert!(events
        .iter()
        .any(|e| matches!(e, Event::AgentInstanceCompleted { .. })));
    assert!(!events
        .iter()
        .any(|e| matches!(e, Event::ElementCompleted { .. })));
    assert!(engine.state.jobs.contains_key(&agent_instance.job_key));

    let events = engine
        .apply_command(
            Command::complete_job(agent_instance.job_key).with_job_lease(agent_instance.job_lease),
        )
        .unwrap();
    assert!(events
        .iter()
        .any(|e| matches!(e, Event::ElementCompleted { element_id, .. } if element_id == "agent")));
    assert!(events
        .iter()
        .any(|e| matches!(e, Event::ElementActivated { element_id, .. } if element_id == "end")));
}

#[test]
fn external_agent_activation_creates_a_job_and_no_agent_instance() {
    // Camunda parity (#1099): an `external` agent is job-backed. On activation it
    // creates a normal job (activatable through the standard job loop) and does
    // NOT auto-mint an AgentInstance — the worker mints it via CREATE.
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="agent-proc" isExecutable="true">
          <bpmn:startEvent id="start" />
          <bpmn:serviceTask id="agent">
            <bpmn:extensionElements>
              <zeebe:agentDefinition agentType="external" />
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:endEvent id="end" />
          <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="end" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml).unwrap().remove(0);
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let events = engine
        .apply_command(Command::create_instance("agent-proc"))
        .unwrap();

    // A job was created on activation.
    let job_created = events
        .iter()
        .any(|e| matches!(e, Event::JobCreated { element_id, .. } if element_id == "agent"));
    assert!(
        job_created,
        "an external agent must create a job on activation"
    );

    // NO AgentInstance was auto-minted.
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, Event::AgentInstanceCreated { .. })),
        "an external agent must not auto-mint an AgentInstance"
    );

    // The job is activatable through the standard job loop (job type = element id).
    let activated = engine.activate_jobs("agent", "W", 10, 1_000, 0);
    assert_eq!(
        activated.len(),
        1,
        "the external agent's job is activatable"
    );
    assert_eq!(activated[0].element_id, "agent");
}

#[test]
fn external_agent_lease_gated_create_mints_on_a_valid_job_lease() {
    use crate::agent::{AgentDefinition, AgentInstanceStatus};
    let (mut engine, pi, eik) = external_agent_instance();

    // The worker activates the agent job (standard job loop) and learns its
    // lease deadline — the lease "token".
    let job = engine
        .activate_jobs_with_options("agent", "W", 1, 1_000, 100, lease_options())
        .pop()
        .expect("the external agent's job is activatable");

    // A lease-gated CREATE that references the ACTIVATED job with the matching
    // lease and elementInstanceKey mints the AgentInstance (INITIALIZING).
    let events = engine
        .apply_command(Command::CreateAgentInstance {
            element_instance_key: eik,
            job_key: job.key,
            job_lease: job
                .lease_token
                .clone()
                .expect("external agent job carries a lease"),
            definition: AgentDefinition {
                model: Some("gpt-4o".to_string()),
                ..Default::default()
            },
            limits: None,
            history: vec![],
        })
        .unwrap();
    let created = events
        .iter()
        .find_map(|e| match e {
            Event::AgentInstanceCreated { agent_instance, .. } => Some(agent_instance.clone()),
            _ => None,
        })
        .expect("a valid lease-gated CREATE mints the AgentInstance");
    assert_eq!(created.status, AgentInstanceStatus::Initializing);
    assert_eq!(created.element_instance_key, eik);
    assert_eq!(created.agent_type, crate::agent::AgentType::External);
    // The validated job lease is recorded on the AgentInstance.
    assert_eq!(created.job_key, job.key);
    assert_eq!(
        created.job_lease,
        job.lease_token
            .clone()
            .expect("external agent job carries a lease")
    );
    assert_eq!(
        stored_agent_instance(&engine, pi, created.agent_instance_key).status,
        AgentInstanceStatus::Initializing
    );
}

#[test]
fn external_agent_create_without_an_activated_job_is_rejected() {
    use crate::agent::AgentDefinition;
    let (mut engine, _pi, eik) = external_agent_instance();

    // The job exists but has NOT been activated: no lease to gate on → reject.
    let job_key = engine
        .state
        .jobs
        .values()
        .find(|j| j.element_instance_key == eik)
        .map(|j| j.key)
        .expect("the external agent has a job");
    let err = engine
        .apply_command(Command::CreateAgentInstance {
            element_instance_key: eik,
            job_key,
            job_lease: "not-activated".to_string(),
            definition: AgentDefinition::default(),
            limits: None,
            history: vec![],
        })
        .unwrap_err();
    assert!(
        matches!(err, EngineError::AgentInstanceJobNotActive { .. }),
        "CREATE on a non-activated job must be rejected, got {err:?}"
    );
    assert!(
        engine
            .state
            .instances
            .get(&_pi)
            .map(|pi| pi.agent_instances.is_empty())
            .unwrap_or(true),
        "a rejected CREATE mints nothing"
    );
}

#[test]
fn external_agent_job_completion_advances_the_token() {
    use crate::agent::AgentDefinition;
    let (mut engine, _pi, eik) = external_agent_instance();
    let job = engine
        .activate_jobs_with_options("agent", "W", 1, 1_000, 100, lease_options())
        .pop()
        .expect("activatable");
    engine
        .apply_command(Command::CreateAgentInstance {
            element_instance_key: eik,
            job_key: job.key,
            job_lease: job
                .lease_token
                .clone()
                .expect("external agent job carries a lease"),
            definition: AgentDefinition::default(),
            limits: None,
            history: vec![],
        })
        .unwrap();

    // Completing the agent job resumes the parked token and routes the outgoing
    // flow, exactly like a service task — the harness drives the standard loop.
    let events = engine
        .apply_command(Command::complete_job(job.key).with_job_lease(job.lease_token.unwrap()))
        .unwrap();
    assert!(
        events.iter().any(
            |e| matches!(e, Event::ElementCompleted { element_id, .. } if element_id == "agent")
        ),
        "the agent element completes when its job completes"
    );
    assert!(
        events.iter().any(
            |e| matches!(e, Event::ElementActivated { element_id, .. } if element_id == "end")
        ),
        "the token advances to the end event"
    );
}

#[test]
fn external_agent_refreshes_job_lease_across_reactivation() {
    use crate::agent::{AgentDefinition, AgentHistoryRole};
    let (mut engine, pi, eik) = external_agent_instance();

    // First activation → lease token L1; a lease-gated CREATE records it.
    let job1 = engine
        .activate_jobs_with_options("agent", "W", 1, 1_000, 100, lease_options())
        .pop()
        .expect("activatable");
    let created = engine
        .apply_command(Command::CreateAgentInstance {
            element_instance_key: eik,
            job_key: job1.key,
            job_lease: job1
                .lease_token
                .clone()
                .expect("first activation carries a lease"),
            definition: AgentDefinition::default(),
            limits: None,
            history: vec![],
        })
        .unwrap();
    let aik = created
        .iter()
        .find_map(|e| match e {
            Event::AgentInstanceCreated { agent_instance, .. } => {
                Some(agent_instance.agent_instance_key)
            }
            _ => None,
        })
        .expect("CREATE mints");
    assert_eq!(
        stored_agent_instance(&engine, pi, aik).job_lease,
        job1.lease_token
            .clone()
            .expect("first activation carries a lease")
    );

    // The lease expires and the worker re-activates the SAME job, learning a
    // NEW lease token L2 (an opaque token, distinct from the deadline and from
    // the previous activation's token).
    engine
        .apply_command(Command::ExpireJobs { now: job1.deadline })
        .unwrap();
    let job2 = engine
        .activate_jobs_with_options("agent", "W", 1, 1_000, 5_000, lease_options())
        .pop()
        .expect("re-activatable after lease expiry");
    assert_eq!(job2.key, job1.key, "same job, fresh lease");
    assert_ne!(
        job2.lease_token, job1.lease_token,
        "re-activation mints a fresh lease token"
    );

    // CREATE still conflicts after reactivation; UPDATE owns reassociation.
    let err = engine
        .apply_command(Command::CreateAgentInstance {
            element_instance_key: eik,
            job_key: job2.key,
            job_lease: job2
                .lease_token
                .clone()
                .expect("second activation carries a lease"),
            definition: AgentDefinition::default(),
            limits: None,
            history: vec![],
        })
        .unwrap_err();
    assert!(matches!(
        err,
        EngineError::AgentInstanceAlreadyExists { .. }
    ));
    assert_eq!(
        stored_agent_instance(&engine, pi, aik).job_lease,
        job1.lease_token
            .clone()
            .expect("first activation carries a lease"),
        "a rejected CREATE must not change the registration"
    );

    // A history-bearing UPDATE under L2 is accepted AND likewise records the
    // freshly-validated lease token onto the snapshot.
    engine
        .apply_command(Command::UpdateAgentInstance {
            agent_instance_key: aik,
            element_instance_key: eik,
            element_id: "agent".to_string(),
            process_instance_key: pi,
            job_key: job2.key,
            job_lease: job2
                .lease_token
                .clone()
                .expect("second activation carries a lease"),
            status: None,
            metrics: Default::default(),
            tools: None,
            history: vec![worker_history_turn(1, 200, AgentHistoryRole::Assistant)],
        })
        .expect("a history-bearing UPDATE under the fresh lease is accepted");
    assert_eq!(
        stored_agent_instance(&engine, pi, aik).job_lease,
        job2.lease_token
            .clone()
            .expect("second activation carries a lease"),
        "a history-bearing UPDATE records the validated lease token"
    );
}

/// #1106 divergence 1 — because the lease token is independent of `deadline`, a
/// lock extension (`UpdateJobTimeout`, which moves `deadline` but is NOT a
/// re-activation) leaves the lease valid. The pre-#1106 engine, keying the lease
/// off `deadline`, would have spuriously rejected the worker's still-live lease
/// after any timeout extension.
#[test]
fn external_agent_lease_survives_a_job_timeout_extension() {
    use crate::agent::AgentDefinition;
    let (mut engine, _pi, eik) = external_agent_instance();
    let job = engine
        .activate_jobs_with_options("agent", "W", 1, 1_000, 100, lease_options())
        .pop()
        .expect("activatable");
    let lease = job.lease_token.expect("lease token");

    // Extend the lock — this moves the deadline but must NOT change the token.
    engine
        .apply_command_at(Command::update_job_timeout(job.key, 50_000), 200)
        .expect("a lock extension on an activated job succeeds");
    let moved_deadline = engine.job(job.key).and_then(|j| j.deadline);
    assert_ne!(
        moved_deadline,
        Some(job.deadline),
        "the timeout extension moved the deadline"
    );

    // The worker's original lease token still validates.
    engine
        .apply_command(Command::CreateAgentInstance {
            element_instance_key: eik,
            job_key: job.key,
            job_lease: lease,
            definition: AgentDefinition::default(),
            limits: None,
            history: vec![],
        })
        .expect("the lease survives a deadline-moving lock extension");
}

/// #1106 divergence 2 — an `external` CREATE is allowed to be **jobless**
/// (`job_key == 0`) when it carries no history batch (Camunda's `jobKey == -1`
/// short-circuit), but the job becomes required the moment a batch is attached.
#[test]
fn external_agent_create_is_jobless_only_without_history() {
    use crate::agent::{AgentDefinition, AgentHistoryRole};
    let (mut engine, _pi, eik) = external_agent_instance();

    // Jobless CREATE + no history → mints (no live job required yet).
    let ok = engine
        .apply_command(Command::CreateAgentInstance {
            element_instance_key: eik,
            job_key: 0,
            job_lease: String::new(),
            definition: AgentDefinition::default(),
            limits: None,
            history: vec![],
        })
        .expect("a jobless, history-free CREATE is allowed");
    assert!(
        ok.iter()
            .any(|e| matches!(e, Event::AgentInstanceCreated { .. })),
        "the jobless CREATE mints the AgentInstance"
    );

    // Jobless CREATE + a history batch → rejected: a batch must be attributed to
    // the active job that produced it.
    let (mut engine, _pi, eik) = external_agent_instance();
    let err = engine
        .apply_command(Command::CreateAgentInstance {
            element_instance_key: eik,
            job_key: 0,
            job_lease: String::new(),
            definition: AgentDefinition::default(),
            limits: None,
            history: vec![history_turn(0, 200, AgentHistoryRole::Assistant)],
        })
        .unwrap_err();
    assert!(
        matches!(err, EngineError::AgentInstanceJobRequiredForHistory { .. }),
        "a jobless CREATE carrying history must be rejected, got {err:?}"
    );
}

/// #1106 divergence 3 — a **history-free** UPDATE that nonetheless supplies a
/// `job_key` must still validate it (active + matching lease + element). Only a
/// fully job-optional UPDATE (no job AND no history) skips the gate. The
/// pre-#1106 engine skipped validation whenever history was empty, silently
/// accepting a stale/foreign job.
#[test]
fn external_agent_history_free_update_validates_a_supplied_job() {
    use crate::agent::AgentDefinition;
    let (mut engine, pi, eik) = external_agent_instance();
    let job = engine
        .activate_jobs_with_options("agent", "W", 1, 1_000, 100, lease_options())
        .pop()
        .expect("activatable");
    let lease = job.lease_token.expect("lease token");
    let created = engine
        .apply_command(Command::CreateAgentInstance {
            element_instance_key: eik,
            job_key: job.key,
            job_lease: lease,
            definition: AgentDefinition::default(),
            limits: None,
            history: vec![],
        })
        .unwrap();
    let aik = created
        .iter()
        .find_map(|e| match e {
            Event::AgentInstanceCreated { agent_instance, .. } => {
                Some(agent_instance.agent_instance_key)
            }
            _ => None,
        })
        .expect("CREATE mints");

    // A history-free UPDATE that supplies a STALE lease token is rejected.
    let err = engine
        .apply_command(Command::UpdateAgentInstance {
            agent_instance_key: aik,
            element_instance_key: eik,
            element_id: "agent".to_string(),
            process_instance_key: pi,
            job_key: job.key,
            job_lease: "stale-lease".to_string(),
            status: Some(crate::agent::AgentInstanceStatus::Thinking),
            metrics: Default::default(),
            tools: None,
            history: vec![],
        })
        .unwrap_err();
    assert!(
        matches!(err, EngineError::AgentInstanceJobLeaseMismatch { .. }),
        "a history-free UPDATE with a supplied-but-stale job must be rejected, got {err:?}"
    );

    // A fully job-optional UPDATE (no job, no history) remains ungated.
    engine
        .apply_command(Command::UpdateAgentInstance {
            agent_instance_key: aik,
            element_instance_key: eik,
            element_id: "agent".to_string(),
            process_instance_key: pi,
            job_key: 0,
            job_lease: String::new(),
            status: Some(crate::agent::AgentInstanceStatus::Thinking),
            metrics: Default::default(),
            tools: None,
            history: vec![],
        })
        .expect("a job-optional, history-free UPDATE is not gated");
}

/// #1106 divergence 4 — an ordinary (non-agent) job activates **lease-less**
/// (`lease_token == None`, Camunda's `!hasLeaseToken()`), so the lease
/// comparison is skipped for it. This is what makes the token-carrying gate on
/// `validate_agent_job_context` conditional rather than unconditional.
#[test]
fn ordinary_job_activates_lease_less() {
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p" isExecutable="true">
          <bpmn:startEvent id="start" />
          <bpmn:serviceTask id="svc">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="work" />
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:endEvent id="end" />
          <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="svc" />
          <bpmn:sequenceFlow id="f2" sourceRef="svc" targetRef="end" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml).unwrap().remove(0);
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    engine.apply_command(Command::create_instance("p")).unwrap();
    let job = engine
        .activate_jobs("work", "W", 1, 1_000, 100)
        .pop()
        .expect("the service task job is activatable");
    assert_eq!(
        job.lease_token, None,
        "an ordinary (non-agent) job must activate lease-less"
    );
}

#[test]
fn agent_instance_create_rejects_invalid_caller_supplied_job_attribution() {
    use crate::agent::{AgentDefinition, AgentHistoryRole, AgentInstanceLimits};
    let (mut engine, pi, eik) = external_agent_instance();
    let err = engine
        .apply_command(Command::CreateAgentInstance {
            element_instance_key: eik,
            job_key: 999_999,
            job_lease: "invalid-lease".to_string(),
            definition: AgentDefinition::default(),
            limits: Some(AgentInstanceLimits::default()),
            history: vec![history_turn(0, 10, AgentHistoryRole::Configuration)],
        })
        .unwrap_err();
    assert!(matches!(err, EngineError::AgentInstanceJobNotActive { .. }));
    assert!(engine.state.instances[&pi].agent_instances.is_empty());
    assert!(engine.state.instances[&pi].agent_history.is_empty());
}

#[test]
fn agent_instance_create_on_plain_service_task_missing_agent_definition_is_rejected() {
    use crate::agent::AgentDefinition;
    // A job-worker service task is an eligible TYPE (SERVICE_TASK) but carries no
    // agentDefinition, so CREATE rejects it as missing the agentDefinitionKey.
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="plain-proc" isExecutable="true">
          <bpmn:startEvent id="start" />
          <bpmn:serviceTask id="job">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="worker" />
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:endEvent id="end" />
          <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="job" />
          <bpmn:sequenceFlow id="f2" sourceRef="job" targetRef="end" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml).unwrap().remove(0);
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let events = engine
        .apply_command(Command::create_instance("plain-proc"))
        .unwrap();
    let eik = events
        .iter()
        .find_map(|e| match e {
            Event::ElementActivated {
                element_instance_key,
                element_id,
                ..
            } if element_id == "job" => Some(*element_instance_key),
            _ => None,
        })
        .expect("the service task should activate and park");

    let err = engine
        .apply_command(Command::CreateAgentInstance {
            element_instance_key: eik,
            job_key: 0,
            job_lease: String::new(),
            definition: AgentDefinition::default(),
            limits: None,
            history: vec![],
        })
        .unwrap_err();
    assert!(matches!(
        err,
        EngineError::AgentInstanceMissingAgentDefinition { .. }
    ));
}

/// #986 — a declared `fetchVariables` read-set supplied to `activate_jobs_with_fetch`
/// is stamped onto the durable `JobActivated` event (engine-native read
/// provenance for reification), while a fetch-all activation records none.
#[test]
fn activation_stamps_the_declared_read_set_on_job_activated() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();

    // Declared activation: worker asks for [amount, currency].
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let events = engine
        .apply_command_at(
            Command::activate_jobs_with_fetch(
                "payment",
                "w",
                1,
                60_000,
                0,
                vec!["amount".to_string(), "currency".to_string()],
            ),
            0,
        )
        .unwrap();
    let declared = events
        .iter()
        .find_map(|e| match e {
            Event::JobActivated {
                fetch_variables, ..
            } => Some(fetch_variables.clone()),
            _ => None,
        })
        .expect("a JobActivated was emitted");
    assert_eq!(
        declared,
        vec!["amount".to_string(), "currency".to_string()],
        "the declared read-set is stamped onto the activation event"
    );

    // Fetch-all activation: a second instance, activated without a declared set,
    // records an empty (undeclared) read-set.
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let events = engine
        .apply_command_at(Command::activate_jobs("payment", "w", 1, 60_000, 0), 0)
        .unwrap();
    let undeclared = events
        .iter()
        .find_map(|e| match e {
            Event::JobActivated {
                fetch_variables, ..
            } => Some(fetch_variables.clone()),
            _ => None,
        })
        .expect("a JobActivated was emitted");
    assert!(
        undeclared.is_empty(),
        "a fetch-all activation records no declared read-set"
    );
}

/// #986 — the new `fetch_variables` field must NOT change the journal byte shape
/// of a declaration-free activation: `skip_serializing_if` omits it entirely when
/// empty, so historical/undeclared `JobActivated` records serialize identically.
#[cfg(feature = "serde")]
#[test]
fn declaration_free_job_activated_is_byte_identical() {
    let undeclared = Event::JobActivated {
        job_key: 7,
        instance_key: 3,
        durable: false,
        worker: "w".to_string(),
        deadline: 60_000,
        activated_at: Some(1),
        fetch_variables: Vec::new(),
        lease_token: None,
    };
    let json = serde_json::to_string(&undeclared).unwrap();
    assert!(
        !json.contains("fetch_variables"),
        "an empty read-set must be omitted from the serialized event: {json}"
    );
    assert!(
        !json.contains("lease_token"),
        "a lease-less activation must omit the lease token from the serialized event: {json}"
    );

    // A declared read-set IS serialized, and round-trips.
    let declared = Event::JobActivated {
        job_key: 7,
        instance_key: 3,
        durable: false,
        worker: "w".to_string(),
        deadline: 60_000,
        activated_at: Some(1),
        fetch_variables: vec!["a".to_string(), "c".to_string()],
        lease_token: None,
    };
    let json = serde_json::to_string(&declared).unwrap();
    assert!(json.contains("fetch_variables"));
    let back: Event = serde_json::from_str(&json).unwrap();
    assert_eq!(back, declared);
}

// ---- Camunda 8.10 `JobCompletionRequest.businessId` (#1291) ----------------

/// start -> work(svc "work") -> end, deployed, one instance created with the
/// given business id and its job activated. Returns (engine, instance, job).
fn job_parked_instance(business_id: Option<&str>) -> (Engine, Key, Key) {
    let proc = ProcessBuilder::new("biz")
        .start_event("s")
        .service_task("work", "work")
        .end_event("e")
        .connect("s", "work")
        .connect("work", "e")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(proc)).unwrap();
    let created = engine
        .apply_command(Command::create_instance_full(
            "biz",
            HashMap::new(),
            Vec::new(),
            business_id.map(str::to_string),
        ))
        .unwrap();
    let instance = created.iter().find_map(|e| e.instance_key()).unwrap();
    let job = engine
        .activate_jobs("work", "w", 1, 60_000, 0)
        .into_iter()
        .next()
        .expect("job activates")
        .key;
    (engine, instance, job)
}

#[test]
fn completing_a_job_assigns_the_business_id_to_its_root_instance() {
    let (mut engine, instance, job) = job_parked_instance(None);
    let events = engine
        .apply_command(Command::complete_job(job).with_business_id(Some("order-9".into())))
        .unwrap();
    let assigned = events
        .iter()
        .position(|e| {
            *e == Event::ProcessInstanceBusinessIdAssigned {
                instance_key: instance,
                business_id: "order-9".into(),
            }
        })
        .expect("the assignment is journaled");
    let completed = events
        .iter()
        .position(|e| matches!(e, Event::JobCompleted { .. }))
        .expect("the job completes");
    assert!(assigned < completed, "assignment precedes the completion");
    assert_eq!(
        engine.state().instances[&instance].business_id.as_deref(),
        Some("order-9")
    );
}

#[test]
fn re_sending_the_assigned_business_id_is_an_idempotent_no_op() {
    let (mut engine, _instance, job) = job_parked_instance(Some("order-9"));
    let events = engine
        .apply_command(Command::complete_job(job).with_business_id(Some("order-9".into())))
        .unwrap();
    assert!(events
        .iter()
        .any(|e| matches!(e, Event::JobCompleted { .. })));
    assert!(!events
        .iter()
        .any(|e| matches!(e, Event::ProcessInstanceBusinessIdAssigned { .. })));
}

#[test]
fn a_different_business_id_rejects_the_whole_completion() {
    let (mut engine, instance, job) = job_parked_instance(Some("order-9"));
    let err = engine
        .apply_command(Command::complete_job(job).with_business_id(Some("other".into())))
        .unwrap_err();
    assert_eq!(
        err,
        EngineError::BusinessIdAlreadyAssigned {
            instance_key: instance
        }
    );
    assert!(
        !matches!(engine.state().jobs[&job].state, state::JobState::Completed),
        "the job stays open after a rejected completion"
    );
    assert_eq!(
        engine.state().instances[&instance].business_id.as_deref(),
        Some("order-9")
    );
}

#[test]
fn an_empty_business_id_rejects_the_completion() {
    let (mut engine, instance, job) = job_parked_instance(None);
    let err = engine
        .apply_command(Command::complete_job(job).with_business_id(Some(String::new())))
        .unwrap_err();
    let _ = instance;
    assert_eq!(err, EngineError::BusinessIdInvalid { chars: 0 });
    assert!(!matches!(
        engine.state().jobs[&job].state,
        state::JobState::Completed
    ));
}

#[test]
fn a_child_instance_business_id_rejects_the_completion() {
    let leaf = ProcessBuilder::new("leaf")
        .start_event("ls")
        .service_task("leaf_work", "leaf-job")
        .end_event("le")
        .connect("ls", "leaf_work")
        .connect("leaf_work", "le")
        .build()
        .unwrap();
    let root = ProcessBuilder::new("root")
        .start_event("s")
        .call_activity("c1", "leaf")
        .end_event("e")
        .connect("s", "c1")
        .connect("c1", "e")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(leaf)).unwrap();
    engine.apply_command(Command::DeployProcess(root)).unwrap();
    engine
        .apply_command(Command::create_instance("root"))
        .unwrap();
    let job = engine
        .activate_jobs("leaf-job", "w", 1, 60_000, 0)
        .into_iter()
        .next()
        .expect("the child's job activates");
    let err = engine
        .apply_command(Command::complete_job(job.key).with_business_id(Some("x".into())))
        .unwrap_err();
    assert_eq!(
        err,
        EngineError::BusinessIdOnChildInstance {
            instance_key: job.instance_key
        }
    );
    assert!(!matches!(
        engine.state().jobs[&job.key].state,
        state::JobState::Completed
    ));
}

/// The spec's `BusinessId` range (1..=256 characters) is enforced in the
/// engine for every command that carries one, so non-REST surfaces (Falcon,
/// wasm) cannot persist an out-of-range id (#1295 review). Characters, not
/// bytes: 256 multi-byte characters are accepted.
#[test]
fn business_ids_outside_the_spec_range_are_rejected_on_every_command() {
    let max = "é".repeat(crate::BUSINESS_ID_MAX_CHARS);
    let over = "x".repeat(crate::BUSINESS_ID_MAX_CHARS + 1);
    let too_long = EngineError::BusinessIdInvalid {
        chars: crate::BUSINESS_ID_MAX_CHARS + 1,
    };

    // Job completion (assignment).
    let (mut engine, instance, job) = job_parked_instance(None);
    let err = engine
        .apply_command(Command::complete_job(job).with_business_id(Some(over.clone())))
        .unwrap_err();
    assert_eq!(err, too_long);
    assert_eq!(engine.state().instances[&instance].business_id, None);
    engine
        .apply_command(Command::complete_job(job).with_business_id(Some(max.clone())))
        .expect("256 characters is within range");
    assert_eq!(
        engine.state().instances[&instance].business_id.as_deref(),
        Some(max.as_str())
    );

    // Instance creation.
    let (mut engine, _, _) = job_parked_instance(None);
    let before = engine.state().instances.len();
    for (id, want) in [
        (over.clone(), too_long.clone()),
        (String::new(), EngineError::BusinessIdInvalid { chars: 0 }),
    ] {
        let err = engine
            .apply_command(Command::create_instance_full(
                "biz",
                HashMap::new(),
                Vec::new(),
                Some(id),
            ))
            .unwrap_err();
        assert_eq!(err, want);
    }
    assert_eq!(
        engine.state().instances.len(),
        before,
        "no instance created"
    );

    // Message publish / correlate.
    let err = engine
        .apply_command(Command::CorrelateMessage {
            message_name: "m".into(),
            correlation_key: "k".into(),
            variables: HashMap::new(),
            business_id: Some(over),
        })
        .unwrap_err();
    assert_eq!(err, too_long);
}
