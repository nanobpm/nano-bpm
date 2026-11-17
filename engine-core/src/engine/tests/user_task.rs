//! `user_task` engine tests (slice 7 of #1201), extracted verbatim.
use super::*;

/// A FEEL `function` or `range` result converts to `Value::Null` at the API
/// boundary but keeps the kind `UNKNOWN`. The nullable due/follow-up dates
/// must accept only a native FEEL `null` as "no date" — anything else,
/// including a value that merely *renders* as null, is a wrong-type incident
/// (Zeebe `evaluateDateTimeExpression`, #1398 review).
#[test]
fn should_raise_an_incident_when_a_due_date_expression_yields_a_function() {
    use crate::model::UserTaskProps;
    let def = ProcessBuilder::new("fn-date")
        .start_event("start")
        .user_task_with(
            "review",
            UserTaskProps {
                assignee: None,
                candidate_groups: None,
                candidate_users: None,
                due_date: Some("=function(x) x".to_string()),
                follow_up_date: Some("=[1..2]".to_string()),
                priority: None,
                form_id: None,
                form_binding_type: crate::model::BindingType::Latest,
                form_version_tag: None,
                external_form_reference: None,
            },
        )
        .end_event("end")
        .connect("start", "review")
        .connect("review", "end")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("fn-date"))
        .unwrap();
    assert!(
        !created
            .iter()
            .any(|e| matches!(e, Event::UserTaskCreated { .. })),
        "no user task is created on a wrong-typed date"
    );
    let incident = engine.active_incidents()[0].clone();
    assert_eq!(incident.kind, state::IncidentKind::ExtractValueError);
    assert!(
        incident.reason.contains("but was 'UNKNOWN'"),
        "a function/range result is not an absent date: {}",
        incident.reason
    );
}

#[test]
fn subprocess_terminate_end_cancels_a_resting_user_task_in_its_scope() {
    // A terminate end must cancel a resting user task in the torn-down scope —
    // `ElementCompleted` alone leaves it queryable as `Created` and completable.
    //
    //  sub: sub_start -> split =< ut(user task), trigger(svc) -> stop(terminate) >
    let orch = ProcessBuilder::new("orch")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .parallel_gateway("split")
        .contained_in("split", "sub")
        .user_task("ut")
        .contained_in("ut", "sub")
        .service_task("trigger", "trigger-job")
        .contained_in("trigger", "sub")
        .terminate_end_event("stop")
        .contained_in("stop", "sub")
        .end_event("done")
        .connect("start", "sub")
        .connect("sub_start", "split")
        .connect("split", "ut")
        .connect("split", "trigger")
        .connect("trigger", "stop")
        .connect("sub", "done")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(orch)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("orch"))
        .unwrap();
    let ut_key = created
        .iter()
        .find_map(|e| match e {
            Event::UserTaskCreated { user_task_key, .. } => Some(*user_task_key),
            _ => None,
        })
        .expect("user task created");
    assert_eq!(
        engine.state().user_tasks[&ut_key].state,
        crate::state::UserTaskState::Created
    );

    let events = complete_one(&mut engine, "trigger-job");
    assert!(events.iter().any(|e| matches!(
        e,
        Event::UserTaskCanceled { user_task_key, .. } if *user_task_key == ut_key
    )));
    assert_eq!(
        engine.state().user_tasks[&ut_key].state,
        crate::state::UserTaskState::Canceled
    );
}

#[test]
fn should_park_on_a_user_task_then_resume_when_completed() {
    // start -> review (user task) -> end
    let def = ProcessBuilder::new("approval")
        .start_event("start")
        .user_task("review")
        .end_event("end")
        .connect("start", "review")
        .connect("review", "end")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    let created = engine
        .apply_command(Command::create_instance("approval"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // The token parks on the user task: a UserTaskCreated event was emitted
    // and the instance is not yet complete.
    let user_task_key = created
        .iter()
        .find_map(|e| match e {
            Event::UserTaskCreated { user_task_key, .. } => Some(*user_task_key),
            _ => None,
        })
        .expect("user task created");
    assert!(!engine.is_completed(instance_key));

    // Assigning keeps it parked; the assignee is recorded.
    engine
        .apply_command(Command::assign_user_task(user_task_key, "alice"))
        .unwrap();
    assert_eq!(
        engine.state().user_tasks[&user_task_key]
            .assignee
            .as_deref(),
        Some("alice")
    );
    assert!(!engine.is_completed(instance_key));

    // Completing it (merging a variable) resumes the token to the end event,
    // completing the instance.
    let vars = HashMap::from([("approved".to_string(), Value::Bool(true))]);
    let done = engine
        .apply_command(Command::complete_user_task_with(user_task_key, vars))
        .unwrap();
    assert!(done.iter().any(|e| matches!(
        e,
        Event::UserTaskCompleted { user_task_key: k, .. } if *k == user_task_key
    )));
    assert!(engine.is_completed(instance_key));
    assert_eq!(
        engine.state().user_tasks[&user_task_key].state,
        state::UserTaskState::Completed
    );

    // Completing again is rejected NOT_FOUND (Zeebe parity, #1401): completion
    // consumes the task, so a second Complete finds no user task at all — it is
    // NOT the INVALID_STATE of a live-but-not-completable task.
    assert_eq!(
        engine.apply_command(Command::complete_user_task(user_task_key)),
        Err(EngineError::UserTaskNotFound { user_task_key })
    );
}

#[test]
fn should_reject_completing_a_canceled_user_task_as_not_found() {
    // Zeebe parity (#1401): a canceled user task is consumed just like a
    // completed one — re-completing it is NOT_FOUND, not INVALID_STATE.
    let def = ProcessBuilder::new("approval")
        .start_event("start")
        .user_task("review")
        .end_event("end")
        .connect("start", "review")
        .connect("review", "end")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("approval"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    let user_task_key = created
        .iter()
        .find_map(|e| match e {
            Event::UserTaskCreated { user_task_key, .. } => Some(*user_task_key),
            _ => None,
        })
        .expect("user task created");

    // Cancelling the instance cancels its parked user task.
    engine
        .apply_command(Command::cancel_instance(instance_key))
        .unwrap();
    assert_eq!(
        engine.state().user_tasks[&user_task_key].state,
        state::UserTaskState::Canceled
    );

    // Completing the canceled task is NOT_FOUND, not INVALID_STATE.
    assert_eq!(
        engine.apply_command(Command::complete_user_task(user_task_key)),
        Err(EngineError::UserTaskNotFound { user_task_key })
    );
}

#[test]
fn should_create_a_user_task_with_resolved_attributes() {
    use crate::model::UserTaskProps;
    // A user task declaring assignee/candidates/dates/priority, partly via
    // FEEL expressions evaluated against the instance variables.
    let def = ProcessBuilder::new("approval")
        .start_event("start")
        .user_task_with(
            "review",
            UserTaskProps {
                assignee: Some("=requester".to_string()),
                candidate_groups: Some("ops,finance".to_string()),
                candidate_users: Some("=reviewers".to_string()),
                due_date: Some("2025-01-01T00:00:00Z".to_string()),
                follow_up_date: None,
                priority: Some("=urgency".to_string()),
                form_id: None,
                form_binding_type: crate::model::BindingType::Latest,
                form_version_tag: None,
                external_form_reference: None,
            },
        )
        .end_event("end")
        .connect("start", "review")
        .connect("review", "end")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    let vars = HashMap::from([
        ("requester".to_string(), Value::Str("alice".to_string())),
        (
            "reviewers".to_string(),
            Value::List(vec![
                Value::Str("bob".to_string()),
                Value::Str("carol".to_string()),
            ]),
        ),
        ("urgency".to_string(), Value::Int(80)),
    ]);
    let created = engine
        .apply_command(Command::CreateInstance {
            process_id: "approval".to_string(),
            variables: vars,
            tags: Vec::new(),
            business_id: None,
            process_definition_key: None,
            version: None,
        })
        .unwrap();
    let user_task_key = created
        .iter()
        .find_map(|e| match e {
            Event::UserTaskCreated { user_task_key, .. } => Some(*user_task_key),
            _ => None,
        })
        .expect("user task created");

    let task = &engine.state().user_tasks[&user_task_key];
    assert_eq!(task.assignee.as_deref(), Some("alice"));
    assert_eq!(task.candidate_groups, vec!["ops", "finance"]);
    assert_eq!(task.candidate_users, vec!["bob", "carol"]);
    assert_eq!(task.due_date.as_deref(), Some("2025-01-01T00:00:00Z"));
    assert_eq!(task.follow_up_date, None);
    assert_eq!(task.priority, 80);
}

#[test]
fn should_raise_an_incident_when_assignee_expression_is_null() {
    use crate::model::UserTaskProps;
    // #900 stopped storing the raw "=who" literal as the assignee. Zeebe
    // (`BpmnUserTaskBehavior.evaluateAssigneeExpression` ->
    // `evaluateStringExpression`) goes further: a NULL assignee is an
    // EXTRACT_VALUE_ERROR incident — no task is created until the operator
    // fixes the variable (#1398). The nullable dates stay absent.
    let def = ProcessBuilder::new("escalation")
        .start_event("start")
        .user_task_with(
            "review",
            UserTaskProps {
                assignee: Some("=who".to_string()),
                candidate_groups: Some("operators".to_string()),
                candidate_users: None,
                due_date: Some("=maybeNull".to_string()),
                follow_up_date: Some("=maybeNull".to_string()),
                priority: None,
                form_id: None,
                form_binding_type: crate::model::BindingType::Latest,
                form_version_tag: None,
                external_form_reference: None,
            },
        )
        .end_event("end")
        .connect("start", "review")
        .connect("review", "end")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    // maybeNull is explicitly null (intent: escalate unassigned).
    let vars = HashMap::from([
        ("maybeNull".to_string(), Value::Null),
        ("who".to_string(), Value::Null),
    ]);
    let created = engine
        .apply_command(Command::CreateInstance {
            process_id: "escalation".to_string(),
            variables: vars,
            tags: Vec::new(),
            business_id: None,
            process_definition_key: None,
            version: None,
        })
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    assert!(
        !created
            .iter()
            .any(|e| matches!(e, Event::UserTaskCreated { .. })),
        "no user task is created on an unevaluable assignee"
    );
    let incident = engine.active_incidents()[0].clone();
    assert_eq!(incident.kind, state::IncidentKind::ExtractValueError);
    assert!(
        incident.reason.contains("but was 'NULL'"),
        "{}",
        incident.reason
    );

    // The operator supplies the assignee and resolves: the task is created.
    engine
        .apply_command(Command::set_variables(
            instance_key,
            HashMap::from([("who".to_string(), Value::Str("operator-1".into()))]),
        ))
        .unwrap();
    engine
        .apply_command(Command::resolve_incident(incident.key))
        .unwrap();
    let task = engine
        .state()
        .user_tasks
        .values()
        .next()
        .expect("user task created on resolve");
    assert_eq!(task.assignee.as_deref(), Some("operator-1"));
    assert_eq!(
        task.due_date, None,
        "null dueDate expression must be absent"
    );
    assert_eq!(
        task.follow_up_date, None,
        "null followUpDate expression must be absent"
    );
    assert_eq!(task.candidate_groups, vec!["operators"]);
}

#[test]
fn should_create_a_user_task_assigned_when_assignee_expression_is_a_string() {
    use crate::model::UserTaskProps;
    // The non-null counterpart: the same model with maybeNull = "alice" assigns
    // the task to alice (expression results are unchanged by the null fix).
    let def = ProcessBuilder::new("escalation")
        .start_event("start")
        .user_task_with(
            "review",
            UserTaskProps {
                assignee: Some("=maybeNull".to_string()),
                candidate_groups: Some("operators".to_string()),
                candidate_users: None,
                due_date: None,
                follow_up_date: None,
                priority: None,
                form_id: None,
                form_binding_type: crate::model::BindingType::Latest,
                form_version_tag: None,
                external_form_reference: None,
            },
        )
        .end_event("end")
        .connect("start", "review")
        .connect("review", "end")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    let vars = HashMap::from([("maybeNull".to_string(), Value::Str("alice".to_string()))]);
    let created = engine
        .apply_command(Command::CreateInstance {
            process_id: "escalation".to_string(),
            variables: vars,
            tags: Vec::new(),
            business_id: None,
            process_definition_key: None,
            version: None,
        })
        .unwrap();
    let user_task_key = created
        .iter()
        .find_map(|e| match e {
            Event::UserTaskCreated { user_task_key, .. } => Some(*user_task_key),
            _ => None,
        })
        .expect("user task created");

    assert_eq!(
        engine.state().user_tasks[&user_task_key]
            .assignee
            .as_deref(),
        Some("alice")
    );
}

// zeebe-cells: element:UserTask
#[test]
fn should_link_a_user_task_to_its_deployed_form_key() {
    use crate::command::FormResource;
    // A user task declaring a zeebe:formDefinition formId, plus a start form on
    // the process, both referencing deployed forms by id.
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="forms-proc">
          <bpmn:startEvent id="s">
            <bpmn:extensionElements>
              <zeebe:formDefinition formId="start-form" />
            </bpmn:extensionElements>
          </bpmn:startEvent>
          <bpmn:userTask id="review">
            <bpmn:extensionElements>
              <zeebe:userTask />
              <zeebe:formDefinition formId="review-form" />
            </bpmn:extensionElements>
          </bpmn:userTask>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="review" />
          <bpmn:sequenceFlow id="b" sourceRef="review" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml).unwrap().pop().unwrap();
    assert_eq!(def.start_form_id.as_deref(), Some("start-form"));

    let form = |id: &str| FormResource {
        id: id.to_string(),
        resource_name: format!("{id}.form"),
        schema: format!(r#"{{"id":"{id}","type":"default","components":[]}}"#),
        version_tag: None,
    };

    let mut engine = Engine::new();
    // Deploy the user-task form first so it is resolvable at task creation.
    let form_events = engine
        .apply_command(Command::DeployForms(vec![form("review-form")]))
        .unwrap();
    let review_form_key = form_events
        .iter()
        .find_map(|e| match e {
            Event::FormDeployed { form_key, .. } => Some(*form_key),
            _ => None,
        })
        .expect("review-form deployed");
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    let created = engine
        .apply_command(Command::create_instance("forms-proc"))
        .unwrap();
    let (user_task_key, event_form_key) = created
        .iter()
        .find_map(|e| match e {
            Event::UserTaskCreated {
                user_task_key,
                form_key,
                ..
            } => Some((*user_task_key, *form_key)),
            _ => None,
        })
        .expect("user task created");
    assert_eq!(
        event_form_key,
        Some(review_form_key),
        "the event carries the resolved form key"
    );
    let task = &engine.state().user_tasks[&user_task_key];
    assert_eq!(task.form_key, Some(review_form_key));
    assert_eq!(task.external_form_reference, None);
}

#[test]
fn should_default_user_task_priority_to_fifty() {
    let def = ProcessBuilder::new("approval")
        .start_event("start")
        .user_task("review")
        .end_event("end")
        .connect("start", "review")
        .connect("review", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("approval"))
        .unwrap();
    let user_task_key = created
        .iter()
        .find_map(|e| match e {
            Event::UserTaskCreated { user_task_key, .. } => Some(*user_task_key),
            _ => None,
        })
        .unwrap();
    assert_eq!(engine.state().user_tasks[&user_task_key].priority, 50);
}

#[test]
fn should_update_user_task_attributes_via_changeset() {
    use crate::UserTaskChangeset;
    let def = ProcessBuilder::new("approval")
        .start_event("start")
        .user_task("review")
        .end_event("end")
        .connect("start", "review")
        .connect("review", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("approval"))
        .unwrap();
    let user_task_key = created
        .iter()
        .find_map(|e| match e {
            Event::UserTaskCreated { user_task_key, .. } => Some(*user_task_key),
            _ => None,
        })
        .unwrap();

    engine
        .apply_command(Command::update_user_task(
            user_task_key,
            UserTaskChangeset {
                candidate_groups: Some(vec!["ops".to_string()]),
                candidate_users: None,
                due_date: Some(Some("2025-06-01T00:00:00Z".to_string())),
                follow_up_date: None,
                priority: Some(20),
            },
        ))
        .unwrap();

    let task = &engine.state().user_tasks[&user_task_key];
    assert_eq!(task.candidate_groups, vec!["ops"]);
    assert_eq!(task.due_date.as_deref(), Some("2025-06-01T00:00:00Z"));
    assert_eq!(task.priority, 20);

    // Resetting the due date with an empty string clears it.
    engine
        .apply_command(Command::update_user_task(
            user_task_key,
            UserTaskChangeset {
                due_date: Some(Some(String::new())),
                ..Default::default()
            },
        ))
        .unwrap();
    assert_eq!(engine.state().user_tasks[&user_task_key].due_date, None);
}

#[test]
fn user_task_resolves_form_id_to_the_latest_form_key_and_carries_external_reference() {
    use crate::command::FormResource;
    use crate::model::UserTaskProps;

    let form = |schema: &str| FormResource {
        id: "feature-escalation".to_string(),
        resource_name: "feature-escalation.form".to_string(),
        schema: schema.to_string(),
        version_tag: None,
    };
    let v1 = r#"{"id":"feature-escalation","type":"default","components":[]}"#;

    let mut engine = Engine::new();
    let deployed = engine
        .apply_command(Command::DeployForms(vec![form(v1)]))
        .unwrap();
    let form_key_v1 = deployed
        .iter()
        .find_map(|e| match e {
            Event::FormDeployed { form_key, .. } => Some(*form_key),
            _ => None,
        })
        .expect("a form key is minted");

    // A process with two user tasks: one bound to the embedded form by id, one
    // carrying an external form reference.
    let def = ProcessBuilder::new("feature")
        .start_event("start")
        .user_task_with(
            "escalation",
            UserTaskProps {
                form_id: Some("feature-escalation".to_string()),
                ..Default::default()
            },
        )
        .user_task_with(
            "external",
            UserTaskProps {
                external_form_reference: Some("https://forms.example/x".to_string()),
                ..Default::default()
            },
        )
        .end_event("end")
        .connect("start", "escalation")
        .connect("escalation", "external")
        .connect("external", "end")
        .build()
        .unwrap();
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    let created = engine
        .apply_command(Command::create_instance("feature"))
        .unwrap();

    // The embedded-form task resolves its formId to the deployed form's key.
    let escalation_key = created
        .iter()
        .find_map(|e| match e {
            Event::UserTaskCreated {
                user_task_key,
                form_key: Some(k),
                ..
            } => Some((*user_task_key, *k)),
            _ => None,
        })
        .expect("the escalation task carries a resolved form key");
    assert_eq!(escalation_key.1, form_key_v1);
    let task = &engine.state().user_tasks[&escalation_key.0];
    assert_eq!(task.form_key, Some(form_key_v1));
    assert_eq!(task.external_form_reference, None);

    // Complete it to advance to the external-form task.
    engine
        .apply_command(Command::complete_user_task(escalation_key.0))
        .unwrap();

    let external = engine
        .state()
        .user_tasks
        .values()
        .find(|t| t.element_id == "external")
        .expect("the external task exists");
    assert_eq!(external.form_key, None);
    assert_eq!(
        external.external_form_reference.as_deref(),
        Some("https://forms.example/x")
    );
    let external_key = external.key;
    engine
        .apply_command(Command::complete_user_task(external_key))
        .unwrap();

    // Latest binding: redeploy a newer form version, then a fresh instance's
    // task resolves to the new key while the already-bound task keeps its key.
    let v2 = r#"{"id":"feature-escalation","type":"default","components":[{"type":"textfield","key":"why"}]}"#;
    let redeployed = engine
        .apply_command(Command::DeployForms(vec![form(v2)]))
        .unwrap();
    let form_key_v2 = redeployed
        .iter()
        .find_map(|e| match e {
            Event::FormDeployed { form_key, .. } => Some(*form_key),
            _ => None,
        })
        .expect("a new form key is minted");
    assert_ne!(form_key_v2, form_key_v1);

    let created2 = engine
        .apply_command(Command::create_instance("feature"))
        .unwrap();
    let escalation2 = created2
        .iter()
        .find_map(|e| match e {
            Event::UserTaskCreated {
                form_key: Some(k), ..
            } => Some(*k),
            _ => None,
        })
        .expect("the new instance's escalation task resolves a form key");
    assert_eq!(
        escalation2, form_key_v2,
        "new tasks bind to the latest form"
    );
    // The first task's binding is unchanged (it kept its v1 key).
    assert_eq!(
        engine.state().user_tasks[&escalation_key.0].form_key,
        Some(form_key_v1)
    );
}

#[test]
fn user_task_with_external_reference_never_resolves_a_form_key_even_if_form_id_is_set() {
    use crate::command::FormResource;
    use crate::model::UserTaskProps;

    // A deployed form whose id would otherwise resolve, so this test proves the
    // suppression is deliberate (the form is present but must NOT be bound).
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployForms(vec![FormResource {
            id: "feature-escalation".to_string(),
            resource_name: "feature-escalation.form".to_string(),
            schema: r#"{"id":"feature-escalation","type":"default","components":[]}"#.to_string(),
            version_tag: None,
        }]))
        .unwrap();

    // A user task built programmatically with BOTH form_id and
    // external_form_reference set. Zeebe treats them as mutually exclusive, so
    // the engine must let the external reference win and resolve no form_key —
    // guarding the invariant independently of the BPMN parser.
    let def = ProcessBuilder::new("feature")
        .start_event("start")
        .user_task_with(
            "both",
            UserTaskProps {
                form_id: Some("feature-escalation".to_string()),
                external_form_reference: Some("https://forms.example/x".to_string()),
                ..Default::default()
            },
        )
        .end_event("end")
        .connect("start", "both")
        .connect("both", "end")
        .build()
        .unwrap();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    engine
        .apply_command(Command::create_instance("feature"))
        .unwrap();

    let task = engine
        .state()
        .user_tasks
        .values()
        .find(|t| t.element_id == "both")
        .expect("the task exists");
    assert_eq!(
        task.form_key, None,
        "external reference suppresses form_key"
    );
    assert_eq!(
        task.external_form_reference.as_deref(),
        Some("https://forms.example/x")
    );
}

#[test]
fn initial_assignee_routes_through_assigning_after_creating() {
    // creating chain first, then the stripped initial assignee's assigning chain.
    use crate::model::UserTaskProps;
    let (mut engine, _inst) = deploy_and_start(user_task_with_props_and_listeners(
        UserTaskProps {
            assignee: Some("alice".into()),
            ..Default::default()
        },
        vec![
            tl(TaskListenerEventType::Creating, "onCreate"),
            tl(TaskListenerEventType::Assigning, "onAssign"),
        ],
    ));
    let key = only_user_task_key(&engine);
    assert_eq!(engine.state().user_tasks[&key].assignee, None);
    // Only the creating job exists first.
    assert!(engine
        .activate_jobs("onAssign", "W", 10, 1_000, 0)
        .is_empty());
    let create = engine.activate_jobs("onCreate", "W", 10, 1_000, 0);
    assert_eq!(create.len(), 1);
    engine
        .apply_command(Command::complete_job(create[0].key))
        .unwrap();
    // Now the assigning chain for the initial assignee starts.
    let assign = engine.activate_jobs("onAssign", "W", 10, 1_000, 0);
    assert_eq!(assign.len(), 1);
    engine
        .apply_command(Command::complete_job(assign[0].key))
        .unwrap();
    assert_eq!(
        engine.state().user_tasks[&key].assignee.as_deref(),
        Some("alice")
    );
}

// ── #1193: user-task form bindingType parity (deployment / versionTag) ──

/// A process whose `review` user task binds `review-form` with the given
/// `bindingType` (and optional `versionTag`) attributes, verbatim.
fn form_binding_process(binding_attrs: &str) -> crate::model::ProcessDefinition {
    let xml = format!(
        r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:userTask id="review">
            <bpmn:extensionElements>
              <zeebe:userTask />
              <zeebe:formDefinition formId="review-form" {binding_attrs} />
            </bpmn:extensionElements>
          </bpmn:userTask>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="review" />
          <bpmn:sequenceFlow id="b" sourceRef="review" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#
    );
    crate::bpmn::parse_bpmn(&xml).unwrap().pop().unwrap()
}

fn tagged_form(id: &str, tag: Option<&str>) -> crate::command::FormResource {
    // The tag rides the schema too so two tagged versions are never
    // byte-identical (an identical redeploy is idempotently skipped and mints
    // no version).
    crate::command::FormResource {
        id: id.to_string(),
        resource_name: format!("{id}.form"),
        schema: format!(
            r#"{{"id":"{id}","type":"default","versionTag":{},"components":[]}}"#,
            tag.map(|t| format!(r#""{t}""#))
                .unwrap_or_else(|| "null".to_string())
        ),
        version_tag: tag.map(str::to_string),
    }
}

fn form_key_of(events: &[Event], want_id: &str) -> Key {
    events
        .iter()
        .find_map(|e| match e {
            Event::FormDeployed {
                form_key, form_id, ..
            } if form_id == want_id => Some(*form_key),
            _ => None,
        })
        .unwrap_or_else(|| panic!("a FormDeployed for {want_id}"))
}

/// The `form_key` carried by the single `UserTaskCreated` event (or `None`
/// when the events created no task — the binding-miss path).
fn created_form_key(events: &[Event]) -> Option<Key> {
    events.iter().find_map(|e| match e {
        Event::UserTaskCreated { form_key, .. } => *form_key,
        _ => None,
    })
}

// zeebe-cells: element:UserTask
#[test]
fn deployment_binding_pins_the_form_version_deployed_with_the_process() {
    // A `deployment`-bound user-task form resolves the form version deployed
    // alongside the process definition (the shared `deployment_key`), even
    // after a newer form version is deployed — where `latest` would rebind.
    let mut engine = Engine::new();
    let shared = engine
        .apply_command(Command::DeployForms(vec![tagged_form(
            "review-form",
            Some("v1"),
        )]))
        .unwrap();
    let shared_deployment = shared
        .iter()
        .find_map(|e| match e {
            Event::FormDeployed { deployment_key, .. } => Some(*deployment_key),
            _ => None,
        })
        .expect("a deployment key");
    let form_key_v1 = form_key_of(&shared, "review-form");
    // Install the process under the SAME deployment key (what the server's
    // batched multi-resource deployment produces: one deployment key shared by
    // every resource in the request).
    engine.install_deployment(&[Event::ProcessDeployed {
        deployment_key: shared_deployment,
        process_definition_key: 9_000_001,
        version: 1,
        process: form_binding_process(r#"bindingType="deployment""#),
    }]);
    // A newer form version deployed AFTERWARDS (its own deployment) must NOT
    // steal the binding.
    let later = engine
        .apply_command(Command::DeployForms(vec![tagged_form(
            "review-form",
            Some("v2"),
        )]))
        .unwrap();
    let form_key_v2 = form_key_of(&later, "review-form");
    assert_ne!(
        form_key_v1, form_key_v2,
        "a changed redeploy mints a new key"
    );
    assert_eq!(engine.state().forms["review-form"].key, form_key_v2);

    let created = engine.apply_command(Command::create_instance("p")).unwrap();
    assert_eq!(
        created_form_key(&created),
        Some(form_key_v1),
        "the deployment binding pins the co-deployed version, not the latest"
    );
    assert!(engine.active_incidents().is_empty());
}

// zeebe-cells: element:UserTask
#[test]
fn deployment_binding_miss_raises_a_resolvable_incident_and_creates_no_task() {
    // The process deploys alone; the form exists only in OTHER deployments.
    // Zeebe parity: a resolvable incident, no task, and resolving the incident
    // once the form IS co-deployed (via a migrated/redeployed definition)
    // creates the task.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployForms(vec![tagged_form("review-form", None)]))
        .unwrap();
    engine
        .apply_command(Command::DeployProcess(form_binding_process(
            r#"bindingType="deployment""#,
        )))
        .unwrap();

    let created = engine.apply_command(Command::create_instance("p")).unwrap();
    assert!(
        created
            .iter()
            .all(|e| !matches!(e, Event::UserTaskCreated { .. })),
        "no task is created on a deployment-binding miss: {created:?}"
    );
    let incidents = engine.active_incidents();
    assert_eq!(incidents.len(), 1, "one incident: {incidents:?}");
    assert_eq!(
        incidents[0].kind,
        crate::state::IncidentKind::CalledElementError
    );
    assert!(
        incidents[0].reason.contains("review-form"),
        "the reason names the form id: {}",
        incidents[0].reason
    );
    let incident_key = incidents[0].key;

    // Co-deploy the form with a NEW version of the process (one shared
    // deployment key) and migrate the parked instance onto it, exactly as the
    // Camunda incident message prescribes. The tagged schema differs from the
    // first deploy, so this mints a fresh version under a fresh deployment key.
    let co = engine
        .apply_command(Command::DeployForms(vec![tagged_form(
            "review-form",
            Some("co"),
        )]))
        .unwrap();
    let co_deployment = co
        .iter()
        .find_map(|e| match e {
            Event::FormDeployed { deployment_key, .. } => Some(*deployment_key),
            _ => None,
        })
        .expect("co-deployment key");
    let co_form_key = form_key_of(&co, "review-form");
    let instance_key = engine
        .state()
        .instances
        .keys()
        .next()
        .copied()
        .expect("the parked instance");
    engine.install_deployment(&[Event::ProcessDeployed {
        deployment_key: co_deployment,
        process_definition_key: 9_000_002,
        version: 2,
        process: form_binding_process(r#"bindingType="deployment""#),
    }]);
    engine
        .apply_command(Command::migrate_instance(
            instance_key,
            9_000_002,
            // The target definition renames nothing, but migration requires an
            // explicit mapping for every active element (the parked task).
            vec![("review".to_string(), "review".to_string())],
        ))
        .unwrap();

    // Resolving the incident re-drives the activation, which now binds the
    // co-deployed form version.
    let resolved = engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();
    assert_eq!(
        created_form_key(&resolved),
        Some(co_form_key),
        "resolution binds the form deployed with the migrated-to definition"
    );
    assert!(engine.active_incidents().is_empty());
}

// zeebe-cells: element:UserTask
#[test]
fn version_tag_binding_resolves_the_tagged_form_version() {
    let mut engine = Engine::new();
    let v1 = engine
        .apply_command(Command::DeployForms(vec![tagged_form(
            "review-form",
            Some("v1"),
        )]))
        .unwrap();
    let key_v1 = form_key_of(&v1, "review-form");
    let v2 = engine
        .apply_command(Command::DeployForms(vec![tagged_form(
            "review-form",
            Some("v2"),
        )]))
        .unwrap();
    let key_v2 = form_key_of(&v2, "review-form");
    engine
        .apply_command(Command::DeployProcess(form_binding_process(
            r#"bindingType="versionTag" versionTag="v1""#,
        )))
        .unwrap();

    // The binding pins the v1-tagged version even though v2 is the latest.
    let created = engine.apply_command(Command::create_instance("p")).unwrap();
    assert_eq!(created_form_key(&created), Some(key_v1));
    assert_ne!(key_v1, key_v2);
}

// zeebe-cells: element:UserTask
#[test]
fn version_tag_binding_miss_raises_a_resolvable_incident() {
    let mut engine = Engine::new();
    // The form exists, but no version carries the declared tag.
    engine
        .apply_command(Command::DeployForms(vec![tagged_form(
            "review-form",
            Some("v1"),
        )]))
        .unwrap();
    engine
        .apply_command(Command::DeployProcess(form_binding_process(
            r#"bindingType="versionTag" versionTag="v9""#,
        )))
        .unwrap();

    let created = engine.apply_command(Command::create_instance("p")).unwrap();
    assert!(
        created
            .iter()
            .all(|e| !matches!(e, Event::UserTaskCreated { .. })),
        "no task is created on a versionTag miss: {created:?}"
    );
    let incidents = engine.active_incidents();
    assert_eq!(incidents.len(), 1, "one incident: {incidents:?}");
    assert!(
        incidents[0].reason.contains("v9"),
        "the reason names the missing tag: {}",
        incidents[0].reason
    );
    let incident_key = incidents[0].key;

    // Deploy the tagged version, then resolve: the activation re-drive binds it.
    let tagged = engine
        .apply_command(Command::DeployForms(vec![tagged_form(
            "review-form",
            Some("v9"),
        )]))
        .unwrap();
    let key_v9 = form_key_of(&tagged, "review-form");
    let resolved = engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();
    assert_eq!(created_form_key(&resolved), Some(key_v9));
    assert!(engine.active_incidents().is_empty());
}

// zeebe-cells: element:UserTask
#[test]
fn omitted_binding_type_still_resolves_the_latest_form_at_creation() {
    // No `bindingType` attribute at all: the Zeebe default `latest` applies —
    // the pre-#1193 behaviour every existing model relies on.
    let mut engine = Engine::new();
    let v1 = engine
        .apply_command(Command::DeployForms(vec![tagged_form("review-form", None)]))
        .unwrap();
    let _key_v1 = form_key_of(&v1, "review-form");
    engine
        .apply_command(Command::DeployProcess(form_binding_process("")))
        .unwrap();
    let v2 = engine
        .apply_command(Command::DeployForms(vec![tagged_form(
            "review-form",
            Some("v2"),
        )]))
        .unwrap();
    let key_v2 = form_key_of(&v2, "review-form");

    let created = engine.apply_command(Command::create_instance("p")).unwrap();
    assert_eq!(
        created_form_key(&created),
        Some(key_v2),
        "the default binding resolves the latest version at creation"
    );
    assert!(engine.active_incidents().is_empty());
}

// zeebe-cells: element:UserTask
#[test]
fn form_miss_resolution_does_not_reapply_input_mappings() {
    // #1193 review: the form lookup runs after input mappings succeeded, so
    // resolving its incident must re-run only the activation body — never
    // re-apply a (possibly non-idempotent) input mapping.
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:userTask id="review">
            <bpmn:extensionElements>
              <zeebe:userTask />
              <zeebe:formDefinition formId="review-form" bindingType="versionTag" versionTag="v9" />
              <zeebe:ioMapping><zeebe:input source="=counter + 1" target="counter" /></zeebe:ioMapping>
            </bpmn:extensionElements>
          </bpmn:userTask>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="review" />
          <bpmn:sequenceFlow id="b" sourceRef="review" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml).unwrap().pop().unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let mut create = Command::create_instance("p");
    if let Command::CreateInstance { variables, .. } = &mut create {
        variables.insert("counter".to_string(), crate::model::Value::Int(1));
    }
    engine.apply_command(create).unwrap();
    let incident_key = engine.active_incidents()[0].key;
    engine
        .apply_command(Command::DeployForms(vec![tagged_form(
            "review-form",
            Some("v9"),
        )]))
        .unwrap();
    let resolved = engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();
    assert!(created_form_key(&resolved).is_some(), "{resolved:?}");
    assert!(
        resolved.iter().all(|e| !matches!(
            e,
            Event::VariablesUpdated { .. } | Event::ScopedVariablesUpdated { .. }
        )),
        "input mappings must not be re-applied on resolution: {resolved:?}"
    );
}

/// The `deployment_key` stamped on every deploy event in `events` (asserting
/// they all agree — one deployment, one key).
fn sole_deployment_key(events: &[Event]) -> Key {
    let keys: std::collections::BTreeSet<Key> = events
        .iter()
        .filter_map(|e| match e {
            Event::DeploymentCreated { deployment_key }
            | Event::ProcessDeployed { deployment_key, .. }
            | Event::FormDeployed { deployment_key, .. }
            | Event::GenericResourceDeployed { deployment_key, .. }
            | Event::DecisionRequirementsDeployed { deployment_key, .. }
            | Event::DecisionDeployed { deployment_key, .. } => Some(*deployment_key),
            _ => None,
        })
        .collect();
    assert_eq!(keys.len(), 1, "one deployment key per bundle: {keys:?}");
    keys.into_iter().next().unwrap_or_default()
}

fn linked_resource_process() -> crate::model::ProcessDefinition {
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="lp">
          <bpmn:startEvent id="s" />
          <bpmn:serviceTask id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="run-agent" />
              <zeebe:linkedResources>
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
    crate::bpmn::parse_bpmn(xml).unwrap().pop().unwrap()
}

fn policy(content: &str) -> crate::command::GenericResource {
    crate::command::GenericResource {
        resource_id: "policy.md".to_string(),
        resource_name: "policy.md".to_string(),
        content: content.to_string(),
        version_tag: None,
    }
}

// zeebe-cells: element:UserTask
#[test]
fn a_mixed_bundle_shares_one_deployment_key_so_deployment_bindings_resolve() {
    // #1193 review: one HTTP deployment (process + form + generic resource)
    // must be ONE deployment. Per-kind commands minted a key per kind, so a
    // co-deployed `bindingType: deployment` form incidented and a co-deployed
    // linked resource was silently omitted from the worker header.
    let mut engine = Engine::new();
    let events = engine
        .apply_command(Command::DeployBundle(crate::command::DeploymentBundle {
            processes: vec![
                form_binding_process(r#"bindingType="deployment""#),
                linked_resource_process(),
            ],
            forms: vec![tagged_form("review-form", None)],
            resources: vec![policy("# v1")],
            ..Default::default()
        }))
        .unwrap();
    sole_deployment_key(&events);
    let form_key = form_key_of(&events, "review-form");

    let created = engine.apply_command(Command::create_instance("p")).unwrap();
    assert_eq!(created_form_key(&created), Some(form_key));
    assert!(engine.active_incidents().is_empty());

    engine
        .apply_command(Command::create_instance("lp"))
        .unwrap();
    let activated = engine.activate_jobs("run-agent", "w1", 10, 60_000, 0);
    let resource_key = engine.state().resources["policy.md"].key;
    assert_eq!(
        activated[0].custom_headers.get("linkedResources"),
        Some(&format!(
            "[{{\"resourceKey\":\"{resource_key}\",\"resourceType\":\"GenericScript\",\"linkName\":\"policy\"}}]"
        )),
        "the co-deployed linked resource resolves"
    );
}

// zeebe-cells: element:UserTask
#[test]
fn a_partially_changed_bundle_reversions_its_unchanged_resources_under_the_new_key() {
    // Reused resources (#1193 review): redeploying a bundle where only the
    // form changed must not leave the unchanged process on the OLD deployment
    // key (its deployment-bound form lookup would then pin the stale form, or
    // miss outright). Zeebe re-versions every resource of a non-duplicate
    // deployment; an all-duplicate bundle stays a no-op.
    let bundle = |tag: Option<&str>| {
        Command::DeployBundle(crate::command::DeploymentBundle {
            processes: vec![form_binding_process(r#"bindingType="deployment""#)],
            forms: vec![tagged_form("review-form", tag)],
            ..Default::default()
        })
    };
    let mut engine = Engine::new();
    engine.apply_command(bundle(None)).unwrap();

    let again = engine.apply_command(bundle(None)).unwrap();
    assert!(
        again.iter().all(|e| !matches!(
            e,
            Event::ProcessDeployed { .. } | Event::FormDeployed { .. }
        )),
        "an all-duplicate bundle re-versions nothing: {again:?}"
    );

    let changed = engine.apply_command(bundle(Some("v2"))).unwrap();
    let key = sole_deployment_key(&changed);
    assert!(
        changed
            .iter()
            .any(|e| matches!(e, Event::ProcessDeployed { version: 2, .. })),
        "the unchanged process is re-versioned with the changed form"
    );
    assert_eq!(engine.state().processes["p"].deployment_key, key);
    let v2_form = form_key_of(&changed, "review-form");
    let created = engine.apply_command(Command::create_instance("p")).unwrap();
    assert_eq!(created_form_key(&created), Some(v2_form));
    assert!(engine.active_incidents().is_empty());
}

// zeebe-cells: element:UserTask
#[test]
fn an_all_duplicate_bundle_reversions_together_when_its_deployment_bindings_would_dangle() {
    // #1193 review: content equality does not guarantee SHARED deployment
    // membership. A process and its `deployment`-bound form deployed
    // SEPARATELY and then resubmitted unchanged TOGETHER must not be skipped
    // as duplicates — reuse would pin the process to a deployment without the
    // form, so every new instance would raise a deployment-binding incident.
    // The bundle is re-versioned together instead, making the submitted
    // binding resolvable.
    let process = || form_binding_process(r#"bindingType="deployment""#);
    let form = || tagged_form("review-form", None);
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process()))
        .unwrap();
    engine
        .apply_command(Command::DeployForms(vec![form()]))
        .unwrap();

    let together = engine
        .apply_command(Command::DeployBundle(crate::command::DeploymentBundle {
            processes: vec![process()],
            forms: vec![form()],
            ..Default::default()
        }))
        .unwrap();
    let key = sole_deployment_key(&together);
    assert!(
        together
            .iter()
            .any(|e| matches!(e, Event::ProcessDeployed { version: 2, .. })),
        "the unchanged process is re-versioned into the shared deployment: {together:?}"
    );
    assert!(
        together
            .iter()
            .any(|e| matches!(e, Event::FormDeployed { version: 2, .. })),
        "the unchanged form is re-versioned into the shared deployment: {together:?}"
    );
    assert_eq!(engine.state().processes["p"].deployment_key, key);
    let form_key = form_key_of(&together, "review-form");
    let created = engine.apply_command(Command::create_instance("p")).unwrap();
    assert_eq!(created_form_key(&created), Some(form_key));
    assert!(engine.active_incidents().is_empty());

    // Resubmitting the original XML parses to the original `deployment`
    // binding again, so it re-triggers the forced re-version (another version)
    // rather than ever comparing equal to the rebound record — the rewrite
    // cannot wedge the idempotency skip into reusing an unsatisfying
    // deployment.
    let resubmitted = engine
        .apply_command(Command::DeployBundle(crate::command::DeploymentBundle {
            processes: vec![process()],
            forms: vec![form()],
            ..Default::default()
        }))
        .unwrap();
    assert!(
        resubmitted
            .iter()
            .any(|e| matches!(e, Event::ProcessDeployed { version: 3, .. })),
        "the original XML re-versions again rather than reusing: {resubmitted:?}"
    );
    let created = engine.apply_command(Command::create_instance("p")).unwrap();
    assert!(created_form_key(&created).is_some());
    assert!(engine.active_incidents().is_empty());
}

// zeebe-cells: element:UserTask
#[test]
fn an_all_duplicate_bundle_stays_a_noop_when_its_deployment_binding_already_resolves() {
    // The membership gate's negative case: the process and its
    // `deployment`-bound form were deployed together originally, so reuse
    // keeps the submitted binding resolvable and the unchanged resubmission
    // must remain an idempotent no-op (no re-version churn).
    let bundle = || {
        Command::DeployBundle(crate::command::DeploymentBundle {
            processes: vec![form_binding_process(r#"bindingType="deployment""#)],
            forms: vec![tagged_form("review-form", None)],
            ..Default::default()
        })
    };
    let mut engine = Engine::new();
    engine.apply_command(bundle()).unwrap();
    let again = engine.apply_command(bundle()).unwrap();
    assert!(
        again.iter().all(|e| !matches!(
            e,
            Event::ProcessDeployed { .. } | Event::FormDeployed { .. }
        )),
        "a satisfied membership keeps the all-duplicate no-op: {again:?}"
    );
}

// zeebe-cells: element:ServiceTask
#[test]
fn an_all_duplicate_bundle_reversions_together_when_a_deployment_bound_linked_resource_would_dangle(
) {
    // The same membership defect class through a service task's
    // `deployment`-bound linked resource: process and resource deployed
    // separately, then resubmitted unchanged together — reuse would leave the
    // worker's `linkedResources` header unable to resolve the link.
    let process = || linked_resource_process();
    let resource = || policy("# v1");
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process()))
        .unwrap();
    engine
        .apply_command(Command::DeployGenericResources(vec![resource()]))
        .unwrap();

    let together = engine
        .apply_command(Command::DeployBundle(crate::command::DeploymentBundle {
            processes: vec![process()],
            resources: vec![resource()],
            ..Default::default()
        }))
        .unwrap();
    assert!(
        together
            .iter()
            .any(|e| matches!(e, Event::ProcessDeployed { version: 2, .. })),
        "the unchanged process is re-versioned into the shared deployment: {together:?}"
    );
    assert!(
        together
            .iter()
            .any(|e| matches!(e, Event::GenericResourceDeployed { version: 2, .. })),
        "the unchanged resource is re-versioned into the shared deployment: {together:?}"
    );

    engine
        .apply_command(Command::create_instance("lp"))
        .unwrap();
    let activated = engine.activate_jobs("run-agent", "w1", 10, 60_000, 0);
    let resource_key = engine.state().resources["policy.md"].key;
    assert_eq!(
        activated[0].custom_headers.get("linkedResources"),
        Some(&format!(
            "[{{\"resourceKey\":\"{resource_key}\",\"resourceType\":\"GenericScript\",\"linkName\":\"policy\"}}]"
        )),
        "the re-versioned shared deployment resolves the linked resource"
    );
}
