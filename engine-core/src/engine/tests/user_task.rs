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

    // Completing again is rejected: the task is no longer active.
    assert!(matches!(
        engine.apply_command(Command::complete_user_task(user_task_key)),
        Err(EngineError::UserTaskNotActive { .. })
    ));
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
