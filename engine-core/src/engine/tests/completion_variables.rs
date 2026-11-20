//! Task completion variables vs. output mappings (#1533, Zeebe parity).
//!
//! Zeebe's `JobCompleteProcessor` / `UserTaskCompleteProcessor` merge the
//! completion variables LOCAL to the task's element instance when the element
//! declares output mappings (`mergeLocalDocument`), and propagate them from the
//! flow scope otherwise (`mergeDocument`). The defect class guarded here is a
//! completion path that propagates raw completion variables past an output
//! mapping — every completion surface (job, user task, user task deferred
//! behind a `completing` listener) is exercised.
use super::*;

/// `start -> t -> hold -> end`, where `t` is the given task element (its body
/// spliced in verbatim) and `hold` parks the instance so its root scope can be
/// inspected after `t` completes.
fn process_with(task: &str) -> ProcessDefinition {
    let xml = format!(
        r#"<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                             xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
          <bpmn:process id="p" isExecutable="true">
            <bpmn:startEvent id="s" />
            {task}
            <bpmn:serviceTask id="hold">
              <bpmn:extensionElements><zeebe:taskDefinition type="hold" /></bpmn:extensionElements>
            </bpmn:serviceTask>
            <bpmn:endEvent id="e" />
            <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="t" />
            <bpmn:sequenceFlow id="f2" sourceRef="t" targetRef="hold" />
            <bpmn:sequenceFlow id="f3" sourceRef="hold" targetRef="e" />
          </bpmn:process>
        </bpmn:definitions>"#
    );
    crate::bpmn::parse_bpmn(&xml).unwrap().remove(0)
}

const OUTPUT: &str =
    r#"<zeebe:ioMapping><zeebe:output source="=result" target="o1" /></zeebe:ioMapping>"#;

fn start(def: ProcessDefinition) -> (Engine, Key) {
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let inst = engine
        .apply_command(Command::create_instance("p"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    (engine, inst)
}

fn result_vars() -> BTreeMap<String, Value> {
    BTreeMap::from([("result".to_string(), Value::Int(33))])
}

fn complete_job(engine: &mut Engine, job_type: &str, vars: BTreeMap<String, Value>) {
    let job_key = engine.activate_jobs(job_type, "w", 1, 60_000, 0)[0].key;
    engine
        .apply_command(Command::complete_job_with(job_key, vars))
        .unwrap();
}

fn only_user_task(engine: &Engine) -> Key {
    let keys: Vec<Key> = engine.state().user_tasks.keys().copied().collect();
    assert_eq!(keys.len(), 1, "exactly one user task expected");
    keys[0]
}

/// The task completed and the instance parked on `hold`: only the output
/// mapping's `o1` reached the process scope, never the raw `result`.
fn assert_task_local(engine: &Engine, inst: Key) {
    assert_eq!(engine.pending_jobs()[0].job_type, "hold");
    assert_eq!(io_var(engine, inst, "o1"), Some(Value::Int(33)));
    assert_eq!(
        io_var(engine, inst, "result"),
        None,
        "completion variable leaked to the process scope"
    );
}

#[test]
fn job_completion_variables_stay_task_local_when_the_task_has_output_mappings() {
    let (mut engine, inst) = start(process_with(&format!(
        r#"<bpmn:serviceTask id="t"><bpmn:extensionElements>
             <zeebe:taskDefinition type="work" />{OUTPUT}
           </bpmn:extensionElements></bpmn:serviceTask>"#
    )));
    complete_job(&mut engine, "work", result_vars());
    assert_task_local(&engine, inst);
}

#[test]
fn job_completion_variables_propagate_when_the_task_has_no_output_mappings() {
    let (mut engine, inst) = start(process_with(
        r#"<bpmn:serviceTask id="t"><bpmn:extensionElements>
             <zeebe:taskDefinition type="work" />
           </bpmn:extensionElements></bpmn:serviceTask>"#,
    ));
    complete_job(&mut engine, "work", result_vars());
    assert_eq!(io_var(&engine, inst, "result"), Some(Value::Int(33)));
}

#[test]
fn user_task_completion_variables_stay_task_local_when_the_task_has_output_mappings() {
    let (mut engine, inst) = start(process_with(&format!(
        r#"<bpmn:userTask id="t"><bpmn:extensionElements>
             <zeebe:userTask />{OUTPUT}
           </bpmn:extensionElements></bpmn:userTask>"#
    )));
    let key = only_user_task(&engine);
    engine
        .apply_command(Command::complete_user_task_with(key, result_vars()))
        .unwrap();
    assert_task_local(&engine, inst);
}

#[test]
fn deferred_user_task_completion_variables_stay_task_local_when_the_task_has_output_mappings() {
    // The completion variables ride on the pending transition behind a
    // `completing` listener and are merged when the chain drains.
    let (mut engine, inst) = start(process_with(&format!(
        r#"<bpmn:userTask id="t"><bpmn:extensionElements>
             <zeebe:userTask />{OUTPUT}
             <zeebe:taskListeners>
               <zeebe:taskListener eventType="completing" type="lis" />
             </zeebe:taskListeners>
           </bpmn:extensionElements></bpmn:userTask>"#
    )));
    let key = only_user_task(&engine);
    engine
        .apply_command(Command::complete_user_task_with(key, result_vars()))
        .unwrap();
    complete_job(&mut engine, "lis", BTreeMap::new());
    assert_task_local(&engine, inst);
}
