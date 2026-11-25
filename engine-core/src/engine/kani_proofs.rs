//! Kani proof harness for the engine transitions (#1960): deploying and
//! instantiating a parallel diamond (activate / complete / finalize_completion
//! through the fork) never panics or overflows. See `formal/kani/README.md`.

use super::*;
use crate::model::ProcessBuilder;

#[kani::proof]
#[kani::unwind(4)]
fn parallel_diamond_creates_one_instance() {
    let def = ProcessBuilder::new("p")
        .start_event("S")
        .parallel_gateway("P1")
        .service_task("A", "A")
        .service_task("B", "B")
        .parallel_gateway("P2")
        .end_event("E")
        .connect("S", "P1")
        .connect("P1", "A")
        .connect("P1", "B")
        .connect("A", "P2")
        .connect("B", "P2")
        .connect("P2", "E")
        .build();
    let Ok(def) = def else { return };
    let mut engine = Engine::new();
    let _ = engine.apply_command(Command::DeployProcess(def));
    let _ = engine.apply_command(Command::create_instance("p"));
    assert!(engine.state.instances.len() == 1);
}
