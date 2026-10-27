//! Application-facing lifecycle and context preparation, independent of transport.
pub mod engine;
pub mod preparation;
pub mod records;
pub mod runner;
pub use engine::{AgentEngine, AgentRunConfig};
pub use preparation::AdmissionFacade;
#[cfg(not(target_arch = "wasm32"))]
pub use preparation::ContextPreparation;
pub use records::{PlannedRun, RunRepository, RunRuntimeError, StartedRun, unix_time_ms};
pub use runner::{Harness, HarnessError, MAX_RUN_STEPS, RunExecution, RunOptions};

mod agent_loop;
mod single_turn;
pub use agent_loop::AgentLoop;
pub use single_turn::SingleTurnAgent;
