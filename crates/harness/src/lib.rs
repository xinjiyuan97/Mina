//! Multi-agent event-driven orchestration and host configuration/adapters.
//! `multi` owns cross-agent messaging, supervisor and discussion patterns.
//! Single-agent execution is implemented in `agent_core::agent`; the reexports
//! below preserve source compatibility for existing integrations.

pub mod adf;
mod config;
pub mod context_operator;
mod event_runtime;
mod flow_runtime;
mod job_runtime;
pub mod operator_compat;
pub mod operator_fabric;
pub mod run_runtime;
mod runtime;
pub mod script;

pub use config::{
    AdfConfig, AgentConfig, AgentKind, ApprovalConfig, ConfigError, ConfiguredSkill,
    ContextStrategy, HarnessConfig, OrchestrationConfig, QuickJsConfig, ScriptConfig,
};
pub use event_runtime::{EventRuntime, EventRuntimeConfig, RuntimeTickReport};
pub use flow_runtime::{FlowEffectRuntime, FlowEffectRuntimeConfig, FlowEffectTickReport};
pub use job_runtime::{JobRuntime, JobRuntimeConfig, JobTickReport};
pub use run_runtime::{PlannedRun, RunRepository, RunRuntimeError, StartedRun, unix_time_ms};
pub use runtime::{Harness, HarnessError, MAX_RUN_STEPS, RunExecution, RunOptions};

pub mod multi;
