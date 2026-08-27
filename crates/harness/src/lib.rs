//! Host-owned execution runtime for `agent-core` agents.
//!
//! `agent-core` defines agent decisions and portable contracts. This crate owns
//! process runtime concerns: run coordination, durable activation, event/job
//! dispatch, and optional embedded script engines.

pub use agent_core::harness::*;

pub mod adf;
mod config;
mod event_runtime;
mod flow_runtime;
mod job_runtime;
pub mod run_runtime;
mod runtime;
pub mod script;

pub use config::{
    AdfConfig, AgentConfig, AgentKind, ApprovalConfig, ConfigError, ConfiguredSkill,
    ContextStrategy, HarnessConfig, Modalities, Modality, ModelConfig, OrchestrationConfig,
    ProviderConfig, QuickJsConfig, ScriptConfig, SecretSource, SecretString,
};
pub use event_runtime::{EventRuntime, EventRuntimeConfig, RuntimeTickReport};
pub use flow_runtime::{FlowEffectRuntime, FlowEffectRuntimeConfig, FlowEffectTickReport};
pub use job_runtime::{JobRuntime, JobRuntimeConfig, JobTickReport};
pub use run_runtime::{PlannedRun, RunRuntime, RunRuntimeError, StartedRun, unix_time_ms};
pub use runtime::{Harness, HarnessError, MAX_RUN_STEPS, RunExecution, RunOptions};

/// Event contracts plus their host-side durable coordinator.
pub mod event {
    pub use crate::{EventRuntime, EventRuntimeConfig, RuntimeTickReport};
    pub use agent_core::event_runtime::*;
}
