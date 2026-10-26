//! Single-agent event-driven engine. Low-level fabric and high-level run APIs.
pub mod context_operator;
pub mod event_runtime;
pub mod flow_runtime;
pub mod job_runtime;
pub mod memory_fabric;
pub mod operator_compat;
pub mod operator_fabric;
pub mod run_runtime;
pub mod runtime;
pub use event_runtime::{EventRuntime, EventRuntimeConfig, RuntimeTickReport};
pub use flow_runtime::{FlowEffectRuntime, FlowEffectRuntimeConfig, FlowEffectTickReport};
pub use job_runtime::{JobRuntime, JobRuntimeConfig, JobTickReport};
pub use run_runtime::{PlannedRun, RunRepository, RunRuntimeError, StartedRun, unix_time_ms};
pub use runtime::{Harness, HarnessError, MAX_RUN_STEPS, RunExecution, RunOptions};

pub mod bus;
pub mod engine;
pub use bus::OperatorBus;
pub use engine::{AgentEngine, AgentRunConfig};

/// Provider-neutral single-agent contracts (the historical module path remains supported).
pub use crate::harness as contracts;

pub mod definition;
pub mod graph;
pub use definition::{
    AgentDefinition, AgentGraph, GraphLifecycle, GraphLimits, GraphStart, GraphTerminal,
    OperatorDefinition, OperatorSubscription,
};
pub use graph::{
    AgentBuilder, AgentResources, ConfiguredAgent, GraphAgent, GraphAgentConfig, GraphContext,
    GraphEngine, GraphEvent, GraphHost, GraphIngress, GraphOperator, GraphOutput, GraphRun,
    GraphStatus, GraphStepUsage, GraphWaitBridge, OperatorDescriptor, OperatorRegistry,
};

pub mod artifacts;
