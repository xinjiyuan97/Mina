//! Single-agent event-driven engine. Low-level fabric and high-level run APIs.
pub mod event_runtime;
pub mod executor;
pub mod flow_runtime;
pub mod host;
pub mod job_runtime;
pub mod memory_fabric;
pub mod operator_compat;

pub use event_runtime::{EventRuntime, EventRuntimeConfig, RuntimeTickReport};
pub use flow_runtime::{FlowEffectRuntime, FlowEffectRuntimeConfig, FlowEffectTickReport};
pub use host::records::{PlannedRun, RunRepository, RunRuntimeError, StartedRun, unix_time_ms};
pub use host::runner::{Harness, HarnessError, MAX_RUN_STEPS, RunExecution, RunOptions};
pub use job_runtime::{JobRuntime, JobRuntimeConfig, JobTickReport};

pub mod bus;
pub use bus::OperatorBus;
pub use host::engine::{AgentEngine, AgentRunConfig};

pub mod definition;
pub mod graph;
pub use definition::{
    AgentDefinition, AgentGraph, GraphLifecycle, GraphLimits, GraphStart, GraphTerminal,
    OperatorDefinition, OperatorSubscription, TriggerDefinition,
};
pub use graph::{
    AgentBuilder, AgentResources, ConfiguredAgent, GraphAgent, GraphAgentConfig, GraphContext,
    GraphEngine, GraphEvent, GraphHost, GraphIngress, GraphOperator, GraphOutput, GraphRun,
    GraphStatus, GraphStepUsage, GraphWaitBridge, OperatorDescriptor, OperatorRegistry,
};

pub mod artifacts;

pub use host::{AgentLoop, SingleTurnAgent};

pub mod bindings;
pub use bindings::{BindingContract, BindingSnapshot, HostBindings, HostPolicy};

pub mod run_manager;
pub use run_manager::RunManager;
