//! Single-agent decisions, event-driven execution and provider-neutral contracts.
//! `agent` exposes the low-level OperatorBus and high-level AgentEngine APIs.
//! Storage, provider and tool adapters are injected through contracts; Core does
//! not depend on Extension or Harness at runtime.

pub mod adf;
pub mod context;
pub mod contracts;
pub mod memory;
pub mod observability;
pub mod operator;
pub mod sandbox;
pub mod script;
pub mod skill;
pub mod tool;
pub mod tool_authorization;

pub mod agent;
