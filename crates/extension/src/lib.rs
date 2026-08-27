//! Concrete external implementations for `agent-core` ports.
//!
//! Every integration is feature-gated so a small CLI does not link database or
//! monitoring dependencies that only the server needs.

pub use agent_core as core;

#[cfg(feature = "context-model")]
pub mod context;

#[cfg(feature = "adf")]
pub mod adf;

#[cfg(feature = "openai")]
pub mod provider;

#[cfg(feature = "builtin-tools")]
pub mod tool;

#[cfg(feature = "host-sandbox")]
pub mod sandbox;

#[cfg(any(feature = "sqlite", feature = "filesystem"))]
pub mod store;

#[cfg(feature = "observability-tracing")]
pub mod observability;
