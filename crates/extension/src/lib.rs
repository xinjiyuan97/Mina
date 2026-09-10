//! Concrete external implementations for `agent-core` ports.
//!
//! Every integration is feature-gated so a small CLI does not link database or
//! monitoring dependencies that only the server needs.

#[cfg(any(feature = "pptx", feature = "xlsx", feature = "docx"))]
pub mod artifact;

#[cfg(feature = "context-model")]
pub mod context;

#[cfg(feature = "adf")]
pub mod adf;

pub mod provider;

#[cfg(feature = "workspace")]
pub mod workspace;

#[cfg(feature = "builtin-tools")]
pub mod tool;

#[cfg(feature = "host-sandbox")]
pub mod sandbox;

#[cfg(any(
    feature = "sqlite",
    feature = "filesystem",
    feature = "workspace-skill"
))]
pub mod store;

#[cfg(feature = "observability-tracing")]
pub mod observability;
