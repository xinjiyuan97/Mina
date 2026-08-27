//! Stable, provider-neutral Agent contracts and decision runtime.
//!
//! Process orchestration and embedded engines belong to `agent-harness`;
//! providers, databases, host tools and monitoring adapters belong to
//! `agent-extension`. Neither may become a dependency of this crate.

pub mod adf;
pub mod context;
pub mod event_runtime;
pub mod harness;
pub mod memory;
pub mod observability;
pub mod sandbox;
pub mod script;
pub mod skill;
pub mod tool;
