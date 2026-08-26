//! Stable Agent contracts and transport-independent Harness runtime.
//!
//! Provider SDKs, databases, host tools and monitoring vendors belong to
//! `agent-extension` and must never become dependencies of this crate.

pub mod context;
pub mod harness;
pub mod memory;
pub mod observability;
pub mod sandbox;
pub mod script;
pub mod skill;
pub mod tool;
