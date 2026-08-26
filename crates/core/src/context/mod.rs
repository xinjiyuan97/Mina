//! Context budgeting, compression contracts, and the default orchestration engine.

mod contract;
mod engine;
mod strategies;

pub use contract::*;
pub use engine::{ContextBuildRequest, ContextEngine};
pub use strategies::*;
