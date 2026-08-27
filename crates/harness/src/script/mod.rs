//! Host-owned portable script engines and durable JavaScript flows.

pub use agent_core::script::*;

mod flow;
pub use flow::{JavaScriptAgentMachine, JavaScriptFlowPolicy};

#[cfg(feature = "quickjs")]
mod quickjs;
#[cfg(feature = "quickjs")]
pub use quickjs::{QuickJsRuntime, QuickJsRuntimeConfig};
