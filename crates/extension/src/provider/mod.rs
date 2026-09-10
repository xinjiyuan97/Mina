//! Model provider adapters.

pub mod config;

pub use config::*;

#[cfg(feature = "openai")]
mod anthropic;
#[cfg(feature = "openai")]
mod attachment;
#[cfg(feature = "openai")]
mod configured;
#[cfg(feature = "openai")]
mod openai;
#[cfg(feature = "openai")]
mod responses;

#[cfg(feature = "openai")]
pub use anthropic::*;
#[cfg(feature = "openai")]
pub use configured::*;
#[cfg(feature = "openai")]
pub use openai::*;
#[cfg(feature = "openai")]
pub use responses::*;
