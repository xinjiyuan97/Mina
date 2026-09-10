//! Portable workspace filesystem boundary and environment-specific adapters.
//!
//! The agent core only invokes tools. Filesystem semantics belong here so the
//! same file tools can run against a native directory, browser OPFS, or S3.

mod contract;

#[cfg(all(feature = "workspace-native", not(target_arch = "wasm32")))]
mod native;
#[cfg(all(feature = "workspace-opfs", target_arch = "wasm32"))]
mod opfs;
#[cfg(all(feature = "workspace-s3", not(target_arch = "wasm32")))]
mod s3;

pub use contract::*;
#[cfg(all(feature = "workspace-native", not(target_arch = "wasm32")))]
pub use native::*;
#[cfg(all(feature = "workspace-opfs", target_arch = "wasm32"))]
pub use opfs::*;
#[cfg(all(feature = "workspace-s3", not(target_arch = "wasm32")))]
pub use s3::*;
