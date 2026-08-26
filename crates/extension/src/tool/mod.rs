//! Built-in, host-selectable tools for Mina-compatible agents.
//!
//! This crate owns concrete implementations. The harness owns orchestration,
//! validation, risk approval, cancellation, and public events.

mod catalog;
mod command;
mod filesystem;
mod search;
mod time_tool;
mod workspace;

pub use catalog::{BuiltinToolCatalog, BuiltinToolCatalogError};
pub use command::RunCommandTool;
pub use filesystem::{EditTool, ListDirectoryTool, ReadTool, WriteTool};
pub use search::{
    SearchBackend, SearchBackendDescriptor, SearchFuture, SearchHit, SearchRequest, SearchTool,
    WorkspaceSearchBackend,
};
pub use time_tool::GetCurrentTimeTool;
