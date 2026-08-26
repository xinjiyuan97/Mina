//! Built-in, host-selectable tools for Mina-compatible agents.
//!
//! This crate owns concrete implementations. The harness owns orchestration,
//! validation, risk approval, cancellation, and public events.

mod catalog;
mod filesystem;
mod patch;
mod search;
mod terminal;
mod time_tool;
mod workspace;

pub use catalog::{BuiltinToolCatalog, BuiltinToolCatalogError};
pub use filesystem::{EditTool, ListDirectoryTool, ReadTool, WriteTool};
pub use patch::ApplyPatchTool;
pub use search::{
    SearchBackend, SearchBackendDescriptor, SearchFuture, SearchHit, SearchRequest, SearchTool,
    WorkspaceSearchBackend,
};
pub use terminal::{ExecCommandTool, ShellCommandTool, WriteStdinTool};
pub use time_tool::GetCurrentTimeTool;
