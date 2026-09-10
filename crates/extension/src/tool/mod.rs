//! Built-in, host-selectable tools for Mina-compatible agents.
//!
//! This crate owns concrete implementations. The harness owns orchestration,
//! validation, risk approval, cancellation, and public events.

mod catalog;
#[cfg(feature = "docx-tools")]
mod docx;
mod filesystem;
mod javascript;
mod job;
mod native_path;
mod patch;
#[cfg(feature = "pptx-tools")]
mod pptx;
mod search;
mod terminal;
mod time_tool;
#[cfg(feature = "xlsx-tools")]
mod xlsx;

pub use catalog::{BuiltinToolCatalog, BuiltinToolCatalogError};
#[cfg(feature = "docx-tools")]
pub use docx::DocxCheckTool;
pub use filesystem::{EditTool, ListDirectoryTool, ReadTool, WriteTool};
pub use javascript::JavaScriptEvalTool;
pub use job::AsyncJobTool;
pub use patch::ApplyPatchTool;
#[cfg(feature = "pptx-tools")]
pub use pptx::PptxCheckTool;
pub use search::{
    SearchBackend, SearchBackendDescriptor, SearchFuture, SearchHit, SearchRequest, SearchTool,
    WorkspaceSearchBackend,
};
pub use terminal::{ExecCommandTool, ShellCommandTool, WriteStdinTool};
pub use time_tool::GetCurrentTimeTool;
#[cfg(feature = "xlsx-tools")]
pub use xlsx::XlsxCheckTool;
