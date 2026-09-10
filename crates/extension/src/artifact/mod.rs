//! Host-side artifact format support shared by concrete tools and applications.

#[cfg(feature = "ooxml")]
pub(crate) mod ooxml;

#[cfg(feature = "docx")]
pub mod docx;

#[cfg(feature = "pptx")]
pub mod pptx;

#[cfg(feature = "xlsx")]
pub mod xlsx;
