use std::{fmt, future::Future, pin::Pin, str::FromStr};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BlobId(Uuid);

impl BlobId {
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for BlobId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for BlobId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for BlobId {
    type Err = uuid::Error;

    fn from_str(source: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(source).map(Self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobMetadata {
    pub blob_id: BlobId,
    pub media_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub size_bytes: u64,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobObject {
    pub metadata: BlobMetadata,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutBlob {
    pub media_type: String,
    pub name: Option<String>,
    pub data: Vec<u8>,
    pub created_at_ms: i64,
}

pub type BlobStoreFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, BlobStoreError>> + Send + 'a>>;

/// Durable binary-object boundary used by gateways and model adapters.
///
/// Session and checkpoint contracts carry only `BlobId`; provider adapters
/// resolve bytes immediately before constructing an upstream model request.
pub trait BlobStore: Send + Sync + 'static {
    fn put(&self, command: PutBlob) -> BlobStoreFuture<'_, BlobMetadata>;

    fn get(&self, blob_id: BlobId) -> BlobStoreFuture<'_, Option<BlobObject>>;
}

#[derive(Debug, Error)]
pub enum BlobStoreError {
    #[error("blob store backend failed: {0}")]
    Backend(String),
}

impl BlobStoreError {
    #[must_use]
    pub fn backend(message: impl Into<String>) -> Self {
        Self::Backend(message.into())
    }
}
