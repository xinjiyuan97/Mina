use crate::{harness::*, operator_fabric::*};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;

pub struct FabricBlobStore(pub Arc<dyn OperatorFabric>);
const MAX_ARTIFACT_BYTES: usize = 16 * 1024 * 1024;
const CHUNK_BYTES: usize = 256 * 1024;
#[derive(Serialize, Deserialize)]
struct Manifest {
    #[serde(default = "complete")]
    complete: bool,
    #[serde(default)]
    schema_version: Option<u32>,
    metadata: BlobMetadata,
    chunks: usize,
}
fn complete() -> bool {
    true
}
fn backend(error: impl std::fmt::Display) -> BlobStoreError {
    BlobStoreError::backend(error.to_string())
}

impl BlobStore for FabricBlobStore {
    fn put(&self, command: PutBlob) -> BlobStoreFuture<'_, BlobMetadata> {
        Box::pin(async move {
            if command.data.len() > MAX_ARTIFACT_BYTES {
                return Err(backend("artifact_resource_limit"));
            }
            let metadata = BlobMetadata {
                blob_id: BlobId::new(),
                media_type: command.media_type,
                name: command.name,
                size_bytes: command.data.len() as u64,
                created_at_ms: command.created_at_ms,
            };
            // Publish an incomplete manifest first so failed uploads remain ageable and collectible.
            self.0
                .execute(FabricCommand::Transact {
                    checkpoint: CheckpointWrite {
                        key: format!("pipeline-artifact:{}", metadata.blob_id),
                        expected: None,
                        value: json!(Manifest {
                            complete: false,
                            schema_version: Some(1),
                            metadata: metadata.clone(),
                            chunks: command.data.len().div_ceil(CHUNK_BYTES)
                        }),
                    },
                    outbox: vec![],
                    delivery: None,
                })
                .await
                .map_err(backend)?;
            let mut chunks = 0;
            for (index, chunk) in command.data.chunks(CHUNK_BYTES).enumerate() {
                self.0
                    .execute(FabricCommand::Transact {
                        checkpoint: CheckpointWrite {
                            key: format!("pipeline-artifact:{}:{index}", metadata.blob_id),
                            expected: None,
                            value: json!(STANDARD.encode(chunk)),
                        },
                        outbox: vec![],
                        delivery: None,
                    })
                    .await
                    .map_err(backend)?;
                chunks += 1;
            }
            self.0
                .execute(FabricCommand::Transact {
                    checkpoint: CheckpointWrite {
                        key: format!("pipeline-artifact:{}", metadata.blob_id),
                        expected: Some(0),
                        value: json!(Manifest {
                            complete: true,
                            schema_version: Some(1),
                            metadata: metadata.clone(),
                            chunks
                        }),
                    },
                    outbox: vec![],
                    delivery: None,
                })
                .await
                .map_err(backend)?;
            Ok(metadata)
        })
    }
    fn get(&self, blob_id: BlobId) -> BlobStoreFuture<'_, Option<BlobObject>> {
        Box::pin(async move {
            let FabricReply::Checkpoint(saved) = self
                .0
                .execute(FabricCommand::ReadCheckpoint(format!(
                    "pipeline-artifact:{blob_id}"
                )))
                .await
                .map_err(backend)?
            else {
                return Err(backend("invalid reply"));
            };
            let Some(saved) = saved else {
                return Ok(None);
            };
            if saved
                .value
                .get("schema_version")
                .is_some_and(|version| version.as_u64() != Some(1))
            {
                return Err(backend("unsupported_artifact_version"));
            }
            let manifest: Manifest = serde_json::from_value(saved.value).map_err(backend)?;
            if !manifest.complete {
                return Err(backend("artifact_incomplete"));
            }
            if manifest.schema_version.is_some()
                && (manifest.chunks > MAX_ARTIFACT_BYTES / CHUNK_BYTES
                    || manifest.metadata.size_bytes > MAX_ARTIFACT_BYTES as u64)
            {
                return Err(backend("artifact_resource_limit"));
            }
            let mut data = Vec::new();
            for index in 0..manifest.chunks {
                let FabricReply::Checkpoint(Some(saved)) = self
                    .0
                    .execute(FabricCommand::ReadCheckpoint(format!(
                        "pipeline-artifact:{blob_id}:{index}"
                    )))
                    .await
                    .map_err(backend)?
                else {
                    return Err(backend("artifact gap"));
                };
                data.extend(
                    STANDARD
                        .decode(
                            saved
                                .value
                                .as_str()
                                .ok_or_else(|| backend("invalid chunk"))?,
                        )
                        .map_err(backend)?,
                );
                if data.len() as u64 > manifest.metadata.size_bytes {
                    return Err(backend("artifact_resource_limit"));
                }
            }
            if data.len() as u64 != manifest.metadata.size_bytes {
                return Err(backend("artifact size mismatch"));
            }
            Ok(Some(BlobObject {
                metadata: manifest.metadata,
                data,
            }))
        })
    }
}
