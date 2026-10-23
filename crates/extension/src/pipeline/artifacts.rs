use agent_core::{harness::*, operator_fabric::*};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;

pub struct FabricBlobStore(pub Arc<dyn OperatorFabric>);
#[derive(Serialize, Deserialize)]
struct Manifest {
    metadata: BlobMetadata,
    chunks: usize,
}
fn backend(error: impl std::fmt::Display) -> BlobStoreError {
    BlobStoreError::backend(error.to_string())
}

impl BlobStore for FabricBlobStore {
    fn put(&self, command: PutBlob) -> BlobStoreFuture<'_, BlobMetadata> {
        Box::pin(async move {
            let metadata = BlobMetadata {
                blob_id: BlobId::new(),
                media_type: command.media_type,
                name: command.name,
                size_bytes: command.data.len() as u64,
                created_at_ms: command.created_at_ms,
            };
            let mut chunks = 0;
            for (index, chunk) in command.data.chunks(256 * 1024).enumerate() {
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
                        expected: None,
                        value: json!(Manifest {
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
            let manifest: Manifest = serde_json::from_value(saved.value).map_err(backend)?;
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
