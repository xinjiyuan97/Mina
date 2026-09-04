use std::path::{Path, PathBuf};

use agent_core::harness::{
    BlobId, BlobMetadata, BlobObject, BlobStore, BlobStoreError, BlobStoreFuture, PutBlob,
};

/// Filesystem-backed blob storage for durable multimodal inputs.
///
/// Blob identifiers, rather than user-supplied names, determine every path.
/// This keeps file names display-only and prevents path traversal.
#[derive(Debug, Clone)]
pub struct FilesystemBlobStore {
    root: PathBuf,
}

impl FilesystemBlobStore {
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn data_path(&self, blob_id: BlobId) -> PathBuf {
        self.root.join(format!("{blob_id}.blob"))
    }

    fn metadata_path(&self, blob_id: BlobId) -> PathBuf {
        self.root.join(format!("{blob_id}.json"))
    }
}

impl BlobStore for FilesystemBlobStore {
    fn put(&self, command: PutBlob) -> BlobStoreFuture<'_, BlobMetadata> {
        Box::pin(async move {
            let blob_id = BlobId::new();
            let metadata = BlobMetadata {
                blob_id,
                media_type: command.media_type,
                name: command.name,
                size_bytes: command.data.len() as u64,
                created_at_ms: command.created_at_ms,
            };
            tokio::fs::create_dir_all(&self.root)
                .await
                .map_err(|error| BlobStoreError::backend(error.to_string()))?;

            let data_path = self.data_path(blob_id);
            let metadata_path = self.metadata_path(blob_id);
            let temp_data_path = self.root.join(format!(".{blob_id}.blob.tmp"));
            let temp_metadata_path = self.root.join(format!(".{blob_id}.json.tmp"));
            let encoded = serde_json::to_vec(&metadata)
                .map_err(|error| BlobStoreError::backend(error.to_string()))?;

            tokio::fs::write(&temp_data_path, command.data)
                .await
                .map_err(|error| BlobStoreError::backend(error.to_string()))?;
            if let Err(error) = tokio::fs::write(&temp_metadata_path, encoded).await {
                let _ = tokio::fs::remove_file(&temp_data_path).await;
                return Err(BlobStoreError::backend(error.to_string()));
            }
            if let Err(error) = tokio::fs::rename(&temp_data_path, &data_path).await {
                let _ = tokio::fs::remove_file(&temp_data_path).await;
                let _ = tokio::fs::remove_file(&temp_metadata_path).await;
                return Err(BlobStoreError::backend(error.to_string()));
            }
            if let Err(error) = tokio::fs::rename(&temp_metadata_path, &metadata_path).await {
                let _ = tokio::fs::remove_file(&data_path).await;
                let _ = tokio::fs::remove_file(&temp_metadata_path).await;
                return Err(BlobStoreError::backend(error.to_string()));
            }

            Ok(metadata)
        })
    }

    fn get(&self, blob_id: BlobId) -> BlobStoreFuture<'_, Option<BlobObject>> {
        Box::pin(async move {
            let metadata = match tokio::fs::read(self.metadata_path(blob_id)).await {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(error) => return Err(BlobStoreError::backend(error.to_string())),
            };
            let metadata: BlobMetadata = serde_json::from_slice(&metadata)
                .map_err(|error| BlobStoreError::backend(error.to_string()))?;
            if metadata.blob_id != blob_id {
                return Err(BlobStoreError::backend("blob metadata identity mismatch"));
            }
            let data = match tokio::fs::read(self.data_path(blob_id)).await {
                Ok(data) => data,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(error) => return Err(BlobStoreError::backend(error.to_string())),
            };
            if data.len() as u64 != metadata.size_bytes {
                return Err(BlobStoreError::backend("blob size does not match metadata"));
            }
            Ok(Some(BlobObject { metadata, data }))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn round_trips_blob_bytes_and_metadata() {
        let directory = tempfile::tempdir().expect("temporary directory should be created");
        let store = FilesystemBlobStore::new(directory.path());
        let metadata = store
            .put(PutBlob {
                media_type: "image/png".into(),
                name: Some("screen.png".into()),
                data: vec![1, 2, 3],
                created_at_ms: 42,
            })
            .await
            .expect("blob should be stored");

        let object = store
            .get(metadata.blob_id)
            .await
            .expect("blob should load")
            .expect("blob should exist");
        assert_eq!(object.metadata, metadata);
        assert_eq!(object.data, vec![1, 2, 3]);
    }
}
