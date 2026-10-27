use std::sync::Arc;

use agent_core::contracts::{BlobObject, BlobStore, ModelAttachment, ModelError, ModelErrorKind};
use base64::{Engine as _, engine::general_purpose::STANDARD};

pub(super) async fn load_blob(
    store: Option<&Arc<dyn BlobStore>>,
    attachment: &ModelAttachment,
) -> Result<BlobObject, ModelError> {
    let store = store.ok_or_else(|| {
        ModelError::new(
            ModelErrorKind::InvalidRequest,
            "binary input was provided but no blob store is configured",
            false,
        )
    })?;
    let object = store
        .get(attachment.blob_id)
        .await
        .map_err(|_| {
            ModelError::new(
                ModelErrorKind::Internal,
                "binary input could not be loaded",
                true,
            )
        })?
        .ok_or_else(|| {
            ModelError::new(
                ModelErrorKind::InvalidRequest,
                "binary input no longer exists",
                false,
            )
        })?;
    if object.metadata.media_type != attachment.media_type
        || attachment
            .size_bytes
            .is_some_and(|size| size != object.metadata.size_bytes)
        || object.metadata.size_bytes != object.data.len() as u64
        || attachment.name.as_ref().is_some_and(|name| {
            object
                .metadata
                .name
                .as_ref()
                .is_some_and(|stored| name != stored)
        })
    {
        return Err(ModelError::new(
            ModelErrorKind::InvalidRequest,
            "binary input metadata does not match stored metadata",
            false,
        ));
    }
    if attachment.media_type.starts_with("image/")
        && !matches!(
            attachment.media_type.as_str(),
            "image/png" | "image/jpeg" | "image/gif" | "image/webp"
        )
    {
        return Err(ModelError::new(
            ModelErrorKind::InvalidRequest,
            "unsupported image input media type",
            false,
        ));
    }
    Ok(object)
}

pub(super) fn base64_data(data: &[u8]) -> String {
    STANDARD.encode(data)
}

pub(super) fn data_url(media_type: &str, data: &[u8]) -> String {
    format!("data:{media_type};base64,{}", base64_data(data))
}

#[cfg(test)]
pub(super) async fn fixture(
    media_type: &str,
) -> (
    agent_core::contracts::ModelRequest,
    Arc<dyn BlobStore>,
    tempfile::TempDir,
) {
    use agent_core::contracts::{
        BlobId, BlobMetadata, BlobStoreFuture, ModelMessage, ModelRequest, PutBlob, RunId,
    };
    struct Fixed(BlobObject);
    impl BlobStore for Fixed {
        fn put(&self, _: PutBlob) -> BlobStoreFuture<'_, BlobMetadata> {
            Box::pin(async { panic!("read only") })
        }
        fn get(&self, id: BlobId) -> BlobStoreFuture<'_, Option<BlobObject>> {
            Box::pin(async move { Ok((id == self.0.metadata.blob_id).then(|| self.0.clone())) })
        }
    }
    let directory = tempfile::tempdir().expect("temp");
    let metadata = BlobMetadata {
        blob_id: BlobId::new(),
        media_type: media_type.into(),
        name: Some("fixture.bin".into()),
        size_bytes: 3,
        created_at_ms: 1,
    };
    let store: Arc<dyn BlobStore> = Arc::new(Fixed(BlobObject {
        metadata: metadata.clone(),
        data: vec![1, 2, 3],
    }));
    (
        ModelRequest {
            run_id: RunId::new(),
            model: "fake".into(),
            messages: vec![ModelMessage::user_with_attachments(
                "",
                vec![ModelAttachment {
                    blob_id: metadata.blob_id,
                    media_type: metadata.media_type,
                    name: metadata.name,
                    size_bytes: Some(metadata.size_bytes),
                }],
            )],
            tools: vec![],
            max_output_tokens: None,
        },
        store,
        directory,
    )
}
