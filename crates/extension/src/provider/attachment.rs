use std::sync::Arc;

use agent_core::harness::{BlobObject, BlobStore, ModelAttachment, ModelError, ModelErrorKind};
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
    if object.metadata.media_type != attachment.media_type {
        return Err(ModelError::new(
            ModelErrorKind::InvalidRequest,
            "binary input media type does not match stored metadata",
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
