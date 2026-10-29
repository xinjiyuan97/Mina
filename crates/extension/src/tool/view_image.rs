//! Reads bounded workspace images into the provider's shared blob store.
use super::filesystem::{map_workspace_error, parse_tool_path};
use crate::workspace::{ReadRequest, WorkspaceFs};
use agent_core::contracts::{
    BlobStore, ModelAttachment, PutBlob, Tool, ToolCallFuture, ToolCallRequest, ToolConcurrency,
    ToolDefinition, ToolError, ToolExecutionPolicy, ToolOutput,
};
use serde::Deserialize;
use serde_json::json;
use std::{
    io::Cursor,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

const MAX_BYTES: u64 = 16 * 1024 * 1024;
const MAX_DIMENSION: u32 = 8192;
const MAX_PIXELS: u64 = 16_000_000;

pub struct ViewImageTool {
    workspace: Arc<dyn WorkspaceFs>,
    blobs: Arc<dyn BlobStore>,
}
impl ViewImageTool {
    pub fn new(workspace: Arc<dyn WorkspaceFs>, blobs: Arc<dyn BlobStore>) -> Self {
        Self { workspace, blobs }
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Arguments {
    path: String,
}
fn failure(code: &str, message: &str) -> ToolError {
    ToolError::new(code, message, false)
}

fn validate(bytes: &[u8]) -> Result<(&'static str, u32, u32), ToolError> {
    let format = image::guess_format(bytes).map_err(|_| {
        failure(
            "unsupported_image",
            "Expected a PNG, JPEG, GIF or WebP image",
        )
    })?;
    let media_type = match format {
        image::ImageFormat::Png => "image/png",
        image::ImageFormat::Jpeg => "image/jpeg",
        image::ImageFormat::Gif => "image/gif",
        image::ImageFormat::WebP => "image/webp",
        _ => {
            return Err(failure(
                "unsupported_image",
                "Expected a PNG, JPEG, GIF or WebP image",
            ));
        }
    };
    let (width, height) = image::ImageReader::with_format(Cursor::new(bytes), format)
        .into_dimensions()
        .map_err(|_| failure("invalid_image", "Image headers could not be decoded"))?;
    if width == 0
        || height == 0
        || width > MAX_DIMENSION
        || height > MAX_DIMENSION
        || u64::from(width) * u64::from(height) > MAX_PIXELS
    {
        return Err(failure(
            "image_dimensions_exceeded",
            "Image exceeds the 8192-pixel side or 16-megapixel limit",
        ));
    }
    let mut reader = image::ImageReader::with_format(Cursor::new(bytes), format);
    let mut limits = image::Limits::default();
    limits.max_image_width = Some(MAX_DIMENSION);
    limits.max_image_height = Some(MAX_DIMENSION);
    limits.max_alloc = Some(128 * 1024 * 1024);
    reader.limits(limits);
    reader.decode().map_err(|_| {
        failure(
            "invalid_image",
            "Image could not be decoded within resource limits",
        )
    })?;
    Ok((media_type, width, height))
}
impl Tool for ViewImageTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition::new("view_image", "View a PNG, JPEG, GIF or WebP image in the workspace. Returns the image as visual input for the next model turn. Use a workspace-relative path. Maximum 16 MiB, 8192 pixels per side and 16 megapixels; requires a vision-capable model.",
            json!({"type":"object","properties":{"path":{"type":"string","minLength":1,"description":"Workspace-relative image file path"}},"required":["path"],"additionalProperties":false}))
            .with_execution_policy(ToolExecutionPolicy::read_only().with_concurrency(ToolConcurrency::ParallelSafe))
    }
    fn call(&self, request: ToolCallRequest) -> ToolCallFuture {
        let workspace = self.workspace.clone();
        let blobs = self.blobs.clone();
        Box::pin(async move {
            let operation = async {
                let args: Arguments = serde_json::from_value(request.arguments).map_err(|_| {
                    failure(
                        "invalid_tool_arguments",
                        "Expected a workspace-relative image path",
                    )
                })?;
                let path = parse_tool_path(&args.path)?;
                let file = workspace
                    .read(ReadRequest {
                        path: path.clone(),
                        offset: 0,
                        length: None,
                        max_bytes: MAX_BYTES,
                    })
                    .await
                    .map_err(map_workspace_error)?;
                if file.bytes.len() as u64 > MAX_BYTES {
                    return Err(failure("image_too_large", "Image exceeds the 16 MiB limit"));
                }
                let (bytes, media_type, width, height) = tokio::task::spawn_blocking(move || {
                    let (media_type, width, height) = validate(&file.bytes)?;
                    Ok::<_, ToolError>((file.bytes, media_type, width, height))
                })
                .await
                .map_err(|_| failure("image_decode_failed", "Image decoding did not complete"))??;
                if request.cancellation.is_cancelled() {
                    return Err(failure("tool_cancelled", "Image viewing was cancelled"));
                }
                let metadata = blobs
                    .put(PutBlob {
                        media_type: media_type.into(),
                        name: path.file_name().map(str::to_owned),
                        data: bytes,
                        created_at_ms: SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis()
                            .min(i64::MAX as u128) as i64,
                    })
                    .await
                    .map_err(|_| {
                        failure(
                            "image_store_failed",
                            "Image could not be stored for model input",
                        )
                    })?;
                let content = json!({"path":path.as_str(),"media_type":media_type,"width":width,"height":height,"size_bytes":metadata.size_bytes}).to_string();
                Ok(
                    ToolOutput::text(content).with_attachments(vec![ModelAttachment {
                        blob_id: metadata.blob_id,
                        media_type: metadata.media_type,
                        name: metadata.name,
                        size_bytes: Some(metadata.size_bytes),
                    }]),
                )
            };
            tokio::select! {
                biased;
                _ = request.cancellation.cancelled() => Err(failure("tool_cancelled", "Image viewing was cancelled")),
                result = operation => result,
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workspace::NativeWorkspaceFs;
    use agent_core::contracts::*;
    use std::{collections::HashMap, sync::Mutex};
    use tempfile::tempdir;

    #[derive(Default)]
    struct Blobs(Mutex<HashMap<BlobId, BlobObject>>);
    impl BlobStore for Blobs {
        fn put(&self, command: PutBlob) -> BlobStoreFuture<'_, BlobMetadata> {
            Box::pin(async move {
                let metadata = BlobMetadata {
                    blob_id: BlobId::new(),
                    media_type: command.media_type,
                    name: command.name,
                    size_bytes: command.data.len() as u64,
                    created_at_ms: command.created_at_ms,
                };
                self.0.lock().unwrap().insert(
                    metadata.blob_id,
                    BlobObject {
                        metadata: metadata.clone(),
                        data: command.data,
                    },
                );
                Ok(metadata)
            })
        }
        fn get(&self, id: BlobId) -> BlobStoreFuture<'_, Option<BlobObject>> {
            Box::pin(async move { Ok(self.0.lock().unwrap().get(&id).cloned()) })
        }
    }
    fn request(path: &str) -> ToolCallRequest {
        ToolCallRequest {
            run_id: RunId::new(),
            call_id: "image-call".into(),
            name: "view_image".into(),
            arguments: json!({"path":path}),
            cancellation: RunCancellation::new(),
        }
    }
    fn image_bytes(format: image::ImageFormat) -> Vec<u8> {
        let mut bytes = Cursor::new(Vec::new());
        image::DynamicImage::new_rgb8(3, 2)
            .write_to(&mut bytes, format)
            .unwrap();
        bytes.into_inner()
    }
    #[tokio::test]
    async fn view_image_detects_content_and_stores_real_image_bytes() {
        let dir = tempdir().unwrap();
        let blobs = Arc::new(Blobs::default());
        let tool = ViewImageTool::new(
            Arc::new(NativeWorkspaceFs::new(dir.path()).unwrap()),
            blobs.clone(),
        );
        for (format, media_type) in [
            (image::ImageFormat::Png, "image/png"),
            (image::ImageFormat::Jpeg, "image/jpeg"),
            (image::ImageFormat::Gif, "image/gif"),
            (image::ImageFormat::WebP, "image/webp"),
        ] {
            let bytes = image_bytes(format);
            std::fs::write(dir.path().join("misleading.txt"), &bytes).unwrap();
            let output = tool.call(request("misleading.txt")).await.unwrap();
            assert_eq!(output.attachments.len(), 1);
            let attachment = &output.attachments[0];
            assert_eq!(attachment.media_type, media_type);
            assert_eq!(attachment.size_bytes, Some(bytes.len() as u64));
            assert_eq!(
                blobs.get(attachment.blob_id).await.unwrap().unwrap().data,
                bytes
            );
            let result: serde_json::Value = serde_json::from_str(&output.content).unwrap();
            assert_eq!(result["width"], 3);
            assert_eq!(result["height"], 2);
            assert!(!output.content.contains("base64"));
        }
    }
    #[tokio::test]
    async fn view_image_rejects_bad_paths_corruption_limits_and_cancellation() {
        let dir = tempdir().unwrap();
        let blobs = Arc::new(Blobs::default());
        let tool = ViewImageTool::new(
            Arc::new(NativeWorkspaceFs::new(dir.path()).unwrap()),
            blobs.clone(),
        );
        std::fs::write(dir.path().join("fake.png"), b"not an image").unwrap();
        std::fs::write(
            dir.path().join("truncated.png"),
            &image_bytes(image::ImageFormat::Png)[..24],
        )
        .unwrap();
        std::fs::File::create(dir.path().join("large.png"))
            .unwrap()
            .set_len(MAX_BYTES + 1)
            .unwrap();
        image::DynamicImage::new_rgb8(MAX_DIMENSION + 1, 1)
            .save(dir.path().join("wide.png"))
            .unwrap();
        for path in [
            "../outside.png",
            "/tmp/outside.png",
            ".env",
            "missing.png",
            "fake.png",
            "truncated.png",
            "large.png",
            "wide.png",
        ] {
            assert!(tool.call(request(path)).await.is_err(), "{path}");
        }
        let cancelled = request("fake.png");
        cancelled.cancellation.cancel();
        assert_eq!(
            tool.call(cancelled).await.unwrap_err().code(),
            "tool_cancelled"
        );
        assert!(blobs.0.lock().unwrap().is_empty());
    }
    #[cfg(unix)]
    #[tokio::test]
    async fn view_image_cannot_follow_symlinks_outside_workspace() {
        let root = tempdir().unwrap();
        let outside = tempdir().unwrap();
        let file = outside.path().join("secret.png");
        std::fs::write(&file, image_bytes(image::ImageFormat::Png)).unwrap();
        std::os::unix::fs::symlink(file, root.path().join("link.png")).unwrap();
        let tool = ViewImageTool::new(
            Arc::new(NativeWorkspaceFs::new(root.path()).unwrap()),
            Arc::new(Blobs::default()),
        );
        assert!(tool.call(request("link.png")).await.is_err());
    }
    struct VisionModel {
        calls: std::sync::atomic::AtomicUsize,
        blobs: Arc<Blobs>,
    }
    impl ModelPort for VisionModel {
        fn stream(&self, request: ModelRequest) -> ModelEventStream {
            use std::sync::atomic::Ordering;
            let events = if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                assert!(request.tools.iter().any(|tool| tool.name == "view_image"));
                vec![
                    ModelEvent::ToolCallStarted {
                        call_id: "a".into(),
                        name: "view_image".into(),
                    },
                    ModelEvent::ToolCallArgumentsDelta {
                        call_id: "a".into(),
                        delta: json!({"path":"photo.png"}).to_string(),
                    },
                    ModelEvent::ToolCallStarted {
                        call_id: "b".into(),
                        name: "view_image".into(),
                    },
                    ModelEvent::ToolCallArgumentsDelta {
                        call_id: "b".into(),
                        delta: json!({"path":"photo.png"}).to_string(),
                    },
                    ModelEvent::Completed {
                        finish_reason: FinishReason::ToolCall,
                    },
                ]
            } else {
                let messages = request.messages;
                assert_eq!(
                    messages.iter().map(|m| m.role).collect::<Vec<_>>(),
                    vec![
                        ModelRole::System,
                        ModelRole::User,
                        ModelRole::Assistant,
                        ModelRole::Tool,
                        ModelRole::Tool,
                        ModelRole::User,
                        ModelRole::User
                    ]
                );
                assert_eq!(messages[3].tool_call_id.as_deref(), Some("a"));
                assert_eq!(messages[4].tool_call_id.as_deref(), Some("b"));
                for message in &messages[5..] {
                    assert_eq!(message.attachments.len(), 1);
                    let id = message.attachments[0].blob_id;
                    assert_eq!(
                        self.blobs.0.lock().unwrap()[&id].data,
                        image_bytes(image::ImageFormat::Png)
                    );
                }
                vec![
                    ModelEvent::TextDelta {
                        delta: "I can see the images".into(),
                    },
                    ModelEvent::Completed {
                        finish_reason: FinishReason::Stop,
                    },
                ]
            };
            Box::pin(futures_util::stream::iter(events))
        }
    }
    #[tokio::test]
    async fn view_image_reaches_next_model_round_after_all_tool_results_and_survives_recovery() {
        run_image_graph(false).await;
    }
    #[tokio::test]
    async fn view_image_works_with_compactor_and_explicit_attachment_budget() {
        run_image_graph(true).await;
    }
    async fn run_image_graph(compact: bool) {
        use agent_core::agent::{
            AgentBuilder, AgentResources, GraphAgentConfig, GraphStatus, OperatorRegistry,
            memory_fabric::MemoryFabric,
        };
        let root = tempdir().unwrap();
        std::fs::write(
            root.path().join("photo.png"),
            image_bytes(image::ImageFormat::Png),
        )
        .unwrap();
        let blobs = Arc::new(Blobs::default());
        let tools = super::super::BuiltinToolCatalog::new(root.path())
            .with_blob_store(blobs.clone())
            .build()
            .unwrap();
        let model = Arc::new(VisionModel {
            calls: std::sync::atomic::AtomicUsize::new(0),
            blobs,
        });
        let resources = AgentResources::new(Arc::new(MemoryFabric::default()))
            .model("primary", "v1", model.clone())
            .toolset("workspace", "v1", Arc::new(tools));
        let mut config = GraphAgentConfig::new("vision", "system");
        config.parallel_tools = true;
        if compact {
            let mut compaction =
                agent_core::agent::CompactorConfig::new("primary", 32000, 1024, 32000);
            compaction.attachment_tokens = Some(2048);
            config.compaction = Some(compaction);
        }
        let definition = config.definition().unwrap();
        let agent = AgentBuilder::new(OperatorRegistry::builtins(), resources.clone())
            .build(definition.clone())
            .unwrap();
        let run = agent.run("Read photo.png").await.unwrap();
        let finished = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let state = agent.engine.get_run("local", run.run_id).await.unwrap();
                if state.is_terminal() {
                    break state;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            finished.status,
            GraphStatus::Completed,
            "{:?}",
            finished.error
        );
        let saved = serde_json::to_value(&finished).unwrap();
        assert!(saved["states"]["context"]["messages"][5]["attachments"][0]["blob_id"].is_string());
        agent.engine.stop_workers().await;
        let recovered = AgentBuilder::new(OperatorRegistry::builtins(), resources)
            .build(definition)
            .unwrap();
        recovered.engine.recover("local", run.run_id).await.unwrap();
        assert_eq!(model.calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        recovered.engine.stop_workers().await;
    }
}
