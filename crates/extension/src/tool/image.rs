//! OpenAI-compatible Images API exposed as a host-configured tool.
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use agent_core::contracts::{
    BlobStore, PutBlob, Tool, ToolCallFuture, ToolCallRequest, ToolDefinition, ToolError,
    ToolOutput,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::Deserialize;
use serde_json::json;

use crate::provider::{ModelConfig, ProviderConfig};

const MAX_RESPONSE: usize = 24 * 1024 * 1024;
const MAX_IMAGE: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImageGenerationConfig {
    pub model: String,
    pub provider: ProviderConfig,
    #[serde(default = "default_timeout")]
    pub timeout_ms: u64,
}
fn default_timeout() -> u64 {
    180_000
}

/// The host owns credentials and model selection. Calls are not automatically retried:
/// a lost response may still represent a billed, successful generation upstream.
#[derive(Clone)]
pub struct GenerateImageTool {
    client: reqwest::Client,
    endpoint: url::Url,
    headers: reqwest::header::HeaderMap,
    model: String,
    timeout: Duration,
    blobs: Arc<dyn BlobStore>,
    public_path: String,
}
impl GenerateImageTool {
    pub fn new(
        config: ImageGenerationConfig,
        blobs: Arc<dyn BlobStore>,
        public_path: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        if config.timeout_ms == 0 || config.timeout_ms > 240_000 {
            return Err("image timeout_ms must be between 1 and 240000".into());
        }
        if !public_path.starts_with('/')
            || public_path.starts_with("//")
            || public_path.contains(['?', '#'])
        {
            return Err("image public path must be a local URL path".into());
        }
        let mut model = ModelConfig {
            model: config.model,
            modalities: Default::default(),
            context_window: None,
            max_output_tokens: None,
            provider: config.provider,
        };
        model.normalize_and_validate("image_generation")?;
        let ProviderConfig::OpenAiCompatible {
            base_url,
            api_key,
            organization,
            project,
            ..
        } = model.provider
        else {
            return Err("image generation requires an openai-compatible provider".into());
        };
        let mut headers = reqwest::header::HeaderMap::new();
        if let Some(key) = api_key {
            let key = key.resolve()?;
            let mut value =
                reqwest::header::HeaderValue::from_str(&format!("Bearer {}", key.expose_secret()))
                    .map_err(|_| "invalid image API authorization header")?;
            value.set_sensitive(true);
            headers.insert(reqwest::header::AUTHORIZATION, value);
        }
        for (name, value) in [
            ("openai-organization", organization),
            ("openai-project", project),
        ] {
            if let Some(value) = value {
                headers.insert(
                    name,
                    value
                        .parse()
                        .map_err(|_| "invalid image API routing header")?,
                );
            }
        }
        Ok(Self {
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            endpoint: base_url.join("images/generations")?,
            headers,
            model: model.model,
            timeout: Duration::from_millis(config.timeout_ms),
            blobs,
            public_path: public_path.trim_end_matches('/').to_owned(),
        })
    }
    async fn generate(&self, request: &ToolCallRequest) -> Result<ToolOutput, ToolError> {
        let args: Arguments = serde_json::from_value(request.arguments.clone())
            .map_err(|_| failure("invalid_tool_arguments", "Expected only a non-empty prompt"))?;
        if args.prompt.trim().is_empty() || args.prompt.chars().count() > 32000 {
            return Err(failure(
                "invalid_tool_arguments",
                "Prompt must contain 1 to 32000 characters",
            ));
        }
        let mut response = self.client.post(self.endpoint.clone()).headers(self.headers.clone())
            .json(&json!({"model":self.model,"prompt":args.prompt,"n":1,"size":"auto","quality":"auto","output_format":"png"}))
            .send().await.map_err(|_| failure("image_transport", "Image service request failed"))?;
        if !response.status().is_success() {
            return Err(failure(
                "image_provider",
                &format!("Image service returned HTTP {}", response.status().as_u16()),
            ));
        }
        if response
            .content_length()
            .is_some_and(|n| n > MAX_RESPONSE as u64)
        {
            return Err(failure(
                "image_response_limit",
                "Image response exceeds the size limit",
            ));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| failure("image_transport", "Image response was interrupted"))?
        {
            if bytes.len().saturating_add(chunk.len()) > MAX_RESPONSE {
                return Err(failure(
                    "image_response_limit",
                    "Image response exceeds the size limit",
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        let result: ImageResponse = serde_json::from_slice(&bytes)
            .map_err(|_| failure("image_response", "Invalid image service response"))?;
        let encoded = result
            .data
            .into_iter()
            .next()
            .ok_or_else(|| failure("image_response", "Image service returned no image"))?
            .b64_json;
        let data = STANDARD
            .decode(encoded)
            .map_err(|_| failure("image_response", "Invalid image encoding"))?;
        if data.len() > MAX_IMAGE || !data.starts_with(b"\x89PNG\r\n\x1a\n") {
            return Err(failure(
                "image_response",
                "Expected a PNG image within the size limit",
            ));
        }
        let metadata = self
            .blobs
            .put(PutBlob {
                media_type: "image/png".into(),
                name: Some("generated.png".into()),
                data,
                created_at_ms: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64,
            })
            .await
            .map_err(|_| failure("image_storage", "Could not save generated image"))?;
        Ok(ToolOutput::text(json!({"blob_id":metadata.blob_id,"url":format!("{}/{}",self.public_path, metadata.blob_id),"media_type":"image/png","size_bytes":metadata.size_bytes}).to_string()))
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Arguments {
    prompt: String,
}
#[derive(Deserialize)]
struct ImageResponse {
    data: Vec<ImageData>,
}
#[derive(Deserialize)]
struct ImageData {
    b64_json: String,
}
fn failure(code: &str, message: &str) -> ToolError {
    ToolError::new(code, message, false)
}
impl Tool for GenerateImageTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition::new(
            "generate_image",
            "Generate an image from a detailed prompt. Returns a saved PNG URL; show it to the user using Markdown ![description](url).",
            json!({"type":"object","properties":{"prompt":{"type":"string","minLength":1,"maxLength":32000}},"required":["prompt"],"additionalProperties":false}),
        )
    }
    fn call(&self, request: ToolCallRequest) -> ToolCallFuture {
        let tool = self.clone();
        Box::pin(async move {
            tokio::select! {
                biased;
                () = request.cancellation.cancelled() => Err(failure("tool_cancelled", "Image generation cancelled")),
                result = tokio::time::timeout(tool.timeout, tool.generate(&request)) => result.unwrap_or_else(|_| Err(failure("image_timeout", "Image generation timed out"))),
            }
        })
    }
}
