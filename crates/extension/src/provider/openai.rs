//! OpenAI-compatible Chat Completions streaming adapter.
//!
//! Provider SSE, JSON, HTTP headers, credentials, and transport errors terminate
//! in this crate. The Agent only sees `agent_core::harness::ModelEvent` values.

use std::{collections::BTreeMap, sync::Arc};

use agent_core::context::{TokenEstimateRequest, TokenEstimator, TokenSegment};
use agent_core::harness::{
    BlobStore, FinishReason, ModelError, ModelErrorKind, ModelEvent, ModelEventStream,
    ModelMessage, ModelPort, ModelRequest, ModelRole, TokenUsage, TokenUsageSource, ToolDefinition,
};
use agent_harness::{ConfigError, ModelConfig, OpenAiProtocol, ProviderConfig, SecretString};
use async_stream::stream;
use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;

use super::attachment::{base64_data, data_url, load_blob};

/// HTTP/SSE implementation of Mina's provider-neutral model port.
pub struct OpenAiCompatibleProvider {
    client: Client,
    base_url: Url,
    api_key: Option<SecretString>,
    organization: Option<String>,
    project: Option<String>,
    token_estimator: Option<Arc<dyn TokenEstimator>>,
    blob_store: Option<Arc<dyn BlobStore>>,
}

impl OpenAiCompatibleProvider {
    pub fn from_model_config(model: &ModelConfig) -> Result<Self, AdapterConfigError> {
        let ProviderConfig::OpenAiCompatible {
            base_url,
            protocol,
            api_key,
            organization,
            project,
        } = &model.provider
        else {
            return Err(AdapterConfigError::UnsupportedProvider);
        };
        if *protocol != OpenAiProtocol::ChatCompletions {
            return Err(AdapterConfigError::UnsupportedProtocol);
        }

        let api_key = api_key
            .as_ref()
            .map(|source| source.resolve())
            .transpose()?;
        // Streaming requests are bounded by the Harness model deadline. A
        // second reqwest total timeout would race that deadline and turn a
        // legitimate long-running stream into a generic transport failure.
        let client = Client::builder().build()?;

        Ok(Self {
            client,
            base_url: base_url.clone(),
            api_key,
            organization: organization.clone(),
            project: project.clone(),
            token_estimator: None,
            blob_store: None,
        })
    }

    #[must_use]
    pub fn with_token_estimator(mut self, estimator: Arc<dyn TokenEstimator>) -> Self {
        self.token_estimator = Some(estimator);
        self
    }

    #[must_use]
    pub fn with_blob_store(mut self, blob_store: Arc<dyn BlobStore>) -> Self {
        self.blob_store = Some(blob_store);
        self
    }

    fn endpoint(&self) -> Result<Url, ModelError> {
        self.base_url.join("chat/completions").map_err(|_| {
            ModelError::new(
                ModelErrorKind::Internal,
                "model endpoint could not be constructed",
                false,
            )
        })
    }
}

impl ModelPort for OpenAiCompatibleProvider {
    fn stream(&self, request: ModelRequest) -> ModelEventStream {
        let client = self.client.clone();
        let endpoint = self.endpoint();
        let api_key = self.api_key.clone();
        let organization = self.organization.clone();
        let project = self.project.clone();
        let token_estimator = self.token_estimator.clone();
        let blob_store = self.blob_store.clone();

        Box::pin(stream! {
            let endpoint = match endpoint {
                Ok(endpoint) => endpoint,
                Err(error) => {
                    yield ModelEvent::Failed { error };
                    return;
                }
            };

            let body = match ChatCompletionRequest::from_model_request(&request, blob_store.as_ref()).await {
                Ok(body) => body,
                Err(error) => {
                    yield ModelEvent::Failed { error };
                    return;
                }
            };
            let mut builder = client.post(endpoint).json(&body);
            if let Some(api_key) = &api_key {
                builder = builder.bearer_auth(api_key.expose_secret());
            }
            if let Some(organization) = &organization {
                builder = builder.header("OpenAI-Organization", organization);
            }
            if let Some(project) = &project {
                builder = builder.header("OpenAI-Project", project);
            }

            let response = match builder.send().await {
                Ok(response) => response,
                Err(error) => {
                    yield ModelEvent::Failed { error: map_transport_error(error) };
                    return;
                }
            };
            let status = response.status();
            if !status.is_success() {
                yield ModelEvent::Failed { error: map_http_error(status) };
                return;
            }

            yield ModelEvent::Accepted { provider_request_id: None };

            let mut source = response.bytes_stream().eventsource();
            let mut finish_reason = None;
            let mut saw_output = false;
            let mut tool_calls = BTreeMap::new();
            let mut provider_usage = None;
            let mut response_for_estimate = String::new();

            while let Some(event) = source.next().await {
                let event = match event {
                    Ok(event) => event,
                    Err(_) => {
                        yield ModelEvent::Failed {
                            error: ModelError::new(
                                ModelErrorKind::UpstreamUnavailable,
                                "model stream was interrupted",
                                !saw_output,
                            ),
                        };
                        return;
                    }
                };
                let data = event.data.trim();
                if data.is_empty() {
                    continue;
                }
                if data == "[DONE]" {
                    if let Err(error) = validate_tool_stream(&tool_calls) {
                        yield ModelEvent::Failed { error };
                        return;
                    }
                    if let Some(usage) = resolve_usage(
                        &request,
                        &response_for_estimate,
                        provider_usage.take(),
                        token_estimator.as_deref(),
                    ).await {
                        yield ModelEvent::Usage { usage };
                    }
                    yield ModelEvent::Completed {
                        finish_reason: finish_reason.unwrap_or(FinishReason::Unknown),
                    };
                    return;
                }

                let chunk: ChatCompletionChunk = match serde_json::from_str(data) {
                    Ok(chunk) => chunk,
                    Err(_) => {
                        yield ModelEvent::Failed {
                            error: ModelError::new(
                                ModelErrorKind::ProtocolViolation,
                                "model service returned an invalid streaming chunk",
                                false,
                            ),
                        };
                        return;
                    }
                };

                if let Some(usage) = chunk.usage {
                    provider_usage = Some(usage);
                }

                if let Some(choice) = chunk.choices.into_iter().next() {
                    let ChatDelta {
                        content,
                        reasoning_content,
                        reasoning,
                        tool_calls: tool_deltas,
                    } = choice.delta;
                    if let Some(reasoning) = reasoning_content.or(reasoning)
                        && !reasoning.is_empty()
                    {
                        saw_output = true;
                        response_for_estimate.push_str(&reasoning);
                        yield ModelEvent::ReasoningDelta { delta: reasoning };
                    }
                    if let Some(content) = content
                        && !content.is_empty()
                    {
                        saw_output = true;
                        response_for_estimate.push_str(&content);
                        yield ModelEvent::TextDelta { delta: content };
                    }
                    for tool_delta in tool_deltas {
                        if let Some(function) = &tool_delta.function {
                            if let Some(name) = &function.name {
                                response_for_estimate.push_str(name);
                            }
                            if let Some(arguments) = &function.arguments {
                                response_for_estimate.push_str(arguments);
                            }
                        }
                        match map_tool_delta(tool_delta, &mut tool_calls) {
                            Ok(events) => {
                                for event in events {
                                    saw_output = true;
                                    yield event;
                                }
                            }
                            Err(error) => {
                                yield ModelEvent::Failed { error };
                                return;
                            }
                        }
                    }
                    if choice.finish_reason.is_some() {
                        finish_reason = Some(map_finish_reason(choice.finish_reason.as_deref()));
                    }
                }
            }

            if let Some(finish_reason) = finish_reason {
                if let Err(error) = validate_tool_stream(&tool_calls) {
                    yield ModelEvent::Failed { error };
                    return;
                }
                // Some compatible servers close after the final finish-reason chunk
                // without sending the conventional `[DONE]` sentinel.
                if let Some(usage) = resolve_usage(
                    &request,
                    &response_for_estimate,
                    provider_usage.take(),
                    token_estimator.as_deref(),
                ).await {
                    yield ModelEvent::Usage { usage };
                }
                yield ModelEvent::Completed { finish_reason };
            } else {
                yield ModelEvent::Failed {
                    error: ModelError::new(
                        ModelErrorKind::ProtocolViolation,
                        "model stream ended without a completion marker",
                        false,
                    ),
                };
            }
        })
    }
}

#[derive(Debug, Error)]
pub enum AdapterConfigError {
    #[error("the configured provider is not OpenAI-compatible")]
    UnsupportedProvider,

    #[error("the configured OpenAI-compatible provider does not use Chat Completions")]
    UnsupportedProtocol,

    #[error(transparent)]
    Secret(#[from] ConfigError),

    #[error("failed to build HTTP client: {0}")]
    HttpClient(#[from] reqwest::Error),
}

#[derive(Serialize)]
struct ChatCompletionRequest {
    model: String,
    messages: Vec<ChatMessage>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tools: Vec<ChatToolDefinition>,
    stream: bool,
    stream_options: StreamOptions,
    /// `max_tokens` remains widely implemented by compatible servers. A future
    /// capability layer can select `max_completion_tokens` per deployment.
    #[serde(skip_serializing_if = "Option::is_none")]
    max_tokens: Option<u32>,
}

impl ChatCompletionRequest {
    async fn from_model_request(
        request: &ModelRequest,
        blob_store: Option<&Arc<dyn BlobStore>>,
    ) -> Result<Self, ModelError> {
        let mut messages = Vec::with_capacity(request.messages.len());
        for message in &request.messages {
            messages.push(ChatMessage::from_model_message(message, blob_store).await?);
        }
        Ok(Self {
            model: request.model.clone(),
            messages,
            tools: request.tools.iter().map(ChatToolDefinition::from).collect(),
            stream: true,
            stream_options: StreamOptions {
                include_usage: true,
            },
            max_tokens: request.max_output_tokens,
        })
    }
}

#[derive(Serialize)]
struct StreamOptions {
    include_usage: bool,
}

#[derive(Serialize)]
struct ChatMessage {
    role: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<ChatMessageContent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reasoning_content: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tool_calls: Vec<ChatMessageToolCall>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_call_id: Option<String>,
}

#[derive(Serialize)]
#[serde(untagged)]
enum ChatMessageContent {
    Text(String),
    Parts(Vec<serde_json::Value>),
}

impl ChatMessage {
    async fn from_model_message(
        message: &ModelMessage,
        blob_store: Option<&Arc<dyn BlobStore>>,
    ) -> Result<Self, ModelError> {
        let content = if message.attachments.is_empty() {
            if message.role == ModelRole::Assistant
                && !message.tool_calls.is_empty()
                && message.content.is_empty()
            {
                None
            } else {
                Some(ChatMessageContent::Text(message.content.clone()))
            }
        } else {
            if message.role != ModelRole::User {
                return Err(ModelError::new(
                    ModelErrorKind::InvalidRequest,
                    "binary input is only supported on user messages",
                    false,
                ));
            }
            let mut parts = Vec::with_capacity(message.attachments.len() + 1);
            if !message.content.is_empty() {
                parts.push(serde_json::json!({"type": "text", "text": message.content}));
            }
            for attachment in &message.attachments {
                let object = load_blob(blob_store, attachment).await?;
                let media_type = object.metadata.media_type.as_str();
                if media_type.starts_with("image/") {
                    parts.push(serde_json::json!({
                        "type": "image_url",
                        "image_url": {"url": data_url(media_type, &object.data)}
                    }));
                } else if media_type.starts_with("audio/") {
                    let format = match media_type {
                        "audio/wav" | "audio/x-wav" => "wav",
                        "audio/mpeg" | "audio/mp3" => "mp3",
                        _ => {
                            return Err(ModelError::new(
                                ModelErrorKind::InvalidRequest,
                                "Chat Completions only supports WAV or MP3 audio inputs",
                                false,
                            ));
                        }
                    };
                    parts.push(serde_json::json!({
                        "type": "input_audio",
                        "input_audio": {"data": base64_data(&object.data), "format": format}
                    }));
                } else if media_type.starts_with("video/") {
                    return Err(ModelError::new(
                        ModelErrorKind::InvalidRequest,
                        "Chat Completions does not define a portable video input part",
                        false,
                    ));
                } else {
                    let filename = attachment
                        .name
                        .as_ref()
                        .or(object.metadata.name.as_ref())
                        .cloned()
                        .unwrap_or_else(|| format!("{}.bin", attachment.blob_id));
                    parts.push(serde_json::json!({
                        "type": "file",
                        "file": {
                            "filename": filename,
                            "file_data": data_url(media_type, &object.data)
                        }
                    }));
                }
            }
            Some(ChatMessageContent::Parts(parts))
        };
        Ok(Self {
            role: message.role.as_str(),
            content,
            reasoning_content: (!message.reasoning.is_empty()).then(|| message.reasoning.clone()),
            tool_calls: message
                .tool_calls
                .iter()
                .map(ChatMessageToolCall::from)
                .collect(),
            tool_call_id: message.tool_call_id.clone(),
        })
    }
}

#[derive(Serialize)]
struct ChatMessageToolCall {
    id: String,
    #[serde(rename = "type")]
    kind: &'static str,
    function: ChatMessageToolFunction,
}

impl From<&agent_core::harness::ModelToolCall> for ChatMessageToolCall {
    fn from(call: &agent_core::harness::ModelToolCall) -> Self {
        Self {
            id: call.id.clone(),
            kind: "function",
            function: ChatMessageToolFunction {
                name: call.name.clone(),
                arguments: call.arguments.clone(),
            },
        }
    }
}

#[derive(Serialize)]
struct ChatMessageToolFunction {
    name: String,
    arguments: String,
}

#[derive(Serialize)]
struct ChatToolDefinition {
    #[serde(rename = "type")]
    kind: &'static str,
    function: ChatToolFunction,
}

impl From<&ToolDefinition> for ChatToolDefinition {
    fn from(tool: &ToolDefinition) -> Self {
        Self {
            kind: "function",
            function: ChatToolFunction {
                name: tool.name.clone(),
                description: tool.description.clone(),
                parameters: tool.input_schema.clone(),
            },
        }
    }
}

#[derive(Serialize)]
struct ChatToolFunction {
    name: String,
    description: String,
    parameters: serde_json::Value,
}

#[derive(Deserialize)]
struct ChatCompletionChunk {
    #[serde(default)]
    choices: Vec<ChatChunkChoice>,
    #[serde(default)]
    usage: Option<ChatUsage>,
}

#[derive(Deserialize)]
struct ChatChunkChoice {
    #[serde(default)]
    delta: ChatDelta,
    #[serde(default)]
    finish_reason: Option<String>,
}

#[derive(Default, Deserialize)]
struct ChatDelta {
    #[serde(default)]
    content: Option<String>,
    /// DeepSeek-R1 and compatible gateways expose chain-of-thought deltas here.
    #[serde(default)]
    reasoning_content: Option<String>,
    /// A few compatible gateways use the shorter field name.
    #[serde(default)]
    reasoning: Option<String>,
    #[serde(default)]
    tool_calls: Vec<ChatToolCallDelta>,
}

#[derive(Deserialize)]
struct ChatToolCallDelta {
    index: u32,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    function: Option<ChatToolFunctionDelta>,
}

#[derive(Deserialize)]
struct ChatToolFunctionDelta {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    arguments: Option<String>,
}

#[derive(Default)]
struct StreamingToolCall {
    call_id: Option<String>,
    name: Option<String>,
    buffered_arguments: String,
    started: bool,
}

fn map_tool_delta(
    delta: ChatToolCallDelta,
    calls: &mut BTreeMap<u32, StreamingToolCall>,
) -> Result<Vec<ModelEvent>, ModelError> {
    let call = calls.entry(delta.index).or_default();
    if let Some(call_id) = delta.id {
        if call.call_id.as_ref().is_some_and(|known| known != &call_id) {
            return Err(tool_protocol_error(
                "model changed a streaming tool call id",
            ));
        }
        call.call_id = Some(call_id);
    }
    if let Some(function) = delta.function {
        if let Some(name) = function.name {
            if call.name.as_ref().is_some_and(|known| known != &name) {
                return Err(tool_protocol_error("model changed a streaming tool name"));
            }
            call.name = Some(name);
        }
        if let Some(arguments) = function.arguments {
            call.buffered_arguments.push_str(&arguments);
        }
    }

    let mut events = Vec::new();
    if !call.started
        && let (Some(call_id), Some(name)) = (&call.call_id, &call.name)
    {
        if call_id.is_empty() || name.is_empty() {
            return Err(tool_protocol_error(
                "model emitted an empty tool id or name",
            ));
        }
        call.started = true;
        events.push(ModelEvent::ToolCallStarted {
            call_id: call_id.clone(),
            name: name.clone(),
        });
    }
    if call.started && !call.buffered_arguments.is_empty() {
        let Some(call_id) = &call.call_id else {
            return Err(tool_protocol_error("started tool call is missing its id"));
        };
        events.push(ModelEvent::ToolCallArgumentsDelta {
            call_id: call_id.clone(),
            delta: std::mem::take(&mut call.buffered_arguments),
        });
    }
    Ok(events)
}

fn validate_tool_stream(calls: &BTreeMap<u32, StreamingToolCall>) -> Result<(), ModelError> {
    if calls.values().any(|call| !call.started) {
        return Err(tool_protocol_error(
            "model stream ended with an incomplete tool call",
        ));
    }
    Ok(())
}

fn tool_protocol_error(message: &str) -> ModelError {
    ModelError::new(ModelErrorKind::ProtocolViolation, message, false)
}

#[derive(Deserialize)]
struct ChatUsage {
    #[serde(default)]
    prompt_tokens: Option<u64>,
    #[serde(default)]
    completion_tokens: Option<u64>,
    #[serde(default)]
    total_tokens: Option<u64>,
}

async fn resolve_usage(
    request: &ModelRequest,
    response: &str,
    provider: Option<ChatUsage>,
    estimator: Option<&dyn TokenEstimator>,
) -> Option<TokenUsage> {
    if let Some(provider) = &provider
        && let (Some(input), Some(output), Some(total)) = (
            provider.prompt_tokens,
            provider.completion_tokens,
            provider.total_tokens,
        )
        && total >= input.saturating_add(output)
    {
        return Some(TokenUsage {
            input_tokens: input,
            output_tokens: output,
            total_tokens: total,
            source: TokenUsageSource::ProviderReported,
        });
    }

    let estimator = estimator?;
    let input_payload = model_request_for_estimate(request);
    let input = estimator
        .estimate(TokenEstimateRequest {
            model_profile: request.model.clone(),
            segments: vec![TokenSegment {
                identity: "model-request".into(),
                content: input_payload,
            }],
        })
        .await
        .ok()?
        .tokens;
    let output = estimator
        .estimate(TokenEstimateRequest {
            model_profile: request.model.clone(),
            segments: vec![TokenSegment {
                identity: "model-response".into(),
                content: response.into(),
            }],
        })
        .await
        .ok()?
        .tokens;

    let provider_input = provider.as_ref().and_then(|usage| usage.prompt_tokens);
    let provider_output = provider.as_ref().and_then(|usage| usage.completion_tokens);
    let input_tokens = provider_input.unwrap_or(input);
    let output_tokens = provider_output.unwrap_or(output);
    let minimum_total = input_tokens.saturating_add(output_tokens);
    let provider_total = provider
        .as_ref()
        .and_then(|usage| usage.total_tokens)
        .filter(|total| *total >= minimum_total);
    Some(TokenUsage {
        input_tokens,
        output_tokens,
        total_tokens: provider_total.unwrap_or(minimum_total),
        source: if provider_input.is_none() && provider_output.is_none() && provider_total.is_none()
        {
            TokenUsageSource::EstimatorFallback
        } else {
            TokenUsageSource::Mixed
        },
    })
}

fn model_request_for_estimate(request: &ModelRequest) -> String {
    let mut parts = Vec::new();
    for message in &request.messages {
        parts.push(message.role.as_str().to_owned());
        parts.push(message.content.clone());
        parts.push(message.reasoning.clone());
        for call in &message.tool_calls {
            parts.push(call.name.clone());
            parts.push(call.arguments.clone());
        }
    }
    for tool in &request.tools {
        parts.push(tool.name.clone());
        parts.push(tool.description.clone());
        parts.push(tool.input_schema.to_string());
    }
    parts.join("\n")
}

fn map_finish_reason(reason: Option<&str>) -> FinishReason {
    match reason {
        Some("stop") => FinishReason::Stop,
        Some("length") => FinishReason::Length,
        Some("tool_calls" | "function_call") => FinishReason::ToolCall,
        Some("content_filter") => FinishReason::ContentFilter,
        _ => FinishReason::Unknown,
    }
}

fn map_transport_error(error: reqwest::Error) -> ModelError {
    if error.is_timeout() {
        ModelError::new(ModelErrorKind::Timeout, "model request timed out", true)
    } else {
        ModelError::new(
            ModelErrorKind::UpstreamUnavailable,
            "model service is unavailable",
            true,
        )
    }
}

fn map_http_error(status: StatusCode) -> ModelError {
    let (kind, message, retryable) = match status {
        StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY => (
            ModelErrorKind::InvalidRequest,
            "model service rejected the request",
            false,
        ),
        StatusCode::UNAUTHORIZED => (
            ModelErrorKind::Authentication,
            "model service rejected the configured API key",
            false,
        ),
        StatusCode::FORBIDDEN => (
            ModelErrorKind::PermissionDenied,
            "model service denied this request",
            false,
        ),
        StatusCode::TOO_MANY_REQUESTS => (
            ModelErrorKind::RateLimited,
            "model service rate limit was reached",
            true,
        ),
        status if status.is_server_error() => (
            ModelErrorKind::UpstreamUnavailable,
            "model service is temporarily unavailable",
            true,
        ),
        _ => (
            ModelErrorKind::ProtocolViolation,
            "model service returned an unexpected status",
            false,
        ),
    };

    ModelError::new(kind, message, retryable)
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use super::*;
    use agent_core::harness::{
        BlobId, BlobMetadata, BlobObject, BlobStoreError, BlobStoreFuture, ModelAttachment,
        PutBlob, RunId,
    };
    use agent_harness::HarnessConfig;
    use axum::{
        Json, Router,
        http::HeaderMap,
        response::sse::{Event, Sse},
        routing::post,
    };
    use futures_util::stream;
    use tokio::net::TcpListener;

    struct FixedEstimator;

    struct FixedBlobStore(BlobObject);

    impl BlobStore for FixedBlobStore {
        fn put(&self, _command: PutBlob) -> BlobStoreFuture<'_, BlobMetadata> {
            Box::pin(async { Err(BlobStoreError::backend("read only")) })
        }

        fn get(&self, blob_id: BlobId) -> BlobStoreFuture<'_, Option<BlobObject>> {
            Box::pin(
                async move { Ok((self.0.metadata.blob_id == blob_id).then(|| self.0.clone())) },
            )
        }
    }

    impl TokenEstimator for FixedEstimator {
        fn descriptor(&self) -> agent_core::context::ContextComponentDescriptor {
            agent_core::context::ContextComponentDescriptor {
                identity: "test-estimator".into(),
                kind: "fixed".into(),
                version: "1".into(),
            }
        }

        fn estimate(
            &self,
            request: TokenEstimateRequest,
        ) -> agent_core::context::ContextFuture<'_, agent_core::context::TokenEstimate> {
            Box::pin(async move {
                let tokens = if request.segments[0].identity == "model-request" {
                    11
                } else {
                    7
                };
                Ok(agent_core::context::TokenEstimate {
                    tokens,
                    per_segment: vec![tokens],
                    confidence: 0.5,
                    estimator: self.descriptor(),
                })
            })
        }
    }

    #[tokio::test]
    async fn maps_normalized_request_to_streaming_chat_json() {
        let request = ModelRequest {
            run_id: RunId::new(),
            model: "example-model".into(),
            messages: vec![
                ModelMessage::system("Be concise."),
                ModelMessage::user("Hello"),
                ModelMessage::assistant_tool_calls(
                    "",
                    "considering the clock",
                    vec![agent_core::harness::ModelToolCall {
                        id: "call_1".into(),
                        name: "get_current_time".into(),
                        arguments: "{}".into(),
                    }],
                ),
                ModelMessage::tool_result("call_1", r#"{"utc":"now"}"#),
            ],
            tools: vec![ToolDefinition::new(
                "get_current_time",
                "Return the current UTC time.",
                serde_json::json!({"type": "object", "properties": {}}),
            )],
            max_output_tokens: Some(512),
        };

        let body = ChatCompletionRequest::from_model_request(&request, None)
            .await
            .expect("request should map");
        let value = serde_json::to_value(body).expect("request should serialize");

        assert_eq!(value["model"], "example-model");
        assert_eq!(value["messages"][0]["role"], "system");
        assert_eq!(value["messages"][1]["content"], "Hello");
        assert_eq!(value["messages"][2]["role"], "assistant");
        assert_eq!(
            value["messages"][2]["reasoning_content"],
            "considering the clock"
        );
        assert_eq!(value["messages"][2]["tool_calls"][0]["id"], "call_1");
        assert_eq!(value["messages"][3]["role"], "tool");
        assert_eq!(value["messages"][3]["tool_call_id"], "call_1");
        assert_eq!(value["max_tokens"], 512);
        assert_eq!(value["stream"], true);
        assert_eq!(value["stream_options"]["include_usage"], true);
        assert_eq!(value["tools"][0]["type"], "function");
        assert_eq!(value["tools"][0]["function"]["name"], "get_current_time");
    }

    #[tokio::test]
    async fn maps_blob_references_to_multimodal_content_parts() {
        let blob_id = BlobId::new();
        let store: Arc<dyn BlobStore> = Arc::new(FixedBlobStore(BlobObject {
            metadata: BlobMetadata {
                blob_id,
                media_type: "image/png".into(),
                name: Some("screen.png".into()),
                size_bytes: 3,
                created_at_ms: 1,
            },
            data: vec![1, 2, 3],
        }));
        let request = ModelRequest {
            run_id: RunId::new(),
            model: "vision-model".into(),
            messages: vec![ModelMessage::user_with_attachments(
                "describe this",
                vec![ModelAttachment {
                    blob_id,
                    media_type: "image/png".into(),
                    name: Some("screen.png".into()),
                }],
            )],
            tools: Vec::new(),
            max_output_tokens: None,
        };
        let body = ChatCompletionRequest::from_model_request(&request, Some(&store))
            .await
            .expect("multimodal request should map");
        let value = serde_json::to_value(body).expect("request should serialize");
        assert_eq!(value["messages"][0]["content"][0]["type"], "text");
        assert_eq!(value["messages"][0]["content"][1]["type"], "image_url");
        assert!(
            value["messages"][0]["content"][1]["image_url"]["url"]
                .as_str()
                .is_some_and(|url| url.starts_with("data:image/png;base64,"))
        );
    }

    #[tokio::test]
    async fn prefers_complete_provider_usage_and_estimates_missing_fields() {
        let request = ModelRequest {
            run_id: RunId::new(),
            model: "test-model".into(),
            messages: vec![ModelMessage::user("hello")],
            tools: Vec::new(),
            max_output_tokens: None,
        };
        let provider = resolve_usage(
            &request,
            "answer",
            Some(ChatUsage {
                prompt_tokens: Some(3),
                completion_tokens: Some(2),
                total_tokens: Some(5),
            }),
            Some(&FixedEstimator),
        )
        .await
        .expect("provider usage should resolve");
        assert_eq!(provider.input_tokens, 3);
        assert_eq!(provider.output_tokens, 2);
        assert_eq!(provider.source, TokenUsageSource::ProviderReported);

        let estimated = resolve_usage(&request, "answer", None, Some(&FixedEstimator))
            .await
            .expect("missing usage should be estimated");
        assert_eq!(estimated.input_tokens, 11);
        assert_eq!(estimated.output_tokens, 7);
        assert_eq!(estimated.total_tokens, 18);
        assert_eq!(estimated.source, TokenUsageSource::EstimatorFallback);

        let mixed = resolve_usage(
            &request,
            "answer",
            Some(ChatUsage {
                prompt_tokens: Some(3),
                completion_tokens: None,
                total_tokens: None,
            }),
            Some(&FixedEstimator),
        )
        .await
        .expect("partial usage should be completed");
        assert_eq!(mixed.input_tokens, 3);
        assert_eq!(mixed.output_tokens, 7);
        assert_eq!(mixed.source, TokenUsageSource::Mixed);
    }

    #[test]
    fn maps_authentication_errors_without_exposing_response_bodies() {
        let error = map_http_error(StatusCode::UNAUTHORIZED);

        assert_eq!(error.kind(), ModelErrorKind::Authentication);
        assert!(!error.retryable());
        assert!(!error.safe_message().contains("sk-"));
    }

    #[tokio::test]
    async fn streams_an_openai_compatible_chat_completion() {
        async fn completion(
            headers: HeaderMap,
            Json(body): Json<serde_json::Value>,
        ) -> Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>> {
            assert_eq!(
                headers
                    .get("authorization")
                    .and_then(|value| value.to_str().ok()),
                Some("Bearer test-key")
            );
            assert_eq!(body["model"], "mock-model");
            assert_eq!(body["messages"][0]["content"], "ping");
            assert_eq!(body["stream"], true);

            Sse::new(stream::iter(vec![
                Ok(Event::default().data(
                    r#"{"id":"chatcmpl-test","choices":[{"delta":{"reasoning_content":"think"},"finish_reason":null}]}"#,
                )),
                Ok(Event::default().data(
                    r#"{"id":"chatcmpl-test","choices":[{"delta":{"content":"po"},"finish_reason":null}]}"#,
                )),
                Ok(Event::default().data(
                    r#"{"id":"chatcmpl-test","choices":[{"delta":{"content":"ng"},"finish_reason":null}]}"#,
                )),
                Ok(Event::default().data(
                    r#"{"id":"chatcmpl-test","choices":[{"delta":{},"finish_reason":"stop"}]}"#,
                )),
                Ok(Event::default().data(
                    r#"{"id":"chatcmpl-test","choices":[],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}"#,
                )),
                Ok(Event::default().data("[DONE]")),
            ]))
        }

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test listener should bind");
        let address = listener
            .local_addr()
            .expect("test listener should have an address");
        let app = Router::new().route("/v1/chat/completions", post(completion));
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("mock server should run");
        });

        let config = HarnessConfig::from_toml_str(&format!(
            r#"
default_model = "primary"

[models.primary]
model = "mock-model"

[models.primary.provider]
type = "openai-compatible"
base_url = "http://{address}/v1"
api_key = "test-key"
"#
        ))
        .expect("test config should parse");
        let provider = OpenAiCompatibleProvider::from_model_config(config.default_model())
            .expect("provider should build");

        let events: Vec<_> = provider
            .stream(ModelRequest {
                run_id: RunId::new(),
                model: "mock-model".into(),
                messages: vec![ModelMessage::user("ping")],
                tools: Vec::new(),
                max_output_tokens: None,
            })
            .collect()
            .await;

        assert!(matches!(events[0], ModelEvent::Accepted { .. }));
        assert!(matches!(
            &events[1],
            ModelEvent::ReasoningDelta { delta } if delta == "think"
        ));
        assert!(matches!(
            &events[2],
            ModelEvent::TextDelta { delta } if delta == "po"
        ));
        assert!(matches!(
            &events[3],
            ModelEvent::TextDelta { delta } if delta == "ng"
        ));
        assert!(matches!(
            events[4],
            ModelEvent::Usage {
                usage: TokenUsage {
                    total_tokens: 2,
                    ..
                }
            }
        ));
        assert!(matches!(
            events[5],
            ModelEvent::Completed {
                finish_reason: FinishReason::Stop
            }
        ));
        server.abort();
    }

    #[test]
    fn normalizes_streaming_tool_call_fragments() {
        let mut calls = BTreeMap::new();
        let first = map_tool_delta(
            ChatToolCallDelta {
                index: 0,
                id: Some("call_1".into()),
                function: Some(ChatToolFunctionDelta {
                    name: Some("get_current_time".into()),
                    arguments: Some("{".into()),
                }),
            },
            &mut calls,
        )
        .expect("first tool delta should map");
        let second = map_tool_delta(
            ChatToolCallDelta {
                index: 0,
                id: None,
                function: Some(ChatToolFunctionDelta {
                    name: None,
                    arguments: Some("}".into()),
                }),
            },
            &mut calls,
        )
        .expect("second tool delta should map");

        assert!(matches!(
            &first[0],
            ModelEvent::ToolCallStarted { call_id, name }
                if call_id == "call_1" && name == "get_current_time"
        ));
        assert!(matches!(
            &first[1],
            ModelEvent::ToolCallArgumentsDelta { delta, .. } if delta == "{"
        ));
        assert!(matches!(
            &second[0],
            ModelEvent::ToolCallArgumentsDelta { delta, .. } if delta == "}"
        ));
        validate_tool_stream(&calls).expect("tool stream should be complete");
    }
}
