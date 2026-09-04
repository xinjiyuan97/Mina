//! Anthropic Messages API adapter.

use std::{collections::HashMap, sync::Arc};

use agent_core::{
    context::TokenEstimator,
    harness::{
        BlobStore, FinishReason, ModelError, ModelErrorKind, ModelEvent, ModelEventStream,
        ModelPort, ModelRequest, ModelRole, TokenUsage, TokenUsageSource,
    },
};
use agent_harness::{ModelConfig, ProviderConfig, SecretString};
use async_stream::stream;
use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use reqwest::{Client, StatusCode};
use serde::Serialize;
use serde_json::{Value, json};
use url::Url;

use super::{
    attachment::{base64_data, load_blob},
    openai::AdapterConfigError,
};

pub struct AnthropicMessagesProvider {
    client: Client,
    base_url: Url,
    api_key: SecretString,
    version: String,
    blob_store: Option<Arc<dyn BlobStore>>,
    #[allow(dead_code)]
    token_estimator: Option<Arc<dyn TokenEstimator>>,
}

impl AnthropicMessagesProvider {
    pub fn from_model_config(model: &ModelConfig) -> Result<Self, AdapterConfigError> {
        let ProviderConfig::Anthropic {
            base_url,
            api_key,
            version,
        } = &model.provider
        else {
            return Err(AdapterConfigError::UnsupportedProvider);
        };
        Ok(Self {
            client: Client::builder().build()?,
            base_url: base_url.clone(),
            api_key: api_key.resolve()?,
            version: version.clone(),
            blob_store: None,
            token_estimator: None,
        })
    }

    #[must_use]
    pub fn with_blob_store(mut self, blob_store: Arc<dyn BlobStore>) -> Self {
        self.blob_store = Some(blob_store);
        self
    }

    #[must_use]
    pub fn with_token_estimator(mut self, estimator: Arc<dyn TokenEstimator>) -> Self {
        self.token_estimator = Some(estimator);
        self
    }

    fn endpoint(&self) -> Result<Url, ModelError> {
        self.base_url.join("messages").map_err(|_| {
            ModelError::new(
                ModelErrorKind::Internal,
                "model endpoint could not be constructed",
                false,
            )
        })
    }
}

impl ModelPort for AnthropicMessagesProvider {
    fn stream(&self, request: ModelRequest) -> ModelEventStream {
        let client = self.client.clone();
        let endpoint = self.endpoint();
        let api_key = self.api_key.clone();
        let version = self.version.clone();
        let blob_store = self.blob_store.clone();

        Box::pin(stream! {
            let endpoint = match endpoint {
                Ok(endpoint) => endpoint,
                Err(error) => {
                    yield ModelEvent::Failed { error };
                    return;
                }
            };
            let body = match anthropic_request(&request, blob_store.as_ref()).await {
                Ok(body) => body,
                Err(error) => {
                    yield ModelEvent::Failed { error };
                    return;
                }
            };
            let response = match client
                .post(endpoint)
                .header("x-api-key", api_key.expose_secret())
                .header("anthropic-version", version)
                .json(&body)
                .send()
                .await
            {
                Ok(response) => response,
                Err(error) => {
                    yield ModelEvent::Failed { error: map_transport_error(error) };
                    return;
                }
            };
            if !response.status().is_success() {
                yield ModelEvent::Failed { error: map_http_error(response.status()) };
                return;
            }

            let mut source = response.bytes_stream().eventsource();
            let mut accepted = false;
            let mut saw_output = false;
            let mut input_tokens = None;
            let mut output_tokens = None;
            let mut finish_reason = FinishReason::Unknown;
            let mut tool_calls = HashMap::<u64, String>::new();
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
                let value: Value = match serde_json::from_str(data) {
                    Ok(value) => value,
                    Err(_) => {
                        yield ModelEvent::Failed { error: protocol_error("Messages API returned an invalid event") };
                        return;
                    }
                };
                let event_type = value
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or(event.event.as_str());
                match event_type {
                    "message_start" => {
                        if !accepted {
                            accepted = true;
                            input_tokens = value.pointer("/message/usage/input_tokens").and_then(Value::as_u64);
                            yield ModelEvent::Accepted {
                                provider_request_id: value.pointer("/message/id").and_then(Value::as_str).map(str::to_owned),
                            };
                        }
                    }
                    "content_block_start" => {
                        let index = value.get("index").and_then(Value::as_u64).unwrap_or(0);
                        match value.pointer("/content_block/type").and_then(Value::as_str) {
                            Some("tool_use") => {
                                let call_id = value.pointer("/content_block/id").and_then(Value::as_str);
                                let name = value.pointer("/content_block/name").and_then(Value::as_str);
                                let (Some(call_id), Some(name)) = (call_id, name) else {
                                    yield ModelEvent::Failed { error: protocol_error("Messages API emitted an incomplete tool use block") };
                                    return;
                                };
                                tool_calls.insert(index, call_id.into());
                                saw_output = true;
                                yield ModelEvent::ToolCallStarted { call_id: call_id.into(), name: name.into() };
                            }
                            Some("text") => {
                                if let Some(text) = value.pointer("/content_block/text").and_then(Value::as_str)
                                    && !text.is_empty()
                                {
                                    saw_output = true;
                                    yield ModelEvent::TextDelta { delta: text.into() };
                                }
                            }
                            Some("thinking") => {
                                if let Some(thinking) = value.pointer("/content_block/thinking").and_then(Value::as_str)
                                    && !thinking.is_empty()
                                {
                                    saw_output = true;
                                    yield ModelEvent::ReasoningDelta { delta: thinking.into() };
                                }
                            }
                            _ => {}
                        }
                    }
                    "content_block_delta" => {
                        let index = value.get("index").and_then(Value::as_u64).unwrap_or(0);
                        match value.pointer("/delta/type").and_then(Value::as_str) {
                            Some("text_delta") => {
                                if let Some(delta) = value.pointer("/delta/text").and_then(Value::as_str)
                                    && !delta.is_empty()
                                {
                                    saw_output = true;
                                    yield ModelEvent::TextDelta { delta: delta.into() };
                                }
                            }
                            Some("thinking_delta") => {
                                if let Some(delta) = value.pointer("/delta/thinking").and_then(Value::as_str)
                                    && !delta.is_empty()
                                {
                                    saw_output = true;
                                    yield ModelEvent::ReasoningDelta { delta: delta.into() };
                                }
                            }
                            Some("input_json_delta") => {
                                let Some(call_id) = tool_calls.get(&index).cloned() else {
                                    yield ModelEvent::Failed { error: protocol_error("Messages API emitted tool arguments before tool use") };
                                    return;
                                };
                                if let Some(delta) = value.pointer("/delta/partial_json").and_then(Value::as_str)
                                    && !delta.is_empty()
                                {
                                    saw_output = true;
                                    yield ModelEvent::ToolCallArgumentsDelta { call_id, delta: delta.into() };
                                }
                            }
                            _ => {}
                        }
                    }
                    "message_delta" => {
                        output_tokens = value.pointer("/usage/output_tokens").and_then(Value::as_u64).or(output_tokens);
                        finish_reason = map_stop_reason(value.pointer("/delta/stop_reason").and_then(Value::as_str));
                    }
                    "message_stop" => {
                        if let (Some(input_tokens), Some(output_tokens)) = (input_tokens, output_tokens) {
                            yield ModelEvent::Usage {
                                usage: TokenUsage {
                                    input_tokens,
                                    output_tokens,
                                    total_tokens: input_tokens.saturating_add(output_tokens),
                                    source: TokenUsageSource::ProviderReported,
                                },
                            };
                        }
                        yield ModelEvent::Completed { finish_reason };
                        return;
                    }
                    "error" => {
                        yield ModelEvent::Failed {
                            error: ModelError::new(
                                ModelErrorKind::UpstreamUnavailable,
                                "Messages API reported a failed response",
                                false,
                            ),
                        };
                        return;
                    }
                    _ => {}
                }
            }
            yield ModelEvent::Failed {
                error: protocol_error("Messages API stream ended without a terminal event"),
            };
        })
    }
}

#[derive(Serialize)]
struct AnthropicRequest {
    model: String,
    system: String,
    messages: Vec<AnthropicMessage>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tools: Vec<Value>,
    max_tokens: u32,
    stream: bool,
}

#[derive(Serialize)]
struct AnthropicMessage {
    role: &'static str,
    content: Vec<Value>,
}

async fn anthropic_request(
    request: &ModelRequest,
    blob_store: Option<&Arc<dyn BlobStore>>,
) -> Result<AnthropicRequest, ModelError> {
    let mut system = Vec::new();
    let mut messages: Vec<AnthropicMessage> = Vec::new();
    for message in &request.messages {
        if message.role == ModelRole::System {
            if !message.content.is_empty() {
                system.push(message.content.clone());
            }
            continue;
        }
        let (role, mut blocks) = match message.role {
            ModelRole::User => {
                let mut blocks = Vec::new();
                if !message.content.is_empty() {
                    blocks.push(json!({"type": "text", "text": message.content}));
                }
                for attachment in &message.attachments {
                    let object = load_blob(blob_store, attachment).await?;
                    let source = json!({
                        "type": "base64",
                        "media_type": object.metadata.media_type,
                        "data": base64_data(&object.data)
                    });
                    if object.metadata.media_type.starts_with("image/") {
                        blocks.push(json!({"type": "image", "source": source}));
                    } else if object.metadata.media_type == "application/pdf" {
                        blocks.push(json!({
                            "type": "document",
                            "source": source,
                            "title": attachment.name.as_ref().or(object.metadata.name.as_ref())
                        }));
                    } else {
                        return Err(ModelError::new(
                            ModelErrorKind::InvalidRequest,
                            "Messages API supports image and PDF binary inputs",
                            false,
                        ));
                    }
                }
                ("user", blocks)
            }
            ModelRole::Assistant => {
                let mut blocks = Vec::new();
                if !message.content.is_empty() {
                    blocks.push(json!({"type": "text", "text": message.content}));
                }
                for call in &message.tool_calls {
                    let input = serde_json::from_str::<Value>(&call.arguments).map_err(|_| {
                        ModelError::new(
                            ModelErrorKind::InvalidRequest,
                            "stored tool arguments are not valid JSON",
                            false,
                        )
                    })?;
                    blocks.push(json!({"type": "tool_use", "id": call.id, "name": call.name, "input": input}));
                }
                ("assistant", blocks)
            }
            ModelRole::Tool => {
                let Some(call_id) = &message.tool_call_id else {
                    return Err(protocol_error("tool result is missing its call id"));
                };
                (
                    "user",
                    vec![
                        json!({"type": "tool_result", "tool_use_id": call_id, "content": message.content}),
                    ],
                )
            }
            ModelRole::System => unreachable!(),
            _ => {
                return Err(ModelError::new(
                    ModelErrorKind::InvalidRequest,
                    "Messages API does not support this message role",
                    false,
                ));
            }
        };
        if blocks.is_empty() {
            continue;
        }
        if let Some(previous) = messages.last_mut()
            && previous.role == role
        {
            previous.content.append(&mut blocks);
        } else {
            messages.push(AnthropicMessage {
                role,
                content: blocks,
            });
        }
    }
    let tools = request
        .tools
        .iter()
        .map(|tool| {
            json!({
                "name": tool.name,
                "description": tool.description,
                "input_schema": tool.input_schema
            })
        })
        .collect();
    Ok(AnthropicRequest {
        model: request.model.clone(),
        system: system.join("\n\n"),
        messages,
        tools,
        max_tokens: request.max_output_tokens.unwrap_or(4_096),
        stream: true,
    })
}

fn map_stop_reason(reason: Option<&str>) -> FinishReason {
    match reason {
        Some("end_turn" | "stop_sequence") => FinishReason::Stop,
        Some("max_tokens") => FinishReason::Length,
        Some("tool_use") => FinishReason::ToolCall,
        _ => FinishReason::Unknown,
    }
}

fn protocol_error(message: &str) -> ModelError {
    ModelError::new(ModelErrorKind::ProtocolViolation, message, false)
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

    use agent_core::harness::{ModelMessage, RunId};
    use agent_harness::HarnessConfig;
    use axum::{
        Json, Router,
        http::HeaderMap,
        response::sse::{Event, Sse},
        routing::post,
    };
    use futures_util::{StreamExt, stream};
    use tokio::net::TcpListener;

    use super::*;

    #[tokio::test]
    async fn maps_tool_history_to_messages_blocks() {
        let request = ModelRequest {
            run_id: RunId::new(),
            model: "claude-test".into(),
            messages: vec![
                ModelMessage::system("Be concise."),
                ModelMessage::user("hello"),
                ModelMessage::assistant_tool_calls(
                    "",
                    "",
                    vec![agent_core::harness::ModelToolCall {
                        id: "toolu_1".into(),
                        name: "lookup".into(),
                        arguments: "{\"q\":\"x\"}".into(),
                    }],
                ),
                ModelMessage::tool_result("toolu_1", "result"),
            ],
            tools: Vec::new(),
            max_output_tokens: Some(512),
        };
        let value = serde_json::to_value(
            anthropic_request(&request, None)
                .await
                .expect("request should map"),
        )
        .expect("request should serialize");
        assert_eq!(value["system"], "Be concise.");
        assert_eq!(value["messages"][0]["role"], "user");
        assert_eq!(value["messages"][1]["content"][0]["type"], "tool_use");
        assert_eq!(value["messages"][2]["content"][0]["type"], "tool_result");
        assert_eq!(value["max_tokens"], 512);
    }

    #[tokio::test]
    async fn normalizes_messages_sse() {
        async fn messages(
            headers: HeaderMap,
            Json(body): Json<Value>,
        ) -> Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>> {
            assert_eq!(
                headers.get("x-api-key").and_then(|v| v.to_str().ok()),
                Some("test-key")
            );
            assert_eq!(
                headers
                    .get("anthropic-version")
                    .and_then(|v| v.to_str().ok()),
                Some("2023-06-01")
            );
            assert_eq!(body["model"], "claude-test");
            Sse::new(stream::iter(vec![
                Ok(Event::default().data(
                    r#"{"type":"message_start","message":{"id":"msg_1","usage":{"input_tokens":2}}}"#,
                )),
                Ok(Event::default().data(
                    r#"{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"think"}}"#,
                )),
                Ok(Event::default().data(
                    r#"{"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"hello"}}"#,
                )),
                Ok(Event::default().data(
                    r#"{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":3}}"#,
                )),
                Ok(Event::default().data(r#"{"type":"message_stop"}"#)),
            ]))
        }

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let address = listener.local_addr().expect("address should resolve");
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route("/v1/messages", post(messages)),
            )
            .await
            .expect("mock server should run");
        });
        let config = HarnessConfig::from_toml_str(&format!(
            r#"
default_model = "primary"
[models.primary]
model = "claude-test"
[models.primary.provider]
type = "anthropic"
base_url = "http://{address}/v1"
api_key = "test-key"
"#
        ))
        .expect("config should parse");
        let provider = AnthropicMessagesProvider::from_model_config(config.default_model())
            .expect("provider should build");
        let events: Vec<_> = provider
            .stream(ModelRequest {
                run_id: RunId::new(),
                model: "claude-test".into(),
                messages: vec![ModelMessage::user("ping")],
                tools: Vec::new(),
                max_output_tokens: None,
            })
            .collect()
            .await;
        assert!(matches!(events[0], ModelEvent::Accepted { .. }));
        assert!(matches!(&events[1], ModelEvent::ReasoningDelta { delta } if delta == "think"));
        assert!(matches!(&events[2], ModelEvent::TextDelta { delta } if delta == "hello"));
        assert!(matches!(events[3], ModelEvent::Usage { .. }));
        assert!(matches!(
            events[4],
            ModelEvent::Completed {
                finish_reason: FinishReason::Stop
            }
        ));
        server.abort();
    }
}
