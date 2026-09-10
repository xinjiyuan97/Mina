//! OpenAI Responses API adapter.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use agent_core::{
    context::TokenEstimator,
    harness::{
        BlobStore, FinishReason, ModelError, ModelErrorKind, ModelEvent, ModelEventStream,
        ModelMessage, ModelPort, ModelRequest, ModelRole, TokenUsage, TokenUsageSource,
    },
};
use async_stream::stream;
use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use url::Url;

use super::{
    ModelConfig, OpenAiProtocol, ProviderConfig, SecretString,
    attachment::{data_url, load_blob},
    openai::AdapterConfigError,
};

pub struct OpenAiResponsesProvider {
    client: Client,
    base_url: Url,
    api_key: Option<SecretString>,
    organization: Option<String>,
    project: Option<String>,
    blob_store: Option<Arc<dyn BlobStore>>,
    #[allow(dead_code)]
    token_estimator: Option<Arc<dyn TokenEstimator>>,
}

impl OpenAiResponsesProvider {
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
        if *protocol != OpenAiProtocol::Responses {
            return Err(AdapterConfigError::UnsupportedProtocol);
        }
        Ok(Self {
            client: Client::builder().build()?,
            base_url: base_url.clone(),
            api_key: api_key
                .as_ref()
                .map(|source| source.resolve())
                .transpose()?,
            organization: organization.clone(),
            project: project.clone(),
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
        self.base_url.join("responses").map_err(|_| {
            ModelError::new(
                ModelErrorKind::Internal,
                "model endpoint could not be constructed",
                false,
            )
        })
    }
}

impl ModelPort for OpenAiResponsesProvider {
    fn stream(&self, request: ModelRequest) -> ModelEventStream {
        let client = self.client.clone();
        let endpoint = self.endpoint();
        let api_key = self.api_key.clone();
        let organization = self.organization.clone();
        let project = self.project.clone();
        let blob_store = self.blob_store.clone();

        Box::pin(stream! {
            let endpoint = match endpoint {
                Ok(endpoint) => endpoint,
                Err(error) => {
                    yield ModelEvent::Failed { error };
                    return;
                }
            };
            let body = match responses_request(&request, blob_store.as_ref()).await {
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
            if !response.status().is_success() {
                yield ModelEvent::Failed { error: map_http_error(response.status()) };
                return;
            }

            let mut source = response.bytes_stream().eventsource();
            let mut accepted = false;
            let mut saw_output = false;
            let mut tool_calls = HashMap::<String, String>::new();
            let mut provider_tools = ResponsesProviderToolTracker::default();
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
                if data.is_empty() || data == "[DONE]" {
                    continue;
                }
                let value: Value = match serde_json::from_str(data) {
                    Ok(value) => value,
                    Err(_) => {
                        yield ModelEvent::Failed {
                            error: protocol_error("Responses API returned an invalid event"),
                        };
                        return;
                    }
                };
                let event_type = value
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or(event.event.as_str());
                for event in provider_tools.push(event_type, &value) {
                    saw_output = true;
                    yield event;
                }
                match event_type {
                    "response.created" | "response.in_progress" => {
                        if !accepted {
                            accepted = true;
                            let request_id = value
                                .pointer("/response/id")
                                .or_else(|| value.get("response_id"))
                                .and_then(Value::as_str)
                                .map(str::to_owned);
                            yield ModelEvent::Accepted { provider_request_id: request_id };
                        }
                    }
                    "response.output_text.delta" => {
                        if let Some(delta) = value.get("delta").and_then(Value::as_str)
                            && !delta.is_empty()
                        {
                            saw_output = true;
                            yield ModelEvent::TextDelta { delta: delta.into() };
                        }
                    }
                    "response.reasoning_summary_text.delta" | "response.reasoning_text.delta" => {
                        if let Some(delta) = value.get("delta").and_then(Value::as_str)
                            && !delta.is_empty()
                        {
                            saw_output = true;
                            yield ModelEvent::ReasoningDelta { delta: delta.into() };
                        }
                    }
                    "response.output_item.added" => {
                        if value.pointer("/item/type").and_then(Value::as_str) == Some("reasoning") {
                            saw_output = true;
                            yield ModelEvent::ReasoningStarted { redacted: true };
                        } else if value.pointer("/item/type").and_then(Value::as_str) == Some("function_call") {
                            let item_id = value.pointer("/item/id").and_then(Value::as_str);
                            let call_id = value.pointer("/item/call_id").and_then(Value::as_str);
                            let name = value.pointer("/item/name").and_then(Value::as_str);
                            let (Some(call_id), Some(name)) = (call_id, name) else {
                                yield ModelEvent::Failed { error: protocol_error("Responses API emitted an incomplete function call") };
                                return;
                            };
                            if let Some(item_id) = item_id {
                                tool_calls.insert(item_id.into(), call_id.into());
                            }
                            saw_output = true;
                            yield ModelEvent::ToolCallStarted { call_id: call_id.into(), name: name.into() };
                        }
                    }
                    "response.output_item.done" => {
                        if value.pointer("/item/type").and_then(Value::as_str) == Some("reasoning") {
                            yield ModelEvent::ReasoningCompleted { redacted: true };
                        }
                    }
                    "response.function_call_arguments.delta" => {
                        let call_id = value
                            .get("call_id")
                            .and_then(Value::as_str)
                            .map(str::to_owned)
                            .or_else(|| value.get("item_id").and_then(Value::as_str).and_then(|id| tool_calls.get(id).cloned()));
                        let Some(call_id) = call_id else {
                            yield ModelEvent::Failed { error: protocol_error("Responses API emitted arguments before a function call") };
                            return;
                        };
                        if let Some(delta) = value.get("delta").and_then(Value::as_str)
                            && !delta.is_empty()
                        {
                            saw_output = true;
                            yield ModelEvent::ToolCallArgumentsDelta { call_id, delta: delta.into() };
                        }
                    }
                    "response.completed" => {
                        if !accepted {
                            yield ModelEvent::Accepted { provider_request_id: value.pointer("/response/id").and_then(Value::as_str).map(str::to_owned) };
                        }
                        if let Some(usage) = responses_usage(&value) {
                            yield ModelEvent::Usage { usage };
                        }
                        yield ModelEvent::Completed {
                            finish_reason: if tool_calls.is_empty() { FinishReason::Stop } else { FinishReason::ToolCall },
                        };
                        return;
                    }
                    "response.incomplete" => {
                        if let Some(usage) = responses_usage(&value) {
                            yield ModelEvent::Usage { usage };
                        }
                        let reason = value.pointer("/response/incomplete_details/reason").and_then(Value::as_str);
                        yield ModelEvent::Completed {
                            finish_reason: if reason == Some("max_output_tokens") { FinishReason::Length } else { FinishReason::Unknown },
                        };
                        return;
                    }
                    "response.failed" | "error" => {
                        yield ModelEvent::Failed {
                            error: ModelError::new(
                                ModelErrorKind::UpstreamUnavailable,
                                "Responses API reported a failed response",
                                false,
                            ),
                        };
                        return;
                    }
                    _ => {}
                }
            }
            yield ModelEvent::Failed {
                error: protocol_error("Responses API stream ended without a terminal event"),
            };
        })
    }
}

async fn responses_request(
    request: &ModelRequest,
    blob_store: Option<&Arc<dyn BlobStore>>,
) -> Result<Value, ModelError> {
    let mut input = Vec::new();
    for message in &request.messages {
        append_message_items(&mut input, message, blob_store).await?;
    }
    let tools: Vec<_> = request
        .tools
        .iter()
        .map(|tool| {
            json!({
                "type": "function",
                "name": tool.name,
                "description": tool.description,
                "parameters": tool.input_schema,
                "strict": false
            })
        })
        .collect();
    let mut body = json!({
        "model": request.model,
        "input": input,
        "tools": tools,
        "stream": true
    });
    if let Some(max_output_tokens) = request.max_output_tokens {
        body["max_output_tokens"] = json!(max_output_tokens);
    }
    Ok(body)
}

async fn append_message_items(
    input: &mut Vec<Value>,
    message: &ModelMessage,
    blob_store: Option<&Arc<dyn BlobStore>>,
) -> Result<(), ModelError> {
    if message.role == ModelRole::Tool {
        let Some(call_id) = &message.tool_call_id else {
            return Err(protocol_error("tool result is missing its call id"));
        };
        input.push(
            json!({"type": "function_call_output", "call_id": call_id, "output": message.content}),
        );
        return Ok(());
    }

    let mut content = Vec::new();
    if !message.content.is_empty() {
        let content_type = if message.role == ModelRole::Assistant {
            "output_text"
        } else {
            "input_text"
        };
        content.push(json!({"type": content_type, "text": message.content}));
    }
    for attachment in &message.attachments {
        if message.role != ModelRole::User {
            return Err(ModelError::new(
                ModelErrorKind::InvalidRequest,
                "binary input is only supported on user messages",
                false,
            ));
        }
        let object = load_blob(blob_store, attachment).await?;
        if object.metadata.media_type.starts_with("image/") {
            content.push(json!({
                "type": "input_image",
                "image_url": data_url(&object.metadata.media_type, &object.data),
                "detail": "auto"
            }));
        } else {
            let filename = attachment
                .name
                .as_ref()
                .or(object.metadata.name.as_ref())
                .cloned()
                .unwrap_or_else(|| format!("{}.bin", attachment.blob_id));
            content.push(json!({
                "type": "input_file",
                "filename": filename,
                "file_data": data_url(&object.metadata.media_type, &object.data)
            }));
        }
    }
    if !content.is_empty() {
        input.push(json!({"role": message.role.as_str(), "content": content}));
    }
    for call in &message.tool_calls {
        input.push(json!({
            "type": "function_call",
            "call_id": call.id,
            "name": call.name,
            "arguments": call.arguments
        }));
    }
    Ok(())
}

fn responses_usage(value: &Value) -> Option<TokenUsage> {
    let usage = value.pointer("/response/usage")?;
    let input_tokens = usage.get("input_tokens")?.as_u64()?;
    let output_tokens = usage.get("output_tokens")?.as_u64()?;
    Some(TokenUsage {
        input_tokens,
        output_tokens,
        total_tokens: usage
            .get("total_tokens")
            .and_then(Value::as_u64)
            .unwrap_or_else(|| input_tokens.saturating_add(output_tokens)),
        source: TokenUsageSource::ProviderReported,
    })
}

#[derive(Default)]
struct ResponsesProviderToolTracker {
    started: HashSet<String>,
    completed: HashSet<String>,
}

impl ResponsesProviderToolTracker {
    fn push(&mut self, event_type: &str, value: &Value) -> Vec<ModelEvent> {
        match event_type {
            "response.output_item.added" => value
                .get("item")
                .map_or_else(Vec::new, |item| self.start_from_item(item, "in_progress")),
            "response.output_item.done" => value
                .get("item")
                .map_or_else(Vec::new, |item| self.complete_from_item(item)),
            "response.web_search_call.in_progress"
            | "response.web_search_call.searching"
            | "response.web_search_call.completed" => {
                let Some(call_id) = value.get("item_id").and_then(Value::as_str) else {
                    return Vec::new();
                };
                let status = event_type
                    .strip_prefix("response.web_search_call.")
                    .unwrap_or("in_progress");
                self.start(call_id, "web_search", json!({ "status": status }))
            }
            "response.completed" | "response.incomplete" => {
                let mut events = Vec::new();
                if let Some(output) = value.pointer("/response/output").and_then(Value::as_array) {
                    for item in output {
                        events.extend(self.complete_from_item(item));
                    }
                }
                let pending = self
                    .started
                    .iter()
                    .filter(|call_id| !self.completed.contains(*call_id))
                    .cloned()
                    .collect::<Vec<_>>();
                for call_id in pending {
                    events.extend(self.complete(&call_id, json!({ "status": "completed" })));
                }
                events
            }
            _ => Vec::new(),
        }
    }

    fn start_from_item(&mut self, item: &Value, fallback_status: &str) -> Vec<ModelEvent> {
        let Some(name) = provider_tool_name(item.get("type").and_then(Value::as_str)) else {
            return Vec::new();
        };
        let Some(call_id) = item.get("id").and_then(Value::as_str) else {
            return Vec::new();
        };
        let arguments = item.get("action").cloned().unwrap_or_else(|| {
            json!({
                "status": item
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or(fallback_status)
            })
        });
        self.start(call_id, name, arguments)
    }

    fn complete_from_item(&mut self, item: &Value) -> Vec<ModelEvent> {
        if provider_tool_name(item.get("type").and_then(Value::as_str)).is_none() {
            return Vec::new();
        }
        let Some(call_id) = item.get("id").and_then(Value::as_str) else {
            return Vec::new();
        };
        let mut events = self.start_from_item(item, "completed");
        let mut output = json!({
            "status": item
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("completed")
        });
        if let Some(action) = item.get("action") {
            output["action"] = action.clone();
        }
        events.extend(self.complete(call_id, output));
        events
    }

    fn start(&mut self, call_id: &str, name: &str, arguments: Value) -> Vec<ModelEvent> {
        if !self.started.insert(call_id.to_owned()) {
            return Vec::new();
        }
        vec![ModelEvent::ProviderToolCallStarted {
            call_id: call_id.into(),
            name: name.into(),
            arguments,
        }]
    }

    fn complete(&mut self, call_id: &str, output: Value) -> Vec<ModelEvent> {
        if !self.completed.insert(call_id.to_owned()) {
            return Vec::new();
        }
        vec![ModelEvent::ProviderToolCallCompleted {
            call_id: call_id.into(),
            output: output.to_string(),
        }]
    }
}

fn provider_tool_name(item_type: Option<&str>) -> Option<&'static str> {
    match item_type {
        Some("web_search_call") => Some("web_search"),
        _ => None,
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

    use agent_core::harness::{ModelMessage, RunId, ToolDefinition};
    use axum::{
        Json, Router,
        response::sse::{Event, Sse},
        routing::post,
    };
    use futures_util::{StreamExt, stream};
    use tokio::net::TcpListener;

    use super::*;

    #[tokio::test]
    async fn maps_provider_neutral_messages_to_responses_input_items() {
        let request = ModelRequest {
            run_id: RunId::new(),
            model: "gpt-test".into(),
            messages: vec![
                ModelMessage::system("Be concise."),
                ModelMessage::user("hello"),
                ModelMessage::assistant_tool_calls(
                    "checking",
                    "",
                    vec![agent_core::harness::ModelToolCall {
                        id: "call_1".into(),
                        name: "lookup".into(),
                        arguments: "{\"q\":\"x\"}".into(),
                    }],
                ),
                ModelMessage::tool_result("call_1", "result"),
            ],
            tools: vec![ToolDefinition::new(
                "lookup",
                "Look something up",
                json!({"type": "object"}),
            )],
            max_output_tokens: Some(123),
        };
        let value = responses_request(&request, None)
            .await
            .expect("request should map");
        assert_eq!(value["model"], "gpt-test");
        assert_eq!(value["input"][0]["role"], "system");
        assert_eq!(value["input"][1]["content"][0]["type"], "input_text");
        assert_eq!(value["input"][2]["role"], "assistant");
        assert_eq!(value["input"][2]["content"][0]["type"], "output_text");
        assert_eq!(value["input"][3]["type"], "function_call");
        assert_eq!(value["input"][4]["type"], "function_call_output");
        assert_eq!(value["tools"][0]["name"], "lookup");
        assert_eq!(value["max_output_tokens"], 123);
    }

    #[tokio::test]
    async fn normalizes_responses_sse() {
        async fn responses(
            Json(body): Json<Value>,
        ) -> Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>> {
            assert_eq!(body["model"], "gpt-test");
            assert_eq!(body["stream"], true);
            Sse::new(stream::iter(vec![
                Ok(Event::default().data(
                    r#"{"type":"response.created","response":{"id":"resp_1"}}"#,
                )),
                Ok(Event::default().data(
                    r#"{"type":"response.output_item.added","output_index":0,"item":{"type":"reasoning","id":"rs_1","summary":[]}}"#,
                )),
                Ok(Event::default().data(
                    r#"{"type":"response.reasoning_summary_text.delta","delta":"think"}"#,
                )),
                Ok(Event::default().data(
                    r#"{"type":"response.output_item.done","output_index":0,"item":{"type":"reasoning","id":"rs_1","summary":[]}}"#,
                )),
                Ok(Event::default().data(
                    r#"{"type":"response.web_search_call.in_progress","item_id":"ws_1"}"#,
                )),
                Ok(Event::default().data(
                    r#"{"type":"response.web_search_call.searching","item_id":"ws_1"}"#,
                )),
                Ok(Event::default().data(
                    r#"{"type":"response.output_item.done","item":{"type":"web_search_call","id":"ws_1","status":"completed","action":{"type":"search","query":"Mina"}}}"#,
                )),
                Ok(Event::default().data(
                    r#"{"type":"response.output_text.delta","delta":"hello"}"#,
                )),
                Ok(Event::default().data(
                    r#"{"type":"response.completed","response":{"id":"resp_1","usage":{"input_tokens":2,"output_tokens":3,"total_tokens":5}}}"#,
                )),
            ]))
        }

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let address = listener.local_addr().expect("address should resolve");
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route("/v1/responses", post(responses)),
            )
            .await
            .expect("mock server should run");
        });
        let model = ModelConfig {
            model: "gpt-test".into(),
            modalities: crate::provider::Modalities::default(),
            context_window: None,
            max_output_tokens: None,
            provider: ProviderConfig::OpenAiCompatible {
                base_url: Url::parse(&format!("http://{address}/v1/"))
                    .expect("test URL should parse"),
                protocol: OpenAiProtocol::Responses,
                api_key: Some(crate::provider::SecretSource::Literal("test-key".into())),
                organization: None,
                project: None,
            },
        };
        let provider =
            OpenAiResponsesProvider::from_model_config(&model).expect("provider should build");
        let events: Vec<_> = provider
            .stream(ModelRequest {
                run_id: RunId::new(),
                model: "gpt-test".into(),
                messages: vec![ModelMessage::user("ping")],
                tools: Vec::new(),
                max_output_tokens: None,
            })
            .collect()
            .await;
        assert!(matches!(events[0], ModelEvent::Accepted { .. }));
        assert!(matches!(
            events[1],
            ModelEvent::ReasoningStarted { redacted: true }
        ));
        assert!(matches!(&events[2], ModelEvent::ReasoningDelta { delta } if delta == "think"));
        assert!(matches!(
            events[3],
            ModelEvent::ReasoningCompleted { redacted: true }
        ));
        assert!(matches!(
            &events[4],
            ModelEvent::ProviderToolCallStarted { call_id, name, .. }
                if call_id == "ws_1" && name == "web_search"
        ));
        assert!(matches!(
            &events[5],
            ModelEvent::ProviderToolCallCompleted { call_id, .. } if call_id == "ws_1"
        ));
        assert!(matches!(&events[6], ModelEvent::TextDelta { delta } if delta == "hello"));
        assert!(matches!(events[7], ModelEvent::Usage { .. }));
        assert!(matches!(
            events[8],
            ModelEvent::Completed {
                finish_reason: FinishReason::Stop
            }
        ));
        server.abort();
    }
}
