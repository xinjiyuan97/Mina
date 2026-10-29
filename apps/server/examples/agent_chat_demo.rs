//! Local agent-chat/1 host backed by Core static graph operators.
use agent_core::{
    agent::{
        AgentDefinition, AgentResources, GraphAgent, Harness, OperatorRegistry, RunOptions,
        memory_fabric::MemoryFabric,
    },
    contracts::{
        BlobId, BlobStore, ModelAttachment, ModelMessage, PutBlob, RunCancellation, RunId,
        ToolRegistry,
    },
};
use agent_extension::{
    output::{AgentChatAdapter, AgentChatConfig},
    provider::{ConfiguredModelProvider, ModelConfig},
    store::FilesystemBlobStore,
    tool::{BuiltinToolCatalog, GenerateImageTool, ImageGenerationConfig},
};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{delete, get, post},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    convert::Infallible,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

const MAX_BODY: usize = 50 * 1024 * 1024;
const MAX_FILE: usize = 10 * 1024 * 1024;
type DemoHarness = Harness<GraphAgent>;

// Log metadata only: never serialize model requests, response text, or credentials.
struct DebugProvider(ConfiguredModelProvider);
impl agent_core::contracts::ModelPort for DebugProvider {
    fn stream(
        &self,
        request: agent_core::contracts::ModelRequest,
    ) -> agent_core::contracts::ModelEventStream {
        use agent_core::contracts::ModelEvent;
        let run_id = request.run_id;
        let mut upstream = self.0.stream(request);
        Box::pin(async_stream::stream! {
            let started = std::time::Instant::now();
            let mut last = started;
            let mut count = 0_u64;
            tracing::info!(%run_id, "upstream_start");
            let mut watchdog = tokio::time::interval(Duration::from_secs(5));
            watchdog.tick().await;
            loop {
                tokio::select! {
                    event = upstream.next() => {
                        let Some(event) = event else { break; };
                        let (kind, bytes) = match &event {
                            ModelEvent::TextDelta { delta } => ("text", delta.len()),
                            ModelEvent::ReasoningDelta { delta } => ("reasoning", delta.len()),
                            ModelEvent::Completed { .. } => ("completed", 0),
                            ModelEvent::Failed { .. } => ("failed", 0),
                            _ => ("metadata", 0),
                        };
                        count += 1;
                        tracing::debug!(%run_id, count, kind, bytes, elapsed_ms = started.elapsed().as_millis() as u64, gap_ms = last.elapsed().as_millis() as u64, "upstream_event");
                        last = std::time::Instant::now();
                        yield event;
                    }
                    _ = watchdog.tick() => {
                        tracing::info!(%run_id, count, idle_ms = last.elapsed().as_millis() as u64, "upstream_wait");
                    }
                }
            }
            tracing::info!(%run_id, count, elapsed_ms = started.elapsed().as_millis() as u64, "upstream_end");
        })
    }
}
type History = Arc<tokio::sync::Mutex<Vec<ModelMessage>>>;
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    listen: SocketAddr,
    #[serde(default)]
    graph: Option<std::path::PathBuf>,
    model: ModelConfig,
    #[serde(default)]
    image_generation: Option<ImageGenerationConfig>,
    #[serde(default)]
    workspace: Option<WorkspaceConfig>,
    #[serde(default)]
    context: Option<ContextConfig>,
    #[serde(default = "prompt")]
    system_prompt: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkspaceConfig {
    root: std::path::PathBuf,
    #[serde(default)]
    terminal: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ContextConfig {
    skill_directory: std::path::PathBuf,
}
fn prompt() -> String {
    "You are Mina, a helpful assistant.".into()
}
#[derive(Default)]
struct Registry {
    turns: HashMap<String, RunCancellation>,
    conversations: HashMap<String, History>,
}
#[derive(Clone)]
struct App {
    harness: DemoHarness,
    registry: Arc<Mutex<Registry>>,
    blobs: Arc<dyn BlobStore>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Request {
    protocol: String,
    conversation_id: String,
    turn_id: String,
    run_id: String,
    input: Vec<Input>,
}
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
enum Input {
    Message {
        #[serde(rename = "inputId")]
        input_id: String,
        parts: Vec<Part>,
    },
}
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
enum Part {
    Text {
        text: String,
    },
    File {
        url: String,
        #[serde(rename = "mediaType")]
        media_type: String,
        name: Option<String>,
    },
}
fn failure(status: StatusCode, code: &str, message: &str) -> Response {
    (
        status,
        Json(json!({"scope":"turn", "code":code, "message":message, "retryable":false})),
    )
        .into_response()
}
fn parse_input(request: &Request) -> Result<(String, Vec<PutBlob>), &'static str> {
    let [Input::Message { input_id, parts }] = request.input.as_slice() else {
        return Err("Exactly one message input is required");
    };
    if [
        &request.conversation_id,
        &request.turn_id,
        &request.run_id,
        input_id,
    ]
    .iter()
    .any(|id| id.trim().is_empty() || id.len() > 256)
    {
        return Err("IDs must contain 1–256 bytes");
    }
    let mut text = Vec::new();
    let mut files = Vec::new();
    for part in parts {
        match part {
            Part::Text { text: value } => text.push(value.as_str()),
            Part::File {
                url,
                media_type,
                name,
            } => {
                if files.len() >= 10 {
                    return Err("At most 10 files are allowed");
                }
                if name.as_ref().is_some_and(|name| name.len() > 512) {
                    return Err("File name exceeds 512 bytes");
                }
                let (prefix, encoded) = url
                    .split_once(',')
                    .ok_or("File url must be a base64 data URL")?;
                if prefix != format!("data:{media_type};base64") {
                    return Err(
                        "Expected a base64 data URL matching mediaType; remote/blob URLs are unsupported",
                    );
                }
                if encoded.len() > MAX_FILE.div_ceil(3) * 4 {
                    return Err("File exceeds 10 MiB");
                }
                let data = STANDARD.decode(encoded).map_err(|_| "Invalid base64")?;
                if data.is_empty() || data.len() > MAX_FILE {
                    return Err("File must contain 1 byte to 10 MiB");
                }
                let valid = match media_type.as_str() {
                    "image/png" => data.starts_with(b"\x89PNG\r\n\x1a\n"),
                    "image/jpeg" => data.starts_with(&[0xff, 0xd8, 0xff]),
                    "image/gif" => data.starts_with(b"GIF87a") || data.starts_with(b"GIF89a"),
                    "image/webp" => data.starts_with(b"RIFF") && data.get(8..12) == Some(b"WEBP"),
                    "application/pdf" => data.starts_with(b"%PDF-"),
                    "text/plain" | "text/csv" | "text/markdown" => {
                        std::str::from_utf8(&data).is_ok()
                    }
                    _ => {
                        return Err(
                            "Supported files: PNG, JPEG, GIF, WebP, PDF and UTF-8 plain text, CSV, Markdown",
                        );
                    }
                };
                if !valid {
                    return Err("File content does not match mediaType");
                }
                files.push(PutBlob {
                    media_type: media_type.clone(),
                    name: name.clone(),
                    data,
                    created_at_ms: agent_core::agent::unix_time_ms(),
                });
            }
        }
    }
    let text = text.join("\n");
    if text.trim().is_empty() && files.is_empty() {
        return Err("Message is empty");
    }
    Ok((text, files))
}
// Dropping an unpolled or disconnected response must cancel its execution.
struct CancelOnDrop(RunCancellation);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}
fn stamp(event: &mut Value, request: &Request, seq: u64) {
    event["eventId"] = json!(seq.to_string());
    event["conversationId"] = json!(request.conversation_id);
    event["turnId"] = json!(request.turn_id);
    event["runId"] = json!(request.run_id);
}
async fn chat(
    State(app): State<App>,
    headers: HeaderMap,
    Json(request): Json<Request>,
) -> Response {
    if request.protocol != "agent-chat/1" {
        return failure(
            StatusCode::UPGRADE_REQUIRED,
            "validation",
            "Supported protocol: agent-chat/1",
        );
    }
    if headers.contains_key("last-event-id") {
        return failure(
            StatusCode::CONFLICT,
            "resume_expired",
            "Demo does not support replay",
        );
    }
    let (text, files) = match parse_input(&request) {
        Ok(input) => input,
        Err(message) => return failure(StatusCode::BAD_REQUEST, "validation", message),
    };
    let conversation = {
        let mut registry = app.registry.lock().expect("registry");
        if registry.turns.contains_key(&request.turn_id) {
            return failure(
                StatusCode::CONFLICT,
                "duplicate_turn",
                "Turn already accepted",
            );
        }
        if registry.conversations.len() >= 10_000
            && !registry
                .conversations
                .contains_key(&request.conversation_id)
        {
            return failure(
                StatusCode::SERVICE_UNAVAILABLE,
                "capacity",
                "Demo registry full; restart server",
            );
        }
        Arc::clone(
            registry
                .conversations
                .entry(request.conversation_id.clone())
                .or_default(),
        )
    };
    let mut history = match conversation.try_lock_owned() {
        Ok(history) => history,
        Err(_) => {
            return failure(
                StatusCode::CONFLICT,
                "conversation_busy",
                "Wait for the current turn to finish",
            );
        }
    };
    let mut attachments = Vec::new();
    for file in files {
        let blob = match app.blobs.put(file).await {
            Ok(blob) => blob,
            Err(_) => {
                return failure(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "storage",
                    "Could not store attachment",
                );
            }
        };
        attachments.push(ModelAttachment {
            blob_id: blob.blob_id,
            size_bytes: Some(blob.size_bytes),
            media_type: blob.media_type,
            name: blob.name,
        });
    }
    let execution = {
        let mut registry = app.registry.lock().expect("registry");
        if registry.turns.contains_key(&request.turn_id) {
            return failure(
                StatusCode::CONFLICT,
                "duplicate_turn",
                "Turn already accepted",
            );
        }
        if registry.turns.len() >= 10_000 {
            return failure(
                StatusCode::SERVICE_UNAVAILABLE,
                "capacity",
                "Demo registry full; restart server",
            );
        }
        let run = match app.harness.start_with_options_and_attachments(
            RunId::new(),
            text.clone(),
            attachments.clone(),
            history.clone(),
            RunOptions::default(),
        ) {
            Ok(run) => run,
            Err(_) => return failure(StatusCode::BAD_REQUEST, "validation", "Invalid model input"),
        };
        registry
            .turns
            .insert(request.turn_id.clone(), run.cancellation.clone());
        run
    };
    let internal_run_id = execution.run_id;
    tracing::info!(run_id = %internal_run_id, turn_id = %request.turn_id, wire_run_id = %request.run_id, history_messages = history.len(), attachments = attachments.len(), "request_accepted");
    let guard = CancelOnDrop(execution.cancellation);
    let mut config = AgentChatConfig::new(
        &request.conversation_id,
        &request.turn_id,
        format!("assistant:{}", request.turn_id),
    );
    config.capabilities.block_ids = false;
    config.capabilities.permissions = false;
    config.limits.max_attachment_bytes = MAX_FILE as u64;
    config.limits.max_attachment_count = 10;
    config.limits.disconnect_grace_period_ms = 0;
    let mut adapter = AgentChatAdapter::new(config);
    let output = async_stream::stream! {
        let _guard = guard;
        let started = std::time::Instant::now();
        let mut seq = 1;
        let mut hello = adapter.server_hello();
        stamp(&mut hello, &request, seq);
        yield Ok::<_, Infallible>(Event::default().id(seq.to_string()).data(hello.to_string()));
        let mut events = execution.events;
        let mut answer = String::new();
        while let Some(event) = events.next().await {
            let frames = adapter.adapt(&event);
            let failed = frames.iter().any(|frame| frame.event["type"] == "error");
            for frame in frames {
                let mut value = frame.event;
                if failed && value["type"] == "message-end" { continue; }
                if value["type"] == "error" {
                    value["scope"] = json!("turn");
                    value["message"] = json!("Model request failed; check model configuration and upstream availability");
                    value.as_object_mut().expect("object").remove("error");
                }
                if value["type"] == "text-delta" && let Some(delta) = value["delta"].as_str() { answer.push_str(delta); }
                if value["type"] == "message-end" && matches!(value["finishReason"].as_str(), Some("stop" | "length")) {
                    history.push(ModelMessage::user_with_attachments(text.clone(), attachments.clone()));
                    history.push(ModelMessage::assistant_tool_calls(answer.clone(), "", Vec::new()));
                }
                seq += 1;
                stamp(&mut value, &request, seq);
                tracing::debug!(run_id = %internal_run_id, seq, kind = value["type"].as_str().unwrap_or("unknown"), bytes = value["delta"].as_str().map_or(0, str::len), elapsed_ms = started.elapsed().as_millis() as u64, "sse_yield");
                if value["type"] == "message-end" || value["type"] == "error" {
                    tracing::info!(run_id = %internal_run_id, finish_reason = value["finishReason"].as_str().unwrap_or("error"), elapsed_ms = started.elapsed().as_millis() as u64, "request_terminal");
                    drop(history);
                    yield Ok(Event::default().id(seq.to_string()).data(value.to_string()));
                    return;
                }
                yield Ok(Event::default().id(seq.to_string()).data(value.to_string()));
            }
        }
        seq += 1;
        let mut error = json!({"type":"error","scope":"turn","code":"transport","message":"Run ended without a terminal event","retryable":true});
        stamp(&mut error, &request, seq);
        yield Ok(Event::default().id(seq.to_string()).data(error.to_string()));
    };
    let mut response = Sse::new(output)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
        .into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        "no-cache, no-transform".parse().expect("header"),
    );
    response
        .headers_mut()
        .insert("x-accel-buffering", "no".parse().expect("header"));
    response
}
async fn cancel(State(app): State<App>, Path(turn): Path<String>) -> StatusCode {
    if let Some(cancel) = app.registry.lock().expect("registry").turns.get(&turn) {
        cancel.cancel();
    }
    StatusCode::NO_CONTENT
}
async fn image_blob(State(app): State<App>, Path(id): Path<BlobId>) -> Response {
    match app.blobs.get(id).await {
        Ok(Some(blob))
            if blob.metadata.media_type == "image/png"
                && blob.metadata.name.as_deref() == Some("generated.png") =>
        {
            (
                [
                    (header::CONTENT_TYPE, "image/png"),
                    (header::CACHE_CONTROL, "private, max-age=3600"),
                    (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
                ],
                blob.data,
            )
                .into_response()
        }
        Ok(_) => StatusCode::NOT_FOUND.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}
fn router(app: App) -> Router {
    Router::new()
        .route(
            "/health",
            get(|| async {
                Json(json!({"status":"ok","protocol":"agent-chat/1","engine":"static_graph"}))
            }),
        )
        .route("/chat", post(chat))
        .route("/images/{id}", get(image_blob))
        .route("/turns/{turn}", delete(cancel))
        .layer(DefaultBodyLimit::max(MAX_BODY))
        .with_state(app)
}
fn build_app(
    mut config: Config,
    blobs: Arc<dyn BlobStore>,
) -> Result<App, Box<dyn std::error::Error>> {
    config.model.normalize_and_validate("model")?;
    let provider = ConfiguredModelProvider::from_model_config(&config.model)?
        .with_blob_store(Arc::clone(&blobs));
    let mut tools = if let Some(workspace) = &config.workspace {
        std::fs::create_dir_all(&workspace.root)?;
        let mut catalog =
            BuiltinToolCatalog::new(&workspace.root).with_blob_store(Arc::clone(&blobs));
        if workspace.terminal {
            catalog = catalog.enable_terminal_tools();
        }
        catalog.build()?
    } else {
        ToolRegistry::new()
    };
    let image_timeout = config.image_generation.as_ref().map(|c| c.timeout_ms);
    if let Some(image) = config.image_generation {
        tools.register(GenerateImageTool::new(
            image,
            Arc::clone(&blobs),
            "/images",
        )?)?;
    }
    let uses_tools = config.workspace.is_some() || image_timeout.is_some();
    let graph_path = config.graph.clone().unwrap_or_else(|| {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/agents/demo.yaml")
    });
    let mut definition = AgentDefinition::from_file(graph_path)?;
    if uses_tools
        && !definition
            .operators
            .values()
            .any(|op| op.kind == "agent.tools")
    {
        return Err("Configured tools require a graph with agent.tools".into());
    }
    for op in definition.operators.values_mut() {
        if op.kind == "agent.context" {
            op.config["system_prompt"] = json!(config.system_prompt);
        }
        if op.kind == "agent.model" {
            op.config["model_name"] = json!(config.model.model);
            if let Some(limit) = config.model.max_output_tokens {
                op.config["max_output_tokens"] = json!(limit);
            }
        }
    }
    let preparation = config.context.map(|context| {
        use agent_core::{context::*, skill::SkillOrchestrator};
        use agent_extension::store::{FilesystemSkillStore, InMemoryMemoryStore, InMemoryRunStore};
        let policy = agent_core::contracts::preparation::ContextPolicy {
            default_skills: vec![],
            max_skills: 8,
            memory_enabled: false,
            memory_scope: "demo".into(),
            max_memories: 0,
            agent_profile: "demo".into(),
            agent_instruction: config.system_prompt.clone(),
            model_profile: config.model.model.clone(),
            available_tools: agent_core::contracts::ToolPort::definitions(&tools)
                .into_iter()
                .map(|t| t.name)
                .collect(),
            budget: ContextBudget {
                model_context_tokens: 65536,
                reserved_output_tokens: u64::from(config.model.max_output_tokens.unwrap_or(4096)),
                reserved_tool_schema_tokens: 4096,
                max_skill_tokens: 4096,
                max_memory_tokens: 0,
                max_history_tokens: 16000,
            },
            policy_version: 1,
            allow_run_adf: false,
            script_runtime: json!({}),
        };
        let preparer = agent_core::agent::host::preparation::ContextPreparation {
            context: ContextEngine::new(
                Arc::new(InMemoryRunStore::default()),
                Arc::new(InMemoryMemoryStore::default()),
                Arc::new(SlidingWindowCompressor::new(Arc::new(
                    HeuristicTokenEstimator,
                ))),
                Arc::new(InMemoryContextArtifactStore::default()),
            ),
            skills: Arc::new(SkillOrchestrator::new(Arc::new(FilesystemSkillStore::new(
                context.skill_directory,
                "demo-skills",
            )))),
        };
        (Arc::new(preparer), policy)
    });
    let resources = AgentResources::new(Arc::new(MemoryFabric::default()))
        .model("primary", "demo-v1", Arc::new(DebugProvider(provider)))
        .toolset("workspace", "demo-v1", Arc::new(tools));
    let mut agent = GraphAgent::new(definition, OperatorRegistry::builtins(), resources)?;
    if let Some((preparer, policy)) = preparation {
        agent = agent.with_context_preparation(preparer, policy);
    }
    let harness = Harness::new(agent).with_run_timeout(Duration::from_secs(300));
    Ok(App {
        harness,
        blobs,
        registry: Arc::default(),
    })
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all("data")?;
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open("data/agent-chat-demo-debug.log")?;
    tracing_subscriber::fmt()
        .with_ansi(false)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                tracing_subscriber::EnvFilter::new("agent_chat_demo=debug,agent_core=info")
            }),
        )
        .with_writer(std::sync::Mutex::new(log))
        .init();
    eprintln!("Diagnostic log: data/agent-chat-demo-debug.log (metadata only)");
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config/agent-chat-demo.toml".into());
    let source = std::fs::read_to_string(path)?;
    let config: Config =
        toml::from_str(&source).map_err(|_| "Invalid demo TOML; check the example config")?;
    let listen = config.listen;
    let app = build_app(
        config,
        Arc::new(FilesystemBlobStore::new("data/agent-chat-demo-blobs")),
    )?;
    let listener = tokio::net::TcpListener::bind(listen).await?;
    println!(
        "Agent chat demo listening on http://{}",
        listener.local_addr()?
    );
    axum::serve(listener, router(app))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn request(turn: &str) -> Value {
        json!({"protocol":"agent-chat/1","conversationId":"c","turnId":turn,"runId":turn,"input":[{"type":"message","inputId":turn,"parts":[{"type":"text","text":"hello"}]}]})
    }
    #[tokio::test]
    async fn graph_workspace_tools_read_write_and_run_real_commands() {
        let seen = Arc::new(Mutex::new(Vec::<Value>::new()));
        let observed = seen.clone();
        let upstream = Router::new().route("/v1/chat/completions", post(move |Json(body): Json<Value>| {
            let observed = observed.clone();
            async move {
                let names: Vec<_> = body["tools"].as_array().expect("tools").iter().filter_map(|t| t["function"]["name"].as_str()).collect();
                for name in ["list_directory", "read", "write", "edit", "search", "shell_command", "exec_command", "write_stdin"] {
                    assert!(names.contains(&name), "missing {name}");
                }
                let results: Vec<_> = body["messages"].as_array().expect("messages").iter().filter(|m| m["role"] == "tool").collect();
                let step = results.len();
                if let Some(result) = results.last() {
                    let result: Value = serde_json::from_str(result["content"].as_str().expect("content")).expect("result");
                    observed.lock().expect("seen").push(result);
                }
                let calls = [
                    ("list_directory", json!({})),
                    ("write", json!({"path":"proof.txt","content":"from-write"})),
                    ("read", json!({"path":"proof.txt"})),
                    ("shell_command", json!({"command":"printf '|from-shell' >> proof.txt"})),
                    ("exec_command", json!({"cmd":"printf '|from-exec' >> proof.txt","yield_time_ms":1000})),
                    ("read", json!({"path":"proof.txt"})),
                    ("read", json!({"path":"../outside.txt"})),
                ];
                let chunk = if let Some((name, args)) = calls.get(step) {
                    json!({"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":format!("call-{step}"),"type":"function","function":{"name":name,"arguments":args.to_string()}}]},"finish_reason":"tool_calls"}]})
                } else {
                    json!({"choices":[{"index":0,"delta":{"content":"workspace-tools-ok"},"finish_reason":"stop"}]})
                };
                ([(header::CONTENT_TYPE,"text/event-stream")], format!("data: {chunk}\n\ndata: [DONE]\n\n"))
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let base = format!("http://{}/v1/", listener.local_addr().expect("addr"));
        let upstream_task =
            tokio::spawn(async move { axum::serve(listener, upstream).await.expect("upstream") });
        let dir = tempfile::tempdir().expect("temp");
        let root = dir.path().join("workspace");
        std::fs::write(dir.path().join("outside.txt"), "private-outside").expect("outside");
        let config = Config {
            context: None,
            workspace: Some(WorkspaceConfig {
                root: root.clone(),
                terminal: true,
            }),
            graph: None,
            image_generation: None,
            listen: "127.0.0.1:0".parse().expect("addr"),
            system_prompt: prompt(),
            model: serde_json::from_value(
                json!({"model":"mock","provider":{"type":"openai-compatible","base_url":base}}),
            )
            .expect("model"),
        };
        let app = build_app(
            config,
            Arc::new(FilesystemBlobStore::new(dir.path().join("blobs"))),
        )
        .expect("app");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let address = listener.local_addr().expect("address");
        let server =
            tokio::spawn(async move { axum::serve(listener, router(app)).await.expect("demo") });
        let output = reqwest::Client::new()
            .post(format!("http://{address}/chat"))
            .json(&request("workspace-turn"))
            .send()
            .await
            .expect("chat")
            .text()
            .await
            .expect("sse");
        assert!(output.contains("workspace-tools-ok"), "{output}");
        assert!(output.contains("message-end"), "{output}");
        assert!(!output.contains("permission-request"), "{output}");
        assert!(!output.contains("private-outside"), "{output}");
        assert_eq!(
            std::fs::read_to_string(root.join("proof.txt")).expect("file"),
            "from-write|from-shell|from-exec"
        );
        let results = seen.lock().expect("results");
        assert_eq!(results.len(), 7);
        assert_eq!(results[2]["content"], "from-write");
        assert_eq!(results[5]["content"], "from-write|from-shell|from-exec");
        assert_eq!(results[6]["ok"], false);
        server.abort();
        upstream_task.abort();
    }

    #[tokio::test]
    async fn demo_uses_shared_skill_preparation_and_budgets_supplied_history() {
        let seen = Arc::new(Mutex::new(Vec::<Value>::new()));
        let observed = seen.clone();
        let upstream = Router::new().route("/v1/chat/completions", post(move |Json(body):Json<Value>| {
            let observed=observed.clone(); async move {
                observed.lock().expect("seen").push(body);
                let chunk=json!({"choices":[{"index":0,"delta":{"content":"prepared-answer"},"finish_reason":"stop"}]});
                ([(header::CONTENT_TYPE,"text/event-stream")],format!("data: {chunk}\n\ndata: [DONE]\n\n"))
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let base = format!("http://{}/v1/", listener.local_addr().expect("addr"));
        let upstream_task =
            tokio::spawn(async move { axum::serve(listener, upstream).await.expect("upstream") });
        let dir = tempfile::tempdir().expect("temp");
        let skill_root = dir.path().join("skills");
        let package = skill_root.join("greeting");
        std::fs::create_dir_all(&package).expect("dir");
        std::fs::write(
            package.join("skill.toml"),
            r#"skill_id = "greeting"
version = "1.0.0"
description = "Greeting instructions"
required_tools = ["read"]
optional_tools = []
required_capabilities = ["tool_calling"]
[compatibility]
contract_version = 1
minimum_harness_version = "0.1.0"
[activation]
type = "routable"
hints = ["hello"]
"#,
        )
        .expect("manifest");
        std::fs::write(
            package.join("SKILL.md"),
            "Use the shared-preparation-marker when greeting.",
        )
        .expect("skill");
        let config = Config {
            context: Some(ContextConfig {
                skill_directory: skill_root,
            }),
            workspace: Some(WorkspaceConfig {
                root: dir.path().join("workspace"),
                terminal: false,
            }),
            graph: None,
            image_generation: None,
            listen: "127.0.0.1:0".parse().expect("addr"),
            system_prompt: prompt(),
            model: serde_json::from_value(
                json!({"model":"mock","provider":{"type":"openai-compatible","base_url":base}}),
            )
            .expect("model"),
        };
        let app = build_app(
            config,
            Arc::new(FilesystemBlobStore::new(dir.path().join("blobs"))),
        )
        .expect("app");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let address = listener.local_addr().expect("addr");
        let server =
            tokio::spawn(async move { axum::serve(listener, router(app)).await.expect("demo") });
        for turn in ["prepared-1", "prepared-2"] {
            let result = reqwest::Client::new()
                .post(format!("http://{address}/chat"))
                .json(&request(turn))
                .send()
                .await
                .expect("chat")
                .text()
                .await
                .expect("sse");
            assert!(result.contains("prepared-answer"), "{result}");
        }
        let seen = seen.lock().expect("seen");
        assert_eq!(seen.len(), 2);
        for body in seen.iter() {
            assert!(
                body["messages"]
                    .to_string()
                    .contains("shared-preparation-marker")
            );
            let tools = body["tools"].as_array().expect("tools");
            assert_eq!(tools.len(), 1);
            assert_eq!(tools[0]["function"]["name"], "read");
        }
        assert!(
            seen[1]["messages"]
                .as_array()
                .expect("messages")
                .iter()
                .any(|m| m["role"] == "assistant" && m["content"] == "prepared-answer")
        );
        server.abort();
        upstream_task.abort();
    }

    const PNG: &str = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYII=";

    fn image_config(base: &str) -> ImageGenerationConfig {
        serde_json::from_value(json!({"model":"test-image-model","provider":{"type":"openai-compatible","base_url":base,"api_key":"test-secret","organization":"test-org","project":"test-project"}})).expect("image config")
    }

    #[tokio::test]
    async fn graph_generates_image_and_serves_saved_png() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = calls.clone();
        let upstream = Router::new()
            .route("/v1/images/generations", post(move |headers: HeaderMap, Json(body): Json<Value>| {
                let observed = observed.clone();
                async move {
                    observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    assert_eq!(headers["authorization"], "Bearer test-secret");
                    assert_eq!(headers["openai-organization"], "test-org");
                    assert_eq!(headers["openai-project"], "test-project");
                    assert_eq!(body, json!({"model":"test-image-model","prompt":"a blue bird","n":1,"size":"auto","quality":"auto","output_format":"png"}));
                    Json(json!({"data":[{"b64_json":PNG}]}))
                }
            }))
            .route("/v1/chat/completions", post(|Json(body): Json<Value>| async move {
                assert!(body["tools"].as_array().expect("tools").iter().any(|t| t["function"]["name"] == "generate_image"));
                let result = body["messages"].as_array().expect("messages").iter().find(|m| m["role"] == "tool");
                let chunk = if let Some(result) = result {
                    let output: Value = serde_json::from_str(result["content"].as_str().expect("content")).expect("image output");
                    assert_eq!(output["media_type"], "image/png");
                    json!({"choices":[{"index":0,"delta":{"content":format!("![bird]({})", output["url"].as_str().expect("url"))},"finish_reason":"stop"}]})
                } else {
                    json!({"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"image-call","type":"function","function":{"name":"generate_image","arguments":"{\"prompt\":\"a blue bird\"}"}}]},"finish_reason":"tool_calls"}]})
                };
                ([(header::CONTENT_TYPE,"text/event-stream")], format!("data: {chunk}\n\ndata: [DONE]\n\n"))
            }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let base = format!("http://{}/v1/", listener.local_addr().expect("addr"));
        let upstream_task =
            tokio::spawn(async move { axum::serve(listener, upstream).await.expect("upstream") });
        let config = Config {
            context: None,
            workspace: None,
            graph: None,
            image_generation: Some(image_config(&base)),
            listen: "127.0.0.1:0".parse().expect("addr"),
            system_prompt: prompt(),
            model: serde_json::from_value(
                json!({"model":"mock","provider":{"type":"openai-compatible","base_url":base}}),
            )
            .expect("model"),
        };
        let dir = tempfile::tempdir().expect("temp");
        let app = build_app(config, Arc::new(FilesystemBlobStore::new(dir.path()))).expect("app");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let address = listener.local_addr().expect("address");
        let server =
            tokio::spawn(async move { axum::serve(listener, router(app)).await.expect("demo") });
        let client = reqwest::Client::new();
        let output = client
            .post(format!("http://{address}/chat"))
            .json(&request("image-turn"))
            .send()
            .await
            .expect("chat")
            .text()
            .await
            .expect("sse");
        assert!(output.contains("message-end"), "{output}");
        let events: Vec<Value> = output
            .lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .map(|line| serde_json::from_str(line).expect("event"))
            .collect();
        let text: String = events
            .iter()
            .filter(|event| event["type"] == "text-delta")
            .filter_map(|event| event["delta"].as_str())
            .collect();
        let path = text
            .strip_prefix("![bird](")
            .and_then(|s| s.strip_suffix(')'))
            .unwrap_or_else(|| panic!("image Markdown missing: {output}"));
        let image = client
            .get(format!("http://{address}{path}"))
            .send()
            .await
            .expect("image");
        assert_eq!(image.status(), StatusCode::OK);
        assert_eq!(image.headers()[header::CONTENT_TYPE], "image/png");
        assert_eq!(
            image.bytes().await.expect("png").as_ref(),
            STANDARD.decode(PNG).expect("png")
        );
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(
            client
                .get(format!("http://{address}/images/{}", BlobId::new()))
                .send()
                .await
                .expect("missing")
                .status(),
            StatusCode::NOT_FOUND
        );
        server.abort();
        upstream_task.abort();
    }

    #[tokio::test]
    async fn image_tool_bounds_errors_and_cancels_without_retry() {
        use agent_core::contracts::{Tool, ToolCallRequest};
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = calls.clone();
        let upstream = Router::new().route(
            "/v1/images/generations",
            post(move |Json(body): Json<Value>| {
                let observed = observed.clone();
                async move {
                    observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    match body["prompt"].as_str().expect("prompt") {
                        "error" => {
                            (StatusCode::BAD_GATEWAY, "secret upstream detail").into_response()
                        }
                        "large" => (
                            [(header::CONTENT_LENGTH, "30000000")],
                            axum::body::Body::from_stream(futures_util::stream::pending::<
                                Result<Vec<u8>, Infallible>,
                            >()),
                        )
                            .into_response(),
                        "slow" => {
                            tokio::time::sleep(Duration::from_secs(5)).await;
                            Json(json!({"data":[{"b64_json":PNG}]})).into_response()
                        }
                        _ => Json(json!({"data":[{"b64_json":"not-an-image"}]})).into_response(),
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let base = format!("http://{}/v1/", listener.local_addr().expect("addr"));
        let server =
            tokio::spawn(async move { axum::serve(listener, upstream).await.expect("upstream") });
        let dir = tempfile::tempdir().expect("temp");
        let tool = GenerateImageTool::new(
            image_config(&base),
            Arc::new(FilesystemBlobStore::new(dir.path())),
            "/images",
        )
        .expect("tool");
        let call = |arguments, cancellation| ToolCallRequest {
            run_id: RunId::new(),
            call_id: "image-call".into(),
            name: "generate_image".into(),
            arguments,
            cancellation,
        };
        for (prompt, code) in [
            ("error", "image_provider"),
            ("invalid", "image_response"),
            ("large", "image_response_limit"),
        ] {
            let error = tool
                .call(call(json!({"prompt":prompt}), RunCancellation::new()))
                .await
                .expect_err("failure");
            assert_eq!(error.code(), code);
            assert!(!error.safe_message().contains("secret"));
            assert!(!error.retryable());
        }
        let before = calls.load(std::sync::atomic::Ordering::SeqCst);
        for args in [
            json!({"prompt":" "}),
            json!({"prompt":"bird","model":"override"}),
        ] {
            assert_eq!(
                tool.call(call(args, RunCancellation::new()))
                    .await
                    .expect_err("invalid")
                    .code(),
                "invalid_tool_arguments"
            );
        }
        let cancellation = RunCancellation::new();
        cancellation.cancel();
        assert_eq!(
            tool.call(call(json!({"prompt":"slow"}), cancellation))
                .await
                .expect_err("cancel")
                .code(),
            "tool_cancelled"
        );
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), before);
        let mut config = image_config(&base);
        config.timeout_ms = 40;
        let timed = GenerateImageTool::new(
            config,
            Arc::new(FilesystemBlobStore::new(dir.path())),
            "/images",
        )
        .expect("tool");
        assert_eq!(
            timed
                .call(call(json!({"prompt":"slow"}), RunCancellation::new()))
                .await
                .expect_err("timeout")
                .code(),
            "image_timeout"
        );
        let cancellation = RunCancellation::new();
        let pending = tool.call(call(json!({"prompt":"slow"}), cancellation.clone()));
        let (result, ()) = tokio::join!(pending, async {
            tokio::time::sleep(Duration::from_millis(40)).await;
            cancellation.cancel();
        });
        assert_eq!(result.expect_err("cancel").code(), "tool_cancelled");
        server.abort();
    }

    #[test]
    fn validates_files_and_config() {
        let mut config: Config =
            toml::from_str(include_str!("../../../config/agent-chat-demo.example.toml"))
                .expect("config");
        config.model.normalize_and_validate("model").expect("valid");
        let mut value = request("t");
        value["input"][0]["parts"] = json!([{"type":"file","url":"data:image/png;base64,iVBORw0KGgo=","mediaType":"image/png"}]);
        let (_, files) =
            parse_input(&serde_json::from_value(value.clone()).expect("request")).expect("file");
        assert_eq!(files[0].data, b"\x89PNG\r\n\x1a\n");
        for url in [
            "blob:local",
            "https://example.com/file",
            "data:image/png;base64,aGVsbG8=",
        ] {
            value["input"][0]["parts"][0]["url"] = json!(url);
            assert!(parse_input(&serde_json::from_value(value.clone()).expect("request")).is_err());
        }
    }
    #[tokio::test]
    async fn operator_http_multiturn_files_limits_and_errors() {
        let received = Arc::new(Mutex::new(Vec::<Value>::new()));
        let records = Arc::clone(&received);
        let mock = Router::new().route("/v1/chat/completions", post(move |Json(body): Json<Value>| {
            let records = Arc::clone(&records);
            async move {
                records.lock().expect("records").push(body.clone());
                if body["messages"].to_string().contains("fail-test") { return StatusCode::BAD_GATEWAY.into_response(); }
                let chunks = [json!({"id":"mock","choices":[{"index":0,"delta":{"role":"assistant","content":"Hello demo"},"finish_reason":null}]}), json!({"id":"mock","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":3,"total_tokens":13}})];
                let mut body = chunks.iter().map(|c| format!("data: {c}\n\n")).collect::<String>();
                body.push_str("data: [DONE]\n\n");
                ([(header::CONTENT_TYPE, "text/event-stream")], body).into_response()
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("mock bind");
        let address = listener.local_addr().expect("address");
        let mock_task = tokio::spawn(async move {
            axum::serve(listener, mock).await.expect("mock");
        });
        let config = Config { context: None, workspace: None, image_generation: None, graph: None, listen: "127.0.0.1:0".parse().expect("address"), system_prompt: prompt(), model: serde_json::from_value(json!({"model":"mock","provider":{"type":"openai-compatible","base_url":format!("http://{address}/v1/")}})).expect("model") };
        let temp = tempfile::tempdir().expect("temp");
        let app = build_app(config, Arc::new(FilesystemBlobStore::new(temp.path()))).expect("app");
        let held = chat(
            State(app.clone()),
            HeaderMap::new(),
            Json(serde_json::from_value(request("held")).expect("request")),
        )
        .await;
        assert_eq!(held.status(), StatusCode::OK);
        assert_eq!(
            chat(
                State(app.clone()),
                HeaderMap::new(),
                Json(serde_json::from_value(request("busy")).expect("request"))
            )
            .await
            .status(),
            StatusCode::CONFLICT
        );
        drop(held);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let base = format!("http://{}", listener.local_addr().expect("address"));
        let server = tokio::spawn(async move {
            axum::serve(listener, router(app)).await.expect("server");
        });
        let client = reqwest::Client::new();
        let image = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYII=";
        let mut first = request("first");
        first["input"][0]["parts"]
            .as_array_mut()
            .expect("parts")
            .push(json!({"type":"file","url":image,"mediaType":"image/png","name":"pixel.png"}));
        for value in [&first, &request("second")] {
            let response = client
                .post(format!("{base}/chat"))
                .json(value)
                .send()
                .await
                .expect("request");
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                response.headers()[header::CACHE_CONTROL],
                "no-cache, no-transform"
            );
            let body = response.text().await.expect("stream");
            let events: Vec<Value> = body
                .lines()
                .filter_map(|l| l.strip_prefix("data: "))
                .map(|v| serde_json::from_str(v).expect("JSON"))
                .collect();
            assert_eq!(events.first().expect("hello")["type"], "server-hello");
            assert_eq!(
                events.last().expect("terminal")["finishReason"],
                "stop",
                "{body}"
            );
            assert!(events.iter().any(|event| event["delta"] == "Hello demo"));
            assert_eq!(events.last().unwrap()["usage"]["totalTokens"], 13);
            let ids: Vec<_> = body
                .lines()
                .filter_map(|l| l.strip_prefix("id: "))
                .collect();
            for (index, event) in events.iter().enumerate() {
                assert_eq!(event["eventId"], (index + 1).to_string());
                assert_eq!(event["eventId"], ids[index]);
            }
        }
        {
            let records = received.lock().expect("records");
            let messages = records[1]["messages"].as_array().expect("messages");
            assert_eq!(messages.iter().filter(|m| m["role"] != "system").count(), 3);
            assert!(
                messages
                    .iter()
                    .any(|m| m["role"] == "assistant" && m["content"] == "Hello demo")
            );
            assert!(messages.iter().any(|m| m.to_string().contains(image)));
        }
        assert_eq!(
            client
                .post(format!("{base}/chat"))
                .json(&first)
                .send()
                .await
                .expect("duplicate")
                .status(),
            StatusCode::CONFLICT
        );
        assert_eq!(
            client
                .post(format!("{base}/chat"))
                .header("Last-Event-ID", "1")
                .json(&first)
                .send()
                .await
                .expect("resume")
                .status(),
            StatusCode::CONFLICT
        );
        let mut isolated = request("isolated");
        isolated["conversationId"] = json!("other");
        client
            .post(format!("{base}/chat"))
            .json(&isolated)
            .send()
            .await
            .expect("isolated")
            .text()
            .await
            .expect("stream");
        assert_eq!(
            received.lock().expect("records")[2]["messages"]
                .as_array()
                .expect("messages")
                .iter()
                .filter(|m| m["role"] != "system")
                .count(),
            1
        );
        let mut invalid = request("invalid");
        invalid["protocol"] = json!("unsupported");
        let mut body = invalid.to_string();
        body.extend(std::iter::repeat_n(' ', MAX_BODY - body.len()));
        assert_eq!(
            client
                .post(format!("{base}/chat"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(body.clone())
                .send()
                .await
                .expect("limit")
                .status(),
            StatusCode::UPGRADE_REQUIRED
        );
        body.push(' ');
        assert_eq!(
            client
                .post(format!("{base}/chat"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(body)
                .send()
                .await
                .expect("over limit")
                .status(),
            StatusCode::PAYLOAD_TOO_LARGE
        );
        let mut failed = request("failed");
        failed["input"][0]["parts"][0]["text"] = json!("fail-test");
        let body = client
            .post(format!("{base}/chat"))
            .json(&failed)
            .send()
            .await
            .expect("failure")
            .text()
            .await
            .expect("stream");
        let events: Vec<Value> = body
            .lines()
            .filter_map(|l| l.strip_prefix("data: "))
            .map(|v| serde_json::from_str(v).expect("JSON"))
            .collect();
        assert_eq!(events.last().expect("error")["type"], "error");
        assert_eq!(events.last().expect("error")["scope"], "turn");
        assert!(!events.iter().any(|v| v["type"] == "message-end"));
        assert_eq!(
            client
                .delete(format!("{base}/turns/failed"))
                .send()
                .await
                .expect("cancel")
                .status(),
            StatusCode::NO_CONTENT
        );
        server.abort();
        mock_task.abort();
    }
    #[tokio::test]
    async fn streams_each_chunk_before_upstream_can_finish() {
        let advance = Arc::new(tokio::sync::Semaphore::new(0));
        let permits = Arc::clone(&advance);
        let upstream = Router::new().route("/v1/chat/completions", post(move || {
            let permits = Arc::clone(&permits);
            async move {
                let stream = async_stream::stream! {
                    for index in 0..96 {
                        let word = format!("fragment-{index:04}-live");
                        yield Ok::<_, Infallible>(Event::default().data(json!({"id":"mock","choices":[{"index":0,"delta":{"content":word},"finish_reason":null}]}).to_string()));
                        // The upstream cannot emit another fragment or finish until
                        // the HTTP client has observed this exact fragment.
                        permits.acquire().await.expect("permit").forget();
                    }
                    yield Ok(Event::default().data(json!({"id":"mock","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}).to_string()));
                    yield Ok(Event::default().data("[DONE]"));
                };
                Sse::new(stream)
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("address");
        let upstream_task = tokio::spawn(async move {
            axum::serve(listener, upstream).await.expect("upstream");
        });
        let config = Config { context: None, workspace: None, image_generation: None, graph: None, listen: "127.0.0.1:0".parse().expect("address"), system_prompt: prompt(), model: serde_json::from_value(json!({"model":"mock","provider":{"type":"openai-compatible","base_url":format!("http://{addr}/v1/")}})).expect("model") };
        let temp = tempfile::tempdir().expect("temp");
        let app = build_app(config, Arc::new(FilesystemBlobStore::new(temp.path()))).expect("app");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            axum::serve(listener, router(app)).await.expect("server");
        });
        let response = reqwest::Client::new()
            .post(format!("http://{addr}/chat"))
            .json(&request("stream-test"))
            .send()
            .await
            .expect("request");
        assert_eq!(response.headers()["x-accel-buffering"], "no");
        let mut chunks = response.bytes_stream();
        let mut body = String::new();
        for index in 0..96 {
            let word = format!("fragment-{index:04}-live");
            let started = std::time::Instant::now();
            tokio::time::timeout(Duration::from_secs(2), async {
                while !body.contains(&word) {
                    let chunk = chunks
                        .next()
                        .await
                        .expect("stream must remain open")
                        .expect("chunk");
                    body.push_str(std::str::from_utf8(&chunk).expect("UTF8"));
                }
            })
            .await
            .expect("a fragment must reach the HTTP client before upstream completion");
            eprintln!("{word} reached client in {:?}", started.elapsed());
            assert!(!body.contains("message-end"));
            advance.add_permits(1);
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            while let Some(chunk) = chunks.next().await {
                body.push_str(std::str::from_utf8(&chunk.expect("chunk")).expect("UTF8"));
            }
        })
        .await
        .expect("terminal stream");
        assert!(body.contains("message-end"));
        server.abort();
        upstream_task.abort();
    }
}
