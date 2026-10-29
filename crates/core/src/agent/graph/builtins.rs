use super::*;
use crate::contracts::*;
use futures_util::StreamExt;
use serde_json::json;
mod compactor;
mod flow;
mod policy;

pub(super) fn object_schema() -> Value {
    json!({"type":"object"})
}
fn protocol(event: &str) -> Value {
    match event {
        "agent.start.v1" => {
            json!({"anyOf":[{"type":"string","minLength":1},{"type":"object","additionalProperties":false,"required":["input"],"properties":{"input":{"type":"string"},"attachments":{"type":"array"},"prior_messages":{"type":"array"}}}]})
        }
        "approval.decided.v1" => {
            json!({"type":"object","additionalProperties":false,"required":["approval_id","actor","binding_digest","resolution"],"properties":{
            "approval_id":{"type":"string","format":"uuid"},"actor":{"type":"string","minLength":1},"binding_digest":{"type":"string","pattern":"^sha256:[0-9a-f]{64}$"},
            "resolution":{"type":"object","additionalProperties":false,"required":["decision"],"properties":{"decision":{"enum":["allow-once","deny"]},"reason":{"type":"string"}}}}})
        }
        "approval.expire.v1" => {
            json!({"type":"object","additionalProperties":false,"required":["approval_id"],"properties":{"approval_id":{"type":"string","format":"uuid"}}})
        }
        "context.prepare.requested.v1" => {
            json!({"type":"object","required":["prepare_id","source_digest","round","messages","tools","current_user_index"],"properties":{"prepare_id":{"type":"string","minLength":1},"source_digest":{"type":"string","minLength":1},"round":{"type":"integer","minimum":1},"messages":{"type":"array"},"tools":{"type":"array"},"current_user_index":{"type":"integer","minimum":0}}})
        }
        "context.prepared.v1" => {
            json!({"type":"object","required":["prepare_id","source_digest","round"],"properties":{"prepare_id":{"type":"string","minLength":1},"source_digest":{"type":"string","minLength":1},"round":{"type":"integer","minimum":1},"messages":{"type":"array"},"current_user_index":{"type":"integer","minimum":0},"code":{"type":"string","minLength":1}},"oneOf":[{"required":["messages","current_user_index"],"not":{"required":["code"]}},{"required":["code"],"not":{"required":["messages"]}}]})
        }
        "model.requested.v1" => {
            json!({"type":"object","required":["round","messages","tools"],"properties":{"round":{"type":"integer","minimum":1},"messages":{"type":"array"},"tools":{"type":"array"}}})
        }
        "gate.open.v1" | "stop.tool_calls.v1" => {
            json!({"type":"object","required":["batch_id","items","context"],"properties":{"batch_id":{"type":"string","minLength":1,"maxLength":128},"context":{},"items":{"type":"array","minItems":1,"maxItems":128,"items":{"type":"object","required":["id","payload"],"properties":{"id":{"type":"string","minLength":1,"maxLength":128},"payload":{}}}}}})
        }
        "gate.dispatch.v1" | "gate.result.v1" => {
            json!({"type":"object","required":["batch_id","id","payload"],"properties":{"batch_id":{"type":"string","minLength":1,"maxLength":128},"id":{"type":"string","minLength":1,"maxLength":128},"payload":{}}})
        }
        "gate.completed.v1" => {
            json!({"type":"object","required":["batch_id","context","results"],"properties":{"batch_id":{"type":"string"},"context":{},"results":{"type":"array","items":{"type":"object","required":["id","payload"],"properties":{"id":{"type":"string"},"payload":{}}}}}})
        }
        "model.output.v1" => {
            json!({"type":"object","required":["round","text"],"properties":{"round":{"type":"integer","minimum":1},"text":{"type":"string"}}})
        }
        "stop.failed.v1" | "context.failed.v1" => {
            json!({"type":"object","required":["round","code"],"properties":{"round":{"type":"integer","minimum":1},"code":{"type":"string"}}})
        }
        "stop.completed.v1" => {
            json!({"type":"object","required":["output"],"properties":{"output":{"type":"string"}}})
        }
        "agent.failed.v1" => {
            json!({"type":"object","required":["code"],"properties":{"code":{"type":"string"}}})
        }
        _ => object_schema(),
    }
}
pub(super) fn descriptor(
    kind: &str,
    inputs: &[&str],
    outputs: &[&str],
    config_schema: Value,
    replay_safe: bool,
) -> OperatorDescriptor {
    OperatorDescriptor {
        kind: kind.into(),
        version: 1,
        config_schema,
        inputs: inputs
            .iter()
            .map(|s| (s.to_string(), protocol(s)))
            .collect(),
        outputs: outputs
            .iter()
            .map(|s| (s.to_string(), protocol(s)))
            .collect(),
        resources: BTreeMap::new(),
        replay_safe,
        stateless_parallel: false,
    }
}
pub(super) fn registry() -> OperatorRegistry {
    let mut registry = OperatorRegistry::default();
    flow::register(&mut registry);
    compactor::register(&mut registry);
    policy::register(&mut registry);
    super::waits::register(&mut registry);
    let mut model = descriptor(
        "agent.model",
        &["model.requested.v1", "context.failed.v1"],
        &[
            "model.output.v1",
            "stop.completed.v1",
            "stop.tool_calls.v1",
            "stop.failed.v1",
            "stop.cancelled.v1",
        ],
        json!({"type":"object","additionalProperties":false,"required":["model","timeout_ms"],"properties":{"model":{"type":"string","minLength":1},"model_name":{"type":"string","minLength":1},"timeout_ms":{"type":"integer","minimum":1,"maximum":86400000},"max_output_tokens":{"type":"integer","minimum":1}}}),
        false,
    );
    model.resources.insert("model".into(), ResourceKind::Model);
    registry
        .register(model, |config, resources| {
            let config: ModelConfig =
                serde_json::from_value(config.clone()).map_err(|e| invalid("config", e))?;
            let port = resources.get_model(&config.model)?;
            Ok(Arc::new(Model { config, port }))
        })
        .expect("model registration");
    let mut tools = descriptor(
        "agent.tools",
        &["gate.dispatch.v1", "tool.authorized.v1"],
        &["gate.result.v1", "wait.requested.v1"],
        json!({"type":"object","additionalProperties":false,"required":["toolset","timeout_ms"],"properties":{"toolset":{"type":"string","minLength":1},"timeout_ms":{"type":"integer","minimum":1,"maximum":86400000},"allowed_tools":{"type":"array","items":{"type":"string"},"uniqueItems":true},"review_level":{"type":"integer","minimum":0,"maximum":100},"allow_run_adf":{"type":"boolean"},"allow_suspend":{"type":"boolean"},"parallel":{"type":"boolean"}}}),
        false,
    );
    tools.stateless_parallel = true;
    tools
        .resources
        .insert("toolset".into(), ResourceKind::Toolset);
    registry
        .register(tools, |config, resources| {
            let config: ToolsConfig =
                serde_json::from_value(config.clone()).map_err(|e| invalid("config", e))?;
            let port = resources.get_toolset(&config.toolset)?;
            Ok(Arc::new(Tools {
                scheduling: resources.toolsets[&config.toolset].scheduling.clone(),
                config,
                port,
            }))
        })
        .expect("tools registration");
    registry
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ModelConfig {
    model: String,
    model_name: Option<String>,
    timeout_ms: u64,
    max_output_tokens: Option<u32>,
}
struct Model {
    config: ModelConfig,
    port: Arc<dyn ModelPort>,
}
impl GraphOperator for Model {
    fn handle(&self, context: GraphContext) -> GraphFuture<'_> {
        Box::pin(async move {
            let round = context.event.payload["round"].clone();
            if context.event.kind == "context.failed.v1" {
                return Ok(GraphOutput::new(
                    context.state,
                    vec![GraphEvent::new("stop.failed.v1", context.event.payload)],
                ));
            }
            if context.state["last_round"]
                .as_u64()
                .is_some_and(|last| round.as_u64().is_some_and(|next| next <= last))
            {
                return Ok(GraphOutput::new(context.state, vec![]));
            }
            let tools_enabled = context.event.payload["tools"]
                .as_array()
                .is_some_and(|v| !v.is_empty());
            let request = ModelRequest {
                run_id: context.run_id,
                model: self
                    .config
                    .model_name
                    .clone()
                    .unwrap_or_else(|| self.config.model.clone()),
                messages: serde_json::from_value(context.event.payload["messages"].clone())
                    .map_err(|e| invalid("messages", e))?,
                tools: serde_json::from_value(context.event.payload["tools"].clone())
                    .map_err(|e| invalid("tools", e))?,
                max_output_tokens: self.config.max_output_tokens,
            };
            let mut usage = None;
            let mut failure = None;
            let result = tokio::time::timeout(
                std::time::Duration::from_millis(self.config.timeout_ms),
                async {
                    let mut stream = self.port.stream(request);
                    let mut text = String::new();
                    let mut calls: Vec<ModelToolCall> = vec![];
                    let mut bytes = 0usize;
                    while let Some(event) = stream.next().await {
                        match event {
                            ModelEvent::TextDelta { delta } => {
                                bytes = bytes.saturating_add(delta.len());
                                text.push_str(&delta);
                            }
                            ModelEvent::ToolCallStarted { call_id, name } => {
                                if call_id.is_empty()
                                    || call_id.len() > 128
                                    || calls.len() >= 128
                                    || calls.iter().any(|c| c.id == call_id)
                                {
                                    return Err("invalid_tool_calls");
                                }
                                calls.push(ModelToolCall {
                                    id: call_id,
                                    name,
                                    arguments: String::new(),
                                });
                            }
                            ModelEvent::ToolCallArgumentsDelta { call_id, delta } => {
                                bytes = bytes.saturating_add(delta.len());
                                let Some(call) = calls.iter_mut().find(|c| c.id == call_id) else {
                                    return Err("invalid_tool_calls");
                                };
                                call.arguments.push_str(&delta);
                            }
                            // Providers report a cumulative snapshot for this call.
                            ModelEvent::Usage { usage: reported } => usage = Some(reported),
                            ModelEvent::Failed { error } => {
                                failure = Some(error);
                                return Err("model_failed");
                            }
                            ModelEvent::Completed { finish_reason } => {
                                return Ok((text, calls, finish_reason));
                            }
                            _ => {}
                        }
                        if bytes > 512 * 1024 {
                            return Err("output_capacity");
                        }
                    }
                    Err("missing_model_terminal")
                },
            )
            .await;
            let events = match result {
                Ok(Ok((text, calls, reason))) => {
                    let stop = if matches!(
                        reason,
                        FinishReason::Length | FinishReason::ContentFilter
                    ) {
                        GraphEvent::new(
                            "stop.failed.v1",
                            json!({"round":round,"code":"model_stopped_early","finish_reason":reason,"usage":usage}),
                        )
                    } else if calls.is_empty() && reason == FinishReason::ToolCall {
                        GraphEvent::new(
                            "stop.failed.v1",
                            json!({"round":round,"code":"missing_tool_calls","usage":usage}),
                        )
                    } else if calls.is_empty() {
                        GraphEvent::new(
                            "stop.completed.v1",
                            json!({"output":text,"round":round,"finish_reason":reason,"usage":usage}),
                        )
                    } else if !tools_enabled {
                        GraphEvent::new(
                            "stop.failed.v1",
                            json!({"round":round,"code":"tools_disabled","usage":usage}),
                        )
                    } else {
                        let items: Vec<_> = calls
                            .iter()
                            .map(|call| json!({"id":call.id,"payload":call}))
                            .collect();
                        GraphEvent::new(
                            "stop.tool_calls.v1",
                            json!({"batch_id":format!("{}:{}",context.instance,round),"items":items,"context":{"round":round,"text":text,"calls":calls,"tool_snapshot":context.event.payload["tool_snapshot"]},"usage":usage}),
                        )
                    };
                    vec![
                        GraphEvent::new("model.output.v1", json!({"round":round,"text":text})),
                        stop,
                    ]
                }
                Ok(Err(code)) => vec![GraphEvent::new(
                    "stop.failed.v1",
                    json!({"round":round,"code":failure.as_ref().map_or(code, |e: &ModelError| e.kind().code()),
                        "message":failure.as_ref().map_or(code, |e| e.safe_message()),
                        "retryable":failure.as_ref().is_some_and(ModelError::retryable),"usage":usage}),
                )],
                Err(_) => vec![GraphEvent::new(
                    "stop.failed.v1",
                    json!({"round":round,"code":"model_timeout","usage":usage}),
                )],
            };
            Ok(GraphOutput::new(json!({"last_round":round}), events).with_usage(usage))
        })
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ToolsConfig {
    toolset: String,
    timeout_ms: u64,
    allowed_tools: Option<Vec<String>>,
    #[serde(default = "tool_review_default")]
    review_level: u8,
    #[serde(default)]
    allow_run_adf: bool,
    #[serde(default)]
    allow_suspend: bool,
    #[serde(default)]
    parallel: bool,
}
fn tool_review_default() -> u8 {
    ToolApprovalPolicy::DEFAULT_REVIEW_LEVEL
}
struct Tools {
    scheduling: Arc<tokio::sync::RwLock<()>>,
    config: ToolsConfig,
    port: Arc<dyn ToolPort>,
}
impl GraphOperator for Tools {
    fn handle(&self, context: GraphContext) -> GraphFuture<'_> {
        Box::pin(async move {
            let dispatch = &context.event.payload;
            let invoke = async {
                let call = policy::validate(dispatch, self.config.allowed_tools.as_deref(), self.config.allow_run_adf)
                    .map_err(|_| "invalid_tool_call")?;
                let approval: Option<crate::tool_authorization::BoundApproval> = if context.event.kind == "tool.authorized.v1" {
                    let recorded: crate::tool_authorization::ValidatedToolCall = serde_json::from_value(dispatch["authorization"]["call"].clone()).map_err(|_| "invalid_authorization")?;
                    if recorded != call || context.event.from == "$external" { return Err("invalid_authorization"); }
                    serde_json::from_value(dispatch["authorization"]["approval"].clone()).map_err(|_| "invalid_authorization")?
                } else { None };
                if call.invocation.execution.completion != ToolCompletion::Immediate && !self.config.allow_suspend {
                    return Err("suspending_tool_requires_wait_protocol");
                }
                let policy = call.invocation.execution;
                let parallel = self.config.parallel && policy.concurrency == ToolConcurrency::ParallelSafe && policy.completion == ToolCompletion::Immediate;
                let _permit = if parallel {
                    futures_util::future::Either::Left(self.scheduling.read().await)
                } else {
                    futures_util::future::Either::Right(self.scheduling.write().await)
                };
                for attempt in 1..=policy.retry.max_attempts {
                    if context.cancellation.is_cancelled() { return Err("tool_cancelled"); }
                    if ToolApprovalPolicy::new(self.config.review_level).requires_review(call.invocation.risk_level) {
                        let approval = approval.as_ref().ok_or("approval_required")?;
                        if approval.run_id != context.run_id { return Err("invalid_authorization"); }
                        approval.authorize(&call, crate::agent::unix_time_ms()).map_err(|_| "approval_invalid")?;
                    }
                    self.port.validate_at(context.run_id, call.tool_set_revision, &call.invocation.name, &call.invocation.arguments).map_err(|_| "invalid_arguments")?;
                    let cancellation = context.cancellation.child();
                    let request = ToolCallRequest {
                        run_id: context.run_id, call_id: call.invocation.call_id.clone(),
                        name: call.invocation.name.clone(), arguments: call.invocation.arguments.clone(),
                        cancellation: cancellation.clone(),
                    };
                    let output = match tokio::time::timeout(
                        std::time::Duration::from_millis(self.config.timeout_ms),
                        self.port.call_at(call.tool_set_revision, request),
                    ).await {
                        Ok(Ok(output)) => output,
                        Ok(Err(error)) if error.retryable() && policy.idempotency.permits_automatic_retry() && attempt < policy.retry.max_attempts => {
                            let delay = error.retry_after_ms().unwrap_or_else(|| policy.retry.delay_for_attempt(attempt)).min(policy.retry.max_backoff_ms);
                            tokio::select! {
                                _ = context.cancellation.cancelled() => return Err("tool_cancelled"),
                                _ = tokio::time::sleep(std::time::Duration::from_millis(delay)) => {}
                            }
                            continue;
                        }
                        Ok(Err(_)) => return Err("tool_failed"),
                        Err(_) => { cancellation.cancel(); return Err("tool_timeout"); }
                    };
                    if let Some(suspension) = &output.suspension {
                        if !self.config.allow_suspend || policy.completion != ToolCompletion::MaySuspend { return Err("invalid_tool_suspension"); }
                        super::waits::validate_suspension(context.run_id, suspension).map_err(|_| "invalid_tool_suspension")?;
                    }
                    if output.content.len() > 512 * 1024 { return Err("output_capacity"); }
                    return Ok(output);
                }
                Err("tool_attempts_exhausted")
            }.await;
            let event = match invoke {
                Ok(output) => match output.suspension {
                    Some(suspension) => GraphEvent::new(
                        "wait.requested.v1",
                        json!({"dispatch":dispatch,"suspension":suspension}),
                    ),
                    None => GraphEvent::new(
                        "gate.result.v1",
                        json!({"batch_id":dispatch["batch_id"],"id":dispatch["id"],"payload":{"ok":true,"output":output.content,"attachments":output.attachments}}),
                    ),
                },
                Err(code) => policy::failure(dispatch, code, code),
            };
            Ok(GraphOutput::new(Value::Null, vec![event]))
        })
    }
}

pub(super) fn parallel_tool_batch(config: &Value, dispatch: &Value) -> bool {
    if config["parallel"] != true {
        return false;
    }
    let Ok(snapshot) =
        serde_json::from_value::<ToolSetSnapshot>(dispatch["context"]["tool_snapshot"].clone())
    else {
        return false;
    };
    let Ok(calls) =
        serde_json::from_value::<Vec<ModelToolCall>>(dispatch["context"]["calls"].clone())
    else {
        return false;
    };
    let review = ToolApprovalPolicy::new(config["review_level"].as_u64().unwrap_or(50) as u8);
    !calls.is_empty()
        && calls.iter().all(|call| {
            snapshot
                .definitions
                .iter()
                .find(|d| d.name == call.name)
                .is_some_and(|definition| {
                    definition.execution.concurrency == ToolConcurrency::ParallelSafe
                        && definition.execution.completion == ToolCompletion::Immediate
                        && !review.requires_review(definition.risk_level)
                })
        })
}
