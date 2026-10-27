//! Conversation context and domain-independent all-of fan-out/join gate.
use super::*;

pub(super) fn register(registry: &mut OperatorRegistry) {
    let mut context = descriptor(
        "agent.context",
        &["agent.start.v1", "gate.completed.v1"],
        &["model.requested.v1", "context.failed.v1"],
        json!({"type":"object","additionalProperties":false,"required":["system_prompt","max_steps"],"properties":{"system_prompt":{"type":"string"},"max_steps":{"type":"integer","minimum":1,"maximum":100},"toolset":{"type":"string","minLength":1},"allowed_tools":{"type":"array","items":{"type":"string"},"uniqueItems":true},"allow_run_adf":{"type":"boolean"}}}),
        true,
    );
    context
        .resources
        .insert("toolset".into(), ResourceKind::Toolset);
    registry
        .register(context, |config, resources| {
            Ok(Arc::new(Context {
                config: serde_json::from_value(config.clone()).map_err(|e| invalid("config", e))?,
                resources: resources.clone(),
            }))
        })
        .expect("context registration");
    registry
        .register(
            descriptor(
                "agent.gate",
                &["gate.open.v1", "stop.tool_calls.v1", "gate.result.v1"],
                &["gate.dispatch.v1", "gate.completed.v1"],
                json!({"type":"object","additionalProperties":false}),
                true,
            ),
            |_, _| Ok(Arc::new(Gate)),
        )
        .expect("gate registration");
}
fn state<T: serde::de::DeserializeOwned + Default>(value: Value) -> Result<T, FabricError> {
    if value.is_null() {
        Ok(T::default())
    } else {
        serde_json::from_value(value).map_err(|e| invalid("state", e))
    }
}
fn output(value: impl Serialize, events: Vec<GraphEvent>) -> Result<GraphOutput, FabricError> {
    Ok(GraphOutput::new(
        serde_json::to_value(value).map_err(|e| invalid("state", e))?,
        events,
    ))
}
fn round(payload: &Value) -> Result<u32, FabricError> {
    payload["round"]
        .as_u64()
        .and_then(|v| u32::try_from(v).ok())
        .filter(|v| *v > 0)
        .ok_or_else(|| invalid("round", "positive u32 required"))
}
#[derive(Deserialize)]
struct ContextConfig {
    system_prompt: String,
    max_steps: u32,
    toolset: Option<String>,
    allowed_tools: Option<Vec<String>>,
    #[serde(default)]
    allow_run_adf: bool,
}
#[derive(Default, Serialize, Deserialize)]
struct ContextState {
    messages: Vec<ModelMessage>,
    round: u32,
}
struct Context {
    config: ContextConfig,
    resources: AgentResources,
}
impl GraphOperator for Context {
    fn handle(&self, ctx: GraphContext) -> GraphFuture<'_> {
        Box::pin(async move {
            let mut saved: ContextState = state(ctx.state)?;
            match ctx.event.kind.as_str() {
                "agent.start.v1" if saved.round == 0 => {
                    #[derive(Deserialize)]
                    #[serde(deny_unknown_fields)]
                    struct Input {
                        input: String,
                        #[serde(default)]
                        attachments: Vec<ModelAttachment>,
                        #[serde(default)]
                        prior_messages: Vec<ModelMessage>,
                    }
                    let input = if let Some(text) = ctx.event.payload.as_str() {
                        Input {
                            input: text.into(),
                            attachments: vec![],
                            prior_messages: vec![],
                        }
                    } else {
                        serde_json::from_value(ctx.event.payload.clone())
                            .map_err(|e| invalid("input", e))?
                    };
                    if input.input.trim().is_empty() && input.attachments.is_empty() {
                        return Err(invalid("input", "text or attachments required"));
                    }
                    saved.messages = vec![ModelMessage::system(&self.config.system_prompt)];
                    saved.messages.extend(input.prior_messages);
                    saved.messages.push(ModelMessage::user_with_attachments(
                        input.input,
                        input.attachments,
                    ));
                }
                "gate.completed.v1"
                    if round(&ctx.event.payload["context"])? == saved.round && saved.round > 0 =>
                {
                    let batch = &ctx.event.payload["context"];
                    let calls: Vec<ModelToolCall> = serde_json::from_value(batch["calls"].clone())
                        .map_err(|e| invalid("calls", e))?;
                    let results = ctx.event.payload["results"]
                        .as_array()
                        .ok_or_else(|| invalid("results", "required"))?;
                    if calls.len() != results.len() {
                        return Err(invalid("results", "incomplete tool batch"));
                    }
                    let mut assistant =
                        ModelMessage::user(batch["text"].as_str().unwrap_or_default());
                    assistant.role = ModelRole::Assistant;
                    assistant.tool_calls = calls.clone();
                    saved.messages.push(assistant);
                    for (call, result) in calls.iter().zip(results) {
                        if result["id"].as_str() != Some(&call.id) {
                            return Err(invalid("results", "call order mismatch"));
                        }
                        let mut message = ModelMessage::user(
                            result["payload"]["output"]
                                .as_str()
                                .ok_or_else(|| invalid("result.output", "required"))?,
                        );
                        message.role = ModelRole::Tool;
                        message.tool_call_id = Some(call.id.clone());
                        saved.messages.push(message);
                    }
                }
                _ => return output(saved, vec![]),
            }
            if saved.round >= self.config.max_steps {
                let event = GraphEvent::new(
                    "context.failed.v1",
                    json!({"round":saved.round+1,"code":"max_steps_exceeded"}),
                );
                return output(saved, vec![event]);
            }
            saved.round += 1;
            let snapshot = if let Some(name) = &self.config.toolset {
                Some(
                    self.resources
                        .get_toolset(name)?
                        .tool_set_snapshot(ctx.run_id)
                        .map_err(|e| invalid("toolset", e))?
                        .retain(|binding| {
                            let allowed = self
                                .config
                                .allowed_tools
                                .as_ref()
                                .is_none_or(|names| names.contains(&binding.name));
                            match binding.kind {
                                ToolBindingKind::Static => allowed,
                                ToolBindingKind::AdfManagement => {
                                    allowed && self.config.allow_run_adf
                                }
                                ToolBindingKind::AgentDefined => self.config.allow_run_adf,
                            }
                        })
                        .map_err(|e| invalid("toolset", e))?,
                )
            } else {
                None
            };
            let tools = snapshot
                .as_ref()
                .map(|s| s.definitions.clone())
                .unwrap_or_default();
            let event = GraphEvent::new(
                "model.requested.v1",
                json!({"round":saved.round,"messages":saved.messages,"tools":tools,"tool_snapshot":snapshot}),
            );
            output(saved, vec![event])
        })
    }
}

#[derive(Default, Serialize, Deserialize)]
struct GateState {
    batches: BTreeMap<String, Batch>,
}
#[derive(Serialize, Deserialize)]
struct Batch {
    open: Value,
    results: BTreeMap<String, Value>,
    completed: bool,
}
struct Gate;
impl GraphOperator for Gate {
    fn handle(&self, ctx: GraphContext) -> GraphFuture<'_> {
        Box::pin(async move {
            let mut saved: GateState = state(ctx.state)?;
            let payload = &ctx.event.payload;
            let batch_id = payload["batch_id"]
                .as_str()
                .ok_or_else(|| invalid("batch_id", "required"))?;
            if ctx.event.kind != "gate.result.v1" {
                if let Some(batch) = saved.batches.get(batch_id) {
                    if batch.open != *payload {
                        return Err(invalid("batch_id", "conflicting batch"));
                    }
                    return output(saved, vec![]);
                }
                let items = payload["items"]
                    .as_array()
                    .ok_or_else(|| invalid("items", "required"))?;
                let ids: BTreeSet<_> = items.iter().map(|item| item["id"].as_str()).collect();
                if items.is_empty()
                    || items.len() > 128
                    || ids.contains(&None)
                    || ids.len() != items.len()
                {
                    return Err(invalid("items", "nonempty unique branch IDs required"));
                }
                let events = items
                    .iter()
                    .map(|item| {
                        GraphEvent::new(
                            "gate.dispatch.v1",
                            json!({"batch_id":batch_id,"id":item["id"],"payload":item["payload"],"context":payload["context"]}),
                        )
                    })
                    .collect();
                saved.batches.insert(
                    batch_id.into(),
                    Batch {
                        open: payload.clone(),
                        results: BTreeMap::new(),
                        completed: false,
                    },
                );
                return output(saved, events);
            }
            let Some(batch) = saved.batches.get_mut(batch_id) else {
                return output(saved, vec![]);
            };
            if batch.completed {
                return output(saved, vec![]);
            }
            let id = payload["id"]
                .as_str()
                .ok_or_else(|| invalid("id", "required"))?;
            let items = batch.open["items"].as_array().expect("validated open");
            if !items.iter().any(|item| item["id"].as_str() == Some(id)) {
                return output(saved, vec![]);
            }
            batch
                .results
                .entry(id.into())
                .or_insert_with(|| payload["payload"].clone());
            if batch.results.len() != items.len() {
                return output(saved, vec![]);
            }
            let results: Vec<_> = items.iter().map(|item| json!({"id":item["id"],"payload":batch.results[item["id"].as_str().expect("validated branch ID")]})).collect();
            batch.completed = true;
            let event = GraphEvent::new(
                "gate.completed.v1",
                json!({"batch_id":batch_id,"context":batch.open["context"],"results":results}),
            );
            batch.results.clear();
            output(saved, vec![event])
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn ctx(kind: &str, payload: Value, state: Value) -> GraphContext {
        GraphContext {
            tenant: "local".into(),
            run_id: RunId::new(),
            instance: "gate".into(),
            event: PublishedGraphEvent {
                id: crate::contracts::events::EventId::stable("test", kind),
                from: "test".into(),
                kind: kind.into(),
                payload,
            },
            state,
            cancellation: RunCancellation::new(),
        }
    }
    fn open(id: &str) -> Value {
        json!({"batch_id":id,"context":{"document":"proposal"},"items":[
            {"id":"security","payload":{"review":"security"}},
            {"id":"owner","payload":{"review":"ownership"}}
        ]})
    }
    #[tokio::test]
    async fn gate_joins_opaque_approval_results_after_restore_without_duplicate_dispatch() {
        let mut current = Gate
            .handle(ctx("gate.open.v1", open("review"), Value::Null))
            .await
            .unwrap();
        assert_eq!(current.events.len(), 2);
        assert_eq!(
            current.events[0].payload["payload"],
            json!({"review":"security"})
        );
        current = Gate
            .handle(ctx("gate.open.v1", open("review"), current.state))
            .await
            .unwrap();
        assert!(current.events.is_empty());
        // Multiple independent batches may be pending on the same gate.
        current = Gate
            .handle(ctx("gate.open.v1", open("another"), current.state))
            .await
            .unwrap();
        for (batch, id) in [
            ("missing", "security"),
            ("review", "unknown"),
            ("review", "owner"),
        ] {
            current = Gate
                .handle(ctx(
                    "gate.result.v1",
                    json!({"batch_id":batch,"id":id,"payload":{"approved":false}}),
                    current.state,
                ))
                .await
                .unwrap();
            assert!(current.events.is_empty());
        }
        let restored =
            serde_json::from_str(&serde_json::to_string(&current.state).unwrap()).unwrap();
        current = Gate
            .handle(ctx(
                "gate.result.v1",
                json!({"batch_id":"review","id":"owner","payload":{"approved":true}}),
                restored,
            ))
            .await
            .unwrap();
        assert!(current.events.is_empty());
        current = Gate
            .handle(ctx(
                "gate.result.v1",
                json!({"batch_id":"review","id":"security","payload":{"approved":true}}),
                current.state,
            ))
            .await
            .unwrap();
        assert_eq!(current.events.len(), 1);
        assert_eq!(
            current.events[0].payload["context"],
            json!({"document":"proposal"})
        );
        assert_eq!(
            current.events[0].payload["results"],
            json!([
                {"id":"security","payload":{"approved":true}}, {"id":"owner","payload":{"approved":false}}
            ])
        );
        current = Gate
            .handle(ctx("gate.open.v1", open("review"), current.state))
            .await
            .unwrap();
        assert!(current.events.is_empty());
        current = Gate
            .handle(ctx(
                "gate.result.v1",
                json!({"batch_id":"review","id":"owner","payload":null}),
                current.state,
            ))
            .await
            .unwrap();
        assert!(current.events.is_empty());
    }
    #[tokio::test]
    async fn gate_rejects_conflicting_batches_and_duplicate_branch_ids() {
        let started = Gate
            .handle(ctx("gate.open.v1", open("review"), Value::Null))
            .await
            .unwrap();
        let mut changed = open("review");
        changed["context"] = json!("changed");
        assert!(
            Gate.handle(ctx("gate.open.v1", changed, started.state))
                .await
                .is_err()
        );
        let mut duplicate = open("review");
        duplicate["items"][1]["id"] = json!("security");
        assert!(
            Gate.handle(ctx("gate.open.v1", duplicate, Value::Null))
                .await
                .is_err()
        );
    }
}
