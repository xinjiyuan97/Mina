//! One-shot, tool-free final response and a shared terminal outlet.
use super::*;

pub(super) const PROMPT: &str = "The tool execution round limit has been reached. No further tools are available. Give the final response using only the results already obtained. Clearly distinguish completed work from unfinished work and explain any limitations. Do not claim that unexecuted actions succeeded.";

pub(super) fn register(registry: &mut OperatorRegistry) {
    let mut desc = descriptor(
        "agent.finalizer",
        &[
            "finalize.requested.v1",
            "stop.completed.v1",
            "stop.failed.v1",
            "stop.cancelled.v1",
        ],
        &[
            "model.output.v1",
            "stop.completed.v1",
            "stop.failed.v1",
            "stop.cancelled.v1",
        ],
        json!({"type":"object","additionalProperties":false,"required":["model","timeout_ms","max_output_tokens"],"properties":{"model":{"type":"string","minLength":1},"model_name":{"type":"string","minLength":1},"timeout_ms":{"type":"integer","minimum":1,"maximum":86400000},"max_output_tokens":{"type":"integer","minimum":1}}}),
        false,
    );
    desc.resources.insert("model".into(), ResourceKind::Model);
    registry
        .register(desc, |config, resources| {
            let config: ModelConfig =
                serde_json::from_value(config.clone()).map_err(|e| invalid("config", e))?;
            let port = resources.get_model(&config.model)?;
            Ok(Arc::new(Finalizer {
                model: Model { config, port },
            }))
        })
        .expect("finalizer registration");
}

struct Finalizer {
    model: Model,
}
impl GraphOperator for Finalizer {
    fn handle(&self, mut context: GraphContext) -> GraphFuture<'_> {
        Box::pin(async move {
            if context.state["done"] == true {
                return Ok(GraphOutput::new(context.state, vec![]));
            }
            if context.event.kind != "finalize.requested.v1" {
                return Ok(GraphOutput::new(
                    json!({"done":true}),
                    vec![GraphEvent::new(context.event.kind, context.event.payload)],
                ));
            }
            // Never trust callers to disable tools. The instruction is normally
            // included by Context before compaction; standalone use adds it here.
            context.event.payload["tools"] = json!([]);
            let mut messages: Vec<ModelMessage> =
                serde_json::from_value(context.event.payload["messages"].clone())
                    .map_err(|e| invalid("messages", e))?;
            if !messages
                .iter()
                .any(|m| m.role == ModelRole::System && m.content == PROMPT)
            {
                messages.push(ModelMessage::system(PROMPT));
            }
            context.event.payload["messages"] = json!(messages);
            let mut result = self.model.handle(context).await?;
            result.state = json!({"done":true});
            for event in &mut result.events {
                event.payload["termination_reason"] = json!("max_steps_exceeded");
                event.payload["finalized"] = json!(true);
            }
            Ok(result)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Hanging(Arc<AtomicUsize>);
    impl ModelPort for Hanging {
        fn stream(&self, request: ModelRequest) -> ModelEventStream {
            self.0.fetch_add(1, Ordering::SeqCst);
            assert!(request.tools.is_empty());
            Box::pin(futures_util::stream::pending())
        }
    }
    fn context(state: Value) -> GraphContext {
        GraphContext {
            tenant: "local".into(),
            run_id: RunId::new(),
            instance: "finalizer".into(),
            event: PublishedGraphEvent {
                id: crate::contracts::events::EventId::stable("test", "finalize"),
                from: "context".into(),
                kind: "finalize.requested.v1".into(),
                payload: json!({"round":2,"messages":[ModelMessage::user("task")],"tools":[{"invalid":"ignored"}]}),
            },
            state,
            cancellation: RunCancellation::new(),
        }
    }
    #[tokio::test]
    async fn timeout_is_terminal_and_restored_done_state_prevents_another_call() {
        let calls = Arc::new(AtomicUsize::new(0));
        let finalizer = Finalizer {
            model: Model {
                config: ModelConfig {
                    model: "test".into(),
                    model_name: None,
                    timeout_ms: 5,
                    max_output_tokens: Some(256),
                },
                port: Arc::new(Hanging(calls.clone())),
            },
        };
        let output = finalizer.handle(context(Value::Null)).await.unwrap();
        assert_eq!(output.events[0].kind, "stop.failed.v1");
        assert_eq!(output.events[0].payload["code"], "model_timeout");
        assert_eq!(
            output.events[0].payload["termination_reason"],
            "max_steps_exceeded"
        );
        let restored =
            serde_json::from_str(&serde_json::to_string(&output.state).unwrap()).unwrap();
        assert!(
            finalizer
                .handle(context(restored))
                .await
                .unwrap()
                .events
                .is_empty()
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
