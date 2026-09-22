//! Concrete bridge to Mina's single-agent Harness.
use crate::{
    AgentDescriptor, AgentNode, DurableAgentNode, InMemoryBus, Message, PublishError,
    SqliteBusHandle, lifecycle_event,
};
use agent_core::harness::{Agent, OutputChannel, RunEventKind, RunId};
use agent_harness::{Harness, RunOptions};
use futures_util::StreamExt;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

pub struct HarnessAgentNode<A> {
    pub descriptor: AgentDescriptor,
    harness: Harness<A>,
    options: RunOptions,
}

#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error("durable worker interrupted; request remains unacknowledged")]
    Interrupted,
    #[error("task payload requires a string input")]
    InvalidInput,
    #[error(transparent)]
    Publish(#[from] PublishError),
    #[error(transparent)]
    Harness(#[from] agent_harness::HarnessError),
    #[error("Harness stream ended without a terminal event")]
    MissingTerminal,
}

impl<A: Agent> HarnessAgentNode<A> {
    pub fn new(descriptor: AgentDescriptor, harness: Harness<A>, options: RunOptions) -> Self {
        Self {
            descriptor,
            harness,
            options,
        }
    }

    /// Publishes every Harness event and one terminal result, preserving request correlation.
    /// RunOptions belong to the host; incoming messages cannot expand tool permissions.
    pub async fn execute(
        &self,
        request: Message,
        bus: &InMemoryBus,
        cancel: CancellationToken,
    ) -> Result<Value, NodeError> {
        let Some(input) = request.payload.get("input").and_then(Value::as_str) else {
            bus.publish(lifecycle_event(
                &request,
                &self.descriptor.id,
                "task.failed",
                json!({"code":"invalid_input", "retryable":false}),
            ))?;
            return Err(NodeError::InvalidInput);
        };
        let mut execution = match self.harness.start_with_options(
            // Replay of one request keeps its identity; distinct workers and attempts
            // must never share Harness checkpoints or effect identifiers.
            RunId::stable(
                "agent-multi",
                &format!("{}:{}:{}", request.run_id, request.id, self.descriptor.id),
            ),
            input,
            Vec::new(),
            self.options.clone(),
        ) {
            Ok(execution) => execution,
            Err(error) => {
                bus.publish(lifecycle_event(&request, &self.descriptor.id, "task.failed", json!({"code": error.code(), "message": error.to_string(), "retryable": error.retryable()})))?;
                return Err(error.into());
            }
        };
        let mut output = String::new();
        let mut usage = None;
        let mut cancellation_sent = false;
        loop {
            let event = tokio::select! {
                biased;
                _ = cancel.cancelled(), if !cancellation_sent => {
                    execution.cancellation.cancel();
                    cancellation_sent = true;
                    continue;
                }
                event = execution.events.next() => event,
            };
            let Some(event) = event else {
                return Err(NodeError::MissingTerminal);
            };
            let kind = match &event.kind {
                RunEventKind::RunStarted => "task.started",
                RunEventKind::RunCompleted { .. } => "task.completed",
                RunEventKind::RunFailed { .. } => "task.failed",
                RunEventKind::RunCancelled => "task.cancelled",
                _ => "task.progress",
            };
            if let RunEventKind::OutputDelta {
                channel: OutputChannel::AssistantText,
                delta,
            } = &event.kind
            {
                output.push_str(delta);
            }
            if let RunEventKind::UsageUpdated { usage: value } = &event.kind {
                usage = Some(*value);
            }
            let terminal = matches!(kind, "task.completed" | "task.failed" | "task.cancelled");
            let payload = json!({"event": event, "output": if terminal { Some(&output) } else { None }, "usage": usage});
            if let Err(error) = bus.publish(lifecycle_event(
                &request,
                &self.descriptor.id,
                kind,
                payload.clone(),
            )) {
                execution.cancellation.cancel();
                return Err(error.into());
            }
            if terminal {
                return Ok(payload);
            }
        }
    }

    /// Durable variant: publishes lifecycle events to SQLite and is intended to
    /// be called by `serve_durable_node` (which performs the input ack).
    pub async fn execute_durable(
        &self,
        request: Message,
        bus: &SqliteBusHandle,
        cancel: CancellationToken,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if cancel.is_cancelled() {
            return Err(NodeError::Interrupted.into());
        }
        let input = request
            .payload
            .get("input")
            .and_then(Value::as_str)
            .ok_or(NodeError::InvalidInput)?;
        let mut execution = self.harness.start_with_options(
            RunId::stable(
                "agent-multi",
                &format!("{}:{}:{}", request.run_id, request.id, self.descriptor.id),
            ),
            input,
            Vec::new(),
            self.options.clone(),
        )?;
        loop {
            let event = tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    execution.cancellation.cancel();
                    return Err(NodeError::Interrupted.into());
                },
                event = execution.events.next() => event,
            };
            let Some(event) = event else { break };
            let kind = match &event.kind {
                RunEventKind::RunStarted => "task.started",
                RunEventKind::RunCompleted { .. } => "task.completed",
                RunEventKind::RunFailed { .. } => "task.failed",
                RunEventKind::RunCancelled => "task.cancelled",
                _ => "task.progress",
            };
            if let Err(error) = bus
                .publish_async(lifecycle_event(
                    &request,
                    &self.descriptor.id,
                    kind,
                    json!({"event": event}),
                ))
                .await
            {
                execution.cancellation.cancel();
                return Err(error.into());
            }
            if matches!(kind, "task.completed" | "task.failed" | "task.cancelled") {
                return Ok(());
            }
        }
        Err(NodeError::MissingTerminal.into())
    }
}

impl<A: Agent + 'static> DurableAgentNode for HarnessAgentNode<A> {
    fn handle_durable(
        &self,
        message: Message,
        bus: SqliteBusHandle,
    ) -> futures_core::future::BoxFuture<
        'static,
        Result<(), Box<dyn std::error::Error + Send + Sync>>,
    > {
        let node = Self {
            descriptor: self.descriptor.clone(),
            harness: self.harness.clone(),
            options: self.options.clone(),
        };
        Box::pin(async move {
            node.execute_durable(message, &bus, CancellationToken::new())
                .await
        })
    }
    fn handle_durable_with_cancel(
        &self,
        message: Message,
        bus: SqliteBusHandle,
        cancel: CancellationToken,
    ) -> futures_core::future::BoxFuture<
        'static,
        Result<(), Box<dyn std::error::Error + Send + Sync>>,
    > {
        let node = Self {
            descriptor: self.descriptor.clone(),
            harness: self.harness.clone(),
            options: self.options.clone(),
        };
        Box::pin(async move { node.execute_durable(message, &bus, cancel).await })
    }
}

impl<A: Agent + 'static> AgentNode for HarnessAgentNode<A> {
    fn descriptor(&self) -> AgentDescriptor {
        self.descriptor.clone()
    }
    fn handle(
        &self,
        message: Message,
        bus: InMemoryBus,
    ) -> futures_core::future::BoxFuture<'static, ()> {
        self.handle_with_cancel(message, bus, CancellationToken::new())
    }
    fn handle_with_cancel(
        &self,
        message: Message,
        bus: InMemoryBus,
        cancel: CancellationToken,
    ) -> futures_core::future::BoxFuture<'static, ()> {
        let node = Self {
            descriptor: self.descriptor.clone(),
            harness: self.harness.clone(),
            options: self.options.clone(),
        };
        Box::pin(async move {
            let _ = node.execute(message, &bus, cancel).await;
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_core::harness::{
        AgentEvent, AgentEventStream, AgentMetadata, FinishReason, RunRequest,
    };
    struct Echo;
    struct Never;
    impl Agent for Never {
        fn metadata(&self) -> AgentMetadata {
            AgentMetadata::new("never", "1")
        }
        fn run(&self, _: RunRequest) -> AgentEventStream {
            Box::pin(futures_util::stream::pending())
        }
    }
    impl Agent for Echo {
        fn metadata(&self) -> AgentMetadata {
            AgentMetadata::new("echo", "1")
        }
        fn run(&self, request: RunRequest) -> AgentEventStream {
            Box::pin(futures_util::stream::iter(vec![
                AgentEvent::text_delta(request.input),
                AgentEvent::completed(FinishReason::Stop),
            ]))
        }
    }
    #[tokio::test]
    async fn real_harness_publishes_correlated_output_and_terminal() {
        let bus = InMemoryBus::new(32);
        let request = Message::new(
            uuid::Uuid::new_v4(),
            "tasks",
            "supervisor",
            "task.request",
            json!({"input":"hello"}),
        );
        let mut results = bus.subscribe(format!("run.{}.result", request.run_id));
        let node = HarnessAgentNode::new(
            AgentDescriptor {
                id: "echo".into(),
                role: "worker".into(),
            },
            Harness::new(Echo),
            RunOptions::default(),
        );
        let result = node
            .execute(request.clone(), &bus, CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(result["output"], "hello");
        let mut kinds = Vec::new();
        loop {
            let message = tokio::time::timeout(std::time::Duration::from_secs(1), results.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(message.correlation_id, Some(request.id));
            assert_eq!(message.run_id, request.run_id);
            assert_eq!(message.sender, "echo");
            kinds.push(message.kind.clone());
            if message.kind == "task.completed" {
                assert_eq!(message.payload["output"], "hello");
                break;
            }
        }
        assert_eq!(kinds.first().unwrap(), "task.started");
        assert!(kinds.contains(&"task.progress".into()));
    }

    #[tokio::test]
    async fn harness_identity_is_per_worker_and_attempt_but_stable_on_replay() {
        let bus = InMemoryBus::new(128);
        let request = Message::new(
            uuid::Uuid::new_v4(),
            "tasks",
            "s",
            "task.request",
            json!({"input":"hello"}),
        );
        let _results = bus.subscribe(format!("run.{}.result", request.run_id));
        let node = |id: &str| {
            HarnessAgentNode::new(
                AgentDescriptor {
                    id: id.into(),
                    role: "worker".into(),
                },
                Harness::new(Echo),
                RunOptions::default(),
            )
        };
        let a = node("a");
        let b = node("b");
        let first = a
            .execute(request.clone(), &bus, CancellationToken::new())
            .await
            .unwrap();
        let replay = a
            .execute(request.clone(), &bus, CancellationToken::new())
            .await
            .unwrap();
        let other = b
            .execute(request.clone(), &bus, CancellationToken::new())
            .await
            .unwrap();
        let mut retry = request.clone();
        retry.id = uuid::Uuid::new_v4();
        let retried = a
            .execute(retry, &bus, CancellationToken::new())
            .await
            .unwrap();
        let id = &first["event"]["run_id"];
        assert!(id.as_str().is_some());
        assert_eq!(id, &replay["event"]["run_id"]);
        assert_ne!(id, &other["event"]["run_id"]);
        assert_ne!(id, &retried["event"]["run_id"]);
    }

    #[tokio::test]
    async fn cancellation_interrupts_a_pending_harness() {
        let bus = InMemoryBus::new(16);
        let request = Message::new(
            uuid::Uuid::new_v4(),
            "tasks",
            "s",
            "task.request",
            json!({"input":"wait"}),
        );
        let mut results = bus.subscribe(format!("run.{}.result", request.run_id));
        let node = HarnessAgentNode::new(
            AgentDescriptor {
                id: "n".into(),
                role: "worker".into(),
            },
            Harness::new(Never),
            RunOptions::default(),
        );
        let cancel = CancellationToken::new();
        let execution = node.execute(request, &bus, cancel.clone());
        let cancellation = async {
            assert_eq!(results.recv().await.unwrap().kind, "task.started");
            cancel.cancel();
        };
        let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            tokio::join!(execution, cancellation)
        })
        .await
        .unwrap();
        assert_eq!(result.unwrap()["event"]["type"], "run_cancelled");
        assert_eq!(results.recv().await.unwrap().kind, "task.cancelled");
    }

    #[tokio::test]
    async fn invalid_payload_publishes_correlated_failure() {
        let bus = InMemoryBus::new(8);
        let request = Message::new(
            uuid::Uuid::new_v4(),
            "tasks",
            "s",
            "task.request",
            json!({}),
        );
        let mut results = bus.subscribe(format!("run.{}.result", request.run_id));
        let node = HarnessAgentNode::new(
            AgentDescriptor {
                id: "n".into(),
                role: "worker".into(),
            },
            Harness::new(Echo),
            RunOptions::default(),
        );
        assert!(matches!(
            node.execute(request.clone(), &bus, CancellationToken::new())
                .await,
            Err(NodeError::InvalidInput)
        ));
        let failure = results.recv().await.unwrap();
        assert_eq!(failure.kind, "task.failed");
        assert_eq!(failure.correlation_id, Some(request.id));
        assert_eq!(failure.payload["code"], "invalid_input");
    }
}
