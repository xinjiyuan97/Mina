use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use agent_core::harness::{
    ModelEvent, ModelEventStream, ModelPort, ModelRequest, TokenUsage, ToolCallFuture,
    ToolCallRequest, ToolDefinition, ToolPort,
};
pub use agent_core::observability::{
    NoopObservationHook, ObservationEvent, ObservationHook, ObservationKind, ObservationStatus,
};
use async_stream::stream;

#[derive(Debug, Default)]
pub struct TracingObservationHook;

impl ObservationHook for TracingObservationHook {
    fn record(&self, event: ObservationEvent) {
        let event_type = event_type(&event.kind);
        let payload = serde_json::to_string(&event.kind)
            .unwrap_or_else(|_| "{\"type\":\"serialization_failed\"}".to_owned());
        tracing::info!(
            target: "mina::observation",
            trace_id = %event.trace_id,
            observation_id = %event.observation_id,
            observation_type = event_type,
            timestamp_ms = event.timestamp_ms,
            payload = %payload,
            "agent observation"
        );
    }
}

#[derive(Default)]
pub struct CompositeObservationHook {
    hooks: Vec<Arc<dyn ObservationHook>>,
}

impl CompositeObservationHook {
    #[must_use]
    pub fn new(hooks: Vec<Arc<dyn ObservationHook>>) -> Self {
        Self { hooks }
    }
}

impl ObservationHook for CompositeObservationHook {
    fn record(&self, event: ObservationEvent) {
        for hook in &self.hooks {
            emit(hook, event.clone());
        }
    }
}

pub struct ObservedModel<P> {
    inner: Arc<P>,
    hook: Arc<dyn ObservationHook>,
    sequence: AtomicU64,
}

impl<P> ObservedModel<P> {
    pub fn new(inner: P, hook: Arc<dyn ObservationHook>) -> Self {
        Self {
            inner: Arc::new(inner),
            hook,
            sequence: AtomicU64::new(1),
        }
    }
}

impl<P> ModelPort for ObservedModel<P>
where
    P: ModelPort,
{
    fn stream(&self, request: ModelRequest) -> ModelEventStream {
        let inner = Arc::clone(&self.inner);
        let hook = Arc::clone(&self.hook);
        let trace_id = request.run_id.to_string();
        let observation_id = format!(
            "model:{}:{}",
            request.run_id,
            self.sequence.fetch_add(1, Ordering::Relaxed)
        );
        let model = request.model.clone();
        let message_count = request.messages.len();
        let tool_count = request.tools.len();
        emit(
            &hook,
            observation(
                &trace_id,
                &observation_id,
                ObservationKind::ModelStarted {
                    model: model.clone(),
                    message_count,
                    tool_count,
                },
            ),
        );
        let mut events = inner.stream(request);
        Box::pin(stream! {
            let started = Instant::now();
            let mut usage = None;
            let mut terminal = false;
            while let Some(event) = futures_next(&mut events).await {
                match &event {
                    ModelEvent::Usage { usage: next } => usage = Some(*next),
                    ModelEvent::Completed { finish_reason } => {
                        terminal = true;
                        emit_model_finished(
                            &hook,
                            ModelFinish {
                                trace_id: &trace_id,
                                observation_id: &observation_id,
                                model: &model,
                                duration: started.elapsed(),
                                status: ObservationStatus::Completed,
                                finish_reason: Some(finish_reason_name(*finish_reason).into()),
                                error_code: None,
                                retryable: None,
                                usage,
                            },
                        );
                    }
                    ModelEvent::Failed { error } => {
                        terminal = true;
                        emit_model_finished(
                            &hook,
                            ModelFinish {
                                trace_id: &trace_id,
                                observation_id: &observation_id,
                                model: &model,
                                duration: started.elapsed(),
                                status: ObservationStatus::Failed,
                                finish_reason: None,
                                error_code: Some(error.kind().code().to_owned()),
                                retryable: Some(error.retryable()),
                                usage,
                            },
                        );
                    }
                    _ => {}
                }
                yield event;
            }
            if !terminal {
                emit_model_finished(
                    &hook,
                    ModelFinish {
                        trace_id: &trace_id,
                        observation_id: &observation_id,
                        model: &model,
                        duration: started.elapsed(),
                        status: ObservationStatus::Failed,
                        finish_reason: None,
                        error_code: Some("model_stream_incomplete".into()),
                        retryable: Some(false),
                        usage,
                    },
                );
            }
        })
    }
}

pub struct ObservedTools<T> {
    inner: Arc<T>,
    hook: Arc<dyn ObservationHook>,
}

impl<T> ObservedTools<T> {
    pub fn new(inner: T, hook: Arc<dyn ObservationHook>) -> Self {
        Self {
            inner: Arc::new(inner),
            hook,
        }
    }
}

impl<T> ToolPort for ObservedTools<T>
where
    T: ToolPort,
{
    fn definitions(&self) -> Vec<ToolDefinition> {
        self.inner.definitions()
    }

    fn validate(
        &self,
        name: &str,
        arguments: &serde_json::Value,
    ) -> Result<(), agent_core::harness::ToolError> {
        self.inner.validate(name, arguments)
    }

    fn call(&self, request: ToolCallRequest) -> ToolCallFuture {
        let trace_id = request.run_id.to_string();
        let call_id = request.call_id.clone();
        let tool_name = request.name.clone();
        let observation_id = format!("tool:{}:{}", request.run_id, call_id);
        let hook = Arc::clone(&self.hook);
        let future = self.inner.call(request);
        emit(
            &hook,
            observation(
                &trace_id,
                &observation_id,
                ObservationKind::ToolStarted {
                    call_id: call_id.clone(),
                    tool_name: tool_name.clone(),
                },
            ),
        );
        Box::pin(async move {
            let started = Instant::now();
            let result = future.await;
            let kind = match &result {
                Ok(_) => ObservationKind::ToolFinished {
                    call_id,
                    tool_name,
                    status: ObservationStatus::Completed,
                    duration_ms: duration_ms(started.elapsed()),
                    error_code: None,
                    error_category: None,
                    retryable: None,
                    retry_after_ms: None,
                },
                Err(error) => ObservationKind::ToolFinished {
                    call_id,
                    tool_name,
                    status: ObservationStatus::Failed,
                    duration_ms: duration_ms(started.elapsed()),
                    error_code: Some(error.code().to_owned()),
                    error_category: Some(error.category().as_str().to_owned()),
                    retryable: Some(error.retryable()),
                    retry_after_ms: error.retry_after_ms(),
                },
            };
            emit(&hook, observation(&trace_id, &observation_id, kind));
            result
        })
    }
}

async fn futures_next(stream: &mut ModelEventStream) -> Option<ModelEvent> {
    use futures_util::StreamExt;
    stream.next().await
}

struct ModelFinish<'a> {
    trace_id: &'a str,
    observation_id: &'a str,
    model: &'a str,
    duration: Duration,
    status: ObservationStatus,
    finish_reason: Option<String>,
    error_code: Option<String>,
    retryable: Option<bool>,
    usage: Option<TokenUsage>,
}

fn emit_model_finished(hook: &Arc<dyn ObservationHook>, finished: ModelFinish<'_>) {
    emit(
        hook,
        observation(
            finished.trace_id,
            finished.observation_id,
            ObservationKind::ModelFinished {
                model: finished.model.to_owned(),
                status: finished.status,
                duration_ms: duration_ms(finished.duration),
                finish_reason: finished.finish_reason,
                error_code: finished.error_code,
                retryable: finished.retryable,
                input_tokens: finished.usage.map(|value| value.input_tokens),
                output_tokens: finished.usage.map(|value| value.output_tokens),
            },
        ),
    );
}

fn observation(trace_id: &str, observation_id: &str, kind: ObservationKind) -> ObservationEvent {
    ObservationEvent {
        trace_id: trace_id.to_owned(),
        observation_id: observation_id.to_owned(),
        timestamp_ms: unix_time_ms(),
        kind,
    }
}

fn emit(hook: &Arc<dyn ObservationHook>, event: ObservationEvent) {
    let _ = catch_unwind(AssertUnwindSafe(|| hook.record(event)));
}

fn unix_time_ms() -> i64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    i64::try_from(millis).unwrap_or(i64::MAX)
}

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

const fn event_type(kind: &ObservationKind) -> &'static str {
    match kind {
        ObservationKind::ModelStarted { .. } => "model_started",
        ObservationKind::ModelFinished { .. } => "model_finished",
        ObservationKind::ToolStarted { .. } => "tool_started",
        ObservationKind::ToolFinished { .. } => "tool_finished",
    }
}

const fn finish_reason_name(reason: agent_core::harness::FinishReason) -> &'static str {
    match reason {
        agent_core::harness::FinishReason::Stop => "stop",
        agent_core::harness::FinishReason::Length => "length",
        agent_core::harness::FinishReason::ToolCall => "tool_call",
        agent_core::harness::FinishReason::ContentFilter => "content_filter",
        agent_core::harness::FinishReason::Unknown => "unknown",
        _ => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use agent_core::harness::{
        FinishReason, ModelEvent, ModelEventStream, ModelMessage, ModelRequest, RunId, TokenUsage,
        TokenUsageSource,
    };
    use futures_util::{StreamExt, stream};

    use super::*;

    #[derive(Default)]
    struct RecordingHook {
        events: Mutex<Vec<ObservationEvent>>,
    }

    impl ObservationHook for RecordingHook {
        fn record(&self, event: ObservationEvent) {
            self.events
                .lock()
                .expect("recording hook lock should be available")
                .push(event);
        }
    }

    struct FakeModel;

    impl ModelPort for FakeModel {
        fn stream(&self, _request: ModelRequest) -> ModelEventStream {
            Box::pin(stream::iter([
                ModelEvent::Usage {
                    usage: TokenUsage {
                        input_tokens: 12,
                        output_tokens: 3,
                        total_tokens: 15,
                        source: TokenUsageSource::ProviderReported,
                    },
                },
                ModelEvent::Completed {
                    finish_reason: FinishReason::Stop,
                },
            ]))
        }
    }

    #[tokio::test]
    async fn model_middleware_records_one_generation_without_content() {
        let hook = Arc::new(RecordingHook::default());
        let observed = ObservedModel::new(FakeModel, hook.clone());
        let mut events = observed.stream(ModelRequest {
            run_id: RunId::new(),
            model: "test-model".into(),
            messages: vec![ModelMessage::user("secret prompt")],
            tools: Vec::new(),
            max_output_tokens: None,
        });
        while events.next().await.is_some() {}

        let recorded = hook
            .events
            .lock()
            .expect("recording hook lock should be available");
        assert_eq!(recorded.len(), 2);
        assert!(matches!(
            recorded[0].kind,
            ObservationKind::ModelStarted { .. }
        ));
        assert!(matches!(
            recorded[1].kind,
            ObservationKind::ModelFinished {
                input_tokens: Some(12),
                output_tokens: Some(3),
                ..
            }
        ));
        let serialized = serde_json::to_string(&*recorded).expect("events should serialize");
        assert!(!serialized.contains("secret prompt"));
    }
}
