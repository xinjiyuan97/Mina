use crate::contracts::*;
#[cfg(test)]
use futures_util::StreamExt;
use std::{sync::Arc, time::Duration};

/// A bounded model/tool loop for one independent run.
///
/// Every model invocation has its own terminal `ModelEvent`, while the Agent
/// continues across tool results and emits exactly one terminal `AgentEvent`.
pub struct AgentLoop<P, T> {
    provider: Arc<P>,
    tools: Arc<T>,
    approvals: Arc<dyn ApprovalPort>,
    approval_policy: ToolApprovalPolicy,
    model: String,
    system_prompt: String,
    max_output_tokens: Option<u32>,
    max_steps: u32,
    model_timeout: Duration,
    tool_timeout: Duration,
    tool_call_strategy: ToolCallStrategy,
}

const DEFAULT_MODEL_TIMEOUT: Duration = Duration::from_secs(120);
const DEFAULT_TOOL_TIMEOUT: Duration = Duration::from_secs(30);

impl<P, T> AgentLoop<P, T> {
    pub fn model_port(&self) -> Arc<P> {
        self.provider.clone()
    }
    pub fn tool_port(&self) -> Arc<T> {
        self.tools.clone()
    }
    #[must_use]
    pub fn new(
        provider: P,
        tools: T,
        model: impl Into<String>,
        system_prompt: impl Into<String>,
        max_output_tokens: Option<u32>,
        max_steps: u32,
    ) -> Self {
        Self {
            provider: Arc::new(provider),
            tools: Arc::new(tools),
            approvals: Arc::new(RejectAllApprovals),
            approval_policy: ToolApprovalPolicy::default(),
            model: model.into(),
            system_prompt: system_prompt.into(),
            max_output_tokens,
            max_steps,
            model_timeout: DEFAULT_MODEL_TIMEOUT,
            tool_timeout: DEFAULT_TOOL_TIMEOUT,
            tool_call_strategy: ToolCallStrategy::Sequential,
        }
    }

    #[must_use]
    pub const fn with_timeouts(mut self, model_timeout: Duration, tool_timeout: Duration) -> Self {
        self.model_timeout = model_timeout;
        self.tool_timeout = tool_timeout;
        self
    }

    #[must_use]
    pub fn with_approval_port<A>(mut self, approvals: A) -> Self
    where
        A: ApprovalPort,
    {
        self.approvals = Arc::new(approvals);
        self
    }

    #[must_use]
    pub const fn with_approval_policy(mut self, policy: ToolApprovalPolicy) -> Self {
        self.approval_policy = policy;
        self
    }

    #[must_use]
    pub const fn with_tool_call_strategy(mut self, strategy: ToolCallStrategy) -> Self {
        self.tool_call_strategy = strategy;
        self
    }
}

impl<P: ModelPort, T: ToolPort> Agent for AgentLoop<P, T> {
    fn metadata(&self) -> AgentMetadata {
        AgentMetadata::new("agent-loop", env!("CARGO_PKG_VERSION"))
            .with_capability("request_response")
            .with_capability("event_stream")
            .with_capability("model_completion")
            .with_capability("tool_calling")
            .with_capability("tool_set_snapshots")
            .with_capability("run_scoped_dynamic_tools")
            .with_capability("tool_approval")
    }

    fn run(&self, request: RunRequest) -> AgentEventStream {
        use crate::agent::{
            AgentResources, GraphAgent, GraphAgentConfig, OperatorRegistry,
            memory_fabric::MemoryFabric,
        };
        let mut config = GraphAgentConfig::new(&self.model, &self.system_prompt);
        config.max_output_tokens = self.max_output_tokens;
        config.max_steps = self.max_steps.max(1);
        config.allow_run_adf = request.allow_run_adf;
        config.review_level = self.approval_policy.review_level();
        config.parallel_tools = self.tool_call_strategy == ToolCallStrategy::ParallelSafe;
        config.model_timeout_ms = self.model_timeout.as_millis().clamp(1, 86_400_000) as u64;
        config.tool_timeout_ms = self.tool_timeout.as_millis().clamp(1, 86_400_000) as u64;
        let resources = AgentResources::new(Arc::new(MemoryFabric::default()))
            .model("primary", "host-v1", self.provider.clone())
            .toolset("workspace", "host-v1", self.tools.clone());
        match config.definition().and_then(|definition| {
            GraphAgent::new(definition, OperatorRegistry::builtins(), resources)
        }) {
            Ok(agent) => agent
                .with_approval_port(self.approvals.clone())
                .run(request),
            Err(error) => Box::pin(futures_util::stream::once(async move {
                crate::contracts::AgentEvent::failed("graph_invalid", error.to_string(), false)
            })),
        }
    }
}
#[cfg(test)]
mod tests {
    use crate::agent::{Harness, RunOptions};
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures_util::stream;
    use serde_json::json;

    use super::*;
    use crate::contracts::{
        ModelEventStream, RunEventKind, ToolCallFuture, ToolDefinition, ToolError, ToolOutput,
    };

    struct FakeProvider {
        calls: AtomicUsize,
        tool_name: &'static str,
    }

    impl ModelPort for FakeProvider {
        fn stream(&self, request: ModelRequest) -> ModelEventStream {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            assert_eq!(request.tools[0].name, self.tool_name);

            if call == 0 {
                assert_eq!(request.messages.len(), 2);
                return Box::pin(stream::iter(vec![
                    ModelEvent::Accepted {
                        provider_request_id: None,
                    },
                    ModelEvent::ToolCallStarted {
                        call_id: "call_1".into(),
                        name: self.tool_name.into(),
                    },
                    ModelEvent::ToolCallArgumentsDelta {
                        call_id: "call_1".into(),
                        delta: "".into(),
                    },
                    ModelEvent::Usage {
                        usage: TokenUsage {
                            input_tokens: 10,
                            output_tokens: 2,
                            total_tokens: 12,
                            source: TokenUsageSource::ProviderReported,
                        },
                    },
                    ModelEvent::Completed {
                        finish_reason: FinishReason::ToolCall,
                    },
                ]));
            }

            assert_eq!(request.messages.len(), 4);
            assert_eq!(request.messages[2].tool_calls[0].id, "call_1");
            assert_eq!(request.messages[3].role, crate::contracts::ModelRole::Tool);
            assert_eq!(request.messages[3].tool_call_id.as_deref(), Some("call_1"));
            Box::pin(stream::iter(vec![
                ModelEvent::Accepted {
                    provider_request_id: None,
                },
                ModelEvent::TextDelta {
                    delta: "It is noon.".into(),
                },
                ModelEvent::Usage {
                    usage: TokenUsage {
                        input_tokens: 20,
                        output_tokens: 4,
                        total_tokens: 24,
                        source: TokenUsageSource::ProviderReported,
                    },
                },
                ModelEvent::Completed {
                    finish_reason: FinishReason::Stop,
                },
            ]))
        }
    }

    struct FakeTools;

    impl ToolPort for FakeTools {
        fn definitions(&self) -> Vec<ToolDefinition> {
            vec![ToolDefinition::new(
                "get_current_time",
                "Return the current time.",
                json!({"type": "object", "properties": {}}),
            )]
        }

        fn call(&self, request: ToolCallRequest) -> ToolCallFuture {
            Box::pin(async move {
                if request.name != "get_current_time" {
                    return Err(ToolError::new("tool_not_found", "unknown tool", false));
                }
                assert_eq!(request.arguments, json!({}));
                Ok(ToolOutput::text(r#"{"utc":"2026-08-25T00:00:00Z"}"#))
            })
        }
    }

    struct PendingProvider;

    impl ModelPort for PendingProvider {
        fn stream(&self, _request: ModelRequest) -> ModelEventStream {
            Box::pin(stream::pending())
        }
    }

    struct ToolTimeoutProvider {
        calls: AtomicUsize,
    }

    impl ModelPort for ToolTimeoutProvider {
        fn stream(&self, request: ModelRequest) -> ModelEventStream {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                return Box::pin(stream::iter(vec![
                    ModelEvent::ToolCallStarted {
                        call_id: "call_slow".into(),
                        name: "get_current_time".into(),
                    },
                    ModelEvent::ToolCallArgumentsDelta {
                        call_id: "call_slow".into(),
                        delta: "{}".into(),
                    },
                    ModelEvent::Completed {
                        finish_reason: FinishReason::ToolCall,
                    },
                ]));
            }

            assert!(request.messages[3].content.contains("tool_timeout"));
            Box::pin(stream::iter(vec![
                ModelEvent::TextDelta {
                    delta: "The tool timed out.".into(),
                },
                ModelEvent::Completed {
                    finish_reason: FinishReason::Stop,
                },
            ]))
        }
    }

    struct SlowTools;

    impl ToolPort for SlowTools {
        fn definitions(&self) -> Vec<ToolDefinition> {
            FakeTools.definitions()
        }

        fn call(&self, _request: ToolCallRequest) -> ToolCallFuture {
            Box::pin(std::future::pending())
        }
    }

    struct StepLimitProvider {
        calls: Arc<AtomicUsize>,
    }

    impl ModelPort for StepLimitProvider {
        fn stream(&self, request: ModelRequest) -> ModelEventStream {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                assert_eq!(request.tools.len(), 1);
                return Box::pin(stream::iter(vec![
                    ModelEvent::ToolCallStarted {
                        call_id: "call_after_limit".into(),
                        name: "get_current_time".into(),
                    },
                    ModelEvent::ToolCallArgumentsDelta {
                        call_id: "call_after_limit".into(),
                        delta: "{}".into(),
                    },
                    ModelEvent::Completed {
                        finish_reason: FinishReason::ToolCall,
                    },
                ]));
            }

            panic!("model must not run beyond the configured limit");
        }
    }

    struct CountingTools {
        calls: Arc<AtomicUsize>,
    }

    impl ToolPort for CountingTools {
        fn definitions(&self) -> Vec<ToolDefinition> {
            FakeTools.definitions()
        }

        fn call(&self, _request: ToolCallRequest) -> ToolCallFuture {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(ToolOutput::text("should not execute")) })
        }
    }

    struct RiskyTools {
        calls: Arc<AtomicUsize>,
    }

    impl ToolPort for RiskyTools {
        fn definitions(&self) -> Vec<ToolDefinition> {
            vec![
                ToolDefinition::new(
                    "exec_command",
                    "Run a managed terminal command.",
                    json!({"type": "object", "properties": {}}),
                )
                .with_risk_level(ToolRiskLevel::High),
            ]
        }

        fn call(&self, _request: ToolCallRequest) -> ToolCallFuture {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(ToolOutput::text(r#"{"utc":"now"}"#)) })
        }
    }

    struct FixedApproval {
        resolution: ApprovalResolution,
    }

    impl ApprovalPort for FixedApproval {
        fn request(&self, request: ApprovalRequest) -> crate::contracts::ApprovalFuture {
            assert_eq!(request.tool_name, "exec_command");
            assert_eq!(request.risk_level, ToolRiskLevel::High);
            assert_eq!(request.arguments, json!({}));
            let resolution = self.resolution.clone();
            Box::pin(async move { Ok(resolution) })
        }
    }

    #[tokio::test]
    async fn executes_a_tool_and_continues_until_the_model_finishes() {
        let harness = Harness::new(AgentLoop::new(
            FakeProvider {
                calls: AtomicUsize::new(0),
                tool_name: "get_current_time",
            },
            FakeTools,
            "test-model",
            "Use tools when needed.",
            Some(1024),
            4,
        ));

        let events: Vec<_> = harness
            .stream("What time is it?")
            .expect("stream should start")
            .collect()
            .await;

        assert!(events.iter().any(|event| matches!(
            &event.kind,
            RunEventKind::ToolExecutionCompleted { call_id, .. } if call_id == "call_1"
        )));
        assert!(events.iter().any(|event| matches!(
            event.kind,
            RunEventKind::UsageUpdated {
                usage: TokenUsage {
                    total_tokens: 36,
                    ..
                }
            }
        )));
        assert!(matches!(
            events.last().map(|event| &event.kind),
            Some(RunEventKind::RunCompleted {
                finish_reason: FinishReason::Stop
            })
        ));
    }

    #[tokio::test]
    async fn fails_when_a_model_round_exceeds_its_deadline() {
        let harness = Harness::new(
            AgentLoop::new(
                PendingProvider,
                FakeTools,
                "test-model",
                "Use tools when needed.",
                None,
                4,
            )
            .with_timeouts(Duration::from_millis(10), Duration::from_secs(1)),
        );

        let events: Vec<_> = harness
            .stream("Wait forever")
            .expect("stream should start")
            .collect()
            .await;

        assert!(matches!(
            events.last().map(|event| &event.kind),
            Some(RunEventKind::RunFailed { code, .. }) if code == "model_timeout"
        ));
    }

    #[tokio::test]
    async fn returns_tool_timeouts_to_the_model_and_continues() {
        let harness = Harness::new(
            AgentLoop::new(
                ToolTimeoutProvider {
                    calls: AtomicUsize::new(0),
                },
                SlowTools,
                "test-model",
                "Use tools when needed.",
                None,
                4,
            )
            .with_timeouts(Duration::from_secs(1), Duration::from_millis(10)),
        );

        let events: Vec<_> = harness
            .stream("Use the slow tool")
            .expect("stream should start")
            .collect()
            .await;

        assert!(events.iter().any(|event| matches!(
            &event.kind,
            RunEventKind::ToolExecutionFailed { code, .. } if code == "tool_timeout"
        )));
        assert!(matches!(
            events.last().map(|event| &event.kind),
            Some(RunEventKind::RunCompleted { .. })
        ));
    }

    #[tokio::test]
    async fn stops_after_the_configured_number_of_model_rounds() {
        let provider_calls = Arc::new(AtomicUsize::new(0));
        let tool_calls = Arc::new(AtomicUsize::new(0));
        let harness = Harness::new(AgentLoop::new(
            StepLimitProvider {
                calls: Arc::clone(&provider_calls),
            },
            CountingTools {
                calls: Arc::clone(&tool_calls),
            },
            "test-model",
            "Use tools when needed.",
            None,
            4,
        ));

        let execution = harness
            .start_with_options(
                crate::contracts::RunId::new(),
                "Use one more tool",
                Vec::new(),
                RunOptions {
                    allowed_tools: None,
                    allow_run_adf: false,
                    max_steps: Some(1),
                },
            )
            .expect("run should start");
        let events: Vec<_> = execution.events.collect().await;

        assert_eq!(provider_calls.load(Ordering::SeqCst), 1);
        assert_eq!(tool_calls.load(Ordering::SeqCst), 1);
        assert!(events.iter().any(|event| matches!(
            &event.kind, RunEventKind::ToolExecutionCompleted { call_id, .. } if call_id == "call_after_limit"
        )));
        assert!(matches!(events.last().map(|event| &event.kind),
            Some(RunEventKind::RunFailed { code, .. }) if code == "max_steps_exceeded"
        ));
    }

    #[tokio::test]
    async fn executes_a_risky_tool_only_after_approval() {
        let calls = Arc::new(AtomicUsize::new(0));
        let harness = Harness::new(
            AgentLoop::new(
                FakeProvider {
                    calls: AtomicUsize::new(0),
                    tool_name: "exec_command",
                },
                RiskyTools {
                    calls: Arc::clone(&calls),
                },
                "test-model",
                "Use tools when needed.",
                None,
                4,
            )
            .with_approval_port(FixedApproval {
                resolution: ApprovalResolution::allow_once(),
            }),
        );

        let events: Vec<_> = harness
            .stream("Use the risky tool")
            .expect("stream should start")
            .collect()
            .await;

        let approval_index = events
            .iter()
            .position(|event| matches!(event.kind, RunEventKind::ApprovalRequested { .. }))
            .expect("approval should be requested");
        let execution_index = events
            .iter()
            .position(|event| matches!(event.kind, RunEventKind::ToolExecutionStarted { .. }))
            .expect("tool should execute");
        assert!(approval_index < execution_index);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(events.iter().any(|event| matches!(
            &event.kind,
            RunEventKind::ApprovalResolved { resolution, .. }
                if resolution.decision == ApprovalDecision::AllowOnce
        )));
    }

    #[tokio::test]
    async fn numeric_review_level_can_filter_high_risk_tools() {
        let calls = Arc::new(AtomicUsize::new(0));
        let harness = Harness::new(
            AgentLoop::new(
                FakeProvider {
                    calls: AtomicUsize::new(0),
                    tool_name: "exec_command",
                },
                RiskyTools {
                    calls: Arc::clone(&calls),
                },
                "test-model",
                "Use tools when needed.",
                None,
                4,
            )
            .with_approval_policy(ToolApprovalPolicy::new(100)),
        );

        let events: Vec<_> = harness
            .stream("Use the risky tool without review")
            .expect("stream should start")
            .collect()
            .await;

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(
            !events
                .iter()
                .any(|event| matches!(event.kind, RunEventKind::ApprovalRequested { .. }))
        );
    }

    #[tokio::test]
    async fn returns_a_rejected_risky_tool_to_the_model_without_executing_it() {
        let calls = Arc::new(AtomicUsize::new(0));
        let harness = Harness::new(
            AgentLoop::new(
                FakeProvider {
                    calls: AtomicUsize::new(0),
                    tool_name: "exec_command",
                },
                RiskyTools {
                    calls: Arc::clone(&calls),
                },
                "test-model",
                "Use tools when needed.",
                None,
                4,
            )
            .with_approval_port(FixedApproval {
                resolution: ApprovalResolution::deny(Some("not allowed".into())),
            }),
        );

        let events: Vec<_> = harness
            .stream("Use the risky tool")
            .expect("stream should start")
            .collect()
            .await;

        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(events.iter().any(|event| matches!(
            &event.kind,
            RunEventKind::ToolExecutionFailed { code, message, .. }
                if code == "tool_rejected" && message.contains("not allowed")
        )));
        assert!(matches!(
            events.last().map(|event| &event.kind),
            Some(RunEventKind::RunCompleted { .. })
        ));
    }

    struct LegacySuspensionProvider {
        calls: AtomicUsize,
    }

    impl ModelPort for LegacySuspensionProvider {
        fn stream(&self, request: ModelRequest) -> ModelEventStream {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                return Box::pin(stream::iter(vec![
                    ModelEvent::Accepted {
                        provider_request_id: None,
                    },
                    ModelEvent::ToolCallStarted {
                        call_id: "call_suspend".into(),
                        name: "suspending_tool".into(),
                    },
                    ModelEvent::ToolCallArgumentsDelta {
                        call_id: "call_suspend".into(),
                        delta: "{}".into(),
                    },
                    ModelEvent::Completed {
                        finish_reason: FinishReason::ToolCall,
                    },
                ]));
            }
            let result: serde_json::Value = serde_json::from_str(&request.messages[3].content)
                .expect("legacy suspension failure should be JSON");
            assert_eq!(result["error"]["code"], "invalid_tool_suspension");
            Box::pin(stream::iter(vec![
                ModelEvent::Accepted {
                    provider_request_id: None,
                },
                ModelEvent::TextDelta {
                    delta: "durable runtime required".into(),
                },
                ModelEvent::Completed {
                    finish_reason: FinishReason::Stop,
                },
            ]))
        }
    }

    struct LegacySuspendingTool;

    impl ToolPort for LegacySuspendingTool {
        fn definitions(&self) -> Vec<ToolDefinition> {
            vec![ToolDefinition::new(
                "suspending_tool",
                "Requires durable suspension.",
                json!({"type": "object", "additionalProperties": false}),
            )]
        }

        fn call(&self, _request: ToolCallRequest) -> ToolCallFuture {
            Box::pin(async { Ok(ToolOutput::suspend(Vec::new(), Vec::new())) })
        }
    }

    #[tokio::test]
    async fn operator_stream_rejects_invalid_suspension_as_a_structured_tool_error() {
        let harness = Harness::new(AgentLoop::new(
            LegacySuspensionProvider {
                calls: AtomicUsize::new(0),
            },
            LegacySuspendingTool,
            "test-model",
            "Use tools when needed.",
            None,
            4,
        ));

        let events: Vec<_> = harness
            .stream("Suspend")
            .expect("stream should start")
            .collect()
            .await;
        assert!(events.iter().any(|event| matches!(
            &event.kind,
            RunEventKind::ToolExecutionFailed { code, .. }
                if code == "invalid_tool_suspension"
        )));
        assert!(!events.iter().any(|event| matches!(
            &event.kind,
            RunEventKind::ToolExecutionCompleted { call_id, .. }
                if call_id == "call_suspend"
        )));
        assert!(matches!(
            events.last().map(|event| &event.kind),
            Some(RunEventKind::RunCompleted { .. })
        ));
    }

    struct ParallelProvider {
        calls: AtomicUsize,
    }

    impl ModelPort for ParallelProvider {
        fn stream(&self, request: ModelRequest) -> ModelEventStream {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                return Box::pin(stream::iter(vec![
                    ModelEvent::ToolCallStarted {
                        call_id: "call_slow".into(),
                        name: "slow_read".into(),
                    },
                    ModelEvent::ToolCallArgumentsDelta {
                        call_id: "call_slow".into(),
                        delta: "{}".into(),
                    },
                    ModelEvent::ToolCallStarted {
                        call_id: "call_fast".into(),
                        name: "fast_read".into(),
                    },
                    ModelEvent::ToolCallArgumentsDelta {
                        call_id: "call_fast".into(),
                        delta: "{}".into(),
                    },
                    ModelEvent::Completed {
                        finish_reason: FinishReason::ToolCall,
                    },
                ]));
            }

            assert_eq!(
                request.messages[3].tool_call_id.as_deref(),
                Some("call_slow")
            );
            assert_eq!(request.messages[3].content, "slow_read-result");
            assert_eq!(
                request.messages[4].tool_call_id.as_deref(),
                Some("call_fast")
            );
            assert_eq!(request.messages[4].content, "fast_read-result");
            Box::pin(stream::iter(vec![ModelEvent::Completed {
                finish_reason: FinishReason::Stop,
            }]))
        }
    }

    struct ParallelTools {
        active: Arc<AtomicUsize>,
        max_active: Arc<AtomicUsize>,
    }

    impl ToolPort for ParallelTools {
        fn definitions(&self) -> Vec<ToolDefinition> {
            let policy =
                ToolExecutionPolicy::read_only().with_concurrency(ToolConcurrency::ParallelSafe);
            vec![
                ToolDefinition::new("slow_read", "Slow read.", json!({"type": "object"}))
                    .with_execution_policy(policy),
                ToolDefinition::new("fast_read", "Fast read.", json!({"type": "object"}))
                    .with_execution_policy(policy),
            ]
        }

        fn call(&self, request: ToolCallRequest) -> ToolCallFuture {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active.fetch_max(active, Ordering::SeqCst);
            let active_counter = Arc::clone(&self.active);
            let delay = if request.name == "slow_read" { 30 } else { 5 };
            let content = format!("{}-result", request.name);
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(delay)).await;
                active_counter.fetch_sub(1, Ordering::SeqCst);
                Ok(ToolOutput::text(content))
            })
        }
    }

    #[tokio::test]
    async fn operator_agent_overlaps_parallel_safe_tools_and_preserves_result_order() {
        let max_active = Arc::new(AtomicUsize::new(0));
        let harness = Harness::new(
            AgentLoop::new(
                ParallelProvider {
                    calls: AtomicUsize::new(0),
                },
                ParallelTools {
                    active: Arc::new(AtomicUsize::new(0)),
                    max_active: Arc::clone(&max_active),
                },
                "test-model",
                "Use tools when needed.",
                None,
                4,
            )
            .with_tool_call_strategy(ToolCallStrategy::ParallelSafe),
        );

        let events: Vec<_> = harness
            .stream("Read both")
            .expect("stream should start")
            .collect()
            .await;

        assert_eq!(max_active.load(Ordering::SeqCst), 2);
        assert!(matches!(
            events.last().map(|event| &event.kind),
            Some(RunEventKind::RunCompleted { .. })
        ));
    }
}
