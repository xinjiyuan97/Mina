use std::{collections::BTreeSet, time::SystemTime};

use agent_core::{
    event_runtime::{
        CreateSubscription, DeliveryTarget, EventFilter, EventSource, StartPosition,
        SubscriptionId, SubscriptionMode, SubscriptionOwner, SubscriptionScope,
    },
    harness::{
        EffectRequest, JobId, StartJob, Tool, ToolArgumentVisibility, ToolCallFuture,
        ToolCallRequest, ToolCompletion, ToolDefinition, ToolError, ToolErrorCategory,
        ToolExecutionPolicy, ToolOutput, ToolRiskLevel, WaitSpec,
    },
};
use serde::Deserialize;
use serde_json::{Value, json};

/// Host-configured bridge from one model tool call to the durable Job runtime.
///
/// The tool only declares a wait and a StartJob effect. The Flow runtime
/// commits both atomically after the AgentMachine checkpoints, so no job can
/// finish before its completion subscription exists.
#[derive(Debug, Clone)]
pub struct AsyncJobTool {
    allowed_kinds: BTreeSet<String>,
}

impl AsyncJobTool {
    #[must_use]
    pub fn new(allowed_kinds: impl IntoIterator<Item = String>) -> Self {
        Self {
            allowed_kinds: allowed_kinds.into_iter().collect(),
        }
    }

    #[must_use]
    pub const fn allowed_kinds(&self) -> &BTreeSet<String> {
        &self.allowed_kinds
    }
}

impl Tool for AsyncJobTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition::new(
            "async_job",
            "Submit one host-approved durable background job and suspend this tool call until job.completed or job.failed arrives. Use this only for work that may outlive the current process; the job kind must be in the host allowlist.",
            json!({
                "type": "object",
                "properties": {
                    "kind": {
                        "type": "string",
                        "enum": self.allowed_kinds.iter().collect::<Vec<_>>(),
                        "description": "Host-registered job kind."
                    },
                    "input": {
                        "description": "JSON input passed to the selected job worker."
                    },
                    "idempotency_key": {
                        "type": "string",
                        "minLength": 1,
                        "maxLength": 256,
                        "description": "Optional stable key for logical retries. Defaults to this model tool-call id."
                    }
                },
                "required": ["kind", "input"],
                "additionalProperties": false
            }),
        )
        .with_risk_level(ToolRiskLevel::Medium)
        .with_execution_policy(
            ToolExecutionPolicy::idempotent().with_completion(ToolCompletion::MaySuspend),
        )
    }

    fn argument_visibility(&self) -> ToolArgumentVisibility {
        ToolArgumentVisibility::Full
    }

    fn call(&self, request: ToolCallRequest) -> ToolCallFuture {
        let allowed_kinds = self.allowed_kinds.clone();
        Box::pin(async move {
            if request.cancellation.is_cancelled() {
                return Err(ToolError::new(
                    "async_job_cancelled",
                    "the async job request was cancelled",
                    false,
                )
                .with_category(ToolErrorCategory::Cancelled));
            }
            let arguments: AsyncJobArguments = serde_json::from_value(request.arguments)
                .map_err(|_| invalid_arguments("async_job arguments are invalid"))?;
            if !allowed_kinds.contains(&arguments.kind) {
                return Err(ToolError::new(
                    "job_kind_not_allowed",
                    "the requested job kind is not in the host allowlist",
                    false,
                )
                .with_category(ToolErrorCategory::PermissionDenied));
            }

            let idempotency_key = arguments
                .idempotency_key
                .unwrap_or_else(|| request.call_id.clone());
            if idempotency_key.trim().is_empty() || idempotency_key.len() > 256 {
                return Err(invalid_arguments(
                    "async_job idempotency_key must contain 1 to 256 bytes",
                ));
            }
            let requested_at_ms = unix_time_ms();
            let stable_key = format!("{}:{}:{}", request.run_id, arguments.kind, idempotency_key);
            let job_id = JobId::stable("async-job", &stable_key);
            let command = StartJob {
                job_id,
                run_id: request.run_id,
                kind: arguments.kind,
                input: arguments.input,
                idempotency_key,
                requested_at_ms,
            };
            command.validate().map_err(|error| {
                ToolError::new("async_job_invalid_request", error.to_string(), false)
                    .with_category(ToolErrorCategory::InvalidRequest)
            })?;

            let correlation_id = job_id.to_string();
            let wait_key = format!("job:{correlation_id}");
            Ok(ToolOutput::suspend(
                vec![WaitSpec {
                    wait_key: wait_key.clone(),
                    subscription: CreateSubscription {
                        subscription_id: SubscriptionId::stable(
                            "async-job-result",
                            &correlation_id,
                        ),
                        owner: SubscriptionOwner::Run {
                            run_id: request.run_id,
                        },
                        scope: SubscriptionScope::Run {
                            run_id: request.run_id,
                        },
                        filter: EventFilter {
                            topics: vec!["job.completed".into(), "job.failed".into()],
                            sources: vec![EventSource::JobWorker],
                            subject: Some(format!("run/{}", request.run_id)),
                            correlation_id: Some(correlation_id),
                            ..EventFilter::default()
                        },
                        delivery: DeliveryTarget::WakeRun {
                            run_id: request.run_id,
                            wait_key,
                        },
                        mode: SubscriptionMode::Once,
                        start_position: StartPosition::Now,
                        expires_at_ms: None,
                        max_deliveries: Some(1),
                        created_at_ms: requested_at_ms,
                    },
                }],
                vec![EffectRequest::StartJob { command }],
            ))
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AsyncJobArguments {
    kind: String,
    input: Value,
    #[serde(default)]
    idempotency_key: Option<String>,
}

fn invalid_arguments(message: &str) -> ToolError {
    ToolError::new("async_job_invalid_arguments", message, false)
        .with_category(ToolErrorCategory::InvalidRequest)
}

fn unix_time_ms() -> i64 {
    SystemTime::UNIX_EPOCH
        .elapsed()
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use agent_core::harness::{RunCancellation, RunId};

    use super::*;

    #[tokio::test]
    async fn produces_one_atomic_job_wait_and_effect() {
        let tool = AsyncJobTool::new(["builtin.delay".into()]);
        let run_id = RunId::new();
        let output = tool
            .call(ToolCallRequest {
                run_id,
                call_id: "call_job".into(),
                name: "async_job".into(),
                arguments: json!({
                    "kind": "builtin.delay",
                    "input": {"delay_ms": 1, "value": {"answer": 42}},
                    "idempotency_key": "logical-job-1"
                }),
                cancellation: RunCancellation::new(),
            })
            .await
            .expect("allowed job should suspend");
        assert!(output.content.is_empty());
        let suspension = output.suspension.expect("job should suspend");
        assert_eq!(suspension.waits.len(), 1);
        assert_eq!(suspension.effects.len(), 1);
        let EffectRequest::StartJob { command } = &suspension.effects[0] else {
            panic!("job tool must submit a StartJob effect");
        };
        assert_eq!(command.run_id, run_id);
        assert_eq!(command.kind, "builtin.delay");
        assert_eq!(command.idempotency_key, "logical-job-1");
        assert_eq!(
            suspension.waits[0].subscription.filter.correlation_id,
            Some(command.job_id.to_string())
        );
        assert_eq!(tool.definition().risk_level, ToolRiskLevel::Medium);
    }

    #[tokio::test]
    async fn rejects_job_kinds_outside_the_host_allowlist() {
        let tool = AsyncJobTool::new(["builtin.delay".into()]);
        let error = tool
            .call(ToolCallRequest {
                run_id: RunId::new(),
                call_id: "call_job".into(),
                name: "async_job".into(),
                arguments: json!({"kind": "shell.unbounded", "input": {}}),
                cancellation: RunCancellation::new(),
            })
            .await
            .expect_err("unknown job kind must fail closed");
        assert_eq!(error.code(), "job_kind_not_allowed");
        assert_eq!(error.category(), ToolErrorCategory::PermissionDenied);
    }
}
