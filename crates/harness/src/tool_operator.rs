pub use agent_core::tool_operator::ToolRequest;
use agent_core::{
    event_runtime::EventId,
    harness::*,
    operator::{DeliveryClass, OperatorIdentity, OperatorSequence, PayloadContract},
    operator_fabric::*,
    tool_authorization::validate_tool_call,
    tool_operator::{
        MAX_TOOL_OUTPUT_BYTES, ToolExecutionRecord, ToolExecutionResult, payload_schema,
        request_key,
    },
};
use serde_json::json;
use std::{sync::Arc, time::Duration};

pub struct ToolOperator {
    fabric: Arc<dyn OperatorFabric>,
    tools: Arc<dyn ToolPort>,
}

pub struct ExecutionClaim {
    version: u64,
    record: ToolExecutionRecord,
    uncertain: bool,
}

fn invalid(error: impl std::fmt::Display) -> FabricError {
    FabricError::Invalid(error.to_string())
}

fn failed(code: &str, category: ToolErrorCategory, retryable: bool) -> ToolExecutionResult {
    ToolExecutionResult::Finished {
        result: ToolInvocationResult::Failed {
            error: PortableToolError {
                code: code.into(),
                message: code.into(),
                category,
                retryable,
                retry_after_ms: None,
            },
        },
    }
}

fn fact(
    delivery: &ClaimedDelivery,
    request: &ToolRequest,
    kind: &str,
    code: Option<&str>,
    now_ms: i64,
) -> FabricEvent {
    let mut envelope = delivery.event.envelope.clone();
    envelope.event_id = EventId::stable(
        "tool-operator-v1",
        &format!("{}:{kind}:{}", envelope.event_id, delivery.lease.epoch),
    );
    envelope.event_type = kind.into();
    envelope.causation_id = Some(delivery.event.envelope.event_id);
    envelope.operator = OperatorIdentity {
        name: "tool".into(),
        version: 1,
    };
    envelope.operator_seq = OperatorSequence(0);
    envelope.occurred_at_ms = now_ms;
    envelope.payload = json!({"result_key":request.execution_key(),"call_id":request.validated.invocation.call_id,"code":code});
    FabricEvent {
        topic: delivery.event.topic.clone(),
        envelope,
        payload_schema: payload_schema(kind),
    }
}

impl ToolOperator {
    pub fn new(fabric: Arc<dyn OperatorFabric>, tools: Arc<dyn ToolPort>) -> Self {
        Self { fabric, tools }
    }

    pub async fn begin(
        &self,
        delivery: &ClaimedDelivery,
        now_ms: i64,
    ) -> Result<Option<ExecutionClaim>, FabricError> {
        self.verify_delivery(delivery).await?;
        let envelope = &delivery.event.envelope;
        envelope
            .validate(&PayloadContract {
                event_type: "tool.requested".into(),
                delivery_class: DeliveryClass::Durable,
                schema: payload_schema("tool.requested"),
                max_payload_bytes: MAX_EVENT_BYTES,
            })
            .map_err(invalid)?;
        let identity = request_key(&envelope.tenant_id, envelope.run_id, &envelope.effect_id);
        if envelope.payload["request_key"].as_str() != Some(&identity) {
            return Err(FabricError::Conflict);
        }
        let FabricReply::Checkpoint(Some(checkpoint)) = self
            .fabric
            .execute(FabricCommand::ReadCheckpoint(identity))
            .await?
        else {
            return Err(FabricError::NotFound);
        };
        if checkpoint.version != 0 {
            return Err(FabricError::Conflict);
        }
        let request: ToolRequest = serde_json::from_value(checkpoint.value).map_err(invalid)?;
        if request.tenant_id != envelope.tenant_id
            || request.run_id != envelope.run_id
            || request.effect_id != envelope.effect_id
        {
            return Err(FabricError::Conflict);
        }
        {
            let FabricReply::Checkpoint(previous) = self
                .fabric
                .execute(FabricCommand::ReadCheckpoint(request.execution_key()))
                .await?
            else {
                return Err(invalid("checkpoint reply"));
            };
            let expected = previous.as_ref().map(|record| record.version);
            let mut uncertain = false;
            let attempts = if let Some(previous) = previous {
                let record: ToolExecutionRecord =
                    serde_json::from_value(previous.value).map_err(invalid)?;
                if record.request != request {
                    return Err(FabricError::Conflict);
                }
                if record.result.is_some() {
                    return Ok(Some(ExecutionClaim {
                        version: previous.version,
                        record,
                        uncertain: false,
                    }));
                }
                if record.lease.expires_at_ms > now_ms {
                    return Ok(None);
                }
                uncertain = !request
                    .validated
                    .invocation
                    .execution
                    .idempotency
                    .permits_automatic_retry();
                record.attempts.saturating_add(1)
            } else {
                1
            };
            let record = ToolExecutionRecord {
                request: request.clone(),
                lease: delivery.lease.clone(),
                attempts,
                result: None,
            };
            match self
                .fabric
                .execute(FabricCommand::GuardedTransact {
                    lease: delivery.lease.clone(),
                    now_ms,
                    checkpoint: CheckpointWrite {
                        key: request.execution_key(),
                        expected,
                        value: json!(record),
                    },
                    outbox: vec![],
                })
                .await
            {
                Ok(FabricReply::Checkpoint(Some(saved))) => Ok(Some(ExecutionClaim {
                    version: saved.version,
                    record,
                    uncertain,
                })),
                Err(error) => Err(error),
                _ => Err(invalid("transaction reply")),
            }
        }
    }

    pub async fn execute(
        &self,
        delivery: ClaimedDelivery,
        clock: impl Fn() -> i64,
    ) -> Result<(), FabricError> {
        let FabricReply::Receipt(receipt) = self
            .fabric
            .execute(FabricCommand::Receipt(ReceiptKey::for_event(
                &delivery.lease.consumer,
                &delivery.event,
            )))
            .await?
        else {
            return Err(invalid("receipt reply"));
        };
        if let Some(receipt) = receipt {
            self.fabric
                .execute(FabricCommand::Commit {
                    lease: delivery.lease,
                    now_ms: clock(),
                    result: receipt.result,
                    outbox: receipt.outbox,
                })
                .await?;
            return Ok(());
        }
        let Some(mut claim) = self.begin(&delivery, clock()).await? else {
            return Ok(());
        };
        let request = claim.record.request.clone();
        let policy = request.validated.invocation.execution;
        let previously_finished = claim.record.result.is_some();
        let outcome = if let Some(result) = claim.record.result.clone() {
            result
        } else if claim.uncertain {
            ToolExecutionResult::Uncertain {
                code: "execution_interrupted".into(),
            }
        } else if claim.record.attempts > policy.retry.max_attempts {
            failed(
                "tool_retry_exhausted",
                ToolErrorCategory::Unavailable,
                false,
            )
        } else if let Err(code) = self
            .preflight(&delivery.event.envelope.tenant_id, &request, clock())
            .await
        {
            failed(&code, ToolErrorCategory::PermissionDenied, false)
        } else {
            loop {
                let remaining_ms = delivery.lease.expires_at_ms.saturating_sub(clock());
                if remaining_ms <= 0 {
                    return Err(FabricError::Fenced);
                }
                let cancellation = RunCancellation::new();
                let call = &request.validated.invocation;
                let result = tokio::time::timeout(
                    Duration::from_millis(request.timeout_ms.min(remaining_ms as u64)),
                    self.tools.call_at(
                        request.validated.tool_set_revision,
                        ToolCallRequest {
                            run_id: request.run_id,
                            call_id: call.call_id.clone(),
                            name: call.name.clone(),
                            arguments: call.arguments.clone(),
                            cancellation,
                        },
                    ),
                )
                .await;
                let retryable = match &result {
                    Err(_) => true,
                    Ok(Err(error)) => error.retryable(),
                    Ok(Ok(_)) => false,
                };
                if retryable
                    && policy.idempotency.permits_automatic_retry()
                    && claim.record.attempts < policy.retry.max_attempts
                {
                    let delay = match &result {
                        Ok(Err(error)) => error
                            .retry_after_ms()
                            .unwrap_or_else(|| {
                                policy.retry.delay_for_attempt(claim.record.attempts)
                            })
                            .min(policy.retry.max_backoff_ms),
                        _ => policy.retry.delay_for_attempt(claim.record.attempts),
                    };
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                    claim.record.attempts += 1;
                    let FabricReply::Checkpoint(Some(saved)) = self
                        .fabric
                        .execute(FabricCommand::GuardedTransact {
                            lease: delivery.lease.clone(),
                            now_ms: clock(),
                            checkpoint: CheckpointWrite {
                                key: request.execution_key(),
                                expected: Some(claim.version),
                                value: json!(claim.record),
                            },
                            outbox: vec![],
                        })
                        .await?
                    else {
                        return Err(invalid("retry transaction"));
                    };
                    claim.version = saved.version;
                    if let Err(code) = self
                        .preflight(&delivery.event.envelope.tenant_id, &request, clock())
                        .await
                    {
                        break failed(&code, ToolErrorCategory::PermissionDenied, false);
                    }
                    continue;
                }
                break match result {
                    Err(_) if !policy.idempotency.permits_automatic_retry() => {
                        ToolExecutionResult::Uncertain {
                            code: "tool_timeout".into(),
                        }
                    }
                    Err(_) => failed("tool_timeout", ToolErrorCategory::Timeout, true),
                    Ok(Err(error)) => ToolExecutionResult::Finished {
                        result: ToolInvocationResult::Failed {
                            error: PortableToolError {
                                code: error.code().into(),
                                message: error.safe_message().into(),
                                category: error.category(),
                                retryable: error.retryable(),
                                retry_after_ms: error.retry_after_ms(),
                            },
                        },
                    },
                    Ok(Ok(output)) => {
                        if let Some(suspension) = output.suspension {
                            if policy.completion != ToolCompletion::MaySuspend
                                || suspension.waits.is_empty()
                                || suspension
                                    .effects
                                    .iter()
                                    .any(|effect| effect.validate().is_err())
                            {
                                failed(
                                    "invalid_tool_suspension",
                                    ToolErrorCategory::InvalidRequest,
                                    false,
                                )
                            } else {
                                ToolExecutionResult::Finished {
                                    result: ToolInvocationResult::Suspended { suspension },
                                }
                            }
                        } else if output.content.len() > MAX_TOOL_OUTPUT_BYTES {
                            failed(
                                "tool_output_capacity",
                                ToolErrorCategory::ResourceExhausted,
                                false,
                            )
                        } else {
                            ToolExecutionResult::Finished {
                                result: ToolInvocationResult::Completed {
                                    content: output.content,
                                },
                            }
                        }
                    }
                };
            }
        };
        let (kind, code) = match &outcome {
            ToolExecutionResult::Finished {
                result: ToolInvocationResult::Completed { .. },
            } => ("tool.completed", None),
            ToolExecutionResult::Finished {
                result: ToolInvocationResult::Failed { error },
            } => ("tool.failed", Some(error.code.as_str())),
            ToolExecutionResult::Finished {
                result: ToolInvocationResult::Suspended { .. },
            } => ("tool.suspended", None),
            ToolExecutionResult::Uncertain { code } => {
                ("operator.execution.uncertain", Some(code.as_str()))
            }
            ToolExecutionResult::Cancelled => ("tool.cancelled", None),
        };
        let output = fact(&delivery, &request, kind, code, clock());
        claim.record.result = Some(outcome);
        claim.record.lease = delivery.lease.clone();
        let committed = self
            .fabric
            .execute(FabricCommand::Transact {
                checkpoint: CheckpointWrite {
                    key: request.execution_key(),
                    expected: Some(claim.version),
                    value: json!(claim.record),
                },
                outbox: if previously_finished {
                    vec![]
                } else {
                    vec![output]
                },
                delivery: Some((delivery.lease.clone(), clock())),
            })
            .await;
        if matches!(committed, Err(FabricError::Fenced)) {
            let audit = fact(
                &delivery,
                &request,
                "operator.result.stale",
                Some("execution_fenced"),
                clock(),
            );
            let audit_key = format!("tool-audit:{}", audit.envelope.event_id);
            match self
                .fabric
                .execute(FabricCommand::Transact {
                    checkpoint: CheckpointWrite {
                        key: audit_key,
                        expected: None,
                        value: audit.envelope.payload.clone(),
                    },
                    outbox: vec![audit],
                    delivery: None,
                })
                .await
            {
                Ok(_) | Err(FabricError::Fenced) => {}
                Err(error) => return Err(error),
            }
        }
        committed?;
        Ok(())
    }

    pub async fn cancel_requested(
        &self,
        delivery: ClaimedDelivery,
        now_ms: i64,
    ) -> Result<(), FabricError> {
        self.verify_delivery(&delivery).await?;
        let envelope = &delivery.event.envelope;
        if envelope.event_type != "tool.cancel.requested" || envelope.operator.name != "coordinator"
        {
            return Err(FabricError::Conflict);
        }
        let FabricReply::Receipt(receipt) = self
            .fabric
            .execute(FabricCommand::Receipt(ReceiptKey::for_event(
                &delivery.lease.consumer,
                &delivery.event,
            )))
            .await?
        else {
            return Err(FabricError::Conflict);
        };
        if let Some(receipt) = receipt {
            self.fabric
                .execute(FabricCommand::Commit {
                    lease: delivery.lease,
                    now_ms,
                    result: receipt.result,
                    outbox: receipt.outbox,
                })
                .await?;
            return Ok(());
        }
        let FabricReply::Checkpoint(Some(saved)) = self
            .fabric
            .execute(FabricCommand::ReadCheckpoint(request_key(
                &envelope.tenant_id,
                envelope.run_id,
                &envelope.effect_id,
            )))
            .await?
        else {
            return Err(FabricError::NotFound);
        };
        let request: ToolRequest = serde_json::from_value(saved.value).map_err(invalid)?;
        if request.tenant_id != envelope.tenant_id
            || request.run_id != envelope.run_id
            || request.effect_id != envelope.effect_id
            || envelope.payload["result_key"].as_str() != Some(&request.execution_key())
        {
            return Err(FabricError::Conflict);
        }
        {
            let FabricReply::Checkpoint(saved) = self
                .fabric
                .execute(FabricCommand::ReadCheckpoint(request.execution_key()))
                .await?
            else {
                return Err(FabricError::Conflict);
            };
            let expected = saved.as_ref().map(|saved| saved.version);
            let mut execution = match saved {
                Some(saved) => {
                    serde_json::from_value::<ToolExecutionRecord>(saved.value).map_err(invalid)?
                }
                None => ToolExecutionRecord {
                    request: request.clone(),
                    lease: delivery.lease.clone(),
                    attempts: 0,
                    result: None,
                },
            };
            if execution.result.is_some() {
                self.fabric
                    .execute(FabricCommand::Commit {
                        lease: delivery.lease,
                        now_ms,
                        result: json!({"already_finished":true}),
                        outbox: vec![],
                    })
                    .await?;
                return Ok(());
            }
            execution.result = Some(ToolExecutionResult::Cancelled);
            let event = fact(
                &delivery,
                &request,
                "tool.cancelled",
                Some("run_cancelled"),
                now_ms,
            );
            self.fabric
                .execute(FabricCommand::Transact {
                    checkpoint: CheckpointWrite {
                        key: request.execution_key(),
                        expected,
                        value: json!(execution),
                    },
                    outbox: vec![event],
                    delivery: Some((delivery.lease.clone(), now_ms)),
                })
                .await
                .map(|_| ())
        }
    }

    pub async fn cancel(
        &self,
        delivery: &ClaimedDelivery,
        actor: &str,
        now_ms: i64,
    ) -> Result<bool, FabricError> {
        self.verify_delivery(delivery).await?;
        if actor.trim().is_empty() {
            return Err(invalid("actor required"));
        }
        let envelope = &delivery.event.envelope;
        let FabricReply::Checkpoint(Some(saved)) = self
            .fabric
            .execute(FabricCommand::ReadCheckpoint(request_key(
                &envelope.tenant_id,
                envelope.run_id,
                &envelope.effect_id,
            )))
            .await?
        else {
            return Err(FabricError::NotFound);
        };
        let request: ToolRequest = serde_json::from_value(saved.value).map_err(invalid)?;
        if request.tenant_id != envelope.tenant_id
            || request.run_id != envelope.run_id
            || request.effect_id != envelope.effect_id
        {
            return Err(FabricError::Conflict);
        }
        loop {
            let FabricReply::Checkpoint(saved) = self
                .fabric
                .execute(FabricCommand::ReadCheckpoint(request.execution_key()))
                .await?
            else {
                return Err(invalid("checkpoint reply"));
            };
            let Some(saved) = saved else {
                self.begin(delivery, now_ms).await?;
                continue;
            };
            let mut record: ToolExecutionRecord =
                serde_json::from_value(saved.value).map_err(invalid)?;
            if record.result.is_some() {
                return Ok(false);
            }
            record.result = Some(ToolExecutionResult::Cancelled);
            let mut intent = fact(delivery, &request, "tool.cancel.requested", None, now_ms);
            intent.envelope.operator.name = actor.into();
            let cancelled = fact(delivery, &request, "tool.cancelled", None, now_ms);
            match self
                .fabric
                .execute(FabricCommand::Transact {
                    checkpoint: CheckpointWrite {
                        key: request.execution_key(),
                        expected: Some(saved.version),
                        value: json!(record),
                    },
                    outbox: vec![intent, cancelled],
                    delivery: None,
                })
                .await
            {
                Err(FabricError::Fenced) => continue,
                Err(error) => return Err(error),
                _ => return Ok(true),
            }
        }
    }

    async fn verify_delivery(&self, delivery: &ClaimedDelivery) -> Result<(), FabricError> {
        match self
            .fabric
            .execute(FabricCommand::ReadEvent(delivery.event.envelope.event_id))
            .await?
        {
            FabricReply::Event(Some(event)) if event == delivery.event => Ok(()),
            _ => Err(FabricError::Conflict),
        }
    }

    async fn preflight(
        &self,
        tenant: &str,
        request: &ToolRequest,
        now_ms: i64,
    ) -> Result<(), String> {
        if let FabricReply::Checkpoint(Some(saved)) = self
            .fabric
            .execute(FabricCommand::ReadCheckpoint(
                agent_core::effect_pipeline::run_key(tenant, request.run_id),
            ))
            .await
            .map_err(|error| error.to_string())?
        {
            let run: agent_core::effect_pipeline::EffectRun =
                serde_json::from_value(saved.value).map_err(|error| error.to_string())?;
            if !run
                .reducer
                .pending_effects()
                .iter()
                .any(|effect| effect.effect_id == request.effect_id)
            {
                return Err("run_effect_fenced".into());
            }
        }
        let snapshot = self
            .tools
            .tool_set_snapshot(request.run_id)
            .map_err(|error| error.code().to_owned())?;
        let validated = validate_tool_call(
            &snapshot,
            &ToolValidationRequest {
                call_id: request.validated.invocation.call_id.clone(),
                name: request.validated.invocation.name.clone(),
                arguments: request.validated.invocation.arguments.clone(),
            },
            None,
            true,
        )
        .map_err(|error| error.to_string())?;
        if validated != request.validated {
            return Err("tool_revision_changed".into());
        }
        if request
            .approval_policy
            .requires_review(validated.invocation.risk_level)
        {
            let approval = request.approval.as_ref().ok_or("approval_required")?;
            if approval.run_id != request.run_id {
                return Err("approval_run_mismatch".into());
            }
            let authoritative = crate::approval_operator::ApprovalIngress::new(self.fabric.clone())
                .load(tenant, request.run_id, approval.approval_id)
                .await
                .map_err(|error| error.to_string())?;
            if authoritative.effect_id != approval.effect_id {
                return Err("approval_effect_mismatch".into());
            }
            authoritative
                .authorize(&validated, now_ms)
                .map_err(|error| error.to_string())?;
        }
        self.tools
            .validate_at(
                request.run_id,
                validated.tool_set_revision,
                &validated.invocation.name,
                &validated.invocation.arguments,
            )
            .map_err(|error| error.code().to_owned())
    }
}
