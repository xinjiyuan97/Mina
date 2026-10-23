use agent_core::{
    event_runtime::EventId,
    harness::ToolPort,
    operator::{DeliveryClass, OperatorIdentity, OperatorSequence, PayloadContract},
    operator_fabric::*,
    tool_authorization::validate_tool_call,
    tool_preparation::*,
};
use serde_json::json;
use std::{collections::BTreeSet, sync::Arc};

pub struct ToolPreparationOperator {
    fabric: Arc<dyn OperatorFabric>,
    tools: Arc<dyn ToolPort>,
}

fn invalid(error: impl std::fmt::Display) -> FabricError {
    FabricError::Invalid(error.to_string())
}

impl ToolPreparationOperator {
    pub fn new(fabric: Arc<dyn OperatorFabric>, tools: Arc<dyn ToolPort>) -> Self {
        Self { fabric, tools }
    }

    pub async fn execute(&self, delivery: ClaimedDelivery, now_ms: i64) -> Result<(), FabricError> {
        let envelope = &delivery.event.envelope;
        if !matches!(self.fabric.execute(FabricCommand::ReadEvent(envelope.event_id)).await?,
            FabricReply::Event(Some(event)) if event == delivery.event)
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
            return Err(invalid("receipt reply"));
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
        let key = request_key(&envelope.tenant_id, envelope.run_id, &envelope.effect_id);
        if envelope.payload["request_key"].as_str() != Some(&key) {
            return Err(FabricError::Conflict);
        }
        let FabricReply::Checkpoint(Some(saved)) = self
            .fabric
            .execute(FabricCommand::ReadCheckpoint(key))
            .await?
        else {
            return Err(FabricError::NotFound);
        };
        if saved.version != 0 {
            return Err(FabricError::Conflict);
        }
        let request: PreparationRequest = serde_json::from_value(saved.value).map_err(invalid)?;
        if request.tenant_id != envelope.tenant_id
            || request.run_id != envelope.run_id
            || request.effect_id != envelope.effect_id
        {
            return Err(FabricError::Conflict);
        }
        envelope
            .validate(&PayloadContract {
                event_type: request.event_type().into(),
                delivery_class: DeliveryClass::Durable,
                schema: payload_schema(request.event_type()),
                max_payload_bytes: MAX_EVENT_BYTES,
            })
            .map_err(invalid)?;
        let result = self.prepare(&request).await?;
        let prefix = match request.kind {
            PreparationKind::ToolSet => "toolset",
            PreparationKind::Validate { .. } => "tool.validation",
        };
        let suffix = if matches!(result, PreparationResult::Failed { .. }) {
            "failed"
        } else {
            "completed"
        };
        let kind = format!("{prefix}.{suffix}");
        let mut output = delivery.event.clone();
        output.envelope.event_id =
            EventId::stable("tool-preparation-result", &envelope.event_id.to_string());
        output.envelope.event_type = kind.clone();
        output.envelope.causation_id = Some(envelope.event_id);
        output.envelope.operator = OperatorIdentity {
            name: prefix.into(),
            version: 1,
        };
        output.envelope.operator_seq = OperatorSequence(0);
        output.envelope.occurred_at_ms = now_ms;
        output.envelope.payload = json!({"result_key":request.result_key()});
        output.payload_schema = payload_schema(&kind);
        self.fabric
            .execute(FabricCommand::Transact {
                checkpoint: CheckpointWrite {
                    key: request.result_key(),
                    expected: None,
                    value: json!(result),
                },
                outbox: vec![output],
                delivery: Some((delivery.lease, now_ms)),
            })
            .await?;
        Ok(())
    }

    async fn prepare(
        &self,
        request: &PreparationRequest,
    ) -> Result<PreparationResult, FabricError> {
        let fail = |code: &str| PreparationResult::Failed { code: code.into() };
        let current = match self.tools.tool_set_snapshot(request.run_id) {
            Ok(snapshot) => snapshot,
            Err(_) => return Ok(fail("toolset_unavailable")),
        };
        let canonical = agent_core::harness::ToolSetSnapshot::new(
            current.revision,
            current.definitions.clone(),
            current.bindings.clone(),
        );
        if canonical.as_ref().ok() != Some(&current) {
            return Ok(fail("toolset_invalid"));
        }
        match &request.kind {
            PreparationKind::ToolSet => Ok(PreparationResult::ToolSet { snapshot: current }),
            PreparationKind::Validate {
                toolset_effect_id,
                revision,
                calls,
                allowed_tools,
                allow_run_adf,
            } => {
                let key = format!(
                    "{}:result",
                    request_key(&request.tenant_id, request.run_id, toolset_effect_id)
                );
                let FabricReply::Checkpoint(Some(saved)) = self
                    .fabric
                    .execute(FabricCommand::ReadCheckpoint(key))
                    .await?
                else {
                    return Ok(fail("toolset_missing"));
                };
                let locked: PreparationResult =
                    serde_json::from_value(saved.value).map_err(invalid)?;
                let PreparationResult::ToolSet { snapshot } = locked else {
                    return Ok(fail("toolset_failed"));
                };
                if saved.version != 0 || snapshot.revision != *revision || current != snapshot {
                    return Ok(fail("toolset_changed"));
                }
                let mut identities = BTreeSet::new();
                let mut validated = Vec::with_capacity(calls.len());
                for call in calls {
                    if !identities.insert(&call.call_id) {
                        return Ok(fail("duplicate_call_id"));
                    }
                    let Ok(binding) = validate_tool_call(
                        &snapshot,
                        call,
                        allowed_tools.as_deref(),
                        *allow_run_adf,
                    ) else {
                        return Ok(fail("tool_validation_failed"));
                    };
                    if self
                        .tools
                        .validate_at(request.run_id, *revision, &call.name, &call.arguments)
                        .is_err()
                    {
                        return Ok(fail("tool_validation_failed"));
                    }
                    validated.push(binding);
                }
                Ok(PreparationResult::Validated { calls: validated })
            }
        }
    }
}
