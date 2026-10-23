use agent_core::{
    event_runtime::EventId,
    harness::{ApprovalId, ApprovalResolution, RunId},
    operator::*,
    operator_fabric::*,
    tool_authorization::{
        ApprovalStatus, AuthorizationError, BoundApproval, approval_payload_schema,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;

#[derive(Clone)]
pub struct ApprovalIngress {
    fabric: Arc<dyn OperatorFabric>,
}

#[derive(Serialize, Deserialize)]
struct StoredApproval {
    tenant: String,
    approval: BoundApproval,
}

fn key(approval_id: ApprovalId) -> String {
    format!("operator-approval:{approval_id}")
}
fn invalid(error: impl std::fmt::Display) -> FabricError {
    FabricError::Invalid(error.to_string())
}

fn event(
    tenant: &str,
    approval: &BoundApproval,
    kind: &str,
    actor: &str,
    now_ms: i64,
) -> FabricEvent {
    let resolution = match &approval.status {
        ApprovalStatus::Resolved { resolution, .. } => Some(resolution),
        _ => None,
    };
    FabricEvent {
        topic: "agent.approval.v1".into(),
        envelope: OperatorEnvelope {
            schema_version: 1,
            engine_version: EngineVersion::OperatorV1,
            projection_version: 1,
            event_id: EventId::stable(
                "operator-approval-v1",
                &format!("{}:{kind}", approval.approval_id),
            ),
            event_type: kind.into(),
            tenant_id: tenant.into(),
            run_id: approval.run_id,
            session_id: None,
            effect_id: approval.effect_id.clone(),
            invocation_id: approval.approval_id.to_string(),
            attempt: 1,
            issued_revision: RunRevision(1),
            lease_epoch: LeaseEpoch(1),
            correlation_id: approval.approval_id.to_string(),
            causation_id: (kind != "approval.requested").then(|| {
                EventId::stable(
                    "operator-approval-v1",
                    &format!("{}:approval.requested", approval.approval_id),
                )
            }),
            operator: OperatorIdentity {
                name: if kind == "approval.resolved" {
                    "gateway"
                } else {
                    "approval"
                }
                .into(),
                version: 1,
            },
            operator_seq: OperatorSequence(0),
            delivery_class: DeliveryClass::Durable,
            occurred_at_ms: now_ms,
            payload: json!({"approval_id":approval.approval_id,"call_id":approval.call.invocation.call_id,"effect_id":approval.effect_id,
                "arguments_digest":approval.call.arguments_digest,"binding_digest":approval.call.binding_digest,
                "tool_set_revision":approval.call.tool_set_revision,"tool_set_digest":approval.call.tool_set_digest,
                "actor":actor,"resolution":resolution,"expires_at_ms":approval.expires_at_ms}),
        },
        payload_schema: approval_payload_schema(kind),
    }
}

impl ApprovalIngress {
    pub(crate) async fn cancellation_write(
        &self,
        tenant: &str,
        run_id: RunId,
        approval_id: ApprovalId,
        now_ms: i64,
    ) -> Result<Option<(CheckpointWrite, FabricEvent)>, FabricError> {
        let (version, mut stored) = self.read(tenant, run_id, approval_id).await?;
        if !stored.approval.cancel() {
            return Ok(None);
        }
        let event = event(
            tenant,
            &stored.approval,
            "approval.cancelled",
            "coordinator",
            now_ms,
        );
        Ok(Some((
            CheckpointWrite {
                key: key(approval_id),
                expected: Some(version),
                value: json!(stored),
            },
            event,
        )))
    }
    pub fn new(fabric: Arc<dyn OperatorFabric>) -> Self {
        Self { fabric }
    }

    pub(crate) fn pending_request(
        tenant: &str,
        approval: &BoundApproval,
    ) -> Result<(CheckpointWrite, FabricEvent), FabricError> {
        if tenant.trim().is_empty() || !matches!(approval.status, ApprovalStatus::Pending) {
            return Err(invalid("invalid approval request"));
        }
        let rebuilt = match approval.reducer_step {
            Some(step) => BoundApproval::for_reducer(
                approval.run_id,
                &approval.effect_id,
                approval.call.clone(),
                approval.requested_at_ms,
                approval.expires_at_ms,
                step,
            ),
            None => BoundApproval::new(
                approval.run_id,
                &approval.effect_id,
                approval.call.clone(),
                approval.requested_at_ms,
                approval.expires_at_ms,
            ),
        }
        .map_err(invalid)?;
        if &rebuilt != approval {
            return Err(FabricError::Conflict);
        }
        Ok((
            CheckpointWrite {
                key: key(approval.approval_id),
                expected: None,
                value: json!(StoredApproval {
                    tenant: tenant.into(),
                    approval: approval.clone()
                }),
            },
            event(
                tenant,
                approval,
                "approval.requested",
                "coordinator",
                approval.requested_at_ms,
            ),
        ))
    }

    pub async fn request(&self, tenant: &str, approval: BoundApproval) -> Result<(), FabricError> {
        loop {
            match self.request_once(tenant, approval.clone()).await {
                Err(FabricError::Fenced) => continue,
                result => return result,
            }
        }
    }

    async fn request_once(&self, tenant: &str, approval: BoundApproval) -> Result<(), FabricError> {
        let (checkpoint, requested) = Self::pending_request(tenant, &approval)?;
        let FabricReply::Checkpoint(existing) = self
            .fabric
            .execute(FabricCommand::ReadCheckpoint(key(approval.approval_id)))
            .await?
        else {
            return Err(invalid("checkpoint reply"));
        };
        if let Some(existing) = existing {
            let existing: StoredApproval =
                serde_json::from_value(existing.value).map_err(invalid)?;
            if existing.tenant != tenant
                || existing.approval.run_id != approval.run_id
                || existing.approval.call != approval.call
                || existing.approval.effect_id != approval.effect_id
                || existing.approval.requested_at_ms != approval.requested_at_ms
                || existing.approval.expires_at_ms != approval.expires_at_ms
            {
                return Err(FabricError::Conflict);
            }
            return Ok(());
        }
        let outbox = vec![requested];
        self.fabric
            .execute(FabricCommand::Transact {
                checkpoint,
                outbox,
                delivery: None,
            })
            .await?;
        Ok(())
    }

    async fn read(
        &self,
        tenant: &str,
        run_id: RunId,
        approval_id: ApprovalId,
    ) -> Result<(u64, StoredApproval), FabricError> {
        let FabricReply::Checkpoint(Some(record)) = self
            .fabric
            .execute(FabricCommand::ReadCheckpoint(key(approval_id)))
            .await?
        else {
            return Err(FabricError::NotFound);
        };
        let stored: StoredApproval = serde_json::from_value(record.value).map_err(invalid)?;
        if stored.tenant != tenant
            || stored.approval.run_id != run_id
            || stored.approval.approval_id != approval_id
        {
            return Err(FabricError::NotFound);
        }
        Ok((record.version, stored))
    }

    pub async fn load(
        &self,
        tenant: &str,
        run_id: RunId,
        approval_id: ApprovalId,
    ) -> Result<BoundApproval, FabricError> {
        Ok(self.read(tenant, run_id, approval_id).await?.1.approval)
    }

    pub async fn resolve(
        &self,
        tenant: &str,
        run_id: RunId,
        approval_id: ApprovalId,
        actor: &str,
        resolution: ApprovalResolution,
        now_ms: i64,
    ) -> Result<bool, FabricError> {
        if actor.trim().is_empty() {
            return Err(invalid("actor required"));
        }
        self.expire(tenant, run_id, approval_id, now_ms).await?;
        self.change(
            tenant,
            run_id,
            approval_id,
            actor,
            now_ms,
            "approval.resolved",
            |approval| {
                approval
                    .resolve(actor, &approval.call.clone(), resolution.clone(), now_ms)
                    .map_err(|error| match error {
                        AuthorizationError::Conflict => FabricError::Conflict,
                        other => invalid(other),
                    })
            },
        )
        .await
    }

    pub async fn expire(
        &self,
        tenant: &str,
        run_id: RunId,
        approval_id: ApprovalId,
        now_ms: i64,
    ) -> Result<bool, FabricError> {
        self.change(
            tenant,
            run_id,
            approval_id,
            "coordinator",
            now_ms,
            "approval.expired",
            |approval| Ok(approval.expire(now_ms)),
        )
        .await
    }

    pub async fn cancel(
        &self,
        tenant: &str,
        run_id: RunId,
        approval_id: ApprovalId,
        actor: &str,
        now_ms: i64,
    ) -> Result<bool, FabricError> {
        self.change(
            tenant,
            run_id,
            approval_id,
            actor,
            now_ms,
            "approval.cancelled",
            |approval| Ok(approval.cancel()),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn change(
        &self,
        tenant: &str,
        run_id: RunId,
        approval_id: ApprovalId,
        actor: &str,
        now_ms: i64,
        kind: &str,
        change: impl Fn(&mut BoundApproval) -> Result<bool, FabricError>,
    ) -> Result<bool, FabricError> {
        if actor.trim().is_empty() {
            return Err(invalid("actor required"));
        }
        loop {
            let (version, mut stored) = self.read(tenant, run_id, approval_id).await?;
            if !change(&mut stored.approval)? {
                return Ok(false);
            }
            let outbox = vec![event(tenant, &stored.approval, kind, actor, now_ms)];
            match self
                .fabric
                .execute(FabricCommand::Transact {
                    checkpoint: CheckpointWrite {
                        key: key(approval_id),
                        expected: Some(version),
                        value: json!(stored),
                    },
                    outbox,
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
}
