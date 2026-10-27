//! Offline P0 comparison views, not executable adapters or projection receipts.

use crate::{
    contracts::events::{EventEnvelope, PublishEvent},
    contracts::{AgentLoopEffect, AgentLoopEffectKind, RunEvent},
    operator::{LogSequence, PublicRunSequence},
};

#[derive(Debug, PartialEq, Eq)]
pub struct EffectTarget<'scope> {
    pub effect_id: &'scope str,
    pub operator: &'static str,
    pub event_type: &'static str,
    pub legacy_phase: &'static str,
}

pub fn effect_target(effect: &AgentLoopEffect) -> EffectTarget<'_> {
    let (operator, event_type, legacy_phase) = match &effect.kind {
        AgentLoopEffectKind::LoadToolSet { .. } => {
            ("toolset", "toolset.requested", "AwaitingToolSet")
        }
        AgentLoopEffectKind::InvokeModel { .. } => ("model", "model.requested", "AwaitingModel"),
        AgentLoopEffectKind::ValidateTools { .. } => (
            "validator",
            "tool.validation.requested",
            "AwaitingValidation",
        ),
        AgentLoopEffectKind::RequestApproval { .. } => {
            ("approval", "approval.requested", "AwaitingApproval")
        }
        AgentLoopEffectKind::InvokeTool { .. } => ("tool", "tool.requested", "AwaitingTools"),
    };
    EffectTarget {
        effect_id: &effect.effect_id,
        operator,
        event_type,
        legacy_phase,
    }
}

pub struct PublishView<'scope> {
    pub original: &'scope PublishEvent,
    pub log_sequence: Option<LogSequence>,
}

pub fn publish_view(event: &PublishEvent) -> PublishView<'_> {
    PublishView {
        original: event,
        log_sequence: None,
    }
}

pub fn stored_event_view(event: &EventEnvelope) -> PublishView<'_> {
    PublishView {
        original: &event.event,
        log_sequence: Some(LogSequence(event.sequence)),
    }
}

pub struct PublicEventView<'scope> {
    pub original: &'scope RunEvent,
    pub sequence: PublicRunSequence,
    pub sse_event_name: &'static str,
}

pub fn public_event_view(event: &RunEvent) -> PublicEventView<'_> {
    PublicEventView {
        original: event,
        sequence: PublicRunSequence(event.seq),
        sse_event_name: event.kind.event_name(),
    }
}
