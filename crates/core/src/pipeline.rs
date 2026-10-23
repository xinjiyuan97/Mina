use std::{collections::BTreeMap, future::Future, pin::Pin};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    harness::{AgentLoopReducerState, BlobId, FinishReason, RunEvent, RunId, TokenUsage},
    operator::OperatorEnvelope,
};

pub const PIPELINE_TOPIC: &str = "agent.pipeline.v1";
pub const MAX_OUTPUT_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_DELTA_BYTES: usize = 32 * 1024;
pub const MAX_DELTAS: usize = 131_072;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PipelineError {
    #[error("stale revision, attempt or lease")]
    Fenced,
    #[error("conflicting durable fact")]
    Conflict,
    #[error("output gap or lag")]
    Gap,
    #[error("output exceeds bounded capacity")]
    Capacity,
    #[error("unsupported operator_v1 profile")]
    UnsupportedProfile,
    #[error("pipeline storage: {0}")]
    Storage(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvocationToken {
    pub effect_id: String,
    pub invocation_id: String,
    pub attempt: u32,
    pub issued_revision: u64,
    pub lease_epoch: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunPhase {
    Preparing,
    AwaitingModel,
    OutputDraining,
    Terminal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Control {
    ContextCompleted,
    ContextFailed { code: String },
    ModelStarted,
    ModelCompleted,
    ModelFailed { code: String },
    OutputSealed,
    OutputFailed { code: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Intent {
    RunStarted,
    ModelRequested,
    RunCompleted,
    RunFailed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunLedger {
    pub run_id: RunId,
    pub revision: u64,
    pub phase: RunPhase,
    pub token: InvocationToken,
    pub context_completed: bool,
    pub model_started: bool,
    pub model_done: bool,
    pub output_done: bool,
    pub error: Option<String>,
    pub inbox: Vec<Control>,
}

impl RunLedger {
    pub fn new(run_id: RunId, effect_id: String, invocation_id: String) -> Self {
        Self {
            run_id,
            revision: 0,
            phase: RunPhase::Preparing,
            token: InvocationToken {
                effect_id,
                invocation_id,
                attempt: 1,
                issued_revision: 1,
                lease_epoch: 1,
            },
            context_completed: false,
            model_started: false,
            model_done: false,
            output_done: false,
            error: None,
            inbox: Vec::new(),
        }
    }

    pub fn reduce(
        &mut self,
        expected: u64,
        token: &InvocationToken,
        fact: Control,
    ) -> Result<Vec<Intent>, PipelineError> {
        if expected != self.revision || token != &self.token {
            return Err(PipelineError::Fenced);
        }
        if self.inbox.contains(&fact) {
            return Ok(Vec::new());
        }
        if self.phase == RunPhase::Terminal {
            return Err(PipelineError::Conflict);
        }
        let mut intents = Vec::new();
        match &fact {
            Control::ContextFailed { code } => {
                if self.context_completed {
                    return Err(PipelineError::Conflict);
                }
                self.error = Some(code.clone());
                self.model_done = true;
                self.output_done = true;
                intents.push(Intent::RunStarted);
            }
            Control::ContextCompleted => {
                self.context_completed = true;
                self.phase = RunPhase::AwaitingModel;
                intents.extend([Intent::RunStarted, Intent::ModelRequested]);
            }
            _ if !self.context_completed => return Err(PipelineError::Conflict),
            Control::ModelStarted => self.model_started = true,
            Control::ModelCompleted | Control::ModelFailed { .. } => {
                if self.model_done {
                    return Err(PipelineError::Conflict);
                }
                self.model_done = true;
                self.phase = RunPhase::OutputDraining;
                if let Control::ModelFailed { code } = &fact
                    && self.error.is_none()
                {
                    self.error = Some(code.clone());
                }
            }
            Control::OutputSealed | Control::OutputFailed { .. } => {
                if self.output_done {
                    return Err(PipelineError::Conflict);
                }
                self.output_done = true;
                if let Control::OutputFailed { code } = &fact {
                    self.error = Some(code.clone());
                }
            }
        }
        if self.model_done && self.output_done {
            self.phase = RunPhase::Terminal;
            intents.push(if self.error.is_some() {
                Intent::RunFailed
            } else {
                Intent::RunCompleted
            });
        }
        self.inbox.push(fact);
        self.revision += 1;
        Ok(intents)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DeltaChannel {
    Text,
    Reasoning,
    ToolCall { call_id: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeltaItem {
    pub invocation_id: String,
    pub delta_seq: u64,
    pub channel: DeltaChannel,
    pub fragment: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event_type")]
pub enum DeltaMessage {
    #[serde(rename = "model.delta")]
    Single { item: DeltaItem },
    #[serde(rename = "model.delta.batch")]
    Batch { items: Vec<DeltaItem> },
}

impl DeltaMessage {
    pub fn into_items(self) -> Vec<DeltaItem> {
        match self {
            Self::Single { item } => vec![item],
            Self::Batch { items } => items,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputSegment {
    pub first_seq: u64,
    pub last_seq: u64,
    pub channel: DeltaChannel,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputBatch {
    pub batch_id: String,
    pub first_seq: u64,
    pub last_seq: u64,
    pub segments: Vec<OutputSegment>,
    pub coverage_digest: String,
    pub artifact_ref: BlobId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelFinal {
    pub model_error: Option<crate::harness::ModelError>,
    pub journal: Vec<DeltaItem>,
    pub coverage_digest: String,
    pub usage: Option<TokenUsage>,
    pub finish_reason: FinishReason,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinalRef {
    pub model_error: Option<crate::harness::ModelError>,
    pub artifact_ref: BlobId,
    pub final_delta_seq: u64,
    pub coverage_digest: String,
    pub usage: Option<TokenUsage>,
    pub finish_reason: FinishReason,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelRequested {
    pub request_ref: BlobId,
}

pub fn coverage(items: &[DeltaItem]) -> String {
    let mut digest = Sha256::new();
    for item in items {
        let encoded = serde_json::to_vec(item).expect("delta is serializable");
        digest.update((encoded.len() as u64).to_be_bytes());
        digest.update(encoded);
    }
    format!("sha256:{:x}", digest.finalize())
}

pub struct OutputBuffer {
    invocation: String,
    items: BTreeMap<u64, DeltaItem>,
    max_items: usize,
    max_bytes: usize,
    bytes: usize,
}

impl OutputBuffer {
    pub fn new(invocation: String, max_items: usize, max_bytes: usize) -> Self {
        Self {
            invocation,
            items: BTreeMap::new(),
            max_items,
            max_bytes,
            bytes: 0,
        }
    }

    pub fn push(&mut self, items: Vec<DeltaItem>) -> Result<(), PipelineError> {
        for item in items {
            if item.invocation_id != self.invocation || item.delta_seq == 0 {
                return Err(PipelineError::Conflict);
            }
            if let Some(previous) = self.items.get(&item.delta_seq) {
                if previous != &item {
                    return Err(PipelineError::Conflict);
                }
                continue;
            }
            if self.items.len() >= self.max_items
                || self.bytes.saturating_add(item.fragment.len()) > self.max_bytes
            {
                return Err(PipelineError::Capacity);
            }
            self.bytes += item.fragment.len();
            self.items.insert(item.delta_seq, item);
        }
        Ok(())
    }

    pub fn contiguous(&self) -> Vec<DeltaItem> {
        self.items
            .values()
            .enumerate()
            .take_while(|(index, item)| item.delta_seq == *index as u64 + 1)
            .map(|(_, item)| item.clone())
            .collect()
    }

    pub fn has_gap(&self) -> bool {
        self.contiguous().len() != self.items.len()
    }

    pub fn repair(&mut self, journal: &[DeltaItem]) -> Result<(), PipelineError> {
        if journal.iter().enumerate().any(|(index, item)| {
            item.delta_seq != index as u64 + 1 || item.invocation_id != self.invocation
        }) || self
            .items
            .iter()
            .any(|(sequence, item)| journal.get((*sequence - 1) as usize) != Some(item))
        {
            return Err(PipelineError::Gap);
        }
        self.push(journal.to_vec())
    }

    pub fn finish(&self) -> Result<(Vec<OutputSegment>, String), PipelineError> {
        if self.has_gap() {
            return Err(PipelineError::Gap);
        }
        let items = self.contiguous();
        Ok((segments(&items), coverage(&items)))
    }
}

pub fn segments(items: &[DeltaItem]) -> Vec<OutputSegment> {
    let mut result: Vec<OutputSegment> = Vec::new();
    for item in items {
        if let Some(last) = result.last_mut()
            && last.channel == item.channel
            && last.last_seq + 1 == item.delta_seq
        {
            last.last_seq = item.delta_seq;
            last.text.push_str(&item.fragment);
        } else {
            result.push(OutputSegment {
                first_seq: item.delta_seq,
                last_seq: item.delta_seq,
                channel: item.channel.clone(),
                text: item.fragment.clone(),
            });
        }
    }
    result
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PipelineRecord {
    pub start_digest: String,
    pub operator_sequence_base: u64,
    pub session_id: Option<crate::harness::SessionId>,
    pub generation: u64,
    pub ledger: RunLedger,
    pub reducer: AgentLoopReducerState,
    pub model_request_ref: Option<BlobId>,
    pub final_ref: Option<FinalRef>,
    pub batches: Vec<OutputBatch>,
    pub durable_through: u64,
    pub coverage_digest: String,
    pub sealed: bool,
    pub lease_owner: Option<String>,
    pub lease_expires_at_ms: i64,
    pub facts: Vec<OperatorEnvelope>,
    pub public_events: Vec<RunEvent>,
}

pub type PipelineFuture<'scope, T> =
    Pin<Box<dyn Future<Output = Result<T, PipelineError>> + Send + 'scope>>;

pub trait PipelineStore: Send + Sync {
    fn load(&self, run_id: RunId) -> PipelineFuture<'_, Option<PipelineRecord>>;
    fn compare_exchange(
        &self,
        expected: Option<u64>,
        record: PipelineRecord,
    ) -> PipelineFuture<'_, ()>;
}

impl PipelineRecord {
    pub fn validate_append(&self, previous: Option<&Self>) -> Result<(), PipelineError> {
        let previous_count = if let Some(previous) = previous {
            if !self.facts.starts_with(&previous.facts)
                || !self.public_events.starts_with(&previous.public_events)
                || self.start_digest != previous.start_digest
                || self.model_request_ref != previous.model_request_ref
                || self.operator_sequence_base != previous.operator_sequence_base
                || self.session_id != previous.session_id
            {
                return Err(PipelineError::Conflict);
            }
            previous.facts.len()
        } else {
            0
        };
        for (index, event) in self.facts.iter().enumerate().skip(previous_count) {
            let token = &self.ledger.token;
            if event.run_id != self.ledger.run_id
                || event.effect_id != token.effect_id
                || event.invocation_id != token.invocation_id
                || event.attempt != token.attempt
                || event.issued_revision.0 != token.issued_revision
                || event.lease_epoch.0 != token.lease_epoch
            {
                return Err(PipelineError::Fenced);
            }
            if event.operator_seq.0 != self.operator_sequence_base + index as u64 + 1
                || event.engine_version != crate::operator::EngineVersion::OperatorV1
            {
                return Err(PipelineError::Conflict);
            }
            event
                .validate(&crate::operator::PayloadContract {
                    event_type: event.event_type.clone(),
                    delivery_class: crate::operator::DeliveryClass::Durable,
                    schema: payload_schema(&event.event_type),
                    max_payload_bytes: 48 * 1024,
                })
                .map_err(|error| PipelineError::Storage(error.to_string()))?;
        }
        if self.public_events.iter().enumerate().any(|(index, event)| {
            event.run_id != self.ledger.run_id || event.seq != index as u64 + 1
        }) {
            return Err(PipelineError::Conflict);
        }
        Ok(())
    }
}

pub fn payload_schema(kind: &str) -> serde_json::Value {
    use serde_json::json;
    let reference = json!({"type":"string", "format":"uuid"});
    let sequence = json!({"type":"integer", "minimum":0});
    let digest = json!({"type":"string", "pattern":"^sha256:[0-9a-f]{64}$"});
    let properties = match kind {
        "model.requested" | "model.started" => json!({"request_ref":reference}),
        "model.completed" | "model.failed" => json!({
            "artifact_ref":reference, "final_delta_seq":sequence, "coverage_digest":digest,
            "usage":{"type":["object","null"]}, "finish_reason":{"type":"string"}, "error":{"type":["string","null"]}, "model_error":{"type":["object","null"]}
        }),
        "output.batch" => json!({
            "batch_id":{"type":"string","minLength":1}, "first_seq":{"type":"integer","minimum":1},
            "last_seq":{"type":"integer","minimum":1}, "segments":{"type":"array","minItems":1},
            "coverage_digest":digest, "artifact_ref":reference
        }),
        "output.sealed" => json!({"final_delta_seq":sequence,"coverage_digest":digest}),
        "output.failed" => {
            json!({"code":{"type":"string","minLength":1},"durable_through":sequence})
        }
        "run.started" => json!({}),
        "run.completed" => json!({"finish_reason":{"type":"string"}}),
        "run.failed" => json!({"code":{"type":"string","minLength":1}}),
        _ => return json!(false),
    };
    let required: Vec<_> = properties
        .as_object()
        .expect("schema object")
        .keys()
        .cloned()
        .collect();
    json!({"type":"object", "properties":properties, "required":required,"additionalProperties":false})
}
