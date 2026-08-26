use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservationEvent {
    pub trace_id: String,
    pub observation_id: String,
    pub timestamp_ms: i64,
    #[serde(flatten)]
    pub kind: ObservationKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ObservationKind {
    ModelStarted {
        model: String,
        message_count: usize,
        tool_count: usize,
    },
    ModelFinished {
        model: String,
        status: ObservationStatus,
        duration_ms: u64,
        finish_reason: Option<String>,
        error_code: Option<String>,
        retryable: Option<bool>,
        input_tokens: Option<u64>,
        output_tokens: Option<u64>,
    },
    ToolStarted {
        call_id: String,
        tool_name: String,
    },
    ToolFinished {
        call_id: String,
        tool_name: String,
        status: ObservationStatus,
        duration_ms: u64,
        error_code: Option<String>,
        error_category: Option<String>,
        retryable: Option<bool>,
        retry_after_ms: Option<u64>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservationStatus {
    Completed,
    Failed,
    Cancelled,
}

/// Synchronous and non-fallible by design. Exporters should enqueue records to
/// a bounded worker and must never make Agent execution depend on telemetry.
pub trait ObservationHook: Send + Sync + 'static {
    fn record(&self, event: ObservationEvent);
}

#[derive(Debug, Default)]
pub struct NoopObservationHook;

impl ObservationHook for NoopObservationHook {
    fn record(&self, _event: ObservationEvent) {}
}
