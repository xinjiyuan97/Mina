use std::sync::Arc;

use super::{
    MemoryCandidate, MemoryComponentDescriptor, MemoryError, MemoryExtractionRequest,
    MemoryExtractor, MemoryFuture, MemoryId, MemoryKind, MemoryRecord, MemorySensitivity,
    MemoryStore, MemoryWriteDecision, MemoryWritePolicy, PutMemory,
};
use crate::harness::ContentPart;

#[derive(Debug, Default)]
pub struct RuleMemoryExtractor;

impl MemoryExtractor for RuleMemoryExtractor {
    fn descriptor(&self) -> MemoryComponentDescriptor {
        MemoryComponentDescriptor {
            identity: "memory:rule-extractor".into(),
            kind: "rule_extractor".into(),
            version: env!("CARGO_PKG_VERSION").into(),
        }
    }

    fn extract(&self, request: MemoryExtractionRequest) -> MemoryFuture<'_, Vec<MemoryCandidate>> {
        Box::pin(async move {
            let text = request
                .content
                .iter()
                .filter_map(ContentPart::as_text)
                .collect::<Vec<_>>()
                .join("\n");
            let lowered = text.to_lowercase();
            let explicit = lowered.contains("remember ")
                || lowered.contains("remember:")
                || text.contains("记住")
                || text.contains("请记得");
            if !explicit {
                return Ok(Vec::new());
            }
            Ok(vec![MemoryCandidate {
                proposed: MemoryRecord {
                    memory_id: MemoryId::new(),
                    scope: request.scope,
                    kind: MemoryKind::Semantic,
                    content: request.content,
                    source_refs: request.source_refs,
                    confidence: 1.0,
                    salience: 0.8,
                    version: 1,
                    expires_at_ms: None,
                    supersedes: None,
                    created_at_ms: request.now_ms,
                },
                sensitivity: detect_sensitivity(&text),
                extraction_reason: "explicit memory phrase".into(),
            }])
        })
    }
}

#[derive(Debug, Default)]
pub struct HostMemoryWritePolicy;

impl MemoryWritePolicy for HostMemoryWritePolicy {
    fn descriptor(&self) -> MemoryComponentDescriptor {
        MemoryComponentDescriptor {
            identity: "memory:host-write-policy".into(),
            kind: "host_write_policy".into(),
            version: env!("CARGO_PKG_VERSION").into(),
        }
    }

    fn decide(&self, candidate: MemoryCandidate) -> MemoryFuture<'_, MemoryWriteDecision> {
        Box::pin(async move {
            match candidate.sensitivity {
                MemorySensitivity::Secret => Ok(MemoryWriteDecision::Reject {
                    reason: "secret-like content is not eligible for memory".into(),
                }),
                MemorySensitivity::Private => Ok(MemoryWriteDecision::RequireApproval {
                    reason: "private content requires explicit approval".into(),
                }),
                MemorySensitivity::Public => Ok(MemoryWriteDecision::Accept {
                    normalized: PutMemory {
                        record: candidate.proposed,
                        expected_absent: true,
                    },
                }),
            }
        })
    }
}

pub struct MemoryWriter {
    store: Arc<dyn MemoryStore>,
    policy: Arc<dyn MemoryWritePolicy>,
}

impl MemoryWriter {
    pub fn new(store: Arc<dyn MemoryStore>, policy: Arc<dyn MemoryWritePolicy>) -> Self {
        Self { store, policy }
    }

    pub async fn process(
        &self,
        candidates: Vec<MemoryCandidate>,
    ) -> Result<Vec<MemoryWriteOutcome>, MemoryError> {
        let mut outcomes = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            match self.policy.decide(candidate).await? {
                MemoryWriteDecision::Accept { normalized } => {
                    outcomes.push(MemoryWriteOutcome::Stored(
                        self.store.put(normalized).await?,
                    ));
                }
                MemoryWriteDecision::Reject { reason } => {
                    outcomes.push(MemoryWriteOutcome::Rejected(reason));
                }
                MemoryWriteDecision::RequireApproval { reason } => {
                    outcomes.push(MemoryWriteOutcome::ApprovalRequired(reason));
                }
            }
        }
        Ok(outcomes)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum MemoryWriteOutcome {
    Stored(MemoryRecord),
    Rejected(String),
    ApprovalRequired(String),
}

fn detect_sensitivity(text: &str) -> MemorySensitivity {
    let lower = text.to_lowercase();
    if ["api_key", "api key", "password", "secret", "token="]
        .iter()
        .any(|needle| lower.contains(needle))
    {
        MemorySensitivity::Secret
    } else if ["身份证", "手机号", "住址", "private"]
        .iter()
        .any(|needle| lower.contains(needle))
    {
        MemorySensitivity::Private
    } else {
        MemorySensitivity::Public
    }
}
