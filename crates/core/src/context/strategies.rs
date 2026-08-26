use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use super::{
    CompressionRequest, CompressionResult, ContextArtifact, ContextArtifactCandidate,
    ContextArtifactId, ContextArtifactRef, ContextArtifactStore, ContextComponentDescriptor,
    ContextCompressor, ContextError, ContextFuture, ContextItem, ContextPriority,
    DroppedContextItem, InvalidateContextArtifacts, PutContextArtifact, ReusableArtifactQuery,
    SummaryGenerator, SummaryRequest, SummaryResult, TokenEstimate, TokenEstimateRequest,
    TokenEstimator, TokenSegment,
};
use sha2::{Digest, Sha256};

#[derive(Debug, Default)]
pub struct HeuristicTokenEstimator;

impl TokenEstimator for HeuristicTokenEstimator {
    fn descriptor(&self) -> ContextComponentDescriptor {
        descriptor("context:heuristic-token-v1", "heuristic_token_estimator")
    }

    fn estimate(&self, request: TokenEstimateRequest) -> ContextFuture<'_, TokenEstimate> {
        Box::pin(async move {
            let per_segment: Vec<_> = request
                .segments
                .iter()
                .map(|segment| estimate_text(&segment.content))
                .collect();
            Ok(TokenEstimate {
                tokens: per_segment.iter().copied().sum(),
                per_segment,
                confidence: 0.55,
                estimator: self.descriptor(),
            })
        })
    }
}

pub struct NoopFailCompressor {
    estimator: Arc<dyn TokenEstimator>,
}

impl NoopFailCompressor {
    pub fn new(estimator: Arc<dyn TokenEstimator>) -> Self {
        Self { estimator }
    }
}

impl ContextCompressor for NoopFailCompressor {
    fn descriptor(&self) -> ContextComponentDescriptor {
        descriptor("context:noop-fail-v1", "noop_fail_compressor")
    }

    fn compress(&self, request: CompressionRequest) -> ContextFuture<'_, CompressionResult> {
        Box::pin(async move {
            let estimate = estimate_items(
                &*self.estimator,
                &request.model_profile,
                &request.candidates,
            )
            .await?;
            if estimate.tokens > request.budget.available_input_tokens()
                || !partitions_fit(&request.candidates, &estimate.per_segment, &request.budget)
            {
                return Err(ContextError::BudgetExceeded);
            }
            Ok(CompressionResult {
                retained: request.candidates,
                dropped: Vec::new(),
                artifact_candidates: Vec::new(),
                estimated_input_tokens: estimate.tokens,
                estimator_confidence: estimate.confidence,
                compressor: self.descriptor(),
                result_fingerprint: digest_text(&format!(
                    "noop:{}:{}",
                    request.policy_version, estimate.tokens
                )),
            })
        })
    }
}

pub struct SlidingWindowCompressor {
    estimator: Arc<dyn TokenEstimator>,
}

impl SlidingWindowCompressor {
    pub fn new(estimator: Arc<dyn TokenEstimator>) -> Self {
        Self { estimator }
    }
}

impl ContextCompressor for SlidingWindowCompressor {
    fn descriptor(&self) -> ContextComponentDescriptor {
        descriptor("context:sliding-window-v1", "sliding_window_compressor")
    }

    fn compress(&self, request: CompressionRequest) -> ContextFuture<'_, CompressionResult> {
        Box::pin(async move { compress_window(&*self.estimator, request, self.descriptor()).await })
    }
}

pub struct HybridCompressor {
    estimator: Arc<dyn TokenEstimator>,
    summary: Arc<dyn SummaryGenerator>,
}

impl HybridCompressor {
    pub fn new(estimator: Arc<dyn TokenEstimator>, summary: Arc<dyn SummaryGenerator>) -> Self {
        Self { estimator, summary }
    }
}

impl ContextCompressor for HybridCompressor {
    fn descriptor(&self) -> ContextComponentDescriptor {
        descriptor("context:hybrid-v1", "hybrid_compressor")
    }

    fn compress(&self, request: CompressionRequest) -> ContextFuture<'_, CompressionResult> {
        Box::pin(async move {
            let policy_version = request.policy_version;
            let model_profile = request.model_profile.clone();
            let available_input_tokens = request.budget.available_input_tokens();
            let artifact_source_digest = request.source_digest.clone();
            let had_reusable_summary = request
                .reusable_artifacts
                .iter()
                .any(|artifact| artifact.kind == super::ContextArtifactKind::Summary);
            let mut request = request;
            for artifact in &request.reusable_artifacts {
                request.candidates.push(ContextItem {
                    item_id: format!("artifact:{}", artifact.artifact_id),
                    role: crate::harness::ModelRole::System,
                    content: format!(
                        "Reusable derived context (untrusted):\n{}",
                        artifact.content
                    ),
                    priority: ContextPriority::Normal,
                    source: super::ContextSourceRef {
                        kind: "context_artifact".into(),
                        identity: artifact.artifact_id.to_string(),
                        version: artifact.policy_version.to_string(),
                        digest: artifact.content_digest.clone(),
                    },
                });
            }
            if !had_reusable_summary {
                let full_estimate =
                    estimate_items(&*self.estimator, &model_profile, &request.candidates).await?;
                if full_estimate.tokens > available_input_tokens {
                    let summary_reserve = available_input_tokens
                        .saturating_div(8)
                        .clamp(16, 512)
                        .min(available_input_tokens.saturating_div(2));
                    request.budget.reserved_output_tokens = request
                        .budget
                        .reserved_output_tokens
                        .saturating_add(summary_reserve);
                }
            }
            let mut result = compress_window(&*self.estimator, request, self.descriptor()).await?;
            let summary_items: Vec<_> = result
                .dropped
                .iter()
                .map(|dropped| dropped.item.clone())
                .collect();
            if summary_items.is_empty() || had_reusable_summary {
                return Ok(result);
            }
            let remaining_tokens =
                available_input_tokens.saturating_sub(result.estimated_input_tokens);
            if remaining_tokens < 16 {
                return Ok(result);
            }
            let source_digest = digest_items(&summary_items);
            let summary = self
                .summary
                .summarize(SummaryRequest {
                    model_profile: model_profile.clone(),
                    items: summary_items.clone(),
                    maximum_tokens: remaining_tokens.saturating_sub(8).min(512),
                    source_digest: source_digest.clone(),
                })
                .await?;
            let summary_item = ContextItem {
                item_id: format!("summary:{}", summary.output_digest),
                role: crate::harness::ModelRole::System,
                content: format!(
                    "Conversation summary (untrusted derived context):\n{}",
                    summary.content
                ),
                priority: ContextPriority::Normal,
                source: super::ContextSourceRef {
                    kind: "summary".into(),
                    identity: summary.generator.identity.clone(),
                    version: summary.generator.version.clone(),
                    digest: summary.output_digest.clone(),
                },
            };
            let estimate = self
                .estimator
                .estimate(TokenEstimateRequest {
                    model_profile,
                    segments: vec![TokenSegment {
                        identity: summary_item.item_id.clone(),
                        content: summary_item.content.clone(),
                    }],
                })
                .await?;
            if result
                .estimated_input_tokens
                .saturating_add(estimate.tokens)
                <= available_input_tokens
            {
                result.retained.insert(0, summary_item);
                result.estimated_input_tokens = result
                    .estimated_input_tokens
                    .saturating_add(estimate.tokens);
                result.artifact_candidates.push(ContextArtifactCandidate {
                    kind: super::ContextArtifactKind::Summary,
                    source_refs: summary_items.into_iter().map(|item| item.source).collect(),
                    content: summary.content,
                    source_digest: artifact_source_digest,
                    content_digest: summary.output_digest,
                    policy_version,
                    generator: summary.generator,
                });
                result.result_fingerprint = digest_items(&result.retained);
            }
            Ok(result)
        })
    }
}

#[derive(Debug, Default)]
pub struct DeterministicSummaryGenerator;

impl SummaryGenerator for DeterministicSummaryGenerator {
    fn descriptor(&self) -> ContextComponentDescriptor {
        descriptor(
            "context:deterministic-summary-v1",
            "deterministic_summary_generator",
        )
    }

    fn summarize(&self, request: SummaryRequest) -> ContextFuture<'_, SummaryResult> {
        Box::pin(async move {
            let mut content = request
                .items
                .iter()
                .map(|item| format!("- {:?}: {}", item.role, item.content.replace('\n', " ")))
                .collect::<Vec<_>>()
                .join("\n");
            let maximum_chars =
                usize::try_from(request.maximum_tokens.saturating_mul(4)).unwrap_or(usize::MAX);
            if content.chars().count() > maximum_chars {
                content = content.chars().take(maximum_chars).collect();
                content.push('…');
            }
            let output_digest = digest_text(&content);
            Ok(SummaryResult {
                content,
                input_digest: request.source_digest,
                output_digest,
                generator: self.descriptor(),
            })
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct InMemoryContextArtifactStore {
    artifacts: Arc<Mutex<HashMap<ContextArtifactId, ContextArtifact>>>,
}

impl ContextArtifactStore for InMemoryContextArtifactStore {
    fn descriptor(&self) -> ContextComponentDescriptor {
        descriptor("context-artifact:in-memory", "in_memory_artifact_store")
    }

    fn find_reusable(
        &self,
        query: ReusableArtifactQuery,
    ) -> ContextFuture<'_, Vec<ContextArtifactRef>> {
        Box::pin(async move {
            let artifacts = self
                .artifacts
                .lock()
                .map_err(|_| ContextError::Component("artifact store lock poisoned".into()))?;
            Ok(artifacts
                .values()
                .filter(|artifact| {
                    artifact.source_digest == query.source_digest
                        && artifact.policy_version == query.policy_version
                        && (query.kinds.is_empty() || query.kinds.contains(&artifact.kind))
                })
                .map(|artifact| ContextArtifactRef {
                    artifact_id: artifact.artifact_id,
                    content_digest: artifact.content_digest.clone(),
                })
                .collect())
        })
    }

    fn get(&self, artifact: ContextArtifactRef) -> ContextFuture<'_, Option<ContextArtifact>> {
        Box::pin(async move {
            Ok(self
                .artifacts
                .lock()
                .map_err(|_| ContextError::Component("artifact store lock poisoned".into()))?
                .get(&artifact.artifact_id)
                .filter(|stored| stored.content_digest == artifact.content_digest)
                .cloned())
        })
    }

    fn put(&self, command: PutContextArtifact) -> ContextFuture<'_, ContextArtifactRef> {
        Box::pin(async move {
            let mut artifacts = self
                .artifacts
                .lock()
                .map_err(|_| ContextError::Component("artifact store lock poisoned".into()))?;
            if let Some(existing) = artifacts.values().find(|artifact| {
                artifact.source_digest == command.candidate.source_digest
                    && artifact.content_digest == command.candidate.content_digest
                    && artifact.policy_version == command.candidate.policy_version
            }) {
                return Ok(ContextArtifactRef {
                    artifact_id: existing.artifact_id,
                    content_digest: existing.content_digest.clone(),
                });
            }
            let artifact = ContextArtifact {
                artifact_id: ContextArtifactId::new(),
                kind: command.candidate.kind,
                source_refs: command.candidate.source_refs,
                policy_version: command.candidate.policy_version,
                generator: command.candidate.generator,
                content: command.candidate.content,
                source_digest: command.candidate.source_digest,
                content_digest: command.candidate.content_digest,
                created_at_ms: command.created_at_ms,
            };
            let reference = ContextArtifactRef {
                artifact_id: artifact.artifact_id,
                content_digest: artifact.content_digest.clone(),
            };
            artifacts.insert(artifact.artifact_id, artifact);
            Ok(reference)
        })
    }

    fn invalidate(&self, command: InvalidateContextArtifacts) -> ContextFuture<'_, u64> {
        Box::pin(async move {
            let mut artifacts = self
                .artifacts
                .lock()
                .map_err(|_| ContextError::Component("artifact store lock poisoned".into()))?;
            let before = artifacts.len();
            artifacts.retain(|_, artifact| artifact.source_digest != command.source_digest);
            Ok(u64::try_from(before.saturating_sub(artifacts.len())).unwrap_or(u64::MAX))
        })
    }
}

async fn compress_window(
    estimator: &dyn TokenEstimator,
    request: CompressionRequest,
    compressor: ContextComponentDescriptor,
) -> Result<CompressionResult, ContextError> {
    let estimate = estimate_items(estimator, &request.model_profile, &request.candidates).await?;
    let available = request.budget.available_input_tokens();
    let mut retained = vec![false; request.candidates.len()];
    let mut drop_reasons = vec![None::<String>; request.candidates.len()];
    let mut used = 0_u64;
    let mut skill_used = 0_u64;
    let mut memory_used = 0_u64;
    let mut history_used = 0_u64;
    for (index, item) in request.candidates.iter().enumerate() {
        if item.priority == ContextPriority::Required {
            used = used.saturating_add(estimate.per_segment[index]);
            retained[index] = true;
        }
    }
    if used > available {
        return Err(ContextError::BudgetExceeded);
    }
    for priority in [
        ContextPriority::High,
        ContextPriority::Normal,
        ContextPriority::Low,
    ] {
        for index in (0..request.candidates.len()).rev() {
            if request.candidates[index].priority != priority {
                continue;
            }
            let tokens = estimate.per_segment[index];
            let category = context_category(&request.candidates[index]);
            let (category_used, category_limit) = match category {
                ContextCategory::Skill => (skill_used, request.budget.max_skill_tokens),
                ContextCategory::Memory => (memory_used, request.budget.max_memory_tokens),
                ContextCategory::History => (history_used, request.budget.max_history_tokens),
                ContextCategory::Other => (0, u64::MAX),
            };
            if category_used.saturating_add(tokens) > category_limit {
                drop_reasons[index] = Some(format!(
                    "{} context exceeded its partition budget",
                    category.name()
                ));
                continue;
            }
            if used.saturating_add(tokens) > available {
                drop_reasons[index] = Some("item did not fit the total input token budget".into());
                continue;
            }
            retained[index] = true;
            used = used.saturating_add(tokens);
            match category {
                ContextCategory::Skill => skill_used = skill_used.saturating_add(tokens),
                ContextCategory::Memory => memory_used = memory_used.saturating_add(tokens),
                ContextCategory::History => history_used = history_used.saturating_add(tokens),
                ContextCategory::Other => {}
            }
        }
    }
    let mut retained_items = Vec::new();
    let mut dropped = Vec::new();
    for (index, item) in request.candidates.into_iter().enumerate() {
        if retained[index] {
            retained_items.push(item);
        } else {
            dropped.push(DroppedContextItem {
                item,
                reason: drop_reasons[index].take().unwrap_or_else(|| {
                    "lower priority item did not fit the input token budget".into()
                }),
            });
        }
    }
    let result_fingerprint = digest_items(&retained_items);
    Ok(CompressionResult {
        retained: retained_items,
        dropped,
        artifact_candidates: Vec::new(),
        estimated_input_tokens: used,
        estimator_confidence: estimate.confidence,
        compressor,
        result_fingerprint,
    })
}

#[derive(Debug, Clone, Copy)]
enum ContextCategory {
    Skill,
    Memory,
    History,
    Other,
}

impl ContextCategory {
    const fn name(self) -> &'static str {
        match self {
            Self::Skill => "skill",
            Self::Memory => "memory",
            Self::History => "history",
            Self::Other => "other",
        }
    }
}

fn context_category(item: &ContextItem) -> ContextCategory {
    match item.source.kind.as_str() {
        "skill" => ContextCategory::Skill,
        "memory" => ContextCategory::Memory,
        "session_message" => ContextCategory::History,
        _ => ContextCategory::Other,
    }
}

fn partitions_fit(items: &[ContextItem], tokens: &[u64], budget: &super::ContextBudget) -> bool {
    let mut skill = 0_u64;
    let mut memory = 0_u64;
    let mut history = 0_u64;
    for (item, tokens) in items.iter().zip(tokens.iter().copied()) {
        match context_category(item) {
            ContextCategory::Skill => skill = skill.saturating_add(tokens),
            ContextCategory::Memory => memory = memory.saturating_add(tokens),
            ContextCategory::History => history = history.saturating_add(tokens),
            ContextCategory::Other => {}
        }
    }
    skill <= budget.max_skill_tokens
        && memory <= budget.max_memory_tokens
        && history <= budget.max_history_tokens
}

async fn estimate_items(
    estimator: &dyn TokenEstimator,
    model_profile: &str,
    items: &[ContextItem],
) -> Result<TokenEstimate, ContextError> {
    estimator
        .estimate(TokenEstimateRequest {
            model_profile: model_profile.into(),
            segments: items
                .iter()
                .map(|item| TokenSegment {
                    identity: item.item_id.clone(),
                    content: item.content.clone(),
                })
                .collect(),
        })
        .await
}

fn estimate_text(text: &str) -> u64 {
    let (ascii, non_ascii) = text
        .chars()
        .fold((0_u64, 0_u64), |(ascii, non_ascii), character| {
            if character.is_ascii() {
                (ascii.saturating_add(1), non_ascii)
            } else {
                (ascii, non_ascii.saturating_add(1))
            }
        });
    ascii
        .saturating_add(3)
        .saturating_div(4)
        .saturating_add(non_ascii)
        .saturating_add(4)
}

fn descriptor(identity: &str, kind: &str) -> ContextComponentDescriptor {
    ContextComponentDescriptor {
        identity: identity.into(),
        kind: kind.into(),
        version: env!("CARGO_PKG_VERSION").into(),
    }
}

fn digest_items(items: &[ContextItem]) -> String {
    let mut hasher = Sha256::new();
    for item in items {
        hasher.update(item.item_id.as_bytes());
        hasher.update(item.source.digest.as_bytes());
        hasher.update(item.content.as_bytes());
    }
    format!("sha256:{:x}", hasher.finalize())
}

fn digest_text(text: &str) -> String {
    format!("sha256:{:x}", Sha256::digest(text.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{ContextBudget, ContextSourceRef};

    fn item(id: &str, kind: &str, content: &str, priority: ContextPriority) -> ContextItem {
        ContextItem {
            item_id: id.into(),
            role: crate::harness::ModelRole::User,
            content: content.into(),
            priority,
            source: ContextSourceRef {
                kind: kind.into(),
                identity: id.into(),
                version: "1".into(),
                digest: digest_text(content),
            },
        }
    }

    fn budget(total: u64) -> ContextBudget {
        ContextBudget {
            model_context_tokens: total,
            reserved_output_tokens: 0,
            reserved_tool_schema_tokens: 0,
            max_skill_tokens: total,
            max_memory_tokens: total,
            max_history_tokens: total,
        }
    }

    #[tokio::test]
    async fn estimator_counts_non_ascii_and_marks_estimates() {
        let estimator = HeuristicTokenEstimator;
        let estimate = estimator
            .estimate(TokenEstimateRequest {
                model_profile: "test".into(),
                segments: vec![TokenSegment {
                    identity: "one".into(),
                    content: "hello 世界".into(),
                }],
            })
            .await
            .expect("estimate should succeed");

        assert!(estimate.tokens > 0);
        assert!(estimate.confidence < 1.0);
        assert_eq!(estimate.per_segment.len(), 1);
    }

    #[tokio::test]
    async fn sliding_window_never_drops_required_context() {
        let estimator: Arc<dyn TokenEstimator> = Arc::new(HeuristicTokenEstimator);
        let compressor = SlidingWindowCompressor::new(estimator);
        let result = compressor
            .compress(CompressionRequest {
                candidates: vec![
                    item(
                        "old",
                        "session_message",
                        &"old ".repeat(100),
                        ContextPriority::Low,
                    ),
                    item(
                        "current",
                        "current_input",
                        "required",
                        ContextPriority::Required,
                    ),
                ],
                source_digest: "source".into(),
                budget: budget(20),
                reusable_artifacts: Vec::new(),
                model_profile: "test".into(),
                policy_version: 1,
            })
            .await
            .expect("compression should succeed");

        assert!(result.retained.iter().any(|item| item.item_id == "current"));
        assert!(result.dropped.iter().any(|item| item.item.item_id == "old"));
    }

    #[tokio::test]
    async fn noop_fails_when_a_partition_exceeds_its_budget() {
        let estimator: Arc<dyn TokenEstimator> = Arc::new(HeuristicTokenEstimator);
        let compressor = NoopFailCompressor::new(estimator);
        let mut constrained = budget(10_000);
        constrained.max_memory_tokens = 1;
        let error = compressor
            .compress(CompressionRequest {
                candidates: vec![item(
                    "memory",
                    "memory",
                    "a memory that needs several tokens",
                    ContextPriority::Normal,
                )],
                source_digest: "source".into(),
                budget: constrained,
                reusable_artifacts: Vec::new(),
                model_profile: "test".into(),
                policy_version: 1,
            })
            .await
            .expect_err("partition overflow should fail");

        assert!(matches!(error, ContextError::BudgetExceeded));
    }
}
