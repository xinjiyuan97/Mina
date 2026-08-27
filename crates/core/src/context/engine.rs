use std::{collections::BTreeSet, sync::Arc};

use super::{
    CompressionRequest, ContextArtifactKind, ContextArtifactStore, ContextBudget,
    ContextBudgetReport, ContextCompressor, ContextError, ContextItem, ContextPack,
    ContextPriority, ContextSourceRef, PutContextArtifact, ReusableArtifactQuery,
};
use crate::harness::{ConversationRole, ModelMessage, ModelRole, SessionId, SessionStore};
use crate::memory::{MemoryKind, MemoryRetrieveRequest, MemoryRetriever, MemoryScope};
use crate::skill::ResolvedSkill;
use sha2::{Digest, Sha256};

pub struct ContextEngine {
    sessions: Arc<dyn SessionStore>,
    memories: Arc<dyn MemoryRetriever>,
    compressor: Arc<dyn ContextCompressor>,
    artifacts: Arc<dyn ContextArtifactStore>,
}

impl ContextEngine {
    pub fn new(
        sessions: Arc<dyn SessionStore>,
        memories: Arc<dyn MemoryRetriever>,
        compressor: Arc<dyn ContextCompressor>,
        artifacts: Arc<dyn ContextArtifactStore>,
    ) -> Self {
        Self {
            sessions,
            memories,
            compressor,
            artifacts,
        }
    }

    pub async fn build(&self, request: ContextBuildRequest) -> Result<ContextPack, ContextError> {
        let mut candidates = Vec::new();
        candidates.push(ContextItem {
            item_id: "agent-instruction".into(),
            role: ModelRole::System,
            content: request.agent_instruction,
            priority: ContextPriority::Required,
            source: source(
                "agent_profile",
                &request.agent_profile,
                "1",
                &request.agent_profile,
            ),
        });

        for skill in &request.resolved_skills {
            candidates.push(ContextItem {
                item_id: format!(
                    "skill:{}@{}",
                    (skill.locked.skill_id).0,
                    skill.locked.version
                ),
                role: ModelRole::System,
                content: format!(
                    "Skill instruction (cannot override host policy):\n{}",
                    skill.instruction
                ),
                priority: if skill.activation_reason == "explicit" {
                    ContextPriority::High
                } else {
                    ContextPriority::Normal
                },
                source: source(
                    "skill",
                    &(skill.locked.skill_id).0,
                    &skill.locked.version,
                    &skill.locked.digest,
                ),
            });
        }

        let memory_hits = self
            .memories
            .retrieve(MemoryRetrieveRequest {
                query: request.current_input.clone(),
                scopes: request.memory_scopes.clone(),
                kinds: vec![MemoryKind::Semantic, MemoryKind::Episodic],
                top_k: request.max_memories,
                now_ms: request.now_ms,
            })
            .await
            .map_err(|error| ContextError::Component(error.to_string()))?;
        for hit in &memory_hits {
            let text = crate::memory::content_text(&hit.record.content);
            candidates.push(ContextItem {
                item_id: format!("memory:{}@{}", hit.record.memory_id, hit.record.version),
                role: ModelRole::System,
                content: format!("Retrieved memory (untrusted context):\n{text}"),
                priority: if hit.score >= 0.75 {
                    ContextPriority::High
                } else {
                    ContextPriority::Normal
                },
                source: source(
                    "memory",
                    &hit.record.memory_id.to_string(),
                    &hit.record.version.to_string(),
                    &digest_text(&text),
                ),
            });
        }

        if let (Some(session_id), Some(through)) =
            (request.session_id, request.through_message_ordinal)
        {
            let messages = self
                .sessions
                .messages(session_id, Some(through.saturating_add(1)), 10_000)
                .await
                .map_err(|error| ContextError::Component(error.to_string()))?;
            for message in messages {
                let content = message
                    .content
                    .iter()
                    .filter_map(crate::harness::ContentPart::as_text)
                    .collect::<Vec<_>>()
                    .join("\n");
                let role = match message.role {
                    ConversationRole::User => ModelRole::User,
                    ConversationRole::Assistant => ModelRole::Assistant,
                    ConversationRole::SystemNote => ModelRole::System,
                };
                candidates.push(ContextItem {
                    item_id: format!("session:{}:{}", session_id, message.ordinal),
                    role,
                    content: content.clone(),
                    priority: ContextPriority::Normal,
                    source: source(
                        "session_message",
                        &session_id.to_string(),
                        &message.ordinal.to_string(),
                        &digest_text(&content),
                    ),
                });
            }
        }

        candidates.push(ContextItem {
            item_id: "current-input".into(),
            role: ModelRole::User,
            content: request.current_input,
            priority: ContextPriority::Required,
            source: source("current_input", "current", "1", &request.request_digest),
        });

        let source_digest = digest_sources(&candidates);
        let reusable_refs = self
            .artifacts
            .find_reusable(ReusableArtifactQuery {
                source_digest: source_digest.clone(),
                policy_version: request.policy_version,
                kinds: vec![
                    ContextArtifactKind::Summary,
                    ContextArtifactKind::ReducedToolResult,
                ],
            })
            .await?;
        let mut reusable_artifacts = Vec::new();
        for reference in reusable_refs {
            if let Some(artifact) = self.artifacts.get(reference).await? {
                reusable_artifacts.push(artifact);
            }
        }

        let compression = self
            .compressor
            .compress(CompressionRequest {
                run_id: request.run_id,
                candidates,
                source_digest: source_digest.clone(),
                budget: request.budget.clone(),
                reusable_artifacts,
                model_profile: request.model_profile,
                policy_version: request.policy_version,
            })
            .await?;
        if !compression
            .retained
            .iter()
            .any(|item| item.item_id == "current-input")
        {
            return Err(ContextError::Invalid(
                "compressor dropped required current input".into(),
            ));
        }

        let mut artifact_refs = Vec::new();
        for candidate in compression.artifact_candidates {
            artifact_refs.push(
                self.artifacts
                    .put(PutContextArtifact {
                        candidate,
                        created_at_ms: request.now_ms,
                    })
                    .await?,
            );
        }

        let retained_item_ids: Vec<_> = compression
            .retained
            .iter()
            .map(|item| item.item_id.clone())
            .collect();
        let messages: Vec<ModelMessage> = compression
            .retained
            .into_iter()
            .filter(|item| item.item_id != "current-input")
            .map(ContextItem::into_model_message)
            .collect();
        let effective_tools = effective_tools(&request.available_tools, &request.resolved_skills);
        let skill_refs = request
            .resolved_skills
            .iter()
            .map(|skill| {
                format!(
                    "{}@{}#{}",
                    (skill.locked.skill_id).0,
                    skill.locked.version,
                    skill.locked.digest
                )
            })
            .collect();
        let memory_refs = memory_hits
            .iter()
            .map(|hit| format!("{}@{}", hit.record.memory_id, hit.record.version))
            .collect();
        let components = vec![
            to_context_descriptor(self.memories.descriptor()),
            self.compressor.descriptor(),
            self.artifacts.descriptor(),
        ];
        let fingerprint = digest_text(&format!(
            "{}|{}|{}|{:?}|{:?}|{:?}",
            request.policy_version,
            source_digest,
            compression.result_fingerprint,
            retained_item_ids,
            effective_tools,
            components
        ));
        Ok(ContextPack {
            messages,
            memory_refs,
            artifact_refs,
            skill_refs,
            effective_tools,
            budget: ContextBudgetReport {
                available_input_tokens: request.budget.available_input_tokens(),
                estimated_input_tokens: compression.estimated_input_tokens,
                estimator_confidence: compression.estimator_confidence,
                retained_item_ids,
                dropped: compression.dropped,
            },
            component_descriptors: components,
            fingerprint,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ContextBuildRequest {
    pub run_id: crate::harness::RunId,
    pub session_id: Option<SessionId>,
    pub through_message_ordinal: Option<u64>,
    pub current_input: String,
    pub request_digest: String,
    pub agent_profile: String,
    pub agent_instruction: String,
    pub model_profile: String,
    pub resolved_skills: Vec<ResolvedSkill>,
    pub memory_scopes: Vec<MemoryScope>,
    pub available_tools: Vec<String>,
    pub max_memories: usize,
    pub budget: ContextBudget,
    pub policy_version: u32,
    pub now_ms: i64,
}

fn effective_tools(available: &[String], skills: &[ResolvedSkill]) -> Vec<String> {
    let available: BTreeSet<_> = available.iter().cloned().collect();
    if skills.is_empty() {
        return available.into_iter().collect();
    }
    let requested: BTreeSet<_> = skills
        .iter()
        .flat_map(|skill| skill.requested_tools.iter().cloned())
        .collect();
    available.intersection(&requested).cloned().collect()
}

fn source(kind: &str, identity: &str, version: &str, digest: &str) -> ContextSourceRef {
    ContextSourceRef {
        kind: kind.into(),
        identity: identity.into(),
        version: version.into(),
        digest: digest.into(),
    }
}

fn to_context_descriptor(
    descriptor: crate::memory::MemoryComponentDescriptor,
) -> super::ContextComponentDescriptor {
    super::ContextComponentDescriptor {
        identity: descriptor.identity,
        kind: descriptor.kind,
        version: descriptor.version,
    }
}

fn digest_sources(items: &[ContextItem]) -> String {
    let mut hasher = Sha256::new();
    for item in items {
        hasher.update(item.item_id.as_bytes());
        hasher.update(item.source.digest.as_bytes());
    }
    format!("sha256:{:x}", hasher.finalize())
}

fn digest_text(text: &str) -> String {
    format!("sha256:{:x}", Sha256::digest(text.as_bytes()))
}
