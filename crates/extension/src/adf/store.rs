use std::{
    collections::HashMap,
    sync::{Mutex, MutexGuard},
};

use agent_core::{
    adf::{
        ADF_CONTRACT_VERSION, AdfArtifact, AdfArtifactDescriptor, AdfArtifactQuery, AdfArtifactRef,
        AdfArtifactStore, AdfFuture, AdfId, AdfOwner, AdfStoreCapabilities, AdfStoreError,
        PutAdfArtifact,
    },
    skill::ComponentDescriptor,
};
use sha2::{Digest, Sha256};

#[derive(Debug, Default)]
pub struct InMemoryAdfArtifactStore {
    state: Mutex<StoreState>,
}

#[derive(Debug, Default)]
struct StoreState {
    artifacts: HashMap<(AdfId, u64), AdfArtifact>,
    idempotency: HashMap<(AdfOwner, String), AdfArtifactRef>,
}

impl InMemoryAdfArtifactStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> Result<MutexGuard<'_, StoreState>, AdfStoreError> {
        self.state
            .lock()
            .map_err(|_| AdfStoreError::backend("ADF artifact store lock was poisoned"))
    }
}

impl AdfArtifactStore for InMemoryAdfArtifactStore {
    fn descriptor(&self) -> ComponentDescriptor {
        ComponentDescriptor {
            identity: "adf:in-memory-v1".into(),
            kind: "in_memory_adf_store".into(),
            version: env!("CARGO_PKG_VERSION").into(),
        }
    }

    fn capabilities(&self) -> AdfStoreCapabilities {
        AdfStoreCapabilities {
            readable: true,
            writable: true,
            deactivatable: true,
        }
    }

    fn put(&self, command: PutAdfArtifact) -> AdfFuture<'_, AdfArtifactRef> {
        Box::pin(async move {
            validate_artifact(&command)?;
            let mut state = self.lock()?;
            let owner = command.artifact.definition.owner.clone();
            let idempotency_key = (owner, command.idempotency_key);
            if let Some(existing_ref) = state.idempotency.get(&idempotency_key) {
                let existing = state
                    .artifacts
                    .get(&(existing_ref.adf_id, existing_ref.revision))
                    .ok_or_else(|| {
                        AdfStoreError::backend("idempotency index references a missing artifact")
                    })?;
                if same_content(existing, &command.artifact) {
                    return Ok(existing_ref.clone());
                }
                return Err(AdfStoreError::IdempotencyConflict);
            }

            let definition = &command.artifact.definition;
            let key = (definition.adf_id, definition.revision);
            if let Some(existing) = state.artifacts.get(&key) {
                if same_content(existing, &command.artifact) {
                    return Ok(existing.definition.source_ref.clone());
                }
                return Err(AdfStoreError::ArtifactConflict);
            }
            let reference = definition.source_ref.clone();
            state.artifacts.insert(key, command.artifact);
            state.idempotency.insert(idempotency_key, reference.clone());
            Ok(reference)
        })
    }

    fn get(&self, reference: AdfArtifactRef) -> AdfFuture<'_, Option<AdfArtifact>> {
        Box::pin(async move {
            let state = self.lock()?;
            let artifact = state
                .artifacts
                .get(&(reference.adf_id, reference.revision))
                .filter(|artifact| artifact.definition.source_ref == reference)
                .cloned();
            Ok(artifact)
        })
    }

    fn list(&self, query: AdfArtifactQuery) -> AdfFuture<'_, Vec<AdfArtifactDescriptor>> {
        Box::pin(async move {
            let state = self.lock()?;
            let mut descriptors = state
                .artifacts
                .values()
                .filter(|artifact| {
                    query
                        .owner
                        .as_ref()
                        .is_none_or(|owner| &artifact.definition.owner == owner)
                        && (!query.active_only || artifact.active)
                })
                .map(|artifact| AdfArtifactDescriptor {
                    reference: artifact.definition.source_ref.clone(),
                    canonical_name: artifact.definition.canonical_name.clone(),
                    runtime: artifact.definition.runtime.kind,
                    owner: artifact.definition.owner.clone(),
                    effective_risk: artifact.definition.effective_risk,
                    active: artifact.active,
                })
                .collect::<Vec<_>>();
            descriptors.sort_by(|left, right| {
                (&left.canonical_name, left.reference.revision)
                    .cmp(&(&right.canonical_name, right.reference.revision))
            });
            Ok(descriptors)
        })
    }

    fn deactivate(&self, reference: AdfArtifactRef) -> AdfFuture<'_, ()> {
        Box::pin(async move {
            let mut state = self.lock()?;
            let artifact = state
                .artifacts
                .get_mut(&(reference.adf_id, reference.revision))
                .filter(|artifact| artifact.definition.source_ref == reference)
                .ok_or(AdfStoreError::NotFound)?;
            artifact.active = false;
            Ok(())
        })
    }
}

fn validate_artifact(command: &PutAdfArtifact) -> Result<(), AdfStoreError> {
    let artifact = &command.artifact;
    let definition = &artifact.definition;
    if command.idempotency_key.trim().is_empty()
        || command.idempotency_key.len() > 256
        || definition.contract_version != ADF_CONTRACT_VERSION
        || definition.revision == 0
        || definition.source_ref.adf_id != definition.adf_id
        || definition.source_ref.revision != definition.revision
        || definition.source_ref.digest != definition.source_digest
        || definition.source_ref.store_identity != "adf:in-memory-v1"
    {
        return Err(AdfStoreError::InvalidArtifact(
            "identity, revision, scope, or idempotency metadata is inconsistent".into(),
        ));
    }
    if digest(&artifact.source) != definition.source_digest {
        return Err(AdfStoreError::InvalidArtifact(
            "source does not match its locked digest".into(),
        ));
    }
    Ok(())
}

fn same_content(left: &AdfArtifact, right: &AdfArtifact) -> bool {
    let left = &left.definition;
    let right = &right.definition;
    left.revision == right.revision
        && left.canonical_name == right.canonical_name
        && left.description == right.description
        && left.input_schema == right.input_schema
        && left.output_schema == right.output_schema
        && left.entrypoint == right.entrypoint
        && left.runtime == right.runtime
        && left.source_digest == right.source_digest
        && left.manifest_digest == right.manifest_digest
        && left.effective_risk == right.effective_risk
        && left.granted_capabilities == right.granted_capabilities
        && left.owner == right.owner
        && left.execution_mode == right.execution_mode
}

fn digest(source: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(source.as_bytes());
    format!("sha256:{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use agent_core::{
        adf::{
            AdfExecutionMode, AdfOwner, AdfRuntimeDescriptor, AdfRuntimeKind, LockedAdfDefinition,
        },
        harness::RunId,
        tool::ToolRiskLevel,
    };
    use serde_json::json;

    use super::*;

    fn artifact(run_id: RunId, idempotency_suffix: &str) -> (PutAdfArtifact, AdfArtifactRef) {
        let source = "export function main(input) { return input; }";
        let source_digest = digest(source);
        let adf_id = AdfId::new();
        let reference = AdfArtifactRef {
            adf_id,
            revision: 1,
            digest: source_digest.clone(),
            store_identity: "adf:in-memory-v1".into(),
        };
        let definition = LockedAdfDefinition {
            contract_version: ADF_CONTRACT_VERSION,
            adf_id,
            revision: 1,
            canonical_name: "adf_identity_12345678".into(),
            description: "Returns its JSON input.".into(),
            input_schema: json!({"type": "object"}),
            output_schema: None,
            entrypoint: "main".into(),
            runtime: AdfRuntimeDescriptor {
                identity: "adf:quickjs-v1".into(),
                kind: AdfRuntimeKind::JavaScript,
                version: "0.1.0".into(),
                script_runtime: None,
                profile: None,
            },
            source_ref: reference.clone(),
            source_digest,
            manifest_digest: "sha256:manifest".into(),
            effective_risk: ToolRiskLevel::Low,
            granted_capabilities: Vec::new(),
            owner: AdfOwner::Run { run_id },
            execution_mode: AdfExecutionMode::Sync,
            created_by_run_id: run_id,
            created_at_ms: 1,
        };
        (
            PutAdfArtifact {
                artifact: AdfArtifact {
                    definition,
                    source: source.into(),
                    active: true,
                },
                idempotency_key: format!("define-{idempotency_suffix}"),
            },
            reference,
        )
    }

    #[tokio::test]
    async fn stores_lists_and_deactivates_a_locked_artifact() {
        let store = InMemoryAdfArtifactStore::new();
        let run_id = RunId::new();
        let (command, reference) = artifact(run_id, "identity");

        assert_eq!(
            store.put(command).await.expect("put should succeed"),
            reference
        );
        let loaded = store
            .get(reference.clone())
            .await
            .expect("get should succeed")
            .expect("artifact should exist");
        assert!(loaded.active);
        assert_eq!(loaded.definition.owner, AdfOwner::Run { run_id });

        store
            .deactivate(reference.clone())
            .await
            .expect("deactivate should succeed");
        assert!(
            store
                .list(AdfArtifactQuery {
                    owner: Some(AdfOwner::Run { run_id }),
                    active_only: true,
                })
                .await
                .expect("list should succeed")
                .is_empty()
        );
        assert!(
            !store
                .get(reference)
                .await
                .expect("get after deactivate should succeed")
                .expect("deactivated artifact should remain readable")
                .active
        );
    }

    #[tokio::test]
    async fn idempotency_replays_identical_content_and_rejects_conflicts() {
        let store = InMemoryAdfArtifactStore::new();
        let run_id = RunId::new();
        let (first, reference) = artifact(run_id, "same-key");
        let replay = first.clone();
        store.put(first).await.expect("first put should succeed");
        assert_eq!(
            store
                .put(replay)
                .await
                .expect("identical idempotency replay should succeed"),
            reference
        );

        let (mut conflict, _) = artifact(run_id, "same-key");
        conflict.idempotency_key = "define-same-key".into();
        conflict.artifact.definition.description = "Different semantics.".into();
        conflict.artifact.definition.manifest_digest = "sha256:different-manifest".into();
        assert!(matches!(
            store.put(conflict).await,
            Err(AdfStoreError::IdempotencyConflict)
        ));
    }
}
