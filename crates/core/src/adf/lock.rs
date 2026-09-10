use serde_json::json;
use sha2::{Digest, Sha256};

use crate::harness::RunId;

use super::{
    ADF_CONTRACT_VERSION, AdfArtifact, AdfArtifactRef, AdfDefinitionRequest, AdfError,
    AdfErrorKind, AdfId, AdfOwner, AdfPolicyDecision, LockedAdfDefinition, PutAdfArtifact,
};

/// Validated inputs used to turn a policy-approved ADF request into an
/// immutable artifact definition.
#[derive(Debug, Clone)]
pub struct LockAdfDefinition {
    pub request: AdfDefinitionRequest,
    pub decision: AdfPolicyDecision,
    pub owner: AdfOwner,
    pub adf_id: AdfId,
    pub revision: u64,
    pub store_identity: String,
    pub created_by_run_id: RunId,
    pub created_at_ms: i64,
}

/// Locks an ADF definition after policy evaluation.
///
/// This is domain logic rather than host orchestration, so it lives beside the
/// ADF contracts in `agent-core` and can be reused by every extension host.
pub fn lock_definition(command: LockAdfDefinition) -> Result<PutAdfArtifact, AdfError> {
    if command.revision == 0
        || command.store_identity.trim().is_empty()
        || command.owner.scope() != command.request.requested_scope
        || command.decision.scope != command.request.requested_scope
        || command.decision.execution_mode != command.request.execution_mode
        || command.decision.runtime.kind != command.request.runtime.kind()
    {
        return Err(AdfError::new(
            AdfErrorKind::InvalidRequest,
            "adf_lock_invalid",
            "ADF policy, owner, runtime, scope, or revision is inconsistent",
            false,
        ));
    }
    let request = command.request;
    let decision = command.decision;
    let owner = command.owner;
    let source_digest = digest(request.source.as_bytes());
    let slug = request
        .requested_name
        .to_ascii_lowercase()
        .replace('-', "_");
    let short_digest = source_digest
        .strip_prefix("sha256:")
        .and_then(|digest| digest.get(..8))
        .ok_or_else(|| {
            AdfError::new(
                AdfErrorKind::Internal,
                "adf_digest_failed",
                "ADF source digest could not be generated",
                false,
            )
        })?;
    let canonical_name = format!("adf_{slug}_{short_digest}");
    let manifest = json!({
        "contract_version": ADF_CONTRACT_VERSION,
        "canonical_name": &canonical_name,
        "description": &request.description,
        "input_schema": &request.input_schema,
        "output_schema": &request.output_schema,
        "entrypoint": &request.entrypoint,
        "runtime": &decision.runtime,
        "source_digest": &source_digest,
        "effective_risk": decision.effective_risk,
        "granted_capabilities": &decision.granted_capabilities,
        "owner": &owner,
        "execution_mode": decision.execution_mode,
    });
    let manifest_bytes = serde_json::to_vec(&manifest).map_err(|_| {
        AdfError::new(
            AdfErrorKind::Internal,
            "adf_digest_failed",
            "ADF manifest digest could not be generated",
            false,
        )
    })?;
    let manifest_digest = digest(&manifest_bytes);
    let source_ref = AdfArtifactRef {
        adf_id: command.adf_id,
        revision: command.revision,
        digest: source_digest.clone(),
        store_identity: command.store_identity,
    };
    let definition = LockedAdfDefinition {
        contract_version: ADF_CONTRACT_VERSION,
        adf_id: command.adf_id,
        revision: command.revision,
        canonical_name,
        description: request.description,
        input_schema: request.input_schema,
        output_schema: request.output_schema,
        entrypoint: request.entrypoint,
        runtime: decision.runtime,
        source_ref,
        source_digest,
        manifest_digest,
        effective_risk: decision.effective_risk,
        granted_capabilities: decision.granted_capabilities,
        owner,
        execution_mode: decision.execution_mode,
        created_by_run_id: command.created_by_run_id,
        created_at_ms: command.created_at_ms,
    };
    Ok(PutAdfArtifact {
        artifact: AdfArtifact {
            definition,
            source: request.source,
            active: true,
        },
        idempotency_key: request.idempotency_key,
    })
}

fn digest(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("sha256:{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::{
        adf::{
            AdfExecutionMode, AdfRuntimeDescriptor, AdfRuntimeKind, AdfRuntimeRequest, AdfScope,
        },
        script::{SCRIPT_ABI_VERSION, ScriptIsolation, ScriptRuntimeDescriptor},
        tool::ToolRiskLevel,
    };

    fn command(run_id: RunId, adf_id: AdfId) -> LockAdfDefinition {
        LockAdfDefinition {
            request: AdfDefinitionRequest {
                requested_name: "sum-values".into(),
                description: "Sums values.".into(),
                input_schema: json!({"type": "object"}),
                output_schema: None,
                runtime: AdfRuntimeRequest::JavaScript,
                source: "export function main(input) { return input; }".into(),
                entrypoint: "main".into(),
                requested_capabilities: Vec::new(),
                requested_scope: AdfScope::Run,
                execution_mode: AdfExecutionMode::Sync,
                idempotency_key: "define-sum-v1".into(),
            },
            decision: AdfPolicyDecision {
                runtime: AdfRuntimeDescriptor {
                    identity: "adf:quickjs-v1".into(),
                    kind: AdfRuntimeKind::JavaScript,
                    version: "0.1.0".into(),
                    script_runtime: Some(ScriptRuntimeDescriptor {
                        identity: "script:quickjs-v1".into(),
                        kind: "quickjs".into(),
                        version: "0.1.0".into(),
                        engine: "quickjs".into(),
                        engine_version: "test".into(),
                        abi_version: SCRIPT_ABI_VERSION,
                        isolation: ScriptIsolation::InProcess,
                    }),
                    profile: None,
                },
                effective_risk: ToolRiskLevel::Low,
                granted_capabilities: Vec::new(),
                scope: AdfScope::Run,
                execution_mode: AdfExecutionMode::Sync,
            },
            owner: AdfOwner::Run { run_id },
            adf_id,
            revision: 1,
            store_identity: "adf:test".into(),
            created_by_run_id: run_id,
            created_at_ms: 1,
        }
    }

    #[test]
    fn locks_source_and_manifest_digests_independently_of_generated_identity() {
        let run_id = RunId::new();
        let first = lock_definition(command(run_id, AdfId::new())).expect("lock should succeed");
        let second = lock_definition(command(run_id, AdfId::new())).expect("lock should succeed");

        assert_eq!(
            first.artifact.definition.canonical_name,
            second.artifact.definition.canonical_name
        );
        assert_eq!(
            first.artifact.definition.manifest_digest,
            second.artifact.definition.manifest_digest
        );
        assert_eq!(
            first.artifact.definition.source_ref.digest,
            first.artifact.definition.source_digest
        );
        assert_ne!(
            first.artifact.definition.adf_id,
            second.artifact.definition.adf_id
        );
    }
}
