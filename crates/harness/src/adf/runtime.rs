use std::sync::Arc;

use jsonschema::JSONSchema;
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::script::{
    ScriptError, ScriptErrorKind, ScriptExecutionRequest, ScriptLimits, ScriptModuleRef,
    ScriptPurpose, ScriptRuntime, ScriptSource, ScriptValidationRequest,
};

use super::{
    ADF_CONTRACT_VERSION, AdfArtifact, AdfArtifactRef, AdfDefinitionRequest, AdfError,
    AdfErrorKind, AdfExecutionFuture, AdfExecutionOutput, AdfExecutionRequest, AdfExecutor, AdfId,
    AdfOwner, AdfPolicyDecision, AdfRuntimeDescriptor, AdfRuntimeKind, AdfValidationFuture,
    PutAdfArtifact,
};

#[derive(Debug, Clone)]
pub struct LockAdfDefinition {
    pub request: AdfDefinitionRequest,
    pub decision: AdfPolicyDecision,
    pub owner: AdfOwner,
    pub adf_id: AdfId,
    pub revision: u64,
    pub store_identity: String,
    pub created_by_run_id: agent_core::harness::RunId,
    pub created_at_ms: i64,
}

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
    let definition = super::LockedAdfDefinition {
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

#[derive(Clone)]
pub struct JavaScriptAdfExecutor {
    runtime: Arc<dyn ScriptRuntime>,
    limits: ScriptLimits,
}

impl std::fmt::Debug for JavaScriptAdfExecutor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("JavaScriptAdfExecutor")
            .field("runtime", &self.runtime.descriptor())
            .field("limits", &self.limits)
            .finish()
    }
}

impl JavaScriptAdfExecutor {
    #[must_use]
    pub fn new(runtime: Arc<dyn ScriptRuntime>, limits: ScriptLimits) -> Self {
        Self { runtime, limits }
    }
}

impl AdfExecutor for JavaScriptAdfExecutor {
    fn descriptor(&self) -> AdfRuntimeDescriptor {
        let script_runtime = self.runtime.descriptor();
        AdfRuntimeDescriptor {
            identity: format!("adf:{}", script_runtime.identity),
            kind: AdfRuntimeKind::JavaScript,
            version: env!("CARGO_PKG_VERSION").into(),
            script_runtime: Some(script_runtime),
            profile: None,
        }
    }

    fn validate(&self, artifact: AdfArtifact) -> AdfValidationFuture {
        let runtime = Arc::clone(&self.runtime);
        let limits = self.limits;
        Box::pin(async move {
            let definition = artifact.definition;
            if definition.runtime.kind != AdfRuntimeKind::JavaScript {
                return Err(AdfError::new(
                    AdfErrorKind::InvalidRequest,
                    "adf_runtime_mismatch",
                    "the ADF definition is not a JavaScript function",
                    false,
                ));
            }
            runtime
                .validate(ScriptValidationRequest {
                    language: agent_core::script::ScriptLanguage::JavaScript,
                    purpose: ScriptPurpose::Tool,
                    source: ScriptSource::ResolvedArtifact {
                        reference: ScriptModuleRef {
                            module_id: definition.adf_id.to_string(),
                            revision: definition.revision,
                            digest: definition.source_digest,
                        },
                        source: artifact.source,
                    },
                    export: definition.entrypoint,
                    requested_capabilities: definition.granted_capabilities,
                    limits,
                })
                .await
                .map(|_| ())
                .map_err(map_script_error)
        })
    }

    fn execute(&self, request: AdfExecutionRequest) -> AdfExecutionFuture {
        let runtime = Arc::clone(&self.runtime);
        let limits = self.limits;
        Box::pin(async move {
            let definition = &request.artifact.definition;
            if definition.runtime.kind != AdfRuntimeKind::JavaScript {
                return Err(AdfError::new(
                    AdfErrorKind::InvalidRequest,
                    "adf_runtime_mismatch",
                    "the ADF definition is not a JavaScript function",
                    false,
                ));
            }
            validate_value(
                &definition.input_schema,
                &request.arguments,
                "adf_invalid_arguments",
                "ADF arguments do not match input_schema",
            )?;
            let execution = runtime
                .execute(ScriptExecutionRequest {
                    execution_id: request.call_id,
                    run_id: Some(request.run_id),
                    language: agent_core::script::ScriptLanguage::JavaScript,
                    purpose: ScriptPurpose::Tool,
                    source: ScriptSource::ResolvedArtifact {
                        reference: ScriptModuleRef {
                            module_id: definition.adf_id.to_string(),
                            revision: definition.revision,
                            digest: definition.source_digest.clone(),
                        },
                        source: request.artifact.source,
                    },
                    export: definition.entrypoint.clone(),
                    input: request.arguments,
                    granted_capabilities: definition.granted_capabilities.clone(),
                    limits,
                    cancellation: request.cancellation,
                })
                .await
                .map_err(map_script_error)?;
            if let Some(output_schema) = &definition.output_schema {
                validate_value(
                    output_schema,
                    &execution.value,
                    "adf_output_invalid",
                    "ADF result does not match output_schema",
                )?;
            }
            let content = serde_json::to_string(&execution.value).map_err(|_| {
                AdfError::new(
                    AdfErrorKind::Internal,
                    "adf_output_invalid",
                    "ADF result could not be encoded as JSON",
                    false,
                )
            })?;
            Ok(AdfExecutionOutput {
                value: execution.value,
                content: Some(content),
            })
        })
    }
}

fn validate_value(
    schema: &serde_json::Value,
    value: &serde_json::Value,
    code: &'static str,
    message: &'static str,
) -> Result<(), AdfError> {
    let validator = JSONSchema::compile(schema).map_err(|_| {
        AdfError::new(
            AdfErrorKind::InvalidRequest,
            "adf_invalid_schema",
            "ADF schema is invalid",
            false,
        )
    })?;
    validator
        .validate(value)
        .map_err(|_| AdfError::new(AdfErrorKind::InvalidRequest, code, message, false))
}

fn map_script_error(error: ScriptError) -> AdfError {
    let kind = match error.kind() {
        ScriptErrorKind::InvalidRequest
        | ScriptErrorKind::InvalidSource
        | ScriptErrorKind::InvalidResult => AdfErrorKind::InvalidRequest,
        ScriptErrorKind::NotFound => AdfErrorKind::NotFound,
        ScriptErrorKind::CapabilityDenied => AdfErrorKind::PermissionDenied,
        ScriptErrorKind::ResourceExhausted => AdfErrorKind::ResourceExhausted,
        ScriptErrorKind::Timeout => AdfErrorKind::Timeout,
        ScriptErrorKind::Cancelled => AdfErrorKind::Cancelled,
        ScriptErrorKind::Unavailable => AdfErrorKind::Unavailable,
        ScriptErrorKind::Runtime | ScriptErrorKind::Internal => AdfErrorKind::Internal,
    };
    AdfError::new(kind, error.code(), error.safe_message(), error.retryable())
}

fn digest(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("sha256:{:x}", hasher.finalize())
}

#[cfg(test)]
mod lock_tests {
    use serde_json::json;

    use super::*;
    use agent_core::{
        adf::{AdfExecutionMode, AdfRuntimeRequest, AdfScope},
        harness::RunId,
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

#[cfg(all(test, feature = "quickjs"))]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::script::QuickJsRuntime;
    use agent_core::{
        adf::{
            ADF_CONTRACT_VERSION, AdfArtifact, AdfArtifactRef, AdfExecutionMode, AdfId, AdfOwner,
            LockedAdfDefinition,
        },
        harness::{RunCancellation, RunId},
        tool::ToolRiskLevel,
    };

    fn artifact(run_id: RunId) -> AdfArtifact {
        let source = "export function main(input) { return { total: input.values.reduce((a, b) => a + b, 0) }; }";
        let digest = {
            use sha2::{Digest, Sha256};
            let mut hasher = Sha256::new();
            hasher.update(source.as_bytes());
            format!("sha256:{:x}", hasher.finalize())
        };
        let adf_id = AdfId::new();
        AdfArtifact {
            definition: LockedAdfDefinition {
                contract_version: ADF_CONTRACT_VERSION,
                adf_id,
                revision: 1,
                canonical_name: "adf_sum_values_12345678".into(),
                description: "Sums values.".into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {"values": {"type": "array", "items": {"type": "number"}}},
                    "required": ["values"]
                }),
                output_schema: Some(json!({
                    "type": "object",
                    "properties": {"total": {"type": "number"}},
                    "required": ["total"]
                })),
                entrypoint: "main".into(),
                runtime: AdfRuntimeDescriptor {
                    identity: "adf:quickjs-v1".into(),
                    kind: AdfRuntimeKind::JavaScript,
                    version: "0.1.0".into(),
                    script_runtime: None,
                    profile: None,
                },
                source_ref: AdfArtifactRef {
                    adf_id,
                    revision: 1,
                    digest: digest.clone(),
                    store_identity: "adf:test".into(),
                },
                source_digest: digest,
                manifest_digest: "sha256:manifest".into(),
                effective_risk: ToolRiskLevel::Low,
                granted_capabilities: Vec::new(),
                owner: AdfOwner::Run { run_id },
                execution_mode: AdfExecutionMode::Sync,
                created_by_run_id: run_id,
                created_at_ms: 1,
            },
            source: source.into(),
            active: true,
        }
    }

    #[tokio::test]
    async fn executes_a_locked_javascript_artifact_through_the_script_port() {
        let run_id = RunId::new();
        let executor = JavaScriptAdfExecutor::new(
            Arc::new(QuickJsRuntime::default()),
            ScriptLimits::default(),
        );
        let output = executor
            .execute(AdfExecutionRequest {
                run_id,
                call_id: "call-1".into(),
                artifact: artifact(run_id),
                arguments: json!({"values": [2, 3, 5]}),
                cancellation: RunCancellation::new(),
            })
            .await
            .expect("locked JavaScript ADF should execute");

        assert_eq!(output.value, json!({"total": 10}));
        assert_eq!(output.content.as_deref(), Some("{\"total\":10}"));
    }
}
