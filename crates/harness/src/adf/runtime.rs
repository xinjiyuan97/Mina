use std::sync::Arc;

use agent_core::adf::{
    AdfArtifact, AdfError, AdfErrorKind, AdfExecutionFuture, AdfExecutionOutput,
    AdfExecutionRequest, AdfExecutor, AdfRuntimeDescriptor, AdfRuntimeKind, AdfValidationFuture,
};
use agent_core::script::{
    ScriptError, ScriptErrorKind, ScriptExecutionRequest, ScriptLimits, ScriptModuleRef,
    ScriptPurpose, ScriptRuntime, ScriptSource, ScriptValidationRequest,
};
use jsonschema::JSONSchema;

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
