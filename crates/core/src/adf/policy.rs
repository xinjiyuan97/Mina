use jsonschema::JSONSchema;

use crate::{script::ScriptRuntimeDescriptor, tool::ToolRiskLevel};

use super::{
    AdfDefinitionRequest, AdfError, AdfErrorKind, AdfExecutionMode, AdfPolicy, AdfPolicyDecision,
    AdfPolicyRequest, AdfRuntimeDescriptor, AdfRuntimeKind, AdfRuntimeRequest, AdfScope,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunScopedJavaScriptPolicy {
    pub max_source_bytes: u64,
    pub runtime: ScriptRuntimeDescriptor,
}

impl RunScopedJavaScriptPolicy {
    #[must_use]
    pub const fn new(runtime: ScriptRuntimeDescriptor) -> Self {
        Self {
            max_source_bytes: 256 * 1024,
            runtime,
        }
    }
}

impl AdfPolicy for RunScopedJavaScriptPolicy {
    fn evaluate(&self, request: &AdfPolicyRequest) -> Result<AdfPolicyDecision, AdfError> {
        validate_definition(&request.definition, self.max_source_bytes)?;
        if !matches!(request.definition.runtime, AdfRuntimeRequest::JavaScript) {
            return Err(denied(
                "adf_runtime_not_allowed",
                "the foundational ADF policy only allows JavaScript",
            ));
        }
        if request.definition.requested_scope != AdfScope::Run {
            return Err(denied(
                "adf_scope_denied",
                "the foundational ADF policy only allows run-scoped functions",
            ));
        }
        if request.definition.execution_mode != AdfExecutionMode::Sync {
            return Err(denied(
                "adf_execution_mode_denied",
                "durable jobs are not enabled in the foundational ADF policy",
            ));
        }
        if !request.definition.requested_capabilities.is_empty() {
            return Err(denied(
                "adf_capability_denied",
                "host capabilities are not enabled for foundational ADF functions",
            ));
        }

        Ok(AdfPolicyDecision {
            runtime: AdfRuntimeDescriptor {
                identity: "adf:quickjs-v1".into(),
                kind: AdfRuntimeKind::JavaScript,
                version: env!("CARGO_PKG_VERSION").into(),
                script_runtime: Some(self.runtime.clone()),
                profile: None,
            },
            effective_risk: ToolRiskLevel::Low,
            granted_capabilities: Vec::new(),
            scope: AdfScope::Run,
            execution_mode: AdfExecutionMode::Sync,
        })
    }
}

pub fn validate_definition(
    definition: &AdfDefinitionRequest,
    max_source_bytes: u64,
) -> Result<(), AdfError> {
    if !valid_slug(&definition.requested_name)
        || definition.description.trim().is_empty()
        || !valid_identifier(&definition.entrypoint)
        || definition.idempotency_key.trim().is_empty()
        || definition.idempotency_key.len() > 256
    {
        return Err(invalid(
            "adf_invalid_definition",
            "ADF name, description, entrypoint, or idempotency key is invalid",
        ));
    }
    let source_bytes = u64::try_from(definition.source.len()).unwrap_or(u64::MAX);
    if definition.source.trim().is_empty() || source_bytes > max_source_bytes {
        return Err(AdfError::new(
            AdfErrorKind::ResourceExhausted,
            "adf_limit_exceeded",
            "ADF source must be non-empty and within the host size limit",
            false,
        ));
    }
    JSONSchema::compile(&definition.input_schema).map_err(|_| {
        invalid(
            "adf_invalid_schema",
            "ADF input_schema is not a supported JSON Schema",
        )
    })?;
    if let Some(output_schema) = &definition.output_schema {
        JSONSchema::compile(output_schema).map_err(|_| {
            invalid(
                "adf_invalid_schema",
                "ADF output_schema is not a supported JSON Schema",
            )
        })?;
    }
    Ok(())
}

fn valid_slug(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '_' | '-'))
}

fn valid_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first.is_ascii_alphabetic() || first == '_' || first == '$')
        && value.len() <= 128
        && chars.all(|character| {
            character.is_ascii_alphanumeric() || character == '_' || character == '$'
        })
}

fn invalid(code: &'static str, message: &'static str) -> AdfError {
    AdfError::new(AdfErrorKind::InvalidRequest, code, message, false)
}

fn denied(code: &'static str, message: &'static str) -> AdfError {
    AdfError::new(AdfErrorKind::PermissionDenied, code, message, false)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::{
        adf::{AdfDefinitionRequest, AdfPolicyRequest},
        harness::RunId,
        script::{SCRIPT_ABI_VERSION, ScriptIsolation, ScriptRuntimeDescriptor},
    };

    fn policy() -> RunScopedJavaScriptPolicy {
        RunScopedJavaScriptPolicy::new(ScriptRuntimeDescriptor {
            identity: "script:quickjs-test".into(),
            kind: "quickjs".into(),
            version: "0.1.0".into(),
            engine: "quickjs".into(),
            engine_version: "test".into(),
            abi_version: SCRIPT_ABI_VERSION,
            isolation: ScriptIsolation::InProcess,
        })
    }

    fn request() -> AdfPolicyRequest {
        AdfPolicyRequest {
            run_id: RunId::new(),
            definition: AdfDefinitionRequest {
                requested_name: "sum_values".into(),
                description: "Sums numeric values.".into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {"values": {"type": "array"}},
                    "required": ["values"]
                }),
                output_schema: None,
                runtime: AdfRuntimeRequest::JavaScript,
                source: "export function main(input) { return input.values.length; }".into(),
                entrypoint: "main".into(),
                requested_capabilities: Vec::new(),
                requested_scope: AdfScope::Run,
                execution_mode: AdfExecutionMode::Sync,
                idempotency_key: "define-sum-v1".into(),
            },
        }
    }

    #[test]
    fn foundational_policy_allows_only_pure_run_scoped_javascript() {
        let policy = policy();
        let decision = policy
            .evaluate(&request())
            .expect("pure JavaScript should be allowed");
        assert_eq!(decision.scope, AdfScope::Run);
        assert_eq!(decision.runtime.kind, AdfRuntimeKind::JavaScript);
        assert_eq!(decision.effective_risk, ToolRiskLevel::Low);

        let mut denied_request = request();
        denied_request.definition.requested_scope = AdfScope::Workspace;
        let error = policy
            .evaluate(&denied_request)
            .expect_err("workspace promotion is not foundational behavior");
        assert_eq!(error.code(), "adf_scope_denied");
    }

    #[test]
    fn validates_schema_and_source_limits() {
        let mut policy = policy();
        policy.max_source_bytes = 8;
        let error = policy
            .evaluate(&request())
            .expect_err("source must respect the host ceiling");
        assert_eq!(error.code(), "adf_limit_exceeded");
    }
}
