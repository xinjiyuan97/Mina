use std::{sync::Arc, time::Instant};

use rquickjs::{Context, Function, Module, Object, Runtime, Value, promise::MaybePromise};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use agent_core::script::{
    SCRIPT_ABI_VERSION, ScriptError, ScriptErrorKind, ScriptExecutionFuture, ScriptExecutionOutput,
    ScriptExecutionRequest, ScriptIsolation, ScriptLanguage, ScriptLimits, ScriptLog,
    ScriptPurpose, ScriptRuntime, ScriptRuntimeDescriptor, ScriptUsage, ScriptValidationFuture,
    ScriptValidationOutput, ScriptValidationRequest,
};

const RQUICKJS_VERSION: &str = "0.12.2";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuickJsRuntimeConfig {
    pub max_source_bytes: u64,
    pub max_limits: ScriptLimits,
    pub max_concurrent_executions: usize,
}

impl Default for QuickJsRuntimeConfig {
    fn default() -> Self {
        Self {
            max_source_bytes: 256 * 1024,
            max_limits: ScriptLimits::default(),
            max_concurrent_executions: 2,
        }
    }
}

#[derive(Clone)]
pub struct QuickJsRuntime {
    config: QuickJsRuntimeConfig,
    executions: Arc<Semaphore>,
}

impl std::fmt::Debug for QuickJsRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QuickJsRuntime")
            .field("config", &self.config)
            .field("available_executions", &self.executions.available_permits())
            .finish()
    }
}

impl QuickJsRuntime {
    pub fn new(config: QuickJsRuntimeConfig) -> Result<Self, ScriptError> {
        if config.max_source_bytes == 0 || config.max_concurrent_executions == 0 {
            return Err(invalid_request(
                "script_invalid_runtime_config",
                "QuickJS source and concurrency limits must be positive",
            ));
        }
        config.max_limits.validate_against(&config.max_limits)?;
        Ok(Self {
            executions: Arc::new(Semaphore::new(config.max_concurrent_executions)),
            config,
        })
    }

    fn try_acquire(&self) -> Result<OwnedSemaphorePermit, ScriptError> {
        Arc::clone(&self.executions)
            .try_acquire_owned()
            .map_err(|_| {
                ScriptError::new(
                    ScriptErrorKind::Unavailable,
                    "script_runtime_busy",
                    "the QuickJS execution pool is busy",
                    true,
                )
            })
    }
}

impl Default for QuickJsRuntime {
    fn default() -> Self {
        Self::new(QuickJsRuntimeConfig::default())
            .expect("the built-in QuickJS runtime limits are valid")
    }
}

impl ScriptRuntime for QuickJsRuntime {
    fn descriptor(&self) -> ScriptRuntimeDescriptor {
        ScriptRuntimeDescriptor {
            identity: "script:quickjs-v1".into(),
            kind: "quickjs".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            engine: "quickjs".into(),
            engine_version: format!("rquickjs-{RQUICKJS_VERSION}"),
            abi_version: SCRIPT_ABI_VERSION,
            isolation: ScriptIsolation::InProcess,
        }
    }

    fn validate(&self, request: ScriptValidationRequest) -> ScriptValidationFuture {
        let config = self.config.clone();
        let permit = self.try_acquire();
        Box::pin(async move {
            let _permit = permit?;
            tokio::task::spawn_blocking(move || validate_sync(&config, request))
                .await
                .map_err(join_error)?
        })
    }

    fn execute(&self, request: ScriptExecutionRequest) -> ScriptExecutionFuture {
        let config = self.config.clone();
        let permit = self.try_acquire();
        Box::pin(async move {
            let _permit = permit?;
            tokio::task::spawn_blocking(move || execute_sync(&config, request))
                .await
                .map_err(join_error)?
        })
    }
}

fn validate_sync(
    config: &QuickJsRuntimeConfig,
    request: ScriptValidationRequest,
) -> Result<ScriptValidationOutput, ScriptError> {
    let checked = check_request(
        config,
        request.language,
        request.purpose,
        &request.source,
        &request.export,
        &request.requested_capabilities,
        request.limits,
    )?;
    let runtime = build_runtime(request.limits, None)?;
    let context = Context::full(&runtime).map_err(|error| map_engine_error(error, None))?;
    context
        .with(|context| {
            Module::declare(
                context,
                module_name(&checked.digest),
                checked.source.as_bytes(),
            )
            .map(|_| ())
        })
        .map_err(|_| {
            ScriptError::new(
                ScriptErrorKind::InvalidSource,
                "script_invalid_source",
                "the JavaScript module could not be compiled",
                false,
            )
        })?;

    Ok(ScriptValidationOutput {
        source_digest: checked.digest,
        source_bytes: checked.source_bytes,
        export: request.export,
    })
}

fn execute_sync(
    config: &QuickJsRuntimeConfig,
    request: ScriptExecutionRequest,
) -> Result<ScriptExecutionOutput, ScriptError> {
    let started = Instant::now();
    if request.cancellation.is_cancelled() {
        return Err(cancelled());
    }
    let checked = check_request(
        config,
        request.language,
        request.purpose,
        &request.source,
        &request.export,
        &request.granted_capabilities,
        request.limits,
    )?;
    let deadline = started + std::time::Duration::from_millis(request.limits.timeout_ms);
    let cancellation = request.cancellation.clone();
    let runtime = build_runtime(request.limits, Some((deadline, cancellation.clone())))?;
    let context = Context::full(&runtime)
        .map_err(|error| map_engine_error(error, Some((deadline, &cancellation))))?;
    let input = serde_json::to_string(&request.input).map_err(|_| {
        invalid_request(
            "script_invalid_input",
            "script input must be JSON serializable",
        )
    })?;
    let execution_id = request.execution_id.clone();
    let run_id = request.run_id.map(|run_id| run_id.to_string());
    let execution_purpose = request.purpose;
    let purpose = purpose_name(execution_purpose);
    let export = request.export;

    let output_json = context.with(|context| {
        let declared = Module::declare(
            context.clone(),
            module_name(&checked.digest),
            checked.source.as_bytes(),
        )
        .map_err(|error| map_engine_error(error, Some((deadline, &cancellation))))?;
        let (module, evaluation) = declared
            .eval()
            .map_err(|error| map_engine_error(error, Some((deadline, &cancellation))))?;
        evaluation
            .finish::<()>()
            .map_err(|error| map_engine_error(error, Some((deadline, &cancellation))))?;

        let function: Function = module.get(export.as_str()).map_err(|_| {
            ScriptError::new(
                ScriptErrorKind::NotFound,
                "script_export_not_found",
                "the requested JavaScript export was not found",
                false,
            )
        })?;
        let input = context.json_parse(input.as_bytes()).map_err(|_| {
            invalid_request("script_invalid_input", "script input is not valid JSON")
        })?;
        let execution_context = Object::new(context.clone())
            .map_err(|error| map_engine_error(error, Some((deadline, &cancellation))))?;
        execution_context
            .set("executionId", execution_id.as_str())
            .map_err(|error| map_engine_error(error, Some((deadline, &cancellation))))?;
        execution_context
            .set("purpose", purpose)
            .map_err(|error| map_engine_error(error, Some((deadline, &cancellation))))?;
        if let Some(run_id) = run_id.as_deref() {
            execution_context
                .set("runId", run_id)
                .map_err(|error| map_engine_error(error, Some((deadline, &cancellation))))?;
        }

        if let Some(input_object) = input.as_object()
            && let Ok(activated_at_ms) = input_object.get::<_, i64>("activated_at_ms")
        {
            execution_context
                .set("activatedAtMs", activated_at_ms)
                .map_err(|error| map_engine_error(error, Some((deadline, &cancellation))))?;
        }

        let result: MaybePromise = match execution_purpose {
            ScriptPurpose::FlowStart => function.call((execution_context, input)),
            ScriptPurpose::FlowResume => {
                let input_object = input.as_object().ok_or_else(|| {
                    invalid_request(
                        "script_invalid_input",
                        "flow resume input must contain checkpoint and events",
                    )
                })?;
                let checkpoint: Value = input_object.get("checkpoint").map_err(|_| {
                    invalid_request(
                        "script_invalid_input",
                        "flow resume input is missing checkpoint",
                    )
                })?;
                let events: Value = input_object.get("events").map_err(|_| {
                    invalid_request(
                        "script_invalid_input",
                        "flow resume input is missing events",
                    )
                })?;
                function.call((execution_context, checkpoint, events))
            }
            ScriptPurpose::Eval | ScriptPurpose::Tool => function.call((input, execution_context)),
        }
        .map_err(|error| map_engine_error(error, Some((deadline, &cancellation))))?;
        let result: Value = result
            .finish()
            .map_err(|error| map_engine_error(error, Some((deadline, &cancellation))))?;
        let json = context
            .json_stringify(result)
            .map_err(|_| invalid_result())?
            .ok_or_else(invalid_result)?;
        json.to_string().map_err(|_| invalid_result())
    })?;

    let output_bytes = u64::try_from(output_json.len()).unwrap_or(u64::MAX);
    if output_bytes > request.limits.max_output_bytes {
        return Err(ScriptError::new(
            ScriptErrorKind::ResourceExhausted,
            "script_output_too_large",
            "script output exceeded the configured limit",
            false,
        ));
    }
    let value: JsonValue = serde_json::from_str(&output_json).map_err(|_| invalid_result())?;
    let memory = runtime.memory_usage().memory_used_size.max(0) as u64;

    Ok(ScriptExecutionOutput {
        value,
        logs: Vec::<ScriptLog>::new(),
        usage: ScriptUsage {
            duration_ms: started.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
            peak_memory_bytes: Some(memory),
            host_calls: 0,
            output_bytes,
        },
        module_digest: checked.digest,
    })
}

struct CheckedSource<'a> {
    source: &'a str,
    source_bytes: u64,
    digest: String,
}

#[allow(clippy::too_many_arguments)]
fn check_request<'a>(
    config: &QuickJsRuntimeConfig,
    language: ScriptLanguage,
    _purpose: ScriptPurpose,
    source: &'a super::ScriptSource,
    export: &str,
    capabilities: &[super::ScriptCapability],
    limits: ScriptLimits,
) -> Result<CheckedSource<'a>, ScriptError> {
    if language != ScriptLanguage::JavaScript {
        return Err(invalid_request(
            "script_language_not_supported",
            "the QuickJS runtime only supports JavaScript",
        ));
    }
    if !capabilities.is_empty() {
        return Err(ScriptError::new(
            ScriptErrorKind::CapabilityDenied,
            "script_capability_denied",
            "the foundational QuickJS runtime does not expose host capabilities",
            false,
        ));
    }
    limits.validate_against(&config.max_limits)?;
    if !valid_export(export) {
        return Err(invalid_request(
            "script_invalid_export",
            "the JavaScript export must be a valid identifier",
        ));
    }
    let source_text = source.source();
    let source_bytes = u64::try_from(source_text.len()).unwrap_or(u64::MAX);
    if source_text.trim().is_empty() || source_bytes > config.max_source_bytes {
        return Err(ScriptError::new(
            ScriptErrorKind::ResourceExhausted,
            "script_source_too_large",
            "script source must be non-empty and within the host size limit",
            false,
        ));
    }
    let digest = source_digest(source_text);
    if let Some(expected) = source.expected_digest()
        && expected != digest
    {
        return Err(ScriptError::new(
            ScriptErrorKind::InvalidSource,
            "script_digest_mismatch",
            "script source does not match the locked digest",
            false,
        ));
    }
    Ok(CheckedSource {
        source: source_text,
        source_bytes,
        digest,
    })
}

fn build_runtime(
    limits: ScriptLimits,
    interrupt: Option<(Instant, agent_core::harness::RunCancellation)>,
) -> Result<Runtime, ScriptError> {
    let memory_limit = usize::try_from(limits.memory_bytes).map_err(|_| {
        invalid_request(
            "script_invalid_limits",
            "script memory limit is unsupported on this platform",
        )
    })?;
    let stack_limit = usize::try_from(limits.max_stack_bytes).map_err(|_| {
        invalid_request(
            "script_invalid_limits",
            "script stack limit is unsupported on this platform",
        )
    })?;
    let runtime = Runtime::new().map_err(|error| map_engine_error(error, None))?;
    runtime.set_memory_limit(memory_limit);
    runtime.set_max_stack_size(stack_limit);
    if let Some((deadline, cancellation)) = interrupt {
        runtime.set_interrupt_handler(Some(Box::new(move || {
            cancellation.is_cancelled() || Instant::now() >= deadline
        })));
    }
    Ok(runtime)
}

fn valid_export(export: &str) -> bool {
    let mut chars = export.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first.is_ascii_alphabetic() || first == '_' || first == '$')
        && export.len() <= 128
        && chars.all(|character| {
            character.is_ascii_alphanumeric() || character == '_' || character == '$'
        })
}

fn source_digest(source: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(source.as_bytes());
    format!("sha256:{:x}", hasher.finalize())
}

fn module_name(digest: &str) -> String {
    format!("mina-{}.mjs", digest.trim_start_matches("sha256:"))
}

const fn purpose_name(purpose: ScriptPurpose) -> &'static str {
    match purpose {
        ScriptPurpose::Eval => "eval",
        ScriptPurpose::Tool => "tool",
        ScriptPurpose::FlowStart => "flow_start",
        ScriptPurpose::FlowResume => "flow_resume",
    }
}

fn map_engine_error(
    error: rquickjs::Error,
    interrupt: Option<(Instant, &agent_core::harness::RunCancellation)>,
) -> ScriptError {
    if let Some((deadline, cancellation)) = interrupt {
        if cancellation.is_cancelled() {
            return cancelled();
        }
        if Instant::now() >= deadline {
            return ScriptError::new(
                ScriptErrorKind::Timeout,
                "script_timeout",
                "script execution exceeded the configured timeout",
                false,
            );
        }
    }
    if matches!(error, rquickjs::Error::Allocation) {
        return ScriptError::new(
            ScriptErrorKind::ResourceExhausted,
            "script_memory_exhausted",
            "script execution exceeded the configured memory limit",
            false,
        );
    }
    ScriptError::new(
        ScriptErrorKind::Runtime,
        "script_runtime_failed",
        "the JavaScript runtime could not complete the script",
        false,
    )
}

fn invalid_request(code: &'static str, message: &'static str) -> ScriptError {
    ScriptError::new(ScriptErrorKind::InvalidRequest, code, message, false)
}

fn invalid_result() -> ScriptError {
    ScriptError::new(
        ScriptErrorKind::InvalidResult,
        "script_invalid_result",
        "script output must be JSON serializable",
        false,
    )
}

fn cancelled() -> ScriptError {
    ScriptError::new(
        ScriptErrorKind::Cancelled,
        "script_cancelled",
        "script execution was cancelled",
        false,
    )
}

fn join_error(_error: tokio::task::JoinError) -> ScriptError {
    ScriptError::new(
        ScriptErrorKind::Internal,
        "script_worker_failed",
        "the script execution worker stopped unexpectedly",
        true,
    )
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[tokio::test]
    async fn validates_and_executes_a_json_module() {
        let runtime = QuickJsRuntime::default();
        let source = r#"
            export function main(input, context) {
                return {
                    total: input.values.reduce((sum, value) => sum + value, 0),
                    purpose: context.purpose,
                };
            }
        "#;
        let validation = runtime
            .validate(ScriptValidationRequest {
                language: ScriptLanguage::JavaScript,
                purpose: ScriptPurpose::Eval,
                source: super::super::ScriptSource::Inline {
                    source: source.into(),
                    expected_digest: None,
                },
                export: "main".into(),
                requested_capabilities: Vec::new(),
                limits: ScriptLimits::default(),
            })
            .await
            .expect("module should validate");
        let output = runtime
            .execute(ScriptExecutionRequest::inline(
                "execution-1",
                source,
                json!({"values": [2, 3, 5]}),
            ))
            .await
            .expect("module should execute");

        assert_eq!(output.value, json!({"total": 10, "purpose": "eval"}));
        assert_eq!(output.module_digest, validation.source_digest);
        assert!(output.usage.output_bytes > 0);
    }

    #[tokio::test]
    async fn interrupts_an_infinite_loop() {
        let runtime = QuickJsRuntime::default();
        let mut request = ScriptExecutionRequest::inline(
            "execution-timeout",
            "export function main() { while (true) {} }",
            json!(null),
        );
        request.limits.timeout_ms = 20;

        let error = runtime
            .execute(request)
            .await
            .expect_err("the interrupt handler should stop the loop");
        assert_eq!(error.code(), "script_timeout");
        assert_eq!(error.kind(), ScriptErrorKind::Timeout);
    }

    #[tokio::test]
    async fn rejects_non_json_results() {
        let runtime = QuickJsRuntime::default();
        let request = ScriptExecutionRequest::inline(
            "execution-result",
            "export function main() { return 10n; }",
            json!(null),
        );

        let error = runtime
            .execute(request)
            .await
            .expect_err("BigInt is not JSON serializable");
        assert_eq!(error.code(), "script_invalid_result");
    }

    #[tokio::test]
    async fn verifies_locked_source_digests() {
        let runtime = QuickJsRuntime::default();
        let mut request = ScriptExecutionRequest::inline(
            "execution-digest",
            "export function main() { return null; }",
            json!(null),
        );
        request.source = super::super::ScriptSource::Inline {
            source: request.source.source().into(),
            expected_digest: Some("sha256:not-the-source".into()),
        };

        let error = runtime
            .execute(request)
            .await
            .expect_err("a locked digest must be verified");
        assert_eq!(error.code(), "script_digest_mismatch");
    }
}
