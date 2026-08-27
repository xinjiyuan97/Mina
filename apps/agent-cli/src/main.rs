use std::{
    env,
    io::{self, IsTerminal, Write},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use agent_core::{
    adf::RunScopedJavaScriptPolicy,
    harness::{
        AgentLoop, ApprovalError, ApprovalFuture, ApprovalPort, ApprovalRequest, ApprovalResolution,
    },
    script::{ScriptLimits, ScriptRuntime},
};
use agent_extension::adf::{InMemoryAdfArtifactStore, RunAdfToolSession};
use agent_extension::observability::{
    AsyncObservationConfig, AsyncObservationHook, HookObservationExporter, ObservationHook,
    ObservedMachine, ObservedModel, ObservedTools, TracingObservationHook,
};
use agent_extension::provider::OpenAiCompatibleProvider;
use agent_extension::sandbox::{HostProcessSandbox, ProcessSandbox};
use agent_extension::tool::{
    BuiltinToolCatalog, JavaScriptEvalTool, SearchBackend, WorkspaceSearchBackend,
};
use agent_harness::{
    Harness, HarnessConfig, QuickJsConfig, RunOptions,
    adf::JavaScriptAdfExecutor,
    script::{QuickJsRuntime, QuickJsRuntimeConfig},
};
use tracing_subscriber::EnvFilter;

#[derive(Debug)]
struct TerminalApprovals;

impl ApprovalPort for TerminalApprovals {
    fn request(&self, request: ApprovalRequest) -> ApprovalFuture {
        Box::pin(async move {
            tokio::task::spawn_blocking(move || prompt_for_approval(&request))
                .await
                .map_err(|error| {
                    ApprovalError::new("approval_task_failed", format!("approval failed: {error}"))
                })?
        })
    }
}

fn prompt_for_approval(request: &ApprovalRequest) -> Result<ApprovalResolution, ApprovalError> {
    if !io::stdin().is_terminal() {
        return Ok(ApprovalResolution::deny(Some(
            "risky tools require an interactive terminal".to_owned(),
        )));
    }

    eprintln!(
        "\nApprove tool `{}` ({:?}) with arguments:\n{}",
        request.tool_name,
        request.risk_level,
        serde_json::to_string_pretty(&request.arguments).unwrap_or_else(|_| "{}".to_owned())
    );
    eprint!("Allow once? [y/N] ");
    io::stderr()
        .flush()
        .map_err(|error| ApprovalError::new("approval_prompt_failed", error.to_string()))?;

    let mut answer = String::new();
    io::stdin()
        .read_line(&mut answer)
        .map_err(|error| ApprovalError::new("approval_prompt_failed", error.to_string()))?;
    if answer.trim().eq_ignore_ascii_case("y") || answer.trim().eq_ignore_ascii_case("yes") {
        Ok(ApprovalResolution::allow_once())
    } else {
        Ok(ApprovalResolution::deny(Some("denied by user".to_owned())))
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("mina::observation=info")),
        )
        .with_writer(io::stderr)
        .init();

    let input = env::args().skip(1).collect::<Vec<_>>().join(" ");
    if input.trim().is_empty() {
        return Err("usage: agent-cli <task>".into());
    }

    let config_path = env::var_os("MINA_CONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("config/mina.toml"));
    let config = HarnessConfig::load(config_path)?;
    let model = config.default_model();
    let provider = OpenAiCompatibleProvider::from_model_config(model)?;
    let observation_worker = Arc::new(AsyncObservationHook::new(
        Arc::new(HookObservationExporter::new(Arc::new(
            TracingObservationHook,
        ))),
        AsyncObservationConfig::default(),
    )?);
    let hook: Arc<dyn ObservationHook> = observation_worker.clone();
    let provider = ObservedModel::new(provider, Arc::clone(&hook));
    let workspace = env::current_dir()?;

    let process_sandbox: Arc<dyn ProcessSandbox> = Arc::new(HostProcessSandbox::default());
    let search_backend: Arc<dyn SearchBackend> = Arc::new(WorkspaceSearchBackend::new(&workspace)?);
    let mut tools = BuiltinToolCatalog::new(workspace)
        .with_process_sandbox(process_sandbox)
        .with_search_backend(search_backend)
        .enable_terminal_tools()
        .build()?;
    let quickjs = if config.script().quickjs.enabled {
        let limits = script_limits(&config.script().quickjs);
        let runtime: Arc<dyn ScriptRuntime> =
            Arc::new(QuickJsRuntime::new(QuickJsRuntimeConfig {
                max_source_bytes: config.script().quickjs.max_source_bytes,
                max_limits: limits,
                max_concurrent_executions: config.script().quickjs.max_concurrent_executions,
            })?);
        tools.register(JavaScriptEvalTool::new(Arc::clone(&runtime), limits))?;
        Some((runtime, limits))
    } else {
        None
    };
    let mut tools = RunAdfToolSession::new(tools);
    if config.adf().enabled {
        let (runtime, limits) = quickjs
            .as_ref()
            .expect("validated configuration enables QuickJS for ADF");
        tools = tools.with_adf(
            Arc::new(InMemoryAdfArtifactStore::new()),
            Arc::new(RunScopedJavaScriptPolicy::new(runtime.descriptor())),
            Arc::new(JavaScriptAdfExecutor::new(Arc::clone(runtime), *limits)),
            config.adf().max_active_per_run,
        )?;
    }
    let tools = ObservedTools::new(tools, Arc::clone(&hook));

    let agent = AgentLoop::new(
        provider,
        tools,
        model.model.clone(),
        config.agent().system_prompt.clone(),
        model.max_output_tokens,
        config.agent().max_steps,
    )
    .with_approval_policy(config.approval().policy())
    .with_timeouts(
        Duration::from_secs(config.agent().model_timeout_seconds),
        Duration::from_secs(config.agent().tool_timeout_seconds),
    )
    .with_tool_call_strategy(config.agent().tool_call_strategy)
    .with_approval_port(TerminalApprovals);

    let response = Harness::new(ObservedMachine::new(agent, hook))
        .with_run_timeout(Duration::from_secs(config.agent().run_timeout_seconds))
        .execute_with_options(
            input,
            RunOptions {
                allowed_tools: None,
                allow_run_adf: config.adf().enabled,
                max_steps: None,
            },
        )
        .await;
    let observation_result = observation_worker.shutdown().await;
    let response = response?;
    observation_result?;
    println!("{}", response.output);
    Ok(())
}

fn script_limits(config: &QuickJsConfig) -> ScriptLimits {
    ScriptLimits {
        timeout_ms: config.timeout_ms,
        memory_bytes: config.memory_bytes,
        max_stack_bytes: config.max_stack_bytes,
        max_output_bytes: config.max_output_bytes,
        ..ScriptLimits::default()
    }
}
