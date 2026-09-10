//! Browser-facing WASM boundary for Mina's portable Agent reducers.
//!
//! The crate owns reducer state and checkpoint restoration. Browser I/O stays
//! behind a Worker host so the same `agent-core` decisions remain usable by
//! native server and desktop runtimes.

use agent_core::harness::{
    AGENT_LOOP_CHECKPOINT_SCHEMA_VERSION, AGENT_LOOP_REDUCER_PROTOCOL_VERSION, AgentLoopInput,
    AgentLoopOutcome, AgentLoopReducer, AgentLoopReducerState, AgentLoopStartDispatch,
    AgentLoopTransition, ModelRole, TITLE_REDUCER_PROTOCOL_VERSION, TitleGenerationReducer,
};
#[cfg(target_arch = "wasm32")]
use agent_core::skill::{
    LockedSkill, ResolveSkillsRequest, SkillActivation, SkillId, SkillLock, SkillOrchestrator,
    compile_skills,
};
#[cfg(target_arch = "wasm32")]
use agent_extension::store::WorkspaceSkillStore;
#[cfg(target_arch = "wasm32")]
use agent_extension::workspace::{
    CreateDirRequest, ListRequest, OpfsWorkspaceFs, ReadRequest, RemoveRequest, RenameRequest,
    WorkspaceError, WorkspaceFs, WorkspacePath, WriteMode, WriteRequest,
};
#[cfg(target_arch = "wasm32")]
use serde::Deserialize;
use serde::{Serialize, de::DeserializeOwned};
#[cfg(target_arch = "wasm32")]
use std::sync::Arc;
use wasm_bindgen::prelude::*;

const SYSTEM_PROMPT: &str = include_str!("system-prompt.md");
const SKILL_RESOURCE_HELP: &str = "Resources belonging to an active Skill can be read with read_skill_resource. Pass the exact skill_id and the package-relative resource path; this tool cannot access other Skills or the normal workspace.";

fn browser_system_prompt(instruction: &str) -> String {
    if instruction.trim().is_empty() {
        return SYSTEM_PROMPT.trim().to_owned();
    }
    format!(
        "{}\n\n{}\n\n{}",
        SYSTEM_PROMPT.trim(),
        instruction,
        SKILL_RESOURCE_HELP
    )
}

struct RuntimeCore {
    transition: AgentLoopTransition,
}

impl RuntimeCore {
    fn start(request_json: &str) -> Result<Self, String> {
        let mut request: AgentLoopStartDispatch = decode(request_json)?;
        if !request.start.config.system_prompt.is_empty()
            || request
                .start
                .prior_messages
                .iter()
                .any(|m| m.role == ModelRole::System)
            || request.start.skill_lock.is_some()
            || request.start.context_fingerprint.is_some()
        {
            return Err(
                "browser prompt and skills are owned by WASM; external injection is not supported"
                    .into(),
            );
        }
        request.start.config.system_prompt = browser_system_prompt("");
        let transition = AgentLoopReducer::start_json(&encode(&request)?)
            .map_err(|error| format!("could not start reducer: {error}"))
            .and_then(|response| decode(&response))?;
        Ok(Self { transition })
    }

    fn restore(checkpoint_json: &str) -> Result<Self, String> {
        Self::restore_with_prompt(checkpoint_json, &browser_system_prompt(""))
    }

    fn restore_with_prompt(checkpoint_json: &str, expected: &str) -> Result<Self, String> {
        let state: AgentLoopReducerState = decode(checkpoint_json)?;
        if state.protocol_version != AGENT_LOOP_REDUCER_PROTOCOL_VERSION {
            return Err("unsupported checkpoint protocol".into());
        }
        if state.config.system_prompt != expected
            || state
                .messages
                .first()
                .is_none_or(|m| m.role != ModelRole::System || m.content != expected)
            || state
                .messages
                .iter()
                .skip(1)
                .any(|m| m.role == ModelRole::System)
        {
            return Err(
                "checkpoint prompt does not match the embedded WASM prompt and locked skills"
                    .into(),
            );
        }
        let effects = state.pending_effects();
        let outcome = state.outcome();
        Ok(Self {
            transition: AgentLoopTransition {
                protocol_version: AGENT_LOOP_REDUCER_PROTOCOL_VERSION,
                state,
                events: Vec::new(),
                effects,
                outcome,
            },
        })
    }

    fn dispatch(&mut self, input_json: &str) -> Result<String, String> {
        let input: AgentLoopInput = decode(input_json)?;
        self.transition = AgentLoopReducer::dispatch(self.transition.state.clone(), input);
        encode(&self.transition)
    }

    fn transition_json(&self) -> Result<String, String> {
        encode(&self.transition)
    }

    fn checkpoint_json(&self) -> Result<String, String> {
        encode(&self.transition.state)
    }

    fn is_terminal(&self) -> bool {
        matches!(
            &self.transition.outcome,
            AgentLoopOutcome::Complete { .. }
                | AgentLoopOutcome::Failed { .. }
                | AgentLoopOutcome::Cancelled
        )
    }
}

/// Stateful WASM wrapper around the portable reducer.
#[wasm_bindgen]
pub struct WasmAgent {
    inner: RuntimeCore,
}

#[wasm_bindgen]
impl WasmAgent {
    /// Starts an Agent from a versioned `AgentLoopStartDispatch` JSON value.
    #[wasm_bindgen(constructor)]
    pub fn new(request_json: &str) -> Result<WasmAgent, JsValue> {
        RuntimeCore::start(request_json)
            .map(|inner| Self { inner })
            .map_err(js_error)
    }

    /// Restores an Agent from the reducer-state JSON returned by
    /// `checkpointJson`. Pending effects are exposed again for replay.
    #[wasm_bindgen(js_name = restore)]
    pub fn restore(checkpoint_json: &str) -> Result<WasmAgent, JsValue> {
        RuntimeCore::restore(checkpoint_json)
            .map(|inner| Self { inner })
            .map_err(js_error)
    }

    /// Applies one normalized host input and returns the atomic transition.
    pub fn dispatch(&mut self, input_json: &str) -> Result<String, JsValue> {
        self.inner.dispatch(input_json).map_err(js_error)
    }

    /// Returns the most recent atomic reducer transition.
    #[wasm_bindgen(js_name = transitionJson)]
    pub fn transition_json(&self) -> Result<String, JsValue> {
        self.inner.transition_json().map_err(js_error)
    }

    /// Returns the portable reducer state suitable for OPFS persistence.
    #[wasm_bindgen(js_name = checkpointJson)]
    pub fn checkpoint_json(&self) -> Result<String, JsValue> {
        self.inner.checkpoint_json().map_err(js_error)
    }

    /// Returns the exact SkillLock retained by the reducer checkpoint.
    #[wasm_bindgen(js_name = skillLockJson)]
    pub fn skill_lock_json(&self) -> Result<String, JsValue> {
        encode(&self.inner.transition.state.skill_lock).map_err(js_error)
    }

    #[wasm_bindgen(js_name = isTerminal)]
    pub fn is_terminal(&self) -> bool {
        self.inner.is_terminal()
    }
}

#[wasm_bindgen(js_name = reducerProtocolVersion)]
pub fn reducer_protocol_version() -> u32 {
    AGENT_LOOP_REDUCER_PROTOCOL_VERSION
}

#[wasm_bindgen(js_name = checkpointSchemaVersion)]
pub fn checkpoint_schema_version() -> u32 {
    AGENT_LOOP_CHECKPOINT_SCHEMA_VERSION
}

#[wasm_bindgen(js_name = titleProtocolVersion)]
pub fn title_protocol_version() -> u32 {
    TITLE_REDUCER_PROTOCOL_VERSION
}

#[wasm_bindgen(js_name = skillCompilerVersion)]
pub fn skill_compiler_version() -> u32 {
    agent_core::skill::SKILL_COMPILER_VERSION
}

#[wasm_bindgen(js_name = titleStartJson)]
pub fn title_start_json(request_json: &str) -> Result<String, JsValue> {
    TitleGenerationReducer::start_json(request_json).map_err(js_error)
}

#[wasm_bindgen(js_name = titleDispatchJson)]
pub fn title_dispatch_json(request_json: &str) -> Result<String, JsValue> {
    TitleGenerationReducer::dispatch_json(request_json).map_err(js_error)
}

#[cfg(target_arch = "wasm32")]
#[derive(Deserialize)]
struct BrowserSkillChoice {
    skill_id: String,
    version: String,
}

#[cfg(target_arch = "wasm32")]
#[derive(Deserialize)]
struct BrowserSkillResolveRequest {
    current_input: String,
    #[serde(default)]
    explicit: Vec<BrowserSkillChoice>,
    #[serde(default)]
    profile_defaults: Vec<BrowserSkillChoice>,
    #[serde(default = "default_max_skills")]
    max_skills: usize,
    #[serde(default = "default_true")]
    include_store_defaults: bool,
}

#[cfg(target_arch = "wasm32")]
#[derive(Serialize, Deserialize)]
struct BrowserSkillResolveResult {
    resolved_skills: Vec<agent_core::skill::ResolvedSkill>,
    compiled: agent_core::skill::CompiledSkills,
}

#[cfg(target_arch = "wasm32")]
#[derive(Deserialize)]
struct BrowserSkillResourceRequest {
    locked: LockedSkill,
    path: String,
    #[serde(default = "default_skill_resource_bytes")]
    max_bytes: usize,
}

#[cfg(target_arch = "wasm32")]
#[derive(Serialize)]
struct BrowserSkillResourceResult {
    skill_id: String,
    path: String,
    media_type: Option<String>,
    content: String,
    digest: String,
}

/// Starts with the compiled-in host prompt and Rust-resolved OPFS Skills.
/// No instruction text is accepted from the caller.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(js_name = startBrowserAgent)]
pub async fn start_browser_agent(
    prefix: String,
    request_json: String,
) -> Result<WasmAgent, JsValue> {
    let request: AgentLoopStartDispatch = decode(&request_json).map_err(js_error)?;
    let mut inner = RuntimeCore::start(&request_json).map_err(js_error)?;
    let skills: BrowserSkillResolveResult = decode(
        &skills_resolve_json(
            prefix,
            serde_json::json!({"current_input": request.start.input}).to_string(),
        )
        .await?,
    )
    .map_err(js_error)?;
    if !skills.resolved_skills.is_empty() {
        let prompt = browser_system_prompt(&skills.compiled.instruction);
        let state = &mut inner.transition.state;
        state.config.system_prompt = prompt.clone();
        state.messages[0].content = prompt;
        state.context_fingerprint = Some(
            skills
                .compiled
                .skill_lock
                .compiled_instruction_digest
                .clone(),
        );
        state.skill_lock = Some(skills.compiled.skill_lock);
    }
    Ok(WasmAgent { inner })
}

/// Rebuild the expected prompt from digest-locked packages before restoring.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(js_name = restoreBrowserAgent)]
pub async fn restore_browser_agent(
    prefix: String,
    checkpoint_json: String,
) -> Result<WasmAgent, JsValue> {
    let state: AgentLoopReducerState = decode(&checkpoint_json).map_err(js_error)?;
    let mut prompt = browser_system_prompt("");
    if let Some(lock) = &state.skill_lock {
        let orchestrator = open_skill_orchestrator(&prefix)?;
        let mut resolved = Vec::new();
        for locked in &lock.skills {
            if locked.store_identity != format!("skills:opfs:{prefix}") {
                return Err(js_error("checkpoint Skill belongs to another namespace"));
            }
            let package = orchestrator
                .load_locked(locked)
                .await
                .map_err(skill_error)?;
            let activation_reason = match package.manifest.activation {
                SkillActivation::ProfileDefault => "profile_default",
                SkillActivation::Routable { .. } => "rule_router",
                SkillActivation::ExplicitOnly => {
                    return Err(js_error("unsupported locked Skill activation"));
                }
            };
            resolved.push(agent_core::skill::ResolvedSkill {
                locked: locked.clone(),
                instruction: package.instructions,
                requested_tools: package.manifest.required_tools,
                activation_reason: activation_reason.into(),
            });
        }
        let compiled = compile_skills(&resolved);
        if &compiled.skill_lock != lock {
            return Err(js_error(
                "checkpoint Skill compilation does not match locked packages",
            ));
        }
        prompt = browser_system_prompt(&compiled.instruction);
    }
    RuntimeCore::restore_with_prompt(&checkpoint_json, &prompt)
        .map(|inner| WasmAgent { inner })
        .map_err(js_error)
}

/// Lists validated Skill packages below one OPFS prefix.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(js_name = skillsListJson)]
pub async fn skills_list_json(prefix: String) -> Result<String, JsValue> {
    let orchestrator = open_skill_orchestrator(&prefix)?;
    let packages = orchestrator.list().await.map_err(skill_error)?;
    encode(&packages).map_err(js_error)
}

/// Resolves explicit/default/routable Skills through agent-core and returns a
/// deterministic compiled instruction plus a checkpoint-safe SkillLock.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(js_name = skillsResolveJson)]
pub async fn skills_resolve_json(prefix: String, request_json: String) -> Result<String, JsValue> {
    let request: BrowserSkillResolveRequest = decode(&request_json).map_err(js_error)?;
    let orchestrator = open_skill_orchestrator(&prefix)?;
    let mut profile_defaults = request
        .profile_defaults
        .into_iter()
        .map(|choice| (SkillId(choice.skill_id), choice.version))
        .collect::<Vec<_>>();
    if request.include_store_defaults {
        for descriptor in orchestrator.list().await.map_err(skill_error)? {
            if descriptor.activation == SkillActivation::ProfileDefault {
                profile_defaults.push((descriptor.skill_id, descriptor.version));
            }
        }
    }
    let resolved_skills = orchestrator
        .resolve(ResolveSkillsRequest {
            explicit: request
                .explicit
                .into_iter()
                .map(|choice| (SkillId(choice.skill_id), choice.version))
                .collect(),
            profile_defaults,
            current_input: request.current_input,
            max_skills: request.max_skills.clamp(1, 16),
        })
        .await
        .map_err(skill_error)?;
    let compiled = compile_skills(&resolved_skills);
    encode(&BrowserSkillResolveResult {
        resolved_skills,
        compiled,
    })
    .map_err(js_error)
}

/// Verifies that every package retained by a checkpoint SkillLock is still
/// available with the exact same digest.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(js_name = skillsVerifyLockJson)]
pub async fn skills_verify_lock_json(prefix: String, lock_json: String) -> Result<String, JsValue> {
    let lock: SkillLock = decode(&lock_json).map_err(js_error)?;
    let orchestrator = open_skill_orchestrator(&prefix)?;
    for locked in &lock.skills {
        orchestrator
            .load_locked(locked)
            .await
            .map_err(skill_error)?;
    }
    encode(&lock).map_err(js_error)
}

/// Reads one UTF-8 resource from an exact digest-locked Skill package.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(js_name = skillReadResourceTextJson)]
pub async fn skill_read_resource_text_json(
    prefix: String,
    request_json: String,
) -> Result<String, JsValue> {
    let request: BrowserSkillResourceRequest = decode(&request_json).map_err(js_error)?;
    let normalized = WorkspacePath::parse(&request.path).map_err(workspace_error)?;
    if normalized.is_root() || matches!(normalized.storage_key(), "skill.toml" | "SKILL.md") {
        return Err(skill_error_message(
            "skill_resource_invalid_path",
            "Skill resource path is invalid",
        ));
    }
    let orchestrator = open_skill_orchestrator(&prefix)?;
    let package = orchestrator
        .load_locked(&request.locked)
        .await
        .map_err(skill_error)?;
    let path = normalized.storage_key().to_owned();
    let bytes = package.resources.get(&path).ok_or_else(|| {
        skill_error_message("skill_resource_not_found", "Skill resource was not found")
    })?;
    let max_bytes = request.max_bytes.clamp(1, default_skill_resource_bytes());
    if bytes.len() > max_bytes {
        return Err(skill_error_message(
            "skill_resource_too_large",
            "Skill resource exceeds the configured read limit",
        ));
    }
    let content = String::from_utf8(bytes.clone()).map_err(|_| {
        skill_error_message(
            "skill_resource_not_utf8",
            "Skill resource is not valid UTF-8 text",
        )
    })?;
    let media_type = package
        .manifest
        .resources
        .iter()
        .find(|resource| resource.path == path)
        .and_then(|resource| resource.media_type.clone());
    encode(&BrowserSkillResourceResult {
        skill_id: package.manifest.skill_id.0,
        path,
        media_type,
        content,
        digest: package.digest,
    })
    .map_err(js_error)
}

#[cfg(target_arch = "wasm32")]
fn open_skill_orchestrator(prefix: &str) -> Result<SkillOrchestrator, JsValue> {
    let workspace: Arc<dyn WorkspaceFs> = Arc::new(open_workspace(prefix)?);
    Ok(SkillOrchestrator::new(Arc::new(WorkspaceSkillStore::new(
        workspace,
        format!("skills:opfs:{prefix}"),
    ))))
}

#[cfg(target_arch = "wasm32")]
const fn default_max_skills() -> usize {
    4
}

#[cfg(target_arch = "wasm32")]
const fn default_skill_resource_bytes() -> usize {
    1024 * 1024
}

#[cfg(target_arch = "wasm32")]
const fn default_true() -> bool {
    true
}

#[cfg(target_arch = "wasm32")]
fn skill_error(error: impl std::fmt::Display) -> JsValue {
    skill_error_message("skill_store_failed", &error.to_string())
}

#[cfg(target_arch = "wasm32")]
fn skill_error_message(code: &str, message: &str) -> JsValue {
    JsValue::from_str(
        &serde_json::json!({
            "code": code,
            "message": message,
            "retryable": false,
        })
        .to_string(),
    )
}

/// Returns the descriptor for one OPFS-backed workspace prefix.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(js_name = workspaceDescriptorJson)]
pub fn workspace_descriptor_json(prefix: &str) -> Result<String, JsValue> {
    let workspace = open_workspace(prefix)?;
    encode(&workspace.descriptor()).map_err(js_error)
}

/// Returns the capabilities exposed by the shared WorkspaceFs contract.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(js_name = workspaceCapabilitiesJson)]
pub fn workspace_capabilities_json(prefix: &str) -> Result<String, JsValue> {
    let workspace = open_workspace(prefix)?;
    encode(&workspace.capabilities()).map_err(js_error)
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(js_name = workspaceStatJson)]
pub async fn workspace_stat_json(prefix: String, path: String) -> Result<String, JsValue> {
    let workspace = open_workspace(&prefix)?;
    let metadata = workspace
        .stat(parse_workspace_path(&path)?)
        .await
        .map_err(workspace_error)?;
    encode(&metadata).map_err(js_error)
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(js_name = workspaceReadBytes)]
pub async fn workspace_read_bytes(
    prefix: String,
    path: String,
    offset: u32,
    length: i32,
    max_bytes: u32,
) -> Result<Vec<u8>, JsValue> {
    let workspace = open_workspace(&prefix)?;
    let content = workspace
        .read(ReadRequest {
            path: parse_workspace_path(&path)?,
            offset: u64::from(offset),
            length: (length >= 0).then_some(length as u64),
            max_bytes: u64::from(max_bytes),
        })
        .await
        .map_err(workspace_error)?;
    Ok(content.bytes)
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(js_name = workspaceWriteJson)]
pub async fn workspace_write_json(
    prefix: String,
    path: String,
    bytes: Vec<u8>,
    mode: String,
    create_parents: bool,
) -> Result<String, JsValue> {
    let workspace = open_workspace(&prefix)?;
    let mode = match mode.as_str() {
        "create_new" => WriteMode::CreateNew,
        "truncate" => WriteMode::Truncate,
        "append" => WriteMode::Append,
        _ => return Err(js_error("workspace write mode is invalid")),
    };
    let metadata = workspace
        .write(WriteRequest {
            path: parse_workspace_path(&path)?,
            bytes,
            mode,
            create_parents,
        })
        .await
        .map_err(workspace_error)?;
    encode(&metadata).map_err(js_error)
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(js_name = workspaceListJson)]
pub async fn workspace_list_json(
    prefix: String,
    path: String,
    cursor: Option<String>,
    limit: u32,
) -> Result<String, JsValue> {
    let workspace = open_workspace(&prefix)?;
    let page = workspace
        .list(ListRequest {
            path: parse_workspace_path(&path)?,
            cursor,
            limit: usize::try_from(limit).unwrap_or(usize::MAX),
        })
        .await
        .map_err(workspace_error)?;
    encode(&serde_json::json!({
        "entries": page.entries,
        "next_cursor": page.next_cursor,
    }))
    .map_err(js_error)
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(js_name = workspaceCreateDirectoryJson)]
pub async fn workspace_create_directory_json(
    prefix: String,
    path: String,
    recursive: bool,
) -> Result<String, JsValue> {
    let workspace = open_workspace(&prefix)?;
    let metadata = workspace
        .create_dir(CreateDirRequest {
            path: parse_workspace_path(&path)?,
            recursive,
        })
        .await
        .map_err(workspace_error)?;
    encode(&metadata).map_err(js_error)
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(js_name = workspaceRemove)]
pub async fn workspace_remove(
    prefix: String,
    path: String,
    recursive: bool,
) -> Result<(), JsValue> {
    let workspace = open_workspace(&prefix)?;
    workspace
        .remove(RemoveRequest {
            path: parse_workspace_path(&path)?,
            recursive,
        })
        .await
        .map_err(workspace_error)
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(js_name = workspaceRenameJson)]
pub async fn workspace_rename_json(
    prefix: String,
    from: String,
    to: String,
    overwrite: bool,
) -> Result<String, JsValue> {
    let workspace = open_workspace(&prefix)?;
    let metadata = workspace
        .rename(RenameRequest {
            from: parse_workspace_path(&from)?,
            to: parse_workspace_path(&to)?,
            overwrite,
        })
        .await
        .map_err(workspace_error)?;
    encode(&metadata).map_err(js_error)
}

#[cfg(target_arch = "wasm32")]
fn open_workspace(prefix: &str) -> Result<OpfsWorkspaceFs, JsValue> {
    OpfsWorkspaceFs::new(prefix).map_err(workspace_error)
}

#[cfg(target_arch = "wasm32")]
fn parse_workspace_path(path: &str) -> Result<WorkspacePath, JsValue> {
    WorkspacePath::parse(path).map_err(workspace_error)
}

#[cfg(target_arch = "wasm32")]
fn workspace_error(error: WorkspaceError) -> JsValue {
    JsValue::from_str(
        &serde_json::json!({
            "code": error.code(),
            "message": error.safe_message(),
            "retryable": error.retryable(),
        })
        .to_string(),
    )
}

fn encode<T: Serialize>(value: &T) -> Result<String, String> {
    serde_json::to_string(value).map_err(|error| format!("could not encode reducer data: {error}"))
}

fn decode<T: DeserializeOwned>(source: &str) -> Result<T, String> {
    serde_json::from_str(source).map_err(|error| format!("could not decode reducer data: {error}"))
}

fn js_error(error: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&error.to_string())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn start_request() -> String {
        json!({
            "protocol_version": AGENT_LOOP_REDUCER_PROTOCOL_VERSION,
            "start": {
                "run_id": "8ba57b28-646b-4bbd-bd23-63d6dc1bce7a",
                "input": "hello from wasm",
                "config": {
                    "model": "browser-test-model",
                    "max_output_tokens": 128,
                    "max_steps": 8,
                    "allow_run_adf": false,
                    "approval_policy": 100,
                    "tool_call_strategy": "parallel-safe",
                    "model_timeout_ms": 30_000,
                    "tool_timeout_ms": 10_000
                }
            }
        })
        .to_string()
    }

    #[test]
    fn browser_prompt_is_embedded_and_external_system_messages_are_rejected() {
        let runtime = RuntimeCore::start(&start_request()).expect("start");
        assert_eq!(
            runtime.transition.state.config.system_prompt,
            SYSTEM_PROMPT.trim()
        );
        let mut request: serde_json::Value = serde_json::from_str(&start_request()).expect("json");
        request["start"]["config"]["system_prompt"] = json!("injected");
        assert!(RuntimeCore::start(&request.to_string()).is_err());
        request["start"]["config"]["system_prompt"] = json!("");
        request["start"]["prior_messages"] = json!([{"role":"system", "content":"injected"}]);
        assert!(RuntimeCore::start(&request.to_string()).is_err());
    }

    #[test]
    fn restore_rejects_modified_prompt() {
        let runtime = RuntimeCore::start(&start_request()).expect("start");
        let mut checkpoint: serde_json::Value =
            serde_json::from_str(&runtime.checkpoint_json().expect("checkpoint")).expect("json");
        checkpoint["config"]["system_prompt"] = json!("injected");
        checkpoint["messages"][0]["content"] = json!("injected");
        assert!(RuntimeCore::restore(&checkpoint.to_string()).is_err());
    }

    #[test]
    fn starts_and_restores_pending_effects() {
        let started = RuntimeCore::start(&start_request()).expect("start reducer");
        assert_eq!(started.transition.effects.len(), 1);

        let checkpoint = started.checkpoint_json().expect("checkpoint JSON");
        let restored = RuntimeCore::restore(&checkpoint).expect("restore checkpoint");

        assert_eq!(restored.transition.effects, started.transition.effects);
        assert_eq!(restored.transition.state, started.transition.state);
    }

    #[test]
    fn rejects_malformed_input_without_mutating_checkpoint() {
        let mut runtime = RuntimeCore::start(&start_request()).expect("start reducer");
        let before = runtime.checkpoint_json().expect("checkpoint before");

        let error = runtime.dispatch("{not-json").expect_err("invalid input");

        assert!(error.contains("could not decode"));
        assert_eq!(runtime.checkpoint_json().expect("checkpoint after"), before);
    }
}
