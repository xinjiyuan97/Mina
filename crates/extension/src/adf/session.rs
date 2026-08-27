use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex, MutexGuard},
    time::{SystemTime, UNIX_EPOCH},
};

use agent_core::{
    adf::{
        AdfArtifact, AdfArtifactDescriptor, AdfArtifactStore, AdfDefinitionRequest, AdfError,
        AdfErrorKind, AdfExecutionRequest, AdfExecutor, AdfId, AdfOwner, AdfPolicy,
        AdfPolicyRequest, AdfStoreError,
    },
    harness::{
        RunId, ToolArgumentVisibility, ToolBinding, ToolBindingKind, ToolCallFuture,
        ToolCallRequest, ToolDefinition, ToolError, ToolErrorCategory, ToolOutput, ToolPort,
        ToolRiskLevel, ToolSetSnapshot,
    },
};
use agent_harness::adf::{LockAdfDefinition, lock_definition};
use jsonschema::JSONSchema;
use serde::Deserialize;
use serde_json::{Value, json};
use thiserror::Error;
use tokio::sync::Mutex as AsyncMutex;

const ADF_DEFINE: &str = "adf_define";
const ADF_LIST: &str = "adf_list";
const ADF_REMOVE: &str = "adf_remove";

pub struct RunAdfToolSession<B> {
    base: Arc<B>,
    components: Option<AdfComponents>,
    runs: Arc<Mutex<HashMap<RunId, RunState>>>,
    mutation_locks: Arc<Mutex<HashMap<RunId, Arc<AsyncMutex<()>>>>>,
}

#[derive(Clone)]
struct AdfComponents {
    store: Arc<dyn AdfArtifactStore>,
    policy: Arc<dyn AdfPolicy>,
    executor: Arc<dyn AdfExecutor>,
    max_active_per_run: usize,
}

#[derive(Debug, Clone)]
struct RunState {
    current_revision: u64,
    revisions: BTreeMap<u64, RunRevision>,
}

#[derive(Debug, Clone)]
struct RunRevision {
    base: ToolSetSnapshot,
    adfs: BTreeMap<String, AdfArtifact>,
    idempotency: BTreeMap<String, agent_core::adf::AdfArtifactRef>,
}

impl<B> std::fmt::Debug for RunAdfToolSession<B>
where
    B: ToolPort,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RunAdfToolSession")
            .field("base_tools", &self.base.definitions())
            .field("adf_enabled", &self.components.is_some())
            .field(
                "active_runs",
                &self
                    .runs
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .len(),
            )
            .finish()
    }
}

impl<B> RunAdfToolSession<B>
where
    B: ToolPort,
{
    #[must_use]
    pub fn new(base: B) -> Self {
        Self {
            base: Arc::new(base),
            components: None,
            runs: Arc::new(Mutex::new(HashMap::new())),
            mutation_locks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn with_adf(
        mut self,
        store: Arc<dyn AdfArtifactStore>,
        policy: Arc<dyn AdfPolicy>,
        executor: Arc<dyn AdfExecutor>,
        max_active_per_run: usize,
    ) -> Result<Self, AdfSessionBuildError> {
        if max_active_per_run == 0 {
            return Err(AdfSessionBuildError::InvalidLimit);
        }
        let capabilities = store.capabilities();
        if !capabilities.readable || !capabilities.writable || !capabilities.deactivatable {
            return Err(AdfSessionBuildError::StoreCapabilities);
        }
        for reserved in [ADF_DEFINE, ADF_LIST, ADF_REMOVE] {
            if self
                .base
                .definitions()
                .iter()
                .any(|definition| definition.name == reserved)
            {
                return Err(AdfSessionBuildError::ReservedName(reserved.into()));
            }
        }
        self.components = Some(AdfComponents {
            store,
            policy,
            executor,
            max_active_per_run,
        });
        Ok(self)
    }

    fn lock_runs(&self) -> MutexGuard<'_, HashMap<RunId, RunState>> {
        self.runs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn mutation_lock(&self, run_id: RunId) -> Arc<AsyncMutex<()>> {
        Arc::clone(
            self.mutation_locks
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .entry(run_id)
                .or_insert_with(|| Arc::new(AsyncMutex::new(()))),
        )
    }

    fn management_definitions(&self) -> Vec<ToolDefinition> {
        self.components
            .as_ref()
            .map_or_else(Vec::new, |_| management_definitions())
    }

    fn resolve_revision(&self, run_id: RunId, revision: u64) -> Result<RunRevision, ToolError> {
        self.lock_runs()
            .get(&run_id)
            .and_then(|state| state.revisions.get(&revision))
            .cloned()
            .ok_or_else(revision_mismatch)
    }
}

impl<B> ToolPort for RunAdfToolSession<B>
where
    B: ToolPort,
{
    fn definitions(&self) -> Vec<ToolDefinition> {
        let mut definitions = self.base.definitions();
        definitions.extend(self.management_definitions());
        definitions.sort_by(|left, right| left.name.cmp(&right.name));
        definitions
    }

    fn tool_set_snapshot(&self, run_id: RunId) -> Result<ToolSetSnapshot, ToolError> {
        let base = self.base.tool_set_snapshot(run_id)?;
        if self.components.is_none() {
            return Ok(base);
        }

        let revision = {
            let mut runs = self.lock_runs();
            let state = runs.entry(run_id).or_insert_with(|| {
                let revision = RunRevision {
                    base: base.clone(),
                    adfs: BTreeMap::new(),
                    idempotency: BTreeMap::new(),
                };
                RunState {
                    current_revision: 1,
                    revisions: BTreeMap::from([(1, revision)]),
                }
            });
            let current = state
                .revisions
                .get(&state.current_revision)
                .expect("current ADF revision must exist");
            if current.base.digest != base.digest || current.base.revision != base.revision {
                let next_revision = state.current_revision.saturating_add(1);
                let next = RunRevision {
                    base,
                    adfs: current.adfs.clone(),
                    idempotency: current.idempotency.clone(),
                };
                state.revisions.insert(next_revision, next);
                state.current_revision = next_revision;
            }
            state
                .revisions
                .get(&state.current_revision)
                .cloned()
                .map(|revision| (state.current_revision, revision))
                .expect("current ADF revision must exist")
        };
        compose_snapshot(revision.0, &revision.1)
    }

    fn argument_visibility(&self, name: &str) -> ToolArgumentVisibility {
        if name == ADF_DEFINE {
            ToolArgumentVisibility::DigestOnly
        } else {
            self.base.argument_visibility(name)
        }
    }

    fn validate(&self, name: &str, arguments: &Value) -> Result<(), ToolError> {
        if is_management_tool(name) {
            validate_management(name, arguments)
        } else {
            self.base.validate(name, arguments)
        }
    }

    fn validate_at(
        &self,
        run_id: RunId,
        tool_set_revision: u64,
        name: &str,
        arguments: &Value,
    ) -> Result<(), ToolError> {
        if self.components.is_none() {
            return self
                .base
                .validate_at(run_id, tool_set_revision, name, arguments);
        }
        let revision = self.resolve_revision(run_id, tool_set_revision)?;
        if is_management_tool(name) {
            return validate_management(name, arguments);
        }
        if let Some(artifact) = revision.adfs.get(name) {
            return validate_schema(&artifact.definition.input_schema, arguments);
        }
        if revision.base.binding(name).is_none() {
            return Err(ToolError::new(
                "tool_not_found",
                "the requested tool is not present in this tool-set revision",
                false,
            ));
        }
        self.base
            .validate_at(run_id, revision.base.revision, name, arguments)
    }

    fn call(&self, request: ToolCallRequest) -> ToolCallFuture {
        match self.tool_set_snapshot(request.run_id) {
            Ok(snapshot) => self.call_at(snapshot.revision, request),
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }

    fn call_at(&self, tool_set_revision: u64, request: ToolCallRequest) -> ToolCallFuture {
        let Some(components) = self.components.clone() else {
            return self.base.call_at(tool_set_revision, request);
        };
        let revision = match self.resolve_revision(request.run_id, tool_set_revision) {
            Ok(revision) => revision,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        if let Some(artifact) = revision.adfs.get(&request.name).cloned() {
            return Box::pin(async move {
                let output = components
                    .executor
                    .execute(AdfExecutionRequest {
                        run_id: request.run_id,
                        call_id: request.call_id,
                        artifact,
                        arguments: request.arguments,
                        cancellation: request.cancellation,
                    })
                    .await
                    .map_err(map_adf_error)?;
                Ok(ToolOutput::text(
                    output.content.unwrap_or_else(|| output.value.to_string()),
                ))
            });
        }
        if is_management_tool(&request.name) {
            let runs = Arc::clone(&self.runs);
            let mutation_lock = self.mutation_lock(request.run_id);
            return Box::pin(async move {
                match request.name.as_str() {
                    ADF_DEFINE => {
                        define_adf(components, runs, mutation_lock, tool_set_revision, request)
                            .await
                    }
                    ADF_LIST => list_adfs(runs, tool_set_revision, request),
                    ADF_REMOVE => {
                        remove_adf(components, runs, mutation_lock, tool_set_revision, request)
                            .await
                    }
                    _ => Err(tool_not_found()),
                }
            });
        }
        if revision.base.binding(&request.name).is_none() {
            return Box::pin(async { Err(tool_not_found()) });
        }
        self.base.call_at(revision.base.revision, request)
    }

    fn close_run(&self, run_id: RunId) {
        self.base.close_run(run_id);
        self.lock_runs().remove(&run_id);
        self.mutation_locks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&run_id);
    }
}

async fn define_adf(
    components: AdfComponents,
    runs: Arc<Mutex<HashMap<RunId, RunState>>>,
    mutation_lock: Arc<AsyncMutex<()>>,
    tool_set_revision: u64,
    request: ToolCallRequest,
) -> Result<ToolOutput, ToolError> {
    let definition: AdfDefinitionRequest = serde_json::from_value(request.arguments)
        .map_err(|_| invalid_arguments("adf_define arguments are invalid"))?;
    let _mutation = mutation_lock.lock_owned().await;
    let current = current_revision(&runs, request.run_id, tool_set_revision)?;
    let idempotency_key = definition.idempotency_key.clone();
    let known_idempotency = current.idempotency.contains_key(&idempotency_key);
    if current.adfs.len() >= components.max_active_per_run && !known_idempotency {
        return Err(ToolError::new(
            "adf_active_limit_exceeded",
            "the run has reached its active ADF limit",
            false,
        )
        .with_category(ToolErrorCategory::ResourceExhausted));
    }
    let decision = components
        .policy
        .evaluate(&AdfPolicyRequest {
            run_id: request.run_id,
            definition: definition.clone(),
        })
        .map_err(map_adf_error)?;
    let put = lock_definition(LockAdfDefinition {
        request: definition,
        decision,
        owner: AdfOwner::Run {
            run_id: request.run_id,
        },
        adf_id: AdfId::new(),
        revision: 1,
        store_identity: components.store.descriptor().identity,
        created_by_run_id: request.run_id,
        created_at_ms: unix_time_ms(),
    })
    .map_err(map_adf_error)?;
    if current
        .base
        .binding(&put.artifact.definition.canonical_name)
        .is_some()
        || is_management_tool(&put.artifact.definition.canonical_name)
    {
        return Err(ToolError::new(
            "adf_name_conflict",
            "the generated ADF name conflicts with a host tool",
            false,
        )
        .with_category(ToolErrorCategory::Conflict));
    }
    components
        .executor
        .validate(put.artifact.clone())
        .await
        .map_err(map_adf_error)?;
    let reference = components.store.put(put).await.map_err(map_store_error)?;
    let artifact = components
        .store
        .get(reference)
        .await
        .map_err(map_store_error)?
        .ok_or_else(|| {
            ToolError::new(
                "adf_artifact_not_found",
                "the stored ADF artifact could not be loaded",
                false,
            )
            .with_category(ToolErrorCategory::Internal)
        })?;
    if !artifact.active {
        return Err(ToolError::new(
            "adf_artifact_inactive",
            "the idempotent ADF artifact is inactive",
            false,
        )
        .with_category(ToolErrorCategory::Conflict));
    }

    let snapshot = mutate_revision(&runs, request.run_id, tool_set_revision, |next| {
        if let Some(existing) = next.adfs.get(&artifact.definition.canonical_name) {
            if existing.definition.source_ref == artifact.definition.source_ref {
                next.idempotency.insert(
                    idempotency_key.clone(),
                    artifact.definition.source_ref.clone(),
                );
                return Ok(false);
            }
            return Err(ToolError::new(
                "adf_name_conflict",
                "an active ADF already uses the generated name",
                false,
            )
            .with_category(ToolErrorCategory::Conflict));
        }
        next.adfs
            .insert(artifact.definition.canonical_name.clone(), artifact.clone());
        next.idempotency
            .insert(idempotency_key, artifact.definition.source_ref.clone());
        Ok(true)
    })?;
    Ok(ToolOutput::text(
        json!({
            "ok": true,
            "adf_id": artifact.definition.adf_id,
            "revision": artifact.definition.revision,
            "canonical_name": artifact.definition.canonical_name,
            "source_digest": artifact.definition.source_digest,
            "manifest_digest": artifact.definition.manifest_digest,
            "effective_risk": artifact.definition.effective_risk,
            "granted_capabilities": artifact.definition.granted_capabilities,
            "scope": "run",
            "tool_set_revision": snapshot.revision,
            "tool_set_digest": snapshot.digest,
            "instruction": "The ADF is available starting with the next model step."
        })
        .to_string(),
    ))
}

fn list_adfs(
    runs: Arc<Mutex<HashMap<RunId, RunState>>>,
    tool_set_revision: u64,
    request: ToolCallRequest,
) -> Result<ToolOutput, ToolError> {
    let _: EmptyArguments = serde_json::from_value(request.arguments)
        .map_err(|_| invalid_arguments("adf_list arguments are invalid"))?;
    let revision = revision_at(&runs, request.run_id, tool_set_revision)?;
    let snapshot = compose_snapshot(tool_set_revision, &revision)?;
    let functions = revision
        .adfs
        .values()
        .map(|artifact| descriptor(artifact, true))
        .collect::<Vec<_>>();
    Ok(ToolOutput::text(
        json!({
            "ok": true,
            "tool_set_revision": snapshot.revision,
            "tool_set_digest": snapshot.digest,
            "functions": functions,
        })
        .to_string(),
    ))
}

async fn remove_adf(
    components: AdfComponents,
    runs: Arc<Mutex<HashMap<RunId, RunState>>>,
    mutation_lock: Arc<AsyncMutex<()>>,
    tool_set_revision: u64,
    request: ToolCallRequest,
) -> Result<ToolOutput, ToolError> {
    let arguments: RemoveArguments = serde_json::from_value(request.arguments)
        .map_err(|_| invalid_arguments("adf_remove arguments are invalid"))?;
    let _mutation = mutation_lock.lock_owned().await;
    let current = current_revision(&runs, request.run_id, tool_set_revision)?;
    let artifact = current.adfs.get(&arguments.canonical_name).ok_or_else(|| {
        ToolError::new(
            "adf_not_found",
            "the requested active ADF was not found",
            false,
        )
        .with_category(ToolErrorCategory::NotFound)
    })?;
    components
        .store
        .deactivate(artifact.definition.source_ref.clone())
        .await
        .map_err(map_store_error)?;
    let removed_name = arguments.canonical_name;
    let snapshot = mutate_revision(&runs, request.run_id, tool_set_revision, |next| {
        next.adfs.remove(&removed_name);
        Ok(true)
    })?;
    Ok(ToolOutput::text(
        json!({
            "ok": true,
            "removed": removed_name,
            "tool_set_revision": snapshot.revision,
            "tool_set_digest": snapshot.digest,
            "instruction": "The ADF is unavailable starting with the next model step."
        })
        .to_string(),
    ))
}

fn mutate_revision(
    runs: &Arc<Mutex<HashMap<RunId, RunState>>>,
    run_id: RunId,
    expected_revision: u64,
    mutation: impl FnOnce(&mut RunRevision) -> Result<bool, ToolError>,
) -> Result<ToolSetSnapshot, ToolError> {
    let mut runs = runs.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    let state = runs.get_mut(&run_id).ok_or_else(revision_mismatch)?;
    if state.current_revision != expected_revision {
        return Err(revision_mismatch());
    }
    let current = state
        .revisions
        .get(&expected_revision)
        .cloned()
        .ok_or_else(revision_mismatch)?;
    let mut next = current;
    let changed = mutation(&mut next)?;
    if !changed {
        return compose_snapshot(expected_revision, &next);
    }
    let next_revision = expected_revision.saturating_add(1);
    state.revisions.insert(next_revision, next.clone());
    state.current_revision = next_revision;
    compose_snapshot(next_revision, &next)
}

fn current_revision(
    runs: &Arc<Mutex<HashMap<RunId, RunState>>>,
    run_id: RunId,
    expected_revision: u64,
) -> Result<RunRevision, ToolError> {
    let runs = runs.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    let state = runs.get(&run_id).ok_or_else(revision_mismatch)?;
    if state.current_revision != expected_revision {
        return Err(revision_mismatch());
    }
    state
        .revisions
        .get(&expected_revision)
        .cloned()
        .ok_or_else(revision_mismatch)
}

fn revision_at(
    runs: &Arc<Mutex<HashMap<RunId, RunState>>>,
    run_id: RunId,
    revision: u64,
) -> Result<RunRevision, ToolError> {
    runs.lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(&run_id)
        .and_then(|state| state.revisions.get(&revision))
        .cloned()
        .ok_or_else(revision_mismatch)
}

fn compose_snapshot(revision: u64, run: &RunRevision) -> Result<ToolSetSnapshot, ToolError> {
    let mut definitions = run.base.definitions.clone();
    let mut bindings = run.base.bindings.clone();
    for definition in management_definitions() {
        let visibility = if definition.name == ADF_DEFINE {
            ToolArgumentVisibility::DigestOnly
        } else {
            ToolArgumentVisibility::Full
        };
        bindings.push(ToolBinding {
            name: definition.name.clone(),
            kind: ToolBindingKind::AdfManagement,
            argument_visibility: visibility,
        });
        definitions.push(definition);
    }
    for artifact in run.adfs.values() {
        definitions.push(artifact.definition.tool_definition());
        bindings.push(ToolBinding {
            name: artifact.definition.canonical_name.clone(),
            kind: ToolBindingKind::AgentDefined,
            argument_visibility: ToolArgumentVisibility::Full,
        });
    }
    ToolSetSnapshot::new(revision, definitions, bindings)
}

fn management_definitions() -> Vec<ToolDefinition> {
    vec![
        ToolDefinition::new(
            ADF_DEFINE,
            "Define a bounded Run-scoped JavaScript function as a tool. It becomes callable on the next model step.",
            json!({
                "type": "object",
                "properties": {
                    "requested_name": {"type": "string", "minLength": 1, "maxLength": 64},
                    "description": {"type": "string", "minLength": 1},
                    "input_schema": {"type": "object"},
                    "output_schema": {"type": "object"},
                    "runtime": {
                        "type": "object",
                        "properties": {"type": {"const": "javascript"}},
                        "required": ["type"],
                        "additionalProperties": false
                    },
                    "source": {"type": "string", "minLength": 1},
                    "entrypoint": {"type": "string", "default": "main"},
                    "requested_capabilities": {"type": "array", "maxItems": 0},
                    "requested_scope": {"const": "run"},
                    "execution_mode": {"const": "sync"},
                    "idempotency_key": {"type": "string", "minLength": 1, "maxLength": 256}
                },
                "required": [
                    "requested_name", "description", "input_schema", "runtime", "source",
                    "requested_scope", "execution_mode", "idempotency_key"
                ],
                "additionalProperties": false
            }),
        )
        .with_risk_level(ToolRiskLevel::Low),
        ToolDefinition::new(
            ADF_LIST,
            "List the active Agent Defined Functions in this run.",
            json!({"type": "object", "properties": {}, "additionalProperties": false}),
        ),
        ToolDefinition::new(
            ADF_REMOVE,
            "Deactivate one Agent Defined Function by its canonical name. The change applies on the next model step.",
            json!({
                "type": "object",
                "properties": {"canonical_name": {"type": "string", "minLength": 1}},
                "required": ["canonical_name"],
                "additionalProperties": false
            }),
        ),
    ]
}

fn validate_management(name: &str, arguments: &Value) -> Result<(), ToolError> {
    match name {
        ADF_DEFINE => serde_json::from_value::<AdfDefinitionRequest>(arguments.clone())
            .map(|_| ())
            .map_err(|_| invalid_arguments("adf_define arguments are invalid")),
        ADF_LIST => serde_json::from_value::<EmptyArguments>(arguments.clone())
            .map(|_| ())
            .map_err(|_| invalid_arguments("adf_list arguments are invalid")),
        ADF_REMOVE => serde_json::from_value::<RemoveArguments>(arguments.clone())
            .map(|_| ())
            .map_err(|_| invalid_arguments("adf_remove arguments are invalid")),
        _ => Err(tool_not_found()),
    }
}

fn validate_schema(schema: &Value, arguments: &Value) -> Result<(), ToolError> {
    let validator = JSONSchema::compile(schema).map_err(|_| {
        ToolError::new(
            "adf_invalid_schema",
            "the locked ADF input schema is invalid",
            false,
        )
        .with_category(ToolErrorCategory::Internal)
    })?;
    validator.validate(arguments).map_err(|_| {
        ToolError::new(
            "invalid_tool_arguments",
            "ADF arguments do not match its locked input schema",
            false,
        )
        .with_category(ToolErrorCategory::InvalidRequest)
    })
}

fn descriptor(artifact: &AdfArtifact, active: bool) -> AdfArtifactDescriptor {
    AdfArtifactDescriptor {
        reference: artifact.definition.source_ref.clone(),
        canonical_name: artifact.definition.canonical_name.clone(),
        runtime: artifact.definition.runtime.kind,
        owner: artifact.definition.owner.clone(),
        effective_risk: artifact.definition.effective_risk,
        active,
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EmptyArguments {}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RemoveArguments {
    canonical_name: String,
}

fn is_management_tool(name: &str) -> bool {
    matches!(name, ADF_DEFINE | ADF_LIST | ADF_REMOVE)
}

fn revision_mismatch() -> ToolError {
    ToolError::new(
        "adf_tool_revision_mismatch",
        "the requested tool-set revision is no longer valid for this command",
        false,
    )
    .with_category(ToolErrorCategory::Conflict)
}

fn tool_not_found() -> ToolError {
    ToolError::new(
        "tool_not_found",
        "the requested tool is not present in this tool-set revision",
        false,
    )
    .with_category(ToolErrorCategory::NotFound)
}

fn invalid_arguments(message: &'static str) -> ToolError {
    ToolError::new("invalid_tool_arguments", message, false)
        .with_category(ToolErrorCategory::InvalidRequest)
}

fn map_store_error(error: AdfStoreError) -> ToolError {
    let (code, category, retryable) = match error {
        AdfStoreError::ReadOnly => (
            "adf_store_read_only",
            ToolErrorCategory::PermissionDenied,
            false,
        ),
        AdfStoreError::IdempotencyConflict => (
            "adf_idempotency_conflict",
            ToolErrorCategory::Conflict,
            false,
        ),
        AdfStoreError::ArtifactConflict => {
            ("adf_artifact_conflict", ToolErrorCategory::Conflict, false)
        }
        AdfStoreError::InvalidArtifact(_) => (
            "adf_artifact_invalid",
            ToolErrorCategory::InvalidRequest,
            false,
        ),
        AdfStoreError::NotFound => ("adf_artifact_not_found", ToolErrorCategory::NotFound, false),
        AdfStoreError::Backend(_) => ("adf_store_failed", ToolErrorCategory::Unavailable, true),
    };
    ToolError::new(
        code,
        "the ADF artifact store could not complete the request",
        retryable,
    )
    .with_category(category)
}

fn map_adf_error(error: AdfError) -> ToolError {
    let category = match error.kind() {
        AdfErrorKind::InvalidRequest => ToolErrorCategory::InvalidRequest,
        AdfErrorKind::Conflict => ToolErrorCategory::Conflict,
        AdfErrorKind::PermissionDenied => ToolErrorCategory::PermissionDenied,
        AdfErrorKind::NotFound => ToolErrorCategory::NotFound,
        AdfErrorKind::ResourceExhausted => ToolErrorCategory::ResourceExhausted,
        AdfErrorKind::Timeout => ToolErrorCategory::Timeout,
        AdfErrorKind::Cancelled => ToolErrorCategory::Cancelled,
        AdfErrorKind::Unavailable => ToolErrorCategory::Unavailable,
        AdfErrorKind::Internal => ToolErrorCategory::Internal,
    };
    ToolError::new(error.code(), error.safe_message(), error.retryable()).with_category(category)
}

fn unix_time_ms() -> i64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    i64::try_from(millis).unwrap_or(i64::MAX)
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AdfSessionBuildError {
    #[error("max_active_per_run must be greater than zero")]
    InvalidLimit,
    #[error("ADF store must support read, write, and deactivate")]
    StoreCapabilities,
    #[error("base tool set uses reserved ADF management name `{0}`")]
    ReservedName(String),
}

#[cfg(test)]
mod tests {
    use agent_core::{
        adf::RunScopedJavaScriptPolicy,
        harness::{RunCancellation, ToolPort, ToolRegistry},
        script::{ScriptLimits, ScriptRuntime},
    };
    use agent_harness::{adf::JavaScriptAdfExecutor, script::QuickJsRuntime};

    use super::*;
    use crate::adf::InMemoryAdfArtifactStore;

    fn session() -> RunAdfToolSession<ToolRegistry> {
        let runtime: Arc<dyn ScriptRuntime> = Arc::new(QuickJsRuntime::default());
        RunAdfToolSession::new(ToolRegistry::new())
            .with_adf(
                Arc::new(InMemoryAdfArtifactStore::new()),
                Arc::new(RunScopedJavaScriptPolicy::new(runtime.descriptor())),
                Arc::new(JavaScriptAdfExecutor::new(runtime, ScriptLimits::default())),
                1,
            )
            .expect("ADF session should build")
    }

    fn define_arguments() -> Value {
        json!({
            "requested_name": "sum_values",
            "description": "Sum an array of numbers.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "values": {"type": "array", "items": {"type": "number"}}
                },
                "required": ["values"],
                "additionalProperties": false
            },
            "output_schema": {"type": "number"},
            "runtime": {"type": "javascript"},
            "source": "export function main(input) { return input.values.reduce((sum, value) => sum + value, 0); }",
            "entrypoint": "main",
            "requested_capabilities": [],
            "requested_scope": "run",
            "execution_mode": "sync",
            "idempotency_key": "sum-values-v1"
        })
    }

    fn call(run_id: RunId, call_id: &str, name: &str, arguments: Value) -> ToolCallRequest {
        ToolCallRequest {
            run_id,
            call_id: call_id.into(),
            name: name.into(),
            arguments,
            cancellation: RunCancellation::new(),
        }
    }

    #[tokio::test]
    async fn defines_lists_invokes_and_removes_a_run_scoped_function() {
        let session = session();
        let run_id = RunId::new();
        let initial = session
            .tool_set_snapshot(run_id)
            .expect("initial snapshot should build");
        assert_eq!(initial.revision, 1);
        assert!(initial.binding(ADF_DEFINE).is_some());
        assert!(initial.bindings.iter().all(|binding| {
            matches!(
                binding.kind,
                ToolBindingKind::Static | ToolBindingKind::AdfManagement
            )
        }));

        let arguments = define_arguments();
        session
            .validate_at(run_id, initial.revision, ADF_DEFINE, &arguments)
            .expect("definition request should validate");
        let defined = session
            .call_at(
                initial.revision,
                call(run_id, "call_define", ADF_DEFINE, arguments.clone()),
            )
            .await
            .expect("ADF should be defined");
        let defined: Value =
            serde_json::from_str(&defined.content).expect("define result should be JSON");
        let canonical_name = defined["canonical_name"]
            .as_str()
            .expect("canonical name should exist")
            .to_owned();

        let active = session
            .tool_set_snapshot(run_id)
            .expect("active snapshot should build");
        assert_eq!(active.revision, initial.revision + 1);
        assert_eq!(defined["tool_set_revision"], active.revision);
        assert_eq!(
            active.binding(&canonical_name).map(|binding| binding.kind),
            Some(ToolBindingKind::AgentDefined)
        );

        let invoked = session
            .call_at(
                active.revision,
                call(
                    run_id,
                    "call_dynamic",
                    &canonical_name,
                    json!({"values": [1, 2, 3]}),
                ),
            )
            .await
            .expect("dynamic function should execute");
        assert_eq!(invoked.content, "6");

        let listed = session
            .call_at(
                active.revision,
                call(run_id, "call_list", ADF_LIST, json!({})),
            )
            .await
            .expect("ADF list should succeed");
        assert!(listed.content.contains(&canonical_name));

        let replay = session
            .call_at(
                active.revision,
                call(run_id, "call_replay", ADF_DEFINE, arguments),
            )
            .await
            .expect("idempotent definition should replay");
        let replay: Value =
            serde_json::from_str(&replay.content).expect("replay result should be JSON");
        assert_eq!(replay["tool_set_revision"], active.revision);

        session
            .call_at(
                active.revision,
                call(
                    run_id,
                    "call_remove",
                    ADF_REMOVE,
                    json!({"canonical_name": canonical_name.clone()}),
                ),
            )
            .await
            .expect("ADF should be removed");
        let removed = session
            .tool_set_snapshot(run_id)
            .expect("removed snapshot should build");
        assert!(removed.binding(&canonical_name).is_none());

        let historical = session
            .call_at(
                active.revision,
                call(
                    run_id,
                    "call_historical",
                    &canonical_name,
                    json!({"values": [4, 5]}),
                ),
            )
            .await
            .expect("a call already bound to the old step should remain valid");
        assert_eq!(historical.content, "9");

        let missing = session
            .call_at(
                removed.revision,
                call(
                    run_id,
                    "call_removed",
                    &canonical_name,
                    json!({"values": []}),
                ),
            )
            .await
            .expect_err("new snapshots must not expose a removed ADF");
        assert_eq!(missing.code(), "tool_not_found");
    }
}
