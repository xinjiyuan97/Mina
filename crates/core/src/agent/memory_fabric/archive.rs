use super::*;
use crate::{
    agent::graph::{GraphRun, event_log_key, public_journal_key, run_checkpoint_key},
    contracts::{RunId, run_archive::*},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use std::collections::BTreeSet;

pub const MAINTENANCE_KEY: &str = "$fabric-maintenance-v1";
pub fn maintenance_allowed(command: &FabricCommand) -> bool {
    matches!(
        command,
        FabricCommand::BeginMaintenance { .. }
            | FabricCommand::EndMaintenance { .. }
            | FabricCommand::ExportAgentHistory { .. }
            | FabricCommand::InspectRun { .. }
            | FabricCommand::ExportRun { .. }
            | FabricCommand::ImportRun { .. }
            | FabricCommand::ReadCheckpoint(_)
            | FabricCommand::ReadLog { .. }
            | FabricCommand::ReadEvent(_)
            | FabricCommand::ScanEvents { .. }
            | FabricCommand::Replay { .. }
            | FabricCommand::Offset { .. }
            | FabricCommand::Receipt(_)
            | FabricCommand::DeadLetters { .. }
            | FabricCommand::ArtifactRoots
            | FabricCommand::InspectRetention { .. }
    )
}
impl FabricState {
    /// One in-memory view / one SQLite read transaction fixes membership and contents.
    pub(super) fn export_agent_history(
        &self,
        tenant: &str,
        agent_id: &str,
    ) -> Result<FabricReply, FabricError> {
        use crate::contracts::agent_history::{AgentHistory, HistoryBlob, RunHistory};
        key(tenant)?;
        key(agent_id)?;
        let mut members = BTreeSet::new();
        for checkpoint in self.checkpoints.values() {
            let value = &checkpoint.value;
            if value["namespace"] == "$agent-run"
                && value["tenant"] == tenant
                && value["payload"]["agent_id"] == agent_id
            {
                let id = value["run_id"]
                    .as_str()
                    .ok_or_else(|| invalid("agent membership"))?;
                members.insert(id.to_owned());
            }
        }
        let mut runs = Vec::new();
        let mut total = 0usize;
        for id in members {
            let run: RunId = id.parse().map_err(storage)?;
            let root = run_checkpoint_key(tenant, run);
            let saved = self
                .checkpoints
                .get(&root)
                .ok_or_else(|| invalid("incomplete_history: missing run"))?;
            if saved.value["retired"] == true {
                return Err(invalid("incomplete_history: retired run"));
            }
            let graph: GraphRun = serde_json::from_value(saved.value.clone()).map_err(storage)?;
            if graph.tenant != tenant
                || graph.run_id != run
                || graph.agent_id.as_deref() != Some(agent_id)
            {
                return Err(FabricError::Conflict);
            }
            if !graph.is_terminal() {
                continue;
            }
            // Historical wait facts live in the graph log. Live external subscription
            // tables are neither historical evidence nor needed for this non-recovery format.
            let state = self.run_state(tenant, run, true, Some(&json!({})), true)?;
            let events: Vec<Value> = state
                .logs
                .get(&event_log_key(tenant, run))
                .map(|items| items.iter().map(|v| v.as_ref().clone()).collect())
                .unwrap_or_default();
            if events.len() as u64 != graph.event_count
                || events.is_empty()
                || events
                    .last()
                    .is_none_or(|v| v["kind"] != "engine.terminal.v1")
            {
                return Err(invalid("incomplete_history: graph event gap"));
            }
            let journal = public_journal_key(tenant, run);
            let public_events: Vec<Value> = state
                .logs
                .get(&format!("{journal}:events"))
                .map(|items| items.iter().map(|v| v.as_ref().clone()).collect())
                .unwrap_or_default();
            let public_through = state
                .checkpoints
                .get(&journal)
                .and_then(|v| v.value["public"].as_u64())
                .unwrap_or(0);
            if public_events.len() as u64 != public_through {
                return Err(invalid("incomplete_history: public event gap"));
            }
            let mut blobs = Vec::new();
            for (key, manifest) in &state.checkpoints {
                if !key.starts_with("pipeline-artifact:") || !manifest.value["metadata"].is_object()
                {
                    continue;
                }
                let metadata =
                    serde_json::from_value(manifest.value["metadata"].clone()).map_err(storage)?;
                let chunks = manifest.value["chunks"]
                    .as_u64()
                    .ok_or_else(|| invalid("blob chunks"))?;
                let mut bytes = Vec::new();
                for index in 0..chunks {
                    bytes.extend(
                        STANDARD
                            .decode(
                                state.checkpoints[&format!("{key}:{index}")]
                                    .value
                                    .as_str()
                                    .ok_or_else(|| invalid("blob encoding"))?,
                            )
                            .map_err(storage)?,
                    );
                }
                blobs.push(HistoryBlob {
                    metadata,
                    base64: STANDARD.encode(bytes),
                });
            }
            let history = RunHistory {
                run_id: run,
                final_state: saved.value.clone(),
                events_through: graph.event_count,
                events,
                routed_events: state
                    .events
                    .iter()
                    .map(|event| event.event.clone())
                    .collect(),
                completion: state
                    .checkpoints
                    .get(&crate::contracts::host::host_record_key(
                        tenant,
                        "$completion",
                        &run.to_string(),
                    ))
                    .map(|record| serde_json::from_value(record.value.clone()).map_err(storage))
                    .transpose()?,
                public_events_through: public_through,
                public_events,
                blobs,
            };
            total = total.saturating_add(serde_json::to_vec(&history).map_err(storage)?.len());
            if total > MAX_BUNDLE_BYTES {
                return Err(FabricError::Capacity);
            }
            runs.push(history);
        }
        Ok(FabricReply::AgentHistory(AgentHistory::new(
            tenant, agent_id, runs,
        )?))
    }
    pub(super) fn begin_maintenance(
        &mut self,
        token: String,
        now: i64,
    ) -> Result<FabricReply, FabricError> {
        key(&token)?;
        if let Some(saved) = self.checkpoints.get(MAINTENANCE_KEY) {
            return if saved.value == json!(token) {
                Ok(FabricReply::Maintenance)
            } else {
                Err(FabricError::Fenced)
            };
        }
        if self
            .subscriptions
            .iter()
            .any(|s| s.lease.as_ref().is_some_and(|l| l.expires_at_ms > now))
        {
            return Err(FabricError::Fenced);
        }
        self.checkpoints.insert(
            MAINTENANCE_KEY.into(),
            Arc::new(FabricCheckpoint {
                version: 0,
                value: json!(token),
            }),
        );
        Ok(FabricReply::Maintenance)
    }
    pub(super) fn end_maintenance(&mut self, token: &str) -> Result<FabricReply, FabricError> {
        self.check_maintenance(token)?;
        self.checkpoints.remove(MAINTENANCE_KEY);
        Ok(FabricReply::Maintenance)
    }
    fn check_maintenance(&self, token: &str) -> Result<(), FabricError> {
        if !self
            .checkpoints
            .get(MAINTENANCE_KEY)
            .is_some_and(|s| s.value == json!(token))
        {
            return Err(FabricError::Fenced);
        }
        Ok(())
    }
    fn run_state(
        &self,
        tenant: &str,
        run: RunId,
        strict: bool,
        external: Option<&Value>,
        history_only: bool,
    ) -> Result<Self, FabricError> {
        key(tenant)?;
        let root = run_checkpoint_key(tenant, run);
        let saved = self.checkpoints.get(&root).ok_or(FabricError::NotFound)?;
        if saved.value["retired"] == true {
            return Err(invalid("retention_expired"));
        }
        let graph: GraphRun = serde_json::from_value(saved.value.clone()).map_err(storage)?;
        if graph.tenant != tenant || graph.run_id != run {
            return Err(FabricError::Conflict);
        }
        if strict
            && graph.definition.operators.values().any(|op| {
                !(matches!(
                    op.kind.as_str(),
                    "agent.context"
                        | "agent.model"
                        | "agent.gate"
                        | "agent.tools"
                        | "agent.tool_policy"
                ) || (external.is_some() && op.kind == "agent.wait"))
            })
        {
            return Err(invalid(
                "incomplete_dependencies: external waits/custom operators require an archive adapter",
            ));
        }
        let mut result = Self::default();
        let public = public_journal_key(tenant, run);
        let mut keys = BTreeSet::from([root.clone(), public.clone()]);
        if tenant == "local" {
            keys.insert(format!("graph-admission:{run}"));
        }
        keys.insert(crate::contracts::host::host_record_key(
            tenant,
            "$completion",
            &run.to_string(),
        ));
        let wait_index = format!(
            "graph-waits:{}",
            crate::contracts::events::EventId::stable(
                "graph-waits",
                &json!([tenant, run]).to_string()
            )
        );
        if let Some(index) = self.checkpoints.get(&wait_index) {
            if strict && external.is_none() {
                return Err(invalid("incomplete_dependencies: external wait state"));
            }
            for registration in index
                .value
                .as_object()
                .ok_or_else(|| invalid("wait index"))?
                .keys()
            {
                keys.extend([
                    registration.clone(),
                    format!("{registration}:cleaned"),
                    format!("{registration}:wake"),
                ]);
            }
            keys.insert(wait_index);
        }
        for (key, checkpoint) in &self.checkpoints {
            if key.starts_with("host-record:")
                && checkpoint.value["tenant"] == tenant
                && checkpoint.value["run_id"] == json!(run)
            {
                keys.insert(key.clone());
            }
            if (key.starts_with("graph-wait-subscription:")
                || key.starts_with("graph-job-binding:"))
                && checkpoint.value["tenant"] == tenant
                && checkpoint.value["run"] == json!(run)
            {
                keys.insert(key.clone());
            }
        }
        for key in keys {
            if let Some(c) = self.checkpoints.get(&key) {
                result.checkpoints.insert(key, c.clone());
            }
        }
        for log in [event_log_key(tenant, run), format!("{public}:events")] {
            if let Some(items) = self.logs.get(&log) {
                result.logs.insert(log, items.clone());
            }
        }
        result.events = self
            .events
            .iter()
            .filter(|e| e.event.envelope.tenant_id == tenant && e.event.envelope.run_id == run)
            .cloned()
            .collect();
        let ids: BTreeSet<_> = result
            .events
            .iter()
            .map(|e| e.event.envelope.event_id.to_string())
            .collect();
        let topics: BTreeSet<_> = result
            .events
            .iter()
            .map(|e| e.event.topic.as_str())
            .collect();
        for sub in self.subscriptions.iter().filter(|_| !history_only) {
            if sub.spec.consumer.starts_with(&format!("{root}:"))
                || (topics.contains(sub.spec.topic.as_str())
                    && sub.spec.tenant.as_deref().is_none_or(|t| t == tenant))
            {
                if strict
                    && self.events.iter().any(|e| {
                        e.event.topic == sub.spec.topic
                            && sub
                                .spec
                                .tenant
                                .as_deref()
                                .is_none_or(|t| t == e.event.envelope.tenant_id)
                            && (e.event.envelope.tenant_id != tenant
                                || e.event.envelope.run_id != run)
                    })
                {
                    return Err(invalid("incomplete_dependencies: shared subscription"));
                }
                result.subscriptions.push(sub.clone());
            }
        }
        result.receipts = self
            .receipts
            .iter()
            .filter(|r| ids.contains(&r.key.event_id.to_string()))
            .cloned()
            .collect();
        result.dead_letters = self
            .dead_letters
            .iter()
            .filter(|r| ids.contains(&r.event_id.to_string()))
            .cloned()
            .collect();
        if strict {
            // Known blob reference fields, never UUID-looking strings in arbitrary text.
            fn refs(v: &Value, found: &mut BTreeSet<String>) {
                match v {
                    Value::Object(map) => {
                        for (k, v) in map {
                            if matches!(k.as_str(), "blob_id" | "previous") {
                                if let Some(id) = v
                                    .as_str()
                                    .filter(|s| s.parse::<crate::contracts::BlobId>().is_ok())
                                {
                                    found.insert(id.into());
                                }
                            }
                            refs(v, found);
                        }
                    }
                    Value::Array(items) => {
                        for v in items {
                            refs(v, found);
                        }
                    }
                    _ => {}
                }
            }
            let mut pending = BTreeSet::new();
            refs(
                &serde_json::to_value(&result).map_err(storage)?,
                &mut pending,
            );
            if let Some(dependencies) = external {
                refs(dependencies, &mut pending);
            }
            let mut visited = BTreeSet::new();
            while let Some(id) = pending.pop_first() {
                if !visited.insert(id.clone()) {
                    continue;
                }
                let key = format!("pipeline-artifact:{id}");
                let manifest = self
                    .checkpoints
                    .get(&key)
                    .ok_or_else(|| invalid("incomplete_dependencies: external or missing blob"))?;
                if manifest
                    .value
                    .get("schema_version")
                    .is_some_and(|v| !v.is_null() && *v != json!(1))
                    || manifest.value["complete"] == false
                    || manifest.value["metadata"]["blob_id"] != id
                {
                    return Err(invalid("incomplete blob"));
                }
                let count = manifest.value["chunks"]
                    .as_u64()
                    .filter(|n| *n <= 64)
                    .ok_or_else(|| invalid("blob chunks"))?;
                let mut bytes = vec![];
                result.checkpoints.insert(key.clone(), manifest.clone());
                for index in 0..count {
                    let key = format!("{key}:{index}");
                    let chunk = self
                        .checkpoints
                        .get(&key)
                        .ok_or_else(|| invalid("blob gap"))?;
                    bytes.extend(
                        STANDARD
                            .decode(
                                chunk
                                    .value
                                    .as_str()
                                    .ok_or_else(|| invalid("blob encoding"))?,
                            )
                            .map_err(storage)?,
                    );
                    if bytes.len() > 16 * 1024 * 1024 {
                        return Err(invalid("blob size"));
                    }
                    result.checkpoints.insert(key, chunk.clone());
                }
                if manifest.value["metadata"]["size_bytes"].as_u64() != Some(bytes.len() as u64) {
                    return Err(invalid("blob size"));
                }
                if manifest.value["metadata"]["media_type"]
                    .as_str()
                    .is_some_and(|m| m == "application/json" || m.ends_with("+json"))
                {
                    refs(
                        &serde_json::from_slice::<Value>(&bytes).map_err(storage)?,
                        &mut pending,
                    );
                }
            }
        }
        Ok(result)
    }
    pub(super) fn inspect_run(&self, tenant: &str, run: RunId) -> Result<FabricReply, FabricError> {
        let state = self.run_state(tenant, run, false, None, false)?;
        let status = state.checkpoints[&run_checkpoint_key(tenant, run)].value["status"].clone();
        let value = serde_json::to_value(state).map_err(storage)?;
        let snapshot_id = crate::agent::bindings::digest(&value)?;
        let mut items = vec![];
        for (kind, records) in value.as_object().ok_or(FabricError::Conflict)? {
            match records {
                Value::Object(map) => {
                    for (id, value) in map {
                        items.push(RunInspectionItem {
                            logical_id: crate::agent::bindings::digest(&(kind, id))?,
                            kind: kind.clone(),
                            schema_version: 1,
                            revision: value["version"].as_u64(),
                            payload: value.clone(),
                        });
                    }
                }
                Value::Array(records) => {
                    for (i, value) in records.iter().enumerate() {
                        items.push(RunInspectionItem {
                            logical_id: crate::agent::bindings::digest(&(kind, i))?,
                            kind: kind.clone(),
                            schema_version: 1,
                            revision: None,
                            payload: value.clone(),
                        });
                    }
                }
                _ => {}
            }
        }
        items.sort_by(|a, b| (&a.kind, &a.logical_id).cmp(&(&b.kind, &b.logical_id)));
        Ok(FabricReply::RunInspection(RunInspectionSnapshot {
            api_version: 1,
            tenant: tenant.into(),
            run_id: run,
            snapshot_id,
            status,
            diagnostic_only: true,
            items,
        }))
    }
    pub(super) fn export_run(
        &self,
        tenant: &str,
        run: RunId,
        token: &str,
    ) -> Result<FabricReply, FabricError> {
        self.check_maintenance(token)?;
        let state = self.run_state(tenant, run, true, None, false)?;
        Ok(FabricReply::RunBundle(RunBundle::new(
            tenant.into(),
            run,
            serde_json::to_value(state).map_err(storage)?,
        )?))
    }
    pub fn export_with_dependencies(
        &self,
        tenant: &str,
        run: RunId,
        token: &str,
        dependencies: Value,
    ) -> Result<FabricReply, FabricError> {
        self.check_maintenance(token)?;
        let state = self.run_state(tenant, run, true, Some(&dependencies), false)?;
        Ok(FabricReply::RunBundle(
            RunBundle::new(
                tenant.into(),
                run,
                serde_json::to_value(state).map_err(storage)?,
            )?
            .with_dependencies(dependencies)?,
        ))
    }
    pub(super) fn import_run(
        &mut self,
        bundle: RunBundle,
        token: &str,
    ) -> Result<FabricReply, FabricError> {
        if bundle.dependencies.is_some() {
            return Err(invalid("external archive adapter required"));
        }
        self.import_archive(bundle, token, false)
    }
    /// Adapter must validate and restore external dependencies in the same transaction.
    pub fn import_archive(
        &mut self,
        bundle: RunBundle,
        token: &str,
        external: bool,
    ) -> Result<FabricReply, FabricError> {
        self.check_maintenance(token)?;
        bundle.verify()?;
        let marker = crate::contracts::run_archive::import_marker(&bundle.tenant, bundle.run_id);
        if self
            .checkpoints
            .get(&run_checkpoint_key(&bundle.tenant, bundle.run_id))
            .is_some_and(|v| v.value["retired"] == true)
        {
            return Err(invalid("retention_expired"));
        }
        if self
            .checkpoints
            .get(&marker)
            .is_some_and(|v| v.value == json!([bundle.tenant, bundle.digest]))
        {
            return Ok(FabricReply::Maintenance);
        }
        let mut incoming: Self = serde_json::from_value(bundle.data.clone()).map_err(storage)?;
        let checked = incoming.run_state(
            &bundle.tenant,
            bundle.run_id,
            true,
            if external {
                bundle.dependencies.as_ref()
            } else {
                None
            },
            false,
        )?;
        if serde_json::to_value(&checked).map_err(storage)? != bundle.data {
            return Err(invalid("bundle contains unrelated or unsupported records"));
        }
        let graph = bundle.graph_run()?;
        if incoming
            .logs
            .get(&event_log_key(&bundle.tenant, bundle.run_id))
            .map_or(0, |v| v.len() as u64)
            != graph.event_count
        {
            return Err(invalid("graph log watermark mismatch"));
        }
        let mut ids = BTreeSet::new();
        for (sequence, event) in incoming.events.iter().enumerate() {
            validate_events(std::slice::from_ref(&event.event), false, self.limits)?;
            if event.retired
                || event.event.envelope.operator_seq.0 != sequence as u64 + 1
                || !ids.insert(event.event.envelope.event_id.to_string())
            {
                return Err(invalid("event sequence or identity"));
            }
        }
        for subscription in &incoming.subscriptions {
            let count = incoming
                .events
                .iter()
                .filter(|e| {
                    e.event.topic == subscription.spec.topic
                        && subscription
                            .spec
                            .event_type
                            .as_ref()
                            .is_none_or(|t| *t == e.event.envelope.event_type)
                        && subscription
                            .spec
                            .tenant
                            .as_ref()
                            .is_none_or(|t| *t == e.event.envelope.tenant_id)
                })
                .count() as u64;
            if subscription.offset > count {
                return Err(FabricError::AckHole);
            }
        }
        let root = run_checkpoint_key(&bundle.tenant, bundle.run_id);
        if self.checkpoints.contains_key(&root) {
            return Err(FabricError::Conflict);
        }
        for (key, value) in &incoming.checkpoints {
            if self
                .checkpoints
                .get(key)
                .is_some_and(|old| **old != **value)
            {
                return Err(FabricError::Conflict);
            }
        }
        if incoming.logs.keys().any(|key| self.logs.contains_key(key))
            || incoming.events.iter().any(|e| {
                self.events
                    .iter()
                    .any(|old| old.event.envelope.event_id == e.event.envelope.event_id)
            })
            || incoming.subscriptions.iter().any(|s| {
                self.subscriptions.iter().any(|old| {
                    old.spec.consumer == s.spec.consumer
                        && old.spec.subscription == s.spec.subscription
                })
            })
        {
            return Err(FabricError::Conflict);
        }
        for s in &mut incoming.subscriptions {
            s.lease = None;
            s.epoch = s
                .epoch
                .checked_add(1)
                .ok_or_else(|| invalid("lease epoch overflow"))?;
        }
        // The global maintenance fence remains held; imports cannot execute until explicitly released.
        let completion_key = crate::contracts::host::host_record_key(
            &bundle.tenant,
            "$completion",
            &bundle.run_id.to_string(),
        );
        if let Some(record) = incoming.checkpoints.get(&completion_key) {
            let log = crate::contracts::host::completion_log(&bundle.tenant);
            self.logs
                .entry(log)
                .or_default()
                .push(Arc::new(record.value.clone()));
        }
        self.checkpoints.extend(incoming.checkpoints);
        self.logs.extend(incoming.logs);
        self.events.extend(incoming.events);
        self.subscriptions.extend(incoming.subscriptions);
        self.receipts.extend(incoming.receipts);
        self.dead_letters.extend(incoming.dead_letters);
        self.checkpoints.insert(
            marker,
            Arc::new(FabricCheckpoint {
                version: 0,
                value: json!([bundle.tenant, bundle.digest]),
            }),
        );
        Ok(FabricReply::Maintenance)
    }
}
