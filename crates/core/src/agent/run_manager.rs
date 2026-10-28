//! Trusted host management API. Raw inspection payloads require host authorization.
use super::{AgentResources, GraphRun, HostPolicy, OperatorRegistry};
use crate::contracts::{RunId, fabric::*, host::*, run_archive::*};
use serde_json::json;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

pub struct RunManager {
    tenant: String,
    fabric: Arc<dyn OperatorFabric>,
    snapshots: Mutex<BTreeMap<String, (Instant, RunInspectionSnapshot)>>,
}
impl RunManager {
    /// Attach an existing Run to its host Agent. Immutable once assigned; useful for
    /// migrating Runs admitted before with_agent_id was configured.
    pub async fn register_run(&self, agent_id: &str, run_id: RunId) -> Result<(), FabricError> {
        if agent_id.trim().is_empty() || agent_id.len() > 128 {
            return Err(FabricError::Invalid("agent identity".into()));
        }
        let key = super::graph::run_checkpoint_key(&self.tenant, run_id);
        let FabricReply::Checkpoint(Some(saved)) = self
            .fabric
            .execute(FabricCommand::ReadCheckpoint(key.clone()))
            .await?
        else {
            return Err(FabricError::NotFound);
        };
        let mut run: GraphRun = serde_json::from_value(saved.value).map_err(storage)?;
        if run.tenant != self.tenant
            || run.run_id != run_id
            || run.agent_id.as_deref().is_some_and(|id| id != agent_id)
        {
            return Err(FabricError::Conflict);
        }
        run.agent_id = Some(agent_id.into());
        self.fabric
            .execute(FabricCommand::HostTransact {
                records: vec![crate::contracts::agent_history::membership(
                    &self.tenant,
                    agent_id,
                    run_id,
                )],
                completion: None,
                transaction: Box::new(FabricCommand::Transact {
                    checkpoint: CheckpointWrite {
                        key,
                        expected: Some(saved.version),
                        value: serde_json::to_value(run).map_err(storage)?,
                    },
                    outbox: vec![],
                    delivery: None,
                }),
            })
            .await
            .map(|_| ())
    }
    /// Exports only terminal Runs at one consistent read boundary. Never pauses workers.
    /// Membership is recorded atomically by AgentResources::with_agent_id at admission.
    pub async fn export_agent_history(
        &self,
        agent_id: &str,
    ) -> Result<crate::contracts::agent_history::AgentHistory, FabricError> {
        if agent_id.trim().is_empty() || agent_id.len() > 128 {
            return Err(FabricError::Invalid("agent identity".into()));
        }
        match self
            .fabric
            .execute(FabricCommand::ExportAgentHistory {
                tenant: self.tenant.clone(),
                agent_id: agent_id.into(),
            })
            .await?
        {
            FabricReply::AgentHistory(history) => Ok(history),
            _ => Err(FabricError::Conflict),
        }
    }
    pub fn new(tenant: impl Into<String>, fabric: Arc<dyn OperatorFabric>) -> Self {
        Self {
            tenant: tenant.into(),
            fabric,
            snapshots: Mutex::new(BTreeMap::new()),
        }
    }
    /// Full diagnostic content, not HTTP-safe. Cursors expire after 60 seconds.
    pub async fn inspect_run(
        &self,
        run_id: RunId,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<RunInspectionPage, FabricError> {
        if limit == 0 || limit > MAX_BATCH_ITEMS {
            return Err(FabricError::Invalid("page limit".into()));
        }
        let (id, offset) = if let Some(cursor) = cursor {
            let (id, offset) = cursor
                .rsplit_once(':')
                .ok_or_else(|| FabricError::Invalid("cursor".into()))?;
            (
                id.to_string(),
                offset
                    .parse::<usize>()
                    .map_err(|_| FabricError::Invalid("cursor".into()))?,
            )
        } else {
            let FabricReply::RunInspection(snapshot) = self
                .fabric
                .execute(FabricCommand::InspectRun {
                    tenant: self.tenant.clone(),
                    run_id,
                })
                .await?
            else {
                return Err(FabricError::Conflict);
            };
            if serde_json::to_vec(&snapshot).map_err(storage)?.len() > MAX_BUNDLE_BYTES {
                return Err(FabricError::Capacity);
            }
            let id = uuid::Uuid::new_v4().to_string();
            let mut cache = self.snapshots.lock().map_err(storage)?;
            cache.retain(|_, (time, _)| time.elapsed() < Duration::from_secs(60));
            if cache.len() >= 8 {
                return Err(FabricError::Capacity);
            }
            cache.insert(id.clone(), (Instant::now(), snapshot));
            (id, 0)
        };
        let cache = self.snapshots.lock().map_err(storage)?;
        let (time, snapshot) = cache
            .get(&id)
            .ok_or_else(|| FabricError::Invalid("snapshot_expired".into()))?;
        if time.elapsed() >= Duration::from_secs(60) {
            return Err(FabricError::Invalid("snapshot_expired".into()));
        }
        if snapshot.tenant != self.tenant
            || snapshot.run_id != run_id
            || offset > snapshot.items.len()
        {
            return Err(FabricError::Invalid("cursor scope".into()));
        }
        let mut items = vec![];
        let mut size = 0;
        for item in snapshot.items.iter().skip(offset).take(limit) {
            let mut item = item.clone();
            let mut bytes = serde_json::to_vec(&item).map_err(storage)?.len();
            if bytes > MAX_CHECKPOINT_BYTES / 2 {
                item.payload = json!({"content_omitted":true,"bytes":bytes,"digest":super::bindings::digest(&item.payload)?});
                bytes = serde_json::to_vec(&item).map_err(storage)?.len();
            }
            if size + bytes > MAX_CHECKPOINT_BYTES {
                break;
            }
            size += bytes;
            items.push(item);
        }
        let end = offset + items.len();
        Ok(RunInspectionPage {
            api_version: 1,
            snapshot_id: snapshot.snapshot_id.clone(),
            status: snapshot.status.clone(),
            items,
            next_cursor: (end < snapshot.items.len()).then(|| format!("{id}:{end}")),
        })
    }
    /// Global Fabric barrier. Stop workers and drain leases before acquiring it.
    /// Keep the source fenced throughout a migration; only explicitly release on one owner.
    pub async fn begin_maintenance(&self, token: &str, now_ms: i64) -> Result<(), FabricError> {
        self.fabric
            .execute(FabricCommand::BeginMaintenance {
                token: token.into(),
                now_ms,
            })
            .await
            .map(|_| ())
    }
    pub async fn end_maintenance(&self, token: &str) -> Result<(), FabricError> {
        self.fabric
            .execute(FabricCommand::EndMaintenance {
                token: token.into(),
            })
            .await
            .map(|_| ())
    }
    pub async fn export_run(
        &self,
        run_id: RunId,
        maintenance_token: &str,
    ) -> Result<RunBundle, FabricError> {
        match self
            .fabric
            .execute(FabricCommand::ExportRun {
                tenant: self.tenant.clone(),
                run_id,
                maintenance_token: maintenance_token.into(),
            })
            .await?
        {
            FabricReply::RunBundle(bundle) => Ok(bundle),
            _ => Err(FabricError::Conflict),
        }
    }
    pub fn validate_import(
        &self,
        bundle: &RunBundle,
        registry: &OperatorRegistry,
        resources: &AgentResources,
        policy: Option<&HostPolicy>,
    ) -> Result<GraphRun, FabricError> {
        if bundle.tenant != self.tenant || !Arc::ptr_eq(&self.fabric, &resources.fabric) {
            return Err(FabricError::Conflict);
        }
        let saved = bundle.graph_run()?;
        saved.validate_resources(registry, resources)?;
        if let Some(policy) = policy {
            let mut constrained = saved.definition.clone();
            constrained.apply_host_policy(policy)?;
            if constrained != saved.definition {
                return Err(FabricError::Invalid(
                    "import requires reauthorization under current host policy".into(),
                ));
            }
        } else if saved.definition.binding_snapshot.is_some() {
            return Err(FabricError::Invalid("current host policy required".into()));
        }
        Ok(saved)
    }
    /// Revalidates immediately before atomic publication. Leaves the destination paused.
    pub async fn import_run(
        &self,
        bundle: RunBundle,
        token: &str,
        registry: &OperatorRegistry,
        resources: &AgentResources,
        policy: Option<&HostPolicy>,
    ) -> Result<(), FabricError> {
        self.validate_import(&bundle, registry, resources, policy)?;
        self.fabric
            .execute(FabricCommand::ImportRun {
                bundle,
                maintenance_token: token.into(),
            })
            .await
            .map(|_| ())
    }
    pub async fn read_host_record(
        &self,
        namespace: &str,
        operation_id: &str,
    ) -> Result<Option<HostRecord>, FabricError> {
        match self
            .fabric
            .execute(FabricCommand::ReadCheckpoint(host_record_key(
                &self.tenant,
                namespace,
                operation_id,
            )))
            .await?
        {
            FabricReply::Checkpoint(record) => record
                .map(|r| serde_json::from_value(r.value).map_err(storage))
                .transpose(),
            _ => Err(FabricError::Conflict),
        }
    }
    /// Submit immutable idempotent host records in the same transaction as Fabric work.
    pub async fn commit_host_records(
        &self,
        transaction: FabricCommand,
        records: Vec<HostRecord>,
    ) -> Result<FabricReply, FabricError> {
        if records
            .iter()
            .any(|r| r.tenant != self.tenant || r.namespace.starts_with('$'))
        {
            return Err(FabricError::Invalid(
                "host record tenant/reserved namespace".into(),
            ));
        }
        self.fabric
            .execute(FabricCommand::HostTransact {
                records,
                completion: None,
                transaction: Box::new(transaction),
            })
            .await
    }
    pub async fn completion(&self, run_id: RunId) -> Result<Option<RunCompletion>, FabricError> {
        match self
            .fabric
            .execute(FabricCommand::ReadCheckpoint(host_record_key(
                &self.tenant,
                "$completion",
                &run_id.to_string(),
            )))
            .await?
        {
            FabricReply::Checkpoint(record) => record
                .map(|r| serde_json::from_value(r.value).map_err(storage))
                .transpose(),
            _ => Err(FabricError::Conflict),
        }
    }
    pub async fn list_completions(
        &self,
        after: u64,
        limit: usize,
    ) -> Result<CompletionPage, FabricError> {
        match self
            .fabric
            .execute(FabricCommand::ReadLog {
                log: completion_log(&self.tenant),
                after,
                limit,
            })
            .await?
        {
            FabricReply::Log(page) => {
                let Some(page) = page else {
                    return Ok(CompletionPage {
                        through: 0,
                        items: vec![],
                    });
                };
                Ok(CompletionPage {
                    through: page.through,
                    items: page
                        .items
                        .into_iter()
                        .enumerate()
                        .map(|(i, value)| {
                            Ok((
                                after + i as u64 + 1,
                                serde_json::from_value(value).map_err(storage)?,
                            ))
                        })
                        .collect::<Result<_, FabricError>>()?,
                })
            }
            _ => Err(FabricError::Conflict),
        }
    }
    pub async fn completion_offset(&self, consumer: &str) -> Result<u64, FabricError> {
        match self
            .fabric
            .execute(FabricCommand::ReadCheckpoint(host_record_key(
                &self.tenant,
                "$completion-consumer",
                consumer,
            )))
            .await?
        {
            FabricReply::Checkpoint(record) => {
                Ok(record.map_or(0, |r| r.value.as_u64().unwrap_or(0)))
            }
            _ => Err(FabricError::Conflict),
        }
    }
    /// Call only after the consumer transaction has committed its unique completion ledger.
    pub async fn acknowledge_completion(
        &self,
        consumer: &str,
        sequence: u64,
        completion_id: &str,
    ) -> Result<(), FabricError> {
        if consumer.is_empty() || consumer.len() > MAX_KEY_BYTES || sequence == 0 {
            return Err(FabricError::Invalid("consumer/sequence".into()));
        }
        let page = self.list_completions(sequence - 1, 1).await?;
        if page
            .items
            .first()
            .is_none_or(|(_, r)| r.completion_id != completion_id)
        {
            return Err(FabricError::Conflict);
        }
        let key = host_record_key(&self.tenant, "$completion-consumer", consumer);
        {
            let FabricReply::Checkpoint(old) = self
                .fabric
                .execute(FabricCommand::ReadCheckpoint(key.clone()))
                .await?
            else {
                return Err(FabricError::Conflict);
            };
            let offset = old.as_ref().map_or(0, |r| r.value.as_u64().unwrap_or(0));
            if sequence <= offset {
                return Ok(());
            }
            if sequence != offset + 1 {
                return Err(FabricError::AckHole);
            }
            match self
                .fabric
                .execute(FabricCommand::Transact {
                    checkpoint: CheckpointWrite {
                        key: key.clone(),
                        expected: old.map(|r| r.version),
                        value: json!(sequence),
                    },
                    outbox: vec![],
                    delivery: None,
                })
                .await
            {
                Err(FabricError::Fenced) => Err(FabricError::Fenced),
                result => result.map(|_| ()),
            }
        }
    }
}
fn storage(error: impl std::fmt::Display) -> FabricError {
    FabricError::Storage(error.to_string())
}
