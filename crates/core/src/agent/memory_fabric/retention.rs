//! Maintenance transitions validate a complete plan before their infallible mutations.
use super::*;
use crate::{
    agent::graph::{GraphRun, event_log_key, public_journal_key, run_checkpoint_key as run_key},
    contracts::{BlobId, RunId},
};
use serde_json::{Value, json};
use std::collections::{BTreeSet, HashSet};

fn strings(value: &Value, found: &mut HashSet<String>) {
    match value {
        Value::String(s) => {
            found.insert(s.clone());
        }
        Value::Array(values) => {
            for value in values {
                strings(value, found);
            }
        }
        Value::Object(values) => {
            for value in values.values() {
                strings(value, found);
            }
        }
        _ => {}
    }
}
impl FabricState {
    pub(super) fn ensure_available(&self, event: &FabricEvent) -> Result<(), FabricError> {
        if self
            .checkpoints
            .get(&run_key(&event.envelope.tenant_id, event.envelope.run_id))
            .is_some_and(|c| c.value["retired"] == true)
        {
            return Err(invalid("retention_expired"));
        }
        Ok(())
    }
    pub(super) fn retire_run(
        &mut self,
        tenant: &str,
        run: RunId,
        now: i64,
        retention: i64,
        dry_run: bool,
    ) -> Result<CollectionReport, FabricError> {
        key(tenant)?;
        if retention <= 0 {
            return Err(invalid("retention bounds"));
        }
        let identity = run_key(tenant, run);
        let saved = self
            .checkpoints
            .get(&identity)
            .ok_or(FabricError::NotFound)?;
        if saved.value["retired"] == true {
            return Ok(CollectionReport {
                dry_run,
                ..Default::default()
            });
        }
        let record: GraphRun = serde_json::from_value(saved.value.clone()).map_err(storage)?;
        if record.tenant != tenant || record.run_id != run {
            return Err(FabricError::Conflict);
        }
        if !record.is_terminal() {
            return Err(invalid("run_not_terminal"));
        }
        if !record.cleanup_complete {
            return Err(invalid("run_cleanup_pending"));
        }
        let matches_run = |event: &FabricEvent| {
            event.envelope.tenant_id == tenant && event.envelope.run_id == run
        };
        let event_ids: HashSet<_> = self
            .events
            .iter()
            .filter(|e| matches_run(&e.event))
            .map(|e| e.event.envelope.event_id)
            .collect();
        let latest = self
            .events
            .iter()
            .filter(|e| matches_run(&e.event))
            .map(|e| e.event.envelope.occurred_at_ms)
            .chain(
                self.receipts
                    .iter()
                    .filter(|r| event_ids.contains(&r.key.event_id))
                    .map(|r| r.committed_at_ms),
            )
            .max()
            .ok_or(FabricError::Conflict)?;
        if now < latest || now.saturating_sub(latest) < retention {
            return Err(invalid("retention_not_elapsed"));
        }
        if self
            .events
            .iter()
            .any(|e| matches_run(&e.event) && !e.visible)
        {
            return Err(invalid("pending_outbox"));
        }
        let consumer_prefix = format!("{identity}:");
        let owned_subscription =
            |s: &StoredSubscription| s.spec.consumer.starts_with(&consumer_prefix);
        for (index, subscription) in self.subscriptions.iter().enumerate() {
            if subscription
                .lease
                .as_ref()
                .is_some_and(|l| l.offset > subscription.offset && l.expires_at_ms > now)
                && (owned_subscription(subscription)
                    || self
                        .stream(index)
                        .nth(subscription.offset as usize)
                        .is_some_and(|i| matches_run(&i.event)))
            {
                return Err(FabricError::Fenced);
            }
            // Maintenance must also see facts behind another run's outbox barrier.
            // Delivery replay hides that suffix, but its unacknowledged references
            // still prevent retirement.
            if self
                .events
                .iter()
                .filter(|stored| {
                    let event = &stored.event;
                    event.topic == subscription.spec.topic
                        && subscription
                            .spec
                            .event_type
                            .as_ref()
                            .is_none_or(|kind| kind == &event.envelope.event_type)
                        && subscription
                            .spec
                            .tenant
                            .as_ref()
                            .is_none_or(|tenant| tenant == &event.envelope.tenant_id)
                })
                .skip(usize::try_from(subscription.offset).unwrap_or(usize::MAX))
                .any(|i| matches_run(&i.event))
            {
                return Err(invalid("unacknowledged_run_events"));
            }
        }
        let public_key = public_journal_key(tenant, run);
        let logs = [event_log_key(tenant, run), format!("{public_key}:events")];
        let mut owned = HashSet::from([identity.clone(), public_key]);
        if tenant == "local" {
            owned.insert(format!("graph-admission:{run}"));
        }
        let wait_index = format!(
            "graph-waits:{}",
            crate::contracts::events::EventId::stable(
                "graph-waits",
                &json!([tenant, run]).to_string()
            )
        );
        if let Some(index) = self.checkpoints.get(&wait_index) {
            let registrations = index
                .value
                .as_object()
                .ok_or_else(|| invalid("wait index"))?;
            for (registration, _) in registrations {
                let cleaned = format!("{registration}:cleaned");
                if !self
                    .checkpoints
                    .get(&cleaned)
                    .is_some_and(|c| c.value == true)
                {
                    return Err(invalid("wait_cleanup_pending"));
                }
                owned.insert(cleaned);
                owned.insert(format!("{registration}:wake"));
            }
            owned.insert(wait_index);
        }
        for (key, checkpoint) in &self.checkpoints {
            if (key.starts_with("graph-wait-subscription:")
                || key.starts_with("graph-job-binding:"))
                && checkpoint.value["run"] == json!(run)
                && checkpoint.value["tenant"] == tenant
            {
                owned.insert(key.clone());
            }
        }
        // Protect checkpoint references owned by other live records or facts.
        let mut protected = HashSet::new();
        for (key, checkpoint) in &self.checkpoints {
            if !owned.contains(key) && !key.starts_with("pipeline-artifact:") {
                strings(&checkpoint.value, &mut protected);
            }
        }
        for event in self.events.iter().filter(|e| !matches_run(&e.event)) {
            strings(&event.event.envelope.payload, &mut protected);
        }
        for (key, log) in &self.logs {
            if !logs.contains(key) {
                strings(&serde_json::to_value(log).map_err(storage)?, &mut protected);
            }
        }
        for receipt in self
            .receipts
            .iter()
            .filter(|r| !event_ids.contains(&r.key.event_id))
        {
            strings(
                &serde_json::to_value(receipt).map_err(storage)?,
                &mut protected,
            );
        }
        loop {
            let previous = protected.len();
            for key in protected.clone() {
                if let Some(checkpoint) = self.checkpoints.get(&key) {
                    strings(&checkpoint.value, &mut protected);
                }
            }
            if protected.len() == previous {
                break;
            }
        }
        if protected.contains(&identity) {
            return Err(invalid("run_still_referenced"));
        }
        owned.retain(|key| !protected.contains(key));
        let report = CollectionReport {
            checkpoints: owned.iter().filter(|k| **k != identity).count(),
            logs: logs
                .iter()
                .filter(|log| self.logs.contains_key(*log))
                .count(),
            subscriptions: self
                .subscriptions
                .iter()
                .filter(|s| owned_subscription(s))
                .count(),
            receipts: self
                .receipts
                .iter()
                .filter(|r| event_ids.contains(&r.key.event_id))
                .count(),
            dead_letters: self
                .dead_letters
                .iter()
                .filter(|d| event_ids.contains(&d.event_id))
                .count(),
            event_payloads: self
                .events
                .iter()
                .filter(|e| matches_run(&e.event) && !e.retired)
                .count(),
            dry_run,
            ..Default::default()
        };
        let version = saved
            .version
            .checked_add(1)
            .ok_or_else(|| invalid("checkpoint version overflow"))?;
        if dry_run {
            return Ok(report);
        }
        // No fallible operations after this boundary. Event slots and ids remain stable.
        self.schema_version = 3;
        for key in owned {
            self.checkpoints.remove(&key);
        }
        self.checkpoints.insert(
            identity,
            Arc::new(FabricCheckpoint {
                version,
                value: json!({"retired":true,"tenant_id":tenant,"run_id":run,"retired_at_ms":now}),
            }),
        );
        for log in logs {
            self.logs.remove(&log);
        }
        self.subscriptions.retain(|s| !owned_subscription(s));
        self.receipts
            .retain(|r| !event_ids.contains(&r.key.event_id));
        self.dead_letters
            .retain(|d| !event_ids.contains(&d.event_id));
        for event in self.events.iter_mut().filter(|e| matches_run(&e.event)) {
            let event = Arc::make_mut(event);
            event.retired = true;
            event.event.envelope.payload = json!({});
            event.event.payload_schema = json!(true);
        }
        Ok(report)
    }

    pub(super) fn artifact_roots(&self, external: &[BlobId]) -> Result<Vec<BlobId>, FabricError> {
        let mut found = HashSet::new();
        for (key, checkpoint) in &self.checkpoints {
            if !key.starts_with("pipeline-artifact:") {
                strings(&checkpoint.value, &mut found);
            }
        }
        for value in [
            serde_json::to_value(&self.logs).map_err(storage)?,
            serde_json::to_value(&self.events).map_err(storage)?,
            serde_json::to_value(&self.receipts).map_err(storage)?,
            serde_json::to_value(&self.dead_letters).map_err(storage)?,
        ] {
            strings(&value, &mut found);
        }
        found.extend(external.iter().map(ToString::to_string));
        Ok(found.into_iter().filter_map(|s| s.parse().ok()).collect())
    }

    pub(super) fn collect_artifacts(
        &mut self,
        now: i64,
        grace: i64,
        external: &[BlobId],
        dry_run: bool,
    ) -> Result<CollectionReport, FabricError> {
        use base64::{Engine, engine::general_purpose::STANDARD};
        if grace <= 0 || external.len() > 4096 {
            return Err(invalid("retention bounds"));
        }
        if self.subscriptions.iter().any(|s| {
            s.lease
                .as_ref()
                .is_some_and(|l| l.offset > s.offset && l.expires_at_ms > now)
        }) {
            return Err(FabricError::Fenced);
        }
        let manifests: std::collections::HashMap<BlobId, (&String, &Arc<FabricCheckpoint>)> = self
            .checkpoints
            .iter()
            .filter_map(|(k, v)| {
                k.strip_prefix("pipeline-artifact:")
                    .and_then(|s| s.parse::<BlobId>().ok())
                    .map(|id| (id, (k, v)))
            })
            .collect();
        let mut pending = self.artifact_roots(external)?;
        for (id, (_, checkpoint)) in &manifests {
            if checkpoint
                .value
                .get("schema_version")
                .is_some_and(|version| !version.is_null() && version.as_u64() != Some(1))
            {
                return Err(invalid("unsupported_artifact_version"));
            }
            if checkpoint.value["metadata"]["blob_id"] != json!(id) {
                return Err(invalid("artifact identity mismatch"));
            }
            let created = checkpoint.value["metadata"]["created_at_ms"]
                .as_i64()
                .ok_or_else(|| invalid("artifact metadata"))?;
            if now < created || now.saturating_sub(created) < grace {
                if checkpoint.value["complete"] == false {
                    return Err(invalid("active_artifact_upload"));
                }
                pending.push(*id);
            }
        }
        let mut marked = HashSet::new();
        while let Some(id) = pending.pop() {
            if !marked.insert(id) {
                continue;
            }
            let Some((key, checkpoint)) = manifests.get(&id) else {
                continue;
            };
            if checkpoint.value["complete"] == false {
                return Err(invalid("referenced_artifact_incomplete"));
            }
            if !checkpoint.value["metadata"]["media_type"]
                .as_str()
                .is_some_and(|t| t == "application/json" || t.ends_with("+json"))
            {
                continue;
            }
            let count = checkpoint.value["chunks"]
                .as_u64()
                .filter(|n| *n <= 64)
                .ok_or_else(|| invalid("artifact chunk bounds"))?;
            let mut bytes = Vec::new();
            for index in 0..count {
                let chunk = self
                    .checkpoints
                    .get(&format!("{key}:{index}"))
                    .ok_or_else(|| invalid("artifact gap"))?;
                bytes.extend(
                    STANDARD
                        .decode(
                            chunk
                                .value
                                .as_str()
                                .ok_or_else(|| invalid("artifact encoding"))?,
                        )
                        .map_err(storage)?,
                );
                if bytes.len() > 16 * 1024 * 1024 {
                    return Err(invalid("artifact resource limit"));
                }
            }
            let value: Value = serde_json::from_slice(&bytes).map_err(storage)?;
            if checkpoint.value["metadata"]["size_bytes"].as_u64() != Some(bytes.len() as u64) {
                return Err(invalid("artifact size mismatch"));
            }
            let mut refs = HashSet::new();
            strings(&value, &mut refs);
            pending.extend(refs.into_iter().filter_map(|s| s.parse::<BlobId>().ok()));
        }
        let mut remove = BTreeSet::new();
        let mut report = CollectionReport {
            dry_run,
            ..Default::default()
        };
        for (id, (key, checkpoint)) in &manifests {
            if marked.contains(id) {
                continue;
            }
            report.artifacts += 1;
            report.artifact_bytes = report.artifact_bytes.saturating_add(
                checkpoint.value["metadata"]["size_bytes"]
                    .as_u64()
                    .ok_or_else(|| invalid("artifact size"))?,
            );
            remove.insert((*key).clone());
            remove.extend(
                self.checkpoints
                    .keys()
                    .filter(|k| k.starts_with(&format!("{key}:")))
                    .cloned(),
            );
        }
        // Legacy uploads could leave chunks without a manifest/creation time.
        // Observe them first; only a later maintenance pass can age them out.
        let mut orphans: std::collections::HashMap<BlobId, Vec<String>> = Default::default();
        for key in self.checkpoints.keys() {
            let Some((id, index)) = key
                .strip_prefix("pipeline-artifact:")
                .and_then(|key| key.split_once(':'))
            else {
                continue;
            };
            let (Ok(id), Ok(_)) = (id.parse::<BlobId>(), index.parse::<usize>()) else {
                continue;
            };
            if !manifests.contains_key(&id) {
                orphans.entry(id).or_default().push(key.clone());
            }
        }
        let mut quarantine = Vec::new();
        for (id, chunks) in orphans {
            if marked.contains(&id) {
                return Err(invalid("referenced_artifact_incomplete"));
            }
            let marker = format!("pipeline-artifact-gc:{id}");
            if let Some(saved) = self.checkpoints.get(&marker) {
                let observed = saved.value["observed_at_ms"]
                    .as_i64()
                    .ok_or_else(|| invalid("invalid artifact quarantine"))?;
                if now.saturating_sub(observed) >= grace {
                    report.artifacts += 1;
                    remove.extend(chunks);
                    remove.insert(marker);
                }
            } else {
                quarantine.push(marker);
            }
        }
        // Successful retries supersede quarantine markers.
        for id in manifests.keys() {
            let marker = format!("pipeline-artifact-gc:{id}");
            if self.checkpoints.contains_key(&marker) {
                remove.insert(marker);
            }
        }
        report.checkpoints = remove.len();
        report.quarantined_artifacts = quarantine.len();
        if !dry_run {
            for key in remove {
                self.checkpoints.remove(&key);
            }
            for key in quarantine {
                self.checkpoints.insert(
                    key,
                    Arc::new(FabricCheckpoint {
                        version: 0,
                        value: json!({"observed_at_ms":now}),
                    }),
                );
            }
        }
        Ok(report)
    }
}
