use std::{
    collections::HashMap,
    fs,
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use agent_core::memory::{
    CreateMemoryWriteProposal, ForgetMemory, MemoryApprovalId, MemoryApprovalStatus,
    MemoryComponentDescriptor, MemoryError, MemoryFuture, MemoryId, MemoryKind, MemoryListQuery,
    MemoryLocator, MemoryPage, MemoryProposalStore, MemoryRecord, MemoryRetrieveRequest,
    MemoryRetriever, MemoryStore, MemoryWriteProposal, PutMemory, RankedMemory,
    ResolveMemoryWriteProposal, SupersedeMemory, content_text,
};
use rusqlite::{Connection, ErrorCode, OptionalExtension, TransactionBehavior, params};

const MAX_PAGE_SIZE: usize = 1_000;

#[derive(Debug, Clone)]
struct StoredMemory {
    record: MemoryRecord,
    active: bool,
    deleted: bool,
}

#[derive(Debug, Clone, Default)]
pub struct InMemoryMemoryStore {
    records: Arc<Mutex<HashMap<MemoryId, StoredMemory>>>,
    proposals: Arc<Mutex<HashMap<MemoryApprovalId, MemoryWriteProposal>>>,
}

impl InMemoryMemoryStore {
    fn lock(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, HashMap<MemoryId, StoredMemory>>, MemoryError> {
        self.records
            .lock()
            .map_err(|_| MemoryError::backend("in-memory memory store lock was poisoned"))
    }
}

impl MemoryProposalStore for InMemoryMemoryStore {
    fn descriptor(&self) -> MemoryComponentDescriptor {
        descriptor("memory-proposal:in-memory", "in_memory_proposal_store")
    }

    fn create(&self, command: CreateMemoryWriteProposal) -> MemoryFuture<'_, MemoryWriteProposal> {
        Box::pin(async move {
            let mut proposals = self
                .proposals
                .lock()
                .map_err(|_| MemoryError::backend("in-memory proposal lock was poisoned"))?;
            let id = command.proposal.approval_id;
            if let Some(existing) = proposals.get(&id) {
                if existing == &command.proposal {
                    return Ok(existing.clone());
                }
                return Err(MemoryError::Invalid(
                    "memory approval id was reused with different content".into(),
                ));
            }
            if command.proposal.status != MemoryApprovalStatus::Pending {
                return Err(MemoryError::Invalid(
                    "new memory write proposals must be pending".into(),
                ));
            }
            proposals.insert(id, command.proposal.clone());
            Ok(command.proposal)
        })
    }

    fn get_proposal(
        &self,
        approval_id: MemoryApprovalId,
    ) -> MemoryFuture<'_, Option<MemoryWriteProposal>> {
        Box::pin(async move {
            Ok(self
                .proposals
                .lock()
                .map_err(|_| MemoryError::backend("in-memory proposal lock was poisoned"))?
                .get(&approval_id)
                .cloned())
        })
    }

    fn resolve_proposal(
        &self,
        command: ResolveMemoryWriteProposal,
    ) -> MemoryFuture<'_, MemoryWriteProposal> {
        Box::pin(async move {
            validate_resolution(&command)?;
            let mut proposals = self
                .proposals
                .lock()
                .map_err(|_| MemoryError::backend("in-memory proposal lock was poisoned"))?;
            let proposal = proposals
                .get_mut(&command.approval_id)
                .ok_or(MemoryError::Invalid("memory approval was not found".into()))?;
            if proposal.status != MemoryApprovalStatus::Pending {
                if proposal.status == command.decision {
                    return Ok(proposal.clone());
                }
                return Err(MemoryError::Invalid(
                    "memory approval was already resolved differently".into(),
                ));
            }
            proposal.status = command.decision;
            proposal.resolved_at_ms = Some(command.resolved_at_ms);
            proposal.resolution_reason = command.reason;
            Ok(proposal.clone())
        })
    }

    fn pending_proposals(&self, limit: usize) -> MemoryFuture<'_, Vec<MemoryWriteProposal>> {
        Box::pin(async move {
            let mut pending = self
                .proposals
                .lock()
                .map_err(|_| MemoryError::backend("in-memory proposal lock was poisoned"))?
                .values()
                .filter(|proposal| proposal.status == MemoryApprovalStatus::Pending)
                .cloned()
                .collect::<Vec<_>>();
            pending
                .sort_by_key(|proposal| (proposal.created_at_ms, proposal.approval_id.to_string()));
            pending.truncate(limit.clamp(1, MAX_PAGE_SIZE));
            Ok(pending)
        })
    }
}

impl MemoryStore for InMemoryMemoryStore {
    fn descriptor(&self) -> MemoryComponentDescriptor {
        descriptor("memory:in-memory", "in_memory_store")
    }

    fn put(&self, command: PutMemory) -> MemoryFuture<'_, MemoryRecord> {
        Box::pin(async move {
            validate_record(&command.record)?;
            let mut records = self.lock()?;
            if let Some(existing) = records.get(&command.record.memory_id) {
                if existing.record == command.record && !existing.deleted {
                    return Ok(existing.record.clone());
                }
                return Err(MemoryError::AlreadyExists(command.record.memory_id));
            }
            records.insert(
                command.record.memory_id,
                StoredMemory {
                    record: command.record.clone(),
                    active: true,
                    deleted: false,
                },
            );
            Ok(command.record)
        })
    }

    fn get(&self, locator: MemoryLocator) -> MemoryFuture<'_, Option<MemoryRecord>> {
        Box::pin(async move {
            Ok(self.lock()?.get(&locator.memory_id).and_then(|stored| {
                (!stored.deleted
                    && locator
                        .version
                        .is_none_or(|version| version == stored.record.version))
                .then(|| stored.record.clone())
            }))
        })
    }

    fn list(&self, query: MemoryListQuery) -> MemoryFuture<'_, MemoryPage> {
        Box::pin(async move {
            let limit = query.limit.clamp(1, MAX_PAGE_SIZE);
            let mut records: Vec<_> = self
                .lock()?
                .values()
                .filter(|stored| stored.active && !stored.deleted)
                .map(|stored| stored.record.clone())
                .filter(|record| matches_query(record, &query))
                .collect();
            records.sort_by_key(|record| (record.created_at_ms, record.memory_id.to_string()));
            let has_more = records.len() > limit;
            records.truncate(limit);
            Ok(MemoryPage { records, has_more })
        })
    }

    fn supersede(&self, mut command: SupersedeMemory) -> MemoryFuture<'_, MemoryRecord> {
        Box::pin(async move {
            let mut records = self.lock()?;
            let existing = records
                .get_mut(&command.existing_id)
                .ok_or(MemoryError::NotFound(command.existing_id))?;
            if existing.deleted || !existing.active {
                return Err(MemoryError::NotFound(command.existing_id));
            }
            if existing.record.version != command.expected_version {
                return Err(MemoryError::VersionConflict {
                    expected: command.expected_version,
                    actual: existing.record.version,
                });
            }
            command.replacement.supersedes = Some(command.existing_id);
            command.replacement.version = existing.record.version.saturating_add(1);
            validate_record(&command.replacement)?;
            existing.active = false;
            records.insert(
                command.replacement.memory_id,
                StoredMemory {
                    record: command.replacement.clone(),
                    active: true,
                    deleted: false,
                },
            );
            Ok(command.replacement)
        })
    }

    fn forget(&self, command: ForgetMemory) -> MemoryFuture<'_, ()> {
        Box::pin(async move {
            let mut records = self.lock()?;
            let stored = records
                .get_mut(&command.memory_id)
                .ok_or(MemoryError::NotFound(command.memory_id))?;
            if let Some(expected) = command.expected_version
                && expected != stored.record.version
            {
                return Err(MemoryError::VersionConflict {
                    expected,
                    actual: stored.record.version,
                });
            }
            stored.deleted = true;
            stored.active = false;
            Ok(())
        })
    }
}

impl MemoryRetriever for InMemoryMemoryStore {
    fn descriptor(&self) -> MemoryComponentDescriptor {
        descriptor("memory:in-memory-keyword", "keyword_retriever")
    }

    fn retrieve(&self, request: MemoryRetrieveRequest) -> MemoryFuture<'_, Vec<RankedMemory>> {
        Box::pin(async move {
            let terms = query_terms(&request.query);
            let strategy = MemoryRetriever::descriptor(self);
            let mut ranked: Vec<_> = self
                .lock()?
                .values()
                .filter(|stored| stored.active && !stored.deleted)
                .map(|stored| stored.record.clone())
                .filter(|record| retrieve_filter(record, &request))
                .filter_map(|record| {
                    let text = content_text(&record.content).to_lowercase();
                    let matches = terms.iter().filter(|term| text.contains(*term)).count();
                    if !terms.is_empty() && matches == 0 {
                        return None;
                    }
                    let lexical = if terms.is_empty() {
                        0.0
                    } else {
                        matches as f32 / terms.len() as f32
                    };
                    let score = lexical * 0.7 + record.salience.clamp(0.0, 1.0) * 0.3;
                    Some(RankedMemory {
                        record,
                        score,
                        score_explanation: format!(
                            "keyword_matches={matches};query_terms={};salience_weight=0.3",
                            terms.len()
                        ),
                        strategy: strategy.clone(),
                    })
                })
                .collect();
            ranked.sort_by(|left, right| {
                right.score.total_cmp(&left.score).then_with(|| {
                    left.record
                        .memory_id
                        .to_string()
                        .cmp(&right.record.memory_id.to_string())
                })
            });
            ranked.truncate(request.top_k.clamp(1, MAX_PAGE_SIZE));
            Ok(ranked)
        })
    }
}

#[derive(Debug, Clone)]
pub struct SqliteMemoryStore {
    connection: Arc<Mutex<Connection>>,
    identity: String,
}

impl SqliteMemoryStore {
    pub async fn open(
        path: impl AsRef<Path>,
        identity: impl Into<String>,
    ) -> Result<Self, MemoryError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent).map_err(|error| {
                MemoryError::backend(format!("create memory store directory: {error}"))
            })?;
        }
        let migration_path = path.clone();
        let connection = run_blocking(move || {
            let connection = connect(&migration_path)?;
            migrate(&connection)?;
            Ok(connection)
        })
        .await?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
            identity: identity.into(),
        })
    }
}

impl MemoryStore for SqliteMemoryStore {
    fn descriptor(&self) -> MemoryComponentDescriptor {
        descriptor(&self.identity, "sqlite_store")
    }

    fn put(&self, command: PutMemory) -> MemoryFuture<'_, MemoryRecord> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            run_blocking(move || {
                validate_record(&command.record)?;
                let mut connection = lock_connection(&connection)?;
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(|error| sql_error("begin memory put", error))?;
                if let Some(existing) = load_record(&transaction, command.record.memory_id, true)? {
                    if existing == command.record {
                        return Ok(existing);
                    }
                    return Err(MemoryError::AlreadyExists(command.record.memory_id));
                }
                insert_record(&transaction, &command.record)?;
                transaction
                    .commit()
                    .map_err(|error| sql_error("commit memory put", error))?;
                Ok(command.record)
            })
            .await
        })
    }

    fn get(&self, locator: MemoryLocator) -> MemoryFuture<'_, Option<MemoryRecord>> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            run_blocking(move || {
                let connection = lock_connection(&connection)?;
                Ok(
                    load_record(&connection, locator.memory_id, false)?.filter(|record| {
                        locator
                            .version
                            .is_none_or(|version| version == record.version)
                    }),
                )
            })
            .await
        })
    }

    fn list(&self, query: MemoryListQuery) -> MemoryFuture<'_, MemoryPage> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            run_blocking(move || {
                let connection = lock_connection(&connection)?;
                let mut statement = connection
                    .prepare(
                        "SELECT record_json FROM memory_records
                         WHERE active = 1 AND deleted = 0
                         ORDER BY created_at_ms ASC, memory_id ASC",
                    )
                    .map_err(|error| sql_error("prepare memory list", error))?;
                let rows = statement
                    .query_map([], |row| row.get::<_, String>(0))
                    .map_err(|error| sql_error("query memory list", error))?;
                let limit = query.limit.clamp(1, MAX_PAGE_SIZE);
                let mut records = Vec::new();
                for row in rows {
                    let json = row.map_err(|error| sql_error("read memory list row", error))?;
                    let record: MemoryRecord = decode(&json)?;
                    if matches_query(&record, &query) {
                        records.push(record);
                    }
                }
                let has_more = records.len() > limit;
                records.truncate(limit);
                Ok(MemoryPage { records, has_more })
            })
            .await
        })
    }

    fn supersede(&self, mut command: SupersedeMemory) -> MemoryFuture<'_, MemoryRecord> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            run_blocking(move || {
                let mut connection = lock_connection(&connection)?;
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(|error| sql_error("begin memory supersede", error))?;
                let existing = load_record(&transaction, command.existing_id, false)?
                    .ok_or(MemoryError::NotFound(command.existing_id))?;
                if existing.version != command.expected_version {
                    return Err(MemoryError::VersionConflict {
                        expected: command.expected_version,
                        actual: existing.version,
                    });
                }
                command.replacement.supersedes = Some(existing.memory_id);
                command.replacement.version = existing.version.saturating_add(1);
                validate_record(&command.replacement)?;
                transaction
                    .execute(
                        "UPDATE memory_records SET active = 0 WHERE memory_id = ?1",
                        [existing.memory_id.to_string()],
                    )
                    .map_err(|error| sql_error("deactivate superseded memory", error))?;
                transaction
                    .execute(
                        "DELETE FROM memory_fts WHERE memory_id = ?1",
                        [existing.memory_id.to_string()],
                    )
                    .map_err(|error| sql_error("remove superseded memory index", error))?;
                insert_record(&transaction, &command.replacement)?;
                transaction
                    .commit()
                    .map_err(|error| sql_error("commit memory supersede", error))?;
                Ok(command.replacement)
            })
            .await
        })
    }

    fn forget(&self, command: ForgetMemory) -> MemoryFuture<'_, ()> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            run_blocking(move || {
                let mut connection = lock_connection(&connection)?;
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(|error| sql_error("begin memory forget", error))?;
                let existing = load_record(&transaction, command.memory_id, false)?
                    .ok_or(MemoryError::NotFound(command.memory_id))?;
                if let Some(expected) = command.expected_version
                    && expected != existing.version
                {
                    return Err(MemoryError::VersionConflict {
                        expected,
                        actual: existing.version,
                    });
                }
                transaction
                    .execute(
                        "UPDATE memory_records SET active = 0, deleted = 1 WHERE memory_id = ?1",
                        [command.memory_id.to_string()],
                    )
                    .map_err(|error| sql_error("forget memory", error))?;
                transaction
                    .execute(
                        "DELETE FROM memory_fts WHERE memory_id = ?1",
                        [command.memory_id.to_string()],
                    )
                    .map_err(|error| sql_error("remove forgotten memory index", error))?;
                transaction
                    .commit()
                    .map_err(|error| sql_error("commit memory forget", error))?;
                Ok(())
            })
            .await
        })
    }
}

impl MemoryProposalStore for SqliteMemoryStore {
    fn descriptor(&self) -> MemoryComponentDescriptor {
        descriptor(
            &format!("{}:proposals", self.identity),
            "sqlite_memory_proposal_store",
        )
    }

    fn create(&self, command: CreateMemoryWriteProposal) -> MemoryFuture<'_, MemoryWriteProposal> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            run_blocking(move || {
                if command.proposal.status != MemoryApprovalStatus::Pending {
                    return Err(MemoryError::Invalid(
                        "new memory write proposals must be pending".into(),
                    ));
                }
                let connection = lock_connection(&connection)?;
                let encoded = encode_proposal(&command.proposal)?;
                if let Some(existing_json) = connection
                    .query_row(
                        "SELECT proposal_json FROM memory_write_proposals WHERE approval_id = ?1",
                        [command.proposal.approval_id.to_string()],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()
                    .map_err(|error| sql_error("read replayed memory proposal", error))?
                {
                    let existing = decode_proposal(&existing_json)?;
                    if existing == command.proposal {
                        return Ok(existing);
                    }
                    return Err(MemoryError::Invalid(
                        "memory approval id was reused with different content".into(),
                    ));
                }
                connection
                    .execute(
                        "INSERT INTO memory_write_proposals (
                            approval_id, status, created_at_ms, resolved_at_ms, proposal_json
                         ) VALUES (?1, 'pending', ?2, NULL, ?3)",
                        params![
                            command.proposal.approval_id.to_string(),
                            command.proposal.created_at_ms,
                            encoded,
                        ],
                    )
                    .map_err(|error| sql_error("insert memory proposal", error))?;
                Ok(command.proposal)
            })
            .await
        })
    }

    fn get_proposal(
        &self,
        approval_id: MemoryApprovalId,
    ) -> MemoryFuture<'_, Option<MemoryWriteProposal>> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            run_blocking(move || {
                let connection = lock_connection(&connection)?;
                connection
                    .query_row(
                        "SELECT proposal_json FROM memory_write_proposals WHERE approval_id = ?1",
                        [approval_id.to_string()],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()
                    .map_err(|error| sql_error("read memory proposal", error))?
                    .map(|json| decode_proposal(&json))
                    .transpose()
            })
            .await
        })
    }

    fn resolve_proposal(
        &self,
        command: ResolveMemoryWriteProposal,
    ) -> MemoryFuture<'_, MemoryWriteProposal> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            validate_resolution(&command)?;
            run_blocking(move || {
                let mut connection = lock_connection(&connection)?;
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(|error| sql_error("begin memory proposal resolution", error))?;
                let json = transaction
                    .query_row(
                        "SELECT proposal_json FROM memory_write_proposals WHERE approval_id = ?1",
                        [command.approval_id.to_string()],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()
                    .map_err(|error| sql_error("load memory proposal for resolution", error))?
                    .ok_or_else(|| MemoryError::Invalid("memory approval was not found".into()))?;
                let mut proposal = decode_proposal(&json)?;
                if proposal.status != MemoryApprovalStatus::Pending {
                    if proposal.status == command.decision {
                        return Ok(proposal);
                    }
                    return Err(MemoryError::Invalid(
                        "memory approval was already resolved differently".into(),
                    ));
                }
                proposal.status = command.decision;
                proposal.resolved_at_ms = Some(command.resolved_at_ms);
                proposal.resolution_reason = command.reason;
                transaction
                    .execute(
                        "UPDATE memory_write_proposals SET status = ?1, resolved_at_ms = ?2,
                            proposal_json = ?3 WHERE approval_id = ?4",
                        params![
                            approval_status_key(proposal.status),
                            proposal.resolved_at_ms,
                            encode_proposal(&proposal)?,
                            proposal.approval_id.to_string(),
                        ],
                    )
                    .map_err(|error| sql_error("resolve memory proposal", error))?;
                transaction
                    .commit()
                    .map_err(|error| sql_error("commit memory proposal resolution", error))?;
                Ok(proposal)
            })
            .await
        })
    }

    fn pending_proposals(&self, limit: usize) -> MemoryFuture<'_, Vec<MemoryWriteProposal>> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            run_blocking(move || {
                let connection = lock_connection(&connection)?;
                let mut statement = connection
                    .prepare(
                        "SELECT proposal_json FROM memory_write_proposals
                         WHERE status = 'pending' ORDER BY created_at_ms, approval_id LIMIT ?1",
                    )
                    .map_err(|error| sql_error("prepare pending memory proposals", error))?;
                let rows = statement
                    .query_map(
                        [i64::try_from(limit.clamp(1, MAX_PAGE_SIZE)).unwrap_or(i64::MAX)],
                        |row| row.get::<_, String>(0),
                    )
                    .map_err(|error| sql_error("query pending memory proposals", error))?;
                rows.map(|row| {
                    decode_proposal(
                        &row.map_err(|error| sql_error("read pending memory proposal", error))?,
                    )
                })
                .collect()
            })
            .await
        })
    }
}

impl MemoryRetriever for SqliteMemoryStore {
    fn descriptor(&self) -> MemoryComponentDescriptor {
        descriptor(&format!("{}:fts", self.identity), "sqlite_fts_retriever")
    }

    fn retrieve(&self, request: MemoryRetrieveRequest) -> MemoryFuture<'_, Vec<RankedMemory>> {
        let connection = Arc::clone(&self.connection);
        let strategy = MemoryRetriever::descriptor(self);
        Box::pin(async move {
            run_blocking(move || {
                let connection = lock_connection(&connection)?;
                let terms = query_terms(&request.query);
                let mut candidates = Vec::<(MemoryRecord, f32, String)>::new();
                if terms.is_empty() {
                    let mut statement = connection
                        .prepare(
                            "SELECT record_json FROM memory_records
                             WHERE active = 1 AND deleted = 0
                             ORDER BY created_at_ms DESC LIMIT 1000",
                        )
                        .map_err(|error| sql_error("prepare recent memory retrieval", error))?;
                    let rows = statement
                        .query_map([], |row| row.get::<_, String>(0))
                        .map_err(|error| sql_error("query recent memory retrieval", error))?;
                    for row in rows {
                        let record: MemoryRecord =
                            decode(&row.map_err(|error| sql_error("read recent memory", error))?)?;
                        candidates.push((
                            record.clone(),
                            record.salience,
                            "empty_query;salience".into(),
                        ));
                    }
                } else {
                    let expression = terms
                        .iter()
                        .map(|term| format!("\"{}\"", term.replace('"', "\"\"")))
                        .collect::<Vec<_>>()
                        .join(" OR ");
                    let mut statement = connection
                        .prepare(
                            "SELECT r.record_json, bm25(memory_fts)
                             FROM memory_fts
                             JOIN memory_records r ON r.memory_id = memory_fts.memory_id
                             WHERE memory_fts MATCH ?1 AND r.active = 1 AND r.deleted = 0
                             ORDER BY bm25(memory_fts) ASC LIMIT 1000",
                        )
                        .map_err(|error| sql_error("prepare FTS memory retrieval", error))?;
                    let rows = statement
                        .query_map([expression], |row| {
                            Ok((row.get::<_, String>(0)?, row.get::<_, f64>(1)?))
                        })
                        .map_err(|error| sql_error("query FTS memory retrieval", error))?;
                    for row in rows {
                        let (json, rank) =
                            row.map_err(|error| sql_error("read FTS memory", error))?;
                        let record: MemoryRecord = decode(&json)?;
                        let lexical = (1.0 / (1.0 + rank.abs())) as f32;
                        let score = lexical * 0.8 + record.salience.clamp(0.0, 1.0) * 0.2;
                        candidates.push((
                            record,
                            score,
                            format!("bm25={rank:.6};lexical_weight=0.8;salience_weight=0.2"),
                        ));
                    }
                }
                let mut ranked: Vec<_> = candidates
                    .into_iter()
                    .filter(|(record, _, _)| retrieve_filter(record, &request))
                    .map(|(record, score, explanation)| RankedMemory {
                        record,
                        score,
                        score_explanation: explanation,
                        strategy: strategy.clone(),
                    })
                    .collect();
                ranked.sort_by(|left, right| {
                    right.score.total_cmp(&left.score).then_with(|| {
                        left.record
                            .memory_id
                            .to_string()
                            .cmp(&right.record.memory_id.to_string())
                    })
                });
                ranked.truncate(request.top_k.clamp(1, MAX_PAGE_SIZE));
                Ok(ranked)
            })
            .await
        })
    }
}

fn descriptor(identity: &str, kind: &str) -> MemoryComponentDescriptor {
    MemoryComponentDescriptor {
        identity: identity.into(),
        kind: kind.into(),
        version: env!("CARGO_PKG_VERSION").into(),
    }
}

fn validate_record(record: &MemoryRecord) -> Result<(), MemoryError> {
    if record.scope.0.trim().is_empty() {
        return Err(MemoryError::Invalid("scope must not be empty".into()));
    }
    if record.content.is_empty() || content_text(&record.content).trim().is_empty() {
        return Err(MemoryError::Invalid(
            "text content must not be empty".into(),
        ));
    }
    if !(0.0..=1.0).contains(&record.confidence) || !(0.0..=1.0).contains(&record.salience) {
        return Err(MemoryError::Invalid(
            "confidence and salience must be between zero and one".into(),
        ));
    }
    if record.version == 0 {
        return Err(MemoryError::Invalid("version must be positive".into()));
    }
    if record.source_refs.is_empty() {
        return Err(MemoryError::Invalid("source refs must not be empty".into()));
    }
    Ok(())
}

fn matches_query(record: &MemoryRecord, query: &MemoryListQuery) -> bool {
    (query.scopes.is_empty() || query.scopes.contains(&record.scope))
        && (query.kinds.is_empty() || query.kinds.contains(&record.kind))
        && query
            .after_created_at_ms
            .is_none_or(|after| record.created_at_ms > after)
}

fn retrieve_filter(record: &MemoryRecord, request: &MemoryRetrieveRequest) -> bool {
    (request.scopes.is_empty() || request.scopes.contains(&record.scope))
        && (request.kinds.is_empty() || request.kinds.contains(&record.kind))
        && record
            .expires_at_ms
            .is_none_or(|expires| expires > request.now_ms)
}

fn query_terms(query: &str) -> Vec<String> {
    let mut terms: Vec<_> = query
        .split(|character: char| !character.is_alphanumeric())
        .map(str::to_lowercase)
        .filter(|term| !term.is_empty())
        .collect();
    terms.sort();
    terms.dedup();
    terms
}

fn connect(path: &Path) -> Result<Connection, MemoryError> {
    let connection =
        Connection::open(path).map_err(|error| sql_error("open memory database", error))?;
    connection
        .busy_timeout(Duration::from_secs(5))
        .map_err(|error| sql_error("set memory database timeout", error))?;
    connection
        .execute_batch("PRAGMA foreign_keys = ON; PRAGMA journal_mode = WAL;")
        .map_err(|error| sql_error("configure memory database", error))?;
    Ok(connection)
}

fn lock_connection(
    connection: &Mutex<Connection>,
) -> Result<std::sync::MutexGuard<'_, Connection>, MemoryError> {
    connection
        .lock()
        .map_err(|_| MemoryError::backend("SQLite memory connection lock was poisoned"))
}

fn migrate(connection: &Connection) -> Result<(), MemoryError> {
    connection
        .execute_batch(
            "BEGIN IMMEDIATE;
             CREATE TABLE IF NOT EXISTS memory_records (
                memory_id TEXT PRIMARY KEY NOT NULL,
                scope TEXT NOT NULL,
                kind TEXT NOT NULL,
                version INTEGER NOT NULL,
                expires_at_ms INTEGER,
                created_at_ms INTEGER NOT NULL,
                active INTEGER NOT NULL,
                deleted INTEGER NOT NULL,
                record_json TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS memory_scope_created_idx
                ON memory_records(scope, active, created_at_ms);
             CREATE VIRTUAL TABLE IF NOT EXISTS memory_fts USING fts5(
                content, memory_id UNINDEXED
             );
             CREATE TABLE IF NOT EXISTS memory_write_proposals (
                approval_id TEXT PRIMARY KEY NOT NULL,
                status TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                resolved_at_ms INTEGER,
                proposal_json TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS memory_write_proposals_pending_idx
                ON memory_write_proposals(status, created_at_ms);
             COMMIT;",
        )
        .map_err(|error| sql_error("create memory schema", error))
}

fn validate_resolution(command: &ResolveMemoryWriteProposal) -> Result<(), MemoryError> {
    if !matches!(
        command.decision,
        MemoryApprovalStatus::Approved
            | MemoryApprovalStatus::Denied
            | MemoryApprovalStatus::Expired
    ) {
        return Err(MemoryError::Invalid(
            "memory approval resolution must be approved, denied, or expired".into(),
        ));
    }
    Ok(())
}

const fn approval_status_key(status: MemoryApprovalStatus) -> &'static str {
    match status {
        MemoryApprovalStatus::Pending => "pending",
        MemoryApprovalStatus::Approved => "approved",
        MemoryApprovalStatus::Denied => "denied",
        MemoryApprovalStatus::Expired => "expired",
    }
}

fn encode_proposal(proposal: &MemoryWriteProposal) -> Result<String, MemoryError> {
    serde_json::to_string(proposal)
        .map_err(|error| MemoryError::backend(format!("encode memory proposal: {error}")))
}

fn decode_proposal(json: &str) -> Result<MemoryWriteProposal, MemoryError> {
    serde_json::from_str(json)
        .map_err(|error| MemoryError::backend(format!("decode memory proposal: {error}")))
}

fn insert_record(connection: &Connection, record: &MemoryRecord) -> Result<(), MemoryError> {
    let result = connection.execute(
        "INSERT INTO memory_records (
            memory_id, scope, kind, version, expires_at_ms, created_at_ms,
            active, deleted, record_json
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1, 0, ?7)",
        params![
            record.memory_id.to_string(),
            record.scope.0,
            memory_kind_key(record.kind),
            i64::try_from(record.version)
                .map_err(|_| MemoryError::Invalid("version is too large".into()))?,
            record.expires_at_ms,
            record.created_at_ms,
            encode(record)?,
        ],
    );
    match result {
        Ok(_) => {}
        Err(error) if is_constraint_violation(&error) => {
            return Err(MemoryError::AlreadyExists(record.memory_id));
        }
        Err(error) => return Err(sql_error("insert memory", error)),
    }
    connection
        .execute(
            "INSERT INTO memory_fts (content, memory_id) VALUES (?1, ?2)",
            params![content_text(&record.content), record.memory_id.to_string()],
        )
        .map_err(|error| sql_error("index memory", error))?;
    Ok(())
}

fn load_record(
    connection: &Connection,
    memory_id: MemoryId,
    include_deleted: bool,
) -> Result<Option<MemoryRecord>, MemoryError> {
    let sql = if include_deleted {
        "SELECT record_json FROM memory_records WHERE memory_id = ?1"
    } else {
        "SELECT record_json FROM memory_records WHERE memory_id = ?1 AND deleted = 0"
    };
    let json: Option<String> = connection
        .query_row(sql, [memory_id.to_string()], |row| row.get(0))
        .optional()
        .map_err(|error| sql_error("read memory", error))?;
    json.map(|json| decode(&json)).transpose()
}

const fn memory_kind_key(kind: MemoryKind) -> &'static str {
    match kind {
        MemoryKind::Semantic => "semantic",
        MemoryKind::Episodic => "episodic",
    }
}

fn encode(record: &MemoryRecord) -> Result<String, MemoryError> {
    serde_json::to_string(record)
        .map_err(|error| MemoryError::backend(format!("encode memory record: {error}")))
}

fn decode(json: &str) -> Result<MemoryRecord, MemoryError> {
    serde_json::from_str(json)
        .map_err(|error| MemoryError::backend(format!("decode memory record: {error}")))
}

fn is_constraint_violation(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(failure, _)
            if failure.code == ErrorCode::ConstraintViolation
    )
}

fn sql_error(operation: &str, error: rusqlite::Error) -> MemoryError {
    MemoryError::backend(format!("{operation}: {error}"))
}

async fn run_blocking<T: Send + 'static>(
    operation: impl FnOnce() -> Result<T, MemoryError> + Send + 'static,
) -> Result<T, MemoryError> {
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|error| MemoryError::backend(format!("memory worker failed: {error}")))?
}

#[cfg(test)]
mod tests {
    use agent_core::harness::ContentPart;
    use agent_core::memory::{MemoryScope, MemorySourceRef};
    use tempfile::tempdir;

    use super::*;

    fn record(text: &str, created_at_ms: i64) -> MemoryRecord {
        MemoryRecord {
            memory_id: MemoryId::new(),
            scope: MemoryScope("user:test".into()),
            kind: MemoryKind::Semantic,
            content: vec![ContentPart::text(text)],
            source_refs: vec![MemorySourceRef::ExplicitUserInput { run_id: None }],
            confidence: 1.0,
            salience: 0.8,
            version: 1,
            expires_at_ms: None,
            supersedes: None,
            created_at_ms,
        }
    }

    async fn assert_store_contract(store: &dyn MemoryStore) {
        let original = record("the preferred language is Rust", 10);
        store
            .put(PutMemory {
                record: original.clone(),
                expected_absent: true,
            })
            .await
            .expect("put should succeed");
        let loaded = store
            .get(MemoryLocator {
                memory_id: original.memory_id,
                version: Some(1),
            })
            .await
            .expect("get should succeed")
            .expect("memory should exist");
        assert_eq!(loaded, original);

        let replacement = record("the preferred language is modern Rust", 20);
        let replacement = store
            .supersede(SupersedeMemory {
                existing_id: original.memory_id,
                expected_version: 1,
                replacement,
            })
            .await
            .expect("supersede should succeed");
        assert_eq!(replacement.version, 2);
        assert_eq!(replacement.supersedes, Some(original.memory_id));

        store
            .forget(ForgetMemory {
                memory_id: replacement.memory_id,
                expected_version: Some(2),
            })
            .await
            .expect("forget should succeed");
        assert!(
            store
                .get(MemoryLocator {
                    memory_id: replacement.memory_id,
                    version: None,
                })
                .await
                .expect("get after forget should work")
                .is_none()
        );
    }

    async fn assert_proposal_contract(store: &dyn MemoryProposalStore) {
        let proposal = MemoryWriteProposal {
            approval_id: MemoryApprovalId::new(),
            candidate: agent_core::memory::MemoryCandidate {
                proposed: record("private preference", 30),
                sensitivity: agent_core::memory::MemorySensitivity::Private,
                extraction_reason: "explicit memory phrase".into(),
            },
            reason: "private content requires approval".into(),
            status: MemoryApprovalStatus::Pending,
            created_at_ms: 30,
            resolved_at_ms: None,
            resolution_reason: None,
        };
        store
            .create(CreateMemoryWriteProposal {
                proposal: proposal.clone(),
            })
            .await
            .expect("proposal should persist");
        store
            .create(CreateMemoryWriteProposal {
                proposal: proposal.clone(),
            })
            .await
            .expect("proposal replay should be idempotent");
        assert_eq!(
            store
                .pending_proposals(10)
                .await
                .expect("pending proposals should load"),
            vec![proposal.clone()]
        );
        let resolved = store
            .resolve_proposal(ResolveMemoryWriteProposal {
                approval_id: proposal.approval_id,
                decision: MemoryApprovalStatus::Approved,
                reason: Some("approved in test".into()),
                resolved_at_ms: 40,
            })
            .await
            .expect("proposal should resolve");
        assert_eq!(resolved.status, MemoryApprovalStatus::Approved);
        assert!(
            store
                .pending_proposals(10)
                .await
                .expect("pending query should work")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn in_memory_adapter_obeys_store_contract() {
        let store = InMemoryMemoryStore::default();
        assert_store_contract(&store).await;
        assert_proposal_contract(&store).await;
    }

    #[tokio::test]
    async fn sqlite_adapter_obeys_store_contract_and_retrieves_with_fts() {
        let directory = tempdir().expect("temporary directory should exist");
        let store = SqliteMemoryStore::open(directory.path().join("state.sqlite3"), "test")
            .await
            .expect("store should open");
        assert_proposal_contract(&store).await;
        let searchable = record("Mina uses a durable SQLite memory index", 1);
        store
            .put(PutMemory {
                record: searchable.clone(),
                expected_absent: true,
            })
            .await
            .expect("searchable memory should persist");
        let hits = store
            .retrieve(MemoryRetrieveRequest {
                query: "durable SQLite".into(),
                scopes: vec![MemoryScope("user:test".into())],
                kinds: vec![MemoryKind::Semantic],
                top_k: 5,
                now_ms: 100,
            })
            .await
            .expect("FTS retrieval should succeed");
        assert_eq!(hits[0].record.memory_id, searchable.memory_id);
        assert!(hits[0].score_explanation.contains("bm25"));

        assert_store_contract(&store).await;
        drop(store);
        let reopened = SqliteMemoryStore::open(directory.path().join("state.sqlite3"), "test")
            .await
            .expect("store should reopen");
        assert!(
            reopened
                .get(MemoryLocator {
                    memory_id: searchable.memory_id,
                    version: Some(1),
                })
                .await
                .expect("reopened get should work")
                .is_some()
        );
    }
}
