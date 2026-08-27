//! SQLite persistence adapter for the harness single-run state contract.

use std::{
    collections::{BTreeMap, HashMap},
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use agent_core::context::{
    ContextArtifact, ContextArtifactId, ContextArtifactRef, ContextArtifactStore,
    ContextComponentDescriptor, ContextError, ContextFuture, InvalidateContextArtifacts,
    PutContextArtifact, ReusableArtifactQuery,
};
use agent_core::harness::{
    ArchiveSession, BeginRunResult, BeginSessionRun, ContentPart, ConversationRole, CreateSession,
    FinalizeSessionRun, MAX_SESSION_PAGE_SIZE, MessageId, ObservedRunEvent, RunEvent, RunId,
    RunSnapshot, RunStatus, RunStore, RunStoreError, RunStoreFuture, SessionId, SessionMessage,
    SessionSnapshot, SessionStatus, SessionStore, SessionStoreError, SessionStoreFuture,
};
use rusqlite::{Connection, ErrorCode, OptionalExtension, TransactionBehavior, params};

const SCHEMA_VERSION: i64 = 3;
const MAX_EVENT_PAGE_SIZE: usize = 10_000;

#[derive(Debug, Clone)]
struct MemoryRun {
    snapshot: RunSnapshot,
    events: BTreeMap<u64, RunEvent>,
}

#[derive(Debug, Clone)]
struct MemorySessionRun {
    run_id: RunId,
    request_hash: String,
    context_through_ordinal: u64,
    finalized: bool,
}

#[derive(Debug, Clone)]
struct MemorySession {
    snapshot: SessionSnapshot,
    messages: BTreeMap<u64, SessionMessage>,
    runs: HashMap<String, MemorySessionRun>,
}

/// Process-local adapter useful for deterministic tests and embedders that do
/// not need restart durability. Production server composition uses SQLite.
#[derive(Debug, Clone, Default)]
pub struct InMemoryRunStore {
    runs: Arc<Mutex<HashMap<RunId, MemoryRun>>>,
    sessions: Arc<Mutex<HashMap<SessionId, MemorySession>>>,
}

impl RunStore for InMemoryRunStore {
    fn create_run(&self, snapshot: RunSnapshot) -> RunStoreFuture<'_, RunSnapshot> {
        Box::pin(async move {
            let mut runs = self.lock()?;
            if runs.contains_key(&snapshot.run_id) {
                return Err(RunStoreError::AlreadyExists(snapshot.run_id));
            }
            runs.insert(
                snapshot.run_id,
                MemoryRun {
                    snapshot: snapshot.clone(),
                    events: BTreeMap::new(),
                },
            );
            Ok(snapshot)
        })
    }

    fn get_run(&self, run_id: RunId) -> RunStoreFuture<'_, Option<RunSnapshot>> {
        Box::pin(async move { Ok(self.lock()?.get(&run_id).map(|run| run.snapshot.clone())) })
    }

    fn set_execution_manifest(
        &self,
        run_id: RunId,
        manifest: serde_json::Value,
    ) -> RunStoreFuture<'_, RunSnapshot> {
        Box::pin(async move {
            let mut runs = self.lock()?;
            let run = runs
                .get_mut(&run_id)
                .ok_or(RunStoreError::NotFound(run_id))?;
            if run.snapshot.last_seq != 0 || run.snapshot.status != RunStatus::Accepted {
                return Err(RunStoreError::backend(
                    "execution manifest can only be set before a run starts",
                ));
            }
            run.snapshot.execution_manifest = Some(manifest);
            Ok(run.snapshot.clone())
        })
    }

    fn append_event(
        &self,
        event: RunEvent,
        observed_at_ms: i64,
    ) -> RunStoreFuture<'_, RunSnapshot> {
        Box::pin(async move {
            let mut runs = self.lock()?;
            let run = runs
                .get_mut(&event.run_id)
                .ok_or(RunStoreError::NotFound(event.run_id))?;
            if let Some(existing) = run.events.get(&event.seq) {
                return if existing == &event {
                    Ok(run.snapshot.clone())
                } else {
                    Err(RunStoreError::EventConflict {
                        run_id: event.run_id,
                        seq: event.seq,
                    })
                };
            }
            run.snapshot.apply(&event, observed_at_ms)?;
            run.events.insert(event.seq, event);
            Ok(run.snapshot.clone())
        })
    }

    fn events_after(
        &self,
        run_id: RunId,
        after_seq: u64,
        limit: usize,
    ) -> RunStoreFuture<'_, Vec<RunEvent>> {
        Box::pin(async move {
            let runs = self.lock()?;
            let run = runs.get(&run_id).ok_or(RunStoreError::NotFound(run_id))?;
            Ok(run
                .events
                .range((after_seq.saturating_add(1))..)
                .take(limit.clamp(1, MAX_EVENT_PAGE_SIZE))
                .map(|(_, event)| event.clone())
                .collect())
        })
    }

    fn unfinished_runs(&self) -> RunStoreFuture<'_, Vec<RunSnapshot>> {
        Box::pin(async move {
            let mut snapshots: Vec<_> = self
                .lock()?
                .values()
                .filter(|run| !run.snapshot.is_terminal())
                .map(|run| run.snapshot.clone())
                .collect();
            snapshots.sort_by_key(|snapshot| snapshot.created_at_ms);
            Ok(snapshots)
        })
    }
}

impl InMemoryRunStore {
    fn lock(&self) -> Result<std::sync::MutexGuard<'_, HashMap<RunId, MemoryRun>>, RunStoreError> {
        self.runs
            .lock()
            .map_err(|_| RunStoreError::backend("in-memory run store lock was poisoned"))
    }

    fn lock_sessions(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, HashMap<SessionId, MemorySession>>, SessionStoreError>
    {
        self.sessions
            .lock()
            .map_err(|_| SessionStoreError::backend("in-memory session store lock was poisoned"))
    }
}

impl SessionStore for InMemoryRunStore {
    fn create_session(&self, command: CreateSession) -> SessionStoreFuture<'_, SessionSnapshot> {
        Box::pin(async move {
            let mut sessions = self.lock_sessions()?;
            if sessions.contains_key(&command.session_id) {
                return Err(SessionStoreError::AlreadyExists(command.session_id));
            }
            let snapshot = SessionSnapshot::new(&command);
            sessions.insert(
                command.session_id,
                MemorySession {
                    snapshot: snapshot.clone(),
                    messages: BTreeMap::new(),
                    runs: HashMap::new(),
                },
            );
            Ok(snapshot)
        })
    }

    fn list_sessions(
        &self,
        status: Option<SessionStatus>,
        limit: usize,
    ) -> SessionStoreFuture<'_, Vec<SessionSnapshot>> {
        Box::pin(async move {
            let sessions = self.lock_sessions()?;
            let mut snapshots: Vec<_> = sessions
                .values()
                .map(|session| session.snapshot.clone())
                .filter(|session| status.is_none_or(|status| session.status == status))
                .collect();
            snapshots.sort_by(|left, right| {
                right
                    .updated_at_ms
                    .cmp(&left.updated_at_ms)
                    .then_with(|| right.created_at_ms.cmp(&left.created_at_ms))
                    .then_with(|| {
                        left.session_id
                            .to_string()
                            .cmp(&right.session_id.to_string())
                    })
            });
            snapshots.truncate(limit.clamp(1, MAX_SESSION_PAGE_SIZE));
            Ok(snapshots)
        })
    }

    fn begin_run(
        &self,
        command: BeginSessionRun,
        initial_run: RunSnapshot,
    ) -> SessionStoreFuture<'_, BeginRunResult> {
        Box::pin(async move {
            let mut sessions = self.lock_sessions()?;
            let session = sessions
                .get_mut(&command.session_id)
                .ok_or(SessionStoreError::NotFound(command.session_id))?;

            if let Some(existing) = session.runs.get(&command.idempotency_key) {
                if existing.request_hash != command.request_hash {
                    return Err(SessionStoreError::IdempotencyConflict);
                }
                return Ok(BeginRunResult {
                    session: session.snapshot.clone(),
                    run_id: existing.run_id,
                    context_through_ordinal: existing.context_through_ordinal,
                    replayed: true,
                });
            }
            if session.snapshot.status == SessionStatus::Archived {
                return Err(SessionStoreError::Archived);
            }
            if session.snapshot.revision != command.expected_revision {
                return Err(SessionStoreError::RevisionConflict {
                    expected: command.expected_revision,
                    actual: session.snapshot.revision,
                });
            }
            if let Some(active) = session.snapshot.active_run_id {
                return Err(SessionStoreError::Busy(active));
            }
            if initial_run.run_id != command.run_id {
                return Err(SessionStoreError::backend(
                    "initial run id does not match begin command",
                ));
            }

            let mut runs = self
                .runs
                .lock()
                .map_err(|_| SessionStoreError::backend("in-memory run store lock was poisoned"))?;
            if runs.contains_key(&command.run_id) {
                return Err(SessionStoreError::backend("run id already exists"));
            }

            let context_through_ordinal = session.snapshot.next_message_ordinal.saturating_sub(1);
            let ordinal = session.snapshot.next_message_ordinal;
            session.messages.insert(
                ordinal,
                SessionMessage {
                    message_id: MessageId::new(),
                    session_id: command.session_id,
                    ordinal,
                    role: ConversationRole::User,
                    content: command.input,
                    source_run_id: Some(command.run_id),
                    created_at_ms: command.created_at_ms,
                },
            );
            session.snapshot.next_message_ordinal = ordinal.saturating_add(1);
            session.snapshot.active_run_id = Some(command.run_id);
            session.snapshot.revision = session.snapshot.revision.saturating_add(1);
            session.snapshot.updated_at_ms = command.created_at_ms;
            session.runs.insert(
                command.idempotency_key,
                MemorySessionRun {
                    run_id: command.run_id,
                    request_hash: command.request_hash,
                    context_through_ordinal,
                    finalized: false,
                },
            );
            runs.insert(
                initial_run.run_id,
                MemoryRun {
                    snapshot: initial_run,
                    events: BTreeMap::new(),
                },
            );

            Ok(BeginRunResult {
                session: session.snapshot.clone(),
                run_id: command.run_id,
                context_through_ordinal,
                replayed: false,
            })
        })
    }

    fn finalize_run(&self, command: FinalizeSessionRun) -> SessionStoreFuture<'_, SessionSnapshot> {
        Box::pin(async move {
            if !command.run.is_terminal() {
                return Err(SessionStoreError::RunNotTerminal);
            }
            let mut sessions = self.lock_sessions()?;
            let session = sessions
                .get_mut(&command.session_id)
                .ok_or(SessionStoreError::NotFound(command.session_id))?;
            let Some(session_run) = session
                .runs
                .values_mut()
                .find(|record| record.run_id == command.run.run_id)
            else {
                return Err(SessionStoreError::RunMismatch(command.run.run_id));
            };
            if session_run.finalized {
                return Ok(session.snapshot.clone());
            }
            if session.snapshot.active_run_id != Some(command.run.run_id) {
                return Err(SessionStoreError::RunMismatch(command.run.run_id));
            }

            if command.run.status == RunStatus::Completed {
                let ordinal = session.snapshot.next_message_ordinal;
                session.messages.insert(
                    ordinal,
                    SessionMessage {
                        message_id: MessageId::new(),
                        session_id: command.session_id,
                        ordinal,
                        role: ConversationRole::Assistant,
                        content: vec![ContentPart::text(command.run.output.clone())],
                        source_run_id: Some(command.run.run_id),
                        created_at_ms: command.finalized_at_ms,
                    },
                );
                session.snapshot.next_message_ordinal = ordinal.saturating_add(1);
            }
            session.snapshot.active_run_id = None;
            session.snapshot.revision = session.snapshot.revision.saturating_add(1);
            session.snapshot.updated_at_ms = command.finalized_at_ms;
            session_run.finalized = true;
            Ok(session.snapshot.clone())
        })
    }

    fn archive_session(&self, command: ArchiveSession) -> SessionStoreFuture<'_, SessionSnapshot> {
        Box::pin(async move {
            let mut sessions = self.lock_sessions()?;
            let session = sessions
                .get_mut(&command.session_id)
                .ok_or(SessionStoreError::NotFound(command.session_id))?;
            if session.snapshot.revision != command.expected_revision {
                return Err(SessionStoreError::RevisionConflict {
                    expected: command.expected_revision,
                    actual: session.snapshot.revision,
                });
            }
            if let Some(active) = session.snapshot.active_run_id {
                return Err(SessionStoreError::Busy(active));
            }
            session.snapshot.status = SessionStatus::Archived;
            session.snapshot.revision = session.snapshot.revision.saturating_add(1);
            session.snapshot.updated_at_ms = command.archived_at_ms;
            Ok(session.snapshot.clone())
        })
    }

    fn get_session(
        &self,
        session_id: SessionId,
    ) -> SessionStoreFuture<'_, Option<SessionSnapshot>> {
        Box::pin(async move {
            Ok(self
                .lock_sessions()?
                .get(&session_id)
                .map(|session| session.snapshot.clone()))
        })
    }

    fn messages(
        &self,
        session_id: SessionId,
        before: Option<u64>,
        limit: usize,
    ) -> SessionStoreFuture<'_, Vec<SessionMessage>> {
        Box::pin(async move {
            let sessions = self.lock_sessions()?;
            let session = sessions
                .get(&session_id)
                .ok_or(SessionStoreError::NotFound(session_id))?;
            let before = before.unwrap_or(u64::MAX);
            let mut messages: Vec<_> = session
                .messages
                .range(..before)
                .rev()
                .take(limit.clamp(1, MAX_EVENT_PAGE_SIZE))
                .map(|(_, message)| message.clone())
                .collect();
            messages.reverse();
            Ok(messages)
        })
    }

    fn pending_finalizations(&self) -> SessionStoreFuture<'_, Vec<(SessionId, RunSnapshot)>> {
        Box::pin(async move {
            let sessions = self.lock_sessions()?;
            let runs = self
                .runs
                .lock()
                .map_err(|_| SessionStoreError::backend("in-memory run store lock was poisoned"))?;
            let mut pending = Vec::new();
            for (session_id, session) in sessions.iter() {
                for session_run in session.runs.values() {
                    if session_run.finalized {
                        continue;
                    }
                    if let Some(run) = runs.get(&session_run.run_id)
                        && run.snapshot.is_terminal()
                    {
                        pending.push((*session_id, run.snapshot.clone()));
                    }
                }
            }
            Ok(pending)
        })
    }
}

/// A durable store backed by a local SQLite database.
///
/// All contracts owned by this adapter share one process-long connection.
/// Operations still run on Tokio's blocking pool, while the connection mutex
/// prevents concurrent access without repeatedly opening and closing SQLite.
#[derive(Debug, Clone)]
pub struct SqliteRunStore {
    path: Arc<PathBuf>,
    connection: Arc<Mutex<Connection>>,
}

impl SqliteRunStore {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, RunStoreError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent).map_err(|error| {
                RunStoreError::backend(format!(
                    "failed to create store directory {}: {error}",
                    parent.display()
                ))
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
            path: Arc::new(path),
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        self.path.as_path()
    }
}

impl RunStore for SqliteRunStore {
    fn create_run(&self, snapshot: RunSnapshot) -> RunStoreFuture<'_, RunSnapshot> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            run_blocking(move || {
                let connection = lock_run_connection(&connection)?;
                let snapshot_json = encode(&snapshot, "run snapshot")?;
                let result = connection.execute(
                    "INSERT INTO runs (
                        run_id, status, last_seq, created_at_ms, updated_at_ms, snapshot_json
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![
                        snapshot.run_id.to_string(),
                        status_key(snapshot.status),
                        to_sql_u64(snapshot.last_seq, "last_seq")?,
                        snapshot.created_at_ms,
                        snapshot.updated_at_ms,
                        snapshot_json,
                    ],
                );

                match result {
                    Ok(_) => Ok(snapshot),
                    Err(error) if is_constraint_violation(&error) => {
                        Err(RunStoreError::AlreadyExists(snapshot.run_id))
                    }
                    Err(error) => Err(sql_error("create run", error)),
                }
            })
            .await
        })
    }

    fn get_run(&self, run_id: RunId) -> RunStoreFuture<'_, Option<RunSnapshot>> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            run_blocking(move || {
                let connection = lock_run_connection(&connection)?;
                load_snapshot(&connection, run_id)
            })
            .await
        })
    }

    fn set_execution_manifest(
        &self,
        run_id: RunId,
        manifest: serde_json::Value,
    ) -> RunStoreFuture<'_, RunSnapshot> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            run_blocking(move || {
                let mut connection = lock_run_connection(&connection)?;
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(|error| sql_error("begin execution manifest update", error))?;
                let mut snapshot =
                    load_snapshot(&transaction, run_id)?.ok_or(RunStoreError::NotFound(run_id))?;
                if snapshot.last_seq != 0 || snapshot.status != RunStatus::Accepted {
                    return Err(RunStoreError::backend(
                        "execution manifest can only be set before a run starts",
                    ));
                }
                snapshot.execution_manifest = Some(manifest);
                transaction
                    .execute(
                        "UPDATE runs SET snapshot_json = ?1 WHERE run_id = ?2 AND last_seq = 0",
                        params![encode(&snapshot, "run snapshot")?, run_id.to_string()],
                    )
                    .map_err(|error| sql_error("set execution manifest", error))?;
                transaction
                    .commit()
                    .map_err(|error| sql_error("commit execution manifest", error))?;
                Ok(snapshot)
            })
            .await
        })
    }

    fn append_event(
        &self,
        event: RunEvent,
        observed_at_ms: i64,
    ) -> RunStoreFuture<'_, RunSnapshot> {
        self.append_events(vec![ObservedRunEvent::new(event, observed_at_ms)])
    }

    fn append_events(&self, events: Vec<ObservedRunEvent>) -> RunStoreFuture<'_, RunSnapshot> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            run_blocking(move || {
                let first = events
                    .first()
                    .ok_or_else(|| RunStoreError::backend("cannot append an empty event batch"))?;
                let run_id = first.event.run_id;
                if events.iter().any(|item| item.event.run_id != run_id) {
                    return Err(RunStoreError::backend(
                        "one event batch cannot contain multiple runs",
                    ));
                }

                let mut connection = lock_run_connection(&connection)?;
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(|error| sql_error("begin event batch transaction", error))?;
                let mut snapshot =
                    load_snapshot(&transaction, run_id)?.ok_or(RunStoreError::NotFound(run_id))?;
                let previous_seq = snapshot.last_seq;
                let mut inserted = false;

                for observed in events {
                    let event = observed.event;
                    let existing_json: Option<String> = transaction
                        .query_row(
                            "SELECT event_json FROM run_events WHERE run_id = ?1 AND seq = ?2",
                            params![event.run_id.to_string(), to_sql_u64(event.seq, "seq")?],
                            |row| row.get(0),
                        )
                        .optional()
                        .map_err(|error| sql_error("read existing event", error))?;

                    if let Some(existing_json) = existing_json {
                        let existing: RunEvent = decode(&existing_json, "run event")?;
                        if existing != event {
                            return Err(RunStoreError::EventConflict {
                                run_id: event.run_id,
                                seq: event.seq,
                            });
                        }
                        continue;
                    }

                    snapshot.apply(&event, observed.observed_at_ms)?;
                    transaction
                        .execute(
                            "INSERT INTO run_events (run_id, seq, observed_at_ms, event_json)
                             VALUES (?1, ?2, ?3, ?4)",
                            params![
                                event.run_id.to_string(),
                                to_sql_u64(event.seq, "seq")?,
                                observed.observed_at_ms,
                                encode(&event, "run event")?,
                            ],
                        )
                        .map_err(|error| sql_error("insert run event", error))?;
                    inserted = true;
                }

                if inserted {
                    let updated = transaction
                        .execute(
                            "UPDATE runs
                             SET status = ?1, last_seq = ?2, updated_at_ms = ?3, snapshot_json = ?4
                             WHERE run_id = ?5 AND last_seq = ?6",
                            params![
                                status_key(snapshot.status),
                                to_sql_u64(snapshot.last_seq, "last_seq")?,
                                snapshot.updated_at_ms,
                                encode(&snapshot, "run snapshot")?,
                                snapshot.run_id.to_string(),
                                to_sql_u64(previous_seq, "last_seq")?,
                            ],
                        )
                        .map_err(|error| sql_error("update run snapshot", error))?;
                    if updated != 1 {
                        return Err(RunStoreError::backend(
                            "run snapshot changed during event batch transaction",
                        ));
                    }
                }

                transaction
                    .commit()
                    .map_err(|error| sql_error("commit event batch", error))?;
                Ok(snapshot)
            })
            .await
        })
    }

    fn events_after(
        &self,
        run_id: RunId,
        after_seq: u64,
        limit: usize,
    ) -> RunStoreFuture<'_, Vec<RunEvent>> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            run_blocking(move || {
                let connection = lock_run_connection(&connection)?;
                if load_snapshot(&connection, run_id)?.is_none() {
                    return Err(RunStoreError::NotFound(run_id));
                }
                let page_size = limit.clamp(1, MAX_EVENT_PAGE_SIZE);
                let mut statement = connection
                    .prepare(
                        "SELECT event_json FROM run_events
                         WHERE run_id = ?1 AND seq > ?2
                         ORDER BY seq ASC LIMIT ?3",
                    )
                    .map_err(|error| sql_error("prepare event query", error))?;
                let rows = statement
                    .query_map(
                        params![
                            run_id.to_string(),
                            to_sql_u64(after_seq, "after_seq")?,
                            i64::try_from(page_size).unwrap_or(i64::MAX),
                        ],
                        |row| row.get::<_, String>(0),
                    )
                    .map_err(|error| sql_error("query run events", error))?;
                let mut events = Vec::new();
                for row in rows {
                    let json = row.map_err(|error| sql_error("read run event row", error))?;
                    events.push(decode(&json, "run event")?);
                }
                Ok(events)
            })
            .await
        })
    }

    fn unfinished_runs(&self) -> RunStoreFuture<'_, Vec<RunSnapshot>> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            run_blocking(move || {
                let connection = lock_run_connection(&connection)?;
                let mut statement = connection
                    .prepare(
                        "SELECT snapshot_json FROM runs
                         WHERE status NOT IN ('completed', 'failed', 'cancelled')
                         ORDER BY created_at_ms ASC",
                    )
                    .map_err(|error| sql_error("prepare unfinished run query", error))?;
                let rows = statement
                    .query_map([], |row| row.get::<_, String>(0))
                    .map_err(|error| sql_error("query unfinished runs", error))?;
                let mut snapshots = Vec::new();
                for row in rows {
                    let json = row.map_err(|error| sql_error("read unfinished run row", error))?;
                    snapshots.push(decode(&json, "run snapshot")?);
                }
                Ok(snapshots)
            })
            .await
        })
    }
}

impl SessionStore for SqliteRunStore {
    fn create_session(&self, command: CreateSession) -> SessionStoreFuture<'_, SessionSnapshot> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            run_session_blocking(move || {
                let connection = lock_session_connection(&connection)?;
                let snapshot = SessionSnapshot::new(&command);
                let result = connection.execute(
                    "INSERT INTO sessions (
                        session_id, status, revision, next_message_ordinal,
                        active_run_id, created_at_ms, updated_at_ms, snapshot_json
                     ) VALUES (?1, ?2, ?3, ?4, NULL, ?5, ?6, ?7)",
                    params![
                        snapshot.session_id.to_string(),
                        session_status_key(snapshot.status),
                        to_session_sql_u64(snapshot.revision, "revision")?,
                        to_session_sql_u64(snapshot.next_message_ordinal, "next_message_ordinal")?,
                        snapshot.created_at_ms,
                        snapshot.updated_at_ms,
                        session_encode(&snapshot, "session snapshot")?,
                    ],
                );
                match result {
                    Ok(_) => Ok(snapshot),
                    Err(error) if is_constraint_violation(&error) => {
                        Err(SessionStoreError::AlreadyExists(command.session_id))
                    }
                    Err(error) => Err(session_sql_error("create session", error)),
                }
            })
            .await
        })
    }

    fn list_sessions(
        &self,
        status: Option<SessionStatus>,
        limit: usize,
    ) -> SessionStoreFuture<'_, Vec<SessionSnapshot>> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            run_session_blocking(move || {
                let connection = lock_session_connection(&connection)?;
                let status = status.map(session_status_key);
                let mut statement = connection
                    .prepare(
                        "SELECT snapshot_json FROM sessions
                         WHERE (?1 IS NULL OR status = ?1)
                         ORDER BY updated_at_ms DESC, created_at_ms DESC, session_id ASC
                         LIMIT ?2",
                    )
                    .map_err(|error| session_sql_error("prepare session list", error))?;
                let rows = statement
                    .query_map(
                        params![
                            status,
                            i64::try_from(limit.clamp(1, MAX_SESSION_PAGE_SIZE))
                                .unwrap_or(i64::MAX),
                        ],
                        |row| row.get::<_, String>(0),
                    )
                    .map_err(|error| session_sql_error("query session list", error))?;
                let mut sessions = Vec::new();
                for row in rows {
                    let json =
                        row.map_err(|error| session_sql_error("read session list row", error))?;
                    sessions.push(session_decode(&json, "session snapshot")?);
                }
                Ok(sessions)
            })
            .await
        })
    }

    fn begin_run(
        &self,
        command: BeginSessionRun,
        initial_run: RunSnapshot,
    ) -> SessionStoreFuture<'_, BeginRunResult> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            run_session_blocking(move || {
                if initial_run.run_id != command.run_id {
                    return Err(SessionStoreError::backend(
                        "initial run id does not match begin command",
                    ));
                }
                let mut connection = lock_session_connection(&connection)?;
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(|error| session_sql_error("begin session run transaction", error))?;

                let replay: Option<(String, String, i64)> = transaction
                    .query_row(
                        "SELECT run_id, request_hash, context_through_ordinal
                         FROM session_runs
                         WHERE session_id = ?1 AND idempotency_key = ?2",
                        params![command.session_id.to_string(), command.idempotency_key],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )
                    .optional()
                    .map_err(|error| session_sql_error("read session idempotency", error))?;
                if let Some((run_id, request_hash, context_through)) = replay {
                    if request_hash != command.request_hash {
                        return Err(SessionStoreError::IdempotencyConflict);
                    }
                    let run_id = run_id
                        .parse()
                        .map_err(|_| SessionStoreError::backend("stored run id is invalid"))?;
                    let session = load_session_for_contract(&transaction, command.session_id)?
                        .ok_or(SessionStoreError::NotFound(command.session_id))?;
                    transaction
                        .commit()
                        .map_err(|error| session_sql_error("commit idempotent begin", error))?;
                    return Ok(BeginRunResult {
                        session,
                        run_id,
                        context_through_ordinal: from_session_sql_u64(
                            context_through,
                            "context_through_ordinal",
                        )?,
                        replayed: true,
                    });
                }

                let mut session = load_session_for_contract(&transaction, command.session_id)?
                    .ok_or(SessionStoreError::NotFound(command.session_id))?;
                if session.status == SessionStatus::Archived {
                    return Err(SessionStoreError::Archived);
                }
                if session.revision != command.expected_revision {
                    return Err(SessionStoreError::RevisionConflict {
                        expected: command.expected_revision,
                        actual: session.revision,
                    });
                }
                if let Some(active) = session.active_run_id {
                    return Err(SessionStoreError::Busy(active));
                }

                transaction
                    .execute(
                        "INSERT INTO runs (
                            run_id, status, last_seq, created_at_ms, updated_at_ms, snapshot_json
                         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                        params![
                            initial_run.run_id.to_string(),
                            status_key(initial_run.status),
                            to_session_sql_u64(initial_run.last_seq, "last_seq")?,
                            initial_run.created_at_ms,
                            initial_run.updated_at_ms,
                            session_encode(&initial_run, "run snapshot")?,
                        ],
                    )
                    .map_err(|error| {
                        if is_constraint_violation(&error) {
                            SessionStoreError::backend("run id already exists")
                        } else {
                            session_sql_error("create session run", error)
                        }
                    })?;

                let context_through_ordinal = session.next_message_ordinal.saturating_sub(1);
                let ordinal = session.next_message_ordinal;
                let message = SessionMessage {
                    message_id: MessageId::new(),
                    session_id: command.session_id,
                    ordinal,
                    role: ConversationRole::User,
                    content: command.input,
                    source_run_id: Some(command.run_id),
                    created_at_ms: command.created_at_ms,
                };
                transaction
                    .execute(
                        "INSERT INTO session_messages (
                            session_id, ordinal, message_id, role, source_run_id,
                            created_at_ms, message_json
                         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                        params![
                            command.session_id.to_string(),
                            to_session_sql_u64(ordinal, "message ordinal")?,
                            message.message_id.to_string(),
                            conversation_role_key(message.role),
                            command.run_id.to_string(),
                            command.created_at_ms,
                            session_encode(&message, "session message")?,
                        ],
                    )
                    .map_err(|error| session_sql_error("append session user message", error))?;

                session.next_message_ordinal = ordinal.saturating_add(1);
                session.active_run_id = Some(command.run_id);
                session.revision = session.revision.saturating_add(1);
                session.updated_at_ms = command.created_at_ms;
                update_session_row(&transaction, &session)?;
                transaction
                    .execute(
                        "INSERT INTO session_runs (
                            session_id, run_id, idempotency_key, request_hash,
                            context_through_ordinal, finalized_at_ms
                         ) VALUES (?1, ?2, ?3, ?4, ?5, NULL)",
                        params![
                            command.session_id.to_string(),
                            command.run_id.to_string(),
                            command.idempotency_key,
                            command.request_hash,
                            to_session_sql_u64(context_through_ordinal, "context_through_ordinal")?,
                        ],
                    )
                    .map_err(|error| session_sql_error("link session run", error))?;
                transaction
                    .commit()
                    .map_err(|error| session_sql_error("commit begin session run", error))?;

                Ok(BeginRunResult {
                    session,
                    run_id: command.run_id,
                    context_through_ordinal,
                    replayed: false,
                })
            })
            .await
        })
    }

    fn finalize_run(&self, command: FinalizeSessionRun) -> SessionStoreFuture<'_, SessionSnapshot> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            run_session_blocking(move || {
                if !command.run.is_terminal() {
                    return Err(SessionStoreError::RunNotTerminal);
                }
                let mut connection = lock_session_connection(&connection)?;
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(|error| session_sql_error("begin finalize transaction", error))?;
                let finalized_at: Option<Option<i64>> = transaction
                    .query_row(
                        "SELECT finalized_at_ms FROM session_runs
                         WHERE session_id = ?1 AND run_id = ?2",
                        params![
                            command.session_id.to_string(),
                            command.run.run_id.to_string()
                        ],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(|error| session_sql_error("read session run finalization", error))?;
                let Some(finalized_at) = finalized_at else {
                    return Err(SessionStoreError::RunMismatch(command.run.run_id));
                };
                let mut session = load_session_for_contract(&transaction, command.session_id)?
                    .ok_or(SessionStoreError::NotFound(command.session_id))?;
                if finalized_at.is_some() {
                    transaction
                        .commit()
                        .map_err(|error| session_sql_error("commit finalize replay", error))?;
                    return Ok(session);
                }
                if session.active_run_id != Some(command.run.run_id) {
                    return Err(SessionStoreError::RunMismatch(command.run.run_id));
                }

                let stored_run_json: Option<String> = transaction
                    .query_row(
                        "SELECT snapshot_json FROM runs WHERE run_id = ?1",
                        [command.run.run_id.to_string()],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(|error| session_sql_error("read terminal run", error))?;
                let stored_run: RunSnapshot = stored_run_json
                    .ok_or(SessionStoreError::RunMismatch(command.run.run_id))
                    .and_then(|json| session_decode(&json, "run snapshot"))?;
                if !stored_run.is_terminal() {
                    return Err(SessionStoreError::RunNotTerminal);
                }

                if stored_run.status == RunStatus::Completed {
                    let ordinal = session.next_message_ordinal;
                    let message = SessionMessage {
                        message_id: MessageId::new(),
                        session_id: command.session_id,
                        ordinal,
                        role: ConversationRole::Assistant,
                        content: vec![ContentPart::text(stored_run.output)],
                        source_run_id: Some(stored_run.run_id),
                        created_at_ms: command.finalized_at_ms,
                    };
                    transaction
                        .execute(
                            "INSERT INTO session_messages (
                                session_id, ordinal, message_id, role, source_run_id,
                                created_at_ms, message_json
                             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                            params![
                                command.session_id.to_string(),
                                to_session_sql_u64(ordinal, "message ordinal")?,
                                message.message_id.to_string(),
                                conversation_role_key(message.role),
                                stored_run.run_id.to_string(),
                                command.finalized_at_ms,
                                session_encode(&message, "session message")?,
                            ],
                        )
                        .map_err(|error| {
                            session_sql_error("append session assistant message", error)
                        })?;
                    session.next_message_ordinal = ordinal.saturating_add(1);
                }
                session.active_run_id = None;
                session.revision = session.revision.saturating_add(1);
                session.updated_at_ms = command.finalized_at_ms;
                update_session_row(&transaction, &session)?;
                transaction
                    .execute(
                        "UPDATE session_runs SET finalized_at_ms = ?1
                         WHERE session_id = ?2 AND run_id = ?3 AND finalized_at_ms IS NULL",
                        params![
                            command.finalized_at_ms,
                            command.session_id.to_string(),
                            command.run.run_id.to_string(),
                        ],
                    )
                    .map_err(|error| session_sql_error("mark session run finalized", error))?;
                transaction
                    .commit()
                    .map_err(|error| session_sql_error("commit session finalization", error))?;
                Ok(session)
            })
            .await
        })
    }

    fn archive_session(&self, command: ArchiveSession) -> SessionStoreFuture<'_, SessionSnapshot> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            run_session_blocking(move || {
                let mut connection = lock_session_connection(&connection)?;
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(|error| session_sql_error("begin archive transaction", error))?;
                let mut session = load_session_for_contract(&transaction, command.session_id)?
                    .ok_or(SessionStoreError::NotFound(command.session_id))?;
                if session.revision != command.expected_revision {
                    return Err(SessionStoreError::RevisionConflict {
                        expected: command.expected_revision,
                        actual: session.revision,
                    });
                }
                if let Some(active) = session.active_run_id {
                    return Err(SessionStoreError::Busy(active));
                }
                session.status = SessionStatus::Archived;
                session.revision = session.revision.saturating_add(1);
                session.updated_at_ms = command.archived_at_ms;
                update_session_row(&transaction, &session)?;
                transaction
                    .commit()
                    .map_err(|error| session_sql_error("commit archive session", error))?;
                Ok(session)
            })
            .await
        })
    }

    fn get_session(
        &self,
        session_id: SessionId,
    ) -> SessionStoreFuture<'_, Option<SessionSnapshot>> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            run_session_blocking(move || {
                let connection = lock_session_connection(&connection)?;
                load_session_for_contract(&connection, session_id)
            })
            .await
        })
    }

    fn messages(
        &self,
        session_id: SessionId,
        before: Option<u64>,
        limit: usize,
    ) -> SessionStoreFuture<'_, Vec<SessionMessage>> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            run_session_blocking(move || {
                let connection = lock_session_connection(&connection)?;
                if load_session_for_contract(&connection, session_id)?.is_none() {
                    return Err(SessionStoreError::NotFound(session_id));
                }
                let before = before.unwrap_or(i64::MAX as u64);
                let mut statement = connection
                    .prepare(
                        "SELECT message_json FROM session_messages
                         WHERE session_id = ?1 AND ordinal < ?2
                         ORDER BY ordinal DESC LIMIT ?3",
                    )
                    .map_err(|error| session_sql_error("prepare session messages", error))?;
                let rows = statement
                    .query_map(
                        params![
                            session_id.to_string(),
                            to_session_sql_u64(before, "before ordinal")?,
                            i64::try_from(limit.clamp(1, MAX_EVENT_PAGE_SIZE)).unwrap_or(i64::MAX),
                        ],
                        |row| row.get::<_, String>(0),
                    )
                    .map_err(|error| session_sql_error("query session messages", error))?;
                let mut messages = Vec::new();
                for row in rows {
                    let json =
                        row.map_err(|error| session_sql_error("read session message", error))?;
                    messages.push(session_decode(&json, "session message")?);
                }
                messages.reverse();
                Ok(messages)
            })
            .await
        })
    }

    fn pending_finalizations(&self) -> SessionStoreFuture<'_, Vec<(SessionId, RunSnapshot)>> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            run_session_blocking(move || {
                let connection = lock_session_connection(&connection)?;
                let mut statement = connection
                    .prepare(
                        "SELECT sr.session_id, r.snapshot_json
                         FROM session_runs sr
                         JOIN runs r ON r.run_id = sr.run_id
                         WHERE sr.finalized_at_ms IS NULL
                           AND r.status IN ('completed', 'failed', 'cancelled')
                         ORDER BY r.updated_at_ms ASC",
                    )
                    .map_err(|error| session_sql_error("prepare pending finalizations", error))?;
                let rows = statement
                    .query_map([], |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                    })
                    .map_err(|error| session_sql_error("query pending finalizations", error))?;
                let mut pending = Vec::new();
                for row in rows {
                    let (session_id, run_json) =
                        row.map_err(|error| session_sql_error("read pending finalization", error))?;
                    let session_id = session_id
                        .parse()
                        .map_err(|_| SessionStoreError::backend("stored session id is invalid"))?;
                    pending.push((session_id, session_decode(&run_json, "run snapshot")?));
                }
                Ok(pending)
            })
            .await
        })
    }
}

impl ContextArtifactStore for SqliteRunStore {
    fn descriptor(&self) -> ContextComponentDescriptor {
        ContextComponentDescriptor {
            identity: "state-store:sqlite-artifacts".into(),
            kind: "sqlite_artifact_store".into(),
            version: env!("CARGO_PKG_VERSION").into(),
        }
    }

    fn find_reusable(
        &self,
        query: ReusableArtifactQuery,
    ) -> ContextFuture<'_, Vec<ContextArtifactRef>> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            run_context_blocking(move || {
                let connection = lock_context_connection(&connection)?;
                let mut statement = connection
                    .prepare(
                        "SELECT artifact_json FROM context_artifacts
                         WHERE source_digest = ?1 AND policy_version = ?2 AND invalidated = 0
                         ORDER BY created_at_ms DESC",
                    )
                    .map_err(|error| context_sql_error("prepare reusable artifacts", error))?;
                let rows = statement
                    .query_map(
                        params![query.source_digest, i64::from(query.policy_version)],
                        |row| row.get::<_, String>(0),
                    )
                    .map_err(|error| context_sql_error("query reusable artifacts", error))?;
                let mut references = Vec::new();
                for row in rows {
                    let json =
                        row.map_err(|error| context_sql_error("read reusable artifact", error))?;
                    let artifact: ContextArtifact = context_decode(&json)?;
                    if query.kinds.is_empty() || query.kinds.contains(&artifact.kind) {
                        references.push(ContextArtifactRef {
                            artifact_id: artifact.artifact_id,
                            content_digest: artifact.content_digest,
                        });
                    }
                }
                Ok(references)
            })
            .await
        })
    }

    fn get(&self, reference: ContextArtifactRef) -> ContextFuture<'_, Option<ContextArtifact>> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            run_context_blocking(move || {
                let connection = lock_context_connection(&connection)?;
                let json: Option<String> = connection
                    .query_row(
                        "SELECT artifact_json FROM context_artifacts
                         WHERE artifact_id = ?1 AND content_digest = ?2 AND invalidated = 0",
                        params![reference.artifact_id.to_string(), reference.content_digest],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(|error| context_sql_error("read context artifact", error))?;
                json.map(|json| context_decode(&json)).transpose()
            })
            .await
        })
    }

    fn put(&self, command: PutContextArtifact) -> ContextFuture<'_, ContextArtifactRef> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            run_context_blocking(move || {
                let connection = lock_context_connection(&connection)?;
                let existing: Option<(String, String)> = connection
                    .query_row(
                        "SELECT artifact_id, content_digest FROM context_artifacts
                         WHERE source_digest = ?1 AND content_digest = ?2
                           AND policy_version = ?3",
                        params![
                            command.candidate.source_digest,
                            command.candidate.content_digest,
                            i64::from(command.candidate.policy_version),
                        ],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .optional()
                    .map_err(|error| context_sql_error("find existing context artifact", error))?;
                if let Some((artifact_id, content_digest)) = existing {
                    connection
                        .execute(
                            "UPDATE context_artifacts SET invalidated = 0
                             WHERE artifact_id = ?1",
                            [&artifact_id],
                        )
                        .map_err(|error| context_sql_error("revive context artifact", error))?;
                    return Ok(ContextArtifactRef {
                        artifact_id: artifact_id.parse().map_err(|_| {
                            ContextError::Component("stored artifact id is invalid".into())
                        })?,
                        content_digest,
                    });
                }
                let artifact = ContextArtifact {
                    artifact_id: ContextArtifactId::new(),
                    kind: command.candidate.kind,
                    source_refs: command.candidate.source_refs,
                    policy_version: command.candidate.policy_version,
                    generator: command.candidate.generator,
                    content: command.candidate.content,
                    source_digest: command.candidate.source_digest,
                    content_digest: command.candidate.content_digest,
                    created_at_ms: command.created_at_ms,
                };
                connection
                    .execute(
                        "INSERT INTO context_artifacts (
                            artifact_id, source_digest, content_digest, policy_version,
                            kind, created_at_ms, invalidated, artifact_json
                         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0, ?7)",
                        params![
                            artifact.artifact_id.to_string(),
                            artifact.source_digest,
                            artifact.content_digest,
                            i64::from(artifact.policy_version),
                            format!("{:?}", artifact.kind),
                            artifact.created_at_ms,
                            context_encode(&artifact)?,
                        ],
                    )
                    .map_err(|error| context_sql_error("insert context artifact", error))?;
                Ok(ContextArtifactRef {
                    artifact_id: artifact.artifact_id,
                    content_digest: artifact.content_digest,
                })
            })
            .await
        })
    }

    fn invalidate(&self, command: InvalidateContextArtifacts) -> ContextFuture<'_, u64> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            run_context_blocking(move || {
                let connection = lock_context_connection(&connection)?;
                let count = connection
                    .execute(
                        "UPDATE context_artifacts SET invalidated = 1
                         WHERE source_digest = ?1 AND invalidated = 0",
                        [command.source_digest],
                    )
                    .map_err(|error| context_sql_error("invalidate context artifacts", error))?;
                Ok(u64::try_from(count).unwrap_or(u64::MAX))
            })
            .await
        })
    }
}

fn connect(path: &Path) -> Result<Connection, RunStoreError> {
    let connection = Connection::open(path).map_err(|error| sql_error("open database", error))?;
    connection
        .busy_timeout(Duration::from_secs(5))
        .map_err(|error| sql_error("set busy timeout", error))?;
    connection
        .execute_batch("PRAGMA foreign_keys = ON; PRAGMA journal_mode = WAL;")
        .map_err(|error| sql_error("configure database", error))?;
    Ok(connection)
}

fn lock_run_connection(
    connection: &Mutex<Connection>,
) -> Result<std::sync::MutexGuard<'_, Connection>, RunStoreError> {
    connection
        .lock()
        .map_err(|_| RunStoreError::backend("SQLite run connection lock was poisoned"))
}

fn migrate(connection: &Connection) -> Result<(), RunStoreError> {
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(|error| sql_error("read schema version", error))?;
    if version > SCHEMA_VERSION {
        return Err(RunStoreError::backend(format!(
            "database schema version {version} is newer than supported version {SCHEMA_VERSION}"
        )));
    }
    if version == 0 {
        connection
            .execute_batch(
                "BEGIN IMMEDIATE;
                 CREATE TABLE runs (
                    run_id TEXT PRIMARY KEY NOT NULL,
                    status TEXT NOT NULL,
                    last_seq INTEGER NOT NULL,
                    created_at_ms INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL,
                    snapshot_json TEXT NOT NULL
                 );
                 CREATE TABLE run_events (
                    run_id TEXT NOT NULL,
                    seq INTEGER NOT NULL,
                    observed_at_ms INTEGER NOT NULL,
                    event_json TEXT NOT NULL,
                    PRIMARY KEY (run_id, seq),
                    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
                 );
                 CREATE INDEX runs_status_created_idx ON runs(status, created_at_ms);
                 PRAGMA user_version = 1;
                 COMMIT;",
            )
            .map_err(|error| sql_error("create run store schema", error))?;
    }
    if version < 2 {
        connection
            .execute_batch(
                "BEGIN IMMEDIATE;
                 CREATE TABLE sessions (
                    session_id TEXT PRIMARY KEY NOT NULL,
                    status TEXT NOT NULL,
                    revision INTEGER NOT NULL,
                    next_message_ordinal INTEGER NOT NULL,
                    active_run_id TEXT,
                    created_at_ms INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL,
                    snapshot_json TEXT NOT NULL,
                    FOREIGN KEY (active_run_id) REFERENCES runs(run_id)
                 );
                 CREATE TABLE session_messages (
                    session_id TEXT NOT NULL,
                    ordinal INTEGER NOT NULL,
                    message_id TEXT NOT NULL UNIQUE,
                    role TEXT NOT NULL,
                    source_run_id TEXT,
                    created_at_ms INTEGER NOT NULL,
                    message_json TEXT NOT NULL,
                    PRIMARY KEY (session_id, ordinal),
                    FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE,
                    FOREIGN KEY (source_run_id) REFERENCES runs(run_id)
                 );
                 CREATE TABLE session_runs (
                    session_id TEXT NOT NULL,
                    run_id TEXT NOT NULL UNIQUE,
                    idempotency_key TEXT NOT NULL,
                    request_hash TEXT NOT NULL,
                    context_through_ordinal INTEGER NOT NULL,
                    finalized_at_ms INTEGER,
                    PRIMARY KEY (session_id, idempotency_key),
                    FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE,
                    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
                 );
                 CREATE INDEX session_messages_created_idx
                    ON session_messages(session_id, ordinal);
                 PRAGMA user_version = 2;
                 COMMIT;",
            )
            .map_err(|error| sql_error("create session store schema", error))?;
    }
    if version < 3 {
        connection
            .execute_batch(
                "BEGIN IMMEDIATE;
                 CREATE TABLE context_artifacts (
                    artifact_id TEXT PRIMARY KEY NOT NULL,
                    source_digest TEXT NOT NULL,
                    content_digest TEXT NOT NULL,
                    policy_version INTEGER NOT NULL,
                    kind TEXT NOT NULL,
                    created_at_ms INTEGER NOT NULL,
                    invalidated INTEGER NOT NULL,
                    artifact_json TEXT NOT NULL,
                    UNIQUE(source_digest, content_digest, policy_version)
                 );
                 CREATE INDEX context_artifacts_reuse_idx
                    ON context_artifacts(source_digest, policy_version, invalidated);
                 PRAGMA user_version = 3;
                 COMMIT;",
            )
            .map_err(|error| sql_error("create context artifact schema", error))?;
    }
    Ok(())
}

fn lock_context_connection(
    connection: &Mutex<Connection>,
) -> Result<std::sync::MutexGuard<'_, Connection>, ContextError> {
    connection
        .lock()
        .map_err(|_| ContextError::Component("SQLite context connection lock was poisoned".into()))
}

fn context_encode(value: &ContextArtifact) -> Result<String, ContextError> {
    serde_json::to_string(value)
        .map_err(|error| ContextError::Component(format!("encode context artifact: {error}")))
}

fn context_decode(json: &str) -> Result<ContextArtifact, ContextError> {
    serde_json::from_str(json)
        .map_err(|error| ContextError::Component(format!("decode context artifact: {error}")))
}

fn context_sql_error(operation: &str, error: rusqlite::Error) -> ContextError {
    ContextError::Component(format!("{operation}: {error}"))
}

async fn run_context_blocking<T: Send + 'static>(
    operation: impl FnOnce() -> Result<T, ContextError> + Send + 'static,
) -> Result<T, ContextError> {
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|error| ContextError::Component(format!("context store worker failed: {error}")))?
}

fn lock_session_connection(
    connection: &Mutex<Connection>,
) -> Result<std::sync::MutexGuard<'_, Connection>, SessionStoreError> {
    connection
        .lock()
        .map_err(|_| SessionStoreError::backend("SQLite session connection lock was poisoned"))
}

fn load_session_for_contract(
    connection: &Connection,
    session_id: SessionId,
) -> Result<Option<SessionSnapshot>, SessionStoreError> {
    let json: Option<String> = connection
        .query_row(
            "SELECT snapshot_json FROM sessions WHERE session_id = ?1",
            [session_id.to_string()],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| session_sql_error("read session snapshot", error))?;
    json.map(|json| session_decode(&json, "session snapshot"))
        .transpose()
}

fn update_session_row(
    connection: &Connection,
    session: &SessionSnapshot,
) -> Result<(), SessionStoreError> {
    let updated = connection
        .execute(
            "UPDATE sessions SET
                status = ?1, revision = ?2, next_message_ordinal = ?3,
                active_run_id = ?4, updated_at_ms = ?5, snapshot_json = ?6
             WHERE session_id = ?7",
            params![
                session_status_key(session.status),
                to_session_sql_u64(session.revision, "revision")?,
                to_session_sql_u64(session.next_message_ordinal, "next_message_ordinal")?,
                session.active_run_id.map(|run_id| run_id.to_string()),
                session.updated_at_ms,
                session_encode(session, "session snapshot")?,
                session.session_id.to_string(),
            ],
        )
        .map_err(|error| session_sql_error("update session snapshot", error))?;
    if updated != 1 {
        return Err(SessionStoreError::NotFound(session.session_id));
    }
    Ok(())
}

const fn session_status_key(status: SessionStatus) -> &'static str {
    match status {
        SessionStatus::Active => "active",
        SessionStatus::Archived => "archived",
    }
}

const fn conversation_role_key(role: ConversationRole) -> &'static str {
    match role {
        ConversationRole::User => "user",
        ConversationRole::Assistant => "assistant",
        ConversationRole::SystemNote => "system_note",
    }
}

fn session_encode(value: &impl serde::Serialize, label: &str) -> Result<String, SessionStoreError> {
    serde_json::to_string(value)
        .map_err(|error| SessionStoreError::backend(format!("failed to encode {label}: {error}")))
}

fn session_decode<T: serde::de::DeserializeOwned>(
    json: &str,
    label: &str,
) -> Result<T, SessionStoreError> {
    serde_json::from_str(json)
        .map_err(|error| SessionStoreError::backend(format!("failed to decode {label}: {error}")))
}

fn to_session_sql_u64(value: u64, label: &str) -> Result<i64, SessionStoreError> {
    i64::try_from(value)
        .map_err(|_| SessionStoreError::backend(format!("{label} exceeds SQLite integer range")))
}

fn from_session_sql_u64(value: i64, label: &str) -> Result<u64, SessionStoreError> {
    u64::try_from(value).map_err(|_| SessionStoreError::backend(format!("{label} is negative")))
}

fn session_sql_error(operation: &str, error: rusqlite::Error) -> SessionStoreError {
    SessionStoreError::backend(format!("{operation}: {error}"))
}

async fn run_session_blocking<T: Send + 'static>(
    operation: impl FnOnce() -> Result<T, SessionStoreError> + Send + 'static,
) -> Result<T, SessionStoreError> {
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|error| SessionStoreError::backend(format!("store worker failed: {error}")))?
}

fn load_snapshot(
    connection: &Connection,
    run_id: RunId,
) -> Result<Option<RunSnapshot>, RunStoreError> {
    let json: Option<String> = connection
        .query_row(
            "SELECT snapshot_json FROM runs WHERE run_id = ?1",
            [run_id.to_string()],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| sql_error("read run snapshot", error))?;
    json.map(|json| decode(&json, "run snapshot")).transpose()
}

fn status_key(status: RunStatus) -> &'static str {
    match status {
        RunStatus::Accepted => "accepted",
        RunStatus::Runnable => "runnable",
        RunStatus::Running => "running",
        RunStatus::WaitingApproval => "waiting_approval",
        RunStatus::WaitingEvent => "waiting_event",
        RunStatus::ExecutingTool => "executing_tool",
        RunStatus::Completed => "completed",
        RunStatus::Failed => "failed",
        RunStatus::Cancelled => "cancelled",
    }
}

fn encode(value: &impl serde::Serialize, label: &str) -> Result<String, RunStoreError> {
    serde_json::to_string(value)
        .map_err(|error| RunStoreError::backend(format!("failed to encode {label}: {error}")))
}

fn decode<T: serde::de::DeserializeOwned>(json: &str, label: &str) -> Result<T, RunStoreError> {
    serde_json::from_str(json)
        .map_err(|error| RunStoreError::backend(format!("failed to decode {label}: {error}")))
}

fn to_sql_u64(value: u64, label: &str) -> Result<i64, RunStoreError> {
    i64::try_from(value)
        .map_err(|_| RunStoreError::backend(format!("{label} exceeds SQLite integer range")))
}

fn is_constraint_violation(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(failure, _)
            if failure.code == ErrorCode::ConstraintViolation
    )
}

fn sql_error(operation: &str, error: rusqlite::Error) -> RunStoreError {
    RunStoreError::backend(format!("{operation}: {error}"))
}

async fn run_blocking<T: Send + 'static>(
    operation: impl FnOnce() -> Result<T, RunStoreError> + Send + 'static,
) -> Result<T, RunStoreError> {
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|error| RunStoreError::backend(format!("store worker failed: {error}")))?
}

#[cfg(test)]
mod tests {
    use agent_core::harness::{FinishReason, OutputChannel, RunEventKind, RunStateError};
    use tempfile::tempdir;

    use super::*;

    #[tokio::test]
    async fn lists_durable_sessions_by_activity_and_status() {
        let directory = tempdir().expect("temp directory should be created");
        let path = directory.path().join("runs.sqlite3");
        let store = SqliteRunStore::open(&path)
            .await
            .expect("store should open");
        let oldest = SessionId::new();
        let archived = SessionId::new();
        let newest = SessionId::new();

        for (session_id, title, created_at_ms) in [
            (oldest, "oldest", 10),
            (archived, "archived", 20),
            (newest, "newest", 25),
        ] {
            store
                .create_session(CreateSession {
                    session_id,
                    agent_profile: "default".into(),
                    title: Some(title.into()),
                    created_at_ms,
                })
                .await
                .expect("session should be created");
        }
        store
            .archive_session(ArchiveSession {
                session_id: archived,
                expected_revision: 0,
                archived_at_ms: 30,
            })
            .await
            .expect("session should archive");
        drop(store);

        let reopened = SqliteRunStore::open(&path)
            .await
            .expect("store should reopen");
        let active = reopened
            .list_sessions(Some(SessionStatus::Active), 100)
            .await
            .expect("active sessions should list");
        let all = reopened
            .list_sessions(None, 100)
            .await
            .expect("all sessions should list");

        assert_eq!(
            active
                .iter()
                .map(|session| session.session_id)
                .collect::<Vec<_>>(),
            vec![newest, oldest]
        );
        assert_eq!(all[0].session_id, archived);
    }

    #[tokio::test]
    async fn persists_snapshots_and_events_across_reopen() {
        let directory = tempdir().expect("temp directory should be created");
        let path = directory.path().join("runs.sqlite3");
        let run_id = RunId::new();

        let store = SqliteRunStore::open(&path)
            .await
            .expect("store should open");
        store
            .create_run(RunSnapshot::new(run_id, "hello", 10))
            .await
            .expect("run should be created");
        let started = RunEvent::new(run_id, 1, RunEventKind::RunStarted);
        store
            .append_event(started.clone(), 11)
            .await
            .expect("start should append");
        store
            .append_event(
                RunEvent::new(
                    run_id,
                    2,
                    RunEventKind::RunCompleted {
                        finish_reason: FinishReason::Stop,
                    },
                ),
                12,
            )
            .await
            .expect("completion should append");
        drop(store);

        let reopened = SqliteRunStore::open(&path)
            .await
            .expect("store should reopen");
        let snapshot = reopened
            .get_run(run_id)
            .await
            .expect("snapshot query should work")
            .expect("snapshot should exist");
        let events = reopened
            .events_after(run_id, 0, 100)
            .await
            .expect("events should load");

        assert_eq!(snapshot.status, RunStatus::Completed);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0], started);
        assert!(
            reopened
                .unfinished_runs()
                .await
                .expect("unfinished query should work")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn identical_event_append_is_idempotent() {
        let directory = tempdir().expect("temp directory should be created");
        let store = SqliteRunStore::open(directory.path().join("runs.sqlite3"))
            .await
            .expect("store should open");
        let run_id = RunId::new();
        store
            .create_run(RunSnapshot::new(run_id, "hello", 10))
            .await
            .expect("run should be created");
        let event = RunEvent::new(run_id, 1, RunEventKind::RunStarted);

        let first = store
            .append_event(event.clone(), 11)
            .await
            .expect("first append should work");
        let replay = store
            .append_event(event, 12)
            .await
            .expect("same append should replay");

        assert_eq!(first, replay);
        assert_eq!(replay.revision, 1);
    }

    #[tokio::test]
    async fn rejects_sequence_gaps_without_partial_writes() {
        let directory = tempdir().expect("temp directory should be created");
        let store = SqliteRunStore::open(directory.path().join("runs.sqlite3"))
            .await
            .expect("store should open");
        let run_id = RunId::new();
        store
            .create_run(RunSnapshot::new(run_id, "hello", 10))
            .await
            .expect("run should be created");

        let error = store
            .append_event(RunEvent::new(run_id, 2, RunEventKind::RunStarted), 11)
            .await
            .expect_err("gap should fail");
        let snapshot = store
            .get_run(run_id)
            .await
            .expect("snapshot query should work")
            .expect("snapshot should exist");

        assert!(matches!(
            error,
            RunStoreError::State(RunStateError::SequenceConflict { .. })
        ));
        assert_eq!(snapshot.status, RunStatus::Accepted);
        assert_eq!(snapshot.last_seq, 0);
    }

    #[tokio::test]
    async fn event_batch_commits_one_complete_projection() {
        let directory = tempdir().expect("temp directory should be created");
        let store = SqliteRunStore::open(directory.path().join("runs.sqlite3"))
            .await
            .expect("store should open");
        let run_id = RunId::new();
        store
            .create_run(RunSnapshot::new(run_id, "hello", 10))
            .await
            .expect("run should be created");

        let snapshot = store
            .append_events(vec![
                ObservedRunEvent::new(RunEvent::new(run_id, 1, RunEventKind::RunStarted), 11),
                ObservedRunEvent::new(
                    RunEvent::new(
                        run_id,
                        2,
                        RunEventKind::OutputDelta {
                            channel: OutputChannel::AssistantText,
                            delta: "batched".into(),
                        },
                    ),
                    12,
                ),
                ObservedRunEvent::new(
                    RunEvent::new(
                        run_id,
                        3,
                        RunEventKind::RunCompleted {
                            finish_reason: FinishReason::Stop,
                        },
                    ),
                    13,
                ),
            ])
            .await
            .expect("batch should append");

        assert_eq!(snapshot.status, RunStatus::Completed);
        assert_eq!(snapshot.output, "batched");
        assert_eq!(snapshot.last_seq, 3);
        assert_eq!(snapshot.revision, 3);
        assert_eq!(
            store
                .events_after(run_id, 0, 10)
                .await
                .expect("events should load")
                .len(),
            3
        );
    }

    #[tokio::test]
    async fn invalid_event_batch_rolls_back_every_event() {
        let directory = tempdir().expect("temp directory should be created");
        let store = SqliteRunStore::open(directory.path().join("runs.sqlite3"))
            .await
            .expect("store should open");
        let run_id = RunId::new();
        store
            .create_run(RunSnapshot::new(run_id, "hello", 10))
            .await
            .expect("run should be created");

        let error = store
            .append_events(vec![
                ObservedRunEvent::new(RunEvent::new(run_id, 1, RunEventKind::RunStarted), 11),
                ObservedRunEvent::new(
                    RunEvent::new(
                        run_id,
                        3,
                        RunEventKind::RunCompleted {
                            finish_reason: FinishReason::Stop,
                        },
                    ),
                    12,
                ),
            ])
            .await
            .expect_err("sequence gap should reject the batch");

        assert!(matches!(
            error,
            RunStoreError::State(RunStateError::SequenceConflict { .. })
        ));
        let snapshot = store
            .get_run(run_id)
            .await
            .expect("snapshot should load")
            .expect("snapshot should exist");
        assert_eq!(snapshot.status, RunStatus::Accepted);
        assert_eq!(snapshot.last_seq, 0);
        assert!(
            store
                .events_after(run_id, 0, 10)
                .await
                .expect("events should load")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn session_begin_finalize_and_reopen_are_atomic_and_idempotent() {
        let directory = tempdir().expect("temp directory should be created");
        let path = directory.path().join("state.sqlite3");
        let store = SqliteRunStore::open(&path)
            .await
            .expect("store should open");
        let session_id = SessionId::new();
        let created = store
            .create_session(CreateSession {
                session_id,
                agent_profile: "default".into(),
                title: Some("test".into()),
                created_at_ms: 1,
            })
            .await
            .expect("session should be created");
        let run_id = RunId::new();
        let command = BeginSessionRun {
            session_id,
            run_id,
            expected_revision: created.revision,
            idempotency_key: "request-1".into(),
            request_hash: "hash-1".into(),
            input: vec![ContentPart::text("hello")],
            created_at_ms: 2,
        };
        let begun = store
            .begin_run(command.clone(), RunSnapshot::new(run_id, "hello", 2))
            .await
            .expect("session run should begin");
        let replay = store
            .begin_run(command, RunSnapshot::new(run_id, "hello", 2))
            .await
            .expect("same idempotency key should replay");
        assert!(!begun.replayed);
        assert!(replay.replayed);
        assert_eq!(replay.run_id, run_id);
        assert_eq!(begun.context_through_ordinal, 0);

        store
            .append_event(RunEvent::new(run_id, 1, RunEventKind::RunStarted), 3)
            .await
            .expect("run should start");
        store
            .append_event(
                RunEvent::new(
                    run_id,
                    2,
                    RunEventKind::OutputDelta {
                        channel: OutputChannel::AssistantText,
                        delta: "world".into(),
                    },
                ),
                4,
            )
            .await
            .expect("output should append");
        let terminal = store
            .append_event(
                RunEvent::new(
                    run_id,
                    3,
                    RunEventKind::RunCompleted {
                        finish_reason: FinishReason::Stop,
                    },
                ),
                5,
            )
            .await
            .expect("run should complete");
        assert_eq!(
            store
                .pending_finalizations()
                .await
                .expect("query should work")
                .len(),
            1
        );
        let finalized = store
            .finalize_run(FinalizeSessionRun {
                session_id,
                run: terminal.clone(),
                finalized_at_ms: 6,
            })
            .await
            .expect("session should finalize");
        let replayed_finalize = store
            .finalize_run(FinalizeSessionRun {
                session_id,
                run: terminal,
                finalized_at_ms: 7,
            })
            .await
            .expect("finalization should replay");
        assert_eq!(finalized, replayed_finalize);
        assert_eq!(finalized.active_run_id, None);
        let messages = store
            .messages(session_id, None, 100)
            .await
            .expect("messages should load");
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].role, ConversationRole::User);
        assert_eq!(messages[1].role, ConversationRole::Assistant);

        drop(store);
        let reopened = SqliteRunStore::open(&path)
            .await
            .expect("store should reopen");
        assert_eq!(
            reopened
                .get_session(session_id)
                .await
                .expect("session query should work")
                .expect("session should exist"),
            finalized
        );
        assert!(
            reopened
                .pending_finalizations()
                .await
                .expect("pending query should work")
                .is_empty()
        );
    }
}
