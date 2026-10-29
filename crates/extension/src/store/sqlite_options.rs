//! Explicit SQLite ownership and durability configuration for the event/Fabric store.
use super::event::{SqliteEventStore, migrate};
use agent_core::contracts::events::EventError;
use rusqlite::{Connection, OptionalExtension};
use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqliteJournalMode {
    Wal,
    Delete,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqliteSynchronous {
    Full,
    Normal,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqliteMigration {
    Apply,
    ValidateOnly,
}
#[derive(Debug, Clone)]
pub struct SqliteOpenOptions {
    pub fabric_limits: agent_core::contracts::fabric::FabricLimits,
    pub journal_mode: SqliteJournalMode,
    pub synchronous: SqliteSynchronous,
    pub busy_timeout: Duration,
    pub migration: SqliteMigration,
}
impl Default for SqliteOpenOptions {
    fn default() -> Self {
        Self {
            fabric_limits: Default::default(),
            journal_mode: SqliteJournalMode::Wal,
            synchronous: SqliteSynchronous::Full,
            busy_timeout: Duration::from_secs(5),
            migration: SqliteMigration::Apply,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqliteConfiguration {
    pub journal_mode: String,
    pub synchronous: i64,
    pub busy_timeout_ms: i64,
    pub foreign_keys: bool,
}
fn error(e: impl std::fmt::Display) -> EventError {
    EventError::backend(format!("SQLite configuration: {e}"))
}
fn inspect(c: &Connection) -> Result<SqliteConfiguration, EventError> {
    Ok(SqliteConfiguration {
        journal_mode: c
            .pragma_query_value(None, "journal_mode", |r| r.get(0))
            .map_err(error)?,
        synchronous: c
            .pragma_query_value(None, "synchronous", |r| r.get(0))
            .map_err(error)?,
        busy_timeout_ms: c
            .pragma_query_value(None, "busy_timeout", |r| r.get(0))
            .map_err(error)?,
        foreign_keys: c
            .pragma_query_value(None, "foreign_keys", |r| r.get(0))
            .map_err(error)?,
    })
}
impl SqliteEventStore {
    /// Opens a file-backed store. Unsupported journal modes (including WAL on memory DBs)
    /// fail rather than silently degrading the requested configuration.
    pub async fn open_with_options(
        path: impl AsRef<Path>,
        identity: impl Into<String>,
        options: SqliteOpenOptions,
    ) -> Result<Self, EventError> {
        let path = path.as_ref().to_owned();
        let identity = identity.into();
        tokio::task::spawn_blocking(move || {
            if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
                std::fs::create_dir_all(parent).map_err(error)?;
            }
            Self::take_connection(Connection::open(path).map_err(error)?, identity, options)
        })
        .await
        .map_err(error)?
    }
    /// Transfers ownership. A connection with an active transaction is rejected.
    pub async fn from_connection(
        connection: Connection,
        identity: impl Into<String>,
        options: SqliteOpenOptions,
    ) -> Result<Self, EventError> {
        let identity = identity.into();
        tokio::task::spawn_blocking(move || Self::take_connection(connection, identity, options))
            .await
            .map_err(error)?
    }
    fn take_connection(
        connection: Connection,
        identity: String,
        options: SqliteOpenOptions,
    ) -> Result<Self, EventError> {
        options.fabric_limits.validate().map_err(error)?;
        if !connection.is_autocommit() {
            return Err(error("active transaction"));
        }
        let millis = i32::try_from(options.busy_timeout.as_millis()).map_err(error)?;
        let journal = match options.journal_mode {
            SqliteJournalMode::Wal => "wal",
            SqliteJournalMode::Delete => "delete",
        };
        let synchronous = match options.synchronous {
            SqliteSynchronous::Full => 2,
            SqliteSynchronous::Normal => 1,
        };
        connection
            .busy_timeout(Duration::from_millis(millis as u64))
            .map_err(error)?;
        connection
            .pragma_update(None, "journal_mode", journal)
            .map_err(error)?;
        connection
            .pragma_update(None, "synchronous", synchronous)
            .map_err(error)?;
        connection
            .pragma_update(None, "foreign_keys", true)
            .map_err(error)?;
        let actual = inspect(&connection)?;
        if actual.journal_mode != journal
            || actual.synchronous != synchronous
            || !actual.foreign_keys
            || actual.busy_timeout_ms != i64::from(millis)
        {
            return Err(error("requested PRAGMA configuration unsupported"));
        }
        match options.migration {
            SqliteMigration::Apply => migrate(&connection)?,
            SqliteMigration::ValidateOnly => {
                // Compare the required schema to a fresh reference without touching the source.
                let reference = Connection::open_in_memory().map_err(error)?;
                migrate(&reference)?;
                let mut query = reference.prepare("SELECT name, sql FROM sqlite_master WHERE name LIKE 'workflow_%' AND sql IS NOT NULL").map_err(error)?;
                let required = query
                    .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
                    .map_err(error)?;
                for entry in required {
                    let (name, sql) = entry.map_err(error)?;
                    let existing: Option<String> = connection
                        .query_row(
                            "SELECT sql FROM sqlite_master WHERE name=?1",
                            [&name],
                            |r| r.get(0),
                        )
                        .optional()
                        .map_err(error)?;
                    if existing.as_deref() != Some(&sql) {
                        return Err(error(format!("migration required: {name}")));
                    }
                }
                let marker: Option<String> = connection
                    .query_row(
                        "SELECT state_json FROM workflow_operator_fabric_v1 WHERE singleton=1",
                        [],
                        |r| r.get(0),
                    )
                    .optional()
                    .map_err(error)?;
                if let Some(marker) = marker {
                    let marker: serde_json::Value = serde_json::from_str(&marker).map_err(error)?;
                    if marker["schema_version"] != 4
                        && marker["schema_version"] != 5
                        && marker["schema_version"] != 6
                    {
                        return Err(error("Fabric migration required"));
                    }
                } else {
                    return Err(error("Fabric initialization required"));
                }
            }
        }
        Ok(Self {
            fabric_limits: options.fabric_limits,
            connection: Arc::new(Mutex::new(connection)),
            identity,
            fabric_changed: tokio::sync::watch::channel(0).0,
        })
    }
    pub async fn configuration(&self) -> Result<SqliteConfiguration, EventError> {
        let connection = self.connection.clone();
        tokio::task::spawn_blocking(move || inspect(&*connection.lock().map_err(error)?))
            .await
            .map_err(error)?
    }
}
