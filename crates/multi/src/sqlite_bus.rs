use crate::{EventBus, Message, Topic};
use crate::{SupervisorStore, TaskRecord};
use rusqlite::{Connection, OptionalExtension, params};
use uuid::Uuid;

/// Durable transport contract. Implementations provide replay and explicit ack;
/// unlike `EventBus`, this contract is synchronous and persistence-oriented.
pub trait DurableEventBus {
    type Error;
    fn publish_durable(&self, message: &Message) -> Result<i64, Self::Error>;
    fn replay(
        &self,
        consumer: &str,
        topic: &Topic,
        limit: u32,
    ) -> Result<Vec<(i64, Message)>, Self::Error>;
    fn acknowledge(&self, consumer: &str, sequence: i64) -> Result<(), Self::Error>;
}

/// Small durable append-only log. Consumers use a named offset and explicit ack.
pub struct SqliteBus {
    conn: Connection,
}

/// Thread-safe handle for sharing one SQLite connection among runtime tasks.
#[derive(Clone)]
pub struct SqliteBusHandle(std::sync::Arc<std::sync::Mutex<SqliteBus>>);
pub struct SqliteSubscription {
    handle: SqliteBusHandle,
    consumer: String,
    topic: Topic,
}

pub struct SqliteSupervisorStore {
    pub bus: SqliteBusHandle,
}
impl SqliteSupervisorStore {
    pub fn new(bus: SqliteBusHandle) -> Self {
        Self { bus }
    }
}
impl SupervisorStore for SqliteSupervisorStore {
    type Error = rusqlite::Error;
    fn load_tasks(&self, run_id: Uuid) -> Result<Vec<TaskRecord>, Self::Error> {
        let guard = self.bus.0.lock().expect("sqlite bus lock");
        let mut stmt = guard
            .conn
            .prepare("SELECT payload FROM supervisor_tasks WHERE run_id=? ORDER BY task_id")?;
        stmt.query_map([run_id.to_string()], |row| {
            let payload: String = row.get(0)?;
            serde_json::from_str(&payload).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })
        })?
        .collect()
    }
    fn save_tasks(&self, run_id: Uuid, tasks: &[TaskRecord]) -> Result<(), Self::Error> {
        let guard = self.bus.0.lock().expect("sqlite bus lock");
        let tx = guard.conn.unchecked_transaction()?;
        for task in tasks {
            tx.execute("INSERT INTO supervisor_tasks(run_id,task_id,state,payload,updated_at) VALUES(?,?,?,?,strftime('%s','now')) ON CONFLICT(run_id,task_id) DO UPDATE SET state=excluded.state,payload=excluded.payload,updated_at=excluded.updated_at", rusqlite::params![run_id.to_string(),task.id.to_string(),serde_json::to_string(&task.status).unwrap(),serde_json::to_string(task).unwrap()])?;
        }
        tx.commit()
    }
    fn commit_result(
        &self,
        run_id: Uuid,
        tasks: &[TaskRecord],
        consumer: &str,
        _topic: &Topic,
        sequence: i64,
        checkpoint: &Message,
    ) -> Result<(), Self::Error> {
        let guard = self.bus.0.lock().expect("sqlite bus lock");
        let tx = guard.conn.unchecked_transaction()?;
        for task in tasks {
            tx.execute("INSERT INTO supervisor_tasks(run_id,task_id,state,payload,updated_at) VALUES(?,?,?,?,strftime('%s','now')) ON CONFLICT(run_id,task_id) DO UPDATE SET state=excluded.state,payload=excluded.payload,updated_at=excluded.updated_at", rusqlite::params![run_id.to_string(), task.id.to_string(), serde_json::to_string(&task.status).unwrap(), serde_json::to_string(task).unwrap()])?;
        }
        tx.execute("INSERT INTO messages(id,run_id,topic,sender,kind,correlation_id,payload) VALUES(?,?,?,?,?,?,?) ON CONFLICT(id) DO NOTHING", rusqlite::params![checkpoint.id.to_string(), checkpoint.run_id.to_string(), checkpoint.topic.0, checkpoint.sender, checkpoint.kind, checkpoint.correlation_id.map(|x| x.to_string()), checkpoint.payload.to_string()])?;
        let message_topic: String =
            tx.query_row("SELECT topic FROM messages WHERE seq=?", [sequence], |r| {
                r.get(0)
            })?;
        let current: i64 = tx
            .query_row(
                "SELECT seq FROM topic_offsets WHERE consumer=? AND topic=?",
                rusqlite::params![consumer, message_topic],
                |r| r.get(0),
            )
            .optional()?
            .unwrap_or(0);
        if sequence > current {
            tx.execute("INSERT INTO topic_offsets(consumer,topic,seq) VALUES(?,?,?) ON CONFLICT(consumer,topic) DO UPDATE SET seq=MAX(topic_offsets.seq,excluded.seq)", rusqlite::params![consumer,message_topic,sequence])?;
        }
        tx.commit()
    }
}
impl SqliteBusHandle {
    pub async fn commit_supervisor_result_async(
        &self,
        run_id: Uuid,
        tasks: Vec<crate::TaskRecord>,
        consumer: String,
        topic: Topic,
        sequence: i64,
        checkpoint: Message,
    ) -> rusqlite::Result<()> {
        let handle = self.clone();
        tokio::task::spawn_blocking(move || {
            let bus = handle.0.lock().expect("sqlite bus lock");
            let tx = bus.conn.unchecked_transaction()?;
            for task in tasks { tx.execute("INSERT INTO supervisor_tasks(run_id,task_id,state,payload,updated_at) VALUES(?,?,?,?,strftime('%s','now')) ON CONFLICT(run_id,task_id) DO UPDATE SET state=excluded.state,payload=excluded.payload,updated_at=excluded.updated_at", params![run_id.to_string(), task.id.to_string(), serde_json::to_string(&task.status).unwrap(), serde_json::to_string(&task).unwrap()])?; }
            let payload = checkpoint.payload.to_string();
            tx.execute("INSERT INTO messages(id,run_id,topic,sender,kind,correlation_id,payload) VALUES(?,?,?,?,?,?,?) ON CONFLICT(id) DO NOTHING", params![checkpoint.id.to_string(), checkpoint.run_id.to_string(), checkpoint.topic.0, checkpoint.sender, checkpoint.kind, checkpoint.correlation_id.map(|x| x.to_string()), payload])?;
            let result_topic: String = tx.query_row("SELECT topic FROM messages WHERE seq=?", [sequence], |r| r.get(0))?;
            if result_topic != topic.0 { return Err(rusqlite::Error::InvalidParameterName("result topic mismatch".into())); }
            tx.execute("INSERT INTO topic_offsets(consumer,topic,seq) VALUES(?,?,?) ON CONFLICT(consumer,topic) DO UPDATE SET seq=MAX(topic_offsets.seq,excluded.seq)", params![consumer, topic.0, sequence])?;
            tx.commit()
        }).await.map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?
    }
    pub async fn upsert_supervisor_tasks(
        &self,
        run_id: Uuid,
        tasks: Vec<crate::TaskRecord>,
    ) -> rusqlite::Result<()> {
        let handle = self.clone();
        tokio::task::spawn_blocking(move || {
            let bus = handle.0.lock().expect("sqlite bus lock");
            let tx = bus.conn.unchecked_transaction()?;
            for task in tasks {
                tx.execute("INSERT INTO supervisor_tasks(run_id,task_id,state,payload,updated_at) VALUES(?,?,?,?,strftime('%s','now')) ON CONFLICT(run_id,task_id) DO UPDATE SET state=excluded.state,payload=excluded.payload,updated_at=excluded.updated_at", rusqlite::params![run_id.to_string(), task.id.to_string(), serde_json::to_string(&task.status).unwrap(), serde_json::to_string(&task).unwrap()])?;
            }
            tx.commit()
        }).await.map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?
    }
    pub async fn commit_messages_async(
        &self,
        consumer: String,
        topic: Topic,
        sequences: Vec<i64>,
        messages: Vec<Message>,
    ) -> rusqlite::Result<()> {
        let handle = self.clone();
        tokio::task::spawn_blocking(move || {
            let bus = handle.0.lock().expect("sqlite bus lock");
            let tx = bus.conn.unchecked_transaction()?;
            let start: i64 = tx.query_row("SELECT seq FROM topic_offsets WHERE consumer=? AND topic=?", params![consumer, topic.0], |r| r.get(0)).optional()?.unwrap_or(0);
            let mut stmt = tx.prepare("SELECT seq FROM messages WHERE seq>? AND topic=? ORDER BY seq LIMIT ?")?;
            let rows: Vec<i64> = stmt.query_map(params![start, topic.0, sequences.len() as u32], |r| r.get(0))?.collect::<rusqlite::Result<_>>()?;
            drop(stmt);
            if sequences.is_empty() || rows != sequences { return Err(rusqlite::Error::InvalidParameterName("batch must be unread prefix".into())); }
            for message in messages { tx.execute("INSERT INTO messages(id,run_id,topic,sender,kind,correlation_id,payload) VALUES(?,?,?,?,?,?,?)", params![message.id.to_string(), message.run_id.to_string(), message.topic.0, message.sender, message.kind, message.correlation_id.map(|x| x.to_string()), message.payload.to_string()])?; }
            tx.execute("INSERT INTO topic_offsets(consumer,topic,seq) VALUES(?,?,?) ON CONFLICT(consumer,topic) DO UPDATE SET seq=excluded.seq", params![consumer, topic.0, sequences.last().unwrap()])?;
            tx.commit()
        }).await.map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?
    }
    /// Atomically publishes a decision and acknowledges an exact unread prefix.
    pub async fn commit_batch_async(
        &self,
        consumer: String,
        topic: Topic,
        sequences: Vec<i64>,
        decision: Message,
    ) -> rusqlite::Result<()> {
        let handle = self.clone();
        tokio::task::spawn_blocking(move || {
            let bus = handle.0.lock().expect("sqlite bus lock");
            let tx = bus.conn.unchecked_transaction()?;
            let limit = u32::try_from(sequences.len()).map_err(|_| rusqlite::Error::InvalidQuery)?;
            let start: i64 = tx.query_row("SELECT seq FROM topic_offsets WHERE consumer=? AND topic=?", params![consumer, topic.0], |r| r.get(0)).optional()?.unwrap_or(0);
            let mut stmt = tx.prepare("SELECT seq FROM messages WHERE seq>? AND topic=? ORDER BY seq LIMIT ?")?;
            let rows: Vec<i64> = stmt.query_map(params![start, topic.0, limit], |r| r.get(0))?.collect::<rusqlite::Result<_>>()?;
            drop(stmt);
            if sequences.is_empty() || rows != sequences {
                return Err(rusqlite::Error::InvalidParameterName("batch must be the unread topic prefix".into()));
            }
            let payload = decision.payload.to_string();
            tx.execute("INSERT INTO messages(id,run_id,topic,sender,kind,correlation_id,payload) VALUES(?,?,?,?,?,?,?)", params![decision.id.to_string(), decision.run_id.to_string(), decision.topic.0, decision.sender, decision.kind, decision.correlation_id.map(|x| x.to_string()), payload])?;
            tx.execute("INSERT INTO topic_offsets(consumer,topic,seq) VALUES(?,?,?) ON CONFLICT(consumer,topic) DO UPDATE SET seq=excluded.seq", params![consumer, topic.0, sequences.last().unwrap()])?;
            tx.commit()
        }).await.map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?
    }
    pub fn new(bus: SqliteBus) -> Self {
        Self(std::sync::Arc::new(std::sync::Mutex::new(bus)))
    }
    pub fn publish(&self, message: &Message) -> rusqlite::Result<i64> {
        self.0
            .lock()
            .expect("sqlite bus lock")
            .publish_idempotent(message)
    }
    pub fn replay(
        &self,
        consumer: &str,
        topic: &Topic,
        limit: u32,
    ) -> rusqlite::Result<Vec<(i64, Message)>> {
        self.0
            .lock()
            .expect("sqlite bus lock")
            .read_after(consumer, topic, limit)
    }
    pub fn ack(&self, consumer: &str, sequence: i64) -> rusqlite::Result<()> {
        self.0
            .lock()
            .expect("sqlite bus lock")
            .ack(consumer, sequence)
    }
    pub async fn publish_async(&self, message: Message) -> rusqlite::Result<i64> {
        let handle = self.clone();
        tokio::task::spawn_blocking(move || handle.publish(&message))
            .await
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?
    }
    pub async fn replay_async(
        &self,
        consumer: String,
        topic: Topic,
        limit: u32,
    ) -> rusqlite::Result<Vec<(i64, Message)>> {
        let handle = self.clone();
        tokio::task::spawn_blocking(move || handle.replay(&consumer, &topic, limit))
            .await
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?
    }
    pub async fn ack_async(&self, consumer: String, sequence: i64) -> rusqlite::Result<()> {
        let handle = self.clone();
        tokio::task::spawn_blocking(move || handle.ack(&consumer, sequence))
            .await
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?
    }
    pub fn subscribe(
        &self,
        consumer: impl Into<String>,
        topic: impl Into<Topic>,
    ) -> SqliteSubscription {
        SqliteSubscription {
            handle: self.clone(),
            consumer: consumer.into(),
            topic: topic.into(),
        }
    }
}
impl SqliteSubscription {
    pub async fn recv(&mut self) -> rusqlite::Result<(i64, Message)> {
        let rows = self
            .handle
            .replay_async(self.consumer.clone(), self.topic.clone(), 1)
            .await?;
        rows.into_iter()
            .next()
            .ok_or(rusqlite::Error::QueryReturnedNoRows)
    }
    pub async fn recv_wait(&mut self) -> rusqlite::Result<(i64, Message)> {
        loop {
            match self.recv().await {
                Err(rusqlite::Error::QueryReturnedNoRows) => {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await
                }
                result => return result,
            }
        }
    }
    pub async fn recv_wait_timeout(
        &mut self,
        timeout: std::time::Duration,
    ) -> rusqlite::Result<(i64, Message)> {
        tokio::time::timeout(timeout, self.recv_wait())
            .await
            .map_err(|_| rusqlite::Error::QueryReturnedNoRows)?
    }
    pub async fn ack(&mut self, sequence: i64) -> rusqlite::Result<()> {
        self.handle.ack_async(self.consumer.clone(), sequence).await
    }
}
impl DurableEventBus for SqliteBusHandle {
    type Error = rusqlite::Error;
    fn publish_durable(&self, message: &Message) -> Result<i64, Self::Error> {
        self.publish(message)
    }
    fn replay(
        &self,
        consumer: &str,
        topic: &Topic,
        limit: u32,
    ) -> Result<Vec<(i64, Message)>, Self::Error> {
        self.replay(consumer, topic, limit)
    }
    fn acknowledge(&self, consumer: &str, sequence: i64) -> Result<(), Self::Error> {
        self.ack(consumer, sequence)
    }
}
impl EventBus for SqliteBusHandle {
    type Subscription = SqliteSubscription;
    type Error = rusqlite::Error;
    fn publish(&self, message: Message) -> Result<usize, Self::Error> {
        self.publish(&message).map(|_| 1)
    }
    fn subscribe(&self, topic: impl Into<Topic>) -> Self::Subscription {
        self.subscribe(format!("ephemeral-{}", Uuid::new_v4()), topic)
    }
}
impl SqliteBus {
    pub fn schema_version(&self) -> rusqlite::Result<i64> {
        self.conn
            .query_row("SELECT version FROM schema_meta LIMIT 1", [], |r| r.get(0))
    }
    pub fn open(path: impl AsRef<std::path::Path>) -> rusqlite::Result<Self> {
        let c = Connection::open(path)?;
        let b = Self { conn: c };
        b.init()?;
        Ok(b)
    }
    pub fn open_memory() -> rusqlite::Result<Self> {
        let c = Connection::open_in_memory()?;
        let b = Self { conn: c };
        b.init()?;
        Ok(b)
    }
    fn init(&self) -> rusqlite::Result<()> {
        self.conn.execute_batch("CREATE TABLE IF NOT EXISTS schema_meta(version INTEGER NOT NULL); CREATE TABLE IF NOT EXISTS messages(seq INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT UNIQUE,run_id TEXT,topic TEXT,sender TEXT,kind TEXT,correlation_id TEXT,payload TEXT); CREATE TABLE IF NOT EXISTS topic_offsets(consumer TEXT NOT NULL,topic TEXT NOT NULL,seq INTEGER NOT NULL,PRIMARY KEY(consumer,topic)); CREATE TABLE IF NOT EXISTS supervisor_tasks(run_id TEXT NOT NULL,task_id TEXT NOT NULL, state TEXT NOT NULL,payload TEXT NOT NULL,updated_at INTEGER NOT NULL,PRIMARY KEY(run_id,task_id));")?;
        self.conn.execute("INSERT INTO schema_meta(version) SELECT 1 WHERE NOT EXISTS (SELECT 1 FROM schema_meta)", [])?;
        let version = self.schema_version()?;
        if version > 1 {
            return Err(rusqlite::Error::InvalidParameterName(
                "unsupported schema version".into(),
            ));
        }
        Ok(())
    }
    pub fn publish(&self, m: &Message) -> rusqlite::Result<i64> {
        self.conn.execute("INSERT INTO messages(id,run_id,topic,sender,kind,correlation_id,payload) VALUES(?,?,?,?,?,?,?)",params![m.id.to_string(),m.run_id.to_string(),m.topic.0,m.sender,m.kind,m.correlation_id.map(|x|x.to_string()),serde_json::to_string(&m.payload).unwrap()])?;
        Ok(self.conn.last_insert_rowid())
    }
    /// The same ID and envelope return the original sequence; conflicting reuse is an error.
    pub fn publish_idempotent(&self, m: &Message) -> rusqlite::Result<i64> {
        let tx = self.conn.unchecked_transaction()?;
        let payload = m.payload.to_string();
        tx.execute("INSERT INTO messages(id,run_id,topic,sender,kind,correlation_id,payload) VALUES(?,?,?,?,?,?,?) ON CONFLICT(id) DO NOTHING", params![m.id.to_string(),m.run_id.to_string(),m.topic.0,m.sender,m.kind,m.correlation_id.map(|x|x.to_string()),payload])?;
        let seq = tx.query_row(
            "SELECT seq FROM messages WHERE id=? AND run_id=? AND topic=? AND sender=? AND kind=? AND correlation_id IS ? AND payload=?",
            params![m.id.to_string(),m.run_id.to_string(),m.topic.0,m.sender,m.kind,m.correlation_id.map(|x|x.to_string()),payload], |r| r.get(0),
        ).optional()?.ok_or_else(|| rusqlite::Error::InvalidParameterName("message ID reused with different content".into()))?;
        tx.commit()?;
        Ok(seq)
    }
    /// Replay in order without advancing the consumer position. Ack only after processing.
    pub fn read_after(
        &self,
        consumer: &str,
        topic: &Topic,
        limit: u32,
    ) -> rusqlite::Result<Vec<(i64, Message)>> {
        let start = self.offset(consumer, &topic.0)?;
        let mut statement = self.conn.prepare("SELECT seq,id,run_id,topic,sender,kind,correlation_id,payload FROM messages WHERE seq>? AND topic=? ORDER BY seq LIMIT ?")?;
        statement
            .query_map(params![start, topic.0, limit], |row| {
                let parse_uuid = |column| -> rusqlite::Result<Uuid> {
                    let value: String = row.get(column)?;
                    Uuid::parse_str(&value).map_err(|e| {
                        rusqlite::Error::FromSqlConversionFailure(
                            column,
                            rusqlite::types::Type::Text,
                            Box::new(e),
                        )
                    })
                };
                let correlation: Option<String> = row.get(6)?;
                let correlation_id = correlation
                    .map(|v| Uuid::parse_str(&v))
                    .transpose()
                    .map_err(|e| {
                        rusqlite::Error::FromSqlConversionFailure(
                            6,
                            rusqlite::types::Type::Text,
                            Box::new(e),
                        )
                    })?;
                let payload: String = row.get(7)?;
                Ok((
                    row.get(0)?,
                    Message {
                        id: parse_uuid(1)?,
                        run_id: parse_uuid(2)?,
                        topic: Topic(row.get(3)?),
                        sender: row.get(4)?,
                        kind: row.get(5)?,
                        correlation_id,
                        payload: serde_json::from_str(&payload).map_err(|e| {
                            rusqlite::Error::FromSqlConversionFailure(
                                7,
                                rusqlite::types::Type::Text,
                                Box::new(e),
                            )
                        })?,
                    },
                ))
            })?
            .collect()
    }
    fn offset(&self, consumer: &str, topic: &str) -> rusqlite::Result<i64> {
        Ok(self
            .conn
            .query_row(
                "SELECT seq FROM topic_offsets WHERE consumer=? AND topic=?",
                params![consumer, topic],
                |row| row.get(0),
            )
            .optional()?
            .unwrap_or(0))
    }
    /// Ack the next message of its topic. Repeated old acknowledgements are harmless;
    /// acknowledging ahead of an unprocessed message is rejected rather than losing it.
    pub fn ack(&self, consumer: &str, seq: i64) -> rusqlite::Result<()> {
        let tx = self.conn.unchecked_transaction()?;
        let topic: String = tx.query_row("SELECT topic FROM messages WHERE seq=?", [seq], |r| {
            r.get(0)
        })?;
        let current = self.offset(consumer, &topic)?;
        if seq > current {
            let next: i64 = tx.query_row(
                "SELECT MIN(seq) FROM messages WHERE topic=? AND seq>?",
                params![topic, current],
                |r| r.get(0),
            )?;
            if next != seq {
                return Err(rusqlite::Error::InvalidParameterName(
                    "ack must follow topic order".into(),
                ));
            }
            tx.execute("INSERT INTO topic_offsets(consumer,topic,seq) VALUES(?,?,?) ON CONFLICT(consumer,topic) DO UPDATE SET seq=MAX(topic_offsets.seq,excluded.seq)", params![consumer,topic,seq])?;
        }
        tx.commit()
    }
}

impl DurableEventBus for SqliteBus {
    type Error = rusqlite::Error;
    fn publish_durable(&self, message: &Message) -> Result<i64, Self::Error> {
        self.publish_idempotent(message)
    }
    fn replay(
        &self,
        consumer: &str,
        topic: &Topic,
        limit: u32,
    ) -> Result<Vec<(i64, Message)>, Self::Error> {
        self.read_after(consumer, topic, limit)
    }
    fn acknowledge(&self, consumer: &str, sequence: i64) -> Result<(), Self::Error> {
        self.ack(consumer, sequence)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn replay_and_ack_survive_reopen() {
        let p = std::env::temp_dir().join(format!("multi-{}.db", Uuid::new_v4()));
        {
            let b = SqliteBus::open(&p).unwrap();
            let m = Message::new(Uuid::new_v4(), "t", "s", "k", serde_json::json!({"x":1}));
            b.publish(&m).unwrap();
            let x = b.read_after("c", &Topic("t".into()), 10).unwrap();
            assert_eq!(x.len(), 1);
            b.ack("c", x[0].0).unwrap();
            assert!(
                b.read_after("c", &Topic("t".into()), 10)
                    .unwrap()
                    .is_empty()
            );
        }
        {
            let b = SqliteBus::open(&p).unwrap();
            assert!(
                b.read_after("c", &Topic("t".into()), 10)
                    .unwrap()
                    .is_empty()
            );
        }
        let _ = std::fs::remove_file(p);
    }
    #[test]
    fn idempotent_publish_deduplicates() {
        let b = SqliteBus::open_memory().unwrap();
        let m = Message::new(Uuid::new_v4(), "t", "s", "k", serde_json::json!(1));
        let a = b.publish_idempotent(&m).unwrap();
        let c = b.publish_idempotent(&m).unwrap();
        assert_eq!(a, c);
        assert_eq!(b.read_after("x", &Topic("t".into()), 10).unwrap().len(), 1);
    }
    #[test]
    fn ack_never_moves_backwards() {
        let b = SqliteBus::open_memory().unwrap();
        let m = |n| Message::new(Uuid::new_v4(), "t", "s", "k", serde_json::json!(n));
        let first = b.publish(&m(1)).unwrap();
        let second = b.publish(&m(2)).unwrap();
        let third = b.publish(&m(3)).unwrap();
        b.ack("c", first).unwrap();
        b.ack("c", second).unwrap();
        b.ack("c", first).unwrap();
        let remaining = b.read_after("c", &Topic("t".into()), 10).unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].0, third);
    }
    #[test]
    fn acknowledgements_are_topic_scoped_and_cannot_skip() {
        let b = SqliteBus::open_memory().unwrap();
        let a = b
            .publish(&Message::new(
                Uuid::new_v4(),
                "a",
                "s",
                "k",
                serde_json::json!(1),
            ))
            .unwrap();
        let z = b
            .publish(&Message::new(
                Uuid::new_v4(),
                "z",
                "s",
                "k",
                serde_json::json!(2),
            ))
            .unwrap();
        let a2 = b
            .publish(&Message::new(
                Uuid::new_v4(),
                "a",
                "s",
                "k",
                serde_json::json!(3),
            ))
            .unwrap();
        b.ack("c", z).unwrap();
        assert_eq!(b.read_after("c", &Topic("a".into()), 10).unwrap()[0].0, a);
        assert!(b.ack("c", a2).is_err());
        assert_eq!(b.read_after("c", &Topic("a".into()), 10).unwrap().len(), 2);
        assert!(b.ack("c", 9999).is_err());
        b.ack("c", a).unwrap();
        b.ack("c", a2).unwrap();
        assert!(
            b.read_after("c", &Topic("a".into()), 10)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            b.read_after("other", &Topic("a".into()), 10).unwrap().len(),
            2
        );
    }
    #[test]
    fn corrupt_storage_returns_error_instead_of_panicking() {
        let b = SqliteBus::open_memory().unwrap();
        let m = Message::new(Uuid::new_v4(), "t", "s", "k", serde_json::json!(1));
        b.publish(&m).unwrap();
        b.conn
            .execute("UPDATE messages SET id='bad-uuid'", [])
            .unwrap();
        assert!(b.read_after("c", &Topic("t".into()), 10).is_err());
    }
    #[test]
    fn conflicting_duplicate_and_failed_ack_leave_storage_unchanged() {
        let b = SqliteBus::open_memory().unwrap();
        let mut m = Message::new(Uuid::new_v4(), "t", "s", "k", serde_json::json!(1));
        let seq = b.publish_idempotent(&m).unwrap();
        m.payload = serde_json::json!(2);
        assert!(b.publish_idempotent(&m).is_err());
        let rows = b.read_after("c", &Topic("t".into()), 10).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1.payload, serde_json::json!(1));
        b.conn.execute_batch("CREATE TRIGGER fail_ack BEFORE INSERT ON topic_offsets BEGIN SELECT RAISE(ABORT, 'injected failure'); END;").unwrap();
        assert!(b.ack("c", seq).is_err());
        assert_eq!(b.read_after("c", &Topic("t".into()), 10).unwrap()[0].0, seq);
        b.conn.execute_batch("DROP TRIGGER fail_ack").unwrap();
        b.ack("c", seq).unwrap();
        assert!(
            b.read_after("c", &Topic("t".into()), 10)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn shared_handle_serializes_concurrent_publishers() {
        let handle = SqliteBusHandle::new(SqliteBus::open_memory().unwrap());
        let mut joins = Vec::new();
        for _ in 0..8 {
            let h = handle.clone();
            joins.push(std::thread::spawn(move || {
                h.publish(&Message::new(
                    Uuid::new_v4(),
                    "t",
                    "p",
                    "k",
                    serde_json::json!(1),
                ))
                .unwrap();
            }));
        }
        for join in joins {
            join.join().unwrap();
        }
        assert_eq!(handle.replay("c", &Topic("t".into()), 20).unwrap().len(), 8);
    }
    #[tokio::test]
    async fn async_handle_round_trip_is_durable_and_idempotent() {
        let handle = SqliteBusHandle::new(SqliteBus::open_memory().unwrap());
        let message = Message::new(
            Uuid::new_v4(),
            "async",
            "worker",
            "task",
            serde_json::json!({"ok":true}),
        );
        let first = handle.publish_async(message.clone()).await.unwrap();
        let duplicate = handle.publish_async(message).await.unwrap();
        assert_eq!(first, duplicate);
        let rows = handle
            .replay_async("consumer".into(), Topic("async".into()), 10)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        handle
            .ack_async("consumer".into(), rows[0].0)
            .await
            .unwrap();
        assert!(
            handle
                .replay_async("consumer".into(), Topic("async".into()), 10)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn durable_subscription_requires_explicit_ack() {
        let handle = SqliteBusHandle::new(SqliteBus::open_memory().unwrap());
        let message = Message::new(Uuid::new_v4(), "t", "worker", "k", serde_json::json!(1));
        handle.publish_async(message.clone()).await.unwrap();
        let mut sub = handle.subscribe("consumer", "t");
        let (seq, received) = sub.recv().await.unwrap();
        assert_eq!(received.id, message.id);
        let (_, duplicate) = sub.recv().await.unwrap();
        assert_eq!(duplicate.id, message.id);
        sub.ack(seq).await.unwrap();
        assert!(matches!(
            sub.recv().await,
            Err(rusqlite::Error::QueryReturnedNoRows)
        ));
    }

    #[tokio::test]
    async fn durable_subscription_recv_waits_for_later_publish() {
        let handle = SqliteBusHandle::new(SqliteBus::open_memory().unwrap());
        let mut sub = handle.subscribe("consumer", "wait");
        let producer = handle.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(15)).await;
            producer
                .publish_async(Message::new(
                    Uuid::new_v4(),
                    "wait",
                    "p",
                    "k",
                    serde_json::json!(42),
                ))
                .await
                .unwrap();
        });
        let (_, message) = tokio::time::timeout(std::time::Duration::from_secs(1), sub.recv_wait())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(message.payload, serde_json::json!(42));
    }

    #[tokio::test]
    async fn sqlite_handle_satisfies_event_bus_contract() {
        let bus = SqliteBusHandle::new(SqliteBus::open_memory().unwrap());
        let mut subscription = <SqliteBusHandle as EventBus>::subscribe(&bus, "contract");
        let message = Message::new(
            Uuid::new_v4(),
            "contract",
            "s",
            "k",
            serde_json::json!(true),
        );
        assert_eq!(
            <SqliteBusHandle as EventBus>::publish(&bus, message.clone()).unwrap(),
            1
        );
        let (_, received) = subscription.recv().await.unwrap();
        assert_eq!(received.id, message.id);
    }

    #[test]
    fn schema_version_is_one_for_new_databases() {
        let bus = SqliteBus::open_memory().unwrap();
        assert_eq!(bus.schema_version().unwrap(), 1);
    }

    #[test]
    fn future_schema_version_is_rejected() {
        let path = std::env::temp_dir().join(format!("mina-schema-{}.db", Uuid::new_v4()));
        let c = Connection::open(&path).unwrap();
        c.execute_batch("CREATE TABLE schema_meta(version INTEGER NOT NULL); INSERT INTO schema_meta VALUES(99);").unwrap();
        drop(c);
        assert!(SqliteBus::open(&path).is_err());
        let _ = std::fs::remove_file(path);
    }
}
